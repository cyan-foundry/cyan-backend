//! STAGE-4 C2 — the REAL remote ingest transports behind `ingest`'s connector
//! seam: **Frame.io C2C** (watch a project/folder; new dailies download and
//! materialize) and **S3** (watch a bucket/prefix). The `folder` kind stays in
//! `ingest.rs` (no transport needed).
//!
//! Design rules:
//! - **The trait is the seam.** `RemoteConnector` = LIST + FETCH + canonical
//!   LOCATION. Unit tests drive `ingest::scan_remote_with_conn` with a fake;
//!   nothing here runs in default `cargo test`.
//! - **Credentials resolve FRESH per scan** — same rail as plugin spawns:
//!   `CYAN_CRED_ENV_FILE` (default `~/.frameio.env`, auto-refreshed) → process
//!   env. Never logged, never persisted on the source row.
//! - **Blocking HTTP on purpose**: scans run on the app's cadence thread (the
//!   FFI caller), never inside the engine's async runtime; the process-DB lock
//!   is NOT held across these calls (see `ingest::scan_remote_global`).

use std::path::Path;

use anyhow::{anyhow, Result};

use crate::ingest::{IngestSource, RemoteConnector, RemoteItem};

/// Browser-ish UA: Frame.io's edge rejects bare library UAs (observed live:
/// default-curl → 401 on a VALID token).
const UA: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) cyan-ingest/1.0";

/// Credential lookup: dotenv file first (fresh read — the auto-refresh rail),
/// then process env. Empty values are absent.
fn cred(key: &str) -> Option<String> {
    let file = crate::mcp_host::cred_env_file();
    crate::mcp_host::dotenv_lookup(&file, key)
        .or_else(|| std::env::var(key).ok())
        .filter(|v| !v.trim().is_empty())
}

fn http() -> Result<reqwest::blocking::Client> {
    reqwest::blocking::Client::builder()
        .user_agent(UA)
        .connect_timeout(std::time::Duration::from_secs(20))
        .timeout(std::time::Duration::from_secs(600)) // dailies are big
        .build()
        .map_err(|e| anyhow!("http client: {e}"))
}

/// Stream a GET to `dest` atomically (tmp + rename) so a torn download never
/// looks like an ingested master.
fn download(client: &reqwest::blocking::Client, url: &str, bearer: Option<&str>, dest: &Path) -> Result<()> {
    if let Some(dir) = dest.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let mut req = client.get(url);
    if let Some(token) = bearer {
        req = req.bearer_auth(token);
    }
    let mut resp = req.send().map_err(|e| anyhow!("download: {e}"))?;
    if !resp.status().is_success() {
        return Err(anyhow!("download failed: HTTP {}", resp.status()));
    }
    let tmp = dest.with_extension("part");
    let mut out = std::fs::File::create(&tmp)?;
    std::io::copy(&mut resp, &mut out).map_err(|e| anyhow!("download write: {e}"))?;
    std::fs::rename(&tmp, dest)?;
    Ok(())
}

/// C3 — retrieve a master by its canonical LOCATION into `dest` (or verify it
/// where it already lives). The "produce master" leg-2 retrieval:
///
/// - `file://<path>` / a bare absolute path → verified in place, no copy;
/// - `s3://bucket/key` → presigned GetObject download;
/// - `frameio://<account>/file/<file_id>` → media_links.original download.
///
/// Returns the local path holding the master's bytes.
pub fn retrieve_by_location(location: &str, dest: &Path) -> Result<std::path::PathBuf> {
    let loc = location.trim();
    if let Some(path) = loc.strip_prefix("file://").or(loc.starts_with('/').then_some(loc)) {
        let p = std::path::PathBuf::from(path);
        if !p.is_file() {
            return Err(anyhow!("master location {loc} does not exist on disk"));
        }
        return Ok(p);
    }
    if loc.starts_with("s3://") {
        let s3 = S3Connector::from_creds()?;
        let (bucket, key) = S3Connector::parse_uri(loc)?;
        if key.is_empty() {
            return Err(anyhow!("s3 master location names no object key: {loc}"));
        }
        let item = RemoteItem {
            name: key.rsplit('/').next().unwrap_or(&key).to_string(),
            provider: "s3",
            ref_id: format!("{bucket}/{key}@"),
            size: 0,
        };
        let src = placeholder_source(loc);
        s3.fetch(&src, &item, dest)?;
        return Ok(dest.to_path_buf());
    }
    if loc.starts_with("frameio://") {
        // frameio://<account>/file/<file_id>
        let rest = loc.strip_prefix("frameio://").unwrap_or_default();
        let parts: Vec<&str> = rest.split('/').filter(|p| !p.is_empty()).collect();
        let (account, file_id) = match parts.as_slice() {
            [account, "file", file_id] => (account.to_string(), file_id.to_string()),
            _ => return Err(anyhow!("unrecognized frameio master location: {loc}")),
        };
        let c2c = FrameioC2cConnector::from_creds()?;
        let item = RemoteItem { name: String::new(), provider: "frameio", ref_id: file_id, size: 0 };
        // fetch() resolves the account from the source URI — hand it one.
        let src = placeholder_source(&format!("frameio://{account}/x"));
        c2c.fetch(&src, &item, dest)?;
        return Ok(dest.to_path_buf());
    }
    Err(anyhow!("no retrieval transport for master location '{loc}'"))
}

/// A synthetic source row for location-based retrieval (the connectors only
/// read `uri` from it).
fn placeholder_source(uri: &str) -> IngestSource {
    IngestSource {
        id: String::new(),
        tenant_id: String::new(),
        board_id: String::new(),
        kind: String::new(),
        uri: uri.to_string(),
        schedule_secs: None,
        last_scan_at: None,
        created_at: 0,
    }
}

/// The prod connector for a source kind. Errors name the missing credential —
/// honestly and without leaking values.
pub fn connector_for(kind: &str) -> Result<Box<dyn RemoteConnector>> {
    match kind {
        "frameio_c2c" => Ok(Box::new(FrameioC2cConnector::from_creds()?)),
        "s3" => Ok(Box::new(S3Connector::from_creds()?)),
        other => Err(anyhow!("no remote connector for kind '{other}'")),
    }
}

// ============================================================================
// Frame.io C2C — watch a project (or one folder); new dailies auto-ingest.
// ============================================================================

/// V4 REST against `api.frame.io`. Source URI forms (ids are UUIDs):
/// - `frameio://<project_id>` — account from creds, watch the project root
/// - `frameio://<account_id>/<project_id>`
/// - `frameio://<account_id>/<project_id>/<folder_id>` — watch one folder
/// - a bare `<project_id>` also works.
pub struct FrameioC2cConnector {
    token: String,
    account_id: Option<String>,
    api_base: String,
}

impl FrameioC2cConnector {
    pub fn from_creds() -> Result<Self> {
        let token = cred("FRAMEIO_IMS_TOKEN")
            .ok_or_else(|| anyhow!("frameio_c2c: FRAMEIO_IMS_TOKEN missing (credential env file / env)"))?;
        Ok(Self {
            token,
            account_id: cred("FRAMEIO_ACCOUNT_ID"),
            api_base: cred("FRAMEIO_API_BASE").unwrap_or_else(|| "https://api.frame.io".to_string()),
        })
    }

    fn get_json(&self, client: &reqwest::blocking::Client, path: &str) -> Result<serde_json::Value> {
        let resp = client
            .get(format!("{}{}", self.api_base, path))
            .bearer_auth(&self.token)
            .send()
            .map_err(|e| anyhow!("frameio GET {path}: {e}"))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(anyhow!("frameio GET {path}: HTTP {status}"));
        }
        resp.json().map_err(|e| anyhow!("frameio GET {path}: bad JSON: {e}"))
    }

    /// `(account_id, project_id, folder_id?)` from the source URI + creds.
    fn resolve_uri(&self, uri: &str) -> Result<(String, String, Option<String>)> {
        let trimmed = uri.trim().strip_prefix("frameio://").unwrap_or(uri.trim());
        let parts: Vec<&str> = trimmed.split('/').filter(|p| !p.is_empty()).collect();
        match parts.as_slice() {
            [project] => {
                let account = self.account_id.clone().ok_or_else(|| {
                    anyhow!("frameio_c2c: URI names only a project — set FRAMEIO_ACCOUNT_ID or use frameio://<account>/<project>")
                })?;
                Ok((account, project.to_string(), None))
            }
            [account, project] => Ok((account.to_string(), project.to_string(), None)),
            [account, project, folder] => {
                Ok((account.to_string(), project.to_string(), Some(folder.to_string())))
            }
            _ => Err(anyhow!("frameio_c2c: unrecognized URI '{uri}'")),
        }
    }
}

impl RemoteConnector for FrameioC2cConnector {
    fn provider(&self) -> &'static str {
        "frameio"
    }

    fn list(&self, source: &IngestSource) -> Result<Vec<RemoteItem>> {
        let client = http()?;
        let (account, project, folder) = self.resolve_uri(&source.uri)?;
        let folder_id = match folder {
            Some(f) => f,
            None => {
                let v = self.get_json(&client, &format!("/v4/accounts/{account}/projects/{project}"))?;
                v["data"]["root_folder_id"]
                    .as_str()
                    .ok_or_else(|| anyhow!("frameio_c2c: project {project} has no root_folder_id"))?
                    .to_string()
            }
        };
        let v = self.get_json(
            &client,
            &format!("/v4/accounts/{account}/folders/{folder_id}/children?page_size=100"),
        )?;
        let items = v["data"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter(|f| f["type"] == "file")
                    // Only settled uploads: a C2C daily mid-upload has no
                    // original yet; it ingests on the next tick.
                    .filter(|f| f["status"] == "transcoded")
                    .filter_map(|f| {
                        Some(RemoteItem {
                            name: f["name"].as_str()?.to_string(),
                            provider: "frameio",
                            ref_id: f["id"].as_str()?.to_string(),
                            size: f["file_size"].as_u64().unwrap_or(0),
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        Ok(items)
    }

    fn fetch(&self, source: &IngestSource, item: &RemoteItem, dest: &Path) -> Result<()> {
        let client = http()?;
        let (account, _, _) = self.resolve_uri(&source.uri)?;
        let v = self.get_json(
            &client,
            &format!("/v4/accounts/{account}/files/{}?include=media_links.original", item.ref_id),
        )?;
        let url = v["data"]["media_links"]["original"]["download_url"]
            .as_str()
            .ok_or_else(|| anyhow!("frameio_c2c: file {} has no original download_url yet", item.ref_id))?;
        // The download URL is a signed CDN link — no bearer.
        download(&client, url, None, dest)
    }

    fn location(&self, source: &IngestSource, item: &RemoteItem) -> String {
        let account = self
            .resolve_uri(&source.uri)
            .map(|(a, _, _)| a)
            .unwrap_or_default();
        format!("frameio://{account}/file/{}", item.ref_id)
    }
}

// ============================================================================
// S3 — watch a bucket/prefix (`s3://bucket/prefix`).
// ============================================================================

/// SigV4 presigned requests via `rusty-s3`, fetched with the blocking client.
/// Creds: `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (+ `AWS_REGION`,
/// optional `AWS_ENDPOINT_URL` for MinIO/LocalStack), dotenv-file first.
pub struct S3Connector {
    credentials: rusty_s3::Credentials,
    region: String,
    endpoint: String,
}

impl S3Connector {
    pub fn from_creds() -> Result<Self> {
        let key = cred("AWS_ACCESS_KEY_ID")
            .ok_or_else(|| anyhow!("s3: AWS_ACCESS_KEY_ID missing (credential env file / env)"))?;
        let secret = cred("AWS_SECRET_ACCESS_KEY")
            .ok_or_else(|| anyhow!("s3: AWS_SECRET_ACCESS_KEY missing (credential env file / env)"))?;
        let region = cred("AWS_REGION").unwrap_or_else(|| "us-east-1".to_string());
        let endpoint = cred("AWS_ENDPOINT_URL")
            .unwrap_or_else(|| format!("https://s3.{region}.amazonaws.com"));
        Ok(Self {
            credentials: rusty_s3::Credentials::new(key, secret),
            region,
            endpoint,
        })
    }

    /// `(bucket, prefix)` from `s3://bucket[/prefix…]`.
    fn parse_uri(uri: &str) -> Result<(String, String)> {
        let rest = uri
            .trim()
            .strip_prefix("s3://")
            .ok_or_else(|| anyhow!("s3: URI must be s3://bucket[/prefix], got '{uri}'"))?;
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        if bucket.is_empty() {
            return Err(anyhow!("s3: URI names no bucket: '{uri}'"));
        }
        Ok((bucket.to_string(), prefix.to_string()))
    }

    fn bucket(&self, name: &str) -> Result<rusty_s3::Bucket> {
        rusty_s3::Bucket::new(
            self.endpoint.parse().map_err(|e| anyhow!("s3 endpoint: {e}"))?,
            rusty_s3::UrlStyle::VirtualHost,
            name.to_string(),
            self.region.clone(),
        )
        .map_err(|e| anyhow!("s3 bucket: {e}"))
    }

    const SIGN_TTL: std::time::Duration = std::time::Duration::from_secs(600);
}

impl RemoteConnector for S3Connector {
    fn provider(&self) -> &'static str {
        "s3"
    }

    fn list(&self, source: &IngestSource) -> Result<Vec<RemoteItem>> {
        use rusty_s3::S3Action;
        let (bucket_name, prefix) = Self::parse_uri(&source.uri)?;
        let bucket = self.bucket(&bucket_name)?;
        let client = http()?;
        let mut action = bucket.list_objects_v2(Some(&self.credentials));
        if !prefix.is_empty() {
            action.with_prefix(&prefix);
        }
        let url = action.sign(Self::SIGN_TTL);
        let resp = client.get(url).send().map_err(|e| anyhow!("s3 list: {e}"))?;
        let status = resp.status();
        let text = resp.text().map_err(|e| anyhow!("s3 list body: {e}"))?;
        if !status.is_success() {
            return Err(anyhow!("s3 list: HTTP {status}"));
        }
        let parsed = rusty_s3::actions::ListObjectsV2::parse_response(&text)
            .map_err(|e| anyhow!("s3 list parse: {e}"))?;
        let items = parsed
            .contents
            .into_iter()
            .filter(|o| !o.key.ends_with('/'))
            .map(|o| {
                let name = o.key.rsplit('/').next().unwrap_or(&o.key).to_string();
                RemoteItem {
                    name,
                    provider: "s3",
                    // etag in the ref: a rewritten object (same key, new bytes)
                    // must read as NEW content, not a dedup hit.
                    ref_id: format!("{bucket_name}/{}@{}", o.key, o.etag.trim_matches('"')),
                    size: o.size,
                }
            })
            .collect();
        Ok(items)
    }

    fn fetch(&self, _source: &IngestSource, item: &RemoteItem, dest: &Path) -> Result<()> {
        use rusty_s3::S3Action;
        let (bucket_name, key) = item
            .ref_id
            .split_once('/')
            .and_then(|(b, rest)| rest.rsplit_once('@').map(|(k, _)| (b.to_string(), k.to_string())))
            .ok_or_else(|| anyhow!("s3: malformed ref '{}'", item.ref_id))?;
        let bucket = self.bucket(&bucket_name)?;
        let client = http()?;
        let url = bucket.get_object(Some(&self.credentials), &key).sign(Self::SIGN_TTL);
        download(&client, url.as_str(), None, dest)
    }

    fn location(&self, _source: &IngestSource, item: &RemoteItem) -> String {
        // Canonical master location = the object, sans the etag suffix.
        let path = item.ref_id.rsplit_once('@').map(|(p, _)| p).unwrap_or(&item.ref_id);
        format!("s3://{path}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c2c(account: Option<&str>) -> FrameioC2cConnector {
        FrameioC2cConnector {
            token: "t".into(),
            account_id: account.map(str::to_string),
            api_base: "https://api.frame.io".into(),
        }
    }

    #[test]
    fn frameio_uri_forms_resolve() {
        let c = c2c(Some("acct-1"));
        assert_eq!(
            c.resolve_uri("frameio://proj-1").unwrap(),
            ("acct-1".into(), "proj-1".into(), None)
        );
        assert_eq!(
            c.resolve_uri("frameio://a2/p2").unwrap(),
            ("a2".into(), "p2".into(), None)
        );
        assert_eq!(
            c.resolve_uri("frameio://a2/p2/f9").unwrap(),
            ("a2".into(), "p2".into(), Some("f9".into()))
        );
        assert_eq!(
            c.resolve_uri("bare-project").unwrap(),
            ("acct-1".into(), "bare-project".into(), None)
        );
        // project-only with no account anywhere is a clear error, not a guess
        assert!(c2c(None).resolve_uri("frameio://p").is_err());
    }

    #[test]
    fn s3_uri_forms_resolve() {
        assert_eq!(
            S3Connector::parse_uri("s3://bkt/dailies/day1").unwrap(),
            ("bkt".into(), "dailies/day1".into())
        );
        assert_eq!(S3Connector::parse_uri("s3://bkt").unwrap(), ("bkt".into(), "".into()));
        assert!(S3Connector::parse_uri("http://x").is_err());
        assert!(S3Connector::parse_uri("s3:///nope").is_err());
    }
}
