//! B5 — the Frame.io token auto-refresh rung (environmental self-heal, device
//! side). A step that fails auth-expired mid-run refreshes the Adobe IMS token
//! via the refresh_token grant and REWRITES the credential env file
//! (`CYAN_CRED_ENV_FILE`, default `~/.frameio.env`) — the same file every
//! plugin spawn and connector scan reads FRESH, so one refresh heals them all.
//! No human, no raw red banner. Secrets never logged; the refresh token never
//! leaves the file except toward Adobe IMS.
//!
//! Blocking HTTP on purpose: this runs on the step-dispatch thread (the FFI
//! caller's), never inside the engine's async runtime — the same rule as
//! `ingest_connectors`.

use std::io::Write;

use anyhow::{anyhow, Result};

/// Adobe IMS public PKCE client id (a public identifier, not a secret) — the
/// same one the `cyan` bring-up rail uses. Overridable via `CYAN_IMS_CLIENT_ID`.
const DEFAULT_IMS_CLIENT_ID: &str = "b32a6b10eea6429aab68ac6c3d2debe3";

/// Refresh `FRAMEIO_IMS_TOKEN` in the credential env file using its
/// `FRAMEIO_REFRESH_TOKEN`. Returns `Ok(true)` when a fresh token was written
/// (the caller may retry the failed step once), `Ok(false)` when no refresh is
/// possible (no file / no refresh token) — the caller falls through to the
/// human-facing error.
pub fn refresh_cred_file() -> Result<bool> {
    let path = crate::mcp_host::cred_env_file();
    if !path.is_file() {
        return Ok(false);
    }
    let body = std::fs::read_to_string(&path)?;
    let lookup = |key: &str| -> Option<String> {
        body.lines()
            .filter_map(|l| l.split_once('='))
            .find(|(k, _)| k.trim() == key)
            .map(|(_, v)| v.trim().to_string())
            .filter(|v| !v.is_empty())
    };
    let Some(refresh_token) = lookup("FRAMEIO_REFRESH_TOKEN") else {
        return Ok(false);
    };

    let client_id = std::env::var("CYAN_IMS_CLIENT_ID")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_IMS_CLIENT_ID.to_string());
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(20))
        .build()?;
    let resp: serde_json::Value = client
        .post(format!(
            "https://ims-na1.adobelogin.com/ims/token/v3?client_id={client_id}"
        ))
        .form(&[
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token.as_str()),
        ])
        .send()?
        .json()?;
    let Some(access) = resp.get("access_token").and_then(|v| v.as_str()) else {
        // The grant was rejected — nothing to retry with. Not an error to raise:
        // the step's own auth error is the honest surface.
        return Ok(false);
    };
    let new_refresh = resp
        .get("refresh_token")
        .and_then(|v| v.as_str())
        .unwrap_or(refresh_token.as_str());

    // Atomic-ish rewrite: filter the two token lines, append fresh ones, 0600.
    let kept: String = body
        .lines()
        .filter(|l| {
            !l.trim_start().starts_with("FRAMEIO_IMS_TOKEN=")
                && !l.trim_start().starts_with("FRAMEIO_REFRESH_TOKEN=")
        })
        .map(|l| format!("{l}\n"))
        .collect();
    let tmp = path.with_extension("env.tmp");
    {
        let mut f = std::fs::File::create(&tmp)?;
        write!(f, "{kept}FRAMEIO_IMS_TOKEN={access}\nFRAMEIO_REFRESH_TOKEN={new_refresh}\n")?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
    }
    std::fs::rename(&tmp, &path).map_err(|e| anyhow!("cred file rewrite: {e}"))?;
    tracing::info!("frameio IMS token auto-refreshed (B5 environmental self-heal)");
    Ok(true)
}

/// Is this error family the expired/invalid-credential class the refresh heals?
pub fn is_auth_error(error_class: &str) -> bool {
    matches!(error_class, "auth" | "unauthorized" | "token_expired")
        || error_class.contains("401")
}
