// src/swarm.rs
//
// Content-addressed, multi-source blob swarming for cyan-backend (G10) — the engine consumer
// of the blob-swarm primitive.
//
// PROVENANCE / DEPENDENCY DECISION (see STATUS_FILE_SWARM_CONSUMER.md): this mirrors
// `xaeroflux::swarm::BlobSwarm` (xaeroflux `feat/blob-swarm`, STATUS_BLOB_SWARM.md). We deliberately
// **mirror** the ~250-line primitive rather than take a path/git dependency on xaeroflux: depending on
// the whole `xaeroflux` crate would drag its entire engine + the `iggy` message broker into
// cyan-backend — exactly the integration surface this repo is actively *stripping* (see the recent
// `strip:` commits). cyan-backend already declares `iroh` 0.95, `iroh-blobs` 0.97, `bytes`, `serde`,
// `anyhow` and `tokio` at the same versions the primitive needs, so mirroring adds **zero** new
// dependencies and keeps the offline-first engine lean and traceable (the simplicity rule). The API is
// kept identical to the upstream so the two stay easy to diff.
//
// One adaptation for the engine seam: unlike the standalone upstream (which builds its own `Router`),
// this `BlobSwarm` does **not** own a Router. The `NetworkActor` already runs a single `Router` over
// its one endpoint (gossip + snapshot + file + dm ALPNs); mounting a *second* Router on the same
// endpoint would race two `accept()` loops. Instead `blobs_protocol()` hands the caller the
// `BlobsProtocol` to `.accept(BLOB_ALPN, ..)` on that existing Router — so blob holders are addressed
// by the node's *normal* node id (already wired by discovery), one endpoint, one router. Additive and
// behavior-preserving: the gossip/file/dm/snapshot paths are untouched.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use iroh::{Endpoint, PublicKey};
use iroh_blobs::store::fs::FsStore;
use iroh_blobs::BlobsProtocol;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

/// ALPN the blob swarm speaks. Re-exported so the engine can advertise it on the endpoint and mount
/// the blobs protocol on the router without naming `iroh-blobs` directly.
pub const BLOB_ALPN: &[u8] = iroh_blobs::ALPN;

/// The content-address type: a blob's Blake3 hash *is* its identity. Re-exported so callers/tests can
/// name it without a direct `iroh-blobs` dependency.
pub use iroh_blobs::Hash;

/// Per-holder dial cap during a multi-source fetch. A departed holder's address lingers in discovery,
/// so an unbounded `connect` would retry it until QUIC's own long timeout; this bounds each attempt so
/// the fetch falls through to a live holder promptly.
const DIAL_TIMEOUT: Duration = Duration::from_secs(5);

// ============================================================================
// i-have / who-has negotiation messages (carried over the existing gossip channel)
// ============================================================================

/// A swarm control message exchanged over gossip to negotiate who holds a content-addressed blob.
/// Hashes are encoded as their Blake3 hex string so the message is plain JSON, like the snapshot
/// protocol's gossip messages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SwarmMessage {
    /// "I have this blob" — `holder_node_id` holds the blob with this hash and will serve it.
    IHave { hash: String, holder_node_id: String },

    /// "Who has this blob?" — `requester_node_id` is looking for holders of this hash.
    WhoHas { hash: String, requester_node_id: String },
}

// ============================================================================
// BlobSwarm
// ============================================================================

/// A blob-swarm participant: a content-addressed store served on the blobs ALPN, plus the negotiation
/// core (holder registry + message handling) and a multi-source fetcher. One symmetric type — a node
/// can both serve held blobs and fetch missing ones.
pub struct BlobSwarm {
    store: FsStore,
    endpoint: Endpoint,
    node_id: String,
    /// hash (hex) -> set of node_ids that announced they hold it.
    holders: Arc<RwLock<HashMap<String, HashSet<String>>>>,
}

impl BlobSwarm {
    /// Build a blob swarm bound to `endpoint` (whose advertised ALPNs must include [`BLOB_ALPN`]).
    /// The content-addressed store is FS-BACKED under `store_root` (RAM-flat, dailies-grade:
    /// fetched chunks land on disk as they arrive and verified ranges persist across restarts —
    /// never a whole blob in memory). Callers pass a per-node root (e.g. `<data>/blobs/<node>`)
    /// so in-process multi-node tests keep honest per-node stores. The caller mounts
    /// [`BlobSwarm::blobs_protocol`] on its `Router` so this node serves held blobs.
    pub async fn new(endpoint: Endpoint, node_id: String, store_root: &Path) -> Result<Self> {
        tokio::fs::create_dir_all(store_root).await?;
        let store = FsStore::load(store_root)
            .await
            .map_err(|e| anyhow!("blob store at {} failed to load: {e}", store_root.display()))?;
        Ok(Self {
            store,
            endpoint,
            node_id,
            holders: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// The blobs protocol over this swarm's store, for the caller to `.accept(BLOB_ALPN, ..)` on its
    /// existing `Router`. Shares the same underlying store, so blobs `add`ed later are served too.
    pub fn blobs_protocol(&self) -> BlobsProtocol {
        BlobsProtocol::new(&self.store, None)
    }

    /// This node's id (the holder/requester id carried in [`SwarmMessage`]s and dialed on fetch).
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    // ---- content addressing ------------------------------------------------

    /// Add bytes to the local store and return their Blake3 hash (the content's identity).
    pub async fn add(&self, data: impl Into<Bytes>) -> Result<Hash> {
        let tag = self
            .store
            .add_bytes(data.into())
            .await
            .map_err(|e| anyhow!("blob add failed: {e}"))?;
        Ok(tag.hash)
    }

    /// Whether the local store holds the blob for `hash`.
    pub async fn has(&self, hash: &Hash) -> Result<bool> {
        self.store
            .has(*hash)
            .await
            .map_err(|e| anyhow!("blob has() failed: {e}"))
    }

    /// Read a locally-held blob's full contents.
    pub async fn get(&self, hash: &Hash) -> Result<Bytes> {
        self.store
            .get_bytes(*hash)
            .await
            .map_err(|e| anyhow!("blob get_bytes failed: {e}"))
    }

    // ---- i-have / who-has negotiation --------------------------------------

    /// Build an `IHave` announcement for a blob this node holds.
    pub fn announce(&self, hash: &Hash) -> SwarmMessage {
        SwarmMessage::IHave {
            hash: hash.to_string(),
            holder_node_id: self.node_id.clone(),
        }
    }

    /// Build a `WhoHas` query for a blob this node wants.
    pub fn query(&self, hash: &Hash) -> SwarmMessage {
        SwarmMessage::WhoHas {
            hash: hash.to_string(),
            requester_node_id: self.node_id.clone(),
        }
    }

    /// Record that `holder` holds the blob identified by `hash_hex` (a holder this node observed via
    /// an `IHave`). Self-announcements are ignored so a node never lists itself as a remote holder.
    pub async fn record_holder(&self, hash_hex: &str, holder: &str) {
        if holder == self.node_id {
            return;
        }
        let mut holders = self.holders.write().await;
        holders
            .entry(hash_hex.to_string())
            .or_default()
            .insert(holder.to_string());
    }

    /// The remote holders this node currently knows for `hash` (its own observed state).
    pub async fn holders(&self, hash: &Hash) -> Vec<String> {
        let holders = self.holders.read().await;
        holders
            .get(&hash.to_string())
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Process one incoming negotiation message (received over gossip):
    /// - `IHave`  → record the holder; nothing to send back.
    /// - `WhoHas` → if this node holds the blob, return an `IHave` reply for the caller to broadcast.
    ///
    /// Pure negotiation logic over a serialized message; the caller owns the gossip transport.
    pub async fn on_message(&self, raw: &[u8]) -> Result<Option<SwarmMessage>> {
        let msg: SwarmMessage =
            serde_json::from_slice(raw).map_err(|e| anyhow!("malformed swarm message: {e}"))?;
        match msg {
            SwarmMessage::IHave { hash, holder_node_id } => {
                self.record_holder(&hash, &holder_node_id).await;
                Ok(None)
            }
            SwarmMessage::WhoHas { hash, .. } => {
                let parsed = Hash::from_str(&hash)
                    .map_err(|e| anyhow!("WhoHas carried an unparseable hash: {e}"))?;
                if self.has(&parsed).await? {
                    Ok(Some(self.announce(&parsed)))
                } else {
                    Ok(None)
                }
            }
        }
    }

    // ---- multi-source fetch ------------------------------------------------

    /// Fetch the blob for `hash` from the given holders, trying them in turn and resuming across holder
    /// churn, then verify its Blake3 hash before returning the bytes.
    ///
    /// `iroh-blobs` does Blake3-verified streaming and writes verified chunks to the store as they
    /// arrive, tracking which ranges are present. So a fetch against a holder only pulls the *missing*
    /// ranges: if one holder drops mid-transfer (or is already gone when we dial it), we fall through to
    /// the next holder and it resumes from where the previous left off. A single holder leaving
    /// therefore never fails the download as long as some holder in the set can serve the rest.
    ///
    /// On completion we recompute the Blake3 hash of the assembled bytes and reject any mismatch
    /// (defence-in-depth on top of verified streaming) before surfacing the blob.
    pub async fn fetch(&self, hash: &Hash, holders: &[String]) -> Result<Bytes> {
        self.fetch_into_store(hash, holders).await?;

        // Integrity gate: surface the blob only if the assembled content's Blake3 hash matches.
        // This materializes the blob — use [`BlobSwarm::fetch_to_path`] for large media.
        let bytes = self.get(hash).await?;
        let computed = Hash::new(&bytes);
        if &computed != hash {
            return Err(anyhow!(
                "integrity check failed: fetched content hashes to {computed}, expected {hash}"
            ));
        }
        Ok(bytes)
    }

    /// RAM-flat fetch for LARGE media: fetch into the fs-backed store (verified chunks land on
    /// disk as they arrive and persist, so this resumes across holder churn AND process restarts),
    /// then export file-to-file to `dest` and stream-verify the exported file's Blake3 in bounded
    /// buffers. No step holds the whole blob in memory. Returns the byte length.
    pub async fn fetch_to_path(&self, hash: &Hash, holders: &[String], dest: &Path) -> Result<u64> {
        self.fetch_into_store(hash, holders).await?;
        let size = self
            .store
            .blobs()
            .export(*hash, dest)
            .await
            .map_err(|e| anyhow!("blob export to {} failed: {e}", dest.display()))?;

        // Integrity gate, RAM-flat: stream the exported file back through Blake3.
        let computed = hash_file_streaming(dest).await?;
        if &computed != hash {
            return Err(anyhow!(
                "integrity check failed: exported file hashes to {computed}, expected {hash}"
            ));
        }
        Ok(size)
    }

    /// The multi-source transfer core shared by [`BlobSwarm::fetch`]/[`BlobSwarm::fetch_to_path`]:
    /// try each holder in turn until the store holds the complete blob.
    async fn fetch_into_store(&self, hash: &Hash, holders: &[String]) -> Result<()> {
        if holders.is_empty() {
            return Err(anyhow!("cannot fetch {hash}: no holders provided"));
        }

        let providers: Vec<PublicKey> = holders
            .iter()
            .map(|id| {
                id.parse::<PublicKey>()
                    .map_err(|e| anyhow!("holder id '{id}' is not a valid node id: {e}"))
            })
            .collect::<Result<_>>()?;

        let remote = self.store.remote();
        let mut last_err: Option<anyhow::Error> = None;
        for provider in &providers {
            // Resume short-circuit: a previous holder may already have delivered the whole blob.
            if self.has(hash).await? {
                break;
            }
            // Bounded dial: a holder that has left the swarm is no longer reachable, and a raw
            // `connect` would retry its stale address until QUIC's own (long) timeout. Cap each dial so
            // churn falls through to the next holder quickly instead of stalling the fetch.
            let conn = match tokio::time::timeout(
                DIAL_TIMEOUT,
                self.endpoint.connect(*provider, BLOB_ALPN),
            )
            .await
            {
                Ok(Ok(conn)) => conn,
                Ok(Err(e)) => {
                    last_err = Some(anyhow!("dial holder {provider} failed: {e}"));
                    continue;
                }
                Err(_) => {
                    last_err = Some(anyhow!("dial holder {provider} timed out (likely departed)"));
                    continue;
                }
            };
            // `fetch` pulls only the ranges still missing from the local store, so this resumes any
            // partial transfer left behind by an earlier holder that dropped.
            if let Err(e) = remote.fetch(conn, *hash).await {
                last_err = Some(anyhow!("fetch from holder {provider} failed: {e}"));
            }
        }

        if !self.has(hash).await? {
            return Err(
                last_err.unwrap_or_else(|| anyhow!("no holder in the set could serve {hash}"))
            );
        }
        Ok(())
    }
}

/// Blake3 of a file's contents streamed in bounded buffers (1 MiB) — the RAM-flat
/// integrity check shared by the swarm export path and callers that must never
/// materialize a whole file (dailies-grade media).
pub async fn hash_file_streaming(path: &Path) -> Result<Hash> {
    use tokio::io::AsyncReadExt;
    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; 1024 * 1024];
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(Hash::from(hasher.finalize()))
}
