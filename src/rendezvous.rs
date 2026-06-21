//! §5 Discoverable rendezvous config — kill the hardcoded bootstrap id.
//!
//! # Why this module exists
//!
//! The engine used to hardcode `DEFAULT_BOOTSTRAP_NODE_ID` and source the relay URL ad-hoc, so
//! every bootstrap/relay redeploy needed an app retune (SUPER_PEER_COMPLETION_SPEC §5). Instead the
//! engine **discovers** a small, **org-signed** per-environment rendezvous config at startup —
//! `{ env, discovery_key, bootstrap: {node_id, addr?}, relay_url? }` — self-published by the
//! bootstrap node to a well-known URL. The app pins the org public key, so the config is
//! tamper-evident: a redeploy republishes, apps pick it up next launch, **zero app rebuild**.
//!
//! ## What replaces the hardcode
//! - [`bundled_fallback`] — the cold-start / offline / no-URL default. It carries the SAME bootstrap
//!   id the engine shipped before, so behavior is **identical when no rendezvous URL is configured**
//!   (the seam: the fetch only runs, and can only change anything, when a URL is explicitly set).
//! - [`resolve`] — the pure decision: a *verified* signed doc wins; anything else (no doc, bad
//!   signature, malformed) falls back to the bundled config. This is what the tests drive — no
//!   network in the unit path.
//! - [`apply`] — sets the engine globals (`BOOTSTRAP_NODE_ID` / `DISCOVERY_KEY` / `RELAY_URL`) from
//!   a resolved config. `OnceCell::set` is first-wins, so an explicit FFI-provided value still wins
//!   over the config (keeps the FFI init path drop-in).
//! - [`fetch_and_apply_if_configured`] — the thin, best-effort network glue wired into init: it does
//!   nothing unless `CYAN_RENDEZVOUS_URL` is set; on any error it stays on the bundled fallback. The
//!   mDNS / LAN-sovereign path needs none of this.
//!
//! Signing reuses the same Ed25519 primitives as capability grants (`XaeroID::ed25519_{sign,pubkey}`
//! / `verify`) so there is one signature scheme in the codebase, not two.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use xaeroid::XaeroID;

/// The bundled cold-start bootstrap node id — the value the engine shipped as `DEFAULT_BOOTSTRAP_NODE_ID`
/// before §5. It is no longer a load-bearing hardcode: it is ONLY the offline/first-run fallback, and
/// a verified signed config overrides it at startup.
pub const BUNDLED_BOOTSTRAP_NODE_ID: &str =
    "f992aa3b5409410b373605002a47e5521f1f2a9d10d2910544c3b37f4d6ed618";

/// The bundled default discovery key (the dev mesh) when no signed config provides one.
pub const BUNDLED_DISCOVERY_KEY: &str = "cyan-dev";

/// Env var naming the signed rendezvous config URL. Absent ⇒ the fetch is skipped entirely and the
/// engine uses the bundled fallback (identical to pre-§5 behavior).
pub const RENDEZVOUS_URL_ENV: &str = "CYAN_RENDEZVOUS_URL";

/// Env var pinning the org Ed25519 public key (hex) that signs the rendezvous config. Absent ⇒ no
/// key is pinned, so NO fetched doc can be trusted and the engine stays on the bundled fallback —
/// the secure default (a fetched config is honored only when its org signature verifies).
pub const ORG_PUBKEY_ENV: &str = "CYAN_ORG_PUBKEY";

/// The bootstrap rendezvous node — the source of truth for its OWN id (no one hardcodes it).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bootstrap {
    pub node_id: String,
    /// Optional serialized `iroh::EndpointAddr` of the bootstrap (additive; discovery resolves it
    /// from the id when absent).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub addr: Option<String>,
}

/// The per-environment rendezvous config the bootstrap self-publishes and the app discovers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RendezvousConfig {
    /// Environment name (`dev`/`qa`/`prod`) — diagnostic / guards against cross-env misfetch.
    pub env: String,
    pub discovery_key: String,
    pub bootstrap: Bootstrap,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relay_url: Option<String>,
}

/// An org-signed rendezvous config: the EXACT JSON bytes that were signed, plus the hex Ed25519
/// signature over those bytes. Keeping `config` as the literal signed string (not re-serialized)
/// means re-serialization differences can never break the signature.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedRendezvous {
    /// The `RendezvousConfig` serialized to JSON — the bytes the signature covers.
    pub config: String,
    /// Hex Ed25519 signature over `config`'s bytes, produced by the org key.
    pub signature: String,
}

/// Why a fetched/decoded rendezvous doc was rejected (⇒ the engine falls back to bundled).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RendezvousError {
    /// The signed envelope or inner config JSON did not parse.
    Malformed,
    /// No org public key is pinned, so no signature can be trusted.
    NoPinnedKey,
    /// The org signature did not verify against the pinned key (tamper / wrong key).
    BadSignature,
}

/// Where the resolved config came from — surfaced for logging/STATUS and asserted by tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// A fetched doc whose org signature verified.
    SignedDoc,
    /// The bundled cold-start/offline fallback (no doc, or it failed verification).
    BundledFallback,
}

impl SignedRendezvous {
    /// Sign a config with the org's 32-byte Ed25519 secret. Used by the bootstrap self-publisher
    /// (and the tests). The config is serialized once and that exact string is what gets signed.
    pub fn sign(config: &RendezvousConfig, org_secret: &[u8; 32]) -> Result<Self, RendezvousError> {
        let json = serde_json::to_string(config).map_err(|_| RendezvousError::Malformed)?;
        let sig = XaeroID::ed25519_sign(json.as_bytes(), org_secret);
        Ok(SignedRendezvous {
            config: json,
            signature: hex::encode(sig),
        })
    }

    /// Decode a signed envelope from its JSON wire form.
    pub fn from_json(s: &str) -> Result<Self, RendezvousError> {
        serde_json::from_str(s).map_err(|_| RendezvousError::Malformed)
    }

    /// Encode the signed envelope to its JSON wire form.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    /// Verify the org signature against `org_pubkey_hex` and, on success, return the inner config.
    /// Any failure (bad key, bad signature, malformed inner JSON) is an error — the caller falls
    /// back to the bundled config rather than trusting an unverified doc.
    pub fn verify(&self, org_pubkey_hex: &str) -> Result<RendezvousConfig, RendezvousError> {
        let pk: [u8; 32] = hex::decode(org_pubkey_hex)
            .ok()
            .and_then(|b| b.try_into().ok())
            .ok_or(RendezvousError::NoPinnedKey)?;
        let sig: [u8; 64] = hex::decode(&self.signature)
            .ok()
            .and_then(|b| b.try_into().ok())
            .ok_or(RendezvousError::BadSignature)?;
        if !XaeroID::verify(self.config.as_bytes(), &sig, &pk) {
            return Err(RendezvousError::BadSignature);
        }
        serde_json::from_str(&self.config).map_err(|_| RendezvousError::Malformed)
    }
}

/// The cold-start / offline / no-URL fallback — the SAME bootstrap id the engine shipped before §5,
/// so behavior is identical when no rendezvous config is configured.
pub fn bundled_fallback() -> RendezvousConfig {
    RendezvousConfig {
        env: "bundled".to_string(),
        discovery_key: BUNDLED_DISCOVERY_KEY.to_string(),
        bootstrap: Bootstrap {
            node_id: BUNDLED_BOOTSTRAP_NODE_ID.to_string(),
            addr: None,
        },
        relay_url: None,
    }
}

/// The pure resolution: a fetched signed doc that VERIFIES against the pinned org key wins; anything
/// else (no doc, no pinned key, bad signature, malformed) falls back to the bundled config. This is
/// the whole §5 decision, free of any network — the unit tests drive it directly.
///
/// * `doc` — the fetched signed-envelope JSON bytes (`None` ⇒ offline / no URL configured).
/// * `org_pubkey_hex` — the pinned org key (`None` ⇒ nothing can be trusted ⇒ bundled).
pub fn resolve(doc: Option<&[u8]>, org_pubkey_hex: Option<&str>) -> (RendezvousConfig, Source) {
    let parsed = doc
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
        .and_then(|s| SignedRendezvous::from_json(s).ok());
    if let (Some(signed), Some(pk)) = (parsed, org_pubkey_hex)
        && let Ok(cfg) = signed.verify(pk)
    {
        return (cfg, Source::SignedDoc);
    }
    (bundled_fallback(), Source::BundledFallback)
}

/// Apply a resolved config to the engine globals. `OnceCell::set` is first-wins, so an explicit
/// FFI-provided `DISCOVERY_KEY` / `RELAY_URL` (set before init) still wins — keeping the FFI init
/// path drop-in. `BOOTSTRAP_NODE_ID` is not FFI-set today, so the config (or bundled fallback)
/// drives it, replacing the old hardcode. Returns the values now in effect (post-set) for logging.
pub fn apply(cfg: &RendezvousConfig) {
    let _ = crate::BOOTSTRAP_NODE_ID.set(cfg.bootstrap.node_id.clone());
    let _ = crate::DISCOVERY_KEY.set(cfg.discovery_key.clone());
    if let Some(relay) = &cfg.relay_url {
        let _ = crate::RELAY_URL.set(relay.clone());
    }
    // `cfg.bootstrap.addr` (the bootstrap's full serialized EndpointAddr) is carried in the signed
    // config shape so discovery can dial directly; it rides the existing addr-seed path at join
    // time (no new global is introduced here — keep this a config resolver, not a wiring change).
}

/// The pinned org pubkey hex, from `CYAN_ORG_PUBKEY` — `None` if unset (⇒ no doc can be trusted).
pub fn pinned_org_pubkey() -> Option<String> {
    std::env::var(ORG_PUBKEY_ENV).ok().filter(|s| !s.is_empty())
}

/// Best-effort startup glue: if `CYAN_RENDEZVOUS_URL` is set, fetch the signed config (bounded),
/// verify it against the pinned org key, and apply it; on ANY error stay on the bundled fallback.
/// Does nothing (and never blocks on the network) when no URL is configured — so the offline /
/// mDNS / no-config path, and any existing FFI init that doesn't set the URL, behave identically.
/// Returns the source actually applied, for logging/STATUS.
pub fn fetch_and_apply_if_configured() -> Source {
    let Some(url) = std::env::var(RENDEZVOUS_URL_ENV).ok().filter(|s| !s.is_empty()) else {
        // No URL configured: do not touch the network; the bundled fallback is used implicitly via
        // `crate::bootstrap_node_id()`. We still `apply` the bundled config so DISCOVERY_KEY has a
        // value when nothing else set it — but `set` is first-wins, so this never overrides FFI.
        apply(&bundled_fallback());
        return Source::BundledFallback;
    };

    let doc = fetch_signed_doc(&url);
    let (cfg, source) = resolve(doc.as_deref(), pinned_org_pubkey().as_deref());
    apply(&cfg);
    source
}

/// Bounded best-effort HTTP GET of the signed rendezvous doc. Errors (offline, timeout, non-200)
/// return `None` so the caller falls back to bundled — the offline-first contract.
fn fetch_signed_doc(url: &str) -> Option<Vec<u8>> {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .ok()?;
    let resp = client.get(url).send().ok()?;
    if !resp.status().is_success() {
        return None;
    }
    resp.bytes().ok().map(|b| b.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_config() -> RendezvousConfig {
        RendezvousConfig {
            env: "dev".to_string(),
            discovery_key: "cyan-dev".to_string(),
            bootstrap: Bootstrap {
                node_id: "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
                    .to_string(),
                addr: None,
            },
            relay_url: Some("relay.dev.cyan.blockxaero.io".to_string()),
        }
    }

    /// A validly-signed doc resolves to its config (bootstrap + relay + discovery_key all from the
    /// doc), tagged as coming from the signed doc — not the bundled fallback.
    #[test]
    fn config_sets_bootstrap_relay_from_signed_doc() {
        let secret = [7u8; 32];
        let org_pub = hex::encode(XaeroID::ed25519_pubkey(&secret));
        let cfg = sample_config();
        let signed = SignedRendezvous::sign(&cfg, &secret).expect("sign");
        let wire = signed.to_json();

        let (resolved, source) = resolve(Some(wire.as_bytes()), Some(&org_pub));
        assert_eq!(source, Source::SignedDoc, "a verified doc must be used, not the fallback");
        assert_eq!(resolved.bootstrap.node_id, cfg.bootstrap.node_id, "bootstrap id from the doc");
        assert_eq!(resolved.relay_url, cfg.relay_url, "relay url from the doc");
        assert_eq!(resolved.discovery_key, "cyan-dev", "discovery key from the doc");
        assert_ne!(
            resolved.bootstrap.node_id, BUNDLED_BOOTSTRAP_NODE_ID,
            "the signed doc overrode the bundled bootstrap id (no hardcode reliance)"
        );
    }

    /// A doc whose signature is tampered (or signed by the wrong key) is rejected and the engine
    /// falls back to the bundled config — never trusting an unverified doc.
    #[test]
    fn bad_signature_rejected_falls_back() {
        let secret = [7u8; 32];
        let org_pub = hex::encode(XaeroID::ed25519_pubkey(&secret));
        let cfg = sample_config();
        let mut signed = SignedRendezvous::sign(&cfg, &secret).expect("sign");

        // Tamper the CONFIG after signing — the signature no longer covers these bytes.
        signed.config = signed.config.replace("cyan-dev", "evil-key");
        let wire = signed.to_json();

        let (resolved, source) = resolve(Some(wire.as_bytes()), Some(&org_pub));
        assert_eq!(source, Source::BundledFallback, "a tampered doc must be rejected");
        assert_eq!(
            resolved.bootstrap.node_id, BUNDLED_BOOTSTRAP_NODE_ID,
            "rejection falls back to the bundled bootstrap id"
        );
        assert_ne!(resolved.discovery_key, "evil-key", "the tampered value never takes effect");

        // Also: a valid doc verified against the WRONG pinned key is rejected.
        let wrong_pub = hex::encode(XaeroID::ed25519_pubkey(&[9u8; 32]));
        let good = SignedRendezvous::sign(&cfg, &secret).expect("sign");
        let (_, src2) = resolve(Some(good.to_json().as_bytes()), Some(&wrong_pub));
        assert_eq!(src2, Source::BundledFallback, "a doc signed by a non-pinned key is rejected");
    }

    /// Offline / no doc (and no pinned key) both resolve to the bundled fallback — the cold-start
    /// path that lets the engine come up with a known bootstrap when the config can't be fetched.
    #[test]
    fn offline_uses_bundled_fallback() {
        // No doc at all (offline / no URL).
        let (resolved, source) = resolve(None, Some("deadbeef"));
        assert_eq!(source, Source::BundledFallback);
        assert_eq!(resolved.bootstrap.node_id, BUNDLED_BOOTSTRAP_NODE_ID);
        assert_eq!(resolved.discovery_key, BUNDLED_DISCOVERY_KEY);

        // A doc present but no pinned key ⇒ still bundled (nothing can be trusted).
        let signed = SignedRendezvous::sign(&sample_config(), &[7u8; 32]).expect("sign");
        let (_, src) = resolve(Some(signed.to_json().as_bytes()), None);
        assert_eq!(src, Source::BundledFallback, "no pinned key ⇒ no trust ⇒ bundled");
    }
}
