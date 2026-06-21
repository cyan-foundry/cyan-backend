//! Portable, signed `.cyangroup` export bundle for cold-start (MESH_HARDENING §11).
//!
//! # What this is (and is NOT)
//!
//! A `.cyangroup` bundle is the existing [`GroupSnapshot`](crate::snapshot) — group,
//! workspaces, boards, file **metadata** (by hash, never bytes), recent chats, board
//! content — packaged so it can be handed to an invitee **out-of-band** (AirDrop, a USB
//! key, a QR-chained transfer) and imported with **no network at all**. On the invitee's
//! first online contact, §5 incremental catch-up reconciles the gap from the watermark the
//! bundle stamped. This is *export-to-signed-bundle + import + verify* — it REUSES the
//! snapshot serialization, it is NOT a new sync engine.
//!
//! # The three security properties (each is a test)
//!
//! 1. **Signed.** The whole bundle is Ed25519-signed by the exporter's XaeroID
//!    ([`GroupBundle::signature`]). Import verifies it against [`GroupBundle::issued_by`];
//!    any tampering (ciphertext, scope, watermark) breaks the signature → rejected.
//! 2. **Strictly grant-scoped — never over-share.** The bundle embeds the invitee's signed
//!    capability [`Grant`]. Import enforces `grant.group_id == bundle.group_id`, the grant's
//!    own signature, and that the decrypted snapshot contains ONLY that one group. A bundle
//!    whose grant is for a different group is refused — no sibling-group data ever leaks.
//! 3. **Encrypted to the invitee.** The snapshot payload is sealed with an X25519 sealed box
//!    to the invitee's public key (`crypto_box`, libsodium-compatible). Only the holder of
//!    the matching secret can open it; everyone else — including anyone who intercepts the
//!    file — sees ciphertext. The invitee's X25519 keypair is derived deterministically from
//!    its Ed25519 identity ([`x25519_from_ed25519`]) so no extra key management is needed.
//!
//! Media is **never** in the bundle: files travel as `(name, hash, size)` metadata only and
//! are fetched later over the content-addressed swarm. Secret key material is held in
//! [`secrecy::SecretString`]/zeroizing types and never logged or persisted in clear.

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

use crate::identity::Grant;
use crate::models::protocol::SnapshotFrame;
use crate::snapshot;

/// Bundle wire/format version. Bumped only on a breaking layout change.
const BUNDLE_VERSION: u8 = 1;

/// Domain-separation context for deriving an X25519 key from an Ed25519 secret. Changing
/// this string changes every derived key (intentional: a different purpose ⇒ a different key).
const X25519_DERIVE_CONTEXT: &str = "cyan-group-bundle x25519 v1";

/// Derive a deterministic X25519 keypair from a 32-byte Ed25519 identity secret.
///
/// We do NOT reuse the Ed25519 scalar directly as an X25519 scalar (mixing a signing key into
/// a DH context is a footgun); instead we KDF the secret through blake3 with a fixed context.
/// Deterministic, so the invitee reconstructs the same secret key from its own identity, and
/// the exporter can be handed the matching public key (hex) ahead of time.
///
/// Returns `(x25519_secret, x25519_public)`. The secret is zeroized on drop by `crypto_box`.
pub fn x25519_from_ed25519(ed25519_secret: &[u8; 32]) -> (crypto_box::SecretKey, crypto_box::PublicKey) {
    let derived = blake3::derive_key(X25519_DERIVE_CONTEXT, ed25519_secret);
    let sk = crypto_box::SecretKey::from_bytes(derived);
    let pk = sk.public_key();
    (sk, pk)
}

/// The invitee's X25519 public key, hex-encoded — what [`export_group`] is handed as the
/// recipient. Derived from the invitee's Ed25519 identity via [`x25519_from_ed25519`].
pub fn invitee_pubkey_hex(invitee_ed25519_secret: &[u8; 32]) -> String {
    let (_, pk) = x25519_from_ed25519(invitee_ed25519_secret);
    hex::encode(pk.to_bytes())
}

/// Why an export failed.
#[derive(Debug)]
pub enum ExportError {
    /// The embedded grant's group does not match the group being exported (would over-share).
    ScopeMismatch,
    /// The recipient public key is not valid 32-byte hex.
    BadRecipientKey,
    /// The group has no state to export (unknown group id).
    EmptyGroup,
    /// Sealing/serialization failed.
    Crypto(String),
}

impl std::fmt::Display for ExportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExportError::ScopeMismatch => write!(f, "grant scope does not match exported group"),
            ExportError::BadRecipientKey => write!(f, "recipient pubkey is not 32-byte hex"),
            ExportError::EmptyGroup => write!(f, "group has no state to export"),
            ExportError::Crypto(e) => write!(f, "bundle crypto failed: {e}"),
        }
    }
}
impl std::error::Error for ExportError {}

/// Why an import was rejected. Each variant is a refusal the security tests assert.
#[derive(Debug, PartialEq, Eq)]
pub enum ImportError {
    /// The bundle's outer Ed25519 signature does not verify (forged or tampered).
    BadSignature,
    /// The embedded grant's own signature does not verify.
    BadGrantSignature,
    /// `grant.group_id != bundle.group_id` — the bundle is out of the invitee's scope.
    OutOfScope,
    /// The decrypted snapshot carries a group other than the scoped one (over-share attempt).
    ScopeLeak,
    /// The sealed payload could not be opened with the invitee's key.
    Undecryptable,
    /// The bundle is structurally malformed (bad version / bad hex / bad payload).
    Malformed,
}

impl std::fmt::Display for ImportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            ImportError::BadSignature => "bundle signature invalid",
            ImportError::BadGrantSignature => "embedded grant signature invalid",
            ImportError::OutOfScope => "bundle out of the invitee's grant scope",
            ImportError::ScopeLeak => "bundle carries out-of-scope group data",
            ImportError::Undecryptable => "bundle could not be decrypted with the invitee's key",
            ImportError::Malformed => "bundle is malformed",
        };
        write!(f, "{s}")
    }
}
impl std::error::Error for ImportError {}

/// A portable, signed, invitee-encrypted group bundle. Serializes to JSON (the `.cyangroup`
/// file body). The `sealed` payload is opaque ciphertext; everything else is public metadata
/// the signature covers so it cannot be tampered without detection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupBundle {
    pub version: u8,
    /// The single group this bundle is scoped to.
    pub group_id: String,
    /// The invitee's signed capability grant — the SCOPE. Import enforces it matches `group_id`.
    pub grant: Grant,
    /// Exporter's Ed25519 public key, hex (verifies `signature`).
    pub issued_by: String,
    /// Invitee's X25519 public key, hex — the bundle is sealed TO this key.
    pub invitee: String,
    /// "Synced as of T" (unix seconds): the exporter's high-water mark at export time. Import
    /// stamps this so §5 catch-up reconciles only the gap on first online contact.
    pub synced_as_of: i64,
    /// X25519 sealed box over the serialized `Vec<SnapshotFrame>` (ciphertext, hex). Never
    /// contains media bytes — files are metadata-only inside the snapshot.
    pub sealed: String,
    /// Ed25519 signature (hex) over [`GroupBundle::signing_payload`].
    pub signature: String,
}

impl GroupBundle {
    /// Deterministic bytes the signature covers — every field a verifier trusts EXCEPT the
    /// signature itself. Tampering any of them (scope, recipient, watermark, ciphertext)
    /// invalidates the signature.
    fn signing_payload(&self) -> Vec<u8> {
        let mut p = Vec::new();
        p.extend_from_slice(b"cyan_group_bundle:v");
        p.extend_from_slice(self.version.to_string().as_bytes());
        p.push(0);
        p.extend_from_slice(self.group_id.as_bytes());
        p.push(0);
        p.extend_from_slice(self.grant.to_qr_payload().as_bytes());
        p.push(0);
        p.extend_from_slice(self.issued_by.as_bytes());
        p.push(0);
        p.extend_from_slice(self.invitee.as_bytes());
        p.push(0);
        p.extend_from_slice(self.synced_as_of.to_string().as_bytes());
        p.push(0);
        p.extend_from_slice(self.sealed.as_bytes());
        p
    }

    /// Sign (or re-sign) the bundle in place with `signer_secret`, setting `signature` over
    /// [`GroupBundle::signing_payload`]. [`export_group`] calls this after assembly; it is also
    /// the seam negative tests use to forge a validly-signed-but-out-of-scope bundle.
    pub fn sign(&mut self, signer_secret: &[u8; 32]) {
        let sig = xaeroid::XaeroID::ed25519_sign(&self.signing_payload(), signer_secret);
        self.signature = hex::encode(sig);
    }

    /// Serialize to the `.cyangroup` file body (compact JSON).
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    /// Parse a `.cyangroup` file body. Does NOT verify — call [`import_group`] next.
    pub fn from_json(s: &str) -> Result<GroupBundle, ImportError> {
        serde_json::from_str(s).map_err(|_| ImportError::Malformed)
    }
}

/// Decode a 32-byte X25519 public key from hex.
fn decode_x25519_pub(hex_str: &str) -> Result<crypto_box::PublicKey, ()> {
    let bytes = hex::decode(hex_str).map_err(|_| ())?;
    let arr: [u8; 32] = bytes.try_into().map_err(|_| ())?;
    Ok(crypto_box::PublicKey::from_bytes(arr))
}

/// Export `group_id` to a signed, invitee-sealed bundle (MESH_HARDENING §11).
///
/// * `grant` — the invitee's signed capability; its `group_id` MUST equal `group_id` (else
///   [`ExportError::ScopeMismatch`]). This is the scope: nothing beyond it is ever packaged.
/// * `invitee_x_pub_hex` — the invitee's X25519 public key (see [`invitee_pubkey_hex`]); the
///   payload is sealed to it.
/// * `signer_secret` — the exporter's 32-byte Ed25519 identity secret. Used only to sign; it
///   is never copied into the bundle and never logged.
/// * `now` — export time (unix seconds), stamped as the `synced_as_of` watermark.
///
/// The snapshot is built scoped to exactly `group_id` (files metadata-only, no bytes), sealed,
/// then the whole bundle is signed.
pub fn export_group(
    group_id: &str,
    grant: &Grant,
    invitee_x_pub_hex: &str,
    signer_secret: &[u8; 32],
    now: i64,
) -> Result<GroupBundle, ExportError> {
    // Scope guard #1: refuse to export under a grant for a different group.
    if grant.group_id != group_id {
        return Err(ExportError::ScopeMismatch);
    }

    let recipient = decode_x25519_pub(invitee_x_pub_hex).map_err(|_| ExportError::BadRecipientKey)?;

    // Reuse the snapshot serialization — strictly this one group, full state (since=None).
    let frames = snapshot::build_snapshot_frames(group_id, None)
        .map_err(|e| ExportError::Crypto(e.to_string()))?;
    if frames.is_empty() {
        return Err(ExportError::EmptyGroup);
    }
    let plaintext = serde_json::to_vec(&frames).map_err(|e| ExportError::Crypto(e.to_string()))?;

    // Seal to the invitee (anonymous X25519 sealed box). An ephemeral keypair is generated
    // per export by `crypto_box`, so the ciphertext reveals nothing about the exporter.
    use rand_chacha::rand_core::SeedableRng;
    let mut rng = rand_chacha::ChaCha20Rng::from_os_rng();
    let sealed = recipient
        .seal(&mut rng, &plaintext)
        .map_err(|e| ExportError::Crypto(format!("seal: {e}")))?;

    let issued_by = crate::identity::pubkey_hex(signer_secret);
    let mut bundle = GroupBundle {
        version: BUNDLE_VERSION,
        group_id: group_id.to_string(),
        grant: grant.clone(),
        issued_by,
        invitee: invitee_x_pub_hex.to_string(),
        synced_as_of: now,
        sealed: hex::encode(sealed),
        signature: String::new(),
    };
    bundle.sign(signer_secret);
    Ok(bundle)
}

/// The verified, decrypted result of an import: the scoped group id plus the frames (so a
/// caller can apply them and/or inspect them). [`import_group`] applies them for you.
#[derive(Debug)]
pub struct ImportedBundle {
    pub group_id: String,
    pub synced_as_of: i64,
    pub frames: Vec<SnapshotFrame>,
}

/// Verify + decrypt a bundle WITHOUT touching storage (the pure check, for tests and for the
/// air-gapped path). Enforces, in order: outer signature · grant signature · grant scope ·
/// decryptability · no out-of-scope group leak. Returns the frames on success.
pub fn verify_and_open(
    bundle: &GroupBundle,
    invitee_ed25519_secret: &[u8; 32],
) -> Result<ImportedBundle, ImportError> {
    if bundle.version != BUNDLE_VERSION {
        return Err(ImportError::Malformed);
    }

    // 1. Outer signature over the exact bytes a verifier trusts.
    let pk = decode_ed25519_pub(&bundle.issued_by).map_err(|_| ImportError::BadSignature)?;
    let sig = decode_ed25519_sig(&bundle.signature).map_err(|_| ImportError::BadSignature)?;
    if !xaeroid::XaeroID::verify(&bundle.signing_payload(), &sig, &pk) {
        return Err(ImportError::BadSignature);
    }

    // 2. The embedded grant must itself be validly signed …
    if !bundle.grant.verify_signature() {
        return Err(ImportError::BadGrantSignature);
    }
    // 3. … and scoped to exactly this group (never another group's data).
    if bundle.grant.group_id != bundle.group_id {
        return Err(ImportError::OutOfScope);
    }

    // 4. Decrypt with the invitee's X25519 secret (derived from its identity).
    let (x_secret, _) = x25519_from_ed25519(invitee_ed25519_secret);
    let sealed = hex::decode(&bundle.sealed).map_err(|_| ImportError::Malformed)?;
    let plaintext = x_secret
        .unseal(&sealed)
        .map_err(|_| ImportError::Undecryptable)?;
    let frames: Vec<SnapshotFrame> =
        serde_json::from_slice(&plaintext).map_err(|_| ImportError::Malformed)?;

    // 5. Scope guard: every Structure frame must carry ONLY the scoped group — defense in
    //    depth against a maliciously-built bundle that signed group A but packed group B.
    for f in &frames {
        if let SnapshotFrame::Structure { group, .. } = f
            && group.id != bundle.group_id
        {
            return Err(ImportError::ScopeLeak);
        }
    }

    Ok(ImportedBundle {
        group_id: bundle.group_id.clone(),
        synced_as_of: bundle.synced_as_of,
        frames,
    })
}

/// Import a bundle: verify + decrypt (via [`verify_and_open`]), apply the snapshot to storage
/// (idempotent upsert — works fully offline), and stamp the "synced as of T" watermark so §5
/// catch-up reconciles the gap on first online contact. Returns the imported group id.
///
/// This is the air-gapped cold-start path: no network is touched.
pub fn import_group(bundle: &GroupBundle, invitee_ed25519_secret: &[u8; 32]) -> Result<String> {
    let opened = verify_and_open(bundle, invitee_ed25519_secret)
        .map_err(|e| anyhow!("import rejected: {e}"))?;
    snapshot::apply_snapshot_frames(&opened.frames)?;
    crate::storage::group_sync_state_set(&opened.group_id, opened.synced_as_of)?;
    Ok(opened.group_id)
}

fn decode_ed25519_pub(hex_str: &str) -> Result<[u8; 32], ()> {
    let bytes = hex::decode(hex_str).map_err(|_| ())?;
    bytes.try_into().map_err(|_| ())
}

fn decode_ed25519_sig(hex_str: &str) -> Result<[u8; 64], ()> {
    let bytes = hex::decode(hex_str).map_err(|_| ())?;
    bytes.try_into().map_err(|_| ())
}
