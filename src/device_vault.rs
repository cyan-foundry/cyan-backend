//! Device identity vault — secure custody for the device XaeroID private key
//! (IDENTITY_W16_W17_SPEC §B, W17).
//!
//! The device's XaeroID private key must live in the **OS secure store**, not a
//! plaintext file on disk. cyan-identity owns the seam ([`Vault`]) + the infra-
//! free fake ([`MemVault`]); this module adds the backend's real macOS impl
//! ([`KeychainVault`], Security-framework Keychain) and the one-time
//! **file → vault migration**, plus the plumbing behind the additive
//! `cyan_delete_identity()` FFI.
//!
//! Everything is `dyn`-dispatched through `Arc<dyn Vault>` so headless/test builds
//! transparently fall back to [`MemVault`] (or `CYAN_VAULT=mem`), keeping
//! `cargo test` free of Keychain prompts. Key material only ever moves as
//! [`SecretString`] — never logged, never `Debug`-printed.

use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use secrecy::{ExposeSecret, SecretString};

// Re-export the cyan-identity seam + fake so consumers (and the FFI layer) have a
// single import site for the vault vocabulary.
pub use cyan_identity::{MemVault, Vault};

/// The vault key under which the device XaeroID private key is stored. Stable so a
/// migrated key is found on every later launch (and wiped by the same id).
pub const DEVICE_KEY_ID: &str = "cyan.device.xaero_id";

/// Keychain *service* name for the device key (the Keychain "where it came from").
#[cfg(target_os = "macos")]
const KEYCHAIN_SERVICE: &str = "io.blockxaero.cyan.identity";

/// Build the process-wide device-key vault. macOS uses the real Keychain; every
/// other target — and any build with `CYAN_VAULT=mem` (headless/CI) — falls back
/// to the in-memory fake so nothing prompts for Keychain access.
pub fn default_device_vault() -> Arc<dyn Vault> {
    if std::env::var("CYAN_VAULT")
        .map(|v| v.eq_ignore_ascii_case("mem"))
        .unwrap_or(false)
    {
        return Arc::new(MemVault::new());
    }
    #[cfg(target_os = "macos")]
    {
        Arc::new(KeychainVault::for_device())
    }
    #[cfg(not(target_os = "macos"))]
    {
        Arc::new(MemVault::new())
    }
}

/// Keychain *service* name for PLUGIN credentials (PLUGIN_CREDENTIAL_ONBOARDING
/// §C) — separate from the identity service so wiping one never touches the other.
#[cfg(target_os = "macos")]
const PLUGIN_KEYCHAIN_SERVICE: &str = "io.blockxaero.cyan.plugins";

/// The device-local PLUGIN credential vault (the DeviceVault of the onboarding
/// design). Same fallback rules as [`default_device_vault`]: `CYAN_VAULT=mem`
/// (or a non-macOS target) uses the in-memory fake so tests never prompt.
pub fn plugin_cred_vault() -> Arc<dyn Vault> {
    if std::env::var("CYAN_VAULT")
        .map(|v| v.eq_ignore_ascii_case("mem"))
        .unwrap_or(false)
    {
        return Arc::new(MemVault::new());
    }
    #[cfg(target_os = "macos")]
    {
        Arc::new(KeychainVault::new(PLUGIN_KEYCHAIN_SERVICE))
    }
    #[cfg(not(target_os = "macos"))]
    {
        Arc::new(MemVault::new())
    }
}

/// The vault key one plugin credential lives under — scoped plugin + provider +
/// tenant, so one operator's many producers never share a credential.
pub fn plugin_cred_key(plugin: &str, provider: &str, tenant: &str) -> String {
    format!("cyan.plugin.{plugin}.{provider}.{tenant}")
}

/// Store the device XaeroID private key (overwriting any existing value).
pub fn store_device_key(vault: &dyn Vault, key: &SecretString) -> Result<()> {
    vault.store(DEVICE_KEY_ID, key)
}

/// Load the device XaeroID private key, or `Ok(None)` if none is stored. A clean
/// absence — "delete identity then relaunch" — is never an error and never panics.
pub fn load_device_key(vault: &dyn Vault) -> Result<Option<SecretString>> {
    vault.load(DEVICE_KEY_ID)
}

/// Wipe the device XaeroID private key from the vault — the engine half of the
/// iOS "delete identity" flow. Idempotent: deleting an absent key is `Ok(())`.
/// Local data/DB is untouched; only the key custody is cleared.
pub fn delete_identity(vault: &dyn Vault) -> Result<()> {
    vault.delete(DEVICE_KEY_ID)
}

/// Migrate a legacy **plaintext file-stored** device key into `vault`, exactly
/// **once**. Returns `Ok(true)` if a key was migrated on this call, `Ok(false)`
/// if there was nothing to do.
///
/// The vault is authoritative: if it already holds `key_id`, this is a no-op
/// (`false`) and the file is left to the caller — we NEVER overwrite a vaulted key
/// with a stale file, so re-running on every startup migrates at most once. When
/// the vault is empty and the file exists, its (trimmed) contents are stored in
/// the vault and the plaintext file is removed so the secret no longer lives on
/// disk.
pub fn migrate_file_key_into_vault(
    file_path: &Path,
    vault: &dyn Vault,
    key_id: &str,
) -> Result<bool> {
    // Already migrated — the vault wins, never clobber it from disk.
    if vault.load(key_id)?.is_some() {
        return Ok(false);
    }
    // Nothing on disk to migrate.
    if !file_path.exists() {
        return Ok(false);
    }
    let contents =
        std::fs::read_to_string(file_path).map_err(|e| anyhow!("read file-stored key: {e}"))?;
    let secret = SecretString::new(contents.trim().to_string());
    vault.store(key_id, &secret)?;
    // The secret now lives in the secure store; drop the plaintext copy.
    std::fs::remove_file(file_path).map_err(|e| anyhow!("remove migrated key file: {e}"))?;
    Ok(true)
}

// ===========================================================================
// KeychainVault — macOS Security-framework backing (real OS secure store)
// ===========================================================================

/// macOS Keychain-backed [`Vault`] for the device key, via the Security framework
/// generic-password API. One service + per-`key` account, so distinct keys never
/// collide. `store` overwrites; `load` of an absent key is a clean `Ok(None)`;
/// `delete` is idempotent — exactly the contract the fake mirrors.
///
/// Validated end-to-end with the app later (it needs the signed Keychain
/// entitlement); headless builds use [`MemVault`] instead. Pure plumbing — no key
/// material is logged.
#[cfg(target_os = "macos")]
pub struct KeychainVault {
    service: String,
}

#[cfg(target_os = "macos")]
impl KeychainVault {
    /// A vault scoped to `service` (the Keychain item's service attribute).
    pub fn new(service: impl Into<String>) -> Self {
        Self {
            service: service.into(),
        }
    }

    /// The device-identity vault on the standard Cyan service name.
    pub fn for_device() -> Self {
        Self::new(KEYCHAIN_SERVICE)
    }
}

#[cfg(target_os = "macos")]
impl Vault for KeychainVault {
    fn store(&self, key: &str, secret: &SecretString) -> Result<()> {
        security_framework::passwords::set_generic_password(
            &self.service,
            key,
            secret.expose_secret().as_bytes(),
        )
        .map_err(|e| anyhow!("keychain store failed: {e}"))
    }

    fn load(&self, key: &str) -> Result<Option<SecretString>> {
        // Security framework error code for "no such item" — a clean absence.
        const ERR_SEC_ITEM_NOT_FOUND: i32 = -25300;
        match security_framework::passwords::get_generic_password(&self.service, key) {
            Ok(bytes) => {
                let s = String::from_utf8(bytes)
                    .map_err(|_| anyhow!("keychain value is not valid UTF-8"))?;
                Ok(Some(SecretString::new(s)))
            }
            Err(e) if e.code() == ERR_SEC_ITEM_NOT_FOUND => Ok(None),
            Err(e) => Err(anyhow!("keychain load failed: {e}")),
        }
    }

    fn delete(&self, key: &str) -> Result<()> {
        // Security framework error code for "no such item" — idempotent delete.
        const ERR_SEC_ITEM_NOT_FOUND: i32 = -25300;
        match security_framework::passwords::delete_generic_password(&self.service, key) {
            Ok(()) => Ok(()),
            Err(e) if e.code() == ERR_SEC_ITEM_NOT_FOUND => Ok(()),
            Err(e) => Err(anyhow!("keychain delete failed: {e}")),
        }
    }
}
