//! W17 §B — device identity vault: secure custody, one-time file→vault migration,
//! and "delete identity" wipe (IDENTITY_W16_W17_SPEC).
//!
//! Exercised against the infra-free [`MemVault`] fake so `cargo test` never
//! touches the real Keychain. Key material moves only as `SecretString`.

use std::fs;

use cyan_backend::device_vault::{
    delete_identity, load_device_key, migrate_file_key_into_vault, store_device_key, MemVault,
    DEVICE_KEY_ID,
};
use secrecy::{ExposeSecret, SecretString};

#[test]
fn file_key_migrates_into_vault_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    let key_file = dir.path().join("device_key.hex");
    let original = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    fs::write(&key_file, format!("{original}\n")).expect("write file key");

    let vault = MemVault::new();

    // First run: the plaintext key migrates into the vault and the file is wiped.
    let migrated = migrate_file_key_into_vault(&key_file, &vault, DEVICE_KEY_ID)
        .expect("migration succeeds");
    assert!(migrated, "the file-stored key is migrated on the first run");
    assert!(!key_file.exists(), "the plaintext key file is removed after migration");
    let loaded = load_device_key(&vault).expect("load").expect("key present in vault");
    assert_eq!(loaded.expose_secret(), original, "vaulted key matches (trimmed) file");

    // Second run: nothing to do — the vault is authoritative, so it is a no-op.
    let again = migrate_file_key_into_vault(&key_file, &vault, DEVICE_KEY_ID)
        .expect("second migration is a clean no-op");
    assert!(!again, "migration runs at most once");

    // Even if a STALE file reappears, the vaulted key is never clobbered.
    fs::write(&key_file, "deadbeef".repeat(8)).expect("rewrite stale file");
    let third = migrate_file_key_into_vault(&key_file, &vault, DEVICE_KEY_ID)
        .expect("stale file does not re-trigger migration");
    assert!(!third, "a vaulted key is never overwritten by a later file");
    assert_eq!(
        load_device_key(&vault).expect("load").expect("key").expose_secret(),
        original,
        "the originally-migrated key is still the one in the vault"
    );
    assert!(key_file.exists(), "the stale file is left untouched (not consumed)");
}

#[test]
fn vault_missing_key_clean_absent_not_panic() {
    let vault = MemVault::new();
    // No key stored yet: a load is a clean Ok(None), never an error or panic
    // ("delete identity then relaunch" must not crash the engine).
    let loaded = load_device_key(&vault).expect("absent key loads cleanly, not an error");
    assert!(loaded.is_none(), "an absent device key is None, not a panic");

    // A delete of an absent key is also idempotent / panic-free.
    delete_identity(&vault).expect("deleting an absent key is a clean no-op");
}

#[test]
fn delete_identity_wipes_key() {
    let vault = MemVault::new();
    store_device_key(&vault, &SecretString::new("super-secret-device-key".to_string()))
        .expect("store device key");
    assert!(
        load_device_key(&vault).expect("load").is_some(),
        "key is present before delete"
    );

    delete_identity(&vault).expect("delete identity wipes the key");
    assert!(
        load_device_key(&vault).expect("load").is_none(),
        "the device key is gone after delete_identity"
    );

    // Idempotent: a second delete is still clean.
    delete_identity(&vault).expect("second delete is a no-op");
}
