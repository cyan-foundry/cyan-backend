use crate::ffi::scaffold::*;
use crate::util::MutexExt;
use crate::models::commands::*;
use crate::models::core::*;
use crate::models::dto::*;
use crate::models::events::*;
use crate::storage;

use crate::core::*;
use rusqlite::OptionalExtension;
// ---------- FFI: lifecycle ----------
#[unsafe(no_mangle)]
pub extern "C" fn cyan_set_data_dir(path: *const c_char) -> bool {
    eprintln!("🔥 cyan_set_data_dir ENTERED");
    let Some(s) = (unsafe { cstr_arg(path) }) else {
        eprintln!("❌ cyan_set_data_dir: path is null");
        return false;
    };
    eprintln!("📁 cyan_set_data_dir path: {}", s);
    let path_buf = PathBuf::from(s);

    // Set cyan_db_path on AIBridge if system exists
    if let Some(system) = SYSTEM.get() {
        let cyan_db_path = path_buf.join("cyan.db");
        let ai_bridge = system.ai_bridge.clone();
        if let Some(rt) = RUNTIME.get() {
            rt.spawn(async move {
                ai_bridge.set_cyan_db_path(cyan_db_path).await;
            });
        }
    }
    eprintln!("returning and setting {path_buf:?}");
    DATA_DIR.set(path_buf).is_ok()
}
#[unsafe(no_mangle)]
pub extern "C" fn cyan_set_discovery_key(key: *const c_char) -> bool {
    let Some(s) = (unsafe { cstr_arg(key) }) else {
        return false;
    };
    DISCOVERY_KEY.set(s).is_ok()
}

/// The git commit this engine was compiled from ("abc123def" / "abc123def-dirty" /
/// "unknown"), embedded by build.rs. The `cyan-build-commit:` prefix makes the stamp
/// greppable in the raw .a / app binary (`strings … | grep cyan-build-commit:`) — how
/// build_static_lib.sh proves the copied xcframework really carries HEAD's bits.
pub const BUILD_STAMP: &str = concat!("cyan-build-commit:", env!("CYAN_BUILD_COMMIT"));

/// FFI: the engine's build commit ("abc123def" or "abc123def-dirty"). Static storage —
/// the caller must NOT free the returned pointer.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_build_commit() -> *const c_char {
    static COMMIT: OnceLock<CString> = OnceLock::new();
    COMMIT
        .get_or_init(|| CString::new(env!("CYAN_BUILD_COMMIT")).unwrap_or_default())
        .as_ptr()
}

/// Initialize Cyan with ephemeral identity (for testing).
/// Different NodeID each launch - use for P2P mesh testing.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_init(db_path: *const c_char) -> bool {
    eprintln!("🏗 {BUILD_STAMP}");
    if SYSTEM.get().is_some() {
        return true;
    }
    let path = unsafe {
        if db_path.is_null() {
            eprintln!("Database path is null");
            return false;
        }
        CStr::from_ptr(db_path).to_string_lossy().to_string()
    };
    let res = std::thread::spawn(|| {
        // §5: resolve the bootstrap/relay/discovery_key from the signed rendezvous config BEFORE the
        // tokio runtime exists (so the best-effort blocking fetch runs outside any async context).
        // No-op unless CYAN_RENDEZVOUS_URL is set ⇒ identical to pre-§5 behavior offline.
        crate::rendezvous::fetch_and_apply_if_configured();

        let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().expect("runtime");
        RUNTIME.set(runtime).ok();

        let rt = RUNTIME.get().expect("Runtime cannot fail!");
        eprintln!("🔴 About to call CyanSystem::new()");
        // Pass None for ephemeral identity (test mode)
        let sys = rt.block_on(async { CyanSystem::new(path, None).await });
        eprintln!("🔴 CyanSystem::new() returned");
        match sys {
            Ok(s) => {
                println!("⚠️ Cyan initialized (EPHEMERAL) with ID: {}", &s.node_id[..16]);
                SYSTEM.set(Arc::new(s)).is_ok()
            }
            Err(e) => {
                eprintln!("Failed init: {e}");
                false
            }
        }
    }).join();

    res.unwrap_or(false)
}

use crate::{CyanSystem, AI_RESPONSE_QUEUE, DATA_DIR, DISCOVERY_KEY, NODE_ID, RELAY_URL, RUNTIME, SYSTEM};
use rusqlite::params;
use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::{c_char, CStr, CString};
use std::path::{Path, PathBuf};
// Initialize tracing (only once)
// Initialize tracing (only once)
use std::sync::{Arc, Mutex, Once, OnceLock};

static TRACING_INIT: Once = Once::new();

/// Initialize Cyan with persistent identity from Swift Keychain.
/// Same NodeID across app launches - use for production.
/// secret_key_hex: 64-character hex string (32 bytes)
/// relay_url: Custom relay URL (can be null to use Iroh defaults)
/// discovery_key: Discovery key for gossip (can be null for "cyan-dev")
#[unsafe(no_mangle)]
pub extern "C" fn cyan_init_with_identity(
    db_path: *const c_char,
    secret_key_hex: *const c_char,
    relay_url: *const c_char,
    discovery_key: *const c_char,
) -> bool {
    TRACING_INIT.call_once(|| {
        use tracing_subscriber::{fmt, prelude::*, EnvFilter};
        use std::fs::File;

        // Create log file in a writable location
        let log_path = dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("../.."))
            .join("cyan_debug.log");

        let file = File::create(&log_path).unwrap_or_else(|_| {
            File::create("/tmp/cyan_debug.log").expect("Cannot create log file")
        });

        let file_layer = fmt::layer()
            .with_writer(Arc::new(file))
            .with_thread_ids(true)
            .with_thread_names(true)
            .with_file(true)
            .with_line_number(true)
            .with_ansi(false);

        let stderr_layer = fmt::layer()
            .with_writer(std::io::stderr)
            .with_thread_ids(true)
            .with_thread_names(true);

        let filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("debug"));

        tracing_subscriber::registry()
            .with(filter)
            .with(file_layer)
            .with(stderr_layer)
            .init();

        tracing::info!("🔵 Tracing initialized - log file: {:?}", log_path);
    });
    eprintln!("🔥 cyan_init_with_identity");
    // Phase-0 fingerprint guardrail: the FIRST thing a booting engine states is which
    // source it was built from, so a stale binary can never be tested against silently.
    eprintln!("🏗 {BUILD_STAMP}");
    tracing::info!("{BUILD_STAMP}");
    if SYSTEM.get().is_some() {
        return true;
    }

    // Parse db_path
    let path = unsafe {
        if db_path.is_null() {
            eprintln!("Database path is null");
            return false;
        }
        CStr::from_ptr(db_path).to_string_lossy().to_string()
    };

    // Parse secret_key_hex
    let secret_key_bytes: [u8; 32] = unsafe {
        if secret_key_hex.is_null() {
            eprintln!("Secret key is null");
            return false;
        }
        let hex_str = match CStr::from_ptr(secret_key_hex).to_str() {
            Ok(s) => s,
            Err(_) => {
                eprintln!("Invalid secret key UTF-8");
                return false;
            }
        };

        let bytes = match hex::decode(hex_str) {
            Ok(b) if b.len() == 32 => b,
            Ok(b) => {
                eprintln!("Secret key must be 32 bytes, got {}", b.len());
                return false;
            }
            Err(e) => {
                eprintln!("Invalid secret key hex: {e}");
                return false;
            }
        };

        match bytes.try_into() {
            Ok(arr) => arr,
            Err(_) => return false,
        }
    };

    // Parse optional relay_url
    if !relay_url.is_null()
        && let Ok(url) = unsafe { CStr::from_ptr(relay_url) }.to_str()
        && !url.is_empty()
    {
        let _ = RELAY_URL.set(url.to_string());
        eprintln!("🌐 Relay URL set: {}", url);
    }

    // Parse optional discovery_key
    if !discovery_key.is_null()
        && let Ok(key) = unsafe { CStr::from_ptr(discovery_key) }.to_str()
        && !key.is_empty()
    {
        let _ = DISCOVERY_KEY.set(key.to_string());
        eprintln!("🔑 Discovery key set: {}", key);
    }

    let res = std::thread::spawn(move || {
        // §5: resolve bootstrap/relay/discovery_key from the signed rendezvous config BEFORE the
        // runtime is built. This runs AFTER the explicit FFI relay_url/discovery_key args above, and
        // `OnceCell::set` is first-wins, so an FFI-provided value still wins — the config only fills
        // in what FFI didn't set (notably the bootstrap id, replacing the old hardcode). No-op unless
        // CYAN_RENDEZVOUS_URL is set, so the existing FFI init path is unchanged when none is provided.
        crate::rendezvous::fetch_and_apply_if_configured();

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("runtime");
        RUNTIME.set(runtime).ok();

        let Some(rt) = RUNTIME.get() else {
            return false;
        };
        let sys = rt.block_on(async {
            CyanSystem::new(path, Some(secret_key_bytes)).await
        });

        match sys {
            Ok(s) => {
                println!("✅ Cyan initialized (PERSISTENT) with ID: {}", &s.node_id[..16]);
                SYSTEM.set(Arc::new(s)).is_ok()
            }
            Err(e) => {
                eprintln!("Failed init with identity: {e}");
                false
            }
        }
    }).join();

    res.unwrap_or(false)
}

#[unsafe(no_mangle)]
pub extern "C" fn cyan_get_xaero_id() -> *const c_char {
    let id = NODE_ID.get_or_init(compute_or_load_node_id);
    to_c_string(id.clone())
}

/// Get the iroh network node ID (PublicKey hex string)
/// This is used for gossip peer discovery
#[unsafe(no_mangle)]
pub extern "C" fn cyan_get_node_id() -> *mut c_char {
    let Some(sys) = SYSTEM.get() else {
        return std::ptr::null_mut();
    };
    match CString::new(sys.node_id.clone()) {
        Ok(s) => s.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn cyan_set_xaero_id(id: *const c_char) -> bool {
    if id.is_null() {
        return false;
    }
    let Ok(s) = unsafe { CStr::from_ptr(id) }.to_str() else {
        return false;
    };
    let s = s.to_string();

    let _ = NODE_ID.set(s.clone());
    save_node_id_to_disk(&s);
    true
}

/// Wipe the device's local identity key — the engine half of the iOS "delete
/// identity" flow (W17 §B). Deletes the device XaeroID private key from the OS
/// secure store (Keychain, or the headless fake) and best-effort removes the
/// on-disk `node_id.txt`, so a fresh identity is created on the next launch.
///
/// Idempotent and panic-free: returns `true` once the key is gone (already-absent
/// counts as success), `false` only if the vault delete itself errored. Local
/// data/DB is untouched — this clears identity custody only.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_delete_identity() -> bool {
    let vault = crate::device_vault();
    if let Err(e) = crate::device_vault::delete_identity(vault.as_ref()) {
        eprintln!("cyan_delete_identity: vault delete failed: {e}");
        return false;
    }
    // Best-effort: drop the cached public node id so a new one is minted next run.
    if let Some(dir) = DATA_DIR.get() {
        let _ = std::fs::remove_file(dir.join("node_id.txt"));
    }
    true
}

#[unsafe(no_mangle)]
pub extern "C" fn cyan_free_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        unsafe {
            let _ = CString::from_raw(ptr);
        }
    }
}

/// Check if the Cyan system is initialized and ready
#[unsafe(no_mangle)]
pub extern "C" fn cyan_is_ready() -> bool {
    SYSTEM.get().is_some()
}

#[unsafe(no_mangle)]
pub extern "C" fn cyan_poll_events(component: *const c_char) -> *mut c_char {
    let Some(cyan) = SYSTEM.get() else {
        return std::ptr::null_mut();
    };

    // Get component name from parameter
    let component_name = unsafe {
        if component.is_null() {
            "unknown"
        } else {
            CStr::from_ptr(component).to_str().unwrap_or("unknown")
        }
    };

    // Route to correct buffer based on component name
    let event_json = match component_name {
        "file_tree" => {
            cyan.file_tree_events.lock().ok().and_then(|mut b| b.pop_front())
        }
        "chat_panel" => {
            cyan.chat_panel_events.lock().ok().and_then(|mut b| b.pop_front())
        }
        "whiteboard" => {
            cyan.whiteboard_events.lock().ok().and_then(|mut b| b.pop_front())
        }
        "board_grid" => {
            cyan.board_grid_events.lock().ok().and_then(|mut b| b.pop_front())
        }
        "network" | "status" => {
            cyan.network_status_events.lock().ok().and_then(|mut b| b.pop_front())
        }
        _ => {
            // Unknown component - log warning but don't fail
            // This helps catch Swift components using wrong names
            tracing::warn!("cyan_poll_events: unknown component '{}' - no events returned", component_name);
            None
        }
    };

    match event_json {
        Some(json) => {
            match CString::new(json) {
                Ok(cstr) => cstr.into_raw(),
                Err(_) => std::ptr::null_mut(),
            }
        }
        None => std::ptr::null_mut(),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn cyan_send_command(_component: *const c_char, json: *const c_char) -> bool {
    let json_str = unsafe { CStr::from_ptr(json).to_string_lossy().to_string() };

    let Some(system) = SYSTEM.get() else {
        return false;
    };

    match serde_json::from_str::<CommandMsg>(&json_str) {
        Ok(command) => match system.command_tx.send(command) {
            Ok(_) => true,
            Err(e) => {
                eprintln!("failed to send command: {e:?}");
                false
            }
        },
        Err(e) => {
            eprintln!("failed to parse command: {e:?}");
            false
        }
    }
}

// ---------- FFI: groups ----------
#[unsafe(no_mangle)]
pub extern "C" fn cyan_create_group(
    name: *const c_char,
    icon: *const c_char,
    color: *const c_char,
) {
    let Some(name) = (unsafe { cstr_arg(name) }) else {
        return;
    };
    let icon = (unsafe { cstr_arg(icon) }).unwrap_or_else(|| "folder.fill".into());
    let color = (unsafe { cstr_arg(color) }).unwrap_or_else(|| "#00AEEF".into());

    let sys = match SYSTEM.get() {
        Some(s) => s.clone(),
        None => return,
    };
    let _ = sys.command_tx.send(CommandMsg::CreateGroup { name, icon, color });
}

#[unsafe(no_mangle)]
pub extern "C" fn cyan_rename_group(id: *const c_char, new_name: *const c_char) {
    let Some(id) = (unsafe { cstr_arg(id) }) else {
        return;
    };
    let Some(name) = (unsafe { cstr_arg(new_name) }) else {
        return;
    };
    let sys = match SYSTEM.get() {
        Some(s) => s.clone(),
        None => return,
    };
    let _ = sys.command_tx.send(CommandMsg::RenameGroup { id, name });
}

#[unsafe(no_mangle)]
pub extern "C" fn cyan_delete_group(id: *const c_char) {
    let Some(id) = (unsafe { cstr_arg(id) }) else {
        return;
    };
    let sys = match SYSTEM.get() {
        Some(s) => s.clone(),
        None => return,
    };
    let _ = sys.command_tx.send(CommandMsg::DeleteGroup { id });
}

#[unsafe(no_mangle)]
pub extern "C" fn cyan_leave_group(id: *const c_char) {
    let Some(id) = (unsafe { cstr_arg(id) }) else {
        return;
    };
    let sys = match SYSTEM.get() {
        Some(s) => s.clone(),
        None => return,
    };
    let _ = sys.command_tx.send(CommandMsg::LeaveGroup { id });
}

#[unsafe(no_mangle)]
pub extern "C" fn cyan_is_group_owner(id: *const c_char) -> bool {
    let Some(id) = (unsafe { cstr_arg(id) }) else {
        return false;
    };
    let sys = match SYSTEM.get() {
        Some(s) => s.clone(),
        None => return false,
    };
    storage::group_is_owner(&id, &sys.node_id)
}

// ---------- FFI: workspaces ----------
#[unsafe(no_mangle)]
pub extern "C" fn cyan_create_workspace(group_id: *const c_char, name: *const c_char) {
    let Some(gid) = (unsafe { cstr_arg(group_id) }) else {
        return;
    };
    let Some(name) = (unsafe { cstr_arg(name) }) else {
        return;
    };
    let sys = match SYSTEM.get() {
        Some(s) => s.clone(),
        None => return,
    };
    let _ = sys.command_tx.send(CommandMsg::CreateWorkspace {
        group_id: gid,
        name,
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn cyan_rename_workspace(id: *const c_char, new_name: *const c_char) {
    let Some(id) = (unsafe { cstr_arg(id) }) else {
        return;
    };
    let Some(name) = (unsafe { cstr_arg(new_name) }) else {
        return;
    };
    let sys = match SYSTEM.get() {
        Some(s) => s.clone(),
        None => return,
    };
    let _ = sys.command_tx.send(CommandMsg::RenameWorkspace { id, name });
}

#[unsafe(no_mangle)]
pub extern "C" fn cyan_delete_workspace(id: *const c_char) {
    let Some(id) = (unsafe { cstr_arg(id) }) else {
        return;
    };
    let sys = match SYSTEM.get() {
        Some(s) => s.clone(),
        None => return,
    };
    let _ = sys.command_tx.send(CommandMsg::DeleteWorkspace { id });
}

#[unsafe(no_mangle)]
pub extern "C" fn cyan_leave_workspace(id: *const c_char) {
    let Some(id) = (unsafe { cstr_arg(id) }) else {
        return;
    };
    let sys = match SYSTEM.get() {
        Some(s) => s.clone(),
        None => return,
    };
    let _ = sys.command_tx.send(CommandMsg::LeaveWorkspace { id });
}

#[unsafe(no_mangle)]
pub extern "C" fn cyan_is_workspace_owner(id: *const c_char) -> bool {
    let Some(id) = (unsafe { cstr_arg(id) }) else {
        return false;
    };
    let sys = match SYSTEM.get() {
        Some(s) => s.clone(),
        None => return false,
    };
    storage::workspace_is_owner(&id, &sys.node_id)
}

// ---------- FFI: boards ----------
#[unsafe(no_mangle)]
pub extern "C" fn cyan_create_board(workspace_id: *const c_char, name: *const c_char) {
    let Some(wid) = (unsafe { cstr_arg(workspace_id) }) else {
        return;
    };
    let Some(name) = (unsafe { cstr_arg(name) }) else {
        return;
    };
    let sys = match SYSTEM.get() {
        Some(s) => s.clone(),
        None => return,
    };
    let _ = sys.command_tx.send(CommandMsg::CreateBoard {
        workspace_id: wid,
        name,
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn cyan_rename_board(id: *const c_char, new_name: *const c_char) {
    let Some(id) = (unsafe { cstr_arg(id) }) else {
        return;
    };
    let Some(name) = (unsafe { cstr_arg(new_name) }) else {
        return;
    };
    let sys = match SYSTEM.get() {
        Some(s) => s.clone(),
        None => return,
    };
    let _ = sys.command_tx.send(CommandMsg::RenameBoard { id, name });
}

#[unsafe(no_mangle)]
pub extern "C" fn cyan_delete_board(id: *const c_char) {
    let Some(id) = (unsafe { cstr_arg(id) }) else {
        return;
    };
    let sys = match SYSTEM.get() {
        Some(s) => s.clone(),
        None => return,
    };
    let _ = sys.command_tx.send(CommandMsg::DeleteBoard { id });
}

#[unsafe(no_mangle)]
pub extern "C" fn cyan_leave_board(id: *const c_char) {
    let Some(id) = (unsafe { cstr_arg(id) }) else {
        return;
    };
    let sys = match SYSTEM.get() {
        Some(s) => s.clone(),
        None => return,
    };
    let _ = sys.command_tx.send(CommandMsg::LeaveBoard { id });
}

#[unsafe(no_mangle)]
pub extern "C" fn cyan_is_board_owner(id: *const c_char) -> bool {
    let Some(id) = (unsafe { cstr_arg(id) }) else {
        return false;
    };
    let sys = match SYSTEM.get() {
        Some(s) => s.clone(),
        None => return false,
    };
    storage::board_is_owner(&id, &sys.node_id)
}

// ---------- FFI: chats ----------
/// Send a chat to a **board** (R11 §1 — chat is board-scoped). The first argument is now a
/// `board_id` (was `workspace_id`); the C ABI is unchanged (same arity/types), so iOS only
/// changes which id it passes. The engine derives the board's workspace + group internally.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_send_chat(
    board_id: *const c_char,
    message: *const c_char,
    parent_id: *const c_char,
) {
    let Some(bid) = (unsafe { cstr_arg(board_id) }) else {
        return;
    };
    let Some(msg) = (unsafe { cstr_arg(message) }) else {
        return;
    };
    let parent = unsafe { cstr_arg(parent_id) }; // Can be null for root messages

    let sys = match SYSTEM.get() {
        Some(s) => s.clone(),
        None => return,
    };
    let _ = sys.command_tx.send(CommandMsg::SendChat {
        board_id: bid,
        message: msg,
        parent_id: parent,
    });
}

/// Load a **board's** chat history (R11 §1). Replays each stored message as a `ChatSent`
/// onto the chat-panel event buffer, then emits `ChatHistoryComplete { board_id, .. }`.
/// Additive FFI — iOS calls this on opening a board's chat panel.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_load_chat_history(board_id: *const c_char) {
    let Some(bid) = (unsafe { cstr_arg(board_id) }) else {
        return;
    };
    if let Some(sys) = SYSTEM.get() {
        let _ = sys.command_tx.send(CommandMsg::LoadChatHistory { board_id: bid });
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn cyan_delete_chat(id: *const c_char) {
    let Some(id) = (unsafe { cstr_arg(id) }) else {
        return;
    };
    let sys = match SYSTEM.get() {
        Some(s) => s.clone(),
        None => return,
    };
    let _ = sys.command_tx.send(CommandMsg::DeleteChat { id });
}

// ---------- FFI: notes (ROUND8 §W2 — board-level authored LWW ledger) ----------
/// Author or edit a note on a board. `note_id` null ⇒ create a new note; non-null ⇒
/// edit that note (LWW on `updated_at`). `tenant_id` null ⇒ derive from the board's
/// group. Additive verb — never replaces any existing FFI.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_note_put(
    board_id: *const c_char,
    note_id: *const c_char,
    tenant_id: *const c_char,
    text: *const c_char,
) {
    let Some(board) = (unsafe { cstr_arg(board_id) }) else {
        return;
    };
    let Some(text) = (unsafe { cstr_arg(text) }) else {
        return;
    };
    let note_id = unsafe { cstr_arg(note_id) }; // null ⇒ new note
    let tenant_id = unsafe { cstr_arg(tenant_id) }; // null ⇒ derive from board's group

    let sys = match SYSTEM.get() {
        Some(s) => s.clone(),
        None => return,
    };
    let _ = sys.command_tx.send(CommandMsg::PutNote {
        board_id: board,
        note_id,
        tenant_id,
        text,
        scope: None,
        kind: None,
    });
}

/// Author or edit a SCOPED note (feat/notes-constitution). Same contract as
/// `cyan_note_put`, plus `scope` (`tenant`|`group`|`board`) and `kind`
/// (`constitution`|`preference`|`editor-note`); null ⇒ `board`/`editor-note`. For
/// `group`/`tenant` scope, `board_id` carries the ANCHOR id (the group/tenant id).
/// Additive verb — never replaces any existing FFI.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_note_put_scoped(
    board_id: *const c_char,
    note_id: *const c_char,
    tenant_id: *const c_char,
    text: *const c_char,
    scope: *const c_char,
    kind: *const c_char,
) {
    let Some(board) = (unsafe { cstr_arg(board_id) }) else {
        return;
    };
    let Some(text) = (unsafe { cstr_arg(text) }) else {
        return;
    };
    let note_id = unsafe { cstr_arg(note_id) }; // null ⇒ new note
    let tenant_id = unsafe { cstr_arg(tenant_id) }; // null ⇒ derive from anchor group
    let scope = unsafe { cstr_arg(scope) }; // null ⇒ "board"
    let kind = unsafe { cstr_arg(kind) }; // null ⇒ "editor-note"

    let sys = match SYSTEM.get() {
        Some(s) => s.clone(),
        None => return,
    };
    let _ = sys.command_tx.send(CommandMsg::PutNote {
        board_id: board,
        note_id,
        tenant_id,
        text,
        scope,
        kind,
    });
}

/// List a board's notes as a JSON array of `NoteDTO`, tenant-scoped to the board's
/// group. Caller owns the returned string and must free it with `cyan_free_string`.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_note_list(board_id: *const c_char) -> *mut c_char {
    let Some(board) = (unsafe { cstr_arg(board_id) }) else {
        return std::ptr::null_mut();
    };
    // Tenant is the board's group (group == tenant); fall back to the board id so an
    // un-grouped board still returns its own notes rather than nothing.
    let tenant = storage::board_get_group_id(&board).unwrap_or_else(|| board.clone());
    let notes = storage::note_list_by_board(&board, &tenant).unwrap_or_default();
    let json = serde_json::to_string(&notes).unwrap_or_else(|_| "[]".to_string());
    match CString::new(json) {
        Ok(s) => s.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

/// Delete a note by id.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_note_delete(id: *const c_char) {
    let Some(id) = (unsafe { cstr_arg(id) }) else {
        return;
    };
    let sys = match SYSTEM.get() {
        Some(s) => s.clone(),
        None => return,
    };
    let _ = sys.command_tx.send(CommandMsg::DeleteNote { id });
}

// ---------- FFI: templates + pinned workflows (ROUND8 §W4) ----------
/// List the templates visible to a tenant as a JSON array of `Template`: the built-in
/// media seeds (always) plus the tenant's own save-as-template results. `tenant_id`
/// null ⇒ seeds only. Caller owns the returned string; free with `cyan_free_string`.
/// Additive verb — never replaces any existing FFI.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_template_list(tenant_id: *const c_char) -> *mut c_char {
    let tenant = unsafe { cstr_arg(tenant_id) }.unwrap_or_default();
    let templates = crate::templates::list_templates(&tenant);
    let json = serde_json::to_string(&templates).unwrap_or_else(|_| "[]".to_string());
    match CString::new(json) {
        Ok(s) => s.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

/// Clone a template into a board as real W1 step cells (the cloned steps are authorable
/// workflow steps that compile + sync like any cell). `tenant_id` null ⇒ derive from the
/// board's group. Runs through the command actor so each cloned step broadcasts.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_workflow_from_template(
    template_id: *const c_char,
    board_id: *const c_char,
    tenant_id: *const c_char,
) {
    let Some(template_id) = (unsafe { cstr_arg(template_id) }) else {
        return;
    };
    let Some(board_id) = (unsafe { cstr_arg(board_id) }) else {
        return;
    };
    let tenant_id = unsafe { cstr_arg(tenant_id) }; // null ⇒ derive from board's group

    let sys = match SYSTEM.get() {
        Some(s) => s.clone(),
        None => return,
    };
    let _ = sys.command_tx.send(CommandMsg::WorkflowFromTemplate {
        template_id,
        board_id,
        tenant_id,
    });
}

/// Save a board's steps as a reusable user template, tenant-scoped. `steps_json` is a
/// JSON array of `TemplateStep` (`{ "text": ..., "plugin": ... }`). Returns the created
/// `Template` as JSON (caller frees with `cyan_free_string`), or null on bad input.
/// Additive verb the iOS save-as-template flow consumes.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_template_save(
    tenant_id: *const c_char,
    name: *const c_char,
    description: *const c_char,
    steps_json: *const c_char,
) -> *mut c_char {
    let Some(tenant) = (unsafe { cstr_arg(tenant_id) }) else {
        return std::ptr::null_mut();
    };
    let Some(name) = (unsafe { cstr_arg(name) }) else {
        return std::ptr::null_mut();
    };
    let description = unsafe { cstr_arg(description) }.unwrap_or_default();
    let Some(steps_json) = (unsafe { cstr_arg(steps_json) }) else {
        return std::ptr::null_mut();
    };
    let Ok(steps) = serde_json::from_str::<Vec<crate::models::dto::TemplateStep>>(&steps_json) else {
        return std::ptr::null_mut();
    };
    let Ok(template) = crate::templates::save_as_template(&tenant, &name, &description, steps) else {
        return std::ptr::null_mut();
    };
    let json = serde_json::to_string(&template).unwrap_or_else(|_| "{}".to_string());
    match CString::new(json) {
        Ok(s) => s.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

/// Set a board's pinned-workflow state (a pinned workflow surfaces for fast cloning).
/// Replicated team state (LWW); runs through the command actor so it broadcasts.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_pin_set(board_id: *const c_char, pinned: bool) {
    let Some(board_id) = (unsafe { cstr_arg(board_id) }) else {
        return;
    };
    let sys = match SYSTEM.get() {
        Some(s) => s.clone(),
        None => return,
    };
    let _ = sys.command_tx.send(CommandMsg::SetPin { board_id, pinned });
}

// ---------- FFI: direct chats ----------
/// Start a direct QUIC chat stream with a peer
#[unsafe(no_mangle)]
pub extern "C" fn cyan_start_direct_chat(
    peer_id: *const c_char,
    workspace_id: *const c_char,
) {
    let Some(pid) = (unsafe { cstr_arg(peer_id) }) else {
        return;
    };
    let Some(wid) = (unsafe { cstr_arg(workspace_id) }) else {
        return;
    };
    let sys = match SYSTEM.get() {
        Some(s) => s.clone(),
        None => return,
    };
    let _ = sys.network_tx.send(NetworkCommand::StartChatStream {
        peer_id: pid,
        workspace_id: wid,
    });
}

/// Send a message on an existing direct chat stream
#[unsafe(no_mangle)]
pub extern "C" fn cyan_send_direct_chat(
    peer_id: *const c_char,
    workspace_id: *const c_char,
    message: *const c_char,
    parent_id: *const c_char,
) {
    let Some(pid) = (unsafe { cstr_arg(peer_id) }) else {
        return;
    };
    let Some(wid) = (unsafe { cstr_arg(workspace_id) }) else {
        return;
    };
    let Some(msg) = (unsafe { cstr_arg(message) }) else {
        return;
    };
    let parent = unsafe { cstr_arg(parent_id) };

    let sys = match SYSTEM.get() {
        Some(s) => s.clone(),
        None => return,
    };
    let _ = sys.network_tx.send(NetworkCommand::SendDirectChat {
        peer_id: pid,
        workspace_id: wid,
        message: msg,
        parent_id: parent,
        attachment: None,
    });
}

// ---------- FFI: uploads ----------
#[unsafe(no_mangle)]
pub extern "C" fn cyan_upload_file_to_group(group_id: *const c_char, path: *const c_char) {
    let Some(gid) = (unsafe { cstr_arg(group_id) }) else {
        return;
    };
    let Some(p) = (unsafe { cstr_arg(path) }) else {
        return;
    };
    let sys = match SYSTEM.get() {
        Some(s) => s.clone(),
        None => return,
    };
    let _ = sys.network_tx.send(NetworkCommand::UploadToGroup {
        group_id: gid,
        path: p,
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn cyan_upload_file_to_workspace(workspace_id: *const c_char, path: *const c_char) {
    let Some(wid) = (unsafe { cstr_arg(workspace_id) }) else {
        return;
    };
    let Some(p) = (unsafe { cstr_arg(path) }) else {
        return;
    };
    let sys = match SYSTEM.get() {
        Some(s) => s.clone(),
        None => return,
    };
    let _ = sys.network_tx.send(NetworkCommand::UploadToWorkspace {
        workspace_id: wid,
        path: p,
    });
}

/// Install a signed `.cyanplugin` bundle into a group's local "Plugins" workspace
/// (BURST C2 — the cyan-backend receiver half of the Market install leg).
///
/// The app fetches the bundle bytes from the Lens marketplace endpoint, base64-encodes
/// them, and calls this. We base64-decode, hand the tar bytes to
/// `storage::install_plugin_bundle`, and — on success — emit a fresh `TreeLoaded` (via the
/// command actor's `Snapshot`) so the Explorer/authoring surface refreshes and
/// `workflow::autocomplete_index` finds the plugin under `@`.
///
/// Idempotent: a re-install replaces the prior bundle (deterministic file id + overwrite).
///
/// Returns a JSON string the caller frees with `cyan_free_string`:
///   `{"success":true,"plugin_id":"<id>","file_id":"<id>"}` on success, else
///   `{"success":false,"error":"<why>"}`. Never null unless the JSON encode itself fails.
///
/// TODO(C2 signature): the bundle carries an XaeroID signature over its embedded
/// `manifest.yaml`; this repo has no `.cyanplugin` unpack path yet, so signature
/// verification is deferred (documented) — the bytes come from the authenticated Lens
/// endpoint (same auth as `/browse`). Wire verification here once the unpack lands.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_install_plugin_bundle(
    group_id: *const c_char,
    plugin_id: *const c_char,
    bundle_bytes_b64: *const c_char,
) -> *mut c_char {
    use base64::Engine as _;

    let err = |msg: String| -> *mut c_char {
        json_cstring(&serde_json::json!({ "success": false, "error": msg }).to_string())
    };

    let Some(gid) = (unsafe { cstr_arg(group_id) }) else {
        return err("missing group_id".to_string());
    };
    let Some(pid) = (unsafe { cstr_arg(plugin_id) }) else {
        return err("missing plugin_id".to_string());
    };
    let Some(b64) = (unsafe { cstr_arg(bundle_bytes_b64) }) else {
        return err("missing bundle_bytes_b64".to_string());
    };

    let bytes = match base64::engine::general_purpose::STANDARD.decode(b64.trim()) {
        Ok(b) => b,
        Err(e) => return err(format!("base64 decode failed: {e}")),
    };
    if bytes.is_empty() {
        return err("empty bundle bytes".to_string());
    }

    match storage::install_plugin_bundle(&gid, &pid, &bytes) {
        Ok(file_id) => {
            // Refresh the tree so the app sees the new plugin (emits TreeLoaded).
            if let Some(sys) = SYSTEM.get() {
                let _ = sys.command_tx.send(CommandMsg::Snapshot {});
            }
            json_cstring(
                &serde_json::json!({
                    "success": true,
                    "plugin_id": pid,
                    "file_id": file_id,
                })
                .to_string(),
            )
        }
        Err(e) => err(format!("install failed: {e}")),
    }
}

/// R10FB §D: demo seeding has been REMOVED. This symbol is kept (inert) only so the C ABI
/// stays stable until iOS stops calling it — the command it sends is a no-op and a fresh
/// DB never gets a "Demo Group"/"Demo Board".
#[unsafe(no_mangle)]
pub extern "C" fn cyan_seed_demo_if_empty() {
    if let Some(sys) = SYSTEM.get() {
        let _ = sys.command_tx.send(CommandMsg::SeedDemoIfEmpty);
    }
}

/// Fix A: actively seed the coherent demo set (3 groups / 10 boards) IN-PROCESS under the
/// app's OWN engine identity, then refresh the tree. Additive to the no-op
/// `cyan_seed_demo_if_empty` — this one really seeds. The seed is idempotent
/// (truncate-then-seed of the managed group ids) so calling it repeatedly converges to
/// exactly the intended set. Fire-and-forget: the handler emits `TreeLoaded` when done, so
/// the app's FileTree refreshes via its normal snapshot event path.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_seed_demo() {
    if let Some(sys) = SYSTEM.get() {
        let _ = sys.command_tx.send(CommandMsg::SeedDemo);
    }
}

// ---------- FFI: unread / notifications (R10FB §N) ----------

/// Unread counts as a JSON object `{board_id: count}` — **board-level only** (R11 §3). One
/// message counts once on its board (no workspace/group rollup, which killed the doubled
/// counts). The app looks up its board-card id in this map; sum the map for the dock badge
/// total. Caller frees with `cyan_free_string`.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_unread_counts() -> *mut c_char {
    let counts = storage::unread_counts().unwrap_or_default();
    let json = serde_json::to_string(&counts).unwrap_or_else(|_| "{}".to_string());
    match CString::new(json) {
        Ok(s) => s.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

/// Mark a **board** read (R11 §3/§5) — `scope_id` is a board id. Clears that board's unread
/// items, then emits `UnreadChanged` so iOS + the dock badge update live. Opening a chat is a
/// READ (this), never a write that increments.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_mark_read(scope_id: *const c_char) {
    let Some(scope_id) = (unsafe { cstr_arg(scope_id) }) else {
        return;
    };
    if let Some(sys) = SYSTEM.get() {
        let _ = sys.command_tx.send(CommandMsg::MarkRead { scope_id });
    }
}

// ---------- FFI: files — delete + handle resolver (R10FB §F) ----------

/// User-initiated file delete (R10FB §F4): soft-delete/tombstone locally and gossip the
/// deletion so it converges to peers. No hard delete in the engine.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_delete_file(file_id: *const c_char) {
    let Some(file_id) = (unsafe { cstr_arg(file_id) }) else {
        return;
    };
    if let Some(sys) = SYSTEM.get() {
        let _ = sys.command_tx.send(CommandMsg::DeleteFile { file_id });
    }
}

/// Resolve a file by its stable workflow handle `group_id:workspace_id:board_id:file_name`
/// (R10FB §F3). Returns the file as JSON, or `null` if no active file matches. Caller frees
/// with `cyan_free_string`.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_resolve_file_handle(
    group_id: *const c_char,
    workspace_id: *const c_char,
    board_id: *const c_char,
    file_name: *const c_char,
) -> *mut c_char {
    let (Some(g), Some(w), Some(b), Some(name)) = (
        unsafe { cstr_arg(group_id) },
        unsafe { cstr_arg(workspace_id) },
        unsafe { cstr_arg(board_id) },
        unsafe { cstr_arg(file_name) },
    ) else {
        return std::ptr::null_mut();
    };
    match storage::file_resolve_handle(&g, &w, &b, &name) {
        Some(file) => match serde_json::to_string(&file).ok().and_then(|j| CString::new(j).ok()) {
            Some(s) => s.into_raw(),
            None => std::ptr::null_mut(),
        },
        None => std::ptr::null_mut(),
    }
}

// ---------- FFI: peer queries ----------
/// Get peers for a specific group as JSON array: ["peer_id_1", "peer_id_2", ...]
#[unsafe(no_mangle)]
pub extern "C" fn cyan_get_group_peers(group_id: *const c_char) -> *mut c_char {
    let Some(gid) = (unsafe { cstr_arg(group_id) }) else {
        return std::ptr::null_mut();
    };
    let Some(sys) = SYSTEM.get() else {
        return std::ptr::null_mut();
    };

    let peers: Vec<String> = {
        let peers_map = sys.peers_per_group.lock_safe();
        peers_map.get(&gid)
            .map(|set| set.iter().map(|pk| pk.to_string()).collect())
            .unwrap_or_default()
    };

    match serde_json::to_string(&peers) {
        Ok(json) => CString::new(json).unwrap_or_default().into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

/// Get all peers grouped by group_id as JSON: { "group_id": ["peer1", "peer2"], ... }
#[unsafe(no_mangle)]
pub extern "C" fn cyan_get_all_peers() -> *mut c_char {
    let Some(sys) = SYSTEM.get() else {
        return std::ptr::null_mut();
    };

    let all_peers: HashMap<String, Vec<String>> = {
        let peers_map = sys.peers_per_group.lock_safe();
        peers_map.iter()
            .map(|(gid, set)| (gid.clone(), set.iter().map(|pk| pk.to_string()).collect()))
            .collect()
    };

    match serde_json::to_string(&all_peers) {
        Ok(json) => CString::new(json).unwrap_or_default().into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

/// Get count of peers for a specific group
#[unsafe(no_mangle)]
pub extern "C" fn cyan_get_group_peer_count(group_id: *const c_char) -> i32 {
    let Some(gid) = (unsafe { cstr_arg(group_id) }) else {
        return 0;
    };
    let Some(sys) = SYSTEM.get() else {
        return 0;
    };

    let peers_map = sys.peers_per_group.lock_safe();
    peers_map.get(&gid)
        .map(|set| set.len() as i32)
        .unwrap_or(0)
}

/// Get total peer count across all groups
#[unsafe(no_mangle)]
pub extern "C" fn cyan_get_total_peer_count() -> i32 {
    let Some(sys) = SYSTEM.get() else {
        return 0;
    };

    let peers_map = sys.peers_per_group.lock_safe();
    peers_map.values()
        .map(|set| set.len())
        .sum::<usize>() as i32
}

/// Get the persistent presence ROSTER for a group (MESH_HARDENING §3) as a JSON array:
/// `[{"peer_id","name","avatar","online","last_seen"}, ...]`.
///
/// Distinct from `cyan_get_group_peers` (which returns only the LIVE neighbor set): this returns
/// every member ever seen in the group — online members in green, offline members greyed with their
/// cached name + last-seen (chat-style roster). `online` is overlaid from the live neighbor set, so
/// a member currently in `peers_per_group` reports `true`. `name`/`avatar` come from the profile
/// store (null until a profile is seen). Tenant-scoped by `group_id`. Caller frees via `cyan_free_string`.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_get_group_members(group_id: *const c_char) -> *mut c_char {
    let Some(gid) = (unsafe { cstr_arg(group_id) }) else {
        return std::ptr::null_mut();
    };
    let Some(sys) = SYSTEM.get() else {
        return std::ptr::null_mut();
    };

    // Live neighbor set for the online overlay (the same map the live-peer FFI reads). No `unwrap`
    // on the lock (engine/FFI path): a poisoned lock degrades to an empty overlay, never a crash.
    let online: HashSet<String> = sys
        .peers_per_group
        .lock()
        .ok()
        .and_then(|peers_map| {
            peers_map
                .get(&gid)
                .map(|set| set.iter().map(|pk| pk.to_string()).collect())
        })
        .unwrap_or_default();

    let members: Vec<serde_json::Value> = crate::storage::group_members_list(&gid)
        .into_iter()
        .map(|(peer_id, name, avatar, last_seen)| {
            let mut m = serde_json::Map::new();
            m.insert("peer_id".to_string(), serde_json::Value::String(peer_id.clone()));
            m.insert(
                "name".to_string(),
                name.map(serde_json::Value::String).unwrap_or(serde_json::Value::Null),
            );
            m.insert(
                "avatar".to_string(),
                avatar.map(serde_json::Value::String).unwrap_or(serde_json::Value::Null),
            );
            m.insert("online".to_string(), serde_json::Value::Bool(online.contains(&peer_id)));
            m.insert("last_seen".to_string(), serde_json::Value::Number(last_seen.into()));
            serde_json::Value::Object(m)
        })
        .collect();

    match serde_json::to_string(&serde_json::Value::Array(members)) {
        Ok(json) => match CString::new(json) {
            Ok(c) => c.into_raw(),
            Err(_) => std::ptr::null_mut(),
        },
        Err(_) => std::ptr::null_mut(),
    }
}

/// Get total object count (whiteboards + files)
#[unsafe(no_mangle)]
pub extern "C" fn cyan_get_object_count() -> i32 {
    let Some(sys) = SYSTEM.get() else {
        return 0;
    };

    let db = sys.db.lock_safe();
    let count: i32 = db.query_row(
        "SELECT COUNT(*) FROM objects WHERE type IN ('whiteboard', 'file')",
        [],
        |row| row.get(0)
    ).unwrap_or(0);

    count
}

// ---------- Board Query FFI ----------

/// Get all boards for a group (across all workspaces in that group)
/// Returns JSON array: [{"id": "...", "workspace_id": "...", "group_id": "...", "name": "...", "created_at": 123}]
#[unsafe(no_mangle)]
pub extern "C" fn cyan_get_boards_for_group(group_id: *const c_char) -> *mut c_char {
    let Some(sys) = SYSTEM.get() else {
        return CString::new("[]").unwrap_or_default().into_raw();
    };

    let gid = unsafe { CStr::from_ptr(group_id) }.to_string_lossy().to_string();

    let boards: Vec<serde_json::Value> = (|| -> rusqlite::Result<Vec<serde_json::Value>> {
        let db = sys.db.lock_safe();

        // First get all workspace IDs for this group
        let mut ws_stmt = db.prepare(
            "SELECT id FROM workspaces WHERE group_id = ?1"
        )?;

        let workspace_ids: Vec<String> = ws_stmt
            .query_map(params![gid.clone()], |row| row.get::<_, String>(0))?
            .filter_map(|r| r.ok())
            .collect();

        if workspace_ids.is_empty() {
            return Ok(Vec::new());
        }

        // Query boards for all workspaces in this group
        let mut all_boards = Vec::new();
        for wid in &workspace_ids {
            let mut stmt = db.prepare(
                "SELECT id, workspace_id, name, created_at FROM objects
                 WHERE type = 'whiteboard' AND workspace_id = ?1
                 ORDER BY created_at DESC"
            )?;

            let boards_iter = stmt.query_map(params![wid], |row| {
                Ok(serde_json::json!({
                    "id": row.get::<_, String>(0)?,
                    "workspace_id": row.get::<_, String>(1)?,
                    "group_id": gid.clone(),
                    "name": row.get::<_, String>(2)?,
                    "created_at": row.get::<_, i64>(3)?,
                    "element_count": 0
                }))
            })?;

            for board in boards_iter.filter_map(|r| r.ok()) {
                all_boards.push(board);
            }
        }
        Ok(all_boards)
    })().unwrap_or_default();

    match serde_json::to_string(&boards) {
        Ok(json) => CString::new(json).unwrap_or_default().into_raw(),
        Err(_) => CString::new("[]").unwrap_or_default().into_raw(),
    }
}

/// Get all boards for a specific workspace
/// Returns JSON array
#[unsafe(no_mangle)]
pub extern "C" fn cyan_get_boards_for_workspace(workspace_id: *const c_char) -> *mut c_char {
    let Some(sys) = SYSTEM.get() else {
        return CString::new("[]").unwrap_or_default().into_raw();
    };

    let wid = unsafe { CStr::from_ptr(workspace_id) }.to_string_lossy().to_string();

    let boards: Vec<serde_json::Value> = (|| -> rusqlite::Result<Vec<serde_json::Value>> {
        let db = sys.db.lock_safe();

        // Get group_id for this workspace
        let group_id: String = db.query_row(
            "SELECT group_id FROM workspaces WHERE id = ?1",
            params![wid.clone()],
            |row| row.get(0)
        ).unwrap_or_default();

        let mut stmt = db.prepare(
            "SELECT id, workspace_id, name, created_at FROM objects
             WHERE type = 'whiteboard' AND workspace_id = ?1
             ORDER BY created_at DESC"
        )?;

        Ok(stmt.query_map(params![wid], |row| {
            Ok(serde_json::json!({
                "id": row.get::<_, String>(0)?,
                "workspace_id": row.get::<_, String>(1)?,
                "group_id": group_id.clone(),
                "name": row.get::<_, String>(2)?,
                "created_at": row.get::<_, i64>(3)?,
                "element_count": 0
            }))
        })?.filter_map(|r| r.ok()).collect())
    })().unwrap_or_default();

    match serde_json::to_string(&boards) {
        Ok(json) => CString::new(json).unwrap_or_default().into_raw(),
        Err(_) => CString::new("[]").unwrap_or_default().into_raw(),
    }
}

/// Get all boards across all groups and workspaces
/// Returns JSON array
#[unsafe(no_mangle)]
pub extern "C" fn cyan_get_all_boards() -> *mut c_char {
    let Some(sys) = SYSTEM.get() else {
        return CString::new("[]").unwrap_or_default().into_raw();
    };

    let boards: Vec<serde_json::Value> = (|| -> rusqlite::Result<Vec<serde_json::Value>> {
        let db = sys.db.lock_safe();

        let mut stmt = db.prepare(
            "SELECT o.id, o.workspace_id, w.group_id, o.name, o.created_at,
                    COALESCE(m.is_pinned, 0) as is_pinned,
                    COALESCE(m.labels, '[]') as labels,
                    COALESCE(m.rating, 0) as rating,
                    COALESCE(m.last_accessed, 0) as last_accessed
             FROM objects o
             LEFT JOIN workspaces w ON o.workspace_id = w.id
             LEFT JOIN board_metadata m ON o.id = m.board_id
             WHERE o.type = 'whiteboard'
             ORDER BY COALESCE(m.is_pinned, 0) DESC, o.created_at DESC"
        )?;

        Ok(stmt.query_map([], |row| {
            Ok(serde_json::json!({
                "id": row.get::<_, String>(0)?,
                "workspace_id": row.get::<_, String>(1)?,
                "group_id": row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                "name": row.get::<_, String>(3)?,
                "created_at": row.get::<_, i64>(4)?,
                "element_count": 0,
                "is_pinned": row.get::<_, i32>(5)? != 0,
                "labels": row.get::<_, String>(6)?,
                "rating": row.get::<_, i32>(7)?,
                "last_accessed": row.get::<_, i64>(8)?
            }))
        })?.filter_map(|r| r.ok()).collect())
    })().unwrap_or_default();

    match serde_json::to_string(&boards) {
        Ok(json) => CString::new(json).unwrap_or_default().into_raw(),
        Err(_) => CString::new("[]").unwrap_or_default().into_raw(),
    }
}

// ---------- Whiteboard Elements FFI ----------

/// Load all elements for a whiteboard/board
/// Returns JSON array of element objects
#[unsafe(no_mangle)]
pub extern "C" fn cyan_load_whiteboard_elements(board_id: *const c_char) -> *mut c_char {
    let Some(sys) = SYSTEM.get() else {
        return CString::new("[]").unwrap_or_default().into_raw();
    };

    let bid = unsafe { CStr::from_ptr(board_id) }.to_string_lossy().to_string();

    let elements: Vec<serde_json::Value> = (|| -> rusqlite::Result<Vec<serde_json::Value>> {
        let db = sys.db.lock_safe();

        let mut stmt = db.prepare(
            "SELECT id, board_id, element_type, x, y, width, height, z_index,
                    style_json, content_json, created_at, updated_at
             FROM whiteboard_elements
             WHERE board_id = ?1
             ORDER BY z_index ASC, created_at ASC"
        )?;

        Ok(stmt.query_map(params![bid], |row| {
            Ok(serde_json::json!({
                "id": row.get::<_, String>(0)?,
                "board_id": row.get::<_, String>(1)?,
                "element_type": row.get::<_, String>(2)?,
                "x": row.get::<_, f64>(3)?,
                "y": row.get::<_, f64>(4)?,
                "width": row.get::<_, f64>(5)?,
                "height": row.get::<_, f64>(6)?,
                "z_index": row.get::<_, i32>(7)?,
                "style_json": row.get::<_, Option<String>>(8)?,
                "content_json": row.get::<_, Option<String>>(9)?,
                "created_at": row.get::<_, i64>(10)?,
                "updated_at": row.get::<_, i64>(11)?
            }))
        })?.filter_map(|r| r.ok()).collect())
    })().unwrap_or_default();

    match serde_json::to_string(&elements) {
        Ok(json) => CString::new(json).unwrap_or_default().into_raw(),
        Err(_) => CString::new("[]").unwrap_or_default().into_raw(),
    }
}

/// Save (insert or update) a whiteboard element
/// Input: JSON object with element fields
/// Returns: true on success
#[unsafe(no_mangle)]
pub extern "C" fn cyan_save_whiteboard_element(element_json: *const c_char) -> bool {
    let Some(sys) = SYSTEM.get() else {
        return false;
    };

    let json_str = unsafe { CStr::from_ptr(element_json) }.to_string_lossy().to_string();

    let Ok(elem) = serde_json::from_str::<serde_json::Value>(&json_str) else {
        return false;
    };

    let id = elem["id"].as_str().unwrap_or("").to_string();
    let board_id = elem["board_id"].as_str().unwrap_or("").to_string();
    let element_type = elem["element_type"].as_str().unwrap_or("rectangle").to_string();
    let x = elem["x"].as_f64().unwrap_or(0.0);
    let y = elem["y"].as_f64().unwrap_or(0.0);
    let width = elem["width"].as_f64().unwrap_or(100.0);
    let height = elem["height"].as_f64().unwrap_or(100.0);
    let z_index = elem["z_index"].as_i64().unwrap_or(0) as i32;
    let style_json = elem["style_json"].as_str().map(|s| s.to_string());
    let content_json = elem["content_json"].as_str().map(|s| s.to_string());
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let created_at = elem["created_at"].as_i64().unwrap_or(now);
    let updated_at = now;

    if id.is_empty() || board_id.is_empty() {
        return false;
    }

    // Check if element exists (for add vs update event)
    let is_new: bool;
    let group_id: String;

    {
        let db = sys.db.lock_safe();

        // Check if exists
        is_new = db.query_row(
            "SELECT 1 FROM whiteboard_elements WHERE id = ?1",
            params![&id],
            |_| Ok(())
        ).is_err();

        // Get group_id via board -> workspace -> group
        group_id = db.query_row(
            "SELECT w.group_id FROM objects o
             JOIN workspaces w ON o.workspace_id = w.id
             WHERE o.id = ?1",
            params![&board_id],
            |row| row.get(0)
        ).unwrap_or_default();

        // Insert or replace
        let result = db.execute(
            "INSERT OR REPLACE INTO whiteboard_elements
             (id, board_id, element_type, x, y, width, height, z_index, style_json, content_json, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![&id, &board_id, &element_type, x, y, width, height, z_index, &style_json, &content_json, created_at, updated_at]
        );

        if result.is_err() {
            return false;
        }
    }

    // Broadcast via gossip
    if !group_id.is_empty() {
        let event = if is_new {
            NetworkEvent::WhiteboardElementAdded {
                id: id.clone(),
                board_id: board_id.clone(),
                element_type,
                x, y, width, height, z_index,
                style_json,
                content_json,
                created_at,
                updated_at,
            }
        } else {
            NetworkEvent::WhiteboardElementUpdated {
                id: id.clone(),
                board_id: board_id.clone(),
                element_type,
                x, y, width, height, z_index,
                style_json,
                content_json,
                updated_at,
            }
        };

        let _ = sys.network_tx.send(NetworkCommand::Broadcast {
            group_id,
            event,
        });
    }

    true
}

/// Delete a whiteboard element by ID
#[unsafe(no_mangle)]
pub extern "C" fn cyan_delete_whiteboard_element(element_id: *const c_char) -> bool {
    let Some(sys) = SYSTEM.get() else {
        return false;
    };

    let id = unsafe { CStr::from_ptr(element_id) }.to_string_lossy().to_string();

    if id.is_empty() {
        return false;
    }

    let board_id: String;
    let group_id: String;

    {
        let db = sys.db.lock_safe();

        // Get board_id before deleting
        board_id = db.query_row(
            "SELECT board_id FROM whiteboard_elements WHERE id = ?1",
            params![&id],
            |row| row.get(0)
        ).unwrap_or_default();

        // Get group_id via board -> workspace -> group
        group_id = if !board_id.is_empty() {
            db.query_row(
                "SELECT w.group_id FROM objects o
                 JOIN workspaces w ON o.workspace_id = w.id
                 WHERE o.id = ?1",
                params![&board_id],
                |row| row.get(0)
            ).unwrap_or_default()
        } else {
            String::new()
        };

        let result = db.execute(
            "DELETE FROM whiteboard_elements WHERE id = ?1",
            params![&id]
        );

        if result.is_err() {
            return false;
        }
    }

    // Broadcast via gossip
    if !group_id.is_empty() && !board_id.is_empty() {
        let _ = sys.network_tx.send(NetworkCommand::Broadcast {
            group_id,
            event: NetworkEvent::WhiteboardElementDeleted {
                id,
                board_id,
            },
        });
    }

    true
}

/// Clear all elements for a whiteboard/board
#[unsafe(no_mangle)]
pub extern "C" fn cyan_clear_whiteboard(board_id: *const c_char) -> bool {
    let Some(sys) = SYSTEM.get() else {
        return false;
    };

    let bid = unsafe { CStr::from_ptr(board_id) }.to_string_lossy().to_string();

    if bid.is_empty() {
        return false;
    }

    let group_id: String;

    {
        let db = sys.db.lock_safe();

        // Get group_id via board -> workspace -> group
        group_id = db.query_row(
            "SELECT w.group_id FROM objects o
             JOIN workspaces w ON o.workspace_id = w.id
             WHERE o.id = ?1",
            params![&bid],
            |row| row.get(0)
        ).unwrap_or_default();

        let result = db.execute(
            "DELETE FROM whiteboard_elements WHERE board_id = ?1",
            params![&bid]
        );

        if result.is_err() {
            return false;
        }
    }

    // Broadcast via gossip
    if !group_id.is_empty() {
        let _ = sys.network_tx.send(NetworkCommand::Broadcast {
            group_id,
            event: NetworkEvent::WhiteboardCleared {
                board_id: bid,
            },
        });
    }

    true
}

/// Get element count for a board (useful for BoardGridView badges)
#[unsafe(no_mangle)]
pub extern "C" fn cyan_get_whiteboard_element_count(board_id: *const c_char) -> i32 {
    let Some(sys) = SYSTEM.get() else {
        return 0;
    };

    let bid = unsafe { CStr::from_ptr(board_id) }.to_string_lossy().to_string();

    let db = sys.db.lock_safe();
    db.query_row(
        "SELECT COUNT(*) FROM whiteboard_elements WHERE board_id = ?1",
        params![bid],
        |row| row.get(0)
    ).unwrap_or(0)
}

/// Get all workspace IDs for a group
/// Returns JSON array of workspace ID strings: ["ws1", "ws2", ...]
#[unsafe(no_mangle)]
pub extern "C" fn cyan_get_workspaces_for_group(group_id: *const c_char) -> *mut c_char {
    let Some(sys) = SYSTEM.get() else {
        return CString::new("[]").unwrap_or_default().into_raw();
    };

    let gid = unsafe { CStr::from_ptr(group_id) }.to_string_lossy().to_string();

    let workspace_ids: Vec<String> = (|| -> rusqlite::Result<Vec<String>> {
        let db = sys.db.lock_safe();

        let mut stmt = db.prepare(
            "SELECT id FROM workspaces WHERE group_id = ?1"
        )?;

        Ok(stmt.query_map(params![gid], |row| row.get::<_, String>(0))?
            .filter_map(|r| r.ok())
            .collect())
    })().unwrap_or_default();

    match serde_json::to_string(&workspace_ids) {
        Ok(json) => CString::new(json).unwrap_or_default().into_raw(),
        Err(_) => CString::new("[]").unwrap_or_default().into_raw(),
    }
}

// ---------- FFI: File Transfer ----------

/// Upload a file with scope (group/workspace/board)
/// scope_json: {"type": "Group", "group_id": "..."} or {"type": "Workspace", "workspace_id": "..."} etc.
/// Returns JSON: {"success": true, "file_id": "...", "hash": "...", "size": 123} or {"success": false, "error": "..."}
#[unsafe(no_mangle)]
pub extern "C" fn cyan_upload_file(path: *const c_char, scope_json: *const c_char) -> *mut c_char {
    eprintln!("🦀 cyan_upload_file called!");
    let Some(file_path) = (unsafe { cstr_arg(path) }) else {
        eprintln!("🦀 cyan_upload_file: invalid path");
        return CString::new(r#"{"success":false,"error":"Invalid path"}"#).unwrap_or_default().into_raw();
    };
    eprintln!("🦀 cyan_upload_file: path = {}", file_path);
    let Some(scope_str) = (unsafe { cstr_arg(scope_json) }) else {
        return CString::new(r#"{"success":false,"error":"Invalid scope"}"#).unwrap_or_default().into_raw();
    };
    let Some(sys) = SYSTEM.get() else {
        eprintln!("🦀 cyan_upload_file: invalid scope");
        return CString::new(r#"{"success":false,"error":"System not initialized"}"#).unwrap_or_default().into_raw();
    };

    eprintln!("🦀 cyan_upload_file: scope = {}", scope_str);
    // Parse scope
    let scope: serde_json::Value = match serde_json::from_str(&scope_str) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("🦀failed to parse scope due to : {e:?}");
            return CString::new(format!(r#"{{"success":false,"error":"Invalid scope JSON: {}"}}"#, e))
                .unwrap_or_default().into_raw();
        }
    };

    // Read file
    let bytes = match std::fs::read(&file_path) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("🦀failed to read file path due to : {e:?}");
            return CString::new(format!(r#"{{"success":false,"error":"Failed to read file: {}"}}"#, e))
                .unwrap_or_default().into_raw();
        }
    };

    let file_name = Path::new(&file_path)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("file")
        .to_string();
    let hash = blake3::hash(&bytes).to_hex().to_string();
    let size = bytes.len() as u64;
    let now = chrono::Utc::now().timestamp();
    eprintln!("🦀 attempting to store file locally!");
    // Store file locally
    let files_dir = DATA_DIR
        .get()
        .cloned()
        .unwrap_or_else(|| PathBuf::from("../.."))
        .join("files");
    if let Err(e) = std::fs::create_dir_all(&files_dir) {
        eprintln!("🦀 failed to create dir due to : {e:?}");
        return CString::new(format!(r#"{{"success":false,"error":"Failed to create files dir at {:?}: {}"}}"#, files_dir, e))
            .unwrap_or_default().into_raw();
    }
    let local_path = files_dir.join(&hash);
    if let Err(e) = std::fs::write(&local_path, &bytes) {
        eprintln!("🦀 failed to write file due  to : {e:?}");
        return CString::new(format!(r#"{{"success":false,"error":"Failed to store file: {}"}}"#, e))
            .unwrap_or_default().into_raw();
    }

    // Determine scope and IDs
    let scope_type = scope["type"].as_str().unwrap_or("");
    let (group_id, workspace_id, board_id): (Option<String>, Option<String>, Option<String>);

    match scope_type {
        "Group" => {
            group_id = scope["group_id"].as_str().map(|s| s.to_string());
            workspace_id = None;
            board_id = None;
        }
        "Workspace" => {
            workspace_id = scope["workspace_id"].as_str().map(|s| s.to_string());
            let db = sys.db.lock_safe();
            group_id = workspace_id.as_ref().and_then(|wid| {
                db.query_row(
                    "SELECT group_id FROM workspaces WHERE id = ?1",
                    params![wid],
                    |row| row.get(0),
                ).ok()
            });
            board_id = None;
        }
        "Board" => {
            board_id = scope["board_id"].as_str().map(|s| s.to_string());
            let db = sys.db.lock_safe();
            let ids: Option<(String, String)> = board_id.as_ref().and_then(|bid| {
                db.query_row(
                    "SELECT o.workspace_id, w.group_id FROM objects o
                     JOIN workspaces w ON o.workspace_id = w.id
                     WHERE o.id = ?1",
                    params![bid],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                ).ok()
            });
            workspace_id = ids.as_ref().map(|(wid, _)| wid.clone());
            group_id = ids.map(|(_, gid)| gid);
        }
        scope_type => {
            eprintln!("🦀 invalid scope type error  {scope_type:?}");
            return CString::new(r#"{"success":false,"error":"Unknown scope type"}"#)
                .unwrap_or_default().into_raw();
        }
    }

    let gid = match &group_id {
        Some(g) => g.clone(),
        None => {
            return CString::new(r#"{"success":false,"error":"Could not determine group"}"#)
                .unwrap_or_default().into_raw();
        }
    };

    // CONTENT-ADDRESSED DEDUP: the same bytes in the same group are ONE file row.
    // Re-uploading identical content re-scopes/renames the existing row instead of
    // minting a second id (the old `blake3("file:{gid}:{name}:{now}")` id folded in
    // the timestamp, so every re-upload duplicated the file in `#` autocomplete).
    let existing_id: Option<String> = {
        let db = sys.db.lock_safe();
        db.query_row(
            "SELECT id FROM objects
             WHERE group_id = ?1 AND hash = ?2 AND type = 'file' AND COALESCE(deleted,0) = 0",
            params![gid, hash],
            |row| row.get(0),
        )
        .optional()
        .ok()
        .flatten()
    };
    let file_id = existing_id.clone().unwrap_or_else(|| {
        blake3::hash(format!("file:{}:{}:{}", gid, file_name, now).as_bytes())
            .to_hex()
            .to_string()
    });

    // Insert into database (or re-scope the deduped row).
    {
        let db = sys.db.lock_safe();
        let result = if existing_id.is_some() {
            db.execute(
                "UPDATE objects SET name = ?2, workspace_id = ?3, board_id = ?4,
                        local_path = ?5, size = ?6 WHERE id = ?1",
                params![
                    file_id,
                    file_name,
                    workspace_id,
                    board_id,
                    local_path.to_string_lossy().to_string(),
                    size as i64
                ],
            )
        } else {
            db.execute(
                "INSERT OR REPLACE INTO objects (id, group_id, workspace_id, board_id, type, name, hash, size, source_peer, local_path, created_at)
                 VALUES (?1, ?2, ?3, ?4, 'file', ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    file_id,
                    group_id,
                    workspace_id,
                    board_id,
                    file_name,
                    hash,
                    size as i64,
                    sys.node_id,
                    local_path.to_string_lossy().to_string(),
                    now
                ],
            )
        };

        if let Err(e) = result {
            return CString::new(format!(r#"{{"success":false,"error":"DB error: {}"}}"#, e))
                .unwrap_or_default().into_raw();
        }
    }

    // Broadcast FileAvailable
    let evt = NetworkEvent::FileAvailable {
        id: file_id.clone(),
        group_id: group_id.clone(),
        workspace_id: workspace_id.clone(),
        board_id: board_id.clone(),
        name: file_name.clone(),
        hash: hash.clone(),
        size,
        source_peer: sys.node_id.clone(),
        created_at: now,
    };

    eprintln!("📤 [FILE-UPLOAD] Broadcasting FileAvailable:");
    eprintln!("   file_id: {}...", &file_id[..16.min(file_id.len())]);
    eprintln!("   group_id (gid): {}...", &gid[..16.min(gid.len())]);

    match sys.network_tx.send(NetworkCommand::Broadcast {
        group_id: gid.clone(),
        event: evt,
    }) {
        Ok(_) => eprintln!("📤 [FILE-UPLOAD] ✓ Broadcast sent to NetworkActor"),
        Err(e) => eprintln!("📤 [FILE-UPLOAD] 🔴 Broadcast FAILED: {}", e),
    }

    // Plugin distribution (G10): a `.cyanplugin` artifact (the files that land in a group's Plugins
    // workspace) is additionally seeded into this node's content-addressed swarm and announced to the
    // group, so members distribute it peer-to-peer. Reuses the existing file scope/storage + the
    // FileAvailable broadcast above — no new client `cyan_*` FFI; normal files are unaffected.
    if file_name.ends_with(".cyanplugin") {
        eprintln!("🧩 [FILE-UPLOAD] Plugin detected — seeding into swarm + announcing to group");
        let _ = sys.network_tx.send(NetworkCommand::SeedAndAnnounceBlob {
            group_id: gid.clone(),
            hash: hash.clone(),
            path: local_path.to_string_lossy().to_string(),
        });
    }

    // Return success
    let result = serde_json::json!({
        "success": true,
        "file_id": file_id,
        "hash": hash,
        "size": size
    });

    CString::new(result.to_string()).unwrap_or_default().into_raw()
}

/// Request download of a file from its source peer
#[unsafe(no_mangle)]
pub extern "C" fn cyan_request_file_download(file_id: *const c_char) -> bool {
    let Some(fid) = (unsafe { cstr_arg(file_id) }) else {
        return false;
    };
    let Some(sys) = SYSTEM.get() else {
        return false;
    };

    // Look up file info
    let file_info: Option<(String, String)> = {
        let db = sys.db.lock_safe();
        db.query_row(
            "SELECT hash, source_peer FROM objects WHERE id = ?1 AND type = 'file'",
            params![fid],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).ok()
    };

    let (hash, source_peer) = match file_info {
        Some((h, sp)) => (h, sp),
        None => return false,
    };

    // Check if already downloaded
    {
        let db = sys.db.lock_safe();
        let local_path: Option<String> = db
            .query_row(
                "SELECT local_path FROM objects WHERE id = ?1",
                params![fid],
                |row| row.get(0),
            )
            .ok()
            .flatten();

        if let Some(path) = local_path
            && Path::new(&path).exists()
        {
            return true; // Already have it locally
        }
    }

    // Check for existing partial transfer
    let resume_offset = {
        let db = sys.db.lock_safe();
        db.query_row(
            "SELECT bytes_received FROM file_transfers WHERE file_id = ?1 AND status = 'in_progress'",
            params![fid],
            |row| row.get::<_, i64>(0),
        ).unwrap_or(0) as u64
    };

    // Send download request (with resume if applicable)
    let _ = sys.network_tx.send(NetworkCommand::RequestFileDownload {
        file_id: fid,
        hash,
        source_peer,
        resume_offset,
    });

    true
}

/// Get file status (local/remote)
/// Returns JSON: {"status": "local", "local_path": "..."} or {"status": "remote"}
#[unsafe(no_mangle)]
pub extern "C" fn cyan_get_file_status(file_id: *const c_char) -> *mut c_char {
    let Some(fid) = (unsafe { cstr_arg(file_id) }) else {
        return CString::new(r#"{"status":"unknown"}"#).unwrap_or_default().into_raw();
    };
    let Some(sys) = SYSTEM.get() else {
        return CString::new(r#"{"status":"unknown"}"#).unwrap_or_default().into_raw();
    };

    let db = sys.db.lock_safe();
    let local_path: Option<String> = db
        .query_row(
            "SELECT local_path FROM objects WHERE id = ?1 AND type = 'file'",
            params![fid],
            |row| row.get(0),
        )
        .ok()
        .flatten();

    let status = match local_path {
        Some(path) if Path::new(&path).exists() => {
            serde_json::json!({
                "status": "local",
                "local_path": path
            })
        }
        _ => {
            serde_json::json!({
                "status": "remote"
            })
        }
    };

    CString::new(status.to_string()).unwrap_or_default().into_raw()
}

/// Get files for a scope
/// scope_json: {"type": "Group", "id": "..."} or {"type": "Workspace", "id": "..."} or {"type": "Board", "id": "..."}
/// Returns JSON array of file objects
#[unsafe(no_mangle)]
pub extern "C" fn cyan_get_files(scope_json: *const c_char) -> *mut c_char {
    let Some(scope_str) = (unsafe { cstr_arg(scope_json) }) else {
        return CString::new("[]").unwrap_or_default().into_raw();
    };
    let Some(sys) = SYSTEM.get() else {
        return CString::new("[]").unwrap_or_default().into_raw();
    };

    let scope: serde_json::Value = match serde_json::from_str(&scope_str) {
        Ok(v) => v,
        Err(_) => return CString::new("[]").unwrap_or_default().into_raw(),
    };

    let scope_type = scope["type"].as_str().unwrap_or("");
    let id = scope["id"].as_str()
        .or_else(|| scope["group_id"].as_str())
        .or_else(|| scope["workspace_id"].as_str())
        .or_else(|| scope["board_id"].as_str())
        .unwrap_or("");

    let files: Vec<serde_json::Value> = (|| -> rusqlite::Result<Vec<serde_json::Value>> {
        let db = sys.db.lock_safe();

        let query = match scope_type {
            "Group" => {
                "SELECT id, group_id, workspace_id, board_id, name, hash, size, source_peer, local_path, created_at
                 FROM objects WHERE type = 'file' AND group_id = ?1"
            }
            "Workspace" => {
                "SELECT id, group_id, workspace_id, board_id, name, hash, size, source_peer, local_path, created_at
                 FROM objects WHERE type = 'file' AND workspace_id = ?1"
            }
            "Board" => {
                "SELECT id, group_id, workspace_id, board_id, name, hash, size, source_peer, local_path, created_at
                 FROM objects WHERE type = 'file' AND board_id = ?1"
            }
            _ => return Ok(Vec::new()),
        };

        let mut stmt = db.prepare(query)?;
        Ok(stmt.query_map(params![id], |row| {
            let local_path: Option<String> = row.get(8)?;
            let is_local = local_path
                .as_ref()
                .map(|p| Path::new(p).exists())
                .unwrap_or(false);

            Ok(serde_json::json!({
                "id": row.get::<_, String>(0)?,
                "group_id": row.get::<_, Option<String>>(1)?,
                "workspace_id": row.get::<_, Option<String>>(2)?,
                "board_id": row.get::<_, Option<String>>(3)?,
                "name": row.get::<_, String>(4)?,
                "hash": row.get::<_, String>(5)?,
                "size": row.get::<_, i64>(6)?,
                "source_peer": row.get::<_, Option<String>>(7)?,
                "local_path": local_path,
                "created_at": row.get::<_, i64>(9)?,
                "is_local": is_local
            }))
        })?
            .filter_map(|r| r.ok())
            .collect())
    })().unwrap_or_default();

    match serde_json::to_string(&files) {
        Ok(json) => CString::new(json).unwrap_or_default().into_raw(),
        Err(_) => CString::new("[]").unwrap_or_default().into_raw(),
    }
}

/// Get local file path if file is downloaded
/// Returns null if file is not local
#[unsafe(no_mangle)]
pub extern "C" fn cyan_get_file_local_path(file_id: *const c_char) -> *mut c_char {
    let Some(fid) = (unsafe { cstr_arg(file_id) }) else {
        return std::ptr::null_mut();
    };
    let Some(sys) = SYSTEM.get() else {
        return std::ptr::null_mut();
    };

    let db = sys.db.lock_safe();
    let local_path: Option<String> = db
        .query_row(
            "SELECT local_path FROM objects WHERE id = ?1 AND type = 'file'",
            params![fid],
            |row| row.get(0),
        )
        .ok()
        .flatten();

    match local_path {
        Some(path) if Path::new(&path).exists() => {
            CString::new(path).unwrap_or_default().into_raw()
        }
        _ => std::ptr::null_mut(),
    }
}

// ==================== NOTEBOOK CELLS FFI ====================

/// Load all notebook cells for a board, ordered by cell_order
#[unsafe(no_mangle)]
pub extern "C" fn cyan_load_notebook_cells(board_id: *const c_char) -> *mut c_char {
    let Some(sys) = SYSTEM.get() else {
        return CString::new("[]").unwrap_or_default().into_raw();
    };

    let bid = unsafe { CStr::from_ptr(board_id) }.to_string_lossy().to_string();

    let cells: Vec<serde_json::Value> = {
        let db = sys.db.lock_safe();

        let mut stmt = match db.prepare(
            "SELECT id, board_id, cell_type, cell_order, content, output,
                    collapsed, height, metadata_json, created_at, updated_at
             FROM notebook_cells
             WHERE board_id = ?1
             ORDER BY cell_order ASC"
        ) {
            Ok(s) => s,
            Err(_) => return CString::new("[]").unwrap_or_default().into_raw(),
        };

        stmt.query_map(params![bid], |row| {
            Ok(serde_json::json!({
                "id": row.get::<_, String>(0)?,
                "board_id": row.get::<_, String>(1)?,
                "cell_type": row.get::<_, String>(2)?,
                "cell_order": row.get::<_, i32>(3)?,
                "content": row.get::<_, Option<String>>(4)?,
                "output": row.get::<_, Option<String>>(5)?,
                "collapsed": row.get::<_, i32>(6)? != 0,
                "height": row.get::<_, Option<f64>>(7)?,
                "metadata_json": row.get::<_, Option<String>>(8)?,
                "created_at": row.get::<_, i64>(9)?,
                "updated_at": row.get::<_, i64>(10)?
            }))
        }).map(|rows| rows.filter_map(|r| r.ok()).collect()).unwrap_or_default()
    };

    match serde_json::to_string(&cells) {
        Ok(json) => CString::new(json).unwrap_or_default().into_raw(),
        Err(_) => CString::new("[]").unwrap_or_default().into_raw(),
    }
}

/// Save (insert or update) a notebook cell
#[unsafe(no_mangle)]
pub extern "C" fn cyan_save_notebook_cell(cell_json: *const c_char) -> bool {
    let Some(sys) = SYSTEM.get() else {
        return false;
    };

    let json_str = unsafe { CStr::from_ptr(cell_json) }.to_string_lossy().to_string();

    let Ok(cell) = serde_json::from_str::<serde_json::Value>(&json_str) else {
        return false;
    };

    let id = cell["id"].as_str().unwrap_or("").to_string();
    let board_id = cell["board_id"].as_str().unwrap_or("").to_string();
    // ROUND8 §W1: the step is the only authorable kind. Collapse any (legacy) kind
    // an older client still sends into the single step primitive; system kinds pass
    // through. Default missing kind to step.
    let cell_type = crate::workflow::coerce_authoring_cell_type(
        cell["cell_type"].as_str().unwrap_or(crate::workflow::STEP_KIND),
    );
    let cell_order = cell["cell_order"].as_i64().unwrap_or(0) as i32;
    let content = cell["content"].as_str().map(|s| s.to_string());
    let output = cell["output"].as_str().map(|s| s.to_string());
    let collapsed = cell["collapsed"].as_bool().unwrap_or(false);
    let height = cell["height"].as_f64();
    let metadata_json = cell["metadata_json"].as_str().map(|s| s.to_string());

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let created_at = cell["created_at"].as_i64().unwrap_or(now);
    let updated_at = now;

    if id.is_empty() || board_id.is_empty() {
        return false;
    }

    let is_new: bool;
    let group_id: String;

    {
        let db = sys.db.lock_safe();

        // Check if exists
        is_new = db.query_row(
            "SELECT 1 FROM notebook_cells WHERE id = ?1",
            params![&id],
            |_| Ok(())
        ).is_err();

        // Get group_id via board -> workspace -> group
        group_id = db.query_row(
            "SELECT w.group_id FROM objects o
             JOIN workspaces w ON o.workspace_id = w.id
             WHERE o.id = ?1",
            params![&board_id],
            |row| row.get(0)
        ).unwrap_or_default();

        // Insert or replace
        let result = db.execute(
            "INSERT OR REPLACE INTO notebook_cells
             (id, board_id, cell_type, cell_order, content, output, collapsed, height, metadata_json, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![&id, &board_id, &cell_type, cell_order, &content, &output, collapsed as i32, height, &metadata_json, created_at, updated_at]
        );

        if result.is_err() {
            return false;
        }
    }

    // Broadcast via gossip
    if !group_id.is_empty() {
        let event = if is_new {
            NetworkEvent::NotebookCellAdded {
                id: id.clone(),
                board_id: board_id.clone(),
                cell_type,
                cell_order,
                content,
            }
        } else {
            NetworkEvent::NotebookCellUpdated {
                id: id.clone(),
                board_id: board_id.clone(),
                cell_type,
                cell_order,
                content,
                output,
                collapsed,
                height,
                metadata_json,
            }
        };

        let _ = sys.network_tx.send(NetworkCommand::Broadcast {
            group_id,
            event,
        });
    }

    true
}

/// Delete a notebook cell
#[unsafe(no_mangle)]
pub extern "C" fn cyan_delete_notebook_cell(cell_id: *const c_char) -> bool {
    let Some(sys) = SYSTEM.get() else {
        return false;
    };

    let cid = unsafe { CStr::from_ptr(cell_id) }.to_string_lossy().to_string();

    let board_id: String;
    let group_id: String;

    {
        let db = sys.db.lock_safe();

        // Get board_id and group_id before delete
        let ids: Option<(String, String)> = db.query_row(
            "SELECT c.board_id, w.group_id
             FROM notebook_cells c
             JOIN objects o ON c.board_id = o.id
             JOIN workspaces w ON o.workspace_id = w.id
             WHERE c.id = ?1",
            params![&cid],
            |row| Ok((row.get(0)?, row.get(1)?))
        ).ok();

        let Some((bid, gid)) = ids else {
            return false;
        };
        board_id = bid;
        group_id = gid;

        // Also clear cell_id from any whiteboard_elements belonging to this cell
        let _ = db.execute(
            "UPDATE whiteboard_elements SET cell_id = NULL WHERE cell_id = ?1",
            params![&cid]
        );

        // Delete the cell
        if db.execute("DELETE FROM notebook_cells WHERE id = ?1", params![&cid]).is_err() {
            return false;
        }
    }

    // Broadcast deletion
    if !group_id.is_empty() {
        let event = NetworkEvent::NotebookCellDeleted {
            id: cid.clone(),
            board_id: board_id.clone(),
        };

        let _ = sys.network_tx.send(NetworkCommand::Broadcast {
            group_id,
            event,
        });
    }

    true
}

/// Reorder cells within a board
#[unsafe(no_mangle)]
pub extern "C" fn cyan_reorder_notebook_cells(board_id: *const c_char, cell_ids_json: *const c_char) -> bool {
    let Some(sys) = SYSTEM.get() else {
        return false;
    };

    let bid = unsafe { CStr::from_ptr(board_id) }.to_string_lossy().to_string();
    let json_str = unsafe { CStr::from_ptr(cell_ids_json) }.to_string_lossy().to_string();

    let Ok(cell_ids) = serde_json::from_str::<Vec<String>>(&json_str) else {
        return false;
    };

    let group_id: String;

    {
        let db = sys.db.lock_safe();

        group_id = db.query_row(
            "SELECT w.group_id FROM objects o
             JOIN workspaces w ON o.workspace_id = w.id
             WHERE o.id = ?1",
            params![&bid],
            |row| row.get(0)
        ).unwrap_or_default();

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        for (idx, cell_id) in cell_ids.iter().enumerate() {
            let _ = db.execute(
                "UPDATE notebook_cells SET cell_order = ?1, updated_at = ?2 WHERE id = ?3 AND board_id = ?4",
                params![idx as i32, now, cell_id, &bid]
            );
        }
    }

    if !group_id.is_empty() {
        let event = NetworkEvent::NotebookCellsReordered {
            board_id: bid.clone(),
            cell_ids: cell_ids.clone(),
        };

        let _ = sys.network_tx.send(NetworkCommand::Broadcast {
            group_id,
            event,
        });
    }

    true
}

/// Get board mode (canvas, notebook, or notes)
#[unsafe(no_mangle)]
pub extern "C" fn cyan_get_board_mode(board_id: *const c_char) -> *mut c_char {
    let Some(sys) = SYSTEM.get() else {
        return CString::new("canvas").unwrap_or_default().into_raw();
    };

    let bid = unsafe { CStr::from_ptr(board_id) }.to_string_lossy().to_string();

    let mode: String = {
        let db = sys.db.lock_safe();
        let raw_mode: String = db.query_row(
            "SELECT COALESCE(board_mode, 'canvas') FROM objects WHERE id = ?1",
            params![bid],
            |row| row.get(0)
        ).unwrap_or_else(|_| "canvas".to_string());

        // Normalize legacy 'freeform' to 'canvas'
        if raw_mode == "freeform" {
            "canvas".to_string()
        } else {
            raw_mode
        }
    };

    CString::new(mode).unwrap_or_default().into_raw()
}

/// Set board mode (canvas, notebook, or notes)
#[unsafe(no_mangle)]
pub extern "C" fn cyan_set_board_mode(board_id: *const c_char, mode: *const c_char) -> bool {
    let Some(sys) = SYSTEM.get() else {
        return false;
    };

    let bid = unsafe { CStr::from_ptr(board_id) }.to_string_lossy().to_string();
    let mode_str = unsafe { CStr::from_ptr(mode) }.to_string_lossy().to_string();

    // Normalize legacy 'freeform' to 'canvas'
    let normalized_mode = if mode_str == "freeform" {
        "canvas".to_string()
    } else {
        mode_str.clone()
    };

    // Validate mode. Fix C: the board FACES are notebook / notes / dashboard / video
    // (BoardFace enum on iOS); "canvas" is the legacy whiteboard mode kept for back-compat.
    // "dashboard" + "video" were previously rejected here, so the app's
    // setActiveFace(.dashboard) returned false and the Run button could not flip the board to
    // the Dashboard face — it stayed on Workflow. Accept all real faces.
    if !matches!(
        normalized_mode.as_str(),
        "canvas" | "notebook" | "notes" | "dashboard" | "video"
    ) {
        tracing::warn!("Invalid board mode: {}", normalized_mode);
        return false;
    }

    let group_id: String;

    {
        let db = sys.db.lock_safe();

        group_id = db.query_row(
            "SELECT w.group_id FROM objects o
             JOIN workspaces w ON o.workspace_id = w.id
             WHERE o.id = ?1",
            params![&bid],
            |row| row.get(0)
        ).unwrap_or_default();

        if db.execute(
            "UPDATE objects SET board_mode = ?1 WHERE id = ?2",
            params![&normalized_mode, &bid]
        ).is_err() {
            return false;
        }
    }

    if !group_id.is_empty() {
        let event = NetworkEvent::BoardModeChanged {
            board_id: bid.clone(),
            mode: normalized_mode.clone(),
        };

        let _ = sys.network_tx.send(NetworkCommand::Broadcast {
            group_id,
            event,
        });
    }

    true
}

/// Load whiteboard elements for a specific cell (canvas cells)
#[unsafe(no_mangle)]
pub extern "C" fn cyan_load_cell_elements(cell_id: *const c_char) -> *mut c_char {
    let Some(sys) = SYSTEM.get() else {
        return CString::new("[]").unwrap_or_default().into_raw();
    };

    let cid = unsafe { CStr::from_ptr(cell_id) }.to_string_lossy().to_string();

    let elements: Vec<serde_json::Value> = {
        let db = sys.db.lock_safe();

        let mut stmt = match db.prepare(
            "SELECT id, board_id, element_type, x, y, width, height, z_index,
                    style_json, content_json, created_at, updated_at, cell_id
             FROM whiteboard_elements
             WHERE cell_id = ?1
             ORDER BY z_index ASC, created_at ASC"
        ) {
            Ok(s) => s,
            Err(_) => return CString::new("[]").unwrap_or_default().into_raw(),
        };

        stmt.query_map(params![cid], |row| {
            Ok(serde_json::json!({
                "id": row.get::<_, String>(0)?,
                "board_id": row.get::<_, String>(1)?,
                "element_type": row.get::<_, String>(2)?,
                "x": row.get::<_, f64>(3)?,
                "y": row.get::<_, f64>(4)?,
                "width": row.get::<_, f64>(5)?,
                "height": row.get::<_, f64>(6)?,
                "z_index": row.get::<_, i32>(7)?,
                "style_json": row.get::<_, Option<String>>(8)?,
                "content_json": row.get::<_, Option<String>>(9)?,
                "created_at": row.get::<_, i64>(10)?,
                "updated_at": row.get::<_, i64>(11)?,
                "cell_id": row.get::<_, Option<String>>(12)?
            }))
        }).map(|rows| rows.filter_map(|r| r.ok()).collect()).unwrap_or_default()
    };

    match serde_json::to_string(&elements) {
        Ok(json) => CString::new(json).unwrap_or_default().into_raw(),
        Err(_) => CString::new("[]").unwrap_or_default().into_raw(),
    }
}

// AI Bridge FFI Exports

fn ai_response_queue() -> &'static Mutex<VecDeque<String>> {
    AI_RESPONSE_QUEUE.get_or_init(|| Mutex::new(VecDeque::with_capacity(16)))
}

/// Handle AI commands via JSON
/// Commands: initialize, image_to_mermaid, ask_analyst, feed_event,
///           set_proactive, register_model, unload_model, infer_model, list_models
/// Returns immediately - poll cyan_poll_ai_response for result
#[unsafe(no_mangle)]
pub extern "C" fn cyan_ai_command(json: *const c_char) -> bool {
    let cmd_json = match unsafe { CStr::from_ptr(json) }.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => {
            if let Ok(mut q) = ai_response_queue().lock() {
                q.push_back(r#"{"success":false,"error":"Invalid UTF-8"}"#.to_string());
            }
            return false;
        }
    };

    let Some(sys) = SYSTEM.get() else {
        if let Ok(mut q) = ai_response_queue().lock() {
            q.push_back(r#"{"success":false,"error":"System not initialized"}"#.to_string());
        }
        return false;
    };

    let Some(runtime) = RUNTIME.get() else {
        if let Ok(mut q) = ai_response_queue().lock() {
            q.push_back(r#"{"success":false,"error":"Runtime not initialized"}"#.to_string());
        }
        return false;
    };

    // Spawn async task - returns immediately
    let bridge = Arc::clone(&sys.ai_bridge);
    runtime.spawn(async move {
        let result = bridge.handle_command(&cmd_json).await;
        eprintln!("🎯 [cyan_ai_command] Queuing response: {} chars", result.len());
        if let Ok(mut q) = ai_response_queue().lock() {
            q.push_back(result);
        }
    });

    true
}

/// Poll for AI command response
/// Returns JSON string or null if no response pending
#[unsafe(no_mangle)]
pub extern "C" fn cyan_poll_ai_response() -> *mut c_char {
    let Ok(mut queue) = ai_response_queue().lock() else {
        return std::ptr::null_mut();
    };

    match queue.pop_front() {
        Some(response) => {
            eprintln!("📤 [cyan_poll_ai_response] Returning: {} chars", response.len());
            CString::new(response)
                .map(|s| s.into_raw())
                .unwrap_or(std::ptr::null_mut())
        }
        None => std::ptr::null_mut(),
    }
}

/// Poll for AI proactive insights (for ConsoleView)
/// Returns JSON string or null if no insights pending
#[unsafe(no_mangle)]
pub extern "C" fn cyan_poll_ai_insights() -> *mut c_char {
    let Some(sys) = SYSTEM.get() else {
        return std::ptr::null_mut();
    };

    let Some(runtime) = RUNTIME.get() else {
        return std::ptr::null_mut();
    };

    match runtime.block_on(sys.ai_bridge.poll_insights()) {
        Some(insight) => match serde_json::to_string(&insight) {
            Ok(json) => CString::new(json).unwrap_or_default().into_raw(),
            Err(_) => std::ptr::null_mut(),
        },
        None => std::ptr::null_mut(),
    }
}
// ============== Board Metadata FFI ==============
// Add this before the final closing brace in lib.rs (after cyan_poll_ai_insights)

/// Get metadata for a single board
#[unsafe(no_mangle)]
pub extern "C" fn cyan_get_board_metadata(board_id: *const c_char) -> *mut c_char {
    let Some(sys) = SYSTEM.get() else {
        return std::ptr::null_mut();
    };

    let bid = unsafe { CStr::from_ptr(board_id) }.to_string_lossy().to_string();

    let metadata: Option<BoardMetadataDTO> = {
        let db = sys.db.lock_safe();

        db.query_row(
            "SELECT board_id, labels, rating, view_count, contains_model,
                    contains_skills, board_type, last_accessed, COALESCE(is_pinned, 0),
                    COALESCE(meta_updated_at, 0), COALESCE(pin_updated_at, 0)
             FROM board_metadata WHERE board_id = ?1",
            params![&bid],
            |row| {
                let labels_json: String = row.get(1)?;
                let skills_json: String = row.get(5)?;

                Ok(BoardMetadataDTO {
                    board_id: row.get(0)?,
                    labels: serde_json::from_str(&labels_json).unwrap_or_default(),
                    rating: row.get(2)?,
                    view_count: row.get(3)?,
                    contains_model: row.get(4)?,
                    contains_skills: serde_json::from_str(&skills_json).unwrap_or_default(),
                    board_type: row.get(6)?,
                    last_accessed: row.get(7)?,
                    is_pinned: row.get::<_, i32>(8)? != 0,
                    meta_updated_at: row.get(9)?,
                    pin_updated_at: row.get(10)?,
                })
            }
        ).ok()
    };

    let result = metadata.unwrap_or_else(|| {
        let db = sys.db.lock_safe();
        let board_type: String = db.query_row(
            "SELECT COALESCE(board_mode, 'canvas') FROM objects WHERE id = ?1",
            params![&bid],
            |row| row.get(0)
        ).unwrap_or_else(|_| "canvas".to_string());

        BoardMetadataDTO {
            board_id: bid,
            board_type,
            is_pinned: false,
            ..Default::default()
        }
    });

    match serde_json::to_string(&result) {
        Ok(json) => CString::new(json).unwrap_or_default().into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

/// Get metadata for all boards in a scope
/// scope_type: "workspace" | "group" | "all"
#[unsafe(no_mangle)]
pub extern "C" fn cyan_get_boards_metadata(scope_type: *const c_char, scope_id: *const c_char) -> *mut c_char {
    let Some(sys) = SYSTEM.get() else {
        return CString::new("[]").unwrap_or_default().into_raw();
    };

    let stype = unsafe { CStr::from_ptr(scope_type) }.to_string_lossy().to_string();
    let sid = unsafe { CStr::from_ptr(scope_id) }.to_string_lossy().to_string();

    let results: Vec<BoardMetadataDTO> = {
        let db = sys.db.lock_safe();

        let query = match stype.as_str() {
            "workspace" => {
                "SELECT o.id, COALESCE(m.labels, '[]'), COALESCE(m.rating, 0),
                        COALESCE(m.view_count, 0), m.contains_model,
                        COALESCE(m.contains_skills, '[]'), COALESCE(o.board_mode, 'canvas'),
                        COALESCE(m.last_accessed, 0), COALESCE(m.is_pinned, 0)
                 FROM objects o
                 LEFT JOIN board_metadata m ON o.id = m.board_id
                 WHERE o.workspace_id = ?1 AND o.type = 'whiteboard'
                 ORDER BY COALESCE(m.is_pinned, 0) DESC, COALESCE(m.rating, 0) DESC, o.created_at DESC"
            }
            "group" => {
                "SELECT o.id, COALESCE(m.labels, '[]'), COALESCE(m.rating, 0),
                        COALESCE(m.view_count, 0), m.contains_model,
                        COALESCE(m.contains_skills, '[]'), COALESCE(o.board_mode, 'canvas'),
                        COALESCE(m.last_accessed, 0), COALESCE(m.is_pinned, 0)
                 FROM objects o
                 JOIN workspaces w ON o.workspace_id = w.id
                 LEFT JOIN board_metadata m ON o.id = m.board_id
                 WHERE w.group_id = ?1 AND o.type = 'whiteboard'
                 ORDER BY COALESCE(m.is_pinned, 0) DESC, COALESCE(m.rating, 0) DESC, o.created_at DESC"
            }
            _ => {
                "SELECT o.id, COALESCE(m.labels, '[]'), COALESCE(m.rating, 0),
                        COALESCE(m.view_count, 0), m.contains_model,
                        COALESCE(m.contains_skills, '[]'), COALESCE(o.board_mode, 'canvas'),
                        COALESCE(m.last_accessed, 0), COALESCE(m.is_pinned, 0)
                 FROM objects o
                 LEFT JOIN board_metadata m ON o.id = m.board_id
                 WHERE o.type = 'whiteboard'
                 ORDER BY COALESCE(m.is_pinned, 0) DESC, COALESCE(m.rating, 0) DESC, o.created_at DESC
                 LIMIT 100"
            }
        };

        let mut stmt = match db.prepare(query) {
            Ok(s) => s,
            Err(_) => return CString::new("[]").unwrap_or_default().into_raw(),
        };

        let param = if stype == "all" { "" } else { &sid };

        stmt.query_map(params![param], |row| {
            let labels_json: String = row.get(1)?;
            let skills_json: String = row.get(5)?;

            Ok(BoardMetadataDTO {
                board_id: row.get(0)?,
                labels: serde_json::from_str(&labels_json).unwrap_or_default(),
                rating: row.get(2)?,
                view_count: row.get(3)?,
                contains_model: row.get(4)?,
                contains_skills: serde_json::from_str(&skills_json).unwrap_or_default(),
                board_type: row.get(6)?,
                last_accessed: row.get(7)?,
                is_pinned: row.get::<_, i32>(8)? != 0,
                // UI list read — the LWW clocks aren't surfaced here (sync uses the
                // board_metadata_list_by_boards path which carries them).
                meta_updated_at: 0,
                pin_updated_at: 0,
            })
        }).map(|rows| rows.filter_map(|r| r.ok()).collect()).unwrap_or_default()
    };

    match serde_json::to_string(&results) {
        Ok(json) => CString::new(json).unwrap_or_default().into_raw(),
        Err(_) => CString::new("[]").unwrap_or_default().into_raw(),
    }
}

/// Get top N boards by rating for a group
#[unsafe(no_mangle)]
pub extern "C" fn cyan_get_top_boards(group_id: *const c_char, limit: i32) -> *mut c_char {
    let Some(sys) = SYSTEM.get() else {
        return CString::new("[]").unwrap_or_default().into_raw();
    };

    let gid = unsafe { CStr::from_ptr(group_id) }.to_string_lossy().to_string();
    let lim = if limit <= 0 { 10 } else { limit.min(50) };

    let results: Vec<serde_json::Value> = {
        let db = sys.db.lock_safe();

        let mut stmt = match db.prepare(
            "SELECT o.id, o.name, o.workspace_id, w.name as workspace_name,
                    COALESCE(m.labels, '[]'), COALESCE(m.rating, 0),
                    COALESCE(o.board_mode, 'canvas'), m.contains_model
             FROM objects o
             JOIN workspaces w ON o.workspace_id = w.id
             LEFT JOIN board_metadata m ON o.id = m.board_id
             WHERE w.group_id = ?1 AND o.type = 'whiteboard'
             ORDER BY COALESCE(m.rating, 0) DESC, COALESCE(m.view_count, 0) DESC
             LIMIT ?2"
        ) {
            Ok(s) => s,
            Err(_) => return CString::new("[]").unwrap_or_default().into_raw(),
        };

        stmt.query_map(params![&gid, lim], |row| {
            let labels_json: String = row.get(4)?;
            let labels: Vec<String> = serde_json::from_str(&labels_json).unwrap_or_default();

            Ok(serde_json::json!({
                "id": row.get::<_, String>(0)?,
                "name": row.get::<_, String>(1)?,
                "workspace_id": row.get::<_, String>(2)?,
                "workspace_name": row.get::<_, String>(3)?,
                "labels": labels,
                "rating": row.get::<_, i32>(5)?,
                "board_type": row.get::<_, String>(6)?,
                "contains_model": row.get::<_, Option<String>>(7)?
            }))
        }).map(|rows| rows.filter_map(|r| r.ok()).collect()).unwrap_or_default()
    };

    match serde_json::to_string(&results) {
        Ok(json) => CString::new(json).unwrap_or_default().into_raw(),
        Err(_) => CString::new("[]").unwrap_or_default().into_raw(),
    }
}

/// Set labels for a board
#[unsafe(no_mangle)]
pub extern "C" fn cyan_set_board_labels(board_id: *const c_char, labels_json: *const c_char) -> bool {
    let Some(sys) = SYSTEM.get() else {
        return false;
    };

    let bid = unsafe { CStr::from_ptr(board_id) }.to_string_lossy().to_string();
    let json_str = unsafe { CStr::from_ptr(labels_json) }.to_string_lossy().to_string();

    let labels: Vec<String> = match serde_json::from_str(&json_str) {
        Ok(l) => l,
        Err(_) => return false,
    };

    let group_id: String;

    {
        let db = sys.db.lock_safe();

        group_id = db.query_row(
            "SELECT w.group_id FROM objects o
             JOIN workspaces w ON o.workspace_id = w.id
             WHERE o.id = ?1",
            params![&bid],
            |row| row.get(0)
        ).unwrap_or_default();

        // Upsert metadata
        if db.execute(
            "INSERT INTO board_metadata (board_id, labels) VALUES (?1, ?2)
             ON CONFLICT(board_id) DO UPDATE SET labels = ?2",
            params![&bid, &json_str]
        ).is_err() {
            return false;
        }
    }

    // Broadcast
    if !group_id.is_empty() {
        let event = NetworkEvent::BoardLabelsUpdated {
            board_id: bid,
            labels,
        };

        let _ = sys.network_tx.send(NetworkCommand::Broadcast {
            group_id,
            event,
        });
    }

    true
}

/// Add a single label to a board
#[unsafe(no_mangle)]
pub extern "C" fn cyan_add_board_label(board_id: *const c_char, label: *const c_char) -> bool {
    let Some(sys) = SYSTEM.get() else {
        return false;
    };

    let bid = unsafe { CStr::from_ptr(board_id) }.to_string_lossy().to_string();
    let new_label = unsafe { CStr::from_ptr(label) }.to_string_lossy().to_string();

    let group_id: String;
    let updated_labels: Vec<String>;

    {
        let db = sys.db.lock_safe();

        group_id = db.query_row(
            "SELECT w.group_id FROM objects o
             JOIN workspaces w ON o.workspace_id = w.id
             WHERE o.id = ?1",
            params![&bid],
            |row| row.get(0)
        ).unwrap_or_default();

        // Get existing labels
        let existing: String = db.query_row(
            "SELECT COALESCE(labels, '[]') FROM board_metadata WHERE board_id = ?1",
            params![&bid],
            |row| row.get(0)
        ).unwrap_or_else(|_| "[]".to_string());

        let mut labels: Vec<String> = serde_json::from_str(&existing).unwrap_or_default();

        // Add if not exists
        if !labels.contains(&new_label) {
            labels.push(new_label);
        }

        updated_labels = labels.clone();
        let labels_json = serde_json::to_string(&labels).unwrap_or_else(|_| "[]".to_string());

        // Upsert
        if db.execute(
            "INSERT INTO board_metadata (board_id, labels) VALUES (?1, ?2)
             ON CONFLICT(board_id) DO UPDATE SET labels = ?2",
            params![&bid, &labels_json]
        ).is_err() {
            return false;
        }
    }

    if !group_id.is_empty() {
        let _ = sys.network_tx.send(NetworkCommand::Broadcast {
            group_id,
            event: NetworkEvent::BoardLabelsUpdated {
                board_id: bid,
                labels: updated_labels,
            },
        });
    }

    true
}

/// Remove a label from a board
#[unsafe(no_mangle)]
pub extern "C" fn cyan_remove_board_label(board_id: *const c_char, label: *const c_char) -> bool {
    let Some(sys) = SYSTEM.get() else {
        return false;
    };

    let bid = unsafe { CStr::from_ptr(board_id) }.to_string_lossy().to_string();
    let remove_label = unsafe { CStr::from_ptr(label) }.to_string_lossy().to_string();

    let group_id: String;
    let updated_labels: Vec<String>;

    {
        let db = sys.db.lock_safe();

        group_id = db.query_row(
            "SELECT w.group_id FROM objects o
             JOIN workspaces w ON o.workspace_id = w.id
             WHERE o.id = ?1",
            params![&bid],
            |row| row.get(0)
        ).unwrap_or_default();

        let existing: String = db.query_row(
            "SELECT COALESCE(labels, '[]') FROM board_metadata WHERE board_id = ?1",
            params![&bid],
            |row| row.get(0)
        ).unwrap_or_else(|_| "[]".to_string());

        let mut labels: Vec<String> = serde_json::from_str(&existing).unwrap_or_default();
        labels.retain(|l| l != &remove_label);

        updated_labels = labels.clone();
        let labels_json = serde_json::to_string(&labels).unwrap_or_else(|_| "[]".to_string());

        let _ = db.execute(
            "UPDATE board_metadata SET labels = ?1 WHERE board_id = ?2",
            params![&labels_json, &bid]
        );
    }

    if !group_id.is_empty() {
        let _ = sys.network_tx.send(NetworkCommand::Broadcast {
            group_id,
            event: NetworkEvent::BoardLabelsUpdated {
                board_id: bid,
                labels: updated_labels,
            },
        });
    }

    true
}

/// Rate a board (0-5)
#[unsafe(no_mangle)]
pub extern "C" fn cyan_rate_board(board_id: *const c_char, rating: i32) -> bool {
    let Some(sys) = SYSTEM.get() else {
        return false;
    };

    let bid = unsafe { CStr::from_ptr(board_id) }.to_string_lossy().to_string();
    let clamped_rating = rating.clamp(0, 5);

    let group_id: String;

    {
        let db = sys.db.lock_safe();

        group_id = db.query_row(
            "SELECT w.group_id FROM objects o
             JOIN workspaces w ON o.workspace_id = w.id
             WHERE o.id = ?1",
            params![&bid],
            |row| row.get(0)
        ).unwrap_or_default();

        if db.execute(
            "INSERT INTO board_metadata (board_id, rating) VALUES (?1, ?2)
             ON CONFLICT(board_id) DO UPDATE SET rating = ?2",
            params![&bid, clamped_rating]
        ).is_err() {
            return false;
        }
    }

    if !group_id.is_empty() {
        let _ = sys.network_tx.send(NetworkCommand::Broadcast {
            group_id,
            event: NetworkEvent::BoardRated {
                board_id: bid,
                rating: clamped_rating,
            },
        });
    }

    true
}

/// Increment view count and update last_accessed
#[unsafe(no_mangle)]
pub extern "C" fn cyan_record_board_view(board_id: *const c_char) -> bool {
    let Some(sys) = SYSTEM.get() else {
        return false;
    };

    let bid = unsafe { CStr::from_ptr(board_id) }.to_string_lossy().to_string();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    let db = sys.db.lock_safe();

    db.execute(
        "INSERT INTO board_metadata (board_id, view_count, last_accessed) VALUES (?1, 1, ?2)
         ON CONFLICT(board_id) DO UPDATE SET view_count = view_count + 1, last_accessed = ?2",
        params![&bid, now]
    ).is_ok()
}

/// Set model info for a board (called when notebook has model cell)
#[unsafe(no_mangle)]
pub extern "C" fn cyan_set_board_model(board_id: *const c_char, model_name: *const c_char) -> bool {
    let Some(sys) = SYSTEM.get() else {
        return false;
    };

    let bid = unsafe { CStr::from_ptr(board_id) }.to_string_lossy().to_string();
    let model = if model_name.is_null() {
        None
    } else {
        let m = unsafe { CStr::from_ptr(model_name) }.to_string_lossy().to_string();
        if m.is_empty() { None } else { Some(m) }
    };

    let db = sys.db.lock_safe();

    db.execute(
        "INSERT INTO board_metadata (board_id, contains_model) VALUES (?1, ?2)
         ON CONFLICT(board_id) DO UPDATE SET contains_model = ?2",
        params![&bid, &model]
    ).is_ok()
}

/// Set skills for a board
#[unsafe(no_mangle)]
pub extern "C" fn cyan_set_board_skills(board_id: *const c_char, skills_json: *const c_char) -> bool {
    let Some(sys) = SYSTEM.get() else {
        return false;
    };

    let bid = unsafe { CStr::from_ptr(board_id) }.to_string_lossy().to_string();
    let json_str = unsafe { CStr::from_ptr(skills_json) }.to_string_lossy().to_string();

    // Validate JSON
    if serde_json::from_str::<Vec<String>>(&json_str).is_err() {
        return false;
    }

    let db = sys.db.lock_safe();

    db.execute(
        "INSERT INTO board_metadata (board_id, contains_skills) VALUES (?1, ?2)
         ON CONFLICT(board_id) DO UPDATE SET contains_skills = ?2",
        params![&bid, &json_str]
    ).is_ok()
}

/// Generate deep link URL for a board
#[unsafe(no_mangle)]
pub extern "C" fn cyan_get_board_link(board_id: *const c_char) -> *mut c_char {
    let Some(sys) = SYSTEM.get() else {
        return std::ptr::null_mut();
    };

    let bid = unsafe { CStr::from_ptr(board_id) }.to_string_lossy().to_string();

    let link: Option<String> = {
        let db = sys.db.lock_safe();

        db.query_row(
            "SELECT w.group_id, o.workspace_id
             FROM objects o
             JOIN workspaces w ON o.workspace_id = w.id
             WHERE o.id = ?1",
            params![&bid],
            |row| {
                let group_id: String = row.get(0)?;
                let workspace_id: String = row.get(1)?;
                Ok(format!("cyan://group/{}/workspace/{}/board/{}", group_id, workspace_id, bid))
            }
        ).ok()
    };

    match link {
        Some(url) => CString::new(url).unwrap_or_default().into_raw(),
        None => CString::new(format!("cyan://board/{}", bid)).unwrap_or_default().into_raw(),
    }
}

/// Search boards by label
#[unsafe(no_mangle)]
pub extern "C" fn cyan_search_boards_by_label(label: *const c_char) -> *mut c_char {
    let Some(sys) = SYSTEM.get() else {
        return CString::new("[]").unwrap_or_default().into_raw();
    };

    let search_label = unsafe { CStr::from_ptr(label) }.to_string_lossy().to_string();
    let pattern = format!("%\"{}%", search_label); // JSON contains pattern

    let results: Vec<serde_json::Value> = {
        let db = sys.db.lock_safe();

        let mut stmt = match db.prepare(
            "SELECT o.id, o.name, o.workspace_id, w.name, w.group_id,
                    COALESCE(m.labels, '[]'), COALESCE(m.rating, 0)
             FROM board_metadata m
             JOIN objects o ON m.board_id = o.id
             JOIN workspaces w ON o.workspace_id = w.id
             WHERE m.labels LIKE ?1
             ORDER BY m.rating DESC
             LIMIT 50"
        ) {
            Ok(s) => s,
            Err(_) => return CString::new("[]").unwrap_or_default().into_raw(),
        };

        stmt.query_map(params![&pattern], |row| {
            let labels_json: String = row.get(5)?;

            Ok(serde_json::json!({
                "id": row.get::<_, String>(0)?,
                "name": row.get::<_, String>(1)?,
                "workspace_id": row.get::<_, String>(2)?,
                "workspace_name": row.get::<_, String>(3)?,
                "group_id": row.get::<_, String>(4)?,
                "labels": serde_json::from_str::<Vec<String>>(&labels_json).unwrap_or_default(),
                "rating": row.get::<_, i32>(6)?,
                "link": format!("cyan://group/{}/workspace/{}/board/{}",
                    row.get::<_, String>(4)?, row.get::<_, String>(2)?, row.get::<_, String>(0)?)
            }))
        }).map(|rows| rows.filter_map(|r| r.ok()).collect()).unwrap_or_default()
    };

    match serde_json::to_string(&results) {
        Ok(json) => CString::new(json).unwrap_or_default().into_raw(),
        Err(_) => CString::new("[]").unwrap_or_default().into_raw(),
    }
}

// ============================================================
// BOARD PINNING FFI FUNCTIONS
// ============================================================

/// Pin a board (show at top of grid).
///
/// R12 C1/C2: pin is a SYNCED, convergent board property. Route through the command
/// actor (`SetBoardPinned`) so it (a) stamps a real `pin_updated_at` clock for per-board
/// LWW merge and (b) gossips `BoardPinned` to peers — the previous direct-SQL write set
/// no clock (LWW could never converge) and never left the device (pin/unpin invisible on
/// the other peer until a full group re-fetch). Returns whether the command was enqueued.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_pin_board(board_id: *const c_char) -> bool {
    set_board_pinned_via_command(board_id, true)
}

/// Unpin a board. See [`cyan_pin_board`] — same convergent command path (R12 C1/C2).
#[unsafe(no_mangle)]
pub extern "C" fn cyan_unpin_board(board_id: *const c_char) -> bool {
    set_board_pinned_via_command(board_id, false)
}

/// Shared helper for [`cyan_pin_board`]/[`cyan_unpin_board`]: enqueue the convergent
/// `SetBoardPinned` command (LWW clock + `BoardPinned` gossip + `BoardMetadataUpdated`
/// UI signal, all handled in the command actor).
fn set_board_pinned_via_command(board_id: *const c_char, is_pinned: bool) -> bool {
    let Some(board_id) = (unsafe { cstr_arg(board_id) }) else {
        return false;
    };
    let sys = match SYSTEM.get() {
        Some(s) => s.clone(),
        None => return false,
    };
    sys.command_tx
        .send(CommandMsg::SetBoardPinned { board_id, is_pinned })
        .is_ok()
}

/// Check if a board is pinned
#[unsafe(no_mangle)]
pub extern "C" fn cyan_is_board_pinned(board_id: *const c_char) -> bool {
    let Some(sys) = SYSTEM.get() else {
        return false;
    };

    let bid = unsafe { CStr::from_ptr(board_id) }.to_string_lossy().to_string();

    let db = sys.db.lock_safe();
    db.query_row(
        "SELECT COALESCE(is_pinned, 0) FROM board_metadata WHERE board_id = ?1",
        params![bid],
        |row| row.get::<_, i32>(0),
    )
        .unwrap_or(0) != 0
}

/// R12 D2/E1: read a board's workflow lifecycle state as JSON so iOS can gate the board face.
/// Shape: `{"board_id","deployed","dashboard_available","locked","updated_at"}`. A board with
/// no deployment returns the default authoring state (deployed=false, locked=false). Additive,
/// read-only — the unlock approval (org grant) is engine-side (`workflow::request_unlock`).
#[unsafe(no_mangle)]
pub extern "C" fn cyan_board_workflow_state(board_id: *const c_char) -> *mut c_char {
    let Some(bid) = (unsafe { cstr_arg(board_id) }) else {
        return std::ptr::null_mut();
    };
    let state = crate::storage::workflow_state_get(&bid);
    let json = serde_json::to_string(&state).unwrap_or_else(|_| "{}".to_string());
    match CString::new(json) {
        Ok(s) => s.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

// ============================================================
// USER PROFILE FFI FUNCTIONS
// ============================================================

/// Get user profile by node_id
/// Returns JSON: {"node_id": "...", "display_name": "...", "avatar_hash": "...", "status": "...", "last_seen": 123}
#[unsafe(no_mangle)]
pub extern "C" fn cyan_get_user_profile(node_id: *const c_char) -> *mut c_char {
    let Some(nid) = (unsafe { cstr_arg(node_id) }) else {
        return std::ptr::null_mut();
    };

    let Some(sys) = SYSTEM.get() else {
        return std::ptr::null_mut();
    };

    let profile: Option<serde_json::Value> = {
        let db = sys.db.lock_safe();
        db.query_row(
            "SELECT node_id, display_name, avatar_hash, status, last_seen, updated_at
             FROM user_profiles WHERE node_id = ?1",
            params![nid],
            |row| {
                Ok(serde_json::json!({
                    "node_id": row.get::<_, String>(0)?,
                    "display_name": row.get::<_, Option<String>>(1)?,
                    "avatar_hash": row.get::<_, Option<String>>(2)?,
                    "status": row.get::<_, String>(3)?,
                    "last_seen": row.get::<_, Option<i64>>(4)?,
                    "updated_at": row.get::<_, Option<i64>>(5)?
                }))
            }
        ).optional().unwrap_or(None)
    };

    match profile {
        Some(p) => CString::new(p.to_string()).unwrap_or_default().into_raw(),
        None => {
            let fallback = serde_json::json!({
                "node_id": nid,
                "display_name": null,
                "avatar_hash": null,
                "status": "unknown",
                "last_seen": null
            });
            CString::new(fallback.to_string()).unwrap_or_default().into_raw()
        }
    }
}

/// Get multiple user profiles at once (batch lookup)
/// Input: JSON array of node_ids ["id1", "id2", ...]
/// Returns: JSON object {"id1": {...}, "id2": {...}, ...}
#[unsafe(no_mangle)]
pub extern "C" fn cyan_get_profiles_batch(node_ids_json: *const c_char) -> *mut c_char {
    let Some(json_str) = (unsafe { cstr_arg(node_ids_json) }) else {
        return CString::new("{}").unwrap_or_default().into_raw();
    };

    let node_ids: Vec<String> = match serde_json::from_str(&json_str) {
        Ok(ids) => ids,
        Err(_) => return CString::new("{}").unwrap_or_default().into_raw(),
    };

    let Some(sys) = SYSTEM.get() else {
        return CString::new("{}").unwrap_or_default().into_raw();
    };

    let mut result = serde_json::Map::new();

    {
        let db = sys.db.lock_safe();

        for nid in &node_ids {
            let profile: Option<serde_json::Value> = db.query_row(
                "SELECT node_id, display_name, avatar_hash, status, last_seen
                 FROM user_profiles WHERE node_id = ?1",
                params![nid],
                |row| {
                    Ok(serde_json::json!({
                        "node_id": row.get::<_, String>(0)?,
                        "display_name": row.get::<_, Option<String>>(1)?,
                        "avatar_hash": row.get::<_, Option<String>>(2)?,
                        "status": row.get::<_, String>(3)?,
                        "last_seen": row.get::<_, Option<i64>>(4)?
                    }))
                }
            ).optional().unwrap_or(None);

            if let Some(p) = profile {
                result.insert(nid.clone(), p);
            } else {
                result.insert(nid.clone(), serde_json::json!({
                    "node_id": nid,
                    "display_name": null,
                    "status": "unknown"
                }));
            }
        }
    }

    CString::new(serde_json::Value::Object(result).to_string()).unwrap_or_default().into_raw()
}

/// Set my profile (display name and optional avatar)
/// avatar_path can be null - if provided, file is hashed and stored in blobs
/// Broadcasts ProfileUpdated to all groups I'm a member of
#[unsafe(no_mangle)]
pub extern "C" fn cyan_set_my_profile(
    display_name: *const c_char,
    avatar_path: *const c_char
) -> bool {
    let Some(name) = (unsafe { cstr_arg(display_name) }) else {
        return false;
    };

    let avatar_path_opt = unsafe { cstr_arg(avatar_path) };

    let Some(sys) = SYSTEM.get() else {
        return false;
    };

    let node_id = sys.node_id.clone();
    let now = chrono::Utc::now().timestamp();

    // Handle avatar if provided
    let avatar_hash: Option<String> = if let Some(path) = avatar_path_opt {
        match std::fs::read(&path) {
            Ok(data) => {
                let hash = blake3::hash(&data).to_hex().to_string();
                if let Some(data_dir) = DATA_DIR.get() {
                    let blobs_dir = data_dir.join("blobs");
                    let _ = std::fs::create_dir_all(&blobs_dir);
                    let blob_path = blobs_dir.join(&hash);
                    let _ = std::fs::write(&blob_path, &data);
                }
                Some(hash)
            }
            Err(_) => None,
        }
    } else {
        let db = sys.db.lock_safe();
        db.query_row(
            "SELECT avatar_hash FROM user_profiles WHERE node_id = ?1",
            params![&node_id],
            |row| row.get(0)
        ).ok()
    };

    // Upsert profile
    {
        let db = sys.db.lock_safe();
        let _ = db.execute(
            "INSERT INTO user_profiles (node_id, display_name, avatar_hash, status, updated_at)
             VALUES (?1, ?2, ?3, 'online', ?4)
             ON CONFLICT(node_id) DO UPDATE SET
                display_name = excluded.display_name,
                avatar_hash = COALESCE(excluded.avatar_hash, user_profiles.avatar_hash),
                status = 'online',
                updated_at = excluded.updated_at",
            params![&node_id, &name, &avatar_hash, now],
        );
    }

    // Broadcast to all groups
    let group_ids: Vec<String> = (|| -> rusqlite::Result<Vec<String>> {
        let db = sys.db.lock_safe();
        let mut stmt = db.prepare("SELECT id FROM groups")?;
        Ok(stmt.query_map([], |row| row.get(0))?
            .filter_map(|r| r.ok())
            .collect())
    })().unwrap_or_default();

    let evt = NetworkEvent::ProfileUpdated {
        node_id: node_id.clone(),
        display_name: name.clone(),
        avatar_hash: avatar_hash.clone(),
    };

    for gid in group_ids {
        let _ = sys.network_tx.send(NetworkCommand::Broadcast {
            group_id: gid,
            event: evt.clone(),
        });
    }

    let _ = sys.event_tx.send(SwiftEvent::Network(evt));

    true
}

/// Get my own node ID (the Iroh public key)
#[unsafe(no_mangle)]
pub extern "C" fn cyan_get_my_node_id() -> *mut c_char {
    let Some(sys) = SYSTEM.get() else {
        return std::ptr::null_mut();
    };

    CString::new(sys.node_id.clone()).unwrap_or_default().into_raw()
}

/// Get my own profile
#[unsafe(no_mangle)]
pub extern "C" fn cyan_get_my_profile() -> *mut c_char {
    let Some(sys) = SYSTEM.get() else {
        return std::ptr::null_mut();
    };

    let node_id = sys.node_id.clone();

    let profile: Option<serde_json::Value> = {
        let db = sys.db.lock_safe();
        db.query_row(
            "SELECT node_id, display_name, avatar_hash, status, last_seen, updated_at
             FROM user_profiles WHERE node_id = ?1",
            params![&node_id],
            |row| {
                Ok(serde_json::json!({
                    "node_id": row.get::<_, String>(0)?,
                    "display_name": row.get::<_, Option<String>>(1)?,
                    "avatar_hash": row.get::<_, Option<String>>(2)?,
                    "status": row.get::<_, String>(3)?,
                    "last_seen": row.get::<_, Option<i64>>(4)?,
                    "updated_at": row.get::<_, Option<i64>>(5)?
                }))
            }
        ).optional().unwrap_or(None)
    };

    match profile {
        Some(p) => CString::new(p.to_string()).unwrap_or_default().into_raw(),
        None => {
            let fallback = serde_json::json!({
                "node_id": node_id,
                "display_name": null,
                "avatar_hash": null,
                "status": "online",
                "last_seen": null
            });
            CString::new(fallback.to_string()).unwrap_or_default().into_raw()
        }
    }
}

/// Update a peer's status (called when gossip events occur)
#[unsafe(no_mangle)]
pub extern "C" fn cyan_update_peer_status(node_id: *const c_char, status: *const c_char) -> bool {
    let Some(nid) = (unsafe { cstr_arg(node_id) }) else {
        return false;
    };
    let Some(stat) = (unsafe { cstr_arg(status) }) else {
        return false;
    };

    let Some(sys) = SYSTEM.get() else {
        return false;
    };

    let now = chrono::Utc::now().timestamp();

    let db = sys.db.lock_safe();
    let result = db.execute(
        "INSERT INTO user_profiles (node_id, status, last_seen, updated_at)
         VALUES (?1, ?2, ?3, ?3)
         ON CONFLICT(node_id) DO UPDATE SET
            status = excluded.status,
            last_seen = excluded.last_seen,
            updated_at = excluded.updated_at",
        params![nid, stat, now],
    );

    result.is_ok()
}


// ============================================================
// GROUP INVITE FFI FUNCTIONS
// ============================================================


/// Join a group from invite JSON
/// This creates the group locally and subscribes to its gossip topic
/// Input: Invite JSON from QR code (same format as xaero_parse_group_invite)
/// Output: {"success": true, "group_id": "...", "group_name": "..."} or {"success": false, "error": "..."}
#[unsafe(no_mangle)]
pub extern "C" fn xaero_join_group_from_invite(invite_json: *const c_char) -> *mut c_char {
    println!("🔵 [SYNC-1] xaero_join_group_from_invite called");
    let Some(json_str) = (unsafe { cstr_arg(invite_json) }) else {
        return json_result_ptr(false, None, None, Some("Invalid invite data"));
    };
    println!("🔵 [SYNC-2] Invite JSON: {}", json_str);

    // Parse the invite JSON
    let invite: serde_json::Value = match serde_json::from_str(&json_str) {
        Ok(v) => v,
        Err(e) => return json_result_ptr(false, None, None, Some(&format!("Parse error: {}", e))),
    };

    join_from_invite(&invite)
}

/// Shared join-from-invite worker, used by `xaero_join_group_from_invite` and the
/// signed-grant `cyan_scan_grant_qr`. Behavior is identical to the original extern
/// fn body — extracting a seam, not a rewrite. The optional `grant` field (signed
/// capability-grant QR payload) is forwarded to the snapshot holder for the
/// per-group snapshot read gate (unchanged from before).
fn join_from_invite(invite: &serde_json::Value) -> *mut c_char {
    // Extract required fields
    let group_id = match invite.get("group_id").and_then(|v| v.as_str()) {
        Some(id) => id.to_string(),
        None => return json_result_ptr(false, None, None, Some("Missing group_id")),
    };

    let group_name = match invite.get("group_name").and_then(|v| v.as_str()) {
        Some(name) => name.to_string(),
        None => return json_result_ptr(false, None, None, Some("Missing group_name")),
    };

    // Optional fields with defaults
    let group_icon = invite
        .get("group_icon")
        .and_then(|v| v.as_str())
        .unwrap_or("folder.fill")
        .to_string();

    let group_color = invite
        .get("group_color")
        .and_then(|v| v.as_str())
        .unwrap_or("#00AEEF")
        .to_string();

    let inviter_node_id = invite
        .get("inviter_node_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    // Inviter's full resolvable address (MESH_HARDENING §2.2): a serialized `EndpointAddr` the QR
    // optionally carries. When present we seed it into the group topic on join (below) so the joiner
    // dials the inviter directly with no relay/bootstrap/mDNS — air-gapped first-join forms a mesh.
    let inviter_addr = invite
        .get("inviter_addr")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    // Optional signed capability-grant QR payload (additive). When present, it is forwarded
    // with the JoinGroup so the snapshot holder can authorize the per-group snapshot read for
    // an enforced group. Absent ⇒ unchanged behavior (fail-open / un-enforced groups).
    let grant = invite
        .get("grant")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    // Get system
    let sys = match SYSTEM.get() {
        Some(s) => s,
        None => return json_result_ptr(false, None, None, Some("System not initialized")),
    };

    // Check if group already exists
    let exists: bool = {
        let db = sys.db.lock_safe();
        db.query_row(
            "SELECT 1 FROM groups WHERE id = ?1",
            params![&group_id],
            |_| Ok(true),
        )
            .unwrap_or(false)
    };

    if exists {
        // Already a member - just return success
        return json_result_ptr(true, Some(&group_id), Some(&group_name), None);
    }

    // Insert group into database
    let now = chrono::Utc::now().timestamp();
    {
        let db = sys.db.lock_safe();
        if let Err(e) = db.execute(
            "INSERT INTO groups (id, name, icon, color, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![&group_id, &group_name, &group_icon, &group_color, now],
        ) {
            return json_result_ptr(false, None, None, Some(&format!("Database error: {}", e)));
        }
    }

    // Subscribe to group's gossip topic with inviter as bootstrap peer
    println!("🔵 [SYNC-3] Sending JoinGroup command for: {}", group_id);
    if let Some(ref inv_id) = inviter_node_id {
        println!("🔵 [SYNC-3a] Using inviter node ID: {}", &inv_id[..16.min(inv_id.len())]);
    }
    let _ = sys.network_tx.send(NetworkCommand::JoinGroup {
        group_id: group_id.clone(),
        bootstrap_peer: inviter_node_id,
        grant,
    });
    println!("🔵 [SYNC-4] JoinGroup command sent");

    // Seed the inviter's full address into the group topic (MESH_HARDENING §2.2). Additive: only
    // when the QR carried `inviter_addr`. This is the ONE seeding pipeline (engine `SeedGroupPeer`)
    // — it makes the inviter resolvable and routes it into the topic so `NeighborUp` fires on first
    // join with no relay/bootstrap reachable. Absent ⇒ unchanged (dial inviter_node_id via discovery).
    if let Some(addr_json) = inviter_addr {
        let _ = sys.network_tx.send(NetworkCommand::SeedGroupPeer {
            group_id: group_id.clone(),
            addr_json,
        });
    }

    // Emit event for UI refresh
    let group = Group {
        id: group_id.clone(),
        name: group_name.clone(),
        icon: group_icon,
        color: group_color,
        created_at: now,
    };
    println!("🔵 [SYNC-5] Emitting GroupCreated event for: {}", group_name);
    let _ = sys.event_tx.send(SwiftEvent::Network(NetworkEvent::GroupCreated(group)));

    println!("🔵 [SYNC-6] xaero_join_group_from_invite returning success");
    tracing::info!("✅ Joined group from invite: {} ({})", group_name, group_id);

    json_result_ptr(true, Some(&group_id), Some(&group_name), None)
}

// Helper function for error responses
#[allow(dead_code)] // pre-existing FFI error helper kept for future call sites
fn json_error_ptr(msg: &str) -> *mut c_char {
    let result = serde_json::json!({
        "error": msg
    });
    CString::new(result.to_string()).unwrap_or_default().into_raw()
}

// Helper function for join result responses
fn json_result_ptr(success: bool, group_id: Option<&str>, group_name: Option<&str>, error: Option<&str>) -> *mut c_char {
    let result = if success {
        serde_json::json!({
            "success": true,
            "group_id": group_id,
            "group_name": group_name
        })
    } else {
        serde_json::json!({
            "success": false,
            "error": error.unwrap_or("Unknown error")
        })
    };
    CString::new(result.to_string()).unwrap_or_default().into_raw()
}

// ============================================================================
// Signed-grant QR FFI (the mesh-half capability grant, surfaced to iOS)
// ============================================================================
//
// ADDITIVE client verbs over the existing `identity::Grant` primitive (signed,
// expiring, revocable — STATUS_IDENTITY_GRANTS). They light up the app's
// `issueGrantQR` / `scanGrantQR` seam (cyan-iOS STATUS_IOS_LOGIN_PRESENCE), which
// returned "unavailable" until this verb shipped. The signed grant is the only
// secret here (it is not a credential); nothing sensitive is logged.

/// Issue a signed, role-carrying grant QR for a group — **Admin/Owner only**.
/// iOS: `issueGrantQR`. Returns `{"success":true,"qr":"<payload>","nonce":...,
/// "expiry":...,"role":...}` or `{"success":false,"error":...}`. The QR encodes a
/// `GrantInvite` (signed grant + group identity + this node as bootstrap peer) that
/// `cyan_scan_grant_qr` joins from.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_issue_grant_qr(
    group_id: *const c_char,
    role: *const c_char,
    ttl_seconds: u64,
) -> *mut c_char {
    use crate::identity::{issue_grant_qr, pubkey_hex, GroupRoster, Role};

    let Some(gid) = (unsafe { cstr_arg(group_id) }) else {
        return json_result_ptr(false, None, None, Some("Invalid group_id"));
    };
    let role_str = (unsafe { cstr_arg(role) }).unwrap_or_else(|| "member".into());
    let Some(role) = Role::parse(&role_str) else {
        return json_result_ptr(false, None, None, Some("Unknown role (owner|admin|member|viewer|guest)"));
    };
    let Some(sys) = SYSTEM.get() else {
        return json_result_ptr(false, None, None, Some("System not initialized"));
    };

    // Authority gate: only the group's Owner/Admin may mint a grant. The persisted
    // mesh authority is group ownership (`group_is_owner`), which maps to Owner.
    if !storage::group_is_owner(&gid, &sys.node_id) {
        return json_result_ptr(false, None, None, Some("Only the group Owner/Admin may issue a grant"));
    }

    // Group display fields for the invite envelope (best-effort).
    let (group_name, group_icon, group_color) = match storage::group_get(&gid) {
        Ok(Some(g)) => (g.name, g.icon, g.color),
        _ => (gid.clone(), "folder.fill".to_string(), "#00AEEF".to_string()),
    };

    // Sign with this node's identity; trust its own pubkey as Owner so the grant is
    // self-consistent (the FFI authority gate above is the real check).
    let secret = sys.secret_key.to_bytes();
    let issuer_pk = pubkey_hex(&secret);
    let mut roster = GroupRoster::new();
    roster.set_role(&gid, &issuer_pk, Role::Owner);

    let now = chrono::Utc::now().timestamp() as u64;
    let ttl = if ttl_seconds == 0 { 24 * 3600 } else { ttl_seconds };
    let expiry = now + ttl;
    let nonce = uuid::Uuid::new_v4().to_string();

    match issue_grant_qr(
        &gid,
        &group_name,
        Some(&group_icon),
        Some(&group_color),
        role,
        &secret,
        &sys.node_id,
        now,
        expiry,
        &nonce,
        &roster,
    ) {
        Ok(qr) => {
            // Stamp this node's full resolvable address into the invite (MESH_HARDENING §2.2) so the
            // joiner can dial us directly with no relay/bootstrap. Additive: if we have no published
            // address yet, the field stays absent and the QR is byte-identical to before.
            let qr = match crate::local_endpoint_addr() {
                Some(addr_json) => match crate::identity::GrantInvite::from_qr_payload(&qr) {
                    Ok(mut invite) => {
                        invite.inviter_addr = Some(addr_json);
                        invite.to_qr_payload()
                    }
                    Err(_) => qr,
                },
                None => qr,
            };
            // Build the success body without `json!` (its expansion trips the
            // workspace `unwrap` lint) — a plain map → string is panic-free.
            let mut out = serde_json::Map::new();
            out.insert("success".to_string(), serde_json::Value::Bool(true));
            out.insert("qr".to_string(), serde_json::Value::String(qr));
            out.insert("nonce".to_string(), serde_json::Value::String(nonce));
            out.insert("expiry".to_string(), serde_json::Value::Number(expiry.into()));
            out.insert("role".to_string(), serde_json::Value::String(role_str));
            match CString::new(serde_json::Value::Object(out).to_string()) {
                Ok(c) => c.into_raw(),
                Err(_) => std::ptr::null_mut(),
            }
        }
        Err(e) => json_result_ptr(false, None, None, Some(&format!("Failed to issue grant: {}", e))),
    }
}

/// Scan a signed grant QR: pre-verify (signature · expiry · group) locally, then JOIN
/// the group (the snapshot holder runs the authoritative issuer-admin / revocation /
/// replay checks and serves the per-group snapshot). iOS: `scanGrantQR`. Returns the
/// same JSON shape as `xaero_join_group_from_invite` on success, or
/// `{"success":false,"error":...}` for a malformed / forged / expired QR.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_scan_grant_qr(qr_payload: *const c_char) -> *mut c_char {
    use crate::identity::scan_grant_qr_at;

    let Some(payload) = (unsafe { cstr_arg(qr_payload) }) else {
        return json_result_ptr(false, None, None, Some("Invalid QR payload"));
    };
    let now = chrono::Utc::now().timestamp() as u64;
    let invite = match scan_grant_qr_at(&payload, now) {
        Ok(inv) => inv,
        Err(e) => return json_result_ptr(false, None, None, Some(&format!("Grant rejected: {}", e))),
    };

    // Build the invite the join path consumes, carrying the signed grant so the
    // holder can authorize the per-group snapshot read. Plain map (no `json!`).
    let mut invite_json = serde_json::Map::new();
    let s = serde_json::Value::String;
    invite_json.insert("group_id".to_string(), s(invite.group_id));
    invite_json.insert("group_name".to_string(), s(invite.group_name));
    invite_json.insert("group_icon".to_string(), s(invite.group_icon.unwrap_or_else(|| "folder.fill".to_string())));
    invite_json.insert("group_color".to_string(), s(invite.group_color.unwrap_or_else(|| "#00AEEF".to_string())));
    invite_json.insert("inviter_node_id".to_string(), s(invite.inviter_node_id));
    // Forward the inviter's full address (§2.2) when the QR carried it, so the join path seeds it.
    if let Some(addr_json) = invite.inviter_addr {
        invite_json.insert("inviter_addr".to_string(), s(addr_json));
    }
    invite_json.insert("grant".to_string(), s(invite.grant.to_qr_payload()));
    join_from_invite(&serde_json::Value::Object(invite_json))
}

// ============================================================================
// §11 Portable Group Export bundle FFI
// ============================================================================

/// This node's X25519 bundle public key (hex) — the recipient key an inviter passes to
/// `cyan_export_group` so the `.cyangroup` is sealed TO this device. Derived deterministically
/// from this node's XaeroID identity (see `group_bundle::invitee_pubkey_hex`); stable across
/// launches. iOS: `bundlePublicKey`. Returns the hex string, or null if not initialized.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_bundle_pubkey() -> *mut c_char {
    let Some(sys) = SYSTEM.get() else {
        return std::ptr::null_mut();
    };
    let secret = sys.secret_key.to_bytes();
    let pk = crate::group_bundle::invitee_pubkey_hex(&secret);
    match CString::new(pk) {
        Ok(c) => c.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

/// Export a group to a signed, invitee-encrypted `.cyangroup` bundle (MESH_HARDENING §11) —
/// **group Owner only**. `invitee_pubkey` is the invitee's X25519 bundle pubkey (from
/// `cyan_bundle_pubkey` on their device). The bundle is STRICTLY scoped to this one group, files
/// are metadata-only (no media bytes), and the whole thing is XaeroID-signed by this node.
///
/// iOS: `exportGroup`. Returns `{"success":true,"group_id":...,"bundle":"<json>","path":"<file>"}`
/// (the bundle body is also written under the data dir) or `{"success":false,"error":...}`.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_export_group(
    group_id: *const c_char,
    invitee_pubkey: *const c_char,
) -> *mut c_char {
    use crate::identity::{pubkey_hex, GroupRoster, Role};

    let Some(gid) = (unsafe { cstr_arg(group_id) }) else {
        return json_result_ptr(false, None, None, Some("Invalid group_id"));
    };
    let Some(invitee) = (unsafe { cstr_arg(invitee_pubkey) }) else {
        return json_result_ptr(false, None, None, Some("Invalid invitee_pubkey"));
    };
    let Some(sys) = SYSTEM.get() else {
        return json_result_ptr(false, None, None, Some("System not initialized"));
    };

    // Authority: only the group Owner/Admin may export it (same gate as issuing a grant).
    if !storage::group_is_owner(&gid, &sys.node_id) {
        return json_result_ptr(false, None, None, Some("Only the group Owner/Admin may export it"));
    }

    // Mint the invitee's scope grant for THIS group (role Member), signed by this node. The
    // export embeds it as the bundle's strict scope; import re-checks it matches the group.
    let secret = sys.secret_key.to_bytes();
    let issuer_pk = pubkey_hex(&secret);
    let mut roster = GroupRoster::new();
    roster.set_role(&gid, &issuer_pk, Role::Owner);
    let now = chrono::Utc::now().timestamp();
    let expiry = (now + 365 * 24 * 3600) as u64;
    let nonce = uuid::Uuid::new_v4().to_string();
    let grant = match crate::identity::Grant::issue(
        &gid, Role::Member, &secret, now as u64, expiry, &nonce, &roster,
    ) {
        Ok(g) => g,
        Err(e) => return json_result_ptr(false, None, None, Some(&format!("Grant: {e}"))),
    };

    let bundle = match crate::group_bundle::export_group(&gid, &grant, &invitee, &secret, now) {
        Ok(b) => b,
        Err(e) => return json_result_ptr(false, None, None, Some(&format!("Export failed: {e}"))),
    };
    let body = bundle.to_json();

    // Best-effort: also drop the bundle on disk so the app can share the file directly.
    let path = DATA_DIR.get().map(|d| d.join("exports").join(format!("{gid}.cyangroup")));
    if let Some(ref p) = path {
        if let Some(parent) = p.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let _ = std::fs::write(p, &body);
    }

    let mut out = serde_json::Map::new();
    out.insert("success".to_string(), serde_json::Value::Bool(true));
    out.insert("group_id".to_string(), serde_json::Value::String(gid));
    out.insert("bundle".to_string(), serde_json::Value::String(body));
    if let Some(p) = path.and_then(|p| p.to_str().map(|s| s.to_string())) {
        out.insert("path".to_string(), serde_json::Value::String(p));
    }
    match CString::new(serde_json::Value::Object(out).to_string()) {
        Ok(c) => c.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

/// Import a `.cyangroup` bundle (MESH_HARDENING §11) — verifies the signature + grant scope,
/// decrypts to THIS device's key, seeds the baseline into storage (works fully offline), and
/// stamps "synced as of T" so §5 catch-up reconciles the gap on first online contact.
///
/// `bundle` is the JSON bundle body (from `cyan_export_group`). iOS: `importGroup`. Returns
/// `{"success":true,"group_id":...}` or `{"success":false,"error":...}` for an unsigned, forged,
/// out-of-scope, or undecryptable bundle.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_import_group(bundle: *const c_char) -> *mut c_char {
    let Some(body) = (unsafe { cstr_arg(bundle) }) else {
        return json_result_ptr(false, None, None, Some("Invalid bundle"));
    };
    let Some(sys) = SYSTEM.get() else {
        return json_result_ptr(false, None, None, Some("System not initialized"));
    };
    let parsed = match crate::group_bundle::GroupBundle::from_json(&body) {
        Ok(b) => b,
        Err(e) => return json_result_ptr(false, None, None, Some(&format!("{e}"))),
    };
    let secret = sys.secret_key.to_bytes();
    match crate::group_bundle::import_group(&parsed, &secret) {
        Ok(gid) => json_result_ptr(true, Some(&gid), None, None),
        Err(e) => json_result_ptr(false, None, None, Some(&format!("{e}"))),
    }
}

// ============================================================================
// Lens Commands FFI
// ============================================================================

/// Parse a lens command string and resolve paths to IDs.
/// Input: raw command string like "/summarize g\Sales\Workspace 1"
/// Returns: JSON with parsed command and resolved IDs, or error.
///
/// Response format:
/// {
///   "type": "summarize",
///   "resolved": { "group_id": "...", "workspace_id": "...", ... },
///   "error": null
/// }
/// or for natural language:
/// { "type": "natural_language", "text": "what happened..." }
/// or for help:
/// { "type": "help", "text": "..." }
#[unsafe(no_mangle)]
pub extern "C" fn cyan_parse_lens_command(input: *const c_char) -> *mut c_char {
    let input_str = match unsafe { CStr::from_ptr(input) }.to_str() {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    
    let command = crate::lens_commands::parse_command(input_str);
    
    let result = match &command {
        crate::lens_commands::LensCommand::Help => {
            serde_json::json!({
                "type": "help",
                "text": crate::lens_commands::help_text()
            })
        }
        crate::lens_commands::LensCommand::Import { source, target, path } => {
            let resolved = path.as_ref().and_then(|p| crate::lens_commands::resolve_path(p).ok());
            serde_json::json!({
                "type": "import",
                "source": source,
                "target": target,
                "resolved": resolved
            })
        }
        crate::lens_commands::LensCommand::Pipeline { action, step_id, path } => {
            let resolved = path.as_ref().and_then(|p| crate::lens_commands::resolve_path(p).ok());
            
            // Extract board_id from ResolvedPath enum
            let board_id: Option<String> = resolved.as_ref().and_then(|r| {
                match r {
                    crate::lens_commands::ResolvedPath::Board { board_id, .. } => Some(board_id.clone()),
                    crate::lens_commands::ResolvedPath::File { board_id, .. } => Some(board_id.clone()),
                    _ => None,
                }
            });
            
            match action.as_str() {
                "compile" => {
                    if let Some(ref bid) = board_id {
                        // compile_pipeline returns the prompt structure
                        // compile_via_llm actually calls vLLM and applies configs
                        serde_json::json!({
                            "type": "pipeline",
                            "action": "compile",
                            "board_id": bid,
                            "needs_llm": true
                        })
                    } else {
                        serde_json::json!({
                            "type": "pipeline",
                            "action": "compile",
                            "success": false,
                            "error": "No board specified. Use: /pipeline compile g\\Group\\Workspace\\Board"
                        })
                    }
                }
                "status" => {
                    if let Some(ref bid) = board_id {
                        match crate::pipeline::pipeline_status(bid) {
                            Ok(data) => serde_json::json!({
                                "type": "pipeline",
                                "action": "status",
                                "success": true,
                                "data": data
                            }),
                            Err(e) => serde_json::json!({
                                "type": "pipeline",
                                "action": "status",
                                "success": false,
                                "error": e.to_string()
                            }),
                        }
                    } else {
                        serde_json::json!({
                            "type": "pipeline",
                            "action": "status",
                            "success": false,
                            "error": "No board specified. Use: /pipeline status g\\Group\\Workspace\\Board"
                        })
                    }
                }
                "export" => {
                    if let Some(ref bid) = board_id {
                        match crate::pipeline::export_airflow_dag(bid, None) {
                            Ok(dag) => serde_json::json!({
                                "type": "pipeline",
                                "action": "export",
                                "success": true,
                                "dag": dag
                            }),
                            Err(e) => serde_json::json!({
                                "type": "pipeline",
                                "action": "export",
                                "success": false,
                                "error": e.to_string()
                            }),
                        }
                    } else {
                        serde_json::json!({
                            "type": "pipeline",
                            "action": "export",
                            "success": false,
                            "error": "No board specified."
                        })
                    }
                }
                "run" => {
                    serde_json::json!({
                        "type": "pipeline",
                        "action": "run",
                        "board_id": board_id
                    })
                }
                "approve" | "reject" | "retry" => {
                    serde_json::json!({
                        "type": "pipeline",
                        "action": action,
                        "step_id": step_id,
                        "board_id": board_id
                    })
                }
                _ => {
                    serde_json::json!({
                        "type": "pipeline",
                        "action": "help",
                        "text": "Pipeline commands: compile, run, status, approve, export"
                    })
                }
            }
        }
        crate::lens_commands::LensCommand::NaturalLanguage { text } => {
            serde_json::json!({
                "type": "natural_language",
                "text": text
            })
        }
        crate::lens_commands::LensCommand::Pin => {
            serde_json::json!({
                "type": "pin"
            })
        }
        crate::lens_commands::LensCommand::Summarize { path } => {
            match crate::lens_commands::resolve_path(path) {
                Ok(resolved) => serde_json::json!({
                    "type": "summarize",
                    "resolved": resolved,
                    "error": null
                }),
                Err(e) => serde_json::json!({
                    "type": "summarize",
                    "resolved": null,
                    "error": e.to_string()
                }),
            }
        }
        crate::lens_commands::LensCommand::SummarizeFile { path } => {
            match crate::lens_commands::resolve_path(path) {
                Ok(resolved) => {
                    // Also extract text if we can find the file
                    let text = if let crate::lens_commands::ResolvedPath::File { file_path: Some(ref fp), .. } = resolved {
                        match crate::lens_commands::extract_text_from_file(fp) {
                            Ok(t) => Some(crate::lens_commands::truncate_to_token_budget(&t, 4000)),
                            Err(e) => {
                                return CString::new(serde_json::json!({
                                    "type": "summarize_file",
                                    "resolved": resolved,
                                    "extracted_text": null,
                                    "error": format!("Text extraction failed: {}", e)
                                }).to_string()).unwrap_or_default().into_raw();
                            }
                        }
                    } else {
                        None
                    };
                    
                    serde_json::json!({
                        "type": "summarize_file",
                        "resolved": resolved,
                        "extracted_text": text,
                        "error": null
                    })
                }
                Err(e) => serde_json::json!({
                    "type": "summarize_file",
                    "resolved": null,
                    "extracted_text": null,
                    "error": e.to_string()
                }),
            }
        }
        crate::lens_commands::LensCommand::Grep { term, path } => {
            match crate::lens_commands::resolve_path(path) {
                Ok(resolved) => serde_json::json!({
                    "type": "grep",
                    "term": term,
                    "resolved": resolved,
                    "error": null
                }),
                Err(e) => serde_json::json!({
                    "type": "grep",
                    "term": term,
                    "resolved": null,
                    "error": e.to_string()
                }),
            }
        }
        crate::lens_commands::LensCommand::Status { path } => {
            let resolved = path.as_ref().and_then(|p| crate::lens_commands::resolve_path(p).ok());
            serde_json::json!({
                "type": "status",
                "resolved": resolved
            })
        }
        crate::lens_commands::LensCommand::Pulse { path } => {
            let resolved = path.as_ref().and_then(|p| crate::lens_commands::resolve_path(p).ok());
            serde_json::json!({
                "type": "pulse",
                "resolved": resolved
            })
        }
    };
    
    match CString::new(result.to_string()) {
        Ok(s) => s.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

/// Extract text from a file at the given path.
/// Returns the extracted text, or null on failure.
/// Supports: PDF, TXT, MD, CSV, JSON, code files.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_extract_file_text(path: *const c_char) -> *mut c_char {
    let path_str = match unsafe { CStr::from_ptr(path) }.to_str() {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    
    match crate::lens_commands::extract_text_from_file(path_str) {
        Ok(text) => {
            let truncated = crate::lens_commands::truncate_to_token_budget(&text, 4000);
            match CString::new(truncated) {
                Ok(s) => s.into_raw(),
                Err(_) => std::ptr::null_mut(),
            }
        }
        Err(_) => std::ptr::null_mut(),
    }
}

/// Create a board with a markdown cell containing the given content.
/// Returns the board ID as a hex string, or null on failure.
/// Uses the command channel so the board gets broadcast to peers via gossip.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_pin_summary_as_board(
    workspace_id: *const c_char,
    board_name: *const c_char,
    markdown_content: *const c_char,
) -> *mut c_char {
    let ws_id = match unsafe { CStr::from_ptr(workspace_id) }.to_str() {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    let name = match unsafe { CStr::from_ptr(board_name) }.to_str() {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    let content = match unsafe { CStr::from_ptr(markdown_content) }.to_str() {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    
    let Some(sys) = SYSTEM.get() else {
        return std::ptr::null_mut();
    };
    
    // Compute the board ID the same way lib.rs does (deterministic)
    let board_id = blake3::hash(format!("board:{}-{}", ws_id, name).as_bytes()).to_hex().to_string();
    
    // Send CreateBoard through command channel — this handles:
    // 1. SQLite insert
    // 2. Gossip broadcast to peers
    // 3. SwiftEvent for local UI
    let _ = sys.command_tx.send(crate::models::commands::CommandMsg::CreateBoard {
        workspace_id: ws_id.to_string(),
        name: name.to_string(),
    });
    
    // Send AddNotebookCell through command channel — same benefits
    let _ = sys.command_tx.send(crate::models::commands::CommandMsg::AddNotebookCell {
        board_id: board_id.clone(),
        cell_type: "markdown".to_string(),
        cell_order: 0,
        content: Some(content.to_string()),
    });
    
    match CString::new(board_id) {
        Ok(s) => s.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

fn json_cstring(s: &str) -> *mut c_char {
    match CString::new(s) {
        Ok(cs) => cs.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

// ============================================================================
// Pipeline FFI Functions
// ============================================================================

/// Compile pipeline via vLLM — reads cells, sends to AI, writes configs back
#[unsafe(no_mangle)]
pub extern "C" fn cyan_pipeline_compile(
    board_id: *const c_char,
) -> *mut c_char {
    let board_id_str = match unsafe { CStr::from_ptr(board_id) }.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return std::ptr::null_mut(),
    };
    
    let system = match SYSTEM.get() {
        Some(s) => s,
        None => return json_cstring(r#"{"error":"System not initialized"}"#),
    };
    
    // Spawn compile as background task — returns immediately
    let command_tx = system.command_tx.clone();
    let event_tx = system.event_tx.clone();
    let bid = board_id_str.clone();
    
    let rt = match crate::RUNTIME.get() {
        Some(rt) => rt,
        None => return json_cstring(r#"{"error":"Runtime not available"}"#),
    };
    
    rt.spawn(async move {
        match crate::pipeline::compile_via_llm(&bid, &command_tx).await {
            Ok(data) => {
                let applied = data["applied"].as_u64().unwrap_or(0);
                eprintln!("🔧 Pipeline compile complete: {} steps configured", applied);
                let _ = event_tx.send(crate::models::events::SwiftEvent::StatusUpdate {
                    message: format!("Pipeline compiled: {} steps configured", applied),
                });
            }
            Err(e) => {
                eprintln!("🔧 Pipeline compile failed: {}", e);
                let _ = event_tx.send(crate::models::events::SwiftEvent::StatusUpdate {
                    message: format!("Pipeline compile failed: {}", e),
                });
            }
        }
    });
    
    json_cstring(&serde_json::json!({
        "status": "compiling",
        "board_id": board_id_str,
        "message": "Pipeline compiling in background"
    }).to_string())
}

/// Run pipeline DAG
#[unsafe(no_mangle)]
pub extern "C" fn cyan_run_pipeline(
    board_id: *const c_char,
) -> *mut c_char {
    let board_id_str = match unsafe { CStr::from_ptr(board_id) }.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return std::ptr::null_mut(),
    };
    
    let system = match SYSTEM.get() {
        Some(s) => s,
        None => return json_cstring(r#"{"error":"System not initialized"}"#),
    };
    
    // Spawn pipeline run as background task — returns immediately
    let command_tx = system.command_tx.clone();
    let event_tx = system.event_tx.clone();
    let bid = board_id_str.clone();
    
    let rt = match crate::RUNTIME.get() {
        Some(rt) => rt,
        None => return json_cstring(r#"{"error":"Runtime not available"}"#),
    };
    
    rt.spawn(async move {
        match crate::pipeline::run_pipeline(&bid, &command_tx, &event_tx).await {
            Ok(data) => {
                tracing::info!("🚀 Pipeline run complete: {}", &data.to_string()[..data.to_string().len().min(200)]);
                let _ = event_tx.send(crate::models::events::SwiftEvent::StatusUpdate {
                    message: format!("Pipeline complete: {}", data["steps_executed"]),
                });
            }
            Err(e) => {
                tracing::error!("🚀 Pipeline run failed: {}", e);
                let _ = event_tx.send(crate::models::events::SwiftEvent::StatusUpdate {
                    message: format!("Pipeline failed: {}", e),
                });
            }
        }
    });
    
    json_cstring(&serde_json::json!({
        "status": "started",
        "board_id": board_id_str,
        "message": "Pipeline running in background"
    }).to_string())
}

/// Approve a pipeline step
#[unsafe(no_mangle)]
pub extern "C" fn cyan_pipeline_approve(
    board_id: *const c_char,
    step_id: *const c_char,
) -> bool {
    let board_id_str = match unsafe { CStr::from_ptr(board_id) }.to_str() {
        Ok(s) => s,
        Err(_) => return false,
    };
    let step_id_str = match unsafe { CStr::from_ptr(step_id) }.to_str() {
        Ok(s) => s,
        Err(_) => return false,
    };
    
    let system = match SYSTEM.get() {
        Some(s) => s,
        None => return false,
    };
    
    let approved = crate::pipeline::approve_step(
        board_id_str,
        step_id_str,
        None,
        &system.command_tx,
        Some(&system.event_tx),
    )
    .is_ok();

    // RESUME the step-through: approving a step unblocks the next one, so re-run the
    // pipeline (approved/skipped steps are skipped) to execute the next step, which
    // then runs → awaits its own approval. This is what lets Rick step THROUGH the
    // workflow one approval at a time, with cost incrementing across steps.
    //
    // REVIEW_LOOP_ONE_BOARD: OPT-IN ONLY. The app's LensRunCoordinator owns the
    // step chain and resumes it itself after an approve — this engine-side
    // auto-resume was a SECOND copy of the step-loop racing it (double-execution
    // on multi-step boards). Kept behind CYAN_ENGINE_AUTORESUME=1 for headless/
    // engine-driven rigs.
    let engine_autoresume =
        std::env::var("CYAN_ENGINE_AUTORESUME").map(|v| v == "1") == Ok(true);
    if approved && engine_autoresume {
        let command_tx = system.command_tx.clone();
        let event_tx = system.event_tx.clone();
        let bid = board_id_str.to_string();
        if let Some(rt) = crate::RUNTIME.get() {
            rt.spawn(async move {
                if let Err(e) = crate::pipeline::run_pipeline(&bid, &command_tx, &event_tx).await {
                    tracing::warn!("resume-after-approve run failed: {}", e);
                }
            });
        }
    }

    approved
}


/// Run ONE locally-bound pipeline step through the on-device cyan-mcp host —
/// the MECHANICAL SPINE verb (additive). The step's compiled metadata must
/// carry `mcp_tool` (a rung-1 deterministic bind stamped at Review, or the
/// cyan-media hint); dispatch goes straight through `PluginHost` with the
/// side-effect approval gate — ZERO Lens/LLM involvement, GPU not required.
///
/// Returns JSON:
///   `{"success":true,"summary":...,"findings":N}`
///   `{"success":false,"gated":true,"error":"needs_human: …"}`  (approve → re-run)
///   `{"success":false,"error":…}`
/// A step with no local bind returns `{"success":false,"error":"not_locally_bound"}` —
/// the caller sends it to the lens instead.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_pipeline_run_step_local(
    board_id: *const c_char,
    step_id: *const c_char,
) -> *mut c_char {
    let board_id_str = match unsafe { CStr::from_ptr(board_id) }.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return std::ptr::null_mut(),
    };
    let step_id_str = match unsafe { CStr::from_ptr(step_id) }.to_str() {
        Ok(s) => s.to_string(),
        Err(_) => return std::ptr::null_mut(),
    };
    let system = match SYSTEM.get() {
        Some(s) => s,
        None => return json_cstring(r#"{"success":false,"error":"System not initialized"}"#),
    };
    let rt = match crate::RUNTIME.get() {
        Some(rt) => rt,
        None => return json_cstring(r#"{"success":false,"error":"Runtime not available"}"#),
    };

    // Locate the compiled cell for this step and its metadata, plus every PRIOR
    // step's persisted result (`pipeline.state.ai_result`) — the deterministic
    // source for `pending` arg completion below.
    let (content, mut metadata, upstream): (String, serde_json::Value, Vec<serde_json::Value>) = {
        let cells = crate::storage::cell_list_by_boards(std::slice::from_ref(&board_id_str))
            .unwrap_or_default();
        let mut found: Option<(String, serde_json::Value)> = None;
        let mut upstream: Vec<serde_json::Value> = Vec::new();
        for c in cells {
            let meta: serde_json::Value =
                serde_json::from_str(c.metadata_json.as_deref().unwrap_or("{}"))
                    .unwrap_or(serde_json::Value::Null);
            if meta["pipeline"]["step_id"].as_str() == Some(step_id_str.as_str()) {
                found = Some((c.content.unwrap_or_default(), meta));
                break; // cells arrive in cell_order — everything before is upstream
            }
            if let Some(res) = meta["pipeline"]["state"]["ai_result"].as_str()
                && let Ok(v) = serde_json::from_str::<serde_json::Value>(res)
            {
                // A local plugin step stores its result as a success envelope
                // over the raw MCP tools/call result. Unwrap through BOTH (the
                // one shared helper) so key lookup sees the TOOL's object
                // (e.g. {"file_id": …}), never a wrapper.
                let unwrapped = crate::pipeline_executor::unwrap_tool_payload(&v);
                if unwrapped.is_object() {
                    upstream.push(unwrapped);
                }
            }
        }
        match found {
            Some((content, meta)) => (content, meta, upstream),
            None => {
                return json_cstring(
                    r#"{"success":false,"error":"step not found (compile first)"}"#,
                )
            }
        }
    };
    if metadata.get("mcp_tool").is_none() {
        return json_cstring(r#"{"success":false,"error":"not_locally_bound"}"#);
    }

    // PENDING ARG COMPLETION (deterministic, zero-LLM): required props Review
    // couldn't resolve fill from the LATEST upstream output carrying the key —
    // e.g. `list_comments.file_id` from the upload step's `{"file_id": …}`.
    // A prop still missing dispatches anyway; the plugin's own validation
    // reports it clearly (never guessed, never routed to a model).
    let pending: Vec<String> = metadata["mcp_tool"]["pending"]
        .as_array()
        .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default();
    if !pending.is_empty() {
        for key in &pending {
            if metadata["mcp_tool"]["args"].get(key).is_some() {
                continue;
            }
            if let Some(v) = upstream.iter().rev().find_map(|o| o.get(key)) {
                metadata["mcp_tool"]["args"][key] = v.clone();
            }
        }
        // CONFIG-CONTEXT fallback at dispatch too (heals a bind compiled before
        // the config existed): per-WORKFLOW plugin_config row → per-TENANT row →
        // the plugin's ambient env (demo stopgap) — the same ladder the bind
        // uses (PLUGIN_CREDENTIAL_ONBOARDING §B).
        let plugin_id = metadata["mcp_tool"]["plugin_id"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let cfg_tenant = crate::storage::board_get_group_id(&board_id_str)
            .filter(|g| !g.is_empty())
            .unwrap_or_else(|| "device".to_string());
        for key in &pending {
            if metadata["mcp_tool"]["args"].get(key).is_none()
                && let Some(v) = crate::plugin_config::context_value(
                    &cfg_tenant,
                    Some(&board_id_str),
                    &plugin_id,
                    key,
                )
            {
                metadata["mcp_tool"]["args"][key] = serde_json::json!(v);
            }
        }

        // A media INPUT still unfilled resolves to the board's CONFORMED v2
        // when the review loop has produced one — the "produce master" step
        // renders from the frame-accurate conform output, never a stale
        // source, and still with zero LLM involvement.
        for key in ["input", "file_path"] {
            if pending.iter().any(|p| p == key)
                && metadata["mcp_tool"]["args"].get(key).is_none()
            {
                let v2: Option<String> = {
                    let conn = crate::storage::db().lock_safe();
                    let tenant = crate::review_loop::board_tenant(&conn, &board_id_str);
                    crate::review_loop::current_proxy_ref(&conn, &tenant, &board_id_str)
                        .ok()
                        .flatten()
                        .and_then(|pref| {
                            crate::review_loop::review_media_info(&conn, &tenant, &pref).ok()
                        })
                        .and_then(|info| {
                            info["derived_proxy_path"].as_str().map(str::to_string)
                        })
                };
                if let Some(v2) = v2 {
                    metadata["mcp_tool"]["args"][key] = serde_json::json!(v2);
                }
            }
        }

        // Still-unfilled media prop: the LATEST upstream media output
        // (cyan-media's `output_path`, media-root-relative → absolutized) is
        // the media this step consumes — the review proxy an upload publishes,
        // the conform input, etc. Found live on the C2C MATERIALIZED run
        // (FABLE_FULL_AUDIT hard gate): no conform-registered media exists on
        // a fresh materialized run, so this deterministic rung was missing and
        // the upload died plugin-side on missing_argument.
        for key in ["input", "file_path"] {
            if pending.iter().any(|p| p == key)
                && metadata["mcp_tool"]["args"].get(key).is_none()
                && let Some(rel) =
                    crate::pipeline_executor::latest_upstream_media_path(&upstream)
            {
                let abs = crate::media_staging::effective_media_root().join(&rel);
                metadata["mcp_tool"]["args"][key] =
                    serde_json::json!(abs.to_string_lossy());
            }
        }
        // An unfilled `name` rides the resolved media prop: the artifact's real
        // basename, never a guess.
        if pending.iter().any(|p| p == "name")
            && metadata["mcp_tool"]["args"].get("name").is_none()
        {
            let media = metadata["mcp_tool"]["args"]
                .get("file_path")
                .or_else(|| metadata["mcp_tool"]["args"].get("input"))
                .and_then(|v| v.as_str())
                .map(str::to_string);
            if let Some(media) = media {
                metadata["mcp_tool"]["args"]["name"] = serde_json::json!(
                    crate::pipeline_executor::media_name_default(&media)
                );
            }
        }
    }

    let executor_type = metadata["pipeline"]["executor"].as_str().unwrap_or("local").to_string();
    let command_tx = system.command_tx.clone();
    let event_tx = system.event_tx.clone();
    let result = rt.block_on(crate::pipeline_executor::execute_pipeline_step(
        &board_id_str,
        &step_id_str,
        &content,
        &executor_type,
        Some(metadata),
        Vec::new(),
        &command_tx,
        &event_tx,
    ));
    match result {
        Ok((summary, findings)) => json_cstring(
            &serde_json::json!({
                "success": true,
                "summary": summary,
                "findings": findings.len(),
            })
            .to_string(),
        ),
        Err(e) => {
            let msg = e.to_string();
            let gated = msg.starts_with("needs_human:");
            json_cstring(
                &serde_json::json!({
                    "success": false,
                    "gated": gated,
                    "error": msg,
                })
                .to_string(),
            )
        }
    }
}

/// Retry a pipeline step (reset to pending, preserve metadata)
#[unsafe(no_mangle)]
pub extern "C" fn cyan_pipeline_retry(
    board_id: *const c_char,
    step_id: *const c_char,
) -> bool {
    let board_id_str = match unsafe { CStr::from_ptr(board_id) }.to_str() {
        Ok(s) => s,
        Err(_) => return false,
    };
    let step_id_str = match unsafe { CStr::from_ptr(step_id) }.to_str() {
        Ok(s) => s,
        Err(_) => return false,
    };
    
    let system = match SYSTEM.get() {
        Some(s) => s,
        None => return false,
    };
    
    let retried = crate::pipeline::retry_step(board_id_str, step_id_str, &system.command_tx).is_ok();
    // RESUME after retry: the step was reset to pending — re-run the pipeline so it
    // executes again (and the run continues), instead of just sitting reset.
    if retried {
        let command_tx = system.command_tx.clone();
        let event_tx = system.event_tx.clone();
        let bid = board_id_str.to_string();
        if let Some(rt) = crate::RUNTIME.get() {
            rt.spawn(async move {
                if let Err(e) = crate::pipeline::run_pipeline(&bid, &command_tx, &event_tx).await {
                    tracing::warn!("resume-after-retry run failed: {}", e);
                }
            });
        }
    }
    retried
}

/// Reject a pipeline step: mark it failed so the run STOPS at the gate (the operator
/// rejected the step's output). Mirror of approve; surfaces via the dashboard events.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_pipeline_reject(
    board_id: *const c_char,
    step_id: *const c_char,
) -> bool {
    let board_id_str = match unsafe { CStr::from_ptr(board_id) }.to_str() {
        Ok(s) => s,
        Err(_) => return false,
    };
    let step_id_str = match unsafe { CStr::from_ptr(step_id) }.to_str() {
        Ok(s) => s,
        Err(_) => return false,
    };
    let system = match SYSTEM.get() {
        Some(s) => s,
        None => return false,
    };
    crate::pipeline::reject_step(
        board_id_str,
        step_id_str,
        None,
        &system.command_tx,
        Some(&system.event_tx),
    )
    .is_ok()
}


/// Reset all pipeline steps to pending (for demo resets)
#[unsafe(no_mangle)]
pub extern "C" fn cyan_pipeline_reset(
    board_id: *const c_char,
) -> bool {
    let board_id_str = match unsafe { CStr::from_ptr(board_id) }.to_str() {
        Ok(s) => s,
        Err(_) => return false,
    };
    
    let system = match SYSTEM.get() {
        Some(s) => s,
        None => return false,
    };
    
    crate::pipeline::reset_pipeline(board_id_str, &system.command_tx).is_ok()
}


/// Load the PERSISTED run state for a board (the single-run state machine). Returns
/// the reconstructed run as JSON — run_id, derived status, per-step states + costs,
/// monotonic total_cost_usd, the awaiting-approval step. The iOS Dashboard calls this
/// on appear so a run + its progress RELOAD after navigating away (no more vanishing),
/// and so cost is one source of truth. Pure read; never mutates.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_pipeline_status(
    board_id: *const c_char,
) -> *mut c_char {
    let board_id_str = match unsafe { CStr::from_ptr(board_id) }.to_str() {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    match crate::pipeline::pipeline_status(board_id_str) {
        Ok(data) => json_cstring(&data.to_string()),
        Err(e) => json_cstring(&serde_json::json!({ "error": e.to_string() }).to_string()),
    }
}

// ============================================================================
// ChangeList store FFI (CYAN_CHANGELIST_STORE_AND_REVIEW_LOOP §Part 1)
// ============================================================================

/// Drive the content-addressed ChangeList store. Additive `cyan_*` surface for the
/// Frame.io review-&-conform loop; both the iOS review rail and Cyan Lens call the
/// same ops through this one JSON entrypoint.
///
/// `cmd_json` is `{ "op": <name>, ... }` where `op` is one of: append, set_state,
/// set_active, supersede, snapshot, branch, diff, conform_plan, get, get_version,
/// set_outcome. Returns a JSON string the caller owns and must free with
/// `cyan_free_string`; errors surface as `{ "error": "<msg>" }` — never a panic
/// across the boundary.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_changelist_command(cmd_json: *const c_char) -> *mut c_char {
    let json_str = match unsafe { cstr_arg(cmd_json) } {
        Some(s) => s,
        None => return json_cstring(&serde_json::json!({ "error": "null command" }).to_string()),
    };
    json_cstring(&crate::changelist::command(&json_str))
}

// ============================================================================
// STAGE-4 ingest FFI (AUTHORING_FIXES_ROUND2 §STAGE 4)
// ============================================================================

/// Drive watched ingest sources + per-asset workflow runs. Additive `cyan_*`
/// surface mirroring `cyan_changelist_command`: `cmd_json` is
/// `{ "op": <name>, ... }` where `op` is one of: source_add, source_list,
/// source_remove, scan_now, scan_due, runs_for_board, produce_master_plan.
/// Returns a JSON string the caller owns and must free with `cyan_free_string`;
/// errors surface as `{ "error": "<msg>" }` — never a panic across the boundary.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_ingest_command(cmd_json: *const c_char) -> *mut c_char {
    let json_str = match unsafe { cstr_arg(cmd_json) } {
        Some(s) => s,
        None => return json_cstring(&serde_json::json!({ "error": "null command" }).to_string()),
    };
    json_cstring(&crate::ingest::command(&json_str))
}

// ============================================================================
// Review-loop state machine FFI (CYAN_REVIEW_LOOP_TRANSITION_CONTRACT)
// ============================================================================

/// Drive the review-loop state machine + editable-proposal lifecycle. Additive
/// `cyan_*` surface layered on the ChangeList store; the iOS review rail and Cyan
/// Lens fire transitions through this one JSON entrypoint.
///
/// `cmd_json` is `{ "op": <name>, "actor": "auto|agent|human", ... }`. Ops:
/// get, start_draft, publish, notes_arrived, version_approved, confirm_notes,
/// conform_run, conform_failed, publish_proxy, finish, delivered, reopen_branch,
/// propose_op, confirm_op, escalate_note, nudges_for. The three-actor authority
/// model is enforced inside: an AGENT may only PROPOSE, and every gated /
/// external_send transition requires `actor=human`. Returns a JSON string the
/// caller owns and must free with `cyan_free_string`; errors surface as
/// `{ "error": "<msg>" }` — never a panic across the boundary.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_review_command(cmd_json: *const c_char) -> *mut c_char {
    let json_str = match unsafe { cstr_arg(cmd_json) } {
        Some(s) => s,
        None => return json_cstring(&serde_json::json!({ "error": "null command" }).to_string()),
    };
    json_cstring(&crate::review_state::command(&json_str))
}

// ============================================================================
// Timecoded Notes FFI
// ============================================================================

/// Save a timecoded note
#[unsafe(no_mangle)]
pub extern "C" fn cyan_save_timecode_note(
    note_json: *const c_char,
) -> bool {
    let json_str = match unsafe { CStr::from_ptr(note_json) }.to_str() {
        Ok(s) => s,
        Err(_) => return false,
    };
    crate::timecode_notes::save_note_ffi(json_str).is_ok()
}

/// Load timecoded notes for a board
#[unsafe(no_mangle)]
pub extern "C" fn cyan_load_timecode_notes(
    board_id: *const c_char,
) -> *mut c_char {
    let board_id_str = match unsafe { CStr::from_ptr(board_id) }.to_str() {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    match crate::timecode_notes::load_notes_ffi(board_id_str) {
        Ok(json) => json_cstring(&json),
        Err(e) => json_cstring(&serde_json::json!({"error": e.to_string()}).to_string()),
    }
}

/// Act on a timecoded note — sends to AI with pipeline context
#[unsafe(no_mangle)]
pub extern "C" fn cyan_act_on_timecode_note(
    note_json: *const c_char,
) -> *mut c_char {
    let json_str = match unsafe { CStr::from_ptr(note_json) }.to_str() {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    match crate::timecode_notes::act_on_note_ffi(json_str) {
        Ok(result) => json_cstring(&serde_json::json!({"success": true, "result": result}).to_string()),
        Err(e) => json_cstring(&serde_json::json!({"error": e.to_string()}).to_string()),
    }
}

/// Export timecoded notes as markdown
#[unsafe(no_mangle)]
pub extern "C" fn cyan_export_notes_markdown(board_id: *const c_char) -> *mut c_char {
    let board_id_str = match unsafe { CStr::from_ptr(board_id) }.to_str() {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    
    match crate::timecode_notes::export_notes_markdown_ffi(board_id_str) {
        Ok(md) => json_cstring(&md),
        Err(e) => {
            eprintln!("Export notes failed: {}", e);
            std::ptr::null_mut()
        }
    }
}
/// The board's playable video, resolved through the SAME path-resolution rails
/// the cyan-media tools use (staged master + newest derived proxy). Returns
/// `{"proxy_path": string|null, "master_uri": string|null, "media_root": string}`
/// as JSON, or null on an invalid board id. Additive — new verb for the app's
/// Video face so the player and the tool inputs can never disagree.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_board_video_media(board_id: *const c_char) -> *mut c_char {
    let board_id_str = match unsafe { CStr::from_ptr(board_id) }.to_str() {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    let info = crate::pipeline_executor::board_video_media(board_id_str);
    json_cstring(&info.to_string())
}

/// Set one per-install / per-workflow plugin CONFIG value
/// (PLUGIN_CREDENTIAL_ONBOARDING §A). Input JSON:
/// `{"plugin_id","key","value","board_id"?,"tenant_id"?}` — with a `board_id`
/// the row is WORKFLOW-scoped (tenant derived from the board's group unless
/// given); without one it is the tenant-wide default. Returns
/// `{"ok":bool,"error"?}`. Secret-looking keys are refused (vault material).
#[unsafe(no_mangle)]
pub extern "C" fn cyan_plugin_config_set(cmd_json: *const c_char) -> *mut c_char {
    let cmd = match unsafe { CStr::from_ptr(cmd_json) }.to_str() {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    let v: serde_json::Value = match serde_json::from_str(cmd) {
        Ok(v) => v,
        Err(e) => return json_cstring(&serde_json::json!({"ok": false, "error": format!("bad json: {e}")}).to_string()),
    };
    let (Some(plugin_id), Some(key), Some(value)) = (
        v["plugin_id"].as_str(),
        v["key"].as_str(),
        v["value"].as_str(),
    ) else {
        return json_cstring(
            &serde_json::json!({"ok": false, "error": "plugin_id, key, value are required"})
                .to_string(),
        );
    };
    let board_id = v["board_id"].as_str().filter(|b| !b.is_empty());
    let tenant = v["tenant_id"]
        .as_str()
        .map(str::to_string)
        .or_else(|| board_id.and_then(crate::storage::board_get_group_id))
        .filter(|t| !t.is_empty())
        .unwrap_or_else(|| "device".to_string());
    let out = {
        let conn = crate::storage::db().lock_safe();
        crate::plugin_config::set(
            &conn,
            &tenant,
            board_id,
            plugin_id,
            key,
            value,
            chrono::Utc::now().timestamp(),
        )
    };
    match out {
        Ok(()) => json_cstring(&serde_json::json!({"ok": true}).to_string()),
        Err(e) => json_cstring(&serde_json::json!({"ok": false, "error": e.to_string()}).to_string()),
    }
}

/// Read plugin CONFIG. Input JSON `{"plugin_id","board_id"?,"tenant_id"?,"key"?}`:
/// with `key` returns `{"ok":true,"value":string|null}` (workflow row wins over
/// tenant row); without it returns `{"ok":true,"values":{key:value,…}}` (tenant
/// defaults merged under workflow overrides — what a settings sheet lists).
#[unsafe(no_mangle)]
pub extern "C" fn cyan_plugin_config_get(cmd_json: *const c_char) -> *mut c_char {
    let cmd = match unsafe { CStr::from_ptr(cmd_json) }.to_str() {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    let v: serde_json::Value = match serde_json::from_str(cmd) {
        Ok(v) => v,
        Err(e) => return json_cstring(&serde_json::json!({"ok": false, "error": format!("bad json: {e}")}).to_string()),
    };
    let Some(plugin_id) = v["plugin_id"].as_str() else {
        return json_cstring(
            &serde_json::json!({"ok": false, "error": "plugin_id is required"}).to_string(),
        );
    };
    let board_id = v["board_id"].as_str().filter(|b| !b.is_empty());
    let tenant = v["tenant_id"]
        .as_str()
        .map(str::to_string)
        .or_else(|| board_id.and_then(crate::storage::board_get_group_id))
        .filter(|t| !t.is_empty())
        .unwrap_or_else(|| "device".to_string());
    let conn = crate::storage::db().lock_safe();
    if let Some(key) = v["key"].as_str() {
        match crate::plugin_config::get(&conn, &tenant, board_id, plugin_id, key) {
            Ok(val) => json_cstring(&serde_json::json!({"ok": true, "value": val}).to_string()),
            Err(e) => json_cstring(&serde_json::json!({"ok": false, "error": e.to_string()}).to_string()),
        }
    } else {
        match crate::plugin_config::list(&conn, &tenant, board_id, plugin_id) {
            Ok(rows) => {
                let map: serde_json::Map<String, serde_json::Value> = rows
                    .into_iter()
                    .map(|(k, val)| (k, serde_json::Value::String(val)))
                    .collect();
                json_cstring(&serde_json::json!({"ok": true, "values": map}).to_string())
            }
            Err(e) => json_cstring(&serde_json::json!({"ok": false, "error": e.to_string()}).to_string()),
        }
    }
}

// Add to ffi/core.rs — path autocomplete for g\ prefix

/// Autocomplete a partial path like "g\", "g\Sales\", "g\Sales\Work"
/// Returns JSON array of [{name, path}] suggestions.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_autocomplete_path(
    partial: *const c_char,
) -> *mut c_char {
    let partial_str = match unsafe { CStr::from_ptr(partial) }.to_str() {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    
    // Bounded read acquire: autocomplete fires per keystroke — parking it
    // behind a mesh sync hangs typing (the P0 hang family). Busy ⇒ no
    // suggestions this keystroke; the next keystroke retries.
    let Some(conn) = crate::storage::try_db_read(crate::storage::READ_LOCK_BUDGET) else {
        return std::ptr::null_mut();
    };

    // Strip g\ or g/ prefix
    let cleaned = partial_str
        .trim_start_matches("g\\")
        .trim_start_matches("g/");
    
    let parts: Vec<&str> = cleaned.split('\\').collect();
    
    let result: Vec<serde_json::Value> = match parts.len() {
        // g\ → list all groups
        0 | 1 if cleaned.is_empty() || !cleaned.contains('\\') => {
            let filter = if cleaned.is_empty() { "" } else { parts[0] };
            let pattern = format!("{}%", filter);
            conn.prepare(
                "SELECT name FROM groups WHERE name LIKE ?1 ORDER BY name LIMIT 10"
            )
            .and_then(|mut stmt| {
                let rows = stmt.query_map(rusqlite::params![pattern], |row| {
                    row.get::<_, String>(0)
                })?;
                Ok(rows.filter_map(|r| r.ok())
                    .map(|name| {
                        serde_json::json!({
                            "name": name,
                            "path": format!("g\\{}", name)
                        })
                    })
                    .collect::<Vec<_>>())
            })
            .unwrap_or_default()
        }
        
        // g\GroupName\ → list workspaces in group
        2 if cleaned.ends_with('\\') || parts[1].is_empty() => {
            let group_name = parts[0];
            let gid: Option<String> = conn.query_row(
                "SELECT id FROM groups WHERE name = ?1 COLLATE NOCASE",
                rusqlite::params![group_name],
                |r| r.get(0),
            ).ok();
            
            if let Some(gid) = gid {
                conn.prepare(
                    "SELECT name FROM workspaces WHERE group_id = ?1 ORDER BY name LIMIT 10"
                )
                .and_then(|mut stmt| {
                    let rows = stmt.query_map(rusqlite::params![gid], |row| {
                        row.get::<_, String>(0)
                    })?;
                    Ok(rows.filter_map(|r| r.ok())
                        .map(|name| {
                            serde_json::json!({
                                "name": name,
                                "path": format!("g\\{}\\{}", group_name, name)
                            })
                        })
                        .collect::<Vec<_>>())
                })
                .unwrap_or_default()
            } else {
                vec![]
            }
        }

        // g\GroupName\Partial → filter workspaces
        2 => {
            let group_name = parts[0];
            let ws_filter = parts[1];
            let gid: Option<String> = conn.query_row(
                "SELECT id FROM groups WHERE name = ?1 COLLATE NOCASE",
                rusqlite::params![group_name],
                |r| r.get(0),
            ).ok();
            
            if let Some(gid) = gid {
                let pattern = format!("{}%", ws_filter);
                conn.prepare(
                    "SELECT name FROM workspaces WHERE group_id = ?1 AND name LIKE ?2 COLLATE NOCASE ORDER BY name LIMIT 10"
                )
                .and_then(|mut stmt| {
                    let rows = stmt.query_map(rusqlite::params![gid, pattern], |row| {
                        row.get::<_, String>(0)
                    })?;
                    Ok(rows.filter_map(|r| r.ok())
                        .map(|name| {
                            serde_json::json!({
                                "name": name,
                                "path": format!("g\\{}\\{}", group_name, name)
                            })
                        })
                        .collect::<Vec<_>>())
                })
                .unwrap_or_default()
            } else {
                vec![]
            }
        }
        
        // g\Group\Workspace\ → list boards
        3 if cleaned.ends_with('\\') || parts[2].is_empty() => {
            let group_name = parts[0];
            let ws_name = parts[1];
            let gid: Option<String> = conn.query_row(
                "SELECT id FROM groups WHERE name = ?1 COLLATE NOCASE",
                rusqlite::params![group_name],
                |r| r.get(0),
            ).ok();
            
            if let Some(gid) = gid {
                let wid: Option<String> = conn.query_row(
                    "SELECT id FROM workspaces WHERE group_id = ?1 AND name = ?2 COLLATE NOCASE",
                    rusqlite::params![gid, ws_name],
                    |r| r.get(0),
                ).ok();
                
                if let Some(wid) = wid {
                    conn.prepare(
                        "SELECT name FROM objects WHERE workspace_id = ?1 AND type = 'whiteboard' ORDER BY name LIMIT 10"
                    )
                    .and_then(|mut stmt| {
                        let rows = stmt.query_map(rusqlite::params![wid], |row| {
                            row.get::<_, String>(0)
                        })?;
                        Ok(rows.filter_map(|r| r.ok())
                            .map(|name| {
                                serde_json::json!({
                                    "name": name,
                                    "path": format!("g\\{}\\{}\\{}", group_name, ws_name, name)
                                })
                            })
                            .collect::<Vec<_>>())
                    })
                    .unwrap_or_default()
                } else {
                    vec![]
                }
            } else {
                vec![]
            }
        }
        
        _ => vec![],
    };
    
    let json = serde_json::to_string(&result).unwrap_or_else(|_| "[]".to_string());
    match CString::new(json) {
        Ok(cs) => cs.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

/// Workflow authoring autocomplete (BURST C1). ADDITIVE — the `g\` file-tree grammar
/// `cyan_autocomplete_path` stays for the Explorer; this speaks the `@`/`#`/`/` workflow
/// grammar the notebook step editor uses.
///
/// Delegates to `workflow::filter_autocomplete(board_id, partial)`: it builds the board's
/// tenant-scoped index (`@` installed plugins · `#` group files + this board's prior-step
/// outputs · `/` control actions) and filters it by the trailing trigger + query in
/// `partial`. A `@sl` cursor returns only matching plugins; `#`/`​/` behave likewise; no
/// active trigger returns the full index.
///
/// Returns JSON (caller frees with `cyan_free_string`):
///   `{"tenant_id":"…","plugins":[{"trigger":"@","kind":"plugin","value":"…","label":"…"}],
///     "artifacts":[…],"actions":[…]}`.
/// Null only on a bad `board_id`/`partial` pointer or a JSON encode failure.
#[unsafe(no_mangle)]
pub extern "C" fn cyan_workflow_autocomplete(
    board_id: *const c_char,
    partial: *const c_char,
) -> *mut c_char {
    let board_id_str = match unsafe { CStr::from_ptr(board_id) }.to_str() {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    // A null/invalid `partial` degrades to the empty query (full index), not a failure.
    let partial_str = unsafe { cstr_arg(partial) }.unwrap_or_default();

    let idx = crate::workflow::filter_autocomplete(board_id_str, &partial_str);
    match serde_json::to_string(&idx) {
        Ok(json) => json_cstring(&json),
        Err(_) => std::ptr::null_mut(),
    }
}

// ============================================================================
// Username normalization
// ============================================================================

/// Human-readable fallback: "User-A3F2" instead of raw hex
pub fn friendly_node_id(node_id: &str) -> String {
    if node_id.len() > 8 {
        format!("User-{}", node_id[..4].to_uppercase())
    } else {
        node_id.to_string()
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn cyan_friendly_node_id(node_id: *const c_char) -> *mut c_char {
    let Some(nid) = (unsafe { cstr_arg(node_id) }) else {
        return std::ptr::null_mut();
    };
    CString::new(friendly_node_id(&nid)).unwrap_or_default().into_raw()
}

// ============================================================================
// Anonymous mode FFI
// ============================================================================

#[unsafe(no_mangle)]
pub extern "C" fn cyan_create_anonymous_session(scope_id: *const c_char) -> *mut c_char {
    let Some(scope) = (unsafe { cstr_arg(scope_id) }) else {
        return std::ptr::null_mut();
    };
    let Some(sys) = SYSTEM.get() else {
        return std::ptr::null_mut();
    };
    
    let secret_bytes = sys.secret_key.to_bytes();
    let session = xaeroid::anonymous::AnonymousSession::new(&secret_bytes, &scope);
    let join_payload = session.join_payload();
    
    let _ = crate::storage::anonymous_session_save(
        &scope,
        &hex::encode(session.ephemeral_pubkey),
        &hex::encode(session.ephemeral_secret),
        &hex::encode(session.commitment),
        &session.handle,
    );
    
    // Broadcast to group
    if let Some(gid) = crate::storage::board_get_group_id(&scope)
        .or_else(|| crate::storage::workspace_get_group_id(&scope)) {
        let evt = NetworkEvent::AnonymousJoined {
            ephemeral_key: hex::encode(session.ephemeral_pubkey),
            commitment: hex::encode(session.commitment),
            handle: session.handle.clone(),
            scope_id: scope.clone(),
            joined_at: chrono::Utc::now().timestamp(),
            signature: join_payload.signature.clone(),
        };
        let _ = sys.network_tx.send(NetworkCommand::Broadcast {
            group_id: gid,
            event: evt.clone(),
        });
        let _ = sys.event_tx.send(SwiftEvent::Network(evt));
    }
    
    let result = serde_json::json!({
        "ephemeral_key": hex::encode(session.ephemeral_pubkey),
        "ephemeral_secret": hex::encode(session.ephemeral_secret),
        "commitment": hex::encode(session.commitment),
        "handle": session.handle,
        "scope_id": scope,
    });
    
    match CString::new(result.to_string()) {
        Ok(s) => s.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn cyan_reveal_anonymous_identity(scope_id: *const c_char) -> *mut c_char {
    let Some(scope) = (unsafe { cstr_arg(scope_id) }) else {
        return std::ptr::null_mut();
    };
    let Some(sys) = SYSTEM.get() else {
        return std::ptr::null_mut();
    };
    
    let Some((_eph_key, eph_secret, _commitment, handle, revealed)) = 
        crate::storage::anonymous_session_get(&scope) else {
        return std::ptr::null_mut();
    };
    if revealed { return std::ptr::null_mut(); }
    
    let secret_bytes = sys.secret_key.to_bytes();
    let eph_secret_bytes: [u8; 32] = match hex::decode(&eph_secret) {
        Ok(b) if b.len() == 32 => match b.try_into() {
            Ok(arr) => arr,
            Err(_) => return std::ptr::null_mut(),
        },
        _ => return std::ptr::null_mut(),
    };
    
    let eph_pubkey = xaeroid::XaeroID::ed25519_pubkey(&eph_secret_bytes);
    let real_pubkey = xaeroid::XaeroID::ed25519_pubkey(&secret_bytes);
    
    let display_name: Option<String> = {
        let db = sys.db.lock_safe();
        db.query_row(
            "SELECT display_name FROM user_profiles WHERE node_id = ?1",
            rusqlite::params![&sys.node_id],
            |row| row.get::<_, String>(0),
        ).ok()
    };
    
    let proof_sig = xaeroid::XaeroID::ed25519_sign(&eph_pubkey, &secret_bytes);
    let _ = crate::storage::anonymous_session_reveal(&scope);
    
    if let Some(gid) = crate::storage::board_get_group_id(&scope)
        .or_else(|| crate::storage::workspace_get_group_id(&scope)) {
        let evt = NetworkEvent::IdentityRevealed {
            ephemeral_key: hex::encode(eph_pubkey),
            real_pubkey: hex::encode(real_pubkey),
            real_name: display_name.clone(),
            handle: handle.clone(),
            scope_id: scope.clone(),
            proof_signature: hex::encode(proof_sig),
            revealed_at: chrono::Utc::now().timestamp(),
        };
        let _ = sys.network_tx.send(NetworkCommand::Broadcast {
            group_id: gid,
            event: evt.clone(),
        });
        let _ = sys.event_tx.send(SwiftEvent::Network(evt));
    }
    
    let result = serde_json::json!({
        "ephemeral_key": hex::encode(eph_pubkey),
        "real_pubkey": hex::encode(real_pubkey),
        "real_name": display_name,
        "handle": handle,
        "scope_id": scope,
        "revealed": true,
    });
    
    match CString::new(result.to_string()) {
        Ok(s) => s.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn cyan_get_anonymous_status(scope_id: *const c_char) -> *mut c_char {
    let Some(scope) = (unsafe { cstr_arg(scope_id) }) else {
        return std::ptr::null_mut();
    };
    let result = if let Some((_ek, _es, _c, handle, revealed)) = 
        crate::storage::anonymous_session_get(&scope) {
        serde_json::json!({ "anonymous": !revealed, "handle": handle, "revealed": revealed })
    } else {
        serde_json::json!({ "anonymous": false, "handle": null, "revealed": false })
    };
    match CString::new(result.to_string()) {
        Ok(s) => s.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn cyan_exit_anonymous_mode(scope_id: *const c_char) -> bool {
    let Some(scope) = (unsafe { cstr_arg(scope_id) }) else { return false; };
    crate::storage::anonymous_session_delete(&scope).is_ok()
}
