use crate::actors::DmAttachment;
use crate::models::events::NetworkEvent;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum NetworkCommand {
    /// Requests group snapshot
    RequestSnapshot {
        from_peer: String,
    },
    JoinGroup {
        group_id: String,
        bootstrap_peer: Option<String>,  // Node ID of peer to bootstrap from (e.g., inviter)
    },
    Broadcast {
        group_id: String,
        event: NetworkEvent,
    },
    UploadToGroup {
        group_id: String,
        path: String,
    },
    UploadToWorkspace {
        workspace_id: String,
        path: String,
    },
    /// Owner dissolves group - broadcasts to all peers before leaving
    DissolveGroup { id: String },
    /// Non-owner leaves group - local cleanup only, no broadcast
    LeaveGroup { id: String },
    /// Owner dissolves workspace
    DissolveWorkspace { id: String, group_id: String },
    /// Non-owner leaves workspace
    LeaveWorkspace { id: String },
    /// Owner dissolves board
    DissolveBoard { id: String, group_id: String },
    /// Non-owner leaves board
    LeaveBoard { id: String },
    DeleteChat { id: String },
    /// Start a direct QUIC chat stream with a peer
    StartChatStream {
        peer_id: String,
        workspace_id: String,
    },
    /// Send a message on an existing direct chat stream.
    ///
    /// `attachment` is **optional and additive**: when present the message carries a file
    /// reference (id + name + blake3 hash + size) and the receiver fetches that file into the
    /// message's scope. Absent (the default) ⇒ identical to the original chat-send behavior, so
    /// the FFI/wire stays drop-in for callers that don't set it.
    SendDirectChat {
        peer_id: String,
        workspace_id: String,
        message: String,
        parent_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        attachment: Option<DmAttachment>,
    },
    /// Request file download from peer (with resume support)
    RequestFileDownload {
        file_id: String,
        hash: String,
        source_peer: String,
        resume_offset: u64,  // 0 for new, >0 for resume
    },
    /// Resume all pending file transfers
    ResumePendingTransfers,
    /// Announce over a group's gossip that this node holds the content-addressed blob `hash`
    /// (G10 swarm i-have). Engine-internal; not surfaced as a new client `cyan_*` FFI.
    SwarmAnnounce { group_id: String, hash: String },
    /// Ask a group (over gossip) which peers hold the content-addressed blob `hash`
    /// (G10 swarm who-has). Engine-internal; not surfaced as a new client `cyan_*` FFI.
    SwarmWhoHas { group_id: String, hash: String },
    /// Seed a content-addressed blob into this node's swarm store from `path`, then announce it
    /// (`IHave`) to `group_id` so members can swarm-fetch it. The engine's plugin-distribution hook:
    /// a `.cyanplugin` upload sends this so the file distributes peer-to-peer (G10). Engine-internal —
    /// emitted by the existing `cyan_upload_file` FFI, NOT a new client `cyan_*` function.
    SeedAndAnnounceBlob { group_id: String, hash: String, path: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum CommandMsg {
    // ═══════════════════════════════════════════════════════════════════════
    // GROUP COMMANDS
    // ═══════════════════════════════════════════════════════════════════════
    CreateGroup {
        name: String,
        icon: String,
        color: String,
    },
    RenameGroup {
        id: String,
        name: String,
    },
    /// Delete group (owner only - will check ownership)
    DeleteGroup {
        id: String,
    },
    /// Leave group (non-owner - local removal only)
    LeaveGroup {
        id: String,
    },

    // ═══════════════════════════════════════════════════════════════════════
    // WORKSPACE COMMANDS
    // ═══════════════════════════════════════════════════════════════════════
    CreateWorkspace {
        group_id: String,
        name: String,
    },
    RenameWorkspace {
        id: String,
        name: String,
    },
    /// Delete workspace (owner only)
    DeleteWorkspace {
        id: String,
    },
    /// Leave workspace (non-owner)
    LeaveWorkspace {
        id: String,
    },

    // ═══════════════════════════════════════════════════════════════════════
    // BOARD COMMANDS
    // ═══════════════════════════════════════════════════════════════════════
    CreateBoard {
        workspace_id: String,
        name: String,
    },
    RenameBoard {
        id: String,
        name: String,
    },
    /// Delete board (owner only)
    DeleteBoard {
        id: String,
    },
    /// Leave board (non-owner)
    LeaveBoard {
        id: String,
    },

    // ═══════════════════════════════════════════════════════════════════════
    // CHAT COMMANDS
    // ═══════════════════════════════════════════════════════════════════════
    SendChat {
        workspace_id: String,
        message: String,
        parent_id: Option<String>,
    },
    DeleteChat {
        id: String,
    },
    LoadChatHistory {
        workspace_id: String,
    },

    // ═══════════════════════════════════════════════════════════════════════
    // DIRECT MESSAGE COMMANDS
    // ═══════════════════════════════════════════════════════════════════════
    StartDirectChat {
        peer_id: String,
        workspace_id: String,
    },
    SendDirectMessage {
        peer_id: String,
        workspace_id: String,
        message: String,
        parent_id: Option<String>,
    },
    LoadDirectMessageHistory {
        peer_id: String,
    },

    // ═══════════════════════════════════════════════════════════════════════
    // WHITEBOARD ELEMENT COMMANDS
    // ═══════════════════════════════════════════════════════════════════════
    CreateWhiteboardElement {
        board_id: String,
        element_type: String,
        x: f64,
        y: f64,
        width: f64,
        height: f64,
        z_index: i32,
        style_json: Option<String>,
        content_json: Option<String>,
    },
    UpdateWhiteboardElement {
        id: String,
        board_id: String,
        element_type: String,
        x: f64,
        y: f64,
        width: f64,
        height: f64,
        z_index: i32,
        style_json: Option<String>,
        content_json: Option<String>,
    },
    DeleteWhiteboardElement {
        id: String,
        board_id: String,
    },
    ClearWhiteboard {
        board_id: String,
    },

    // ═══════════════════════════════════════════════════════════════════════
    // NOTEBOOK CELL COMMANDS
    // ═══════════════════════════════════════════════════════════════════════
    AddNotebookCell {
        board_id: String,
        cell_type: String,
        cell_order: i32,
        content: Option<String>,
    },
    UpdateNotebookCell {
        id: String,
        board_id: String,
        cell_type: String,
        cell_order: i32,
        content: Option<String>,
        output: Option<String>,
        collapsed: bool,
        height: Option<f64>,
        metadata_json: Option<String>,
    },
    DeleteNotebookCell {
        id: String,
        board_id: String,
    },
    ReorderNotebookCells {
        board_id: String,
        cell_ids: Vec<String>,
    },

    // ═══════════════════════════════════════════════════════════════════════
    // BOARD METADATA COMMANDS
    // ═══════════════════════════════════════════════════════════════════════
    UpdateBoardMetadata {
        board_id: String,
        labels: Vec<String>,
        rating: i32,
        view_count: i32,
        contains_model: Option<String>,
        contains_skills: Vec<String>,
        board_type: Option<String>,
        last_accessed: Option<i64>,
        is_pinned: bool,
    },
    IncrementBoardViewCount {
        board_id: String,
    },
    SetBoardPinned {
        board_id: String,
        is_pinned: bool,
    },

    // ═══════════════════════════════════════════════════════════════════════
    // INTEGRATION COMMANDS
    // ═══════════════════════════════════════════════════════════════════════
    AddIntegration {
        scope_type: String,
        scope_id: String,
        integration_type: String,
        config: serde_json::Value,
    },
    RemoveIntegration {
        id: String,
    },

    // ═══════════════════════════════════════════════════════════════════════
    // PROFILE COMMANDS
    // ═══════════════════════════════════════════════════════════════════════
    UpdateProfile {
        display_name: String,
        avatar_hash: Option<String>,
    },

    // ═══════════════════════════════════════════════════════════════════════
    // SYSTEM COMMANDS
    // ═══════════════════════════════════════════════════════════════════════
    Snapshot {},
    SeedDemoIfEmpty,
}