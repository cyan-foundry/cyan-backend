pub use crate::ai_bridge::AIBridge;
use crate::models::core::{Group, Workspace};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum NetworkEvent {
    /// A group snapshot available sent as a
    /// network event from source/peer_id
    /// the current running peer who receives this ack then
    /// QUIC connects and receives the snapshot
    GroupSnapshotAvailable {
        source: String,
        group_id: String,
    },
    GroupCreated(Group),
    GroupRenamed {
        id: String,
        name: String,
    },
    GroupDeleted {
        id: String,
    },
    /// Owner dissolved group - all peers must remove it
    GroupDissolved {
        id: String,
    },
    WorkspaceCreated(Workspace),
    WorkspaceRenamed {
        id: String,
        name: String,
    },
    WorkspaceDeleted {
        id: String,
    },
    /// Owner dissolved workspace - all peers must remove it
    WorkspaceDissolved {
        id: String,
    },
    BoardCreated {
        id: String,
        workspace_id: String,
        name: String,
        created_at: i64,
    },
    BoardRenamed {
        id: String,
        name: String,
    },
    BoardDeleted {
        id: String,
    },
    /// Owner dissolved board - all peers must remove it
    BoardDissolved {
        id: String,
    },
    FileAvailable {
        id: String,
        group_id: Option<String>,
        workspace_id: Option<String>,
        board_id: Option<String>,
        name: String,
        hash: String,
        size: u64,
        source_peer: String,
        created_at: i64,
    },
    // ---- Chat events ----
    ChatSent {
        id: String,
        workspace_id: String,
        message: String,
        author: String,
        parent_id: Option<String>,
        timestamp: i64,
    },
    ChatDeleted {
        id: String,
    },
    // ---- Whiteboard element events ----
    WhiteboardElementAdded {
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
        created_at: i64,
        updated_at: i64,
    },
    WhiteboardElementUpdated {
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
        updated_at: i64,
    },
    WhiteboardElementDeleted {
        id: String,
        board_id: String,
    },
    WhiteboardCleared {
        board_id: String,
    },
    // ---- Notebook cell events ----
    NotebookCellAdded {
        id: String,
        board_id: String,
        cell_type: String,
        cell_order: i32,
        content: Option<String>,
    },
    NotebookCellUpdated {
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
    NotebookCellDeleted {
        id: String,
        board_id: String,
    },
    NotebookCellsReordered {
        board_id: String,
        cell_ids: Vec<String>,
    },
    BoardModeChanged {
        board_id: String,
        mode: String,
    },
    // ---- Board metadata events ----
    BoardMetadataUpdated {
        board_id: String,
        labels: Vec<String>,
        rating: i32,
        contains_model: Option<String>,
        contains_skills: Vec<String>,
    },
    BoardLabelsUpdated {
        board_id: String,
        labels: Vec<String>,
    },
    BoardRated {
        board_id: String,
        rating: i32,
    },
    // ---- User profile events ----
    ProfileUpdated {
        node_id: String,
        display_name: String,
        avatar_hash: Option<String>,
    },
    // ---- MCP plugin relay ----
    /// A relayed event from a locally-hosted MCP plugin, broadcast into the group
    /// mesh so the super-peer (Lens replica) can pick it off gossip and feed Iggy.
    /// The device has no local Iggy, so the mesh IS the transport. Normal peer
    /// devices ignore it (no local consumer); only the super-peer enriches it.
    PluginRelay {
        /// Plugin that emitted the relayed event.
        plugin_id: String,
        /// JSON-RPC method/notification name the plugin pushed.
        method: String,
        /// Event payload as a JSON string (opaque to the mesh; Lens parses it).
        payload: String,
    },
    // ---- Anonymous participation events ----
    AnonymousJoined {
        ephemeral_key: String,
        commitment: String,
        handle: String,
        scope_id: String,
        joined_at: i64,
        signature: String,
    },
    IdentityRevealed {
        ephemeral_key: String,
        real_pubkey: String,
        real_name: Option<String>,
        handle: String,
        scope_id: String,
        proof_signature: String,
        revealed_at: i64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum SwiftEvent {
    Network(NetworkEvent),
    TreeLoaded(String), // The JSON tree
    GroupDeleted { id: String },
    /// Non-owner left group (local removal only)
    GroupLeft { id: String },
    WorkspaceDeleted { id: String },
    /// Non-owner left workspace
    WorkspaceLeft { id: String },
    BoardDeleted { id: String },
    /// Non-owner left board
    BoardLeft { id: String },
    ChatDeleted { id: String },
    /// Error message for UI display
    Error { message: String },
    /// Direct chat stream established with peer
    ChatStreamReady { peer_id: String, workspace_id: String },
    /// Direct chat stream closed
    ChatStreamClosed { peer_id: String },
    /// Peer joined a group topic
    PeerJoined { group_id: String, peer_id: String },
    /// Peer left a group topic
    PeerLeft { group_id: String, peer_id: String },
    /// Live count of connected mesh peers in a group, emitted on every join/leave. Additive and
    /// receive-only — feeds the app's honest status bar (the live peer count).
    PeerCountChanged { group_id: String, count: u32 },
    /// Mesh reachability for a group, emitted on every join/leave. `state` is `"online"`
    /// (≥1 connected peer) or `"local_only"` (0 connected peers — working offline against just
    /// this device's own copy). Lets the status bar distinguish "0 peers → local-only" from
    /// "≥1 peer, caught up → synced". Additive, receive-only.
    MeshReachability { group_id: String, state: String },
    /// Status update for UI (syncing, downloading, etc.)
    StatusUpdate { message: String },
    /// File download progress (0.0 to 1.0)
    FileDownloadProgress { file_id: String, progress: f64 },
    /// File download completed
    FileDownloaded { file_id: String, local_path: String },
    /// File download failed
    FileDownloadFailed { file_id: String, error: String },
    /// Board metadata was updated
    BoardMetadataUpdated { board_id: String },
    /// AI proactive insight generated
    AIInsight {
        insight_json: String,
    },
    /// Direct message received from peer
    DirectMessageReceived {
        id: String,
        peer_id: String,
        message: String,
        timestamp: i64,
        is_incoming: bool,
    },
    // ---- Sync Progress Events (for progressive UI updates) ----
    /// Sync started - shows skeleton UI
    SyncStarted {
        group_id: String,
        group_name: String,
    },
    /// Structure received - can show tree outline
    SyncStructureReceived {
        group_id: String,
        workspace_count: u32,
        board_count: u32,
    },
    /// Board content synced - board is now interactive
    SyncBoardReady {
        board_id: String,
        element_count: u32,
        cell_count: u32,
    },
    /// Files metadata received - shows files (possibly still downloading)
    SyncFilesReceived {
        group_id: String,
        file_count: u32,
    },
    /// Sync complete
    SyncComplete {
        group_id: String,
    },
    /// Chat history finished loading for a workspace
    ChatHistoryComplete {
        workspace_id: String,
    },
}

