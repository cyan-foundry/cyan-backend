pub use crate::ai_bridge::AIBridge;
use crate::models::core::{Group, Workspace};
use serde::{Deserialize, Serialize};
use serde_json::Value;

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
        /// The board this chat belongs to (R11 §1 — chat is board-scoped). `#[serde(default)]`
        /// keeps the gossip wire backward-compatible with a pre-R11 peer.
        #[serde(default)]
        board_id: String,
        workspace_id: String,
        message: String,
        author: String,
        parent_id: Option<String>,
        timestamp: i64,
    },
    ChatDeleted {
        id: String,
    },
    // ---- Note events (ROUND8 §W2 — board-level, authored, LWW ledger) ----
    /// A note was authored. The receiver applies it via the idempotent LWW
    /// upsert-by-id, so `NoteAdded`/`NoteUpdated` are handled identically on apply —
    /// the split is purely informational for the UI.
    NoteAdded {
        id: String,
        board_id: String,
        tenant_id: String,
        author_id: String,
        author_name: String,
        text: String,
        created_at: i64,
        updated_at: i64,
        /// Note SCOPE (feat/notes-constitution): `tenant` | `group` | `board`.
        /// `#[serde(default)]` keeps the event wire-compatible with pre-scope peers.
        #[serde(default = "crate::models::dto::default_note_scope")]
        scope: String,
        /// Note KIND: `constitution` | `preference` | `editor-note`. Same compat rule.
        #[serde(default = "crate::models::dto::default_note_kind")]
        kind: String,
    },
    /// A note was edited. Conflict resolution is LWW on `updated_at` (older edits drop).
    NoteUpdated {
        id: String,
        board_id: String,
        tenant_id: String,
        author_id: String,
        author_name: String,
        text: String,
        created_at: i64,
        updated_at: i64,
        /// Same scope/kind wire-compat contract as `NoteAdded`.
        #[serde(default = "crate::models::dto::default_note_scope")]
        scope: String,
        #[serde(default = "crate::models::dto::default_note_kind")]
        kind: String,
    },
    NoteDeleted {
        id: String,
    },
    // ---- Pin event (ROUND8 §W4 — board-level pinned-workflow state, replicated LWW) ----
    /// A board's pinned-workflow state changed. The receiver applies it via the
    /// idempotent LWW upsert-by-`board_id`, so a stale `PinSet` is dropped on apply.
    PinSet {
        board_id: String,
        tenant_id: String,
        pinned: bool,
        updated_at: i64,
    },
    // ---- Ledger sync deltas (CYAN_FORMAT_SPEC §6.2 — additive) ----
    // Live gossip for the review ledger, on the same group topic notes/pins ride.
    // Receivers apply through the idempotent `changelist::` fns (content unions by
    // `entry_hash`, versions by `version_id`, audits by `audit_hash`; lifecycle and
    // branch heads are ONE LWW lane keyed `updated_at`, ties by higher actor id) —
    // so replays, echoes, and delta-vs-snapshot races all converge identically.
    /// A ChangeEntry was appended (content lane).
    ChangeEntryAppended {
        tenant_id: String,
        entry: Box<crate::changelist::ChangeEntry>,
    },
    /// An entry's lifecycle moved (LWW; the carried audit row always unions).
    ChangeEntryLifecycle {
        tenant_id: String,
        delta: Box<crate::changelist::LifecycleDelta>,
    },
    /// A version was snapshotted (immutable union — concurrent snapshots both survive).
    ChangeVersionCreated {
        tenant_id: String,
        version: Box<crate::changelist::ChangeVersion>,
    },
    /// A branch head moved (LWW on `updated_at`).
    ChangeBranchHead {
        tenant_id: String,
        asset_hash: String,
        branch: String,
        head_version: Option<String>,
        updated_at: i64,
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
    // ---- MCP remote tool invocation (sovereignty model) ----
    /// A cloud Lens `MeshTransport` dispatch routed to THIS host: run `tool` on
    /// the locally-installed `plugin_id` against LOCAL data, tenant-scoped, and
    /// reply with a `RemoteToolResult` carrying the same `corr_id`. This is the
    /// "Contido local, Lens on AWS" path — the plugin + data stay local; Lens only
    /// orchestrates. The device runs it via its existing cyan-mcp host
    /// (`mesh_invoke::RemoteInvokeHandler`). Normal peers ignore it (no consumer).
    RemoteToolCall {
        /// Correlation id matching the result back to this call.
        corr_id: String,
        /// Tenant the call is scoped to (carried on every mesh hop).
        tenant_id: String,
        /// Locally-installed plugin that exposes the tool.
        plugin_id: String,
        /// Tool name to invoke.
        tool: String,
        /// JSON arguments for the call (opaque on the mesh).
        args: Value,
    },
    /// The result of a [`RemoteToolCall`], correlated by `corr_id`. Exactly one of
    /// `result` / `error` is set.
    RemoteToolResult {
        /// Correlation id echoing the originating `RemoteToolCall::corr_id`.
        corr_id: String,
        /// The tool's JSON result, if it ran successfully.
        result: Option<Value>,
        /// A human-readable error, if the tool could not run.
        error: Option<String>,
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
    // ---- Live activity (R10FB §L) ----
    /// A board was edited (step add/edit, element change, rename, …). Gossiped so peers
    /// refresh that board's preview live and show a "recently active/edited" marker —
    /// receive-only on the peer side (no storage write; a transient activity signal).
    BoardChanged {
        board_id: String,
        /// Node id of the peer that made the edit.
        editor: String,
        ts: i64,
        /// R11 §9 — the board's current display name, so a peer can refresh that board's
        /// preview card live (previously the peer's preview stayed blank on edit because the
        /// signal carried no content). `#[serde(default)]` keeps the wire back-compatible.
        #[serde(default)]
        name: String,
        /// R11 §9 — a short content preview (latest cell/note text, truncated) for the card.
        #[serde(default)]
        preview: String,
    },
    // ---- Pin sync (R10FB §B3) ----
    /// A board's pinned flag (`board_metadata.is_pinned`) changed. Pinning is now a
    /// synced board property: the receiver upserts the flag so it lands even with no
    /// prior metadata row. Last write wins (single-FIFO persist keeps per-board order).
    BoardPinned {
        board_id: String,
        is_pinned: bool,
        updated_at: i64,
    },
    // ---- File delete (R10FB §F4) ----
    /// A file was deleted by a user. Soft-delete/tombstone (no hard delete in the engine)
    /// so the deletion converges to peers; the receiver applies the same tombstone.
    FileDeleted {
        id: String,
        deleted_at: i64,
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
    /// R12 B1: a NEW file arrived from another peer (board-scoped, like an inbound chat),
    /// so the app can raise a distinct "file received" notification — separate from the
    /// chat-message event. Emitted once per inbound `FileAvailable` whose `source_peer` is
    /// NOT this device (the sender's own echo never fires it). Additive, receive-only.
    FileReceived {
        id: String,
        #[serde(default)]
        board_id: String,
        #[serde(default)]
        workspace_id: String,
        #[serde(default)]
        group_id: String,
        name: String,
        hash: String,
        size: u64,
        source_peer: String,
        created_at: i64,
    },
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
    /// Chat history finished loading for a board (R11 §1 — board-scoped). `workspace_id` is
    /// kept (back-compat); iOS correlates the completion to the board's chat panel via
    /// `board_id`. `#[serde(default)]` keeps the JSON additive for an older client.
    ChatHistoryComplete {
        #[serde(default)]
        board_id: String,
        workspace_id: String,
    },

    // ── Workflow dashboard events (DASHBOARD_CONTRACT §A) ──────────────────
    // Additive, RECEIVE-ONLY: the app stays a pure event-receiver and renders a
    // read-model of a running workflow. Emitted from the REAL run path
    // (`pipeline::run_pipeline`). Every variant carries `tenant_id` + the scoping
    // keys (`run_id`/`board_id`/`workflow_id`); per-step variants also carry the
    // `stage`/`actor`/`plugin?` slicing dimensions. No new client COMMAND FFI —
    // these ride the existing event poll. Do NOT rename/repurpose existing events.
    /// A workflow run started: the compiled DAG begins executing.
    WorkflowRunStarted {
        tenant_id: String,
        run_id: String,
        board_id: String,
        workflow_id: String,
        workflow_label: String,
        total_steps: u32,
        started_at: i64,
    },
    /// A step changed lifecycle state.
    /// `state` ∈ pending | running | awaiting_approval | approved | done | failed.
    /// `actor` ∈ human | ai.
    StepStateChanged {
        tenant_id: String,
        run_id: String,
        board_id: String,
        workflow_id: String,
        step_id: String,
        name: String,
        stage: String,
        state: String,
        actor: String,
        plugin: Option<String>,
        at: i64,
    },
    /// Progress through the run (items/steps processed).
    StepProgress {
        tenant_id: String,
        run_id: String,
        board_id: String,
        workflow_id: String,
        step_id: String,
        stage: String,
        processed: u64,
        total: u64,
        current_item: Option<String>,
        detail: Option<String>,
    },
    /// A gate opened — the UI shows an approve/reject affordance.
    ApprovalRequested {
        tenant_id: String,
        run_id: String,
        board_id: String,
        workflow_id: String,
        step_id: String,
        name: String,
        stage: String,
        requested_at: i64,
    },
    /// A gate was resolved. `decision` ∈ approved | rejected.
    ApprovalResolved {
        tenant_id: String,
        run_id: String,
        board_id: String,
        workflow_id: String,
        step_id: String,
        stage: String,
        decision: String,
        by: String,
        at: i64,
    },
    /// A workflow run finished. `state` ∈ done | failed | cancelled.
    WorkflowRunFinished {
        tenant_id: String,
        run_id: String,
        board_id: String,
        workflow_id: String,
        state: String,
        finished_at: i64,
    },
    /// The rolled-up read-model (the producer aggregates obs → one snapshot).
    WorkflowStatsUpdated {
        tenant_id: String,
        run_id: String,
        board_id: String,
        workflow_id: String,
        snapshot: crate::dashboard::DashboardSnapshot,
    },
    /// Unread counts changed (R10FB §N). Carries the full `{scope_id: count}` map (board,
    /// workspace and group ids) so the app updates badges LIVE at all three levels and the
    /// dock total without traversing away. Additive, receive-only.
    UnreadChanged {
        counts: std::collections::HashMap<String, i64>,
    },
}

