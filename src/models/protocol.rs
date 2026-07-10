// src/models/protocol.rs
//
// Network protocol types for peer-to-peer communication

use serde::{Deserialize, Serialize};

use crate::models::core::{Group, Workspace};
use crate::models::dto::{
    BoardMetadataDTO, ChatDTO, FileDTO, IntegrationBindingDTO,
    NotebookCellDTO, NoteDTO, PinDTO, WhiteboardDTO, WhiteboardElementDTO, WorkflowStateDTO,
};

// ═══════════════════════════════════════════════════════════════════════════
// SNAPSHOT REQUEST - the joiner's opening message on SNAPSHOT_ALPN
// ═══════════════════════════════════════════════════════════════════════════

/// What a joining peer sends (length-prefixed JSON) to ask a holder for a group's
/// snapshot. `grant` is the signed capability-grant QR payload the joiner scanned
/// (`identity::Grant::to_qr_payload`); the holder verifies it before serving when the
/// group is enforced.
///
/// **Backward-compatible wire:** older peers sent the raw `group_id` bytes with no JSON
/// envelope. The server first tries to parse these bytes as a `SnapshotRequest`; if that
/// fails it falls back to treating the whole payload as a bare `group_id` (grant `None`).
/// So an un-enforced group keeps serving legacy clients unchanged.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotRequest {
    pub group_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant: Option<String>,
    /// MESH_HARDENING §5 incremental catch-up: the requester's high-water mark (unix
    /// seconds). When `Some(t)`, the holder serves ONLY rows newer than `t` (the missing
    /// range) instead of a full re-snapshot. Absent (`None`) ⇒ a full snapshot, the
    /// cold-start / no-common-base fallback. Additive + `skip_serializing_if`, so an older
    /// holder ignores it (serves full) and an older joiner never sends it — fully wire-compatible.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<i64>,
}

// ═══════════════════════════════════════════════════════════════════════════
// SNAPSHOT FRAME - Used for peer-to-peer state sync
// ═══════════════════════════════════════════════════════════════════════════

/// Snapshot frames are sent over QUIC to sync group state between peers.
///
/// The protocol sends frames in order:
/// 1. Structure - group, workspaces, boards (UI unblocks immediately)
/// 2. Content - elements, cells (board content populates)
/// 3. Metadata - chats, files, integrations, board_metadata
/// 4. Complete - signals end of transfer
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "frame_type")]
pub enum SnapshotFrame {
    /// Core structure - sent first, unblocks UI immediately
    Structure {
        group: Group,
        workspaces: Vec<Workspace>,
        boards: Vec<WhiteboardDTO>,
    },
    /// All board content combined - single batched transaction
    Content {
        elements: Vec<WhiteboardElementDTO>,
        cells: Vec<NotebookCellDTO>,
    },
    /// All metadata - chats, files, integrations, board metadata, notes
    Metadata {
        chats: Vec<ChatDTO>,
        files: Vec<FileDTO>,
        integrations: Vec<IntegrationBindingDTO>,
        board_metadata: Vec<BoardMetadataDTO>,
        /// ROUND8 §W2 notes. `#[serde(default)]` keeps the frame wire-compatible: an
        /// older holder serializes Metadata without this field, a newer peer fills it
        /// with an empty vec; a newer holder's extra field is ignored by older peers.
        #[serde(default)]
        notes: Vec<NoteDTO>,
        /// ROUND8 §W4 pinned-workflow state. Same wire-compat contract as `notes`.
        #[serde(default)]
        pins: Vec<PinDTO>,
        /// R12 D2/E1 per-board workflow lifecycle state (deployed/dashboard/locked, LWW on
        /// `updated_at`). Same wire-compat contract as `notes`/`pins`: an older holder omits it
        /// (a newer peer fills an empty vec), a newer holder's extra field is ignored by older
        /// peers — so adding it never breaks a mixed-version snapshot transfer.
        #[serde(default)]
        workflow_states: Vec<WorkflowStateDTO>,
        /// CYAN_FORMAT_SPEC §6.4 — the five review-ledger tables, so a cold joiner
        /// gets the FULL ledger with the snapshot. Same wire-compat contract as
        /// `notes`/`pins`; applied via the idempotent union/LWW `changelist::apply_*`
        /// paths, so a replayed frame or a frame racing live deltas never duplicates.
        #[serde(default)]
        change_entries: Vec<crate::changelist::ChangeEntry>,
        #[serde(default)]
        change_versions: Vec<crate::changelist::ChangeVersion>,
        #[serde(default)]
        change_branches: Vec<crate::changelist::ChangeBranch>,
        #[serde(default)]
        change_audits: Vec<crate::changelist::ChangeAudit>,
        #[serde(default)]
        review_states: Vec<crate::review_state::ReviewState>,
    },
    /// Signals transfer complete
    Complete,
}

// ═══════════════════════════════════════════════════════════════════════════
// FILE TRANSFER PROTOCOL
// ═══════════════════════════════════════════════════════════════════════════

/// Messages for the file transfer protocol (FILE_TRANSFER_ALPN).
///
/// Flow:
/// 1. Client sends Request with file_id, hash, and optional resume offset
/// 2. Server responds with Header (or NotFound/Error)
/// 3. Server streams raw bytes
/// 4. Server sends Complete when done
/// 5. Client verifies hash
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "msg_type")]
pub enum FileTransferMsg {
    /// Client requests a file
    Request {
        file_id: String,
        hash: String,
        /// Resume offset for resumable downloads (0 for fresh download)
        offset: u64,
    },
    /// Server responds with file header before streaming bytes
    Header {
        file_id: String,
        file_name: String,
        total_size: u64,
        hash: String,
        byte_offset: u64,
        /// How many bytes follow in this transfer
        byte_length: u64,
    },
    /// Server sends after file data is complete
    Complete {
        file_id: String,
        hash: String,
    },
    /// Server responds that file was not found
    NotFound { file_id: String },
    /// Server responds with error
    Error { file_id: String, message: String },
    /// Client requests a STRIDED slice for the pipelined parallel-stream transfer
    /// (G8 hardening): the file is cut into `chunk_size`-byte chunks counted from
    /// byte 0; stream `index` (of `stride` parallel streams on one connection)
    /// carries chunks `index, index+stride, index+2·stride, …` in ascending order.
    /// The server answers with a `Header` (`byte_length` = this stream's byte
    /// total) followed by the raw chunk bytes. Additive: legacy peers keep using
    /// `Request`, and a legacy server rejecting this variant makes the new client
    /// fall back to the single-stream path.
    RequestStriped {
        file_id: String,
        hash: String,
        chunk_size: u64,
        stride: u32,
        index: u32,
    },
}