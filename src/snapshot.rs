//! Snapshot assembly, application, and incremental catch-up (MESH_HARDENING §5).
//!
//! # Why this module exists
//!
//! The peer-to-peer snapshot already worked (`network_actor::handle_snapshot_server`
//! builds frames from `storage`, `topic_actor::download_snapshot` applies them). But
//! the build/apply logic was inlined in those two actors, and "delta" only meant *live
//! gossip after a FULL snapshot* — a peer that had been offline too long was forced to
//! re-pull the WHOLE group (§0b). This module factors the build + apply into one place
//! and adds the **`since`-bounded incremental** path:
//!
//! - [`build_snapshot_frames`]`(group_id, None)` → the full snapshot (unchanged behavior,
//!   the cold-start / no-common-base fallback).
//! - [`build_snapshot_frames`]`(group_id, Some(t))` → only the rows whose version is
//!   strictly newer than `t` — the missing range a returning peer needs. Frames keep the
//!   exact same wire shape (`SnapshotFrame`), so an old holder/joiner is unaffected.
//!
//! [`group_high_water`] is the watermark a peer sends as its `since` (the max version it
//! already holds). [`apply_snapshot_frame`] is the idempotent upsert-by-id apply, shared
//! by the live download path AND the §11 bundle import — both converge to the same state.
//!
//! Filtering is done in Rust over the existing `storage::*_list_by_*` reads (same `O(state)`
//! the anti-entropy digest already costs) rather than adding a `since` variant of every
//! list query — one filter, one place, far simpler to read.

use std::collections::HashSet;

use anyhow::Result;

use crate::models::protocol::SnapshotFrame;
use crate::storage;

/// The high-water mark for `group_id`: the maximum version timestamp across every row the
/// node holds (the same version columns the anti-entropy digest uses — `created_at` for
/// immutable structure/files, `updated_at` for mutable content/notes/pins, `timestamp` for
/// chats). A returning peer sends this as `since`; the holder serves only rows newer than it.
/// `0` when the group is empty (⇒ a `since=0` pull is a full pull, the safe fallback).
pub fn group_high_water(group_id: &str) -> i64 {
    let mut hi: i64 = 0;
    let mut bump = |v: i64| {
        if v > hi {
            hi = v;
        }
    };

    if let Ok(Some(g)) = storage::group_get(group_id) {
        bump(g.created_at);
    }
    let workspaces = storage::workspace_list_by_group(group_id).unwrap_or_default();
    let ws_ids: Vec<String> = workspaces.iter().map(|w| w.id.clone()).collect();
    for w in &workspaces {
        bump(w.created_at);
    }
    let boards = storage::board_list_by_workspaces(&ws_ids).unwrap_or_default();
    let board_ids: Vec<String> = boards.iter().map(|b| b.id.clone()).collect();
    for b in &boards {
        bump(b.created_at);
    }
    for e in storage::element_list_by_boards(&board_ids).unwrap_or_default() {
        bump(e.updated_at);
    }
    for c in storage::cell_list_by_boards(&board_ids).unwrap_or_default() {
        bump(c.updated_at);
    }
    for ch in storage::chat_list_by_workspaces(&ws_ids).unwrap_or_default() {
        bump(ch.timestamp);
    }
    // feat/notes-constitution: group/tenant-scoped notes anchor at the group id — include
    // them in the watermark exactly as the digest and the serializer do.
    let mut note_anchor_ids = board_ids.clone();
    note_anchor_ids.push(group_id.to_string());
    for nt in storage::note_list_by_boards(&note_anchor_ids).unwrap_or_default() {
        bump(nt.updated_at);
    }
    for p in storage::pin_list_by_boards(&board_ids).unwrap_or_default() {
        bump(p.updated_at);
    }
    // board_metadata (descriptive + pin LWW lanes) and workflow_state are sent in FULL by the
    // snapshot, but their clocks still count toward the watermark so it stays the true max
    // version across every row a peer holds.
    for m in storage::board_metadata_list_by_boards(&board_ids).unwrap_or_default() {
        bump(m.meta_updated_at);
        bump(m.pin_updated_at);
    }
    for ws in storage::workflow_state_list_by_boards(&board_ids).unwrap_or_default() {
        bump(ws.updated_at);
    }
    for f in storage::file_list_by_group(group_id).unwrap_or_default() {
        bump(f.created_at);
    }
    // Review-ledger tables (§6.4) are sent in FULL like `board_metadata`, but their
    // clocks still count toward the watermark so it stays the true max version
    // across every row a peer holds.
    for e in storage::change_entry_list_by_tenant(group_id).unwrap_or_default() {
        bump(e.created_at);
        bump(e.updated_at);
    }
    for v in storage::change_version_list_by_tenant(group_id).unwrap_or_default() {
        bump(v.created_at);
    }
    for b in storage::change_branch_list_by_tenant(group_id).unwrap_or_default() {
        bump(b.created_at);
        bump(b.updated_at);
    }
    for a in storage::change_audit_list_by_tenant(group_id).unwrap_or_default() {
        bump(a.ts);
    }
    for rs in storage::review_state_list_by_tenant(group_id).unwrap_or_default() {
        bump(rs.updated_at);
    }
    hi
}

/// Keep only items whose version is strictly greater than `since` (when `Some`). With
/// `None` (full snapshot) every item passes — identical to the pre-incremental behavior.
fn newer_than<T>(items: Vec<T>, since: Option<i64>, version: impl Fn(&T) -> i64) -> Vec<T> {
    match since {
        None => items,
        Some(t) => items.into_iter().filter(|i| version(i) > t).collect(),
    }
}

/// Build the ordered snapshot frames for `group_id`. With `since = Some(t)` only rows newer
/// than `t` are included — the incremental catch-up (§5). With `since = None` it is the full
/// snapshot (the fallback when no common base exists). Frame ORDER is preserved
/// (Structure → Content → Metadata → Complete) so the existing apply path is unchanged.
///
/// The `Structure` frame always carries the group row itself (1 row, the frame type requires
/// it and apply is idempotent); its `workspaces`/`boards` lists ARE `since`-filtered, so a
/// pure-content delta carries an empty structure beyond the group. Returns an empty vec if
/// the group is unknown (the holder simply serves nothing, exactly as before).
pub fn build_snapshot_frames(group_id: &str, since: Option<i64>) -> Result<Vec<SnapshotFrame>> {
    let Some(group) = storage::group_get(group_id)? else {
        return Ok(Vec::new());
    };

    let workspaces = storage::workspace_list_by_group(group_id)?;
    let ws_ids: Vec<String> = workspaces.iter().map(|w| w.id.clone()).collect();
    let boards = storage::board_list_by_workspaces(&ws_ids)?;
    let board_ids: Vec<String> = boards.iter().map(|b| b.id.clone()).collect();

    // Structure — the group is always present (frame invariant); ws/boards are since-filtered.
    let structure = SnapshotFrame::Structure {
        group,
        workspaces: newer_than(workspaces, since, |w| w.created_at),
        boards: newer_than(boards, since, |b| b.created_at),
    };

    // Content — mutable rows version on updated_at.
    let elements = newer_than(
        storage::element_list_by_boards(&board_ids)?,
        since,
        |e| e.updated_at,
    );
    let cells = newer_than(
        storage::cell_list_by_boards(&board_ids)?,
        since,
        |c| c.updated_at,
    );
    let content = SnapshotFrame::Content { elements, cells };

    // Metadata — chats on timestamp; files/integrations on created_at; notes/pins on updated_at.
    let chats = newer_than(
        storage::chat_list_by_workspaces(&ws_ids)?,
        since,
        |c| c.timestamp,
    );
    let files = newer_than(
        storage::file_list_by_group(group_id)?,
        since,
        |f| f.created_at,
    );
    let integrations = newer_than(
        storage::integration_list_by_group(group_id)?,
        since,
        |i| i.created_at,
    );
    let board_metadata = storage::board_metadata_list_by_boards(&board_ids)?;
    // feat/notes-constitution: group/tenant-scoped notes anchor at the group id, so the
    // group id joins the anchor set — a scoped note repairs/cold-joins like a board note.
    let mut note_anchor_ids = board_ids.clone();
    note_anchor_ids.push(group_id.to_string());
    let notes = newer_than(
        storage::note_list_by_boards(&note_anchor_ids)?,
        since,
        |n| n.updated_at,
    );
    let pins = newer_than(
        storage::pin_list_by_boards(&board_ids)?,
        since,
        |p| p.updated_at,
    );
    // R12 D2/E1 workflow lifecycle state — sent in FULL like `board_metadata` (one tiny row per
    // deployed board, applied via the idempotent LWW `workflow_state_upsert`), so an incremental
    // catch-up still carries it regardless of `since` and a returning peer reconciles a deploy/lock
    // it missed while offline.
    let workflow_states = storage::workflow_state_list_by_boards(&board_ids)?;
    // CYAN_FORMAT_SPEC §6.4 — the five review-ledger tables (tenant == group id), also sent in
    // FULL like `board_metadata`: ledger rows are tiny text, and the union/LWW apply paths make
    // a full re-send converge without churn, so no `since` filter can ever mask a lane.
    let change_entries = storage::change_entry_list_by_tenant(group_id)?;
    let change_versions = storage::change_version_list_by_tenant(group_id)?;
    let change_branches = storage::change_branch_list_by_tenant(group_id)?;
    let change_audits = storage::change_audit_list_by_tenant(group_id)?;
    let review_states = storage::review_state_list_by_tenant(group_id)?;
    let metadata = SnapshotFrame::Metadata {
        chats,
        files,
        integrations,
        board_metadata,
        notes,
        pins,
        workflow_states,
        change_entries,
        change_versions,
        change_branches,
        change_audits,
        review_states,
    };

    Ok(vec![structure, content, metadata, SnapshotFrame::Complete])
}

/// Count the DATA rows a frame carries, EXCLUDING the always-present single group row in
/// `Structure` (so an incremental delta of M content rows counts M, not M+1). The
/// transfer-size oracle for the "pulled only the delta, not a full re-snapshot" property.
pub fn frame_row_count(frame: &SnapshotFrame) -> u64 {
    match frame {
        SnapshotFrame::Structure { workspaces, boards, .. } => {
            (workspaces.len() + boards.len()) as u64
        }
        SnapshotFrame::Content { elements, cells } => (elements.len() + cells.len()) as u64,
        SnapshotFrame::Metadata {
            chats,
            files,
            integrations,
            board_metadata,
            notes,
            pins,
            workflow_states,
            change_entries,
            change_versions,
            change_branches,
            change_audits,
            review_states,
        } => {
            (chats.len()
                + files.len()
                + integrations.len()
                + board_metadata.len()
                + notes.len()
                + pins.len()
                + workflow_states.len()
                + change_entries.len()
                + change_versions.len()
                + change_branches.len()
                + change_audits.len()
                + review_states.len()) as u64
        }
        SnapshotFrame::Complete => 0,
    }
}

/// Total data rows across a set of frames (sum of [`frame_row_count`]).
pub fn frames_row_count(frames: &[SnapshotFrame]) -> u64 {
    frames.iter().map(frame_row_count).sum()
}

/// Apply ONE snapshot frame to `storage`, idempotent upsert-by-id. This is the single apply
/// path shared by the live download (`topic_actor::download_snapshot`) and the §11 bundle
/// import — so a P2P catch-up and an air-gapped import converge to the identical state.
/// Does NOT emit any `SwiftEvent` (the caller owns progress events); pure storage writes.
pub fn apply_snapshot_frame(frame: &SnapshotFrame) -> Result<()> {
    match frame {
        SnapshotFrame::Structure { group, workspaces, boards } => {
            storage::group_insert_simple(&group.id, &group.name, &group.icon, &group.color)?;
            for w in workspaces {
                storage::workspace_insert(w)?;
            }
            for b in boards {
                storage::board_insert_simple(&b.id, &b.workspace_id, &b.name, b.created_at)?;
            }
        }
        SnapshotFrame::Content { elements, cells } => {
            for e in elements {
                storage::element_insert_simple(
                    &e.id,
                    &e.board_id,
                    &e.element_type,
                    e.x,
                    e.y,
                    e.width,
                    e.height,
                    e.z_index,
                    e.style_json.as_deref(),
                    e.content_json.as_deref(),
                    e.created_at,
                    e.updated_at,
                )?;
            }
            for c in cells {
                storage::cell_insert_simple(
                    &c.id,
                    &c.board_id,
                    &c.cell_type,
                    c.cell_order,
                    c.content.as_deref(),
                    c.output.as_deref(),
                    c.collapsed,
                    c.height,
                    c.metadata_json.as_deref(),
                    c.created_at,
                    c.updated_at,
                )?;
            }
        }
        SnapshotFrame::Metadata {
            chats,
            files,
            integrations,
            board_metadata,
            notes,
            pins,
            workflow_states,
            change_entries,
            change_versions,
            change_branches,
            change_audits,
            review_states,
        } => {
            for ch in chats {
                // R11 §1: chat is board-scoped. A pre-R11 frame may omit board_id; fall back
                // to the workspace so the row is never dropped (the migration re-keys it).
                let board_key = if ch.board_id.is_empty() { &ch.workspace_id } else { &ch.board_id };
                storage::chat_insert_simple(
                    &ch.id,
                    board_key,
                    &ch.workspace_id,
                    &ch.message,
                    &ch.author,
                    ch.parent_id.as_deref(),
                    ch.timestamp,
                    ch.anchor_kind.as_deref(),
                    ch.anchor_id.as_deref(),
                )?;
            }
            for f in files {
                storage::file_insert_simple(
                    &f.id,
                    f.group_id.as_deref(),
                    f.workspace_id.as_deref(),
                    f.board_id.as_deref(),
                    &f.name,
                    &f.hash,
                    f.size,
                    f.source_peer.as_deref(),
                    f.created_at,
                )?;
            }
            for i in integrations {
                storage::integration_insert(
                    &i.id,
                    &i.scope_type,
                    &i.scope_id,
                    &i.integration_type,
                    &i.config,
                    i.created_at,
                )?;
            }
            for m in board_metadata {
                storage::board_metadata_upsert(
                    &m.board_id,
                    &m.labels,
                    m.rating,
                    m.view_count,
                    m.contains_model.as_deref(),
                    &m.contains_skills,
                    Some(&m.board_type),
                    m.last_accessed,
                    m.is_pinned,
                    m.meta_updated_at,
                    m.pin_updated_at,
                )?;
            }
            for n in notes {
                // LENS_AI_NOTES P1 — USER SCOPE IS SOVEREIGN: a holder never serializes
                // user-scoped notes (`note_list_by_boards` excludes them), so one in a
                // frame is foreign. Drop it — a snapshot must not write into this
                // node's sovereign layer.
                if n.scope == "user" {
                    tracing::debug!("dropping inbound user-scoped note {} from snapshot", n.id);
                    continue;
                }
                storage::note_upsert(n)?;
            }
            for p in pins {
                storage::pin_upsert(p)?;
            }
            for ws in workflow_states {
                storage::workflow_state_upsert(ws)?;
            }
            // Review-ledger tables (§6.4): the same union/LWW apply fns the live
            // gossip deltas use, so a snapshot merge and a delta converge identically.
            // Audits BEFORE entries is not required (both are unions), but entries
            // before versions keeps `version_ref`/`entry_hashes` resolvable sooner.
            for e in change_entries {
                storage::change_entry_apply(e)?;
            }
            for v in change_versions {
                storage::change_version_apply(v)?;
            }
            for b in change_branches {
                storage::change_branch_head_apply(
                    &b.tenant_id,
                    &b.asset_hash,
                    &b.branch,
                    b.head_version.as_deref(),
                    b.updated_at,
                )?;
            }
            for a in change_audits {
                storage::change_audit_apply(a)?;
            }
            for rs in review_states {
                storage::review_state_apply(rs)?;
            }
        }
        SnapshotFrame::Complete => {}
    }
    Ok(())
}

/// Apply a whole set of frames in order (the bundle-import convenience over
/// [`apply_snapshot_frame`]).
pub fn apply_snapshot_frames(frames: &[SnapshotFrame]) -> Result<()> {
    for f in frames {
        apply_snapshot_frame(f)?;
    }
    Ok(())
}

/// Pick the CLOSEST reachable holder to catch up from (MESH_HARDENING §5).
///
/// Preference order, deterministic given the inputs:
/// 1. An offer that is a **direct LAN/mesh neighbor** (`lan_peers`) — the lowest-latency,
///    relay-free path. First in sorted order so the pick is stable across runs.
/// 2. Otherwise any offer (a remoter device holder).
/// 3. Otherwise the configured **super-peer** (e.g. the Lens hold-and-serve node) as the
///    last-resort holder when no device peer offered.
///
/// Returns `None` only when there are no offers and no super-peer.
pub fn pick_catchup_holder(
    offers: &[String],
    lan_peers: &HashSet<String>,
    super_peer: Option<&str>,
) -> Option<String> {
    let mut lan: Vec<&String> = offers.iter().filter(|o| lan_peers.contains(*o)).collect();
    if !lan.is_empty() {
        lan.sort();
        return Some(lan[0].clone());
    }
    if let Some(first) = offers.iter().min() {
        return Some(first.clone());
    }
    super_peer.map(|s| s.to_string())
}
