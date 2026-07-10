//! feat/notes-constitution — the MERGE RESOLVER.
//!
//! tenant ⊕ group ⊕ board notes → the board's EFFECTIVE constitution + preferences:
//! the exact strings that populate `ProposeCtx.constitution` / `.preferences` (the
//! frozen `propose_ops` seam) and the Lens `constitution_markdown` context.
//!
//! Merge rule: labeled markdown sections, most-general first, most-specific LAST —
//! so the BOARD section wins on conflict in-context ("board > group > tenant"), and
//! the precedence rule is stated in the merged text itself so any consumer (LLM or
//! human) reads the conflict rule alongside the rules. Absent scopes produce no
//! section; nothing at all produces the EMPTY string (a valid, tested result).
//!
//! Every query is tenant-enforced; the resolver never crosses the tenant boundary.

use crate::storage;

/// The two merged context strings a board's proposer consumes. Field names mirror
/// `ProposeCtx` so the JOIN wiring is mechanical.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveNotes {
    /// Merged `kind = "constitution"` notes (markdown), board wins on conflict.
    pub constitution: String,
    /// Merged `kind = "preference"` notes (markdown), same precedence.
    pub preferences: String,
}

/// Resolve the EFFECTIVE constitution + preferences for a board.
///
/// `tenant_id` scopes every query (isolation); `group_id` is the board's group when
/// known (`None` for an un-grouped board — its section is simply absent); `board_id`
/// is the board. Scope anchors follow the notes model: tenant notes anchor at the
/// tenant id, group notes at the group id, board notes at the board id.
pub fn effective_notes(
    tenant_id: &str,
    group_id: Option<&str>,
    board_id: &str,
) -> anyhow::Result<EffectiveNotes> {
    let constitution = merge_kind(tenant_id, group_id, board_id, "constitution")?;
    let preferences = merge_kind(tenant_id, group_id, board_id, "preference")?;
    tracing::info!(
        tenant_id = %tenant_id,
        "obs constitution_resolved board={board_id} constitution_bytes={} preference_bytes={}",
        constitution.len(),
        preferences.len()
    );
    Ok(EffectiveNotes { constitution, preferences })
}

/// Merge one kind across the three scopes into labeled markdown. Deterministic:
/// scope order is fixed (tenant → group → board), rows order by (created_at, id)
/// via `note_list_scoped`.
fn merge_kind(
    tenant_id: &str,
    group_id: Option<&str>,
    board_id: &str,
    kind: &str,
) -> anyhow::Result<String> {
    let mut sections: Vec<String> = Vec::new();

    push_section(&mut sections, "Tenant", storage::note_list_scoped(tenant_id, "tenant", tenant_id, kind)?);
    if let Some(gid) = group_id {
        push_section(&mut sections, "Group", storage::note_list_scoped(tenant_id, "group", gid, kind)?);
    }
    push_section(&mut sections, "Board", storage::note_list_scoped(tenant_id, "board", board_id, kind)?);

    if sections.is_empty() {
        return Ok(String::new());
    }
    Ok(format!(
        "Precedence: board > group > tenant — the most specific section wins on conflict.\n\n{}",
        sections.join("\n\n")
    ))
}

fn push_section(sections: &mut Vec<String>, label: &str, notes: Vec<crate::models::dto::NoteDTO>) {
    let texts: Vec<&str> = notes
        .iter()
        .map(|n| n.text.trim())
        .filter(|t| !t.is_empty())
        .collect();
    if texts.is_empty() {
        return; // absent/blank scope ⇒ no empty section
    }
    sections.push(format!("## {label}\n{}", texts.join("\n\n")));
}
