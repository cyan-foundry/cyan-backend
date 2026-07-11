//! feat/notes-constitution + LENS_AI_NOTES P1 — the MERGE RESOLVER.
//!
//! The scope CHAIN — tenant ⊕ group ⊕ board ⊕ workflow ⊕ producer ⊕ user — merges
//! notes into the EFFECTIVE constitution + preferences: the exact strings that
//! populate `ProposeCtx.constitution` / `.preferences` (the frozen `propose_ops`
//! seam) and the Lens `constitution_markdown` context.
//!
//! Merge rule: labeled markdown sections, most-general first, most-specific LAST —
//! so the USER section wins on conflict in-context ("user > producer > workflow >
//! board > group > tenant"), and the precedence rule is stated in the merged text
//! itself so any consumer (LLM or human) reads the conflict rule alongside the
//! rules. `creative-dna` notes ride the constitution rail as their own labeled
//! subsection per scope ("Creative DNA (<scope>)"). Absent scopes produce no
//! section; nothing at all produces the EMPTY string (a valid, tested result).
//!
//! The user link is SOVEREIGN (local-first): it merges from the LOCAL ledger only —
//! user-scoped notes are never gossiped or snapshot (see `dispatch_put_note` and
//! `storage::note_list_by_boards`), so this resolver is the only place they act.
//!
//! Every query is tenant-enforced; the resolver never crosses the tenant boundary.

use crate::storage;

/// The two merged context strings a board's proposer consumes. Field names mirror
/// `ProposeCtx` so the JOIN wiring is mechanical.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectiveNotes {
    /// Merged `kind = "constitution"` notes (markdown, plus per-scope
    /// `"creative-dna"` subsections), most-specific scope wins on conflict.
    pub constitution: String,
    /// Merged `kind = "preference"` notes (markdown), same precedence.
    pub preferences: String,
}

/// The FULL scope chain (LENS_AI_NOTES P1). Each link's id is that scope's anchor
/// (what the note row carries in `board_id`); a `None` link is simply absent from
/// the merge — the 3-scope resolver is exactly a chain with the new links `None`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeChain {
    pub tenant_id: String,
    pub group_id: Option<String>,
    pub board_id: String,
    pub workflow_id: Option<String>,
    pub producer_id: Option<String>,
    /// The sovereign, innermost link — merged from the local ledger only.
    pub user_id: Option<String>,
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
    let conn = storage::db()
        .lock()
        .map_err(|e| anyhow::anyhow!("DB lock: {e}"))?;
    effective_notes_with(&conn, tenant_id, group_id, board_id)
}

/// [`effective_notes`] against an ALREADY-HELD connection — the JOIN's seam for
/// callers running inside a dispatch that owns the global DB mutex (the spine's
/// `propose_from_note*` receives `&Connection`; re-locking self-deadlocks, the
/// std Mutex is not reentrant). Same resolution, same obs line.
pub fn effective_notes_with(
    conn: &rusqlite::Connection,
    tenant_id: &str,
    group_id: Option<&str>,
    board_id: &str,
) -> anyhow::Result<EffectiveNotes> {
    effective_notes_chain_with(
        conn,
        &ScopeChain {
            tenant_id: tenant_id.to_string(),
            group_id: group_id.map(str::to_string),
            board_id: board_id.to_string(),
            workflow_id: None,
            producer_id: None,
            user_id: None,
        },
    )
}

/// Resolve the EFFECTIVE constitution + preferences for a FULL scope chain
/// (LENS_AI_NOTES P1), locking the global DB.
pub fn effective_notes_chain(chain: &ScopeChain) -> anyhow::Result<EffectiveNotes> {
    let conn = storage::db()
        .lock()
        .map_err(|e| anyhow::anyhow!("DB lock: {e}"))?;
    effective_notes_chain_with(&conn, chain)
}

/// [`effective_notes_chain`] against an ALREADY-HELD connection (same non-reentrant
/// mutex contract as [`effective_notes_with`]).
pub fn effective_notes_chain_with(
    conn: &rusqlite::Connection,
    chain: &ScopeChain,
) -> anyhow::Result<EffectiveNotes> {
    let constitution = merge_kind(conn, chain, "constitution")?;
    let preferences = merge_kind(conn, chain, "preference")?;
    tracing::info!(
        tenant_id = %chain.tenant_id,
        "obs constitution_resolved board={} constitution_bytes={} preference_bytes={}",
        chain.board_id,
        constitution.len(),
        preferences.len()
    );
    Ok(EffectiveNotes { constitution, preferences })
}

/// Merge one kind across the chain into labeled markdown. Deterministic: scope
/// order is fixed (tenant → group → board → workflow → producer → user), rows
/// order by (created_at, id) via `note_list_scoped_with`. For the constitution
/// kind, each scope's `creative-dna` notes follow that scope's rules as their own
/// labeled subsection — so DNA obeys the same specificity ordering.
fn merge_kind(
    conn: &rusqlite::Connection,
    chain: &ScopeChain,
    kind: &str,
) -> anyhow::Result<String> {
    let links: [(&str, &str, Option<&str>); 6] = [
        ("tenant", "Tenant", Some(chain.tenant_id.as_str())),
        ("group", "Group", chain.group_id.as_deref()),
        ("board", "Board", Some(chain.board_id.as_str())),
        ("workflow", "Workflow", chain.workflow_id.as_deref()),
        ("producer", "Producer", chain.producer_id.as_deref()),
        ("user", "User", chain.user_id.as_deref()),
    ];

    let mut sections: Vec<String> = Vec::new();
    for (scope, label, anchor) in links {
        let Some(anchor) = anchor else { continue };
        push_section(
            &mut sections,
            label,
            storage::note_list_scoped_with(conn, &chain.tenant_id, scope, anchor, kind)?,
        );
        if kind == "constitution" {
            push_section(
                &mut sections,
                &format!("Creative DNA ({label})"),
                storage::note_list_scoped_with(conn, &chain.tenant_id, scope, anchor, "creative-dna")?,
            );
        }
    }

    if sections.is_empty() {
        return Ok(String::new());
    }
    // The precedence header names exactly the links this chain can carry: a legacy
    // 3-scope chain keeps the pre-P1 header BYTE-IDENTICAL (frozen consumers assert
    // on it); an extended chain states the full rule.
    let extended = chain.workflow_id.is_some() || chain.producer_id.is_some() || chain.user_id.is_some();
    let header = if extended {
        "Precedence: user > producer > workflow > board > group > tenant — the most specific section wins on conflict."
    } else {
        "Precedence: board > group > tenant — the most specific section wins on conflict."
    };
    Ok(format!("{header}\n\n{}", sections.join("\n\n")))
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
