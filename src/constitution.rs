//! feat/notes-constitution + LENS_AI_NOTES P1 + A2 structured notes — the MERGE
//! RESOLVER.
//!
//! The scope CHAIN — tenant ⊕ group ⊕ project ⊕ board ⊕ workflow ⊕ producer ⊕
//! role ⊕ user — merges notes into the EFFECTIVE constitution + preferences: the
//! exact strings that populate `ProposeCtx.constitution` / `.preferences` (the
//! frozen `propose_ops` seam) and the Lens `constitution_markdown` context.
//!
//! Merge rule: labeled markdown sections, most-general first, most-specific LAST —
//! so the USER section wins on conflict in-context, and the precedence rule is
//! stated in the merged text itself so any consumer (LLM or human) reads the
//! conflict rule alongside the rules. `creative-dna` notes ride the constitution
//! rail as their own labeled subsection per scope ("Creative DNA (<label>)").
//! Absent scopes produce no section; nothing at all produces the EMPTY string (a
//! valid, tested result). H1/H2 headers stay byte-identical to the pre-A2 output
//! for chains whose `project_id`/`role_id` are `None` (frozen consumers assert).
//!
//! The user link is SOVEREIGN (local-first): it merges from the LOCAL ledger only —
//! user-scoped notes are never gossiped or snapshot (see `dispatch_put_note` and
//! `storage::note_list_for_sync`), so this resolver is the only place they act.
//!
//! A2 adds provenance: [`resolve_with_provenance`] returns the merged strings PLUS
//! the `constitution.v1` blake3 hash, the ordered `contributing` tuples, and the
//! typed [`ResolvedNote`] rows (`notes`, 1:1 with `contributing` — the C-A6-A2
//! seam, D-A2.25). All four are computed from ONE `&Connection` read set — A6's
//! `constitution_hard::classify_hard` is a pure post-pass over `notes` and must
//! NEVER re-query (a second read races LWW and can desync from the hash).
//!
//! Every query is tenant-enforced; the resolver never crosses the tenant boundary.

use crate::models::dto::NoteDTO;
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

/// The FULL scope chain (LENS_AI_NOTES P1, extended by A2 §5). Each link's id is
/// that scope's anchor (what the note row carries in `board_id`); a `None` link
/// is simply absent from the merge — the 3-scope resolver is exactly a chain with
/// the extended links `None`. Chain position = the 0-based index in the frozen
/// most-general-first link array (== `ResolvedNote.link_index`, one convention):
/// tenant 0 · group 1 · project 2 · board 3 · workflow 4 · producer 5 · role 6 ·
/// user 7.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopeChain {
    pub tenant_id: String,
    pub group_id: Option<String>,
    /// A2 — the board's WORKSPACE id (`project` scope anchor); `None` ⇒ link absent.
    pub project_id: Option<String>,
    pub board_id: String,
    pub workflow_id: Option<String>,
    /// Client-producer PERSON id (opaque) — the original meaning, unchanged.
    pub producer_id: Option<String>,
    /// A2 — a bare `PRODUCTION_ROLE_VOCAB` slug; `None` ⇒ link absent. The role
    /// link's query anchor is the GROUP id (else tenant) via
    /// `note_list_role_scoped_with` — a slug anchor would be replication-dead.
    pub role_id: Option<String>,
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
            project_id: None,
            board_id: board_id.to_string(),
            workflow_id: None,
            producer_id: None,
            role_id: None,
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
    let constitution = merge_kind_collect(conn, chain, "constitution", None)?;
    let preferences = merge_kind_collect(conn, chain, "preference", None)?;
    tracing::info!(
        tenant_id = %chain.tenant_id,
        "obs constitution_resolved board={} constitution_bytes={} preference_bytes={}",
        chain.board_id,
        constitution.len(),
        preferences.len()
    );
    Ok(EffectiveNotes { constitution, preferences })
}

/// One link of the frozen 8-link array: `(wire scope, section label, anchor,
/// role slug, chain position)`. The role link is present only when the chain
/// carries a slug; its anchor is the GROUP id (else tenant — §1's group-anchored
/// rule, so role rules actually replicate).
struct Link<'a> {
    scope: &'static str,
    label: String,
    anchor: Option<&'a str>,
    role_slug: Option<&'a str>,
    index: u8,
}

/// The frozen most-general-first link array (NEVER reordered): tenant → group →
/// project → board → workflow → producer → role → user. Index == §1's 0-based
/// "Chain pos" column == `ResolvedNote.link_index` (T32c pins the convention).
fn links<'a>(chain: &'a ScopeChain) -> Vec<Link<'a>> {
    let role_anchor: Option<&str> = chain
        .role_id
        .as_deref()
        .map(|_| chain.group_id.as_deref().unwrap_or(chain.tenant_id.as_str()));
    vec![
        Link { scope: "tenant", label: "Tenant".to_string(), anchor: Some(chain.tenant_id.as_str()), role_slug: None, index: 0 },
        Link { scope: "group", label: "Group".to_string(), anchor: chain.group_id.as_deref(), role_slug: None, index: 1 },
        Link { scope: "project", label: "Project".to_string(), anchor: chain.project_id.as_deref(), role_slug: None, index: 2 },
        Link { scope: "board", label: "Board".to_string(), anchor: Some(chain.board_id.as_str()), role_slug: None, index: 3 },
        Link { scope: "workflow", label: "Workflow".to_string(), anchor: chain.workflow_id.as_deref(), role_slug: None, index: 4 },
        Link { scope: "producer", label: "Producer".to_string(), anchor: chain.producer_id.as_deref(), role_slug: None, index: 5 },
        Link {
            scope: "role",
            label: chain.role_id.as_deref().map(|s| format!("Role: {s}")).unwrap_or_default(),
            anchor: role_anchor,
            role_slug: chain.role_id.as_deref(),
            index: 6,
        },
        Link { scope: "user", label: "User".to_string(), anchor: chain.user_id.as_deref(), role_slug: None, index: 7 },
    ]
}

/// Fetch one link's rows of one kind — the role link reads the group-anchored
/// slug lane (`note_list_role_scoped_with`); every other link reads the plain
/// scoped lane. Deterministic order `(created_at, id)` in both.
fn fetch_link(
    conn: &rusqlite::Connection,
    chain: &ScopeChain,
    link: &Link<'_>,
    kind: &str,
) -> anyhow::Result<Vec<NoteDTO>> {
    let Some(anchor) = link.anchor else { return Ok(Vec::new()) };
    match link.role_slug {
        Some(slug) => {
            storage::note_list_role_scoped_with(conn, &chain.tenant_id, anchor, slug, kind)
        }
        None => storage::note_list_scoped_with(conn, &chain.tenant_id, link.scope, anchor, kind),
    }
}

/// The five precedence headers (§5) — selected by CHAIN SHAPE, top-down, first
/// match. H1/H2 are byte-identical frozen strings (pre-A2 consumers assert on
/// them); H3-H5 state the extended rule for chains carrying project/role links.
fn precedence_header(chain: &ScopeChain) -> &'static str {
    let role = chain.role_id.is_some();
    let project = chain.project_id.is_some();
    let extended =
        chain.workflow_id.is_some() || chain.producer_id.is_some() || chain.user_id.is_some();
    if role && project {
        "Precedence: user > role > producer > workflow > board > project > group > tenant — the most specific section wins on conflict."
    } else if role {
        "Precedence: user > role > producer > workflow > board > group > tenant — the most specific section wins on conflict."
    } else if project {
        "Precedence: user > producer > workflow > board > project > group > tenant — the most specific section wins on conflict."
    } else if extended {
        "Precedence: user > producer > workflow > board > group > tenant — the most specific section wins on conflict."
    } else {
        "Precedence: board > group > tenant — the most specific section wins on conflict."
    }
}

/// Merge one kind across the chain into labeled markdown, optionally collecting
/// the fetched rows (the ONE traversal both the strings and the A2 provenance
/// come from — D-A2.25's "rows already in hand, no extra queries"). Deterministic:
/// link order is the frozen array, rows order by (created_at, id). For the
/// constitution kind, each link's `creative-dna` notes follow that link's rules
/// as their own labeled subsection — so DNA obeys the same specificity ordering.
fn merge_kind_collect(
    conn: &rusqlite::Connection,
    chain: &ScopeChain,
    kind: &str,
    mut collected: Option<&mut Vec<ResolvedNote>>,
) -> anyhow::Result<String> {
    let mut sections: Vec<String> = Vec::new();
    for link in links(chain) {
        if link.anchor.is_none() {
            continue;
        }
        let rows = fetch_link(conn, chain, &link, kind)?;
        if let Some(out) = collected.as_deref_mut() {
            out.extend(rows.iter().map(|n| ResolvedNote::from_row(n, link.index)));
        }
        push_section(&mut sections, &link.label, &rows);
        if kind == "constitution" {
            let dna = fetch_link(conn, chain, &link, "creative-dna")?;
            if let Some(out) = collected.as_deref_mut() {
                out.extend(dna.iter().map(|n| ResolvedNote::from_row(n, link.index)));
            }
            push_section(&mut sections, &format!("Creative DNA ({})", link.label), &dna);
        }
    }

    if sections.is_empty() {
        return Ok(String::new());
    }
    Ok(format!("{}\n\n{}", precedence_header(chain), sections.join("\n\n")))
}

fn push_section(sections: &mut Vec<String>, label: &str, notes: &[NoteDTO]) {
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

// ═══════════════════════════════════════════════════════════════════════════
// A2 §5 — chain population, provenance, and the `constitution.v1` hash
// ═══════════════════════════════════════════════════════════════════════════

/// Per-call resolve options (§5). The **struct default is the sovereignty-safe
/// one**: `include_user: false` (the cloud rail's REQUIRED, never-overridable
/// posture), `include_project: true`. Shipping callers pass `workflow_id`/
/// `producer_id` as `None` — no fake ids from `workflow_id == board_id` (D3).
#[derive(Debug, Clone)]
pub struct ResolveOpts {
    pub workflow_id: Option<String>,
    pub producer_id: Option<String>,
    /// Explicit craft-role override; `None` ⇒ the device `production_role`
    /// local pref (§7) ⇒ absent. Validated ∈ `PRODUCTION_ROLE_VOCAB` either way.
    pub production_role: Option<String>,
    pub include_user: bool,
    pub include_project: bool,
}

impl Default for ResolveOpts {
    fn default() -> Self {
        ResolveOpts {
            workflow_id: None,
            producer_id: None,
            production_role: None,
            include_user: false,
            include_project: true,
        }
    }
}

/// Populate a board's [`ScopeChain`] (§5): tenant = `review_loop::board_tenant`,
/// group = `board_get_group_id_with`, project = `board_get_workspace_id_with`
/// (when `include_project`), workflow/producer = caller-supplied, role = the
/// explicit opt else the device `production_role` pref (validated ∈ vocab), user
/// = `Some(node_id)` only when `include_user` (`node_id` is unused otherwise —
/// cloud-rail callers pass `""`).
pub fn resolve_chain(
    conn: &rusqlite::Connection,
    board_id: &str,
    node_id: &str,
    opts: &ResolveOpts,
) -> ScopeChain {
    let tenant_id = crate::review_loop::board_tenant(conn, board_id);
    let group_id = storage::board_get_group_id_with(conn, board_id);
    let project_id = if opts.include_project {
        storage::board_get_workspace_id_with(conn, board_id)
    } else {
        None
    };
    let role_id = opts
        .production_role
        .clone()
        .or_else(|| storage::local_pref_get_with(conn, storage::PREF_PRODUCTION_ROLE))
        .filter(|r| crate::models::dto::production_role_valid(r));
    ScopeChain {
        tenant_id,
        group_id,
        project_id,
        board_id: board_id.to_string(),
        workflow_id: opts.workflow_id.clone(),
        producer_id: opts.producer_id.clone(),
        role_id,
        user_id: if opts.include_user { Some(node_id.to_string()) } else { None },
    }
}

/// One contributing note's identity tuple — a hash part and the FFI
/// `contributing_ids` source.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Contributing {
    pub id: String,
    pub scope: String,
    pub kind: String,
    pub updated_at: i64,
}

/// The typed snapshot of one contributing row (D-A2.25 — the C-A6-A2 seam).
/// FFI-INTERNAL: never serialized onto a verb; `constitution_hard::classify_hard`
/// consumes it as a pure post-pass.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedNote {
    pub id: String,
    pub scope: String,
    pub kind: String,
    pub text: String,
    /// A1's typed payload, verbatim.
    pub payload: Option<serde_json::Value>,
    pub author_role: Option<String>,
    pub updated_at: i64,
    /// 0-based chain position == §1's "Chain pos" column (T32c — one convention).
    pub link_index: u8,
}

impl ResolvedNote {
    fn from_row(n: &NoteDTO, link_index: u8) -> Self {
        ResolvedNote {
            id: n.id.clone(),
            scope: n.scope.clone(),
            kind: n.kind.clone(),
            text: n.text.clone(),
            payload: n.payload.clone(),
            author_role: n.author_role.clone(),
            updated_at: n.updated_at,
            link_index,
        }
    }
}

/// The provenance-bearing resolve result (§5, REV 2 / D-A2.25). `notes[i]` and
/// `contributing[i]` describe the SAME row ∀i, in the NORMATIVE kind-major
/// canonical order: (1) the constitution pass over the 8 links in chain order —
/// per present link its `constitution` rows then its `creative-dna` rows, each
/// `(created_at, id)`; (2) the preference pass over all links again. Never
/// link-major; no dedup step exists.
#[derive(Debug, Clone)]
pub struct ResolvedConstitution {
    /// Merged markdown (byte-identical to [`effective_notes_chain_with`]'s).
    pub constitution: String,
    pub preferences: String,
    /// 64-hex blake3, `constitution.v1`-tagged. Computed on EVERY Ok resolve —
    /// an empty resolve still hashes (over `chain_canonical` + zero parts); a
    /// resolver Err is `Err`, never a hash (the seam-level Err ≠ empty
    /// discrimination, D-A2.21).
    pub hash: String,
    pub contributing: Vec<Contributing>,
    /// The typed snapshot, 1:1 with `contributing` — see the module docs for the
    /// snapshot-consistency guarantee (ONE read set; A6 never re-queries).
    pub notes: Vec<ResolvedNote>,
}

/// The `constitution.v1` hash version tag. Any change to part ordering, content,
/// or traversal REQUIRES bumping `.v1` → `.v2` (A6 keys its distillation cache
/// on this hash; T32's pinned constant is the tripwire).
pub const CONSTITUTION_HASH_VERSION: &str = "constitution.v1";

/// The canonical chain string — the FIRST hash part, so identical text over
/// different chains still hashes differently.
fn chain_canonical(chain: &ScopeChain) -> String {
    let opt = |o: &Option<String>| o.clone().unwrap_or_else(|| "-".to_string());
    format!(
        "tenant={}\x1fgroup={}\x1fproject={}\x1fboard={}\x1fworkflow={}\x1fproducer={}\x1frole={}\x1fuser={}",
        chain.tenant_id,
        opt(&chain.group_id),
        opt(&chain.project_id),
        chain.board_id,
        opt(&chain.workflow_id),
        opt(&chain.producer_id),
        opt(&chain.role_id),
        opt(&chain.user_id),
    )
}

/// `blake3( "constitution.v1" ⊕ per part: 0x00 ⊕ u64-LE(len) ⊕ part )` — the
/// same length-prefix discipline as the lens `hash_key`, so no part boundary is
/// ambiguous. Parts = `[chain_canonical] ++ ["{scope}\x1f{kind}\x1f{id}\x1f{updated_at}"
/// per contributing note]`.
fn constitution_hash(chain: &ScopeChain, contributing: &[Contributing]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(CONSTITUTION_HASH_VERSION.as_bytes());
    let mut part = |p: &str| {
        hasher.update(&[0u8]);
        hasher.update(&(p.len() as u64).to_le_bytes());
        hasher.update(p.as_bytes());
    };
    part(&chain_canonical(chain));
    for c in contributing {
        part(&format!("{}\x1f{}\x1f{}\x1f{}", c.scope, c.kind, c.id, c.updated_at));
    }
    hasher.finalize().to_hex().to_string()
}

/// Resolve a chain WITH provenance (§5): merged strings + the `constitution.v1`
/// hash + the ordered contributing tuples + the typed rows — all from ONE
/// traversal over ONE connection (snapshot-consistent by construction). Hash
/// stability: the hash flips iff `chain_canonical` or any contributing
/// `(scope, kind, id, updated_at)` tuple changes; non-contributing kinds never
/// move it; hashes are LOCAL (peer divergence ⇒ a cache MISS at worst).
pub fn resolve_with_provenance(
    conn: &rusqlite::Connection,
    chain: &ScopeChain,
) -> anyhow::Result<ResolvedConstitution> {
    let mut notes: Vec<ResolvedNote> = Vec::new();
    let constitution = merge_kind_collect(conn, chain, "constitution", Some(&mut notes))?;
    let preferences = merge_kind_collect(conn, chain, "preference", Some(&mut notes))?;
    let contributing: Vec<Contributing> = notes
        .iter()
        .map(|n| Contributing {
            id: n.id.clone(),
            scope: n.scope.clone(),
            kind: n.kind.clone(),
            updated_at: n.updated_at,
        })
        .collect();
    let hash = constitution_hash(chain, &contributing);
    tracing::info!(
        tenant_id = %chain.tenant_id,
        "obs constitution_resolved board={} constitution_bytes={} preference_bytes={} contributing={} hash={}",
        chain.board_id,
        constitution.len(),
        preferences.len(),
        contributing.len(),
        hash
    );
    Ok(ResolvedConstitution { constitution, preferences, hash, contributing, notes })
}

/// The **Result-returning** cloud-rail resolve (PLAN 2.8): `resolve_chain`
/// (include_user: false by construction — sovereignty is structural) +
/// [`resolve_with_provenance`]. It does NOT route through the Err-swallowing
/// `review_loop::board_constitution_markdown` / `effective_notes_for_board`
/// helpers (those stay untouched for frozen consumers) — on this seam a resolver
/// Err is `Err`, DISTINCT from resolved-empty (D-A2.21).
///
/// NOTE (deviation, file ownership): the DESIGN places this fn in
/// `review_loop.rs` and wires it into `pipeline_executor.rs`'s lens-execute call
/// site under the one `try_db_read` guard (obs `constitution_read_budget_missed`
/// on a guard miss vs `constitution_resolve_error` on Err-in-guard, then the §5
/// send table via [`execute_wire_pair`]). Both files are owned by another
/// workstream in this pass, so the fn lands HERE (additive; same signature
/// shape) and the call-site swap is deferred to that owner.
pub fn board_constitution_markdown_chain(
    conn: &rusqlite::Connection,
    board_id: &str,
) -> anyhow::Result<ResolvedConstitution> {
    let chain = resolve_chain(conn, board_id, "", &ResolveOpts::default());
    resolve_with_provenance(conn, &chain)
}

/// The §5 EXECUTE-wire send table over an `Ok` resolve (T34b pins the rows):
/// markdown non-empty ⇒ `(Some(markdown), Some(hash))`; markdown EMPTY ⇒
/// `(None, None)` — resolved-empty stays ABSENT on the live execute wire (zero
/// lens step-key churn; the `""`+real-hash empty-vs-absent contract lives on
/// `cyan_constitution_effective` + the Rust seam, NOT this wire). The pair is
/// ATOMIC in every row — a hash never travels without the markdown it hashes.
/// Err/budget-miss rows never reach this fn (the caller sends both absent).
pub fn execute_wire_pair(resolved: &ResolvedConstitution) -> (Option<String>, Option<String>) {
    if resolved.constitution.is_empty() {
        (None, None)
    } else {
        (Some(resolved.constitution.clone()), Some(resolved.hash.clone()))
    }
}
