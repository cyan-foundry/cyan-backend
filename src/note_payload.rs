//! A1 structured notes — per-kind payload validation, the legal-clearance state
//! machine, and the batch/scene id helpers. Pure functions over
//! `serde_json::Value`: no I/O, no storage, no channels — `dispatch_put_note` (the
//! local write door) is the ONLY caller that enforces anything here.
//!
//! # §4 common rules
//!
//! `payload` must be a JSON **object** when present; `text` stays the freeform
//! field for EVERY kind. Payloads are NOT deny-unknown-fields — extra keys are
//! kept opaque. `v > 1` ⇒ validators skip and the payload stores opaque (the size
//! cap still applies) — EXCEPT `legal-clearance` (§4.9, which rejects). An absent
//! `"v"` is treated as `1`. Size caps: [`PAYLOAD_MAX_BYTES`] for every kind except
//! `script`, which gets [`SCRIPT_PAYLOAD_MAX_BYTES`].
//!
//! **Reserved key `_meta`** (ORCH-4): an optional JSON object carrying
//! AI-structuring provenance, WRITTEN by cyan-lens (A5). Every validator here
//! **ignores-and-preserves** it — neither required nor an unknown-extra error, its
//! interior is NEVER validated or inspected; it counts toward the size cap.
//! `legal-clearance` interaction: `_meta` is REMARK-class on transitions, but the
//! decided→same-status byte-identical-re-put rule is whole-payload, so a `_meta`
//! edit on a decided record still rejects `legal_record_frozen`.
//!
//! # §4.12 — the AI-structured provenance contract (A1 owns; A5/A4 consume)
//!
//! - **P-1 — `origin_ref` carries the SOURCE-ARTIFACT lane, one value, by
//!   precedence — and every EDIT carries it FORWARD.** If the note derives from a
//!   prior artifact — a chat message (`chat:`), an imported file (`import:`),
//!   another note (`note:`) — that lane wins and is UNCHANGED by AI enrichment.
//!   `struct:` appears ONLY when the structuring call is itself the origin.
//!   Grammar v2, format `<lane>:<opaque>` (the engine never parses past the first
//!   `:`; unknown prefixes legal): `chat:<message_id>` · `note:<note_id>` ·
//!   `gen:<steps_cache_key_prefix16>` (generation LANDINGS only, never a
//!   note-structuring lane, ORCH-1) · `agent:<run_id>` (agent-authored operational
//!   records; evidence-pointer-OPTIONAL on tap-through, ORCH-14) ·
//!   `import:<format>:<blake3-of-source-file>` (`<format>` an open lowercase token
//!   `[a-z0-9_]+`) · `struct:<key16>` (the ONE spelling of the AI-structuring
//!   lane; `<key16>` is server-computed and echoed — clients never derive it).
//!   **Omission clause:** `note_upsert` is whole-row LWW; `"origin_ref": null` and
//!   an ABSENT key are IDENTICAL (both `None`) and the engine does NOT preserve
//!   the stored value — edit envelopes MUST carry the stored `origin_ref` forward,
//!   built from the freshly-read row; omission/null clobbers to NULL fleet-wide
//!   (null is correct on CREATE of an unprovenanced note only).
//! - **P-2 — the confirming human is the author.** `author_id` is engine-stamped;
//!   on an AI-structured note a human confirmed, `author_role` = the human's craft
//!   role. `author_role:"agent"` is RESERVED for notes written WITHOUT a per-note
//!   human confirm — a true agent run (`agent:<run_id>`) or an explicit
//!   auto-accept policy (default OFF). A standing policy is not a per-note confirm.
//! - **P-3 — AI structuring provenance lives in `payload._meta` when a payload
//!   exists (written by A5); otherwise in `origin_ref` (`struct:`) or nowhere.**
//!   One place to look.
//! - **P-4 — batch idempotent ids (ORCH-2): ONE formula, a CROSS-REPO CONTRACT:**
//!   `note_id = blake3_hex("structrow:" + kind + ":" + board_id + ":" + natural_key)`
//!   (full 64-hex lowercase; the prefix is INSIDE the hash input), `natural_key`
//!   per kind: `shot-log` = `{scene}\x1f{take}\x1f{camera_roll | "-"}` ·
//!   `continuity` = `{scene}\x1f{take}`. Batch import is OFFERED ONLY for kinds
//!   with a defined natural-key row here (v1: exactly those two). NO tenant
//!   ingredient. The formula runs at runtime in exactly ONE place — cyan-lens's
//!   server-side response (`suggested_note_id`); iOS consumes verbatim; this
//!   backend only validates FORMAT via [`batch_note_id_format_valid`] — it never
//!   re-derives. `natural_key` bytes are used VERBATIM as produced by the parse —
//!   NO Unicode normalization (NFC/NFD or otherwise) before hashing. Cross-plane
//!   parity = the pinned fixture `tests/fixtures/batch_note_id_vectors.json`,
//!   copied VERBATIM from the spec package — never regenerated.
//!
//! # §4.13 — tolerant-read rule TR-1 (normative)
//!
//! Any note row may arrive from a newer peer with unknown `kind`, unknown `scope`,
//! unknown/`v>1`/malformed `payload`, unknown `author_role`, or unknown
//! `origin_ref` prefix — **inbound gossip/snapshot apply validates NOTHING**
//! (convergence over validation). Every reader MUST degrade per-field and never
//! drop, never panic, never gate on parse success: the resolver leaves unknown
//! kinds/scopes invisible to the merge (safe by construction); `note_from_row` /
//! `cyan_note_list` return the row with `payload = None` + a warn on parse failure
//! (GC-3); the legal gates are the ONE reader that must not fail open — §4.9's
//! fail-closed rule ([`clearance_status`] `Unreadable`). **Scope carve-out
//! (ORCH-10):** TR-1 governs the DEFAULT (mesh-open) inbound path — apply never
//! drops unknown scopes/kinds; newer-peer rows always land. A2's
//! `CHECK_UNKNOWN_SCOPE` DROP applies ONLY inside the opt-in enforced-group RBAC
//! arm — one owner per path.
//!
//! # §4.14 — the two promote verbs (honest positioning)
//!
//! The system has TWO promote flows; both named so neither is duplicated:
//! 1. **`escalate_note` (LIVE)** — changelist ledger (sensed-Frame.io `kind=note`
//!    `ChangeEntry`, NOT `NoteDTO`) → op; backend-enforced
//!    `enforce_gate("escalate_note.promote", &Gate::Human, actor)`, dispatched via
//!    `cyan_review_command`. The in-repo precedent for human-in-the-loop promotes.
//! 2. **Note promote-to-step (NEW)** — `NoteDTO` → `/generate` → notebook cells,
//!    CLIENT-driven (no backend promote verb exists for NoteDTO), human-initiated
//!    by construction, `origin_ref:"note:<id>"` on landed cells.
//!
//! They share the principle and NOTHING else — not a ledger, not a verb, not a
//! vocabulary. Build rule: do NOT route NoteDTO promotes through `escalate_note`;
//! do NOT add a NoteDTO promote verb to the review dispatcher.

use serde_json::Value;

use crate::models::dto::{self, NoteDTO};

/// Serialized-payload size cap for every kind except `script`.
pub const PAYLOAD_MAX_BYTES: usize = 16 * 1024;
/// Serialized-payload size cap for `kind == "script"` only (whole scripts).
pub const SCRIPT_PAYLOAD_MAX_BYTES: usize = 256 * 1024;

// §4.9 typed reject reasons (each is the exact `NoteRejected.reason` string).
pub const REASON_LEGAL_PAYLOAD_REQUIRED: &str = "legal_payload_required";
pub const REASON_LEGAL_VERSION_UNKNOWN: &str = "legal_version_unknown";
pub const REASON_LEGAL_IDENTITY_FROZEN: &str = "legal_identity_frozen";
pub const REASON_LEGAL_TRANSITION_DENIED: &str = "legal_transition_denied";
pub const REASON_LEGAL_RECORD_FROZEN: &str = "legal_record_frozen";
pub const REASON_LEGAL_RECORD_UNREADABLE: &str = "legal_record_unreadable";
/// §6 role-anchor rule reject reason (`scope=="role"` without a valid
/// (`"role"`, slug) pair, or the pair on any other scope).
pub const REASON_ROLE_ANCHOR_INVALID: &str = "role_anchor_invalid";

/// A payload validation failure — every reject at the write door is typed, and
/// the `Display` string is what rides `NoteRejected.reason` / the obs line.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PayloadError {
    #[error("payload must be a JSON object (kind {kind})")]
    NotAnObject { kind: String },
    #[error("payload_too_large: {size} bytes over the {cap}-byte cap (kind {kind})")]
    TooLarge { kind: String, size: usize, cap: usize },
    #[error("payload field `{field}` missing or invalid: expected {expected} (kind {kind})")]
    Field { kind: String, field: String, expected: String },
    /// A §4.9 legal-clearance reject — the payload IS the typed reason const.
    #[error("{0}")]
    Legal(&'static str),
}

impl PayloadError {
    fn field(kind: &str, field: &str, expected: &str) -> Self {
        PayloadError::Field {
            kind: kind.to_string(),
            field: field.to_string(),
            expected: expected.to_string(),
        }
    }
}

/// The §4.9 typed status reader for STORED clearance payloads — consumed by BOTH
/// doors (edit + delete). Fail-closed: a record whose status cannot be read must
/// be treated as possibly-decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClearanceStatus {
    /// Readable and `status == "pending"` (or absent — the schema default).
    Pending,
    /// Readable and `status ∈ {"cleared", "rejected"}`.
    Decided,
    /// Payload absent / not an object / `v != 1` / `status ∉ vocab` — the status
    /// cannot be trusted; both doors freeze the record (`legal_record_unreadable`).
    Unreadable,
}

/// What the write door must do to the clearance stamps after an ALLOWED
/// transition — `cleared_by`/`cleared_at` are always SERVER-controlled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegalStampAction {
    /// pending → cleared|rejected: stamp `cleared_by = node_id`, `cleared_at = now`
    /// (caller values overwritten).
    Stamp,
    /// Born/staying/returning pending: strip both stamps (caller values ignored).
    Strip,
    /// decided → same status, byte-identical re-put: keep the payload verbatim
    /// (only `updated_at` bumps).
    Keep,
}

const LEGAL_KIND_VOCAB: [&str; 6] =
    ["music", "footage", "talent", "trademark", "location", "other"];
const LEGAL_STATUS_VOCAB: [&str; 3] = ["pending", "cleared", "rejected"];
const TURNOVER_STAGE_VOCAB: [&str; 5] = ["editorial", "sound", "color", "vfx", "delivery"];
const TURNOVER_ITEM_KIND_VOCAB: [&str; 7] =
    ["aaf", "edl", "xml", "wav", "qt_ref", "change_list", "other"];
const TURNOVER_STATUS_VOCAB: [&str; 4] = ["staged", "sent", "received", "rejected"];
const QC_RESULT_VOCAB: [&str; 3] = ["pass", "fail", "warn"];

/// `scene_id = "sc:" + first 8 hex of blake3(script_note_id) + ":" + scene.number`
/// (§4.7). Stable across re-imports for unchanged scene numbers; unique across
/// scripts; OPAQUE to the engine — only this fn computes it, nothing parses it.
/// Referential integrity is NOT enforced.
pub fn scene_id(script_note_id: &str, scene_number: &str) -> String {
    let hex = blake3::hash(script_note_id.as_bytes()).to_hex();
    format!("sc:{}:{}", &hex.as_str()[..8], scene_number)
}

/// FORMAT check ONLY for a P-4 batch note id: 64 lowercase hex chars. This
/// backend NEVER re-derives the id (ORCH-2 — cyan-lens computes it server-side,
/// clients consume verbatim; the pinned fixture is the cross-plane parity proof).
pub fn batch_note_id_format_valid(id: &str) -> bool {
    id.len() == 64 && id.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// The payload's `"v"` as an i64; ABSENT ⇒ 1 (§4 common rules); present but not
/// an integer ⇒ `None` (unreadable version).
fn payload_version(map: &serde_json::Map<String, Value>) -> Option<i64> {
    match map.get("v") {
        None => Some(1),
        Some(v) => v.as_i64(),
    }
}

/// Validate `payload` for `kind` per the §4 schemas (§4.1-§4.11). `payload` is
/// taken `&mut` for the ONE coercion the tables allow: `turnover.from_role/
/// to_role` slugs outside `PRODUCTION_ROLE_VOCAB` are coerced ABSENT (provenance
/// posture — never a reject). Rules honored here:
///
/// - not a JSON object ⇒ REJECT (every kind);
/// - size cap: 16KB (256KB for `script`), checked on the serialized bytes;
/// - `_meta` ignored-and-preserved (never inspected), counts toward the cap;
/// - unknown EXTRA fields / a payload on a schema-less kind (`editor-note`,
///   `preference`) ⇒ stored opaque;
/// - `v > 1` ⇒ skip per-kind validation, store opaque — EXCEPT `legal-clearance`,
///   which REJECTS `legal_version_unknown` (§4.9's explicit exception).
///
/// LOCAL AUTHORING ONLY — inbound gossip/snapshot never validates (TR-1).
pub fn validate(kind: &str, payload: &mut Value) -> Result<(), PayloadError> {
    if !payload.is_object() {
        return Err(PayloadError::NotAnObject { kind: kind.to_string() });
    }
    let cap = if kind == "script" { SCRIPT_PAYLOAD_MAX_BYTES } else { PAYLOAD_MAX_BYTES };
    let size = serde_json::to_string(payload).map(|s| s.len()).unwrap_or(usize::MAX);
    if size > cap {
        return Err(PayloadError::TooLarge { kind: kind.to_string(), size, cap });
    }

    // Version gate. `legal-clearance` is the one kind where v != 1 REJECTS instead
    // of storing opaque; every other kind skips validation on v > 1.
    let version = match payload.as_object() {
        Some(map) => payload_version(map),
        None => return Err(PayloadError::NotAnObject { kind: kind.to_string() }),
    };
    if kind == "legal-clearance" {
        if version != Some(1) {
            return Err(PayloadError::Legal(REASON_LEGAL_VERSION_UNKNOWN));
        }
    } else {
        match version {
            Some(1) => {}
            Some(v) if v > 1 => return Ok(()), // opaque store
            _ => return Err(PayloadError::field(kind, "v", "integer 1")),
        }
    }

    match kind {
        "constitution" => validate_constitution(payload),
        "creative-brief" => validate_creative_brief(payload),
        "shot-log" => validate_shot_log(payload),
        "lined-script" => validate_lined_script(payload),
        "continuity" => validate_continuity(payload),
        "script" => validate_script(payload),
        "creative-dna" => validate_creative_dna(payload),
        "decision" => validate_decision(payload),
        "legal-clearance" => validate_legal_clearance(payload),
        "turnover" => validate_turnover(payload),
        "qc-report" => validate_qc_report(payload),
        // Schema-less kinds (§4.8: freeform is the point) — stored opaque.
        _ => Ok(()),
    }
}

// ── per-field helpers (each error names the field + expectation) ──

fn map_of<'a>(kind: &str, p: &'a Value) -> Result<&'a serde_json::Map<String, Value>, PayloadError> {
    p.as_object().ok_or_else(|| PayloadError::NotAnObject { kind: kind.to_string() })
}

fn req_str<'a>(
    kind: &str,
    map: &'a serde_json::Map<String, Value>,
    field: &str,
    max_len: usize,
) -> Result<&'a str, PayloadError> {
    match map.get(field).and_then(Value::as_str) {
        Some(s) if !s.is_empty() && s.len() <= max_len => Ok(s),
        _ => {
            let expected = if max_len == usize::MAX {
                "non-empty string".to_string()
            } else {
                format!("string 1-{max_len}")
            };
            Err(PayloadError::field(kind, field, &expected))
        }
    }
}

fn opt_str(
    kind: &str,
    map: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<(), PayloadError> {
    match map.get(field) {
        None | Some(Value::Null) => Ok(()),
        Some(Value::String(_)) => Ok(()),
        Some(_) => Err(PayloadError::field(kind, field, "string")),
    }
}

fn req_enum<'a>(
    kind: &str,
    map: &'a serde_json::Map<String, Value>,
    field: &str,
    allowed: &[&str],
) -> Result<&'a str, PayloadError> {
    match map.get(field).and_then(Value::as_str) {
        Some(s) if allowed.contains(&s) => Ok(s),
        _ => Err(PayloadError::field(kind, field, &format!("one of {allowed:?}"))),
    }
}

/// An enum field with a schema default: absent (or null) is valid.
fn opt_enum(
    kind: &str,
    map: &serde_json::Map<String, Value>,
    field: &str,
    allowed: &[&str],
) -> Result<(), PayloadError> {
    match map.get(field) {
        None | Some(Value::Null) => Ok(()),
        Some(Value::String(s)) if allowed.contains(&s.as_str()) => Ok(()),
        Some(_) => {
            Err(PayloadError::field(kind, field, &format!("one of {allowed:?} (or absent)")))
        }
    }
}

fn opt_bool(
    kind: &str,
    map: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<(), PayloadError> {
    match map.get(field) {
        None | Some(Value::Null) | Some(Value::Bool(_)) => Ok(()),
        Some(_) => Err(PayloadError::field(kind, field, "boolean")),
    }
}

fn req_int(
    kind: &str,
    map: &serde_json::Map<String, Value>,
    field: &str,
    min: i64,
) -> Result<i64, PayloadError> {
    match map.get(field).and_then(Value::as_i64) {
        Some(n) if n >= min => Ok(n),
        _ => Err(PayloadError::field(kind, field, &format!("integer ≥ {min}"))),
    }
}

fn opt_str_array(
    kind: &str,
    map: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<(), PayloadError> {
    match map.get(field) {
        None | Some(Value::Null) => Ok(()),
        Some(Value::Array(items)) if items.iter().all(Value::is_string) => Ok(()),
        Some(_) => Err(PayloadError::field(kind, field, "array of strings")),
    }
}

// ── §4.1-§4.11 validators ──

/// §4.1 `constitution` — required `category`, `rule` (slug 1-64), `value` (1-256).
fn validate_constitution(p: &Value) -> Result<(), PayloadError> {
    let kind = "constitution";
    let map = map_of(kind, p)?;
    req_enum(kind, map, "category", &["brand", "technical", "delivery", "legal", "creative"])?;
    req_str(kind, map, "rule", 64)?;
    req_str(kind, map, "value", 256)?;
    opt_str(kind, map, "rationale")?;
    Ok(())
}

/// §4.2 `creative-brief` — required `objective`; arrays default `[]`.
fn validate_creative_brief(p: &Value) -> Result<(), PayloadError> {
    let kind = "creative-brief";
    let map = map_of(kind, p)?;
    req_str(kind, map, "objective", usize::MAX)?;
    opt_str(kind, map, "audience")?;
    opt_str(kind, map, "tone")?;
    for f in ["must_haves", "deliverables", "references"] {
        opt_str_array(kind, map, f)?;
    }
    Ok(())
}

/// §4.3 `shot-log` (one entry per note) — required `scene`, `take` (int ≥ 1),
/// `tc_in`, `tc_out` (SMPTE strings, OPAQUE to the engine).
fn validate_shot_log(p: &Value) -> Result<(), PayloadError> {
    let kind = "shot-log";
    let map = map_of(kind, p)?;
    req_str(kind, map, "scene", usize::MAX)?;
    req_int(kind, map, "take", 1)?;
    req_str(kind, map, "tc_in", usize::MAX)?;
    req_str(kind, map, "tc_out", usize::MAX)?;
    opt_bool(kind, map, "circle")?;
    opt_enum(kind, map, "rating", &["NG", "hold", "print"])?;
    opt_enum(kind, map, "sync", &["synced", "unsynced", "mos"])?;
    for f in ["setup", "camera_roll", "sound_roll", "lens", "description"] {
        opt_str(kind, map, f)?;
    }
    Ok(())
}

/// §4.4 `lined-script` (one scene's lining) — required `scene_id`, `setups`;
/// `script_note_id` optional (dangling allowed, §10 B10).
fn validate_lined_script(p: &Value) -> Result<(), PayloadError> {
    let kind = "lined-script";
    let map = map_of(kind, p)?;
    req_str(kind, map, "scene_id", usize::MAX)?;
    opt_str(kind, map, "script_note_id")?;
    let Some(setups) = map.get("setups").and_then(Value::as_array) else {
        return Err(PayloadError::field(kind, "setups", "array of setup objects"));
    };
    for setup in setups {
        let s = map_of(kind, setup)
            .map_err(|_| PayloadError::field(kind, "setups[]", "object"))?;
        req_str(kind, s, "setup", usize::MAX)
            .map_err(|_| PayloadError::field(kind, "setups[].setup", "string"))?;
        let Some(cov) = s.get("coverage").and_then(Value::as_array) else {
            return Err(PayloadError::field(kind, "setups[].coverage", "array of line ranges"));
        };
        for c in cov {
            let c = map_of(kind, c)
                .map_err(|_| PayloadError::field(kind, "setups[].coverage[]", "object"))?;
            let from = req_int(kind, c, "from_line", 1)
                .map_err(|_| PayloadError::field(kind, "coverage[].from_line", "integer ≥ 1"))?;
            let to = req_int(kind, c, "to_line", from).map_err(|_| {
                PayloadError::field(kind, "coverage[].to_line", "integer ≥ from_line")
            })?;
            debug_assert!(to >= from);
        }
    }
    Ok(())
}

/// §4.5 `continuity` — required `scene`, `take`; arrays default `[]`.
fn validate_continuity(p: &Value) -> Result<(), PayloadError> {
    let kind = "continuity";
    let map = map_of(kind, p)?;
    req_str(kind, map, "scene", usize::MAX)?;
    req_int(kind, map, "take", 0)?;
    opt_str(kind, map, "remarks")?;
    opt_str(kind, map, "timing")?;
    opt_str_array(kind, map, "continuity")?;
    match map.get("dialogue_changes") {
        None | Some(Value::Null) => {}
        Some(Value::Array(changes)) => {
            for ch in changes {
                let c = map_of(kind, ch)
                    .map_err(|_| PayloadError::field(kind, "dialogue_changes[]", "object"))?;
                req_str(kind, c, "from", usize::MAX)
                    .map_err(|_| PayloadError::field(kind, "dialogue_changes[].from", "string"))?;
                req_str(kind, c, "to", usize::MAX)
                    .map_err(|_| PayloadError::field(kind, "dialogue_changes[].to", "string"))?;
            }
        }
        Some(_) => {
            return Err(PayloadError::field(kind, "dialogue_changes", "array of change objects"));
        }
    }
    Ok(())
}

/// §4.6 `script` — required `scenes` non-empty; cap 256KB. Maps 1:1 onto .fdx
/// `<Paragraph Type=…>` — a Final Draft plugin fills this with zero remodeling.
fn validate_script(p: &Value) -> Result<(), PayloadError> {
    let kind = "script";
    let map = map_of(kind, p)?;
    opt_str(kind, map, "title")?;
    let scenes = match map.get("scenes").and_then(Value::as_array) {
        Some(s) if !s.is_empty() => s,
        _ => return Err(PayloadError::field(kind, "scenes", "non-empty array of scenes")),
    };
    for scene in scenes {
        let s = map_of(kind, scene).map_err(|_| PayloadError::field(kind, "scenes[]", "object"))?;
        req_str(kind, s, "scene_id", usize::MAX)
            .map_err(|_| PayloadError::field(kind, "scenes[].scene_id", "string"))?;
        req_str(kind, s, "number", usize::MAX)
            .map_err(|_| PayloadError::field(kind, "scenes[].number", "string"))?;
        req_str(kind, s, "heading", usize::MAX)
            .map_err(|_| PayloadError::field(kind, "scenes[].heading", "string"))?;
        opt_str(kind, s, "action")?;
        match s.get("dialogue") {
            None | Some(Value::Null) => {}
            Some(Value::Array(lines)) => {
                for d in lines {
                    let d = map_of(kind, d)
                        .map_err(|_| PayloadError::field(kind, "dialogue[]", "object"))?;
                    req_str(kind, d, "character", usize::MAX)
                        .map_err(|_| PayloadError::field(kind, "dialogue[].character", "string"))?;
                    req_str(kind, d, "lines", usize::MAX)
                        .map_err(|_| PayloadError::field(kind, "dialogue[].lines", "string"))?;
                }
            }
            Some(_) => {
                return Err(PayloadError::field(kind, "scenes[].dialogue", "array of lines"));
            }
        }
    }
    Ok(())
}

/// §4.8 `creative-dna` — both fields required (design's "VO" is `"vo"` on the wire).
fn validate_creative_dna(p: &Value) -> Result<(), PayloadError> {
    let kind = "creative-dna";
    let map = map_of(kind, p)?;
    req_enum(kind, map, "dimension", &["grade", "pace", "feel", "vo", "genre"])?;
    req_str(kind, map, "value", usize::MAX)?;
    Ok(())
}

/// §4.8 `decision` — `status` default `"proposed"`; transitions FREE (a human
/// artifact, not a gate).
fn validate_decision(p: &Value) -> Result<(), PayloadError> {
    let kind = "decision";
    let map = map_of(kind, p)?;
    opt_enum(kind, map, "status", &["proposed", "locked"])?;
    Ok(())
}

/// §4.9 `legal-clearance` SHAPE check — required `item`, `kind`; `status` default
/// `"pending"`. The transition contract is [`check_legal_transition`], which the
/// write door runs IN ADDITION to this.
fn validate_legal_clearance(p: &Value) -> Result<(), PayloadError> {
    let kind = "legal-clearance";
    let map = map_of(kind, p)?;
    req_str(kind, map, "item", usize::MAX)?;
    req_enum(kind, map, "kind", &LEGAL_KIND_VOCAB)?;
    opt_enum(kind, map, "status", &LEGAL_STATUS_VOCAB)?;
    opt_str(kind, map, "note")?;
    Ok(())
}

/// §4.10 `turnover` — the AE orchestration bridge's handoff record. Required
/// `to_stage`, `items` (non-empty, each `ref` non-empty); `from_role`/`to_role`
/// COERCED absent when ∉ `PRODUCTION_ROLE_VOCAB` (provenance posture — the one
/// in-place mutation `validate` performs). **`status` is DESCRIPTIVE, transitions
/// FREE** (like `decision.status`); the ACTUAL send authority is the
/// `external_send` gate on the turnover-send tool — no engine behavior keys off
/// `turnover.status` (deliberate contrast with `legal-clearance`, whose status IS
/// enforced — do not add a second state machine).
pub fn validate_turnover(p: &mut Value) -> Result<(), PayloadError> {
    let kind = "turnover";
    {
        let map = map_of(kind, p)?;
        req_enum(kind, map, "to_stage", &TURNOVER_STAGE_VOCAB)?;
        opt_enum(kind, map, "status", &TURNOVER_STATUS_VOCAB)?;
        opt_str(kind, map, "cut_ref")?;
        opt_str(kind, map, "notes")?;
        if let Some(due) = map.get("due")
            && !due.is_null()
            && due.as_i64().is_none()
        {
            return Err(PayloadError::field(kind, "due", "integer unix seconds"));
        }
        let items = match map.get("items").and_then(Value::as_array) {
            Some(items) if !items.is_empty() => items,
            _ => return Err(PayloadError::field(kind, "items", "non-empty array of items")),
        };
        for item in items {
            let i = map_of(kind, item)
                .map_err(|_| PayloadError::field(kind, "items[]", "object"))?;
            req_enum(kind, i, "kind", &TURNOVER_ITEM_KIND_VOCAB)
                .map_err(|_| {
                    PayloadError::field(kind, "items[].kind", &format!("one of {TURNOVER_ITEM_KIND_VOCAB:?}"))
                })?;
            req_str(kind, i, "ref", 512)
                .map_err(|_| PayloadError::field(kind, "items[].ref", "string 1-512"))?;
            opt_str(kind, i, "note")
                .map_err(|_| PayloadError::field(kind, "items[].note", "string"))?;
        }
    }
    // The coercion: an out-of-vocab craft slug is dropped, never a reject.
    if let Some(map) = p.as_object_mut() {
        for f in ["from_role", "to_role"] {
            let invalid = matches!(
                map.get(f),
                Some(v) if !v.is_null()
                    && v.as_str().map(dto::production_role_valid) != Some(true)
            );
            if invalid {
                map.remove(f);
            }
        }
    }
    Ok(())
}

/// §4.11 `qc-report` — the deep-QC result record. Required `target`, `overall`,
/// `checks` (non-empty, each with non-empty `check`/`expected`/`measured`);
/// `rule_ref` dangling-allowed (B10 posture). ADVISORY DATA: it never fires or
/// blocks a gate — a failing report reaching the legal/delivery gate is a HUMAN's
/// input at the existing approval.
pub fn validate_qc_report(p: &Value) -> Result<(), PayloadError> {
    let kind = "qc-report";
    let map = map_of(kind, p)?;
    req_str(kind, map, "target", 512)?;
    req_enum(kind, map, "overall", &QC_RESULT_VOCAB)?;
    opt_str(kind, map, "tool")?;
    let checks = match map.get("checks").and_then(Value::as_array) {
        Some(c) if !c.is_empty() => c,
        _ => return Err(PayloadError::field(kind, "checks", "non-empty array of checks")),
    };
    for check in checks {
        let c = map_of(kind, check)
            .map_err(|_| PayloadError::field(kind, "checks[]", "object"))?;
        req_str(kind, c, "check", 64)
            .map_err(|_| PayloadError::field(kind, "checks[].check", "slug 1-64"))?;
        req_str(kind, c, "expected", usize::MAX)
            .map_err(|_| PayloadError::field(kind, "checks[].expected", "string"))?;
        req_str(kind, c, "measured", usize::MAX)
            .map_err(|_| PayloadError::field(kind, "checks[].measured", "string"))?;
        req_enum(kind, c, "result", &QC_RESULT_VOCAB)
            .map_err(|_| {
                PayloadError::field(kind, "checks[].result", &format!("one of {QC_RESULT_VOCAB:?}"))
            })?;
        opt_str(kind, c, "rule_ref")
            .map_err(|_| PayloadError::field(kind, "checks[].rule_ref", "string"))?;
    }
    Ok(())
}

// ── §4.9 legal-clearance state machine ──

/// Read a STORED clearance payload's status, fail-closed (§4.9, D-A1-R3).
/// `Unreadable` = payload absent / not an object / `v != 1` / `status ∉ vocab` —
/// NEVER default an unreadable status to `pending`; that would fail-open exactly
/// the record class this rule fails closed. (An ABSENT `status` key on a readable
/// v1 object IS the schema default `pending` — that is readable.)
pub fn clearance_status(stored_payload: Option<&Value>) -> ClearanceStatus {
    let Some(map) = stored_payload.and_then(Value::as_object) else {
        return ClearanceStatus::Unreadable;
    };
    if payload_version(map) != Some(1) {
        return ClearanceStatus::Unreadable;
    }
    match map.get("status") {
        None => ClearanceStatus::Pending,
        Some(Value::String(s)) => match s.as_str() {
            "pending" => ClearanceStatus::Pending,
            "cleared" | "rejected" => ClearanceStatus::Decided,
            _ => ClearanceStatus::Unreadable,
        },
        Some(_) => ClearanceStatus::Unreadable,
    }
}

fn incoming_status(payload: &Value) -> Option<&str> {
    match payload.get("status") {
        None => Some("pending"), // schema default: born/staying pending
        Some(Value::String(s)) if LEGAL_STATUS_VOCAB.contains(&s.as_str()) => Some(s),
        _ => None,
    }
}

/// Strip the server-controlled stamps from a payload clone so IDENTITY/REMARK
/// comparisons never key off caller-supplied stamp noise.
fn identity_fields(payload: &Value) -> (Option<&Value>, Option<&Value>) {
    (payload.get("item"), payload.get("kind"))
}

/// §4.9 — the exact transition contract, every REJECT a typed reason
/// (`PayloadError::Legal(REASON_…)`). Enforced in `dispatch_put_note` for LOCAL
/// authoring only (inbound mesh rows are never validated, TR-1/GC-2).
///
/// ```text
///   pending ──producer only──► cleared ──producer only──► pending (re-open)
///      └────producer only────► rejected ──producer only──► pending (re-open)
///   cleared ⇄ rejected: NEVER direct (must pass through pending); create: born pending only
/// ```
///
/// Field classes: **IDENTITY** = `item`, `kind` — frozen byte-equal on every
/// transition except pending→pending (`legal_identity_frozen`). **REMARK** =
/// payload `note` + DTO `text` — free on transitions. **STAMPS** =
/// `cleared_by`/`cleared_at` — always server-controlled (the returned
/// [`LegalStampAction`] tells the write door what to do; caller values are
/// overwritten/stripped, never trusted).
///
/// Stored-row checks (both NEW at REV 2, D-A1-R3): stored `v > 1` ⇒
/// `legal_version_unknown`; stored otherwise-[`ClearanceStatus::Unreadable`]
/// (absent/unparseable/status ∉ vocab) ⇒ `legal_record_unreadable` — fail-closed.
pub fn check_legal_transition(
    existing: Option<&NoteDTO>,
    new_text: &str,
    new_payload: Option<&Value>,
    author_role: Option<&str>,
) -> Result<LegalStampAction, PayloadError> {
    // Payload REQUIRED on create AND every edit (a payload-less edit would
    // whole-row-clobber `status`).
    let Some(payload) = new_payload else {
        return Err(PayloadError::Legal(REASON_LEGAL_PAYLOAD_REQUIRED));
    };
    let Some(map) = payload.as_object() else {
        return Err(PayloadError::Legal(REASON_LEGAL_PAYLOAD_REQUIRED));
    };
    if payload_version(map) != Some(1) {
        return Err(PayloadError::Legal(REASON_LEGAL_VERSION_UNKNOWN));
    }
    let Some(new_status) = incoming_status(payload) else {
        return Err(PayloadError::Legal(REASON_LEGAL_TRANSITION_DENIED));
    };
    let is_producer = author_role == Some("producer");

    let Some(stored) = existing else {
        // CREATE: born pending only, any author_role; stamps stripped.
        if new_status != "pending" {
            return Err(PayloadError::Legal(REASON_LEGAL_TRANSITION_DENIED));
        }
        return Ok(LegalStampAction::Strip);
    };

    // Stored-row checks, fail-closed (D-A1-R3): v > 1 is distinguishable
    // (upgrade-shaped) — everything else unreadable freezes the record.
    if let Some(stored_map) = stored.payload.as_ref().and_then(Value::as_object)
        && matches!(payload_version(stored_map), Some(v) if v > 1)
    {
        return Err(PayloadError::Legal(REASON_LEGAL_VERSION_UNKNOWN));
    }
    // A readable status implies a readable stored payload object; re-borrow it
    // once so the arms below never unwrap.
    let stored_payload = match clearance_status(stored.payload.as_ref()) {
        ClearanceStatus::Unreadable => {
            return Err(PayloadError::Legal(REASON_LEGAL_RECORD_UNREADABLE));
        }
        ClearanceStatus::Pending | ClearanceStatus::Decided => match stored.payload.as_ref() {
            Some(p) => p,
            None => return Err(PayloadError::Legal(REASON_LEGAL_RECORD_UNREADABLE)),
        },
    };
    let old_status = stored_payload.get("status").and_then(Value::as_str).unwrap_or("pending");

    let identity_frozen = || -> Result<(), PayloadError> {
        if identity_fields(stored_payload) != identity_fields(payload) {
            return Err(PayloadError::Legal(REASON_LEGAL_IDENTITY_FROZEN));
        }
        Ok(())
    };

    match (old_status, new_status) {
        // pending → pending: IDENTITY + REMARK free, any author_role.
        ("pending", "pending") => Ok(LegalStampAction::Strip),
        // pending → cleared|rejected: producer only, IDENTITY frozen, REMARK free.
        ("pending", "cleared") | ("pending", "rejected") => {
            if !is_producer {
                return Err(PayloadError::Legal(REASON_LEGAL_TRANSITION_DENIED));
            }
            identity_frozen()?;
            Ok(LegalStampAction::Stamp)
        }
        // decided → same status: ONLY a byte-identical re-put (updated_at bumps) —
        // ANY difference (incl. `_meta`, note, or DTO text) ⇒ legal_record_frozen.
        (old, new) if old == new => {
            if stored_payload == payload && stored.text == new_text {
                Ok(LegalStampAction::Keep)
            } else {
                Err(PayloadError::Legal(REASON_LEGAL_RECORD_FROZEN))
            }
        }
        // decided → pending (re-open): producer only; IDENTITY frozen; REMARK
        // free; stamps stripped.
        ("cleared", "pending") | ("rejected", "pending") => {
            if !is_producer {
                return Err(PayloadError::Legal(REASON_LEGAL_TRANSITION_DENIED));
            }
            identity_frozen()?;
            Ok(LegalStampAction::Strip)
        }
        // cleared ⇄ rejected: NEVER direct (must pass through pending).
        _ => Err(PayloadError::Legal(REASON_LEGAL_TRANSITION_DENIED)),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn note(kind: &str, text: &str, payload: Option<Value>) -> NoteDTO {
        NoteDTO {
            id: "n1".into(),
            board_id: "b1".into(),
            tenant_id: "t1".into(),
            author_id: "node-1".into(),
            author_name: "Ada".into(),
            text: text.into(),
            created_at: 1,
            updated_at: 1,
            scope: "board".into(),
            kind: kind.into(),
            anchor_kind: None,
            anchor_id: None,
            origin_ref: None,
            payload,
            author_role: None,
        }
    }

    fn pending() -> Value {
        json!({"v":1, "item":"Track: 'Neon Nights'", "kind":"music", "status":"pending"})
    }

    fn cleared() -> Value {
        json!({"v":1, "item":"Track: 'Neon Nights'", "kind":"music", "status":"cleared",
               "cleared_by":"node-p", "cleared_at": 1760000000})
    }

    #[test]
    fn validate_rejects_non_object_and_names_fields() {
        assert!(matches!(
            validate("shot-log", &mut json!("not an object")),
            Err(PayloadError::NotAnObject { .. })
        ));
        // Missing tc_in names the field.
        let err = validate(
            "shot-log",
            &mut json!({"v":1, "scene":"12A", "take":3, "tc_out":"01:02:44:12"}),
        )
        .unwrap_err();
        assert!(err.to_string().contains("tc_in"), "err names the field: {err}");
        // rating outside the closed enum.
        let err = validate(
            "shot-log",
            &mut json!({"v":1, "scene":"12A", "take":3, "tc_in":"01:02:03:04",
                        "tc_out":"01:02:44:12", "rating":"maybe"}),
        )
        .unwrap_err();
        assert!(err.to_string().contains("rating"), "err names the field: {err}");
    }

    #[test]
    fn size_caps_per_kind() {
        let big = "x".repeat(PAYLOAD_MAX_BYTES + 1);
        let err = validate("constitution", &mut json!({"v":1, "rule": big})).unwrap_err();
        assert!(matches!(err, PayloadError::TooLarge { cap: PAYLOAD_MAX_BYTES, .. }));
        assert!(err.to_string().contains(&PAYLOAD_MAX_BYTES.to_string()), "names the cap");
        // script rides the 256KB cap.
        let action = "y".repeat(200 * 1024);
        let mut script = json!({"v":1, "scenes":[{"scene_id":"sc:9f3a2c1d:12", "number":"12A",
            "heading":"INT. EDIT BAY - NIGHT", "action": action}]});
        assert!(validate("script", &mut script).is_ok(), "200KB script passes the 256KB cap");
        let action = "y".repeat(SCRIPT_PAYLOAD_MAX_BYTES + 1);
        let mut script = json!({"v":1, "scenes":[{"scene_id":"s", "number":"1",
            "heading":"h", "action": action}]});
        assert!(matches!(
            validate("script", &mut script),
            Err(PayloadError::TooLarge { cap: SCRIPT_PAYLOAD_MAX_BYTES, .. })
        ));
    }

    #[test]
    fn v2_payload_skips_validation_except_legal() {
        // Typed non-legal kind: v:2 + junk stores opaque.
        assert!(validate("shot-log", &mut json!({"v":2, "junk":true})).is_ok());
        // legal-clearance: v != 1 REJECTS.
        assert_eq!(
            validate("legal-clearance", &mut json!({"v":2, "item":"x", "kind":"music"})),
            Err(PayloadError::Legal(REASON_LEGAL_VERSION_UNKNOWN))
        );
    }

    #[test]
    fn meta_is_ignored_and_preserved_on_every_typed_kind() {
        let meta = json!({"structured_by":"lens", "tier":"fast",
            "prompt_ver":"structure.v1", "confidence":0.92,
            "request_key16":"9f3a2c1d8b40e6aa", "row":3});
        let mut p = json!({"v":1, "scene":"12A", "take":3, "tc_in":"01:02:03:04",
            "tc_out":"01:02:44:12", "_meta": meta.clone()});
        validate("shot-log", &mut p).expect("_meta never validated");
        assert_eq!(p.get("_meta"), Some(&meta), "_meta preserved byte-intact");
        // Even a garbage _meta interior is never inspected.
        let mut p = json!({"v":1, "category":"legal", "rule":"clear-music",
            "value":"always", "_meta": {"tier": 12345, "junk": [null]}});
        validate("constitution", &mut p).expect("interior never inspected");
    }

    #[test]
    fn turnover_validates_and_coerces_roles() {
        let mut p = json!({"v":1, "to_stage":"sound",
            "from_role":"assistant_editor", "to_role":"dj-unknown",
            "items":[{"kind":"aaf", "ref":"frameio://pkg-1"}], "status":"staged"});
        validate_turnover(&mut p).expect("valid turnover");
        assert_eq!(p.get("from_role"), Some(&json!("assistant_editor")), "valid slug kept");
        assert!(p.get("to_role").is_none(), "unknown slug coerced absent");
        // Closed enums reject, typed.
        assert!(validate_turnover(&mut json!({"v":1, "to_stage":"marketing",
            "items":[{"kind":"aaf","ref":"r"}]}))
        .is_err());
        assert!(validate_turnover(&mut json!({"v":1, "to_stage":"sound", "items":[]})).is_err());
        assert!(validate_turnover(&mut json!({"v":1, "to_stage":"sound",
            "items":[{"kind":"aaf","ref":"r"}], "status":"lost"}))
        .is_err());
    }

    #[test]
    fn qc_report_validates() {
        let mut p = json!({"v":1, "target":"proxy://cut-4", "overall":"fail",
            "checks":[{"check":"loudness", "expected":"-14 LUFS",
                       "measured":"-11 LUFS", "result":"fail", "rule_ref":"n-rule-1"}],
            "tool":"cyan-media.probe"});
        validate("qc-report", &mut p).expect("valid qc-report");
        assert!(
            validate("qc-report", &mut json!({"v":1, "target":"t", "overall":"pass", "checks":[]}))
                .is_err(),
            "empty checks rejects"
        );
    }

    #[test]
    fn scene_id_is_deterministic_and_note_scoped() {
        let a = scene_id("note-script-1", "12A");
        assert!(a.starts_with("sc:") && a.ends_with(":12A"));
        assert_eq!(a, scene_id("note-script-1", "12A"), "deterministic");
        assert_ne!(a, scene_id("note-script-2", "12A"), "different script ⇒ different id");
        assert_eq!(a.split(':').nth(1).map(str::len), Some(8), "8-hex prefix");
    }

    #[test]
    fn batch_note_id_format_only() {
        assert!(batch_note_id_format_valid(&"a".repeat(64)));
        assert!(batch_note_id_format_valid(
            "40a097ece7fbdd754e06f671574c87e301e5ce99f0ec71108159c80d1d9ff624"
        ));
        assert!(!batch_note_id_format_valid(&"a".repeat(63)), "length");
        assert!(!batch_note_id_format_valid(&"A".repeat(64)), "lowercase only");
        assert!(!batch_note_id_format_valid(&"g".repeat(64)), "hex only");
    }

    #[test]
    fn clearance_status_fail_closed() {
        assert_eq!(clearance_status(None), ClearanceStatus::Unreadable);
        assert_eq!(clearance_status(Some(&json!("junk"))), ClearanceStatus::Unreadable);
        assert_eq!(clearance_status(Some(&json!({"v":2, "status":"pending"}))), ClearanceStatus::Unreadable);
        assert_eq!(
            clearance_status(Some(&json!({"v":1, "status":"granted"}))),
            ClearanceStatus::Unreadable
        );
        assert_eq!(clearance_status(Some(&pending())), ClearanceStatus::Pending);
        assert_eq!(
            clearance_status(Some(&json!({"v":1, "item":"x", "kind":"music"}))),
            ClearanceStatus::Pending,
            "absent status is the schema default"
        );
        assert_eq!(clearance_status(Some(&cleared())), ClearanceStatus::Decided);
    }

    #[test]
    fn legal_create_born_pending_only() {
        assert_eq!(
            check_legal_transition(None, "t", Some(&cleared()), Some("producer")),
            Err(PayloadError::Legal(REASON_LEGAL_TRANSITION_DENIED)),
            "create decided rejects even for a producer"
        );
        assert_eq!(
            check_legal_transition(None, "t", Some(&pending()), None),
            Ok(LegalStampAction::Strip),
            "born pending, any author_role; caller stamps stripped"
        );
        assert_eq!(
            check_legal_transition(None, "t", None, Some("producer")),
            Err(PayloadError::Legal(REASON_LEGAL_PAYLOAD_REQUIRED))
        );
    }

    #[test]
    fn legal_transitions_gate_on_producer_and_identity() {
        let stored = note("legal-clearance", "clear the track", Some(pending()));
        // producer decides.
        assert_eq!(
            check_legal_transition(Some(&stored), "cleared it", Some(&cleared()), Some("producer")),
            Ok(LegalStampAction::Stamp)
        );
        // non-producer denied.
        assert_eq!(
            check_legal_transition(Some(&stored), "t", Some(&cleared()), Some("editor")),
            Err(PayloadError::Legal(REASON_LEGAL_TRANSITION_DENIED))
        );
        // identity change in the transition write.
        let mut other = cleared();
        other["item"] = json!("Track: 'Different Song'");
        assert_eq!(
            check_legal_transition(Some(&stored), "t", Some(&other), Some("producer")),
            Err(PayloadError::Legal(REASON_LEGAL_IDENTITY_FROZEN))
        );
        // pending → pending is free for anyone, identity included.
        let mut renamed = pending();
        renamed["item"] = json!("Track: 'Renamed'");
        assert_eq!(
            check_legal_transition(Some(&stored), "t", Some(&renamed), None),
            Ok(LegalStampAction::Strip)
        );
    }

    #[test]
    fn legal_decided_frozen_and_reopen() {
        let stored = note("legal-clearance", "clear the track", Some(cleared()));
        // Byte-identical re-put passes (updated_at bumps only).
        assert_eq!(
            check_legal_transition(Some(&stored), "clear the track", Some(&cleared()), None),
            Ok(LegalStampAction::Keep)
        );
        // ANY payload difference (here: the REMARK-class note) freezes.
        let mut remarked = cleared();
        remarked["note"] = json!("re-checked");
        assert_eq!(
            check_legal_transition(Some(&stored), "clear the track", Some(&remarked), Some("producer")),
            Err(PayloadError::Legal(REASON_LEGAL_RECORD_FROZEN))
        );
        // DTO-text difference freezes too.
        assert_eq!(
            check_legal_transition(Some(&stored), "edited text", Some(&cleared()), Some("producer")),
            Err(PayloadError::Legal(REASON_LEGAL_RECORD_FROZEN))
        );
        // cleared → rejected direct: never.
        let mut rejected = cleared();
        rejected["status"] = json!("rejected");
        assert_eq!(
            check_legal_transition(Some(&stored), "t", Some(&rejected), Some("producer")),
            Err(PayloadError::Legal(REASON_LEGAL_TRANSITION_DENIED))
        );
        // Re-open: producer only, identity frozen, remark free.
        let mut reopen = pending();
        reopen["note"] = json!("needs a fresh license term");
        assert_eq!(
            check_legal_transition(Some(&stored), "new text ok", Some(&reopen), Some("producer")),
            Ok(LegalStampAction::Strip)
        );
        assert_eq!(
            check_legal_transition(Some(&stored), "t", Some(&reopen), Some("editor")),
            Err(PayloadError::Legal(REASON_LEGAL_TRANSITION_DENIED))
        );
    }

    #[test]
    fn legal_stored_row_fail_closed() {
        // Stored v>1: upgrade-shaped, distinguishable.
        let stored = note("legal-clearance", "t", Some(json!({"v":2, "status":"pending"})));
        assert_eq!(
            check_legal_transition(Some(&stored), "t", Some(&pending()), Some("producer")),
            Err(PayloadError::Legal(REASON_LEGAL_VERSION_UNKNOWN))
        );
        // Stored status ∉ vocab: unreadable, frozen.
        let stored = note("legal-clearance", "t", Some(json!({"v":1, "status":"granted"})));
        assert_eq!(
            check_legal_transition(Some(&stored), "t", Some(&pending()), Some("producer")),
            Err(PayloadError::Legal(REASON_LEGAL_RECORD_UNREADABLE))
        );
        // Stored payload absent: unreadable, frozen (never defaults to pending).
        let stored = note("legal-clearance", "t", None);
        assert_eq!(
            check_legal_transition(Some(&stored), "t", Some(&pending()), Some("producer")),
            Err(PayloadError::Legal(REASON_LEGAL_RECORD_UNREADABLE))
        );
    }
}
