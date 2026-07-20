//! A6 — the deterministic hard/soft constitution classifier (DETAILED §9i,
//! C-A6-A2 / ORCH-6 v2).
//!
//! A PURE POST-PASS over A2's typed [`ResolvedNote`] rows: called AFTER
//! `constitution::resolve_with_provenance` returns, never spliced inside the
//! merge traversal — the `constitution.v1` hash input set is UNTOUCHED, and this
//! module NEVER re-queries (a second read would race LWW and desync from the
//! hash; order/snapshot-consistency/hash-stability are inherited from §5's
//! guarantees). Output order == the notes traversal order (kind-major).
//!
//! The ONE consumer today is `cyan_constitution_effective`'s additive `"hard"`
//! key (verb A2's, schema THIS module's). Downstream (lens, A6 Phase 4): hard
//! rules are lossless BY CONSTRUCTION — the distiller splices `hard[]` verbatim
//! and no model ever receives or emits them. `legal-clearance` rows are OFF the
//! resolver rail (SYN-4) — clearances gate via `GenLegal`, never the digest.

use crate::constitution::ResolvedNote;

/// One hard constraint (C-A6-A5 wire schema `GenHardRule` — four required
/// strings, field order fixed by declaration, unknown keys ignored on read).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct HardRule {
    pub id: String,
    pub scope: String,
    /// `"legal"` | `"technical"` | `"delivery"` (payload-less lexicon hits record
    /// `"technical"`).
    pub category: String,
    /// The DTO `text` VERBATIM — never a rewording, never the payload rendering.
    pub text: String,
}

/// The hard-lexicon MODAL tokens (word-boundary, case-insensitive).
pub const HARD_LEXICON_MODALS: [&str; 7] =
    ["must", "never", "always", "required", "only", "max", "min"];

/// The number+unit half of the lexicon (§9i rule 2), as the three regex classes:
/// a number+unit token, an `NxM` resolution, and an `HH:MM:SS(:|;)FF` timecode.
pub const HARD_LEXICON_NUMBER_UNIT: &str =
    r"(?i)-?\d+(\.\d+)?\s*(LUFS|dBTP|dB|fps|Hz|kHz|Mbps|kbps|px)";
pub const HARD_LEXICON_RESOLUTION: &str = r"\b\d+\s*[xX]\s*\d+\b";
pub const HARD_LEXICON_TIMECODE: &str = r"\b\d{2}:\d{2}:\d{2}[:;]\d{2}\b";

fn lexicon_matches(text: &str) -> bool {
    // Word-boundary, case-insensitive modal match.
    let modal = regex_cached(
        r"(?i)\b(must|never|always|required|only|max|min)\b",
        &MODAL_RE,
    );
    if modal.is_match(text) {
        return true;
    }
    if regex_cached(HARD_LEXICON_NUMBER_UNIT, &NUMBER_UNIT_RE).is_match(text) {
        return true;
    }
    if regex_cached(HARD_LEXICON_RESOLUTION, &RESOLUTION_RE).is_match(text) {
        return true;
    }
    regex_cached(HARD_LEXICON_TIMECODE, &TIMECODE_RE).is_match(text)
}

static MODAL_RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
static NUMBER_UNIT_RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
static RESOLUTION_RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
static TIMECODE_RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();

fn regex_cached(pattern: &str, cell: &'static std::sync::OnceLock<regex::Regex>) -> &'static regex::Regex {
    cell.get_or_init(|| {
        // The patterns are compile-time consts; a failure here is a programmer
        // error surfaced at first use in tests — degrade to a never-matching
        // regex rather than panic in an engine path.
        regex::Regex::new(pattern)
            .unwrap_or_else(|_| regex::Regex::new(r"\z.").expect("fallback regex"))
    })
}

/// `hard_category` (§9i — the exact rules, in order): (1) `kind != "constitution"`
/// ⇒ NOT hard (`preference`/`creative-dna` are soft by definition). (2) a
/// `constitution` row with a §4.1 `v:1` payload: `category == "legal"` ⇒ HARD
/// always; `technical`/`delivery` ⇒ HARD iff `rule ⊕ " " ⊕ value` matches the
/// hard lexicon; `brand`/`creative` ⇒ soft. (3) a payload-less `constitution` ⇒
/// HARD iff the whole `text` matches the lexicon; category recorded
/// `"technical"`. (4) a `v != 1` payload ⇒ treated payload-less (rule 3).
fn hard_category(note: &ResolvedNote) -> Option<String> {
    if note.kind != "constitution" {
        return None; // rule 1
    }
    let payload_v1 = note
        .payload
        .as_ref()
        .filter(|p| p.get("v").and_then(serde_json::Value::as_i64) == Some(1));
    match payload_v1 {
        Some(p) => {
            // rule 2 — the §4.1 shape: {rule, value, category}.
            let category = p.get("category").and_then(serde_json::Value::as_str).unwrap_or("");
            match category {
                "legal" => Some("legal".to_string()),
                "technical" | "delivery" => {
                    let rule = p.get("rule").and_then(serde_json::Value::as_str).unwrap_or("");
                    let value = p.get("value").and_then(serde_json::Value::as_str).unwrap_or("");
                    if lexicon_matches(&format!("{rule} {value}")) {
                        Some(category.to_string())
                    } else {
                        None
                    }
                }
                _ => None, // brand / creative / unknown ⇒ soft
            }
        }
        // rules 3 + 4 — payload-less (or v != 1, treated payload-less).
        None => {
            if lexicon_matches(&note.text) {
                Some("technical".to_string())
            } else {
                None
            }
        }
    }
}

/// Classify A2's typed rows into the hard constraints (§9i). Pure; preserves the
/// input (kind-major traversal) order; HARD text = the DTO `text` verbatim.
pub fn classify_hard(notes: &[ResolvedNote]) -> Vec<HardRule> {
    notes
        .iter()
        .filter_map(|n| {
            hard_category(n).map(|category| HardRule {
                id: n.id.clone(),
                scope: n.scope.clone(),
                category,
                text: n.text.clone(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn note(kind: &str, text: &str, payload: Option<serde_json::Value>) -> ResolvedNote {
        ResolvedNote {
            id: format!("n-{kind}-{}", text.len()),
            scope: "board".to_string(),
            kind: kind.to_string(),
            text: text.to_string(),
            payload,
            author_role: None,
            updated_at: 1,
            link_index: 3,
        }
    }

    #[test]
    fn legal_always_hard_brand_never() {
        let legal = note(
            "constitution",
            "clear all music",
            Some(json!({"v":1, "rule":"music", "value":"clear it", "category":"legal"})),
        );
        assert_eq!(classify_hard(&[legal]).len(), 1);
        let brand = note(
            "constitution",
            "brand must always be huge",
            Some(json!({"v":1, "rule":"logo", "value":"must always be huge", "category":"brand"})),
        );
        assert!(classify_hard(&[brand]).is_empty(), "brand is soft even with modals");
    }

    #[test]
    fn technical_needs_lexicon() {
        let loud = note(
            "constitution",
            "mix to -14 LUFS",
            Some(json!({"v":1, "rule":"loudness", "value":"-14 LUFS", "category":"technical"})),
        );
        let hits = classify_hard(&[loud]);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].category, "technical");
        assert_eq!(hits[0].text, "mix to -14 LUFS", "text is the DTO text verbatim");

        let prose = note(
            "constitution",
            "keep the grade warm",
            Some(json!({"v":1, "rule":"grade", "value":"keep it warm", "category":"technical"})),
        );
        assert!(classify_hard(&[prose]).is_empty(), "technical prose w/o lexicon is soft");
    }

    #[test]
    fn payload_less_text_rule_and_v2_fallback() {
        let bare = note("constitution", "Never crop the logo", None);
        let hits = classify_hard(&[bare]);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].category, "technical", "payload-less lexicon hit records technical");

        // v:2 payload falls to the text rule (rule 4).
        let v2 = note(
            "constitution",
            "deliver 1920x1080 masters",
            Some(json!({"v":2, "category":"legal"})),
        );
        let hits = classify_hard(&[v2]);
        assert_eq!(hits.len(), 1, "v2 payload treated payload-less; resolution token hits");
        assert_eq!(hits[0].category, "technical");
    }

    #[test]
    fn non_constitution_kinds_never_hard() {
        let pref = note("preference", "always render fast", None);
        let dna = note("creative-dna", "must feel handmade", None);
        assert!(classify_hard(&[pref, dna]).is_empty());
    }

    #[test]
    fn lexicon_classes_match() {
        assert!(lexicon_matches("loudness -14 LUFS"));
        assert!(lexicon_matches("23.976 fps"));
        assert!(lexicon_matches("3840x2160"));
        assert!(lexicon_matches("start at 01:02:03:04"));
        assert!(lexicon_matches("start at 01:02:03;04"));
        assert!(lexicon_matches("ONLY use the approved LUT"));
        assert!(!lexicon_matches("keep it warm and gentle"));
        // Word boundary: "communist" must not hit "must"… ("mustard" contains
        // "must" only without a boundary).
        assert!(!lexicon_matches("mustard-colored gel"));
    }

    #[test]
    fn order_preserved_kind_major_input() {
        let a = note("constitution", "must do A", None);
        let b = note("constitution", "never do BB", None);
        let out = classify_hard(&[a.clone(), b.clone()]);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].id, a.id);
        assert_eq!(out[1].id, b.id);
    }
}
