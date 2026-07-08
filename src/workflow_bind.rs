//! Deterministic rung-1 tool binding for authored workflow steps — the DEVICE
//! mirror of cyan-lens `src/workflow/binding.rs` (same mention grammar, same
//! inline `key=value` synthesis, same "never guess" rule), extended with the one
//! thing only the device can do: resolve a `#file` reference to the board's REAL
//! file row (name → local_path + content hash), so a mechanical step binds the
//! SPECIFIC attached file and never needs a routing LLM turn.
//!
//! Compile (Review) calls [`bind_step`] and stamps the result into the cell's
//! `metadata.mcp_tool`; the run path dispatches bound steps through the local
//! cyan-mcp `PluginHost` (`pipeline_executor::execute_local_mcp_tool_step`),
//! bypassing the Lens ReAct loop entirely — `react_turns_for_routing = 0` by
//! construction, GPU not required.

use std::path::Path;

use serde_json::{Map, Value, json};

use crate::models::dto::FileDTO;

/// An `@` mention parsed from step text: `@plugin` or `@plugin.tool`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mention {
    pub plugin: String,
    pub tool: Option<String>,
}

/// The outcome of deterministic binding for one authored step.
#[derive(Debug, Clone)]
pub enum BindOutcome {
    /// Fully bound: plugin, tool, and a complete `args` object — dispatch
    /// locally with zero LLM turns.
    Bound(StepBind),
    /// An `@` mention was present but did not resolve (or its required args
    /// aren't deterministically available). The step stays on the lens path;
    /// `reason` is surfaced for authoring feedback.
    Miss { mention: String, reason: String },
    /// No `@` mention — an ordinary (creative / manual) step.
    None,
}

/// A resolved deterministic bind. `pending` lists required schema props not
/// resolvable at Review time (inline/`#file`) — the dispatch fills them from
/// UPSTREAM STEP OUTPUTS by exact key match (still deterministic, zero LLM;
/// e.g. `list_comments.file_id` from the upload step's `{"file_id": …}`
/// result). A prop still missing at dispatch is a CLEAR error, never a guess.
#[derive(Debug, Clone)]
pub struct StepBind {
    pub plugin_id: String,
    pub tool: String,
    pub args: Value,
    pub side_effects: Vec<String>,
    pub pending: Vec<String>,
}

/// Parse the FIRST `@plugin`/`@plugin.tool` mention in `content`, if any.
/// Same grammar as the lens: `@` at token start, `[A-Za-z0-9_-]+` segments,
/// trailing punctuation ignored.
pub fn parse_mention(content: &str) -> Option<Mention> {
    for token in content.split_whitespace() {
        let Some(rest) = token.strip_prefix('@') else {
            continue;
        };
        let mention: String = rest
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '-' || *c == '.')
            .collect();
        let mention = mention.trim_matches('.');
        if mention.is_empty() {
            continue;
        }
        return Some(match mention.split_once('.') {
            Some((plugin, tool)) if !tool.is_empty() => Mention {
                plugin: plugin.to_string(),
                tool: Some(tool.to_string()),
            },
            _ => Mention {
                plugin: mention.to_string(),
                tool: None,
            },
        });
    }
    None
}

/// `#ref` tokens in the step text (the file references the author picked from
/// autocomplete). Trailing punctuation is ignored, matching the mention grammar.
pub fn file_refs(content: &str) -> Vec<String> {
    content
        .split_whitespace()
        .filter_map(|tok| {
            let rest = tok.strip_prefix('#')?;
            let r: String = rest.chars().take_while(|c| !",;:)".contains(*c)).collect();
            if r.is_empty() { None } else { Some(r) }
        })
        .collect()
}

/// Resolve a `#ref` against the board's files (board-scoped rows first, then
/// the group's), by NAME (case-insensitive) or id / id-prefix (≥8 chars). The
/// specific attached file, not "whatever the model picked".
pub fn resolve_file_ref(board_id: &str, tenant_id: &str, reference: &str) -> Option<FileDTO> {
    let ref_lc = reference.to_lowercase();
    let board_files = crate::storage::file_list_by_board(board_id).unwrap_or_default();
    let group_files = crate::storage::file_list_by_group(tenant_id).unwrap_or_default();
    let all = board_files.into_iter().chain(group_files);
    let mut by_id_prefix: Option<FileDTO> = None;
    for f in all {
        if f.name.to_lowercase() == ref_lc || f.id == reference {
            return Some(f);
        }
        if reference.len() >= 8 && f.id.starts_with(&ref_lc) && by_id_prefix.is_none() {
            by_id_prefix = Some(f);
        }
    }
    by_id_prefix
}

/// Argument-schema property names a resolved `#file` may fill (first unfilled
/// required one wins). Kept tiny and explicit — deterministic, never fuzzy.
const FILE_PATH_PROPS: [&str; 3] = ["file_path", "input", "path"];

/// The plugin's ENV CONTEXT for a still-unfilled prop — the same context its
/// credential is injected from at spawn, by the shared convention
/// `<PLUGIN>_<PROP>` uppercased: `frameio` + `account_id` → `FRAMEIO_ACCOUNT_ID`.
/// This is how a mechanical step inherits ambient account identity without the
/// author re-typing it on every step (found live: create_comment bound with
/// `account_id` pending, nothing upstream carried the key, and the comment
/// never posted).
pub fn env_context_value(plugin_id: &str, prop: &str) -> Option<String> {
    let var = format!(
        "{}_{}",
        crate::mcp_host::env_token(plugin_id),
        crate::mcp_host::env_token(prop)
    );
    std::env::var(&var).ok().filter(|v| !v.trim().is_empty())
}

/// The step's authored INTENT: the English minus the `@mention`, the inline
/// `key=value` tokens, and the `#file` references — what a human reads as "the
/// message". Fills an unfilled `text` prop (create_comment's note body) so the
/// comment the reviewer sees is the sentence the author wrote, deterministically.
pub fn step_intent(content: &str) -> String {
    content
        .split_whitespace()
        .filter(|t| !t.starts_with('@') && !t.starts_with('#') && !t.contains('='))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Deterministically bind one authored step for a board.
///
/// Rung-1 ladder, all pure lookups:
///   1. `@plugin[.tool]` mention parse (none → `BindOutcome::None`).
///   2. The plugin must be INSTALLED IN THIS BOARD'S GROUP (the group's Plugins workspace has its
///      `.cyanplugin`) — per-group scoping, not device-global.
///   3. The tool must exist in the unpacked bundle's manifest (name or alias); a bare `@plugin`
///      binds only a single-tool manifest.
///   4. `args` synthesized from inline `key=value` tokens + `#file` references (schema-typed
///      coercion). Any unfilled `required` prop ⇒ Miss, never a guess.
pub fn bind_step(board_id: &str, content: &str) -> BindOutcome {
    let Some(mention) = parse_mention(content) else {
        return BindOutcome::None;
    };
    let mention_str = match &mention.tool {
        Some(t) => format!("@{}.{}", mention.plugin, t),
        None => format!("@{}", mention.plugin),
    };
    let tenant_id = crate::storage::board_get_group_id(board_id)
        .filter(|g| !g.is_empty())
        .unwrap_or_else(|| "device".to_string());

    // 2 — per-group install check.
    let installed = crate::storage::plugin_bundles_in_group(
        &tenant_id,
        crate::mcp_host::PLUGINS_WORKSPACE_NAME,
        crate::mcp_host::PLUGIN_BUNDLE_SUFFIX,
    )
    .unwrap_or_default();
    let plugin_lc = mention.plugin.to_lowercase();
    let installed_here = installed.iter().any(|p| {
        p.name
            .strip_suffix(crate::mcp_host::PLUGIN_BUNDLE_SUFFIX)
            .unwrap_or(&p.name)
            .to_lowercase()
            == plugin_lc
    });
    if !installed_here {
        return BindOutcome::Miss {
            mention: mention_str,
            reason: "plugin_not_installed_in_group".to_string(),
        };
    }

    // 3 — manifest lookup from the unpacked bundle.
    let Some(bundle_dir) = crate::storage::ensure_bundle_unpacked(&mention.plugin) else {
        return BindOutcome::Miss {
            mention: mention_str,
            reason: "bundle_not_unpacked".to_string(),
        };
    };
    let manifest = match cyan_mcp::Manifest::from_bundle(&bundle_dir) {
        Ok(m) => m,
        Err(_) =>
            return BindOutcome::Miss {
                mention: mention_str,
                reason: "manifest_unreadable".to_string(),
            },
    };
    bind_with_manifest(board_id, &tenant_id, content, &mention, &manifest)
}

/// The manifest-driven half of [`bind_step`], split out so tests can drive it
/// with a hand-built manifest and a temp DB (no bundle unpack required).
pub fn bind_with_manifest(
    board_id: &str,
    tenant_id: &str,
    content: &str,
    mention: &Mention,
    manifest: &cyan_mcp::Manifest,
) -> BindOutcome {
    let mention_str = match &mention.tool {
        Some(t) => format!("@{}.{}", mention.plugin, t),
        None => format!("@{}", mention.plugin),
    };
    let tool_block = match &mention.tool {
        Some(tool) => {
            let tool_lc = tool.to_lowercase();
            manifest.tools.iter().find(|t| {
                t.name.to_lowercase() == tool_lc
                    || t.aliases.iter().any(|al| al.to_lowercase() == tool_lc)
            })
        }
        None if manifest.tools.len() == 1 => manifest.tools.first(),
        None => None,
    };
    let Some(tool_block) = tool_block else {
        let reason = if mention.tool.is_some() {
            "tool_not_in_manifest"
        } else {
            "ambiguous_plugin_mention"
        };
        return BindOutcome::Miss {
            mention: mention_str,
            reason: reason.to_string(),
        };
    };

    // A resolved `@plugin.tool` mention is MECHANICAL by declaration — it always
    // binds locally. Required args that inline/`#file` can't fill are stamped
    // `pending` for the dispatch to resolve from upstream step outputs (a model
    // must NEVER guess a mechanical tool's args — that was the wrong-file bug).
    let (args, pending) = synthesize_args(
        board_id,
        tenant_id,
        &manifest.name,
        &tool_block.input_schema,
        content,
    );
    BindOutcome::Bound(StepBind {
        plugin_id: manifest.name.clone(),
        tool: tool_block.name.clone(),
        args,
        side_effects: tool_block.side_effects.clone(),
        pending,
    })
}

/// Build the bound tool's `args` from deterministic sources only: inline
/// `key=value` tokens, then `#file` references resolved to the board's real
/// rows (path prop + a `name` default). Returns `(args, pending)` where
/// `pending` = required props still unfilled (the dispatch resolves them from
/// upstream outputs by key; missing there ⇒ a clear error, never a guess).
fn synthesize_args(
    board_id: &str,
    tenant_id: &str,
    plugin_id: &str,
    input_schema: &Value,
    content: &str,
) -> (Value, Vec<String>) {
    let properties = input_schema
        .get("properties")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let required: Vec<&str> = input_schema
        .get("required")
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();

    let inline = inline_kv_pairs(content);
    let mut args = Map::new();

    for (name, prop_schema) in &properties {
        if let Some(raw) = inline.iter().find(|(k, _)| k == name).map(|(_, v)| v) {
            args.insert(name.clone(), coerce(raw, prop_schema));
        }
    }

    // `#file` references fill the first unfilled path-ish prop + `name`.
    let mut fill_file = |file: &FileDTO, args: &mut Map<String, Value>| {
        if let Some(path) = file.local_path.as_deref().filter(|p| !p.is_empty()) {
            for prop in FILE_PATH_PROPS {
                if properties.contains_key(prop) && !args.contains_key(prop) {
                    args.insert(prop.to_string(), json!(path));
                    break;
                }
            }
        }
        if properties.contains_key("name") && !args.contains_key("name") {
            args.insert(
                "name".to_string(),
                json!(
                    Path::new(&file.name)
                        .file_name()
                        .and_then(|s| s.to_str())
                        .unwrap_or(&file.name)
                ),
            );
        }
        // Provenance for the run path + player: the content hash proves WHICH
        // bytes were bound (the wrong-file bug's regression rail). Only stamped
        // when the schema declares it — most tools don't.
        if properties.contains_key("file_hash") && !args.contains_key("file_hash") {
            args.insert("file_hash".to_string(), json!(file.hash));
        }
    };
    let refs = file_refs(content);
    for reference in &refs {
        let Some(file) = resolve_file_ref(board_id, tenant_id, reference) else {
            continue;
        };
        fill_file(&file, &mut args);
    }

    // TIER 2 — the IMPLICIT "attached master": a step authored WITHOUT a
    // `#reference` binds the board's REAL attachment when that is unambiguous
    // (exactly one content-distinct BOARD file with local bytes). Never a
    // seed, never the group's other files, never a guess between two clips —
    // ambiguity stays `pending` (dispatch fills from upstream outputs or
    // errors clearly).
    if refs.is_empty()
        && FILE_PATH_PROPS
            .iter()
            .any(|p| properties.contains_key(*p) && !args.contains_key(*p))
    {
        let board_files = crate::storage::file_list_by_board(board_id).unwrap_or_default();
        let mut with_bytes: Vec<&FileDTO> = board_files
            .iter()
            .filter(|f| f.local_path.as_deref().is_some_and(|p| !p.is_empty()))
            .collect();
        let mut distinct = std::collections::HashSet::new();
        with_bytes.retain(|f| {
            distinct.insert(if f.hash.is_empty() { f.id.clone() } else { f.hash.clone() })
        });
        if let [only] = with_bytes.as_slice() {
            fill_file(only, &mut args);
        }
    }

    // ENV-CONTEXT fallback for required props the author didn't inline — the
    // plugin's ambient identity (e.g. frameio's FRAMEIO_ACCOUNT_ID, the same
    // context its token is injected from at spawn). Required-only: an optional
    // prop from env would surprise; a required one unblocks the bind.
    for r in &required {
        if !args.contains_key(*r)
            && let Some(v) = env_context_value(plugin_id, r)
        {
            args.insert((*r).to_string(), json!(v));
        }
    }

    // The COMMENT/MESSAGE body: an unfilled `text` prop takes the authored
    // intent (the step's English minus mention/kv/#ref tokens) — the reviewer
    // reads the sentence the author wrote, never an empty post.
    if properties.contains_key("text") && !args.contains_key("text") {
        let intent = step_intent(content);
        if !intent.is_empty() {
            args.insert("text".to_string(), json!(intent));
        }
    }

    let pending: Vec<String> = required
        .iter()
        .filter(|r| !args.contains_key(**r))
        .map(|r| r.to_string())
        .collect();
    (Value::Object(args), pending)
}

/// `key=value` tokens in the step text (single-token values; quotes trimmed) —
/// identical to the lens's authoring grammar.
fn inline_kv_pairs(content: &str) -> Vec<(String, String)> {
    content
        .split_whitespace()
        .filter_map(|tok| {
            let (k, v) = tok.split_once('=')?;
            if k.is_empty() || v.is_empty() {
                return None;
            }
            let v = v.trim_matches(|c| c == '"' || c == '\'');
            if v.is_empty() {
                return None;
            }
            Some((k.to_string(), v.to_string()))
        })
        .collect()
}

/// Coerce an inline string to the schema property's declared type; unparseable
/// values stay strings (the plugin's own validation is the oracle).
fn coerce(raw: &str, prop_schema: &Value) -> Value {
    match prop_schema.get("type").and_then(Value::as_str) {
        Some("integer") => raw
            .parse::<i64>()
            .map(Value::from)
            .unwrap_or_else(|_| Value::String(raw.to_string())),
        Some("number") => raw
            .parse::<f64>()
            .map(Value::from)
            .unwrap_or_else(|_| Value::String(raw.to_string())),
        Some("boolean") => raw
            .parse::<bool>()
            .map(Value::from)
            .unwrap_or_else(|_| Value::String(raw.to_string())),
        _ => Value::String(raw.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plugin_dot_tool_mention() {
        let m = parse_mention("push the cut @frameio.upload_file now").expect("mention");
        assert_eq!(m.plugin, "frameio");
        assert_eq!(m.tool.as_deref(), Some("upload_file"));
    }

    #[test]
    fn parses_bare_plugin_and_trailing_punctuation() {
        let m = parse_mention("use @frameio, please").expect("mention");
        assert_eq!(m.plugin, "frameio");
        assert_eq!(m.tool, None);
    }

    #[test]
    fn no_mention_is_none() {
        assert!(parse_mention("plain english step").is_none());
    }

    #[test]
    fn file_refs_strip_trailing_punctuation() {
        assert_eq!(file_refs("upload #sig_source.mp4, then #abc123 now"), vec![
            "sig_source.mp4".to_string(),
            "abc123".to_string()
        ]);
    }

    #[test]
    fn inline_kv_and_coercion() {
        let pairs = inline_kv_pairs("do x=1 y=\"two\" empty= =bad ok=true");
        assert_eq!(pairs, vec![
            ("x".to_string(), "1".to_string()),
            ("y".to_string(), "two".to_string()),
            ("ok".to_string(), "true".to_string()),
        ]);
        assert_eq!(coerce("7", &serde_json::json!({"type":"integer"})), 7);
        assert_eq!(coerce("x", &serde_json::json!({"type":"integer"})), "x");
        assert_eq!(coerce("true", &serde_json::json!({"type":"boolean"})), true);
    }
}
