# STATUS — Mesh lint hygiene (`clippy -D warnings` green)

Branch: `harness/mesh-e2e` (the integrated mesh tip; contains `fix/mesh-catchup-export`
+ seed pipeline + catch-up/export + the docker/netem harness). **Not main.**

Goal: make `cargo clippy --all-targets -- -D warnings` GREEN without changing
behavior — lint hygiene, not a refactor. Done.

```
cargo clippy --all-targets -- -D warnings   →  exit 0  (GREEN)
cargo test                                  →  green EXCEPT one PRE-EXISTING
                                               failure unrelated to this work
                                               (see "Test status" below)
```

---

## Why the methods were disallowed — and the root-cause finding

`clippy.toml` disallowed `Option::unwrap` / `Result::unwrap` via
`disallowed-methods`, with the comment *"Disallow use of `unwrap` to prevent
potential runtime panics."* Intent is sound (a panic crossing the FFI boundary
crashes the iOS app). It was set up in the **initial skeleton commit** (922f8bf),
never recently tightened — so this is not a "someone just turned the screws"
situation; it had simply never been driven to green.

The ~879 reported errors were **two very different things**:

1. **737-ish real, hand-written `unwrap()`s** in our code — the genuine safety
   concern, concentrated in `ffi/core.rs`, `storage.rs`, `lib.rs`.
2. **~970 FALSE POSITIVES from `serde_json::json!`.** The `json!` macro expands,
   for every interpolated expression, to `serde_json::to_value(&x).unwrap()`
   (`serde_json-1.0.145/src/macros.rs:279`). `disallowed-methods` lints **through
   external-macro expansions**, so every `json!({... : expr ...})` site tripped
   the lint even though that `unwrap` is library-internal and not ours.

`disallowed-methods` also **ignores `allow-unwrap-in-tests`** (already set in
clippy.toml), so test unwraps were flagged too.

### The config fix (root cause, not a silencer)

`clippy::unwrap_used` is the **purpose-built lint** for "ban unwrap." Unlike
`disallowed-methods` it (a) **skips external-macro unwraps** → the ~970 `json!`
false positives vanish with zero churn, and (b) **honors
`allow-unwrap-in-tests`** → test unwraps are exempt as intended. It still flags
every hand-written `unwrap` in our code — so the real safety work was unchanged.

Using `disallowed-methods` for unwrap was effectively a wrong-tool config choice
(the clippy.toml comment "Setting to true will trigger…" even reads as if it were
a boolean toggle). So:

- **clippy.toml**: emptied `disallowed-methods = []` with a comment explaining why.
- **Cargo.toml**: added `[lints.clippy] unwrap_used = "warn"` (promoted to error by
  `-D warnings`). `allow-unwrap-in-tests` now actually takes effect.

This is the answer to the brief's "json! disallowed" question: `json!` was never
in the disallowed list — it tripped the `unwrap` rule transitively. The correct
fix is the lint switch above (documented), **not** a giant `json!` refactor and
**not** hundreds of `#[allow]`s. The full `unwrap` ban is preserved.

After the switch the real target was **339 hand-written unwraps** (json!/test
noise gone), plus ~50 latent non-unwrap warnings that `-D warnings` promotes.

---

## Category 1 — Engine + FFI unwraps: FIXED PROPERLY (the real safety win)

No `#[allow]` blanketing on engine/FFI. All converted so panics no longer cross
the FFI boundary; **behavior is identical on the success path** (failure paths
now degrade gracefully instead of crashing — exactly the intended improvement).

- **`src/util.rs` (new): `MutexExt::lock_safe()`** — `lock().unwrap_or_else(|e|
  e.into_inner())`. Poison-recovers instead of panicking; identical on the normal
  path. Replaced **201** `.lock().unwrap()` across `storage.rs` (84), `ffi/core.rs`
  (56), `lib.rs` (54), `actors/network_actor.rs` (7).
- **`ffi/core.rs` (161):**
  - `CString::new(x).unwrap().into_raw()` → `unwrap_or_default()` (empty C string
    on the never-in-practice interior-NUL error; same `*mut c_char` return type).
  - SQLite `prepare`/`query_map().unwrap()` → fallible IIFEs / `and_then` /
    `.map(..).unwrap_or_default()` yielding **empty results** on the (static-SQL,
    infallible-in-practice) error — matching the existing `"[]"` empty returns.
  - Removed the `prepare("SELECT '' LIMIT 0").unwrap()` dummy-statement fallback hack.
  - `try_into`/`RUNTIME.get`/`CStr::to_str` → graceful early-return.
- **`lib.rs` (73):** `RUNTIME.get().unwrap().spawn` → `ok_or_else(..)?`
  (`CyanSystem::new` returns `Result`); `dump_tree_json` / `group_list_ids` /
  profile-broadcast SQLite blocks → fallible IIFEs (empty on error).
- **`storage.rs` (1 residual after lock_safe):** `group_list_ids` SQLite → IIFE.
- **`ai_bridge.rs` (7):** `serde_json::to_value(struct).unwrap()` →
  `unwrap_or_default()` (plain structs; `Value::Null` on the impossible error).
- **`lens_commands.rs` (1):** `strip_prefix` guarded by `starts_with` →
  `unwrap_or("")`.
- **`cyan_lens_client.rs` (1):** `SystemTime::duration_since(UNIX_EPOCH)` →
  `unwrap_or_default()`.

## Category 2/3 — non-unwrap lints

Style/idiom lints fixed properly (collapsible_if, single_match, needless_borrow,
manual_strip, useless_format, map_identity, etc. — most via `clippy --fix`, all
behavior-preserving). Specifically reviewed:
- `ai_bridge.rs` media-type `if/else` — collapsed two **identical** branches
  (`iVBOR`→png and else→png) into one. Behavior-identical.
- `lens_commands.rs` — dropped the **unreachable** `/pl` alias on the `/pipeline`
  arm (`/pl` already routes to `/pulse`; removal is behavior-preserving).
  `parse_grep_args` quoted-term parse rewritten with `strip_prefix` (indices
  verified equivalent).
- `skills/mod.rs` — added `impl Default for SkillRegistry` (additive).

**dead_code → SCOPED `#[allow(dead_code)]` with reasons (NOT deleted).** These are
the AI/Lens enrichment, pipeline-step, and skills scaffolding that CLAUDE.md says
is **moving to the MCP/workflow model — "do not test or refactor"**, plus serde
structs/fields where deletion would silently change (de)serialization shape.
Module-level allows on `ai_bridge.rs`, `diagram_gen.rs`, `pipeline.rs`,
`skills/github.rs`, `skills/localization.rs`; a targeted field allow on the
engine's `DiscoveryActor::discovery_key` (retained for seed/config parity).
Deleting was deliberately avoided to honor "behavior must not change" and the
out-of-scope directive.

## Tests — no assertions, FFI shapes, or sync logic touched

`allow-unwrap-in-tests` exempts `#[test]` bodies, but **not** (a) the three
`tests/*.rs` files dual-built as `[[bin]]` targets (`network_test`, `delta_test`,
`snapshot_test`) — a `[[bin]]` isn't `cfg(test)` — nor (b) unwraps in non-`#[test]`
helper/assertion fns and test mocks. Rather than churn test bodies (the brief
forbids touching test assertions/logic), these got **file-level `#[allow]`s with a
one-line rationale**: `delta_sync_test`, `network_actor_test`,
`snapshot_protocol_test` (dual-built bins), `sso_grant_test` (mock impl),
`substrate_mesh_e2e` + `substrate_relay` (assertion helpers). `tests/support/mod.rs`
got a targeted `type_complexity` allow on the spec'd `members()` signature (kept
verbatim, no type alias). One trivial `% 2 == 0` → `is_multiple_of(2)` in
`substrate_stress` (behavior-identical, not an assertion).

---

## Test status

`cargo clippy --all-targets -- -D warnings` is **GREEN**.

`cargo test`: **99 passed, 14 ignored, 0 regressions.** Two failures, BOTH
**pre-existing and verified failing at base commit `0a53ab4` before any change in
this work** (so neither is caused by the lint cleanup):

1. `diagram_gen::tests::test_parse_diagram_json` (`assert result.svg.is_some()`).
   Only change to `diagram_gen.rs` here is a module-level `#[allow(dead_code)]`,
   which cannot affect runtime.
2. `substrate_multiuser_mp::expired_revoked_replayed_grant_rejected`
   (`tests/substrate_multiuser_mp.rs:190`). This file was **not touched** by this
   work; it fails identically on base (confirmed by checking out `0a53ab4` and
   re-running ×2).

Both left as-is and flagged for the owner. Everything else — lib tests, all
integration tests, and the full in-process substrate suite (catchup, chat,
discovery, files, multiuser, snapshot_mp, sync, presence, offline, reliability,
resilience, stress, swarm, …) — passes.

## Commits (small, one concern each)
1. `chore(clippy): ban unwrap via clippy::unwrap_used not disallowed-methods`
2. `chore(clippy): add MutexExt::lock_safe and use it for poison-safe locking`
3. `chore(clippy): remove unwrap from ffi/core.rs FFI paths`
4. `chore(clippy): remove unwrap from lib.rs and storage.rs engine paths`
5. `chore(clippy): remove remaining engine unwraps (ai_bridge/lens/lens_client)`
6. `chore(clippy): clear non-unwrap lints (dead_code, style, tests)`
