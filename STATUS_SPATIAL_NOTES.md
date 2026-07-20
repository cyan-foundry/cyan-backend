# STATUS — spatial notes (`ref` / `region` / `intent_struct`)

Branch `feat/spatial-notes`. Executes RUN_OPUS_SPATIAL_NOTES.md against
REVIEW_WAIST_SPEC §1–§3 + §5, ANNOTATION_TAXONOMY §3, PLAYER_SENSOR_SPEC §4.

## What landed

- **`src/spatial.rs`** (new) — the four referent classes (`source`, `junction`,
  `entry`, `version`), the region seed, `intent_struct`, the unhashed
  capture-context block, the `FrameMapProvider` / `LineageProvider` seams, the
  resolution rules, the proxy-orphan guard, and cross-org export gating.
- **`src/changelist.rs`** — four additive nullable columns
  (`ref_json`, `region_json`, `intent_struct_json`, `capture_ctx_json`),
  PRAGMA-guarded ALTERs, the canonical-hash extension, and both write paths
  (`append`, `apply_entry`) carrying the groups.
- **`tests/spatial_notes_test.rs`** (new) — the §5 suite by name, 23 tests,
  green, with the fixture cut model (retimes, dissolves, intercuts, lineage).

Two commits: `8ce7e53` (schema + hash), `3bdd266` (resolution + suite).

## T-HASH-BACKCOMPAT — the law holds

`compute_entry_hash` gains keys **only when a group is present**. With all three
absent the canonical object is byte-identical to before, so every pre-region
`entry_hash` — and every `list_hash`/`cut_hash` built from them — is unchanged.

Golden hashes were captured by running the pre-change code at `ea45a25`, not
re-derived from the new code, so the test cannot be vacuous. Verified
load-bearing by mutation: injecting a single `"ref": null` moved
`note_plain` from `fc3b51…` to `39ca24…` and the test failed with the intended
message. Reverted; green.

## Red-before-green

With `resolve()` and `check_proxy_swap` stubbed, **11 of 22 failed** — exactly
the resolution and proxy-guard tests. Implementing the rules took all 11 to
green.

The other 11 were already green from the schema commit and had never been
observed failing, so each load-bearing claim was mutation-tested instead:

| Mutation | Test that caught it |
|---|---|
| `null` instead of omitted in `canonical_extra` | T-HASH-BACKCOMPAT (+ the absent-keys test) |
| `capture_ctx` folded into the content hash | T-HASH-CANONICAL-REGION dedup + replication |
| `region` dropped on the `apply_entry` path | replication/union test |
| version-class notes allowed to migrate | T-VERSION-SPAN |

All reverted; suite 23/23.

## Honestly deferred — engine gaps, not test weakening

1. **Prod `FrameMapProvider` is NOT implemented.** The spec's model is a
   multi-source cut (`v1 = A[0..100] B[0..80] C[0..120]`); the shipped ledger
   models ops against ONE master `asset_hash`, and `conform_map.rs` is a
   single-asset proxy⇄master frame map. There is no cut-structure to back
   `occurrences`/`boundaries`/`dominant_at` in production. The trait, the rules
   and the fixture provider are complete and proven; wiring a real provider needs
   a multi-source cut representation, which is a design decision, not a fill-in.
2. **`LineageProvider` prod impl not wired.** The edges (`swap{new_asset_hash}`
   ops, `derived_from` facts) are derivable from `change_entry` rows, but no
   `derived_from` fact is written anywhere in the engine today, so a prod impl
   would resolve only swap edges. Flagged rather than half-built.
3. **T-PROXY-ORPHAN-GUARD is enforced at the seam, not wired.**
   `spatial::check_proxy_swap` implements and tests the rule, but `ReviewState`
   carries no `proxy_ref`, so there is no stored round→proxy binding for
   `publish_proxy` to check against. Wiring it means a schema + state change to
   shipping review state — deliberately not done as a side effect of this run.
4. **T-LOSSY-DECLARED not implemented.** The AAF / Pro Tools / Premiere / OTIO
   projections do not exist in this crate; only the export *gate* and its
   `dropped` record are built and tested. The projection matrix (§4) is its own
   piece of work.

## FFI

Unchanged. The new fields are `Option` + `skip_serializing_if`, so existing
`cyan_changelist_*` JSON is byte-identical for every entry that carries none of
them. No `cyan_*` signature, `SwiftEvent` or `NetworkCommand` variant was
touched. 36 `ChangeEntry` struct literals across `src/` and `tests/` gained four
explicit `None` fields — exhaustiveness checking was kept deliberately (no
`Default` spread) so the next additive field is again a compile error at every
site rather than a silent `None`.
