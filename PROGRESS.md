# PROGRESS — substrate hardening run (feat/substrate-e2e)

Timestamped log for remote watching. One line at phase start + end with gate result.

## PHASE 0 — branch safety + baseline

- 2026-06-18 12:48 PDT — START. On branch `feat/substrate-e2e` (already created; NOT main).
  `git status -s` shows only the expected untracked substrate files. `cargo build` ✅.
  `cargo test --no-run` ✅ (all targets compile, including `tests/substrate_discovery`).
- 2026-06-18 12:48 PDT — Baseline note: `cargo test --lib` = 19 passed, **1 pre-existing
  FAILURE**: `diagram_gen::tests::test_parse_diagram_json`. Root cause: `parse_diagram_response`
  hardcodes `svg: None` (src/diagram_gen.rs:543) while the test asserts `svg.is_some()`.
  This is committed shipping code, **unrelated to the P2P substrate**, in enrichment-adjacent
  diagram-generation code that the spec marks out of scope. Standing rules forbid touching it
  ("never chase pre-existing red", "never change shipping behavior"). **Substrate-relevant
  baseline is GREEN** (engine compiles; substrate scaffolds are the `todo!()` backlog this run
  implements). Per-phase gates are scoped to substrate-relevant targets; the pre-existing
  diagram red is excluded and re-noted at FINISH.
- 2026-06-18 12:48 PDT — END. Gate: substrate-relevant baseline GREEN. Proceeding to PHASE 1.

### Gate finding — `clippy -D warnings` is pre-existing-red (whole tree)
`cargo clippy --all-targets -- -D warnings` is **not** green at baseline: the lib alone
emits ~1027 warnings (unused imports across skills/bridges, plus a codebase-wide
`disallowed_methods` lint that flags every `.unwrap()`), ~711–751 across all targets.
These are pre-existing in shipping code unrelated to the substrate. Standing rules forbid
fixing them ("never chase pre-existing red", "never change shipping behavior", "small
reviewable diffs"). **Decision (documented, not a spec/test weakening):** the clippy gate
is enforced as "my diff introduces ZERO new clippy warnings", verified per phase. Whole-tree
`-D warnings` cannot be made green within scope; re-noted at FINISH as a real finding.

## PHASE 1 — NodeConfig seam

- 2026-06-18 12:55 PDT — START. Add `src/models/node_config.rs` (NodeConfig, RelayPolicy,
  DiscoveryPolicy, pure `relay_mode_for`); thread `cfg: NodeConfig` into `NetworkActor::new`;
  add `RelayMode::Disabled` branch; replace `RELAY_URL`/`DISCOVERY_KEY` reads with cfg fields;
  build NodeConfig from globals at the FFI init site (seam, not change); update 2 test bins.
- 2026-06-18 12:55 PDT — END. Gate: `cargo build` ✅; `cargo test --lib node_config` ✅ (4/4
  relay_mode_for cases: Disabled→Disabled, Url→Custom, invalid Url→Default, Default→Default);
  all test targets compile ✅; clippy = 0 new warnings from my diff (verified node_config.rs
  clean; network_actor/lib warnings all pre-existing, line-shifted only). Shipping behavior
  unchanged — production still derives NodeConfig from RELAY_URL/DISCOVERY_KEY/BOOTSTRAP_NODE_ID.
  GREEN within scope. Committing.
