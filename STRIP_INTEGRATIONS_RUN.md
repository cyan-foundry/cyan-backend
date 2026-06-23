# Strip the in-engine integration subsystem (integrations → MCP servers now)

Integrations have moved to **MCP servers talking to Cyan Lens**, so cyan-backend's
in-engine integration bridge + the `cyan-backend-integrations` crate are obsolete.
Remove them fully (simplicity: delete, don't deprecate). Read `CLAUDE.md`. This is
**stable, FFI-bearing code** — keep the substrate suite green at every step.

## Scope (remove exactly this; keep everything else)
KEEP: `ai_bridge.rs` + `cyan_lens_client.rs` (the AI→cloud-Lens path stays). The P2P
substrate, chat, files, sync — untouched.
REMOVE (the integration subsystem):
- Files: `src/integration_bridge.rs`, `src/lens_bridge.rs`. And `src/import_orchestrator.rs`
  **iff** it is purely integration/`/import`-driven (confirm by build — `/import` is retired).
- Dep: `cyan-backend-integrations = { path = "../cyan-backend-integrations" }` in `Cargo.toml`.
- Wiring in `src/lib.rs`: the `mod integration_bridge;`/`mod lens_bridge;` + re-exports,
  `AppState.integration_bridge`, the `IntegrationBridge::new_with_lens(...)` construction, and
  `integration_bridge.start_event_forwarder()`.
- FFI in `src/ffi/core.rs`: `cyan_integration_command`, `cyan_poll_integration_events`,
  `cyan_get_connected_integrations`, `cyan_get_integration_graph`, and the
  `pub use crate::integration_bridge::IntegrationBridge;`.
- Event variants in `src/models/events.rs`: `IntegrationLensEvent`, `IntegrationAdded`,
  `IntegrationRemoved`, `IntegrationEvent`, `IntegrationStatus`, `IntegrationGraph`
  (+ any `is_integration_event()` helper that becomes dead).

## ⚠️ This is an INTENTIONAL breaking FFI change (coordinated)
Removing the 4 `cyan_integration_*` functions + the `Integration*` events is deliberate.
iOS (`IntegrationComponentActor`) consumes them and is dropping them in its hardening run. So:
the `cyan_*` symbol set WILL lose those 4 — that's expected here (unlike the additive changes).
Note it loudly in STATUS so the **xcframework is only rebuilt for iOS after iOS drops the callers**.

## Standing rules
- Branch `feat/strip-integrations` off `feat/substrate-e2e`. NEVER `main`. Small commits.
- After each phase: `cargo build` ✅ and the substrate suite green
  (`cargo test --no-fail-fast --test substrate_discovery --test substrate_sync --test substrate_chat
   --test substrate_files --test substrate_offline --test substrate_resilience --test substrate_snapshot_mp`).
  Zero new clippy warnings. No `unwrap()`/`panic!` added. ≤6 attempts/phase → STOP + STATUS.

## PHASE 0 — branch + baseline
`git checkout -b feat/strip-integrations feat/substrate-e2e`. Substrate suite green at baseline. Record it.

## PHASE 1 — remove the subsystem + wiring + dep
Delete `integration_bridge.rs`, `lens_bridge.rs` (and `import_orchestrator.rs` if integration-only).
Remove the `cyan-backend-integrations` dep. Strip the `lib.rs` wiring (`AppState` field, construction,
forwarder, mods/re-exports). Let the compiler guide you to every reference.
GATE → commit "strip: remove integration bridge + lens_bridge + integrations dep".

## PHASE 2 — remove the FFI integration surface + event variants
Delete the 4 `cyan_integration_*` FFI fns + the `IntegrationBridge` re-export in `ffi/core.rs`.
Remove the 6 `Integration*` `SwiftEvent`/`NetworkEvent` variants + any now-dead helper. Fix all
match arms / event routing the compiler flags.
GATE → commit "strip: remove cyan_integration_* FFI + Integration* events (coordinated FFI break)".

## PHASE 3 — fix the two legacy test bins
`tests/snapshot_protocol_test.rs` and `tests/network_actor_test.rs` reference integration symbols.
Minimally update them to compile + still exercise what they test (do NOT weaken the substrate suite).
If a bin is wholly integration-driven and now meaningless, mark it `#[ignore]`/stub with a noted reason
rather than faking it.
GATE → full `cargo build --tests` ✅ + substrate suite green. Commit "strip: fix legacy bins for integration removal".

## FINISH
Write `STATUS_STRIP_INTEGRATIONS.md`: files/deps/FFI fns/events removed (counts), confirm the
substrate suite stayed green, and **flag the iOS coordination**: the 4 `cyan_integration_*` symbols
are gone, so iOS must drop `IntegrationComponentActor` before the next `build_static_lib.sh`. End.
