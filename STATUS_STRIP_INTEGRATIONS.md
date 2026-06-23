# STATUS — strip the in-engine integration subsystem

Branch: `feat/strip-integrations` (off `feat/substrate-e2e`, never `main`).
Result: **DONE.** Integration bridge subsystem removed. Build green, full
`cargo build --tests` green, substrate suite green after every phase.

## ⚠️ COORDINATED FFI BREAK — read before rebuilding the xcframework

This run intentionally removes FFI symbols that iOS (`IntegrationComponentActor`)
consumes. **Do NOT run `build_static_lib.sh` / rebuild the xcframework for iOS
until iOS has dropped the callers** in its hardening run.

`cyan_*` symbols removed (6):

| symbol | in run book's "4"? | why |
| --- | --- | --- |
| `cyan_integration_command` | yes | integration bridge entry point |
| `cyan_poll_integration_events` | yes | drained the integration event buffer |
| `cyan_get_connected_integrations` | yes | integration graph query |
| `cyan_get_integration_graph` | yes | integration graph query |
| `cyan_set_graph_focus` | **no — extra finding** | 5th integration-graph FFI fn, also called `integration_bridge.handle_command`; not listed in the run book but coupled to the removed bridge |
| `cyan_import` | **no — extra finding** | the retired `/import` path; sole consumer of `import_orchestrator.rs`, which had to go with the dep |

Component event JSON also loses the `Integration*` `SwiftEvent` variants
(`IntegrationAdded`, `IntegrationRemoved`, `IntegrationEvent`, `IntegrationStatus`,
`IntegrationGraph`) — iOS must stop decoding these too.

## What was removed (counts)

- **Files (3):** `src/integration_bridge.rs` (1336 LOC), `src/lens_bridge.rs`
  (376 LOC), `src/import_orchestrator.rs` (1175 LOC).
- **Dependency (1):** `cyan-backend-integrations` (path dep) from `Cargo.toml`
  (+ `Cargo.lock`).
- **FFI fns (6):** the 4 `cyan_integration_*` listed above + `cyan_set_graph_focus`
  + `cyan_import`. Plus the `pub use crate::integration_bridge::IntegrationBridge;`
  re-export in `ffi/core.rs`.
- **Events (6):** `NetworkEvent::IntegrationLensEvent` + 5 `SwiftEvent` variants
  (`IntegrationAdded`, `IntegrationRemoved`, `IntegrationEvent`, `IntegrationStatus`,
  `IntegrationGraph`). Plus the dead `SwiftEvent::is_integration_event()` helper
  (it had no callers).
- **lib.rs wiring:** `mod integration_bridge` / `mod lens_bridge` / `pub mod
  import_orchestrator` + their re-exports (`IntegrationBridge`, `LensBridge`,
  `RawEvent`, `XfEvent`); `AppState.integration_bridge` field +
  `IntegrationBridge::new_with_lens(...)` construction + `start_event_forwarder()`;
  the `integration_event_buffer` (field, let, clone, struct init) and the now-unused
  `integration` parameter of `route_event_to_buffers`; the `IntegrationLensEvent` and
  `Integration*` route arms; the `IntegrationAdded`/`IntegrationRemoved` event
  emissions in the `AddIntegration`/`RemoveIntegration` command arms.
- **core.rs import:** dropped `use anyhow::{anyhow, Result};` (became unused once
  `cyan_import` was removed — its only consumer).

## What was deliberately KEPT (not part of the subsystem)

- `src/ai_bridge.rs` and `src/cyan_lens_client.rs` — the AI → cloud-Lens path.
- The `integration_bindings` table + migration, `IntegrationBindingDTO`, and the
  `storage::integration_list_by_group` / `storage::integration_insert` helpers —
  still used by `skills/slack.rs`, `pipeline.rs`, and the **snapshot protocol**
  (`SnapshotFrame::Metadata.integrations`). The P2P snapshot/delta sync still
  carries integration bindings unchanged.
- `CommandMsg::AddIntegration` / `RemoveIntegration` and their command-actor arms.
  They still persist to `integration_bindings`; they just no longer emit a
  (now-removed) `SwiftEvent`. Not listed for removal in the run book, and the table
  they write is consumed by kept code, so they stay.

## Deviation from the run book's phase split (build-green forced it)

The run book put the `cyan_integration_*` FFI fns in PHASE 2, separate from the
bridge removal in PHASE 1. That isn't build-green-able: the FFI fns call
`sys.integration_bridge.handle_command(...)`, the field is initialized from
`IntegrationBridge::new_with_lens(...)`, and the type lives in
`integration_bridge.rs` — all of which PHASE 1 removes along with the dep. So the
bridge + its field/construction + the integration FFI fns are one tightly-coupled
unit and were removed together in the **PHASE 1 commit**. The events (more loosely
coupled) went in the **PHASE 2 commit**. Two build-green commits, same total
removal — just regrouped along the actual dependency boundary.

## PHASE 3 — legacy bins: no fix needed

`tests/snapshot_protocol_test.rs` and `tests/network_actor_test.rs` did **not**
reference any removed integration symbol. `snapshot_protocol_test.rs` carries its
own local `IntegrationBindingDTO` and uses the kept `integrations` snapshot field;
`network_actor_test.rs` only mentioned "integration" in a comment. `cargo build
--tests` is green with no changes, so there is no PHASE 3 commit.

## Verification

- `cargo build` ✅ and `cargo build --tests` ✅ (only pre-existing warnings; no new
  ones in the edited files — `lib.rs`, `ffi/core.rs`, `models/events.rs`).
- Substrate suite green after PHASE 1 and PHASE 2, identical to baseline:
  `substrate_chat` 4, `substrate_discovery` 2, `substrate_files` 5 (+1 ignored),
  `substrate_offline` 3, `substrate_resilience` 5, `substrate_snapshot_mp` 1,
  `substrate_sync` 4 (+1 ignored).
- `grep` for every removed symbol across `src/` + `tests/`: none remain.

## Commits

1. `strip: remove integration bridge + lens_bridge + integrations dep + cyan_integration_* FFI`
2. `strip: remove Integration* events + dead buffer/wiring`
