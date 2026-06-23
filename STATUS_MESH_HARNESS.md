# STATUS — Mesh-hardening local test rig (MESH_HARDENING_SPEC §6/§7/§10/§12)

**Branch:** `harness/mesh-e2e` (off the backend tip `95fa8db` "backend(mesh): incremental
catch-up + signed group export/import + offline-hold seam"). Never `main`.

**What this is:** the Docker + network-emulation rig for the mesh-hardening batch — the Tier-2
counterpart to the in-process substrate suite. It drives the REAL `cyan_node` engine binary in
containers, forces partition/offline/latency for real, and asserts on the **receiver's own
`storage::*`** convergence (the oracle CLAUDE.md mandates), never on logs.

It builds on the existing relay/WS rig (`harness/`, `STATUS_DOCKER_RIG.md`) — same `cyan_node`
image, same stdin/stdout line protocol, same compose networks — and adds the §6/§7 netem
scenarios, the §10 degradation matrix, and the §12 acceptance smoke. (CLAUDE.md calls this rig
`cyan-local-harness/`; on disk it is `harness/` — same thing, one rig, not two.)

## Scope guardrails honored
- iroh 0.95 only; no version bump. Every wait is a bounded `tokio::time::timeout`/deadline poll —
  no `sleep`-as-synchronization, no unbounded `recv()`.
- Assert on real state (`SyncComplete`, then per-node `storage::*` row counts), never logs.
- Additive only. The engine `src/**` and FFI are untouched **except** the TEST-ONLY `cyan_node`
  bin gained additive control verbs (below) to drive the §2/§5/§11 paths the prior backend tracks
  built. No `unwrap`/`panic!` added on any engine/FFI path.
- Gated/`#[ignore]`-able: a plain `cargo test` runs the new binary as **12 ignored**, touches no
  Docker, stays fast. Every scenario also returns early unless `CYAN_RIG=1`.
- No spec/assertion weakened. Capabilities the substrate genuinely lacks are `#[ignore]` red with
  the precise reason — never faked.

## The rig

### Roles (all are `cyan_node` — the real snapshot+delta engine)
| Role | What it is here | Why |
|------|-----------------|-----|
| **peer** | an ordinary device | — |
| **super-peer** | a `cyan_node` that stays ONLINE and holds the group | the **durable-replica substrate role** from `STATUS_HEADLESS_SUPERPEER.md`. The Lens-specific AI/entitlement/offline-message-hold logic lives in cyan-lens (tested there against fakes) — out of this substrate rig. |
| **bootstrap** | a `cyan_node` reachable from both islands | the thin discovery rendezvous role. True cross-net stranger DISCOVERY needs the xaeroflux bootstrap binary (see red row). |
| **relay** | `iroh-relay` 0.95.1 (`Dockerfile.relay`) | NAT/cross-net transport for the split-bridge rungs. |

All roles are launched by the Rust harness via `docker run` onto the compose networks (the
established pattern — see the `docker-compose.yml` header), so one infra file serves every layout.

### Networks (`harness/docker-compose.yml`)
`lan` (shared bridge — direct route), `mesh_a`/`mesh_b` (isolated islands, only common
reachability is the relay/bootstrap), `relaynet` (relay home), `airgap` (`internal: true`, no
gateway). The §6/§10/§12 mesh scenarios run on `lan` with `RELAY=Disabled` (sovereign), so they
need no relay; the cross-net rungs reuse the split bridges.

### Network emulation / chaos (host-side `docker`, works even while a node is unreachable)
| Impairment | Mechanism (`tests/support/dockernode.rs`) |
|---|---|
| **offline** | `docker pause` / `unpause` — process frozen; DB + state survive the return (IP stable, so no re-wire) |
| **partition** | `docker network disconnect` / `connect` — loses reachability without dying (new IP on heal → re-wire) |
| **latency** | `tc qdisc … netem delay` via `docker exec` (needs `iproute2` + `--cap-add NET_ADMIN`, now in the node image) |
| **NAT / relay-only** | split bridges + relay (the existing `test-relay` / `test-ws` rungs) |

### `cyan_node` additive verbs (test-only bin; drive the §2/§5/§11 engine paths)
`seed_peer` (§2 `SeedGroupPeer`), `catch_up` (§5 `CatchUp`), `count members` (§3 roster),
`bundle_pubkey` / `export_group` / `import_group` (§11 signed, grant-scoped, invitee-encrypted
`.cyangroup` — the bundle travels out-of-band over the harness, like email/AirDrop/USB). See the
bin's header doc-comment for the exact protocol lines.

## Scenario matrix — what passes vs `#[ignore]` red

`make -C harness mesh-e2e` runs the 10 runnable scenarios; the 2 red scaffolds are excluded
(they `unimplemented!()` pending real infra). All 12 are `#[ignore]`+`CYAN_RIG`-gated.

### Runnable — EXECUTED GREEN this session (real containers + netem; 10/10)
| Scenario | Spec | Proves (oracle = receiver storage) |
|---|---|---|
| `lan_mesh_forms_and_live_delta_no_infra` | §6 / §10 | no relay/bootstrap/lens/internet → snapshot syncs **and** a live edit propagates over gossip (the mesh actually FORMS, not just one-shot) |
| `partition_then_both_edit_then_heal_converges_via_delta` | §6 | both edit while network-partitioned, heal, bidirectional §5 catch-up → BOTH converge to the union (10 elements each) |
| `node_offline_then_reconnect_incremental_catchup` | §6 / §5 | a paused peer returns and catches up only the missing range from the closest (LAN) holder |
| `lens_down_mesh_and_sync_continue` | §10 | with NO super-peer container, two peers form + sync + live-delta P2P |
| `bootstrap_down_lan_and_superpeer_still_work` | §10 | no bootstrap; LAN peer + durable super-peer holder mesh & sync |
| `all_infra_down_lan_sovereign_works` | §10 | sovereign LAN: full sync + live chat + roster records the peer, all infra down |
| `acceptance_crud_and_continuous_delta_sync` | §12 | group/workspace/board synced on join + continuous bidirectional delta |
| `acceptance_chat_live` | §12 | chat delivers live (no re-open), no loss/dupes |
| `acceptance_presence_roster_persists_offline` | §12 / §3 | a met peer is recorded; its roster row PERSISTS after it goes offline (cached/greyable) |
| `acceptance_airgapped_import_baseline` | §12 / §11 | host exports a signed/scoped/encrypted bundle; an air-gapped importer (never joins/syncs) imports it and holds the full baseline |

### `#[ignore]` red — blocked on real infra (honest, not faked)
| Scenario | Spec | Why red |
|---|---|---|
| `bootstrap_seeded_cross_net_mesh` | §6 / §10 | introducing two strangers across isolated networks with NO pre-shared addrs is the **xaeroflux bootstrap's gossip-discovery rendezvous** role; `cyan_node` can't relay third-party addrs and xaeroflux is untouched by this batch (no bootstrap image built). The cross-net **transport** is already green via `substrate_relay::connects_via_relay_when_direct_blocked` — only the auto-DISCOVERY is unproven in-rig. |
| `offline_peer_message_held_by_superpeer_delivered_on_return` | §10 | hold-for-offline-peer + redeliver-on-return is the **Lens super-peer's** `hold_message`/`deliver_on_reconnect` logic — it lives in cyan-lens (`src/superpeer.rs`), tested there against fakes, and is **not a runnable real binary**. `cyan_node`'s engine has only the §4 content-addressed `mesh_hold` SEAM (persist outgoing broadcasts), not a per-peer redeliver wire protocol. Needs the headless-cyan Lens binary wired to a real `MeshHolder` over iroh (STATUS_HEADLESS_SUPERPEER.md "Tier-2"). |

### §10 rows — coverage map
- **Lens offline** → `lens_down_mesh_and_sync_continue` ✅
- **Bootstrap offline (Lens up)** → `bootstrap_down_lan_and_superpeer_still_work` ✅
- **Lens + bootstrap both offline (air-gapped)** → `all_infra_down_lan_sovereign_works` ✅
- **A peer offline → catch-up on return** → `node_offline_then_reconnect_incremental_catchup` ✅
- **Offline peer's messages held & delivered** → red (Lens super-peer logic; see above)
- **Relay offline / NAT** → existing `test-relay` / `test-ws` rungs (LAN unaffected; NAT'd peers use relay)

## Oracle split (honest)
- **Convergence** (does the receiver end up holding the data) is asserted per-node here — the
  strongest proof, and exactly what CLAUDE.md mandates. The multi-process rig gives every
  `cyan_node` its own SQLite DB, so a count on the receiver really proves receipt.
- **Incremental-ness** of catch-up (a delta, NOT a full re-snapshot) is proven in-process by
  `tests/substrate_catchup.rs` against the holder's served-snapshot metrics. The Docker rung
  drives the same `download_snapshot_since` path end-to-end and proves the data arrives. (The
  process-global metrics oracle can't separate two containers; per-node storage convergence can.)
- A note on catch-up watermarks: the scenarios pass `since: None` (engine uses the requester's
  high-water mark), the realistic path. Edits authored after a real offline/partition window carry
  later wall-clock timestamps, so they sort newer and flow on catch-up.

## How to run
```bash
make -C harness up            # build relay 0.95.1 image + create the isolation networks
make -C harness build-node    # assemble context + build cyan/node:rig (slow first time)
make -C harness mesh-e2e      # up + build-node + all 10 runnable scenarios (CYAN_RIG=1)
make -C harness mesh-one T=all_infra_down_lan_sovereign_works   # one scenario
make -C harness mesh-red      # print the 2 honest-red scenarios + why
make -C harness clean         # tear down relay/networks/peers/.ctx
```
A plain `cargo test` never touches Docker: `tests/substrate_mesh_e2e.rs` reports **12 ignored**.
For post-mortem container logs set `CYAN_RIG_LOG_DIR=<dir>` (peer stderr → `cyan-rig-<name>.log`).

## Verification done in this session
- `cargo build --tests` — clean, 0 errors (the verify gate). New verbs + driver + scenario file
  all compile; clippy clean on the touched files; warnings elsewhere are pre-existing engine lints.
- `cargo test --test substrate_mesh_e2e` (Docker-free) — **12 ignored, 0 run**, default stays fast.
- **Node image built** (`make -C harness build-node` → `cyan/node:rig`) — the `llama-cpp-2 metal`
  transitive dep compiled to a CPU no-op on Linux as expected; the cyan_node release build succeeded.
- **All 10 runnable scenarios EXECUTED GREEN** against real `cyan_node` containers on the
  `cyan-rig_lan` bridge (verified containers actually ran via a concurrent `docker ps` catching
  `cyan-rig-peer-a`, and that none hit the `CYAN_RIG!=1` skip path):

  | Scenario | Result |
  |---|---|
  | `lan_mesh_forms_and_live_delta_no_infra` | ✅ pass |
  | `partition_then_both_edit_then_heal_converges_via_delta` | ✅ pass (bidirectional catch-up converges 10/10) |
  | `node_offline_then_reconnect_incremental_catchup` | ✅ pass |
  | `lens_down_mesh_and_sync_continue` | ✅ pass |
  | `bootstrap_down_lan_and_superpeer_still_work` | ✅ pass |
  | `all_infra_down_lan_sovereign_works` | ✅ pass |
  | `acceptance_crud_and_continuous_delta_sync` | ✅ pass |
  | `acceptance_chat_live` | ✅ pass |
  | `acceptance_presence_roster_persists_offline` | ✅ pass |
  | `acceptance_airgapped_import_baseline` | ✅ pass (signed/scoped/encrypted bundle, offline import) |

  Total: **10/10 runnable green, 2 honest-red** (`#[ignore]`, documented above).
- **netem latency knob verified** independently: `docker exec … tc qdisc add dev eth0 root netem
  delay 100ms` applies + clears cleanly in the image (`iproute2` present, `NET_ADMIN` granted).
- The full `make -C harness mesh-e2e` (which also `up`s the relay) was not run end-to-end only
  because the LAN scenarios need just the `cyan-rig_lan` bridge, not the relay — so they were run
  directly with `CYAN_RIG=1` against that network (created via `docker network create cyan-rig_lan`,
  which `make up` does for you). The relay is only needed by the (separate) `test-relay`/`test-ws`
  rungs and the red cross-net row.

## Files
- `tests/substrate_mesh_e2e.rs` — the 12 scenarios (gated).
- `tests/support/dockernode.rs` — `DockerNode` + chaos (`pause`/`partition`/`set_latency`) + new
  verb wrappers (`seed_peer`/`catch_up`/`count_members`/`export_group`/`import_group`/`post_*`).
- `src/bin/cyan_node.rs` — additive control verbs (§2/§5/§11).
- `harness/docker-compose.yml` — header documents the mesh-e2e roles/launch model (networks
  unchanged — they already cover the scenarios).
- `harness/Dockerfile.node` — `iproute2` + `shape.sh` added for the netem latency knob.
- `harness/scripts/assemble-context.sh` — copies `shape.sh` into the build context.
- `harness/Makefile` — `mesh-e2e` / `mesh-one` / `mesh-red` targets.

## Stop.
Rig + netem scenarios built and build-verified; 10 runnable, 2 honest-red with reasons; gated so
`cargo test` stays infra-free. Run with `make -C harness mesh-e2e`.
