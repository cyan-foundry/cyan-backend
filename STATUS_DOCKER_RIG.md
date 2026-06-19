# STATUS — Docker rig (relay / WebSocket rungs for real)

Branch `feat/docker-rig`. Additive only: new `harness/**` + relay test wiring
(`tests/substrate_relay.rs`, `tests/support/dockernode.rs`). **No engine `src/**` edits.**
Docker was running (`docker info` OK; Docker 29.1.3, Compose v2.40.3-desktop). The default
`cargo test` stays green and Docker-free.

## Rung results

| Rung (test) | Network layout | Result | Oracle |
|---|---|---|---|
| **LAN / direct** — `lan_direct_snapshot_intact` | both peers on `lan`, `RELAY=Disabled` | ✅ **green** | snapshot syncs over direct QUIC; never homes to a relay |
| **Relay-only** — `connects_via_relay_when_direct_blocked` | peer-a `mesh_a`, peer-b `mesh_b` (no a↔b route), relay on both | ✅ **green** | topology (relay is the only common path) + iroh `home is now relay` |
| **WebSocket-only** — `connects_via_websocket_when_udp_fully_blocked` | split bridges **+ outbound UDP black-holed** | ✅ **green** | UDP dead → relay can only use HTTP/WebSocket (TCP); homes to relay; sync intact |
| Offline (air-gapped) — `offline_airgap_snapshot_intact` | both on `internal` `airgap` | ⛔ **documented-red** | blocked by engine seam (below) |
| G8-R 100 MB over relay/WS — `large_file_100mb_over_relay_intact`, `large_file_over_websocket_relay_intact` | (relay/WS path) | ⛔ **documented-red** | needs a `cyan_node` blob verb (below) |
| G8-R throughput floor — `relay_path_meets_relay_throughput_floor` | (relay path) | ⛔ **documented-red** | needs a blob verb to push a measurable payload |
| G11 metering — `relayed_bytes_are_metered` | (relay vs direct) | ⛔ **documented-red** | needs an additive engine byte counter (below) |

Each green rung asserts on the **joiner's own `storage::*` row counts** (1 workspace, 1 board,
5 elements, 3 cells, 3 chats, 1 file-meta) — the snapshot transferred intact across the
forced network path. This is the storage oracle CLAUDE.md mandates, driven over the same
stdin/stdout line protocol the in-process multi-process rig uses.

## The oracle (no engine meter)

Per the brief, the relay path is proven by **iroh's own connection type**, not a custom
byte meter:

1. **Topology (deterministic).** On the relay/WS rungs the peers sit on separate Docker
   bridges `mesh_a` / `mesh_b`. Inter-bridge traffic is dropped by Docker (verified directly:
   a peer on `mesh_a` cannot ping a peer on `mesh_b`, but both reach the relay by name). The
   relay is the only node bridging both, so a successful sync can **only** have crossed it.
2. **iroh connection-type (corroborating).** Each peer container's stderr carries iroh's own
   tracing; the rung reads `home is now relay http://cyan-rig-relay…` from it
   (`DockerNode::homed_to_relay`). On the WS rung this happens with all outbound UDP dropped,
   so the relay is being reached over its TCP/WebSocket transport.

`Endpoint::remote_info()` would give the per-peer connection type directly, but exposing it
needs a new `cyan_node` verb = an engine `src/**` edit, which this additive run does not make.
Topology + the home-relay log are sufficient and honest.

## Docker / version pinning

- **iroh-relay 0.95.1** — pinned to the engine's `iroh-relay` dep so the relay protocol
  matches the peers' iroh 0.95 client. Built from crates.io with `--features server` (the
  `iroh-relay` binary is gated behind it). `--dev` runs a plain-HTTP relay on **port 3340**.
- **cyan_node image** — built for Linux from an *assembled minimal context*
  (`scripts/assemble-context.sh`): the repo plus the three sibling path-deps `xaeroai`,
  `xaeroID` (lowercased to **`xaeroid`** so the case-sensitive Linux FS resolves `../xaeroid`),
  and `cyan-backend-integrations`. Only `Cargo.toml`/`Cargo.lock`/`src`/`tests` are copied
  (the sibling dirs are 25 G+ on disk).
- **`llama-cpp-2 metal` is NOT a blocker.** `xaeroai` depends on
  `llama-cpp-2 = { …, features = ["metal"] }` unconditionally; this was the suspected
  Linux-build killer. An isolated probe proved `llama-cpp-sys-2` gates the Metal backend to
  Apple targets — on Linux the `metal` feature compiles to a no-op CPU backend, and the full
  `cyan_node` release build succeeds.

## Findings encoded along the way

1. **Discovery actor blocks the command loop until the bootstrap neighbor connects.** The
   joiner's `DiscoveryActor` calls `gossip.subscribe_and_join(discovery_topic, [host]).await`
   at startup; with a non-empty bootstrap that `.await` does not return until the neighbor is
   reached, so the actor's command loop never starts and `JoinGroup` is never processed.
   Across split bridges the only path is the relay, so the **host's relay addr must be in the
   joiner's `StaticProvider` before the bootstrap dial retries**. The rig works because the
   `cyan_node` control loop (`node_id`/`addr`/`add_peer`) runs independently of the blocked
   command loop, letting the rig inject the addr early. On loopback/LAN this is invisible —
   mDNS bridges the gap — which is why the in-process rig never hit it.
2. **WebSocket rung & Docker DNS.** Docker's embedded resolver `127.0.0.11` NATs the `:53`
   query to a high port, so a bare `--dport 53 ACCEPT` rule misses it and breaks name
   resolution → iroh's relay HTTPS probe can't resolve the relay. `ws-entrypoint.sh` allows
   loopback + `127.0.0.11` before dropping other outbound UDP.
3. **Offline (internal network) is blocked by unconditional mDNS init.** `NetworkActor::new`
   adds `MdnsDiscovery::builder()` unconditionally; on a gateway-less `internal` Docker bridge
   the mDNS service fails to create and `Endpoint::bind()` returns `Service 'mdns' error`, so
   the peer exits before its control loop starts. The relayless/offline substrate property is
   already proven green by the LAN rung (`RELAY=Disabled`) and in-process by
   `tests/substrate_offline.rs`; only the *zero-internet* variant is blocked.

## Follow-ups (separate engine tasks — NOT done here)

- **G11 relayed-byte meter** (`relayed_bytes_are_metered`): add a per-(tenant, transfer)
  relayed-byte counter in the engine (additive `src/**` seam). The rig already proves *which
  path* was used via iroh's connection type; what's missing is the billing **count**.
- **Large-blob transfer verb** (`large_file_100mb_over_relay_intact`,
  `…_over_websocket_relay_intact`, `relay_path_meets_relay_throughput_floor`): `cyan_node`
  has no upload/fetch/blake3 verb — the fixture snapshot moves metadata only (the 1 file row
  is `file_insert_simple` metadata, not a payload). A blob-transfer verb (engine `src/bin`
  edit) is needed to push a 100 MB payload and assert blake3 integrity + throughput over the
  relay/WS rungs. The relay/WS **paths** themselves are already green.
- **Optional mDNS** (`offline_airgap_snapshot_intact`): make `MdnsDiscovery` init tolerant of
  its own absence so a peer can boot on a gateway-less network (truly air-gapped).

## Default `cargo test` (Docker-free)

The rig adds nothing to the default run: every rung in `tests/substrate_relay.rs` is
`#[ignore]` and `CYAN_RIG=1`-gated, so `cargo test` never invokes Docker and the relay test
binary reports 8 ignored. This diff touches **no `src/**` or `Cargo.toml`** (verified:
`git diff --stat <base> HEAD -- src/ Cargo.toml` is empty), so it cannot change engine test
behavior. The substrate suite and the rest of the integration tests pass as before.

One pre-existing lib unit-test failure is unrelated to this work and out of scope:
`diagram_gen::tests::test_parse_diagram_json` (`src/diagram_gen.rs:641`,
`assert!(result.svg.is_some())`). It is part of the AI/Lens diagram surface that CLAUDE.md
explicitly puts out of scope for substrate work, and depends on AI resources not present in
this environment. Because the lib unit-test binary runs first and `cargo test` fail-fasts
across targets, this one failure makes a bare `cargo test` exit non-zero *before* the
integration suite runs — but it is not introduced or affected by the rig (this diff has zero
`src/`/`Cargo.toml` changes). Verified green here, skipping only that pre-existing failure:

```bash
cargo test --lib -- --skip diagram_gen::tests::test_parse_diagram_json   # 23 ok
cargo test --test substrate_snapshot_mp --test substrate_sync \
           --test substrate_discovery   # in-process substrate suite: ok, Docker-free
cargo test --test substrate_relay        # 8 ignored (rig rungs), Docker-free
```

## How to run

```bash
make -C harness up            # build relay 0.95.1 image + create lan/mesh_a/mesh_b/relaynet
make -C harness build-node    # assemble context + build cyan/node:rig (slow first time)
make -C harness test-lan      # green
make -C harness test-relay    # green
make -C harness test-ws       # green
make -C harness test-all      # up + build-node + the three green rungs
make -C harness clean         # tear down relay/networks/peers/.ctx
```

Rungs are `#[ignore]` and `CYAN_RIG=1`-gated; a plain `cargo test` never touches Docker.
For post-mortem peer logs, set `CYAN_RIG_LOG_DIR=<dir>` (the rig writes
`cyan-rig-<peer>.log` there).
