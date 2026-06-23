# Cyan Docker rig — relay / WebSocket / LAN / offline rungs

Real network isolation for the deferred connectivity rungs that the in-process substrate
suite cannot reach (SUBSTRATE_TEST_SPEC §3/§8). Two `cyan_node` peers run as containers on
Docker bridges; an `iroh-relay` fixture provides the relay path. The rungs are Rust tests
in `tests/substrate_relay.rs`, driven over the same stdin/stdout line protocol the
in-process multi-process rig uses, asserting on the **joiner's own `storage::*` counts**.

## Prereqs
- Docker running (`docker info` succeeds).
- The sibling crates `../xaeroai`, `../xaeroID`, `../cyan-backend-integrations` present on
  disk (they are path-deps of cyan-backend; `scripts/assemble-context.sh` copies a minimal
  slice and lowercases `xaeroID → xaeroid` for the case-sensitive Linux FS).

## Layout
- `Dockerfile.relay` — `iroh-relay` pinned to **0.95.1** (matches the engine's `iroh-relay`
  dep), built with `--features server`. `--dev` serves an HTTP relay on **port 3340**.
- `Dockerfile.node` — builds the test-only `cyan_node` bin for Linux from the assembled
  context. (`llama-cpp-2 metal`, a transitive dep, compiles to a no-op CPU backend on Linux
  — it does NOT block the build; verified.)
- `docker-compose.yml` — the `relay` service + isolation networks `lan / mesh_a / mesh_b /
  relaynet / airgap`.
- `scripts/ws-entrypoint.sh` — drops outbound UDP (keeps DNS) to force relay-over-WebSocket.
- `scripts/assemble-context.sh` — builds the minimal `.ctx/` Docker build context.

## Networks (the ladder)
| rung    | peer-a net | peer-b net | relay | direct route a↔b | forces            |
|---------|-----------|-----------|-------|------------------|-------------------|
| LAN     | `lan`     | `lan`     | off   | yes              | direct QUIC       |
| relay   | `mesh_a`  | `mesh_b`  | on    | **no** (blocked) | relay path        |
| ws      | `mesh_a`  | `mesh_b`  | on    | **no** + UDP drop| relay/WebSocket   |
| offline | `airgap`  | `airgap`  | off   | yes, no internet | offline mesh      |

`mesh_a`/`mesh_b` are isolated Docker bridges (inter-bridge traffic is dropped); the relay
is the only node on both, so a successful sync on the relay/ws rungs can only have crossed
the relay.

## Run
```bash
make -C harness up            # build relay image + create networks
make -C harness build-node    # assemble context + build cyan_node image (slow first time)
make -C harness test-lan      # CYAN_RIG=1 cargo test … lan_direct_snapshot_intact --ignored
make -C harness test-relay    # connects_via_relay_when_direct_blocked
make -C harness test-ws       # connects_via_websocket_when_udp_fully_blocked
make -C harness test-offline  # offline_airgap_snapshot_intact
make -C harness test-all      # up + build-node + all four rungs
make -C harness clean         # tear down relay, networks, peers, .ctx
```

A plain `cargo test` (no `--ignored`, no `CYAN_RIG`) never touches Docker: every rung is
`#[ignore]` and also returns early unless `CYAN_RIG=1`.

## Oracle
The relay/ws rungs are proven two ways: (1) **topology** — the split bridges make the relay
the only possible path, so a successful sync is dispositive; (2) **iroh connection-type** —
each container's stderr shows iroh's own `home is now relay …`, read by the test
(`DockerNode::homed_to_relay`). No custom engine byte-meter is added (G11 — see
STATUS_DOCKER_RIG.md).
