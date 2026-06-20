# STATUS — Round 8: toggle-friendly multi-account LIVE-TEST harness

The friendly front door to the live rig: **one command, human toggles**, so the founder can spin
up an N-account live test in seconds. Built ON the existing rig — it does not duplicate `stress.sh`
(the chaos/scale driver) or the Docker Makefile rungs; it routes to them.

## Deliverables

| file | role |
|------|------|
| `harness/live.sh`            | the front door: parses toggles, routes to the right tier, renders the per-peer table, exits on the verdict. |
| `harness/live.md`            | the three-command quick-start. |
| `tests/substrate_live.rs`    | the honest engine: N real `cyan_node` processes, one shared group, per-peer storage assertions, bounded waits. Gated behind `CYAN_LIVE=1`. |
| `src/bin/cyan_node.rs`       | +2 test-only verbs — `post_chat` (live `ChatSent`) and `post_workflow` (board+steps+pin broadcast). |
| `tests/support/multiprocess.rs` | +`MpNode::post_chat` / `post_workflow` drivers. |
| `harness/scripts/assemble-context.sh`, `harness/Dockerfile.node` | rig-rot fix so the Docker tier builds again (see *Finding* below). |

## Identity — NO login required

Headless `cyan_node` peers **self-identify** from an auto-generated per-peer keypair seeded from
their own data dir — **no SSO, no login, no real accounts**. That is why `--peers` can be large: the
founder is *not* limited by how many logins he has. (The real-login app UX pass is a separate manual
run via the app path — `--app N` only prints a pointer to it; it is intentionally out of scope of the
asserted headless run.)

## Toggles (`./live.sh --help`)

| flag         | default | meaning |
|--------------|---------|---------|
| `--peers N`  | `8`     | N headless peers, each its own data dir + auto identity. |
| `--mode`     | `macos` | `macos` = N native `cyan_node` procs on this Mac; `docker` = the isolation-network rig. |
| `--net`      | `home`  | `home` direct + hole-punch · `corp` relay/WebSocket fallback · `offline` mDNS-LAN only. |
| `--scenario` | `all`   | `sync` · `files` · `chat` · `workflow` · `all`. |
| `--keep`     | off     | leave the rig up for manual poking (Docker tier — see below). |
| `--app N`    | off     | also launch up to N real app instances for a **manual** UX pass (separate path). |

## What each scenario ASSERTS (on each peer's OWN `storage::*`, bounded `timeout`)

Every peer acts, then **all** peers must converge to the **exact** expected count — convergence to an
exact total is the no-dupes / no-loss oracle (id-keyed storage makes a silent over-count impossible;
an under-count is loss). Base = the host fixture every joiner pulls on join (5 elements, 3 chats,
3 cells, 1 file).

- **sync** — each peer creates `EDITS_PER_PEER` whiteboard objects (live `WhiteboardElementAdded`
  broadcast). Assert every peer's `elements` == `5 + N*EDITS_PER_PEER`.
- **chat** — each peer sends `CHATS_PER_PEER` board-chat messages (live `ChatSent` broadcast; the
  receiver persists via the same `ChatSent` apply arm). Assert every peer's `chats` == `3 + N*CHATS_PER_PEER`.
- **files** — each peer uploads one blob (hold + announce). Then **every** peer fetches **every other**
  peer's blob and **independently re-verifies its blake3**. Per-peer PASS = fetched-and-verified all
  N−1 others. This is "all peers receive + can read the files" proven on the receiver, not on a log.
- **workflow** — one peer (the host) authors + lays out a **local-placement workflow**: a workflow
  board + `WF_STEPS` step cells + a **pinned gate**, broadcast as `BoardCreated` + `NotebookCellAdded`
  + `PinSet`. Assert every peer converges on the steps (`cells` == `3 + WF_STEPS`) AND the gate
  (`pins` == 1). *Execution / wave-placement is local/MCP and out of substrate scope (CLAUDE.md) —
  what the MESH carries is the authoring (board + steps + pin), which is exactly what is asserted.*

All four exercise the **real** engine gossip + persist paths (the same `NetworkEvent` apply arms the
app uses); nothing is faked — every scenario converges over **live gossip** (broadcasts to all joined
group members), exactly like the proven `substrate_stress::concurrent_edits_converge_no_dupes`.

### Single-box scale note (why N is a real knob)

Each headless peer is a **full iroh node** (its own QUIC endpoint + gossip + SQLite) on **one Mac**, so
forming the group is genuinely heavy: the host serves a snapshot to every joiner. ~4–6 native peers
converge in a few seconds; **8 is reliable but takes ~15–30 s to form**; much larger N stresses the
single-host snapshot fan-in (documented in `STATUS_STRESS_FABRIC.md`). Group formation runs with **no
anti-entropy sweep** — an early version ran a fast sweep across the fresh full mesh and the storm
starved the *first* joiner's snapshot (`peer1 did not sync within bound`); since every scenario
converges over live broadcasts, the sweep was pure formation-time load and was removed. If a joiner
still can't pull the snapshot in the bound, the harness prints a per-peer `join` FAIL row and a FAIL
verdict (a legible table, never a panic with an empty one) — lower `--peers` for a quick demo.

## macos vs docker — why two tiers

- **macos (default).** N **native** `cyan_node` processes on this Mac, each with its OWN SQLite DB →
  per-node storage assertions are honest (a row can only be in a peer's DB because it arrived over the
  mesh). Relay is disabled; peers full-mesh over loopback. This is the **N-peer scale** tier and
  proves the **offline/LAN rung** directly (relay disabled = the offline property). `home` and
  `offline` both run here.
- **docker.** The isolation-network rig (`docker-compose.yml` + `Dockerfile.node` + `ws-entrypoint.sh`).
  This is where **real per-process UDP blocking** is possible — the only honest way to force the
  relay / WebSocket fallback. These are 2-peer **topology** proofs (the split bridges make the relay
  the only path); the N-peer scale proof stays on the macos tier. `corp` routes here.

## corp / offline mechanism

- **`--net offline`** (macos tier): relay **disabled**, no bootstrap-to-internet, mDNS/LAN discovery
  only. The harness additionally asserts the topology never used a relay tier (`tier != relay/mixed`
  for host→every joiner) and prints `offline_proof=relay-disabled,direct-only`. This is the offline
  rung proven with N peers.
- **`--net corp`** (Docker tier, regardless of `--mode`): simulates a corporate firewall by **dropping
  outbound UDP** so neither direct QUIC nor QUIC-to-relay works — `iroh-relay` must carry traffic over
  its HTTP/**WebSocket** (TCP) transport (`ws-entrypoint.sh`). `live.sh` brings up the rig and runs the
  `test-relay` (forced relay) + `test-ws` (UDP fully dropped → WebSocket) rungs; both assert the
  joiner's own snapshot synced intact across split bridges where the relay is the only route. macOS
  `pf`-based blocking is best-effort and intentionally **not** wired — Docker is the rigorous path.

## `--keep`

Leaves the Docker relay + networks up for manual poking (tear down with `make -C harness clean`). On
the **macos** tier peers are the test process's children (ephemeral), so `--keep` is a no-op there and
says so — keeping N native peers alive past the asserted run is the Docker tier's job.

## Finding (fixed): Docker rig had rotted

The engine grew two sibling **path-deps** since the Docker rig was last green —
`../cyan-mcp` and `../cyan-identity` (`cyan-backend/Cargo.toml`) — but `assemble-context.sh` /
`Dockerfile.node` didn't copy them, so `docker build` failed with `failed to read ../cyan-identity`.
Fixed by assembling + `COPY`ing both crates (they exist on disk, lowercase, with no further
path-deps). The `cyan/node:rig` image now builds (verified, 156 MB), so the corp/docker tier is
functional again.

## Sample run

```text
$ ./harness/live.sh --peers 8 --net home --scenario all
SAMPLE_8PEER_PLACEHOLDER
```

A `sync`-only, `N=3` smoke is the fastest sanity check:

```text
$ ./harness/live.sh --peers 3 --net offline --scenario sync
[live] peers=3 mode=macos net=offline scenario=sync keep=0 app=0
[live] booting 3 headless peers (auto identity, no login) → group → scenario 'sync'…

  SCENARIO   PEER     RESULT DETAIL
  --------   ----     ------ ------
  sync       host     PASS   elements=20/20
  sync       peer1    PASS   elements=20/20
  sync       peer2    PASS   elements=20/20

[PASS] all 3 peers converged on every scenario  (net=offline, mode=macos)
```

## Discipline kept

- Test-first where it asserts; **bounded `tokio::time::timeout`** on every wait; never `sleep`-as-sync.
- Assertions on each peer's own `storage::*` counts / own blake3 re-verify — **never on log lines.**
- iroh 0.95 only; additive, test-only changes (new `cyan_node` verbs + a gated test + a friendly shell
  front door). The FFI surface and shipping behavior are untouched. No `unwrap`/`panic!` in the new
  paths (`?`/`map_err` throughout).
- `cargo test` with no `CYAN_LIVE` never spawns peers (the test returns immediately) — the default
  matrix stays light.
