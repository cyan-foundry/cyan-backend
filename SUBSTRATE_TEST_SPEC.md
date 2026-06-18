# SUBSTRATE_TEST_SPEC — Cyan P2P substrate, end-to-end

Standing spec for the agents that build the **substrate test suite**: proof that
the Cyan P2P layer (cyan-iOS ↔ cyan-backend ↔ xaeroflux bootstrap, and — later —
cyan-lens) is **production-grade**. Read this fully before writing tests. It is
written the same way `cyan-forge/SPEC.md` is: a red backlog of *named tests* an
agent implements to green, behind a harness built once.

iroh is **0.95** everywhere. Do not bump it. Do not touch iroh-blobs' version.

---

## 0. What we are proving (the substrate guarantees)

The product promise is: **fast QUIC sharing of large files, plus chat and file
share, working end-to-end on a LAN with no internet.** Everything below exists to
make that promise testable and regression-proof. Each guarantee is a row; the
test cases in §3 reference these IDs.

| ID  | Guarantee | Why it's the substrate |
|-----|-----------|------------------------|
| G1  | **Discovery** — two nodes sharing a discovery key find each other, via the bootstrap peer *and* via mDNS on a LAN. | nothing syncs until peers meet |
| G2  | **Connectivity ladder** — the connection always completes by descending: direct QUIC → relay (QUIC hole-punch *via* relay) → full relay proxy → **WebSocket-only relay** (UDP fully blocked, 443 only). Each rung is tested. | works on *any* network |
| G2-LAN | **Relay-disabled on LAN** — same-WiFi peers connect with no relay at all. | the "no internet" claim |
| G3  | **Snapshot sync** — a late joiner receives the full current state of a group it joins (workspaces, boards, notebook cells, chat history, file *metadata*). | onboarding a peer |
| G4  | **Delta sync** — a change made *after* join propagates live: board element, notebook cell, workspace/group structure, chat. | live collaboration |
| G5  | **Chat at every level** — group, workspace, and board messages reach all peers in scope. | "chats work on all levels" |
| G6  | **File share P2P** — a file uploaded on A is fetchable on B at each scope (group/workspace/board), blake3-verified. | "file share works on all levels" |
| G7  | **File-via-chat** — a chat carrying an attachment shares the file into the workspace/group the message is posted to. | "file transfer via chat" |
| G8  | **Large-file QUIC** — a large blob (100 MB, then 1 GB) transfers intact and above a throughput floor. | "fast QUIC sharing of large files" |
| G8-R | **Large-file over relay** — same blob completes intact over the **relay** path *and* over the **WebSocket-only** rung, above a (lower) relay throughput floor. | the paid path must actually work |
| G11 | **Relay bytes are metered** — every byte that traverses the relay is counted per (tenant, transfer), exposed for billing; direct-path bytes are *not* charged. | "ask for more money for relay large files" |
| G9  | **Offline / LAN** — G1–G8 all hold with **no internet** (relay disabled, mDNS only). | the headline use case |
| G10 | **Resilience / swarming** *(next phase)* — resume a partial transfer; fetch from multiple sources; survive peer churn mid-transfer. | "robust, production-grade substrate" |

**Out of scope (explicitly).** The Iggy enrichment pipeline, integration events,
and nudges/asks/decisions extraction. Those are moving to the MCP/workflow model
and must not be tested here. Lens is in scope only for **optionality** (§4).

### Transport map — test the right boundary

There are **three** transports, and only one is the substrate. Don't conflate them.

| Boundary | Transport | What it means for tests |
|----------|-----------|-------------------------|
| **iOS ↔ backend** | **FFI** — same process, one device | The backend is a linked Rust lib (`ffi/core.rs`, 216 `extern "C"` `cyan_*` fns exchanging JSON strings). There is **no inbound HTTP server** in cyan-backend. This boundary is **not a network test** — it's a C-ABI contract (one FFI smoke test in §1; everything else drives the engine directly). |
| **backend ↔ backend** (device ↔ device) | **iroh QUIC + gossip** | The P2P substrate. The entire G1–G9 suite lives here. |
| **backend → Lens** | **HTTP** (reqwest client → Lens REST) | One-way; backend is the client. Mostly out of scope — see §4. |

---

## 1. Test strategy — where the tests live and how a node is spun

The honest testable layer is **cyan-backend**: the iOS app is a thin FFI shell
over this engine, so backend-to-backend *is* the substrate. We test by spinning
**N in-process nodes** in one test binary and asserting they converge.

### The node-spin pattern (already proven in-repo)

`tests/network_actor_test.rs` and `tests/delta_sync_test.rs` show it. Per node:

```
temp DB (tempfile::tempdir) → storage::init_db
ephemeral identity         → SecretKey::generate(ChaCha8Rng)
globals BEFORE actor       → RELAY_URL / DISCOVERY_KEY / BOOTSTRAP_NODE_ID
channels                   → (event_tx, event_rx), (cmd_tx, cmd_rx), peers_per_group
NetworkActor::new(secret_key, event_tx, peers_per_group).await
tokio::spawn(actor.start(cmd_rx))
drive:  cmd_tx.send(NetworkCommand::…)
observe: event_rx.recv()  +  storage::* queries
```

- **Drive/observe via internal Rust APIs** (command channel + `storage::*`), not
  the FFI. Cleaner, faster, deterministic. Keep **one** FFI smoke test
  (`cyan_send_command`/`cyan_poll_events`) so the boundary doesn't rot.
- **Assertion oracle** = the receiver's SQLite. After a `SwiftEvent::SyncComplete`
  (or a delta event), query `storage::element_list_by_boards`,
  `chat_list_by_workspaces`, file tables, etc., and assert counts/contents.
- **Determinism**: every wait is a bounded `tokio::time::timeout` (generous, e.g.
  5–15 s) returning a clear failure, never an unbounded `recv()`. No `sleep`-and-pray.

### The one thing to build first — `MeshHarness`

A single test-support module is the foundation for the entire suite. **No test
case is written until this exists and is reviewed.**

`tests/support/mod.rs` (cyan-backend):

```rust
pub struct NodeCfg { relay: RelayPolicy, discovery: DiscoveryPolicy }
pub enum RelayPolicy {
    Disabled,              // LAN/offline — no relay at all (G2-LAN, G9)
    Url(String),           // relay available; direct still preferred (normal)
    RelayOnly(String),     // force the relay path: direct blocked — the PAID path (G8-R)
    WebsocketOnly(String), // UDP fully blocked → relay over WebSocket/443 (worst rung)
}
pub enum DiscoveryPolicy { MdnsOnly, Bootstrap(PublicKey) }

pub struct Node { /* node_id, cmd_tx, event_rx(shared), db_path, .. */ }

pub async fn spawn_node(name: &str, cfg: NodeCfg) -> Node;
pub async fn spawn_mesh(n: usize, cfg: NodeCfg) -> Vec<Node>;   // all share key
impl Node {
    pub async fn cmd(&self, c: NetworkCommand);
    pub async fn wait_for<F>(&self, pred: F, t: Duration) -> Result<SwiftEvent>;
    pub async fn wait_sync(&self, group: &str, t: Duration) -> Result<()>;
    pub fn db(&self) -> &Path;                                   // for storage:: asserts
}
```

`RelayPolicy::Disabled` maps to iroh 0.95 `RelayMode::Disabled`;
`DiscoveryPolicy::MdnsOnly` keeps `MdnsDiscovery` and drops n0/DNS. This is the
switch that makes G9 ("no internet") a one-line config change on every test.

**Forcing the relay rungs is harder than the others — be honest about it.**
`Disabled` / `Url` are pure in-process. `RelayOnly` and `WebsocketOnly` are not:
to *prove* traffic took the relay (and the WebSocket-only relay), the test must
(a) run a **local relay fixture** — our own `iroh-relay` server bound on loopback,
not a public relay — and (b) **black-hole direct UDP** between the two peers so
iroh is forced down the ladder; `WebsocketOnly` additionally blocks the relay's
UDP so the relay itself must carry packets over its HTTP/WebSocket transport.
On a single host that needs Linux **network namespaces** (or the docker-compose
two-node rig), not a plain in-process spawn. So: the relay harness grows a
`RelayFixture` (spawn local relay, hand peers its URL) and a `net_isolate` helper;
the `WebsocketOnly` tests may start `#[ignore]` and run only in the netns/CI rig
until that plumbing exists. Don't fake the rung by *configuring* relay-only and
asserting nothing — assert the path actually carried the bytes (G11 meter is the
oracle: relayed-byte count > 0 means it really used the relay).

---

## 2. Files (the suite layout)

Each file = one agent's lane. One agent owns one file + its tests.

| File | Repo | Guarantees | Notes |
|------|------|-----------|-------|
| `tests/support/mod.rs` | cyan-backend | — | **the harness; build & review first** |
| `tests/substrate_discovery.rs` | cyan-backend | G1, G2 | meet via bootstrap and via mDNS; relay-off connect |
| `tests/substrate_sync.rs` | cyan-backend | G3, G4 | snapshot completeness + live deltas (board, notebook, structure) |
| `tests/substrate_chat.rs` | cyan-backend | G5, G7 | group/workspace/board chat; attachment-in-chat |
| `tests/substrate_files.rs` | cyan-backend | G6, G8 | P2P share per scope; large-file QUIC + throughput floor |
| `tests/substrate_relay.rs` | cyan-backend | G2, G8-R, G11 | the **paid path**: relay-only + WebSocket-only large transfer, metering; needs `RelayFixture` + net isolation |
| `tests/substrate_offline.rs` | cyan-backend | G9 | the headline: the matrix re-run with `RelayPolicy::Disabled` |
| `tests/substrate_swarm.rs` | cyan-backend | G10 | **red/#[ignore]** — drives the swarming build |
| `tests/substrate_mesh.rs` | xaeroflux | G1, G3 primitives | bootstrap mesh, event propagation, peer intro/departure, snapshot serve |
| `tests/substrate_lens.rs` | cyan-backend or cyan-lens | §4 | **red/#[ignore]** — Lens-as-super-peer contract |

---

## 3. The red backlog (named tests)

Names are the contract — an agent implements until these pass. Mirror the
cyan-forge discipline: **never weaken an assertion; if a test looks wrong, stop
and ask.**

### `substrate_discovery.rs` — G1, G2
- `two_nodes_meet_via_bootstrap` — both `Bootstrap(pk)`; within timeout each sees the other in `peers_per_group`.
- `two_nodes_meet_via_mdns_on_lan` — both `MdnsOnly`, `RelayPolicy::Disabled`; they still find each other.
- `direct_quic_preferred_on_lan` — connection established with relay disabled (proves the path is direct).
- `relay_fallback_when_direct_blocked` *(may start #[ignore])* — simulate no direct path; assert relay still connects. Document the simulation method chosen.

### `substrate_sync.rs` — G3, G4
- `late_joiner_gets_full_snapshot` — host seeds group+workspace+board+5 elements+3 chats+1 file-meta; joiner after `SyncComplete` has all of them (count asserts via `storage::*`).
- `delta_board_element_propagates` — post-join element add on A appears on B.
- `delta_notebook_cell_propagates` — notebook cell create/edit on A appears on B. *(Confirm the notebook command/event names with the agent survey before asserting.)*
- `delta_workspace_structure_propagates` — new workspace/board on A appears on B.
- `three_node_convergence` — A, B, C; a change on A reaches both B and C.

### `substrate_chat.rs` — G5, G7
- `group_chat_reaches_all_peers`
- `workspace_chat_reaches_all_peers`
- `board_chat_reaches_all_peers`
- `chat_with_attachment_shares_file_into_scope` — DM/chat carrying `DmAttachment`; receiver ends with both the message *and* the file fetched into that workspace/group scope.

### `substrate_files.rs` — G6, G8
- `file_shared_at_group_scope` / `…_workspace_scope` / `…_board_scope` — upload on A, `RequestFileDownload` on B, blake3 matches, `local_path` set.
- `large_file_100mb_transfers_intact` — generate 100 MB, transfer, assert hash + byte length.
- `large_file_1gb_transfers_intact` *(may be #[ignore] for CI cost; runnable on demand)*.
- `large_file_meets_throughput_floor` — assert MB/s above an agreed floor on loopback (set the number with the agent after a first measurement; this is the "fast QUIC" guard).

### `substrate_relay.rs` — G2 ladder, G8-R, G11 (the paid path)
Needs `RelayFixture` (local `iroh-relay`) + net isolation (see §1). Some may be
`#[ignore]` until the netns/CI rig exists — but spec'd now so the path is built right.
- `connects_via_relay_when_direct_blocked` — UDP between peers black-holed; connection still forms through the local relay; assert the connection type is relayed (not direct).
- `connects_via_websocket_when_udp_fully_blocked` — relay's UDP also blocked; assert the relay carries traffic over its HTTP/WebSocket transport and the peers still talk.
- `large_file_100mb_over_relay_intact` — G6 file, `RelayOnly`; blake3 matches end to end.
- `large_file_over_websocket_relay_intact` — same on the `WebsocketOnly` rung (the worst case must still complete).
- `relay_path_meets_relay_throughput_floor` — measure MB/s on the relay rung; assert ≥ an agreed (lower-than-direct) floor. This number is the SLA we tune toward.
- `relayed_bytes_are_metered` — after a relay transfer, the per-(tenant,transfer) relayed-byte counter is > 0 and equals the payload (±framing); after a *direct* transfer it is 0. **This counter is both the billing rail and the oracle that proves the rung was really used.**

### `substrate_offline.rs` — G9 (the headline)
Re-run the essential matrix with `RelayPolicy::Disabled` + `MdnsOnly`, asserting
**zero** reliance on any non-LAN endpoint:
- `offline_discovery_and_sync`
- `offline_chat_all_levels`
- `offline_file_share_and_large_transfer`

### `substrate_swarm.rs` — G10 (red; drives the next build)
All `#[ignore]` with a `todo!()`/`unimplemented!()` body so they compile red:
- `partial_transfer_resumes_from_offset`
- `file_fetched_from_two_sources_in_parallel`
- `transfer_survives_source_peer_churn`
- `i_have_who_has_negotiation_picks_a_holder`

### `substrate_mesh.rs` — xaeroflux primitives
From the xaeroflux survey (already drafted as T1–T6): `mesh_bootstrap_forms`,
`event_propagates_to_all_peers`, `group_topic_auto_subscribe_on_announce`,
`peer_introduction_lists_both_peers`, `peer_departure_marks_offline`,
`snapshot_request_serve_round_trips`. Add a thin `tests/support` mirroring the
cyan-backend harness (`spawn_local_node`, `wait_for_event`).

---

## 4. Lens — HTTP client leg, test only optionality (`substrate_lens.rs`)

**Reality:** Lens is **not** a mesh peer and is not reached over iroh. cyan-backend
talks to it as a plain **HTTP client** (`src/cyan_lens_client.rs`: reqwest →
Lens's `/api/v1/*` REST API, default `http://localhost:8080`). One-way — backend
is the client, Lens is the server, Lens never writes back into the mesh.

Almost the entire Lens surface (`/query`, `/summarize`, `/nudges`, `/graph/*`,
`/events`) is the **enrichment / integration-graph** layer — explicitly out of
scope (moving to MCP/workflow). So the only substrate-relevant property is
**optionality: the mesh must work fully with Lens unreachable** (offline-first).

- `mesh_fully_functional_with_lens_unreachable` — run the discovery/sync/chat/
  file matrix with `CYAN_LENS_URL` pointing at a dead port; assert
  `CyanLensClient::is_available()` is `false` **and** every G1–G9 behavior still
  passes. Lens being down must never block P2P. This is the one test that ships now.

Testing the backend→Lens HTTP **contract** itself (request shapes of
`send_event`/`query`) belongs to the enrichment track, not the substrate — defer
it, and when it comes, do it against a stub Lens server (tiny axum fixture or
wiremock), not the real vLLM service.

---

## 5. Operating rules for the test agents (non-negotiable)

1. **Harness first.** `tests/support/mod.rs` is built and reviewed before any test case. Everything else imports it.
2. **Tests are the spec.** Never weaken an assertion to get green. If a test seems wrong, stop and ask. (Same rule as cyan-forge.)
3. **In-process, deterministic, bounded.** N nodes in one binary; every wait is a `timeout`; no unbounded `recv()`, no bare `sleep` as synchronization.
4. **No internet in the offline suite.** `RelayPolicy::Disabled` + `MdnsOnly`. A test that needs a public relay to pass is a bug in the test.
5. **Assert on the receiver's storage**, not on log lines.
6. **One agent owns one file.** Coordinate scope here, not by reaching into another file. Shared `support/` changes get a note.
7. **iroh 0.95 only.** No version bumps; no API from 1.x.
8. **Don't test out-of-scope rails** (Iggy/enrichment/integration events).
9. **Green bar before done**: `cargo test --test <file>` passes (ignored ones stay ignored), `cargo clippy -D warnings` clean, no production code weakened to fake a pass. If a test can only pass by changing engine behavior, that's a real finding — raise it, don't paper over it.

---

## 6. Build order (so the headline lands first)

1. `support/mod.rs` (cyan-backend) + `support` in xaeroflux — the harness.
2. `substrate_discovery.rs` (G1/G2-LAN) — proves nodes meet with relay off. *Everything depends on this working.*
3. `substrate_files.rs` (G6/G8) — the "fast QUIC large files" promise.
4. `RelayFixture` + net-isolation plumbing, then `substrate_relay.rs` (G2 ladder/G8-R/G11) — the **paid path** and metering. High priority: this is revenue and the hardest to get right.
5. `substrate_offline.rs` (G9) — the headline, re-using 2 & 3 with `Disabled`.
6. `substrate_sync.rs` (G3/G4) and `substrate_chat.rs` (G5/G7) — breadth.
7. `substrate_mesh.rs` (xaeroflux) in parallel — primitive-level confidence.
8. `substrate_swarm.rs` (G10, red) — the spec for the swarming work that follows.
9. `substrate_lens.rs` — the single optionality test (§4).

---

## 7. How to spin the agents

One Claude Code agent per file, each in its own git worktree off the repo, with
`CLAUDE.md` + this spec as standing context. Suggested launch prompt:

> Read `CLAUDE.md` and `SUBSTRATE_TEST_SPEC.md`. You own **`<file>`** only.
> First confirm `tests/support/mod.rs` exists and compiles; if not, and you are
> the harness agent, build it per §1 and stop for review. Otherwise implement the
> named tests for your file in §3 against the real `NetworkActor`/`storage` APIs,
> using `MeshHarness`. Bounded timeouts only; assert on receiver storage; never
> weaken an assertion; iroh 0.95 only. Run `cargo test --test <file>` and
> `cargo clippy --all-targets -- -D warnings`. Do NOT edit other test files,
> `CLAUDE.md`, or this spec. `git add -A && git commit` your file + any
> `support/` additions, and report which named tests are green vs still red.

Launch the **harness agent alone first**, review, then fan out discovery → files
→ relay → offline → sync/chat in parallel worktrees. The relay agent also owns the
`RelayFixture` + net-isolation plumbing. xaeroflux mesh agent runs anytime. The
swarm agent only scaffolds red files until its upstream work lands; the Lens agent
writes the one optionality test (§4).

---

## 8. iroh hardening & speed checklist (the relay-money path)

Relay isn't just "make it connect" — if customers pay for large files over relay,
the relay path has to be **fast and bullet-proof**. The relay agent treats this as
the work behind G8-R, and each item gets a measurement or an assertion, not a vibe.

**Harden (correctness under bad networks):**
- Pin our **own relay** (`relay.dev.cyan.blockxaero.io`) in config; public n0 relays are fallback only. Tests use a local `iroh-relay` fixture, never a public one.
- Verify the **full ladder degrades cleanly**: direct → relay-QUIC → relay-proxy → WebSocket-only, with bounded timeouts at each rung and no hang when a rung is impossible.
- **Resume / integrity** on the relay path: a transfer interrupted mid-stream resumes from offset (ties into G10) and always blake3-verifies; a flipped byte fails the hash.
- **Backpressure**: large transfers must not OOM — chunked `write_chunk` with a bounded in-flight window; assert steady memory on a 1 GB relay transfer.
- **Churn**: relay drop / reconnect mid-transfer recovers or fails cleanly (never silent truncation).

**Tune (make it flow fast) — measure before/after, keep the numbers in the test:**
- **Stream chunk size** and **send window** — sweep, record MB/s on the WebSocket rung; pick the knee.
- **Parallel streams per file** for large blobs (split → multiplex over the connection) — the biggest lever on a relayed path; gate behind a size threshold.
- **Relay region selection** — nearest relay by latency; assert we don't pin a far region.
- **Congestion / datagram vs stream** on the WebSocket transport — confirm we're not stuck on a tiny default window.
- Record a **throughput baseline per rung** (direct / relay-QUIC / WebSocket) so `relay_path_meets_relay_throughput_floor` has real numbers and regressions are visible.

The G11 relayed-byte meter is the through-line: it bills the customer **and** proves
in every relay test that the bytes actually went over the relay we're charging for.
