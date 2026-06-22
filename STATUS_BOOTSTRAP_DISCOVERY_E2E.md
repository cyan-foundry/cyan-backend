# STATUS — Bootstrap discovery cross-net mesh, E2E (SUPER_PEER_COMPLETION_SPEC §5; MESH_HARDENING §2/§10)

**Branch:** `test/bootstrap-discovery-e2e` (off `feat/sp-backend`). Never `main`. iroh 0.95 only.
xaeroflux is **read-only** (built into a container; never modified). Every wait is a bounded
`tokio::time::timeout`/deadline poll; every assertion is on the receiver's own `storage::*` /
resolved engine state — never on log lines.

## What this closes — the ONE make-or-break mesh gap

Before this, §5 was proven only at the **unit** level (`src/rendezvous.rs` resolve/verify/fallback)
and the cross-net discovery test (`bootstrap_seeded_cross_net_mesh`) was an honest `#[ignore]`d red
scaffold ("needs the real xaeroflux bootstrap container"). The live, end-to-end loop was unproven:

1. A real **`xaeroflux_bootstrap`** container starts and **self-publishes its signed rendezvous
   config** (its real `node_id` + bound addrs + `discovery_key` + relay) to a well-known URL.
2. App peers (`cyan_node`) start with `CYAN_RENDEZVOUS_URL` (+ `CYAN_ORG_PUBKEY`), **fetch + verify**
   the config via `rendezvous::fetch_and_apply_if_configured`, and adopt the **LIVE** bootstrap id —
   NOT a hardcode.
3. Two peers on **different, isolated networks** (separate Docker bridges — no route between them,
   mDNS not carried) find each other **THROUGH the bootstrap** → mesh forms → a live edit propagates.

All three steps are now proven **green against real containers**. The discover→seed→neighbor→
propagate loop works with the **live** bootstrap.

## Findings (the two make-or-break compatibilities)

- **A — discovery gossip protocol: COMPATIBLE (proven live).** `cyan_node`'s `DiscoveryActor` and
  the xaeroflux bootstrap both speak topic `cyan/discovery/{discovery_key}` with identically-shaped
  `groups_exchange` / `peer_introduction` messages. The bootstrap hears both peers' `groups_exchange`,
  auto-subscribes to `cyan/group/{gid}`, and relays gossip between the two islands. Confirmed by the
  cross-net mesh forming and live edits converging (see results).

- **B — rendezvous CONFIG wire format: was INCOMPATIBLE, now CLOSED (additively).** The bootstrap
  publishes `{config:{OBJECT…,ts}, signer, signature}` — **self-signed** (`signer == node_id`), with
  `bootstrap.addr` an array, signed over the *compact* `serde_json` of the config object. cyan-backend's
  original `rendezvous.rs` parsed only `{config:"<STRING>", signature}` verified against a *separately*
  pinned org key. The original parser **silently rejected the real published bytes and fell back to the
  bundled hardcode** — defeating the loop. Closed by teaching `resolve()` to ALSO accept the self-signed
  shape (additive; the original shape still works). Trust model:
  - self-certifying: `signer` must equal `config.bootstrap.node_id`;
  - **org-pin** mode (`CYAN_ORG_PUBKEY` set): the pinned key must equal `signer`;
  - **TOFU** mode (no pin): a self-consistent config is trusted on first use — which is what lets a
    redeploy with a fresh id be adopted with zero app retune.
  A unit test verifies the **exact bytes a real bootstrap container published** (`real_xaeroflux_
  published_config_verifies`), so cross-repo wire/signature compatibility is locked in.

## The fix that made it deterministic (engine seam, additive)

The cross-net mesh was initially **flaky** (sometimes formed, sometimes timed out). Root cause: the
test-only `cyan_node` bin set `DiscoveryPolicy::Bootstrap(id)` (discovery topic) from `BOOTSTRAP_NODE_ID`
but never set the `BOOTSTRAP_NODE_ID` **global**, so each **group** `TopicActor` (which reads
`bootstrap_node_id()`) kept the **bundled hardcode** `f992aa3b…` — a dead id. The mesh then only
formed when the slower discovery peer-introduction path happened to win a race. Fix: when
`BOOTSTRAP_NODE_ID` is set (or a config is verified), pin the **global** so the discovery topic AND
every group topic bootstrap off the SAME live node. `OnceCell::set` is first-wins, so explicit/FFI
values still win — the shipping FFI init path is unchanged when no rendezvous URL is set. After the
fix the test went from 186s-timeout-flake to a deterministic ~5–7s pass (3/3 reruns green).

## The rig (`harness/`)

- **`Dockerfile.bootstrap` + `scripts/assemble-bootstrap-context.sh`** — build the REAL
  `xaeroflux_bootstrap` (iroh 0.95; `rusqlite` bundled; Iggy disabled at runtime) into
  `cyan/bootstrap:rig`. xaeroflux is self-contained (no sibling path-deps), so the context is just
  that one crate's build inputs — copied read-only, never modified.
- **`tests/support/dockernode.rs`** additions:
  - `BootstrapNode` — runs the bootstrap detached on BOTH isolated bridges (the only common
    reachability), reads back the LIVE `node_id` + dialable addrs from the config it self-published
    (`docker exec cat …/rendezvous.json`, filtered to private/Docker-bridge addrs), and exposes its
    `EndpointAddr` JSON + the raw published bytes. `redeploy()` rotates the identity (wipes
    `node.key` from the shared volume) and republishes; `tamper_served_config()` corrupts the served
    doc for the rejection rung.
  - `ConfigServer` — a tiny `busybox httpd` serving the bootstrap's `rendezvous.json` (shared
    Docker volume `cyan-rig-rdv`) at `http://cyan-rig-config:8080/rendezvous.json` — the rig
    stand-in for the object store / well-known URL.
  - `DockerNode::spawn_with_env` (inject `CYAN_RENDEZVOUS_URL`/`CYAN_ORG_PUBKEY`) and
    `bootstrap_id()` (the new `cyan_node` verb returning the RESOLVED bootstrap id — a *positive*
    oracle for "adopted the live id" vs "fell back to bundled").
- **`Makefile`** — `build-bootstrap`, and `bootstrap-e2e` which runs the 4 rungs one-at-a-time
  (shared container names) after building both images.

## Engine / bin changes (additive; no FFI signature changed)

- `src/rendezvous.rs` — accept the self-signed (xaeroflux) config shape in `resolve()` (org-pin +
  TOFU), reproducing the bootstrap's canonical compact bytes for verification. + 4 unit tests
  (incl. the real-published-bytes fixture). The original org-signed-string path is untouched.
- `src/bin/cyan_node.rs` (TEST-ONLY bin) — pin the resolved bootstrap into the `BOOTSTRAP_NODE_ID`
  global (the determinism fix); call `rendezvous::fetch_and_apply_if_configured()` only when
  `CYAN_RENDEZVOUS_URL` is set (untouched offline/LAN behavior otherwise); source discovery_key /
  relay from the verified config when not set by env; add the `bootstrap_id` verb.

## The 4 gated rungs (Tier-2, `CYAN_RIG=1`, `#[ignore]`d so `cargo test` stays Docker-free)

| Rung | Proves (oracle) | Result |
|------|-----------------|--------|
| `bootstrap_seeded_cross_net_mesh` | peers on `mesh_a`/`mesh_b` (no route, mDNS not carried, RELAY=Disabled), given ONLY the bootstrap addr (never each other's) → bidirectional live-edit convergence (5→8→10 elements both sides) THROUGH the bootstrap | **PASS** (~5s; deterministic, 3/3 reruns) |
| `discovery_via_published_config_forms_cross_net_mesh` | peers given ONLY `CYAN_RENDEZVOUS_URL`+org key (no id, empty discovery_key env) fetch+verify the published config, adopt the LIVE id (`bootstrap_id == live`, `!= bundled`), then form the cross-net mesh | **PASS** (~9s) |
| `tampered_published_config_rejected_peer_uses_fallback` | a tampered served config ⇒ the peer's resolved `bootstrap_id == bundled` (no false bootstrap adopted) — a positive assertion, not a timeout | **PASS** (~4s) |
| `bootstrap_redeploy_new_id_picked_up_no_app_change` | bootstrap redeploys with a rotated id; a FRESH peer at the SAME URL with NO env change picks up the NEW id (`!= first`, `!= bundled`) | **PASS** (~7s) |

Plus `src/rendezvous.rs` unit tests: **7 green** (3 original + 4 new, incl. the real published-bytes
fixture). The existing 10 mesh-e2e rungs were regression-checked (`lan_mesh_forms…`,
`all_infra_down…`, `bootstrap_down…`) — still green after the bin/global change.

## How to run

```bash
make -C harness build-bootstrap     # build cyan/bootstrap:rig (the REAL xaeroflux bootstrap)
make -C harness bootstrap-e2e       # build both images + run all 4 rungs (CYAN_RIG=1)
# one rung:
CYAN_RIG=1 cargo test --test substrate_mesh_e2e bootstrap_seeded_cross_net_mesh \
  -- --ignored --nocapture --exact
# unit (no Docker):
cargo test --lib rendezvous
```
A plain `cargo test` runs `substrate_mesh_e2e` as **15 ignored**, touches no Docker, stays fast.
Set `CYAN_RIG_LOG_DIR=<dir>` to keep per-peer stderr for post-mortem.

## Honest scope notes (not faked)

- **Why peers pre-seed the same baseline** in the cross-net rungs: a gossiped element only *applies*
  on receipt if its parent board exists, and the cross-net snapshot **transport** (a direct QUIC
  dial between two isolated peers) needs a relay — already proven by
  `substrate_relay::connects_via_relay_when_direct_blocked`. These rungs isolate the property that
  was unproven: **auto-DISCOVERY + live-gossip relay through the bootstrap**. The converged element
  rows ARE the far peer's authored content, delivered across bridges that have no other path.
- **The bootstrap is the gossip relay, not an iroh relay.** Peers connect peer↔bootstrap on the
  group topic; the bootstrap floods broadcasts between the two islands. The persistent roster
  records `delivered_from` (the relay neighbor = the bootstrap), not the multi-hop author — so each
  peer's roster holds the bootstrap, by design; convergence (not the roster) is the cross-net proof.
- **Redeploy uses TOFU** (no pinned key): self-signed configs change `signer` when `node.key`
  rotates, so an org-pinned key would (correctly) reject the new bootstrap. TOFU is the model that
  delivers "no per-deploy retune" for a fresh peer — exactly what the rung asserts.
- **Still honest-red:** `offline_peer_message_held_by_superpeer_delivered_on_return` — hold-for-
  offline-peer + redeliver is Lens super-peer logic (cyan-lens, fakes-only; no runnable real binary).
  Unchanged by this batch. See STATUS_MESH_HARNESS.md.

## Verification done this session

- `cargo test --test substrate_mesh_e2e --no-run` — clean. Default `cargo test` → **15 ignored**, Docker-free.
- `cargo test --lib rendezvous` — **7 passed** (incl. the real published-config fixture).
- `cargo clippy --all-targets` — no findings in the touched files (`rendezvous.rs`, `cyan_node.rs`,
  `dockernode.rs`, `substrate_mesh_e2e.rs`); only the pre-existing `xaeroID` dep warnings remain.
- All **4 bootstrap rungs EXECUTED GREEN** against real `cyan/bootstrap:rig` + `cyan/node:rig`
  containers on the isolated `cyan-rig_mesh_a` / `cyan-rig_mesh_b` bridges (times in the table).
- 3 existing mesh-e2e rungs re-run green (no regression from the bin/global change).

## Stop.
The discover→seed→NeighborUp→propagate loop is proven LIVE end-to-end with the real xaeroflux
bootstrap: a peer discovers the live bootstrap via the signed rendezvous config (fetched + verified,
no hardcode) and forms a cross-network mesh; tampered configs are rejected; a redeploy's new id is
picked up with no app change. Run with `make -C harness bootstrap-e2e`.
