# STATUS — File-swarm consumer (Wave 2), G10

Branch: `feat/file-swarm-consumer` (off `feat/mcp-local-host`). Do **not** merge to `main`.

Greens cyan-backend's G10 red scaffolds (`tests/substrate_swarm.rs`) by wiring multi-source,
content-addressed file swarming into the engine, on top of the blob-swarm primitive that shipped in
xaeroflux (`feat/blob-swarm`, `STATUS_BLOB_SWARM.md`). Plugins and media can now distribute
peer-to-peer at scale.

---

## Dependency decision — MIRROR (not a path/git dep on xaeroflux)

The brief was "prefer reuse — depend on xaeroflux if a *clean* path/git dep exists; otherwise mirror."
**A path dep is not clean here, so we mirror.** `xaeroflux::swarm::BlobSwarm` is a ~250-line,
self-contained primitive, but the `xaeroflux` crate as a whole drags in its entire second P2P engine
**and the `iggy` message broker** (`iggy = "0.6"` in its `Cargo.toml`). Pulling that into cyan-backend
would:

- re-introduce exactly the integration/broker surface this repo is **actively stripping** (see the
  recent `strip:` commits removing integrations + the Iggy enrichment path), and
- bloat the offline-first engine's build and dependency tree for the sake of one module.

cyan-backend **already declares** every dependency the primitive needs, at the **same versions**
(`iroh` 0.95, `iroh-blobs` 0.97, `bytes`, `serde`, `anyhow`, `tokio`). So mirroring the primitive into
**`src/swarm.rs`** adds **zero** new dependencies, keeps the engine lean and traceable (the simplicity
rule), and stays trivially diff-able against the upstream. The file header records this provenance.

**One adaptation for the engine seam:** the upstream `BlobSwarm` builds its **own** `Router`. The
`NetworkActor` already runs a single `Router` over its one endpoint (gossip + snapshot + file + dm
ALPNs); a second Router on the same endpoint would race two `accept()` loops. So the mirror does **not**
own a Router — it exposes `blobs_protocol()`, and the `NetworkActor` mounts it with
`.accept(BLOB_ALPN, swarm.blobs_protocol())` on the existing Router. Result: **one endpoint, one
router**, and blob holders are addressed by the node's **normal node id** (already wired by discovery).
Fully additive — the gossip/file/dm/snapshot paths are byte-for-byte unchanged.

No `unwrap()`/`panic!` in the new engine paths — all fallible steps use `?`/`map_err`. iroh 0.95 /
iroh-blobs 0.97 only; no version bumps.

---

## What was wired

**Primitive — `src/swarm.rs` (`cyan_backend::swarm::BlobSwarm`)**
- Content addressing (Blake3 = identity): `add(bytes) -> Hash`, `has`, `get`.
- i-have/who-has negotiation: `SwarmMessage::{IHave, WhoHas}` (plain JSON), a holder registry, and
  `on_message()` (records an `IHave`; answers a `WhoHas` with `IHave` when it holds the blob).
  Transport-agnostic — it rides the engine's existing gossip.
- Multi-source fetch + resume: `fetch(hash, holders)` tries holders in turn over `iroh-blobs`' remote
  get API (Blake3-verified streaming, verified ranges persisted), with a bounded per-holder dial
  (`DIAL_TIMEOUT = 5s`) so a departed holder fails fast and the fetch **resumes** against the next.
- Integrity gate: recomputes the Blake3 hash of the assembled bytes and rejects any mismatch before
  surfacing the blob.

**Engine seam — `NetworkActor` (`src/actors/network_actor.rs`)**
- Advertises `BLOB_ALPN` on the endpoint and mounts `swarm.blobs_protocol()` on the existing Router.
- Owns one `Arc<BlobSwarm>` per node; exposes `swarm()` (test-support + the fetch entry point) and
  threads the handle into every `TopicActor`.
- New engine-internal `NetworkCommand`s (additive; **not** client `cyan_*` FFI): `SwarmAnnounce`,
  `SwarmWhoHas`, `SeedAndAnnounceBlob` — routed to the group's `TopicActor`.

**Negotiation over gossip — `TopicActor` (`src/actors/topic_actor.rs`)**
- The gossip receive loop now tries `BlobSwarm::on_message` first (its `{"type":"IHave"|"WhoHas"}`
  shape is disjoint from `NetworkEvent`/`NetworkCommand`); records the holder / re-broadcasts an
  `IHave` reply onto the same group topic.
- `TopicCommand::{AnnounceBlob, QueryBlob}` broadcast the `IHave`/`WhoHas` messages via a new
  `broadcast_swarm_message` helper (rides the existing `GossipSender`, like every other event).

**Plugins-workspace distribution — `.cyanplugin` (content-addressed)**
- **Upload (reuses the existing path, NO new FFI):** `cyan_upload_file` already stages bytes, computes
  Blake3, inserts the file row, and broadcasts `FileAvailable`. For a `.cyanplugin` artifact it now
  *additionally* emits `SeedAndAnnounceBlob`, which adds the bytes to the node's swarm store and
  announces `IHave` to the group — so members learn a holder and can swarm-fetch it. Normal files are
  untouched.
- **Download (reuses `RequestFileDownload`):** the `TopicActor` download path now checks the swarm's
  holder registry for the blob; if holders are known (i.e. an `IHave` was seen, as for a seeded
  plugin) it does a **multi-source, churn/resume, Blake3-verified** `swarm_download_file`, lands the
  bytes on disk, sets `local_path`, and emits `FileDownloaded` — the **same** storage rows + event the
  iOS app already consumes. It **falls back** to the existing single-source file transfer when no
  holders are known, so all non-swarm files behave exactly as before.

**No new client FFI.** The app already sees plugins as files; `cyan_upload_file` /
`cyan_request_file_download` cover the flow. The only new `extern "C"` surface is **none** — the new
`NetworkCommand` variants are engine-internal.

---

## Tests green — `tests/substrate_swarm.rs` (offline, bounded, own-state oracles)

The 4 named G10 scaffolds are implemented and green; a 5th was **added** for the plugin consumer path.
Every wait is a `tokio::time::timeout`; every assertion is on the **receiver's own** per-node blob
store / holder registry (an honest per-node oracle even under the harness's process-global SQLite),
never a log line. `RelayPolicy::Disabled`, loopback addresses wired out-of-band.

1. **`partial_transfer_resumes_from_offset`** — a fetch handed an unreachable first holder (stalled at
   offset 0) followed by a live one resumes against the live holder and the requester's own store ends
   with the complete, Blake3-verified blob; a re-fetch exercises the already-present resume
   short-circuit. *(True mid-byte interruption is the existing `FileTransferMsg`/`resume_offset` wire
   path + the relay resume rung — out of in-process scope; not faked.)*
2. **`file_fetched_from_two_sources_in_parallel`** — two holders add the same bytes → one shared hash;
   a fresh requester fetches from both; its own store holds the verified blob.
3. **`transfer_survives_source_peer_churn`** — a holder is shut down (endpoint closed) and listed
   first; the bounded dial fails fast and the fetch falls through to the survivor; the requester's own
   store holds the verified blob.
4. **`i_have_who_has_negotiation_picks_a_holder`** — over the engine's real group gossip, the requester
   broadcasts `WhoHas`; the holder answers `IHave`; the requester's own holder registry lists the
   holder's node id.
5. **`plugin_seeded_into_plugins_workspace_distributes_to_members`** *(added)* — the uploader seeds a
   `.cyanplugin` via the real `SeedAndAnnounceBlob` hook; the member's own holder registry lists the
   uploader for the plugin's content hash.

```
cargo test --test substrate_swarm   → 5 passed; 0 failed; 0 ignored
```

No regressions in the networking path: `substrate_discovery` (2 passed) and `substrate_reliability`
(3 passed) stay green with the blob ALPN/Router mount in place (tests 4 & 5 themselves exercise full
discovery + gossip via `meet`). All test targets compile.

---

## Clippy

The new files (`src/swarm.rs`, `tests/substrate_swarm.rs`) and every edit add **zero** clippy
findings (the one `cloned_ref_to_slice_refs` in the test was fixed to `std::slice::from_ref`). The
warnings `cargo clippy --all-targets -- -D warnings` still reports are all **pre-existing** unused
imports/vars in untouched files (e.g. `FileTransferMsg`, `TopicId`, `AsyncReadExt`, `diagram_gen`),
the same debt other STATUS docs flag — unrelated to this work, to be cleared in a separate hygiene PR.

**Pre-existing, unrelated failure:** `diagram_gen::tests::test_parse_diagram_json` (SVG rendering in
`src/diagram_gen.rs`, a file this branch never touches) fails environmentally; independent of the
swarm work.

---

## Multi-holder + resume behavior (precise, inherited from the primitive)

- **Multi-source** = multi-holder with fallback: `fetch` is given N holders and pulls from whichever
  can serve, advancing across the set. It is **not** chunk-range parallelism across holders (the
  upstream `Downloader`'s `SplitStrategy::Split` did not operate under offline static addressing — see
  `STATUS_BLOB_SWARM.md`; true range-split parallelism is a future rung).
- **Resume** is real at holder granularity: verified partial chunks persist in the store, and the next
  holder's fetch resumes the missing ranges. The churn test exercises the connect-time case
  deterministically (departed holder, bounded dial, fall-through to the survivor).

## Out of scope (left as-is)

The relay/WebSocket resume rungs (G8-R, G11) need the Docker/netns rig, not in-process — untouched.
The Iggy enrichment pipeline / integration events stay stripped (a key reason we mirrored rather than
depended on xaeroflux). The xcframework was **not** rebuilt.
