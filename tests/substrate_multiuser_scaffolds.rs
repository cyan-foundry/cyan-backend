//! Substrate (multi-process) — RED SCAFFOLDS for multi-user capabilities NOT built in this round.
//!
//! Every test here is `#[ignore]`d with a precise reason. They are honest placeholders, not fake
//! passes: each body `unimplemented!()`s, so running `--ignored` fails loudly rather than green.
//! The reason on each `#[ignore]` is the engine/cross-repo gap that must close first. See
//! STATUS_BACKEND_MULTIUSER.md for the full green-vs-ignored matrix and the build order.
//!
//! What IS green (and where): the grant-gated per-group snapshot join, the no-grant /
//! expired / revoked / replayed rejections, and their offline analogues live in
//! `substrate_multiuser_mp` + `substrate_offline_multiuser_mp`; live presence (join direction)
//! in `substrate_presence`; late-joiner full snapshot in `substrate_snapshot_mp`; concurrent-edit
//! convergence / delta sync in `substrate_sync`; multi-user chat & file transfer in
//! `substrate_chat` / `substrate_files` / `substrate_swarm`; partition/drop resilience in
//! `substrate_resilience`.

// ── 2b. MULTI-SOURCE snapshot serving + Lens fallback ─────────────────────────────────────────

/// The joiner should pull the group snapshot from SEVERAL holders in parallel (load-distributed,
/// like the blob swarm), so one device isn't hammered.
#[test]
#[ignore = "engine gap: snapshot serving is single-source (one SNAPSHOT_ALPN dial to one holder). \
Multi-source snapshot needs holder discovery (gossip SnapshotAvailable already exists) + a \
parallel/deduped pull. The swarm multi-source machinery exists for FILES (swarm.rs::fetch) but is \
not wired to the snapshot path. Build: chunk the snapshot as content-addressed blobs or add a \
multi-holder snapshot fetch loop."]
fn snapshot_served_multi_source_no_single_peer_overload() {
    unimplemented!("multi-source snapshot fetch not built — snapshot is single-source");
}

/// Only when NO device peers are online should the Lens super-peer replica serve the snapshot
/// (Lens is the always-on fallback, not the default).
#[test]
#[ignore = "cross-repo gap: cyan_lens_client is HTTP-only and has no snapshot/replica endpoint; \
Lens does not mount the SNAPSHOT_ALPN or hold a per-group state replica. Needs a Lens-side \
snapshot endpoint + a cyan-backend fallback that dials Lens only after all device holders fail. \
Coordinate with cyan-lens."]
fn lens_replica_serves_snapshot_only_when_all_devices_offline() {
    unimplemented!("Lens snapshot replica not built — Lens client is HTTP-only");
}

// ── 1/2. Concurrency ──────────────────────────────────────────────────────────────────────────

/// Many peers presenting valid grants join the same group at once and all converge on the full
/// per-group snapshot with no lost rows.
#[test]
#[ignore = "not built this round: the grant-gated join is proven for sequential joiners \
(substrate_multiuser_mp). A concurrent-joiner variant needs N joiner processes issued distinct \
grants (distinct nonces) joining simultaneously, then asserting each one's storage converges. \
The engine path supports it (each grant has its own nonce); only the multi-process test harness \
fan-out is unwritten."]
fn concurrent_joiners_all_converge() {
    unimplemented!("concurrent multi-process joiner fan-out test not written");
}

// ── 3. XaeroID ↔ SSO binding ──────────────────────────────────────────────────────────────────

/// A device's XaeroID is bound to its SSO user so the cloud role and the mesh grant resolve the
/// SAME identity.
#[test]
#[ignore = "engine + cross-repo gap: there is no XaeroID↔SSO binding store (no \
{xaeroid_pubkey, sso_user, provider, proof} record, no bind verb/event). The cloud SSO half lives \
in cyan-lens (cyan-identity broker); the bind verb must be coordinated with it. Build: a signed \
binding record + storage + an additive bind FFI/command, then resolve cloud-role↔mesh-grant \
through it."]
fn xaeroid_sso_binding_resolves_same_identity() {
    unimplemented!("XaeroID↔SSO binding store not built — needs cyan-identity/lens coordination");
}

/// With no internet, a previously-SSO'd device authenticates via its CACHED session + grants.
#[test]
#[ignore = "depends on the XaeroID↔SSO binding store above: the cached-session path needs a \
persisted binding + last-good session to fall back to offline. Pure-P2P (XaeroID-only) offline \
auth + offline grant verification ALREADY works (substrate_offline_multiuser_mp); only the \
SSO-cached-session leg is unbuilt."]
fn xaeroid_login_when_sso_unavailable_cached_session() {
    unimplemented!("SSO cached-session path not built — depends on the binding store");
}

// ── 4. Offline revocation propagation ─────────────────────────────────────────────────────────

/// A revocation made while online must apply on the revoked peer's next reconnect.
#[test]
#[ignore = "engine gap: revocation is in-memory per node (MeshAuthorizer::revoke tombstones a \
(group,nonce) locally; offline rejection of a locally-revoked grant IS tested in grant_test). \
Propagation needs the tombstone gossiped as group state so a reconnecting peer learns it — the \
gossiped-revocation path is the documented follow-up from STATUS_IDENTITY_GRANTS, not built here."]
fn revocation_made_online_propagates_on_reconnect() {
    unimplemented!("gossiped revocation tombstone not built — revocation is in-memory per node");
}

// ── ENTERPRISE SCALE (parameterized N, gated for big N) ───────────────────────────────────────

/// Many groups/workspaces/files stay correct under load (small N by default; large N gated behind
/// an env flag so default `cargo test` stays fast).
#[test]
#[ignore = "scale harness not built this round: the per-group correctness primitives are green at \
N=1..3 (snapshot/delta/chat/files across the substrate suite). A parameterized N rig + the \
CYAN_SCALE env gate for the big-N cases (many_groups…, many_concurrent_transfers…, \
many_concurrent_workflows…, sustained_churn…) is future work."]
fn enterprise_scale_many_groups_workspaces_files_stay_correct() {
    unimplemented!("parameterized enterprise-scale rig not built");
}
