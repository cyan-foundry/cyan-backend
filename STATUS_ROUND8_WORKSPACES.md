# STATUS — Round 8 / W3: Default + Plugins workspaces, no empty groups

**Branch:** `feat/round8-workspaces` (off `feat/round8-notes`)
**Scope:** backend only. Additive. iOS workspace UX is batch 2b (not here). xcframework
NOT rebuilt (per the prompt).

## What shipped

Group creation now **auto-provisions** workspaces, so a group is never born empty. Every
new group is seeded with:
1. a **default** workspace — **"General"** — where the user lands (non-system, deletable);
2. a system **"Plugins"** workspace — **per group** — that holds that group's installed
   plugin files; flagged `system` and **non-deletable**.

Both seeded workspaces replicate to a joiner over the **existing snapshot/digest path** —
no new transfer protocol.

### Provisioning (`src/storage.rs`)
- `provision_group_workspaces(group_id, owner_node_id) -> Result<(Workspace, Workspace)>`
  — the single provisioning primitive. Idempotent: workspace ids are **deterministic**
  (`default_workspace_id` = `blake3("default-ws:<gid>")`, `plugins_workspace_id` =
  `blake3("plugins-ws:<gid>")`) and inserts are `INSERT OR IGNORE`, so a replayed
  gossip/snapshot of the seed converges instead of duplicating. Returns both workspaces so
  the caller can broadcast their `WorkspaceCreated` events.
- Constants `DEFAULT_WORKSPACE_NAME = "General"`, `PLUGINS_WORKSPACE_NAME = "Plugins"`.

### The system flag (`workspaces.is_system`)
- New `is_system INTEGER NOT NULL DEFAULT 0` column — idempotent migration in
  `storage::run_migrations` (constant default keeps the `ALTER` valid on existing rows),
  exercised on DBs that predate it.
- `Workspace` (`src/models/core.rs`) gains `system: bool` with `#[serde(default)]` ⇒
  wire-compatible both ways with older peers; persisted rows decode as ordinary
  workspaces.
- `workspace_insert` / `workspace_list_by_group` now carry/read `is_system`.
- `workspace_is_system(id) -> bool` — queryable flag (false for unknown ids).

### Non-deletable guard (`workspace_delete`)
- `workspace_delete` **refuses** before touching anything if the workspace is a system
  workspace (returns `Err`), so the Plugins workspace — and any installed plugin files it
  holds — survive a standalone "delete this workspace" call. Deleting the **whole group**
  still cascades (the group-level delete intentionally removes all of its workspaces).

### The group-create path (`src/lib.rs`, `CommandActor::CreateGroup`)
- After inserting the group, the create path calls `provision_group_workspaces(id,
  Some(node_id))` and broadcasts a `WorkspaceCreated` event for each seeded workspace
  (the same path a normal `CreateWorkspace` uses) — so already-live peers get them via
  gossip and a cold joiner gets them via the snapshot. One obs line per seeded workspace:
  flat `obs group_provision_ws … system=<bool>` carrying `tenant_id` (tenant == group).

### Replication (snapshot/digest — no new transfer)
- Workspaces already ride `SnapshotFrame::Structure` (`workspace_list_by_group`) and the
  `group_digest` (`w␁<id>␁<created_at>` lines), so the seeded pair is detected + pulled by
  the existing bounded anti-entropy sweep.
- Snapshot **apply** (`topic_actor.rs`) was switched from `workspace_insert_simple(id,
  group, name)` to the full `workspace_insert(w)`, so `created_at` **and** the `system`
  flag replicate — a joiner now recognizes the per-group Plugins workspace as system /
  non-deletable too (and the workspace `created_at` converges in the digest instead of
  being reset to the receiver's clock). The gossip `WorkspaceCreated` persist path already
  used `workspace_insert(w)`, so the two paths are now consistent.

## Tests (all named per §W3; none weakened)
Storage (`tests/workspaces_test.rs`, 4/4 green):
- `create_group_seeds_default_workspace`
- `create_group_seeds_plugins_workspace`
- `plugins_workspace_is_system_nondeletable`
- `no_api_path_yields_empty_group` (also asserts provisioning is idempotent — re-running
  yields exactly two workspaces, never more)

Multi-process convergence (`tests/substrate_workspaces_mp.rs`, 1/1 green):
- `seeded_workspaces_sync_to_joiner` — two real `cyan_node` OS processes. The host
  provisions the group (group record + the two seeded workspaces) before its actor starts;
  a cold joiner joins, snapshots, and converges to **both** workspaces — and the system
  flag rides along (`count system_workspaces == 1` on the joiner). Bounded waits; asserted
  on the joiner's own storage counts, never on logs.

Harness additions (additive): `count system_workspaces` kind + `PROVISION_GROUP` boot env
(and a `provision_group` helper) in `cyan_node`.

## No regression
- `tests/workspaces_test.rs` 4/4, `tests/substrate_workspaces_mp.rs` 1/1.
- Workspace-replication paths re-run and hold with the new column/flag:
  `substrate_snapshot_mp::seeded…`/`late_joiner_gets_full_snapshot`,
  `substrate_sync::{three_node_convergence, delta_workspace_structure_propagates}`,
  `substrate_notes_mp::two_peers_converge_on_notes`, the chat/file/grant substrate suites —
  all ✅. The pre-existing plugins-workspace tests
  (`plugin_seeded_into_plugins_workspace_distributes_to_members`,
  `plugin_tool_from_plugins_workspace_is_discoverable`) still pass.
- `cargo build --tests` green. My changed surface (storage / core / topic_actor / lib /
  cyan_node) is **clippy-clean**; the repo's base is not `-D warnings`-clean (pre-existing
  unused-import lints in untouched files) — no new lint introduced by W3.
- Two failures seen only when the *entire* suite runs in one invocation, both
  **pre-existing / environmental, not W3 regressions**: `diagram_gen::tests::
  test_parse_diagram_json` fails on the base branch too; `substrate_stress::
  swarm_blob_multi_fetch_integrity` is the documented single-box CPU/port contention when
  stacking the heaviest MP scenarios — it passes green run on its own (4s).

## Tier-2 / deferred (out of scope here)
- iOS workspace UX (land in default workspace on create, Plugins workspace surfaces
  installed plugin files, no empty-group UI path) — batch 2b.
- Installs landing in the Plugins workspace is the W5 (Marketplace) → W3 handoff; W3 ships
  the workspace + the system flag it lands in, not the install action itself.
- `seed_demo_if_empty` (the first-run demo group) still seeds its own single demo
  workspace directly and is left as-is — it is the demo-seed path, not the authoring
  group-create path, and it already creates a non-empty group.
