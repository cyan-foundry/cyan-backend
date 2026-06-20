# STATUS — DB-open startup crash fix

Branch: `fix/presence-gossip-neighbors` (feature branch; never `main`)

## The bug

A fresh multi-instance run (`run_multi`, each peer with its own `CYAN_DATA_DIR`,
e.g. `~/cyan-multi/peer1`) **panicked** at engine init:

```
src/lib.rs:236 — Failed to open database: SqliteFailure(CannotOpen, 14,
  "unable to open database file: /Users/.../cyan-multi/peer1/cyan.db")
```

Cause: the DB path's **parent directory did not exist yet** for a brand-new data
dir, and the open used `.expect("Failed to open database")`. The panic crossed the
engine thread → storage dead for the whole instance → group create couldn't seed
workspaces, and identity couldn't persist (login every launch).

## The fix (additive, no FFI shape change)

All hardening lives in one place — `src/storage.rs`:

- **`resolve_db_path(requested) -> PathBuf`** — pure/deterministic path resolution.
  An explicit path (the FFI/app contract) wins verbatim, so **shipping behavior is
  unchanged**. When empty, falls back to `$CYAN_DATA_DIR/cyan.db` (the env
  `run_multi`/the app set per instance), else `./cyan.db`. Because it's
  deterministic, a relaunch with the same inputs resolves to the **same** db file —
  this is what makes identity + groups persist across relaunch.
- **`open_db(path) -> Result<Connection>`** — `std::fs::create_dir_all(parent)`
  before opening (covers the missing-data-dir case), then `Connection::open` with
  `map_err` → **typed `Err`, never a panic**. Logs the resolved path at init
  (`tracing::info!`) and a clear `tracing::error!` (path + os error) on failure.
  rusqlite's default `OpenFlags` already create-if-missing; the real failure was the
  missing parent dir, which `create_dir_all` now fixes.

Call sites updated to use the hardened path (both go through the same resolution, so
create and reopen open the SAME db):

- `src/lib.rs` `CyanSystem::new` — replaced
  `Connection::open(db_path).expect(...)` with `storage::resolve_db_path` +
  `storage::open_db(...)?`. The resolved path is logged and reused for
  `storage::init_db`. A bad data dir now returns `Err` up through `CyanSystem::new`
  → FFI `cyan_init*` returns `false` (graceful failure the app can surface) instead
  of crashing the engine thread.
- `src/storage.rs` `init_db` — now resolves + `open_db` (so it also creates the
  parent and never panics).
- `src/bin/cyan_node.rs` `init_base_schema` (the run_multi/dev_stack harness node) —
  reuses `storage::open_db` so its per-process DB also gets the parent dir created.

No `cyan_*` signature, JSON shape, or `SwiftEvent`/`NetworkCommand` variant changed.
The only behavioral change at the FFI surface is that init now **degrades to a
graceful `false`** on a genuinely unopenable path rather than panicking.

## Tests (test-first, bounded, no real panic) — `src/storage.rs::open_db_tests`

- `open_db_creates_missing_parent_dir` — nonexistent nested dir → parent created, db
  opens, connection usable. (Reproduces the run_multi case.)
- `open_db_failure_returns_error_not_panic` — ancestor is a file (so
  `create_dir_all` must fail) → typed `Err`, **no panic**.
- `same_datadir_reopens_same_db` — write a row, drop, reopen same path → row present.
- `distinct_datadirs_are_isolated` — two data dirs share no tables.
- `resolve_db_path_honors_explicit_then_env` — explicit path wins; empty → `cyan.db`.

All 5 pass.

## Test status

- New `open_db_tests`: **5/5 green**.
- `cargo build --all-targets`: clean (no errors).
- Clippy: no new warnings from the changed code.
- Pre-existing, **unrelated** failures (confirmed present on a clean tree via
  `git stash`): `substrate_multiuser_mp::expired_revoked_replayed_grant_rejected`
  (grant replay/nonce) and `diagram_gen::tests::test_parse_diagram_json`. Not
  touched by this change.

## Impact

This unblocks the multi-instance flow: a fresh instance with its own
`CYAN_DATA_DIR` now creates its data dir and opens its db instead of crashing, so
**group-create can seed workspaces** and **identity persists across relaunch** (no
login every launch).
