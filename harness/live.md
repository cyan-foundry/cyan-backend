# `live.sh` — N-account live test, one command

Headless peers **self-identify** (auto identity per peer, **no login / no SSO**), so `--peers` can be
as large as you like — you're not limited by how many accounts you have. Every peer acts; **all** peers
must converge; we assert on each peer's **own storage** (never on log lines). Exit code = verdict.

## The three you'll actually type

```bash
# Full test — 8 peers, normal network, every scenario, asserted:
./harness/live.sh --peers 8 --net home --scenario all

# Corporate firewall sim — block direct UDP so peers MUST fall back to relay / WebSocket:
./harness/live.sh --net corp

# Offline — no internet / no relay / no bootstrap, mDNS-LAN only:
./harness/live.sh --net offline
```

## Toggles (all optional; `--help` lists them)

| flag          | default | meaning                                                              |
|---------------|---------|---------------------------------------------------------------------|
| `--peers N`   | `8`     | N headless peers, each own data dir + auto identity (no login).     |
| `--mode`      | `macos` | `macos` = native `cyan_node` procs on this Mac; `docker` = isolation rig. |
| `--net`       | `home`  | `home` direct/hole-punch · `corp` relay/WebSocket fallback · `offline` mDNS-LAN only. |
| `--scenario`  | `all`   | `sync` · `files` · `chat` · `workflow` · `all`.                     |
| `--keep`      | off     | leave the rig up for poking (Docker tier).                          |
| `--app N`     | off     | also launch real app instances for a **manual** UX pass (separate path). |

## What gets asserted (per peer, bounded waits)

- **sync** — each peer creates whiteboard objects → every peer's element count == the exact union.
- **chat** — each peer sends board chat → every peer's chat count == the exact union (dedupe by id).
- **files** — each peer uploads a blob → every peer fetches + **blake3-verifies** every other peer's blob.
- **workflow** — one peer authors a local-placement workflow → every peer sees the steps + the pinned gate.

`home`/`offline` run on the **macos** tier (N native peers, relay disabled — this *is* the offline/LAN
rung). `corp` routes to the **Docker** rig (real UDP block → relay, then relay-over-WebSocket). See
`STATUS_ROUND8_HARNESS.md` for the full mechanism and a sample run.
