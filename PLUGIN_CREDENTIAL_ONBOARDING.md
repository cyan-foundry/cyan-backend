# Plugin Credential + Config Onboarding — design (refreshed 2026-07-08)

**Status:** design refreshed for the review-loop fix pass; stages A–C below are
implemented in this branch, D–E are the follow-up. **Owner:** Rick.
Supersedes the copy in `~/Downloads/anthropic_data_dump/PLUGIN_CREDENTIAL_ONBOARDING.md`
(the four-stage model there — Declare / Capture / Store / Inject — stands; this
revision adds **per-workflow CONFIG** as a first-class citizen and pins what is
built now vs later).

**Problem, one line:** a plugin needs (a) a *secret credential* (the client's own
token) and (b) *non-secret config* (which Frame.io account, which folder, which
C2C project) — and today both are ambient env vars (`FRAMEIO_IMS_TOKEN`,
`FRAMEIO_ACCOUNT_ID`, `FRAMEIO_FOLDER_ID`) read from the app process. That is a
demo stopgap: one operator serves MANY producers, each with their own account and
target folder, and the app process env is a **launch-time snapshot** (the live
401-mid-session bug: `~/.frameio.env` refreshes hourly on disk, but the running
app injected the token its process was born with).

---

## The two kinds of value (they route differently)

| | Secret credential | Config |
|---|---|---|
| Examples | IMS/OAuth token, API key, service key | `account_id`, `folder_id`, C2C project |
| Declared by | manifest `credentials` block (kind/provider/**locality**) | tool `input_schema` required props |
| Scoped to | install placement (device vs tenant) | **workflow (board) → tenant → demo env**, most-specific wins |
| Stored in | **Vault** (Keychain / Lens TenantVault) — `SecretString`, never logged | `plugin_config` SQLite table (plain rows, non-secret) |
| Injected | spawn **env**, minted/read FRESH per spawn | tool **args**, resolved fresh at bind/dispatch |

The load-bearing invariant is unchanged: **the plugin reads its credential from
the injected environment and its targets from its args — nothing else.** No
change here touches any plugin.

## Stages (A–C built in this pass, D–E follow-up)

**A. `plugin_config` store (engine).** SQLite `plugin_config(tenant_id, board_id,
plugin_id, key, value, updated_at)`, `board_id=''` meaning tenant-wide. Additive
FFI: `cyan_plugin_config_set` / `cyan_plugin_config_get` (JSON, board+tenant
scoped) so the app's install/settings UX can write it. Non-secret ONLY — a key
that looks secret (contains `token`/`secret`/`key`/`password`) is refused with a
clear error and must go to the vault.

**B. Config-scoped arg resolution.** The deterministic bind's context fallback
(`env_context_value`, the thing that fills `account_id`/`folder_id`) becomes
`config_context_value(tenant, board, plugin, prop)`: **workflow row → tenant row
→ process env (demo)**. Setting a workflow's `folder_id` in `plugin_config`
retires `FRAMEIO_FOLDER_ID` for that workflow; the env fallback keeps the demo
green during the transition.

**C. Fresh-per-spawn credential injection (kills the 401).** `bundle_spawn_config`
resolves each declared credential at EVERY spawn, in order:
1. **Vault** — `KeychainVault` service `io.blockxaero.cyan.plugins`, key
   `cyan.plugin.<plugin>.<provider>.<tenant>` (device-local; the Lens TenantVault
   replica is stage E);
2. **Credential dotenv** — a fresh read of `~/.frameio.env`-style files named by
   `CYAN_CRED_ENV_FILE` (the auto-refreshing loader rewrites that file hourly;
   reading it per spawn — instead of the process env snapshot — is what fixes
   401-mid-session with zero plugin changes);
3. **Process env** — the demo stopgap, last.

**D. Capture UX (follow-up).** Install-time "Connect <provider>" sheet keyed off
`manifest.credentials` (OAuth for `adobe_ims`, secure field for `api_key`,
service account for `s2s`) writing the refresh material to the vault, and a
plugin-settings sheet writing `plugin_config` rows per workflow. The engine FFI
from stage A/C is already sufficient for this UI.

**E. TenantVault in Lens + s2s team installs (follow-up).** `locality: tenant`
(the frameio manifest already pins it) routes an Admin's service credential to
the tenant vault on the Lens super-peer for cloud/team runs; per the original
doc's table, device installs keep using the DeviceVault.

## Security invariants (unchanged, enforced in code)

- Credentials are `SecretString`; never logged, never in args/argv/manifest/obs.
- The plugin receives a short-lived token only; refresh/service material stays
  with the host/vault. Renewal is the host's job (the fresh-per-spawn read).
- Device-scoped credentials never replicate; tenant credentials live only in
  that tenant's vault.
- `plugin_config` holds non-secret values ONLY (guarded at the write API).

## Demo → real (why the demo stays green)

Nothing removes the env fallbacks yet — they just moved to the END of each
resolution chain. `manual_test.sh` keeps working with `~/.frameio.env` alone;
setting a vault credential or a `plugin_config` row simply wins over it. The
migration is: capture UI writes vault + config rows → the env fallbacks go cold
→ delete them in a later pass.
