# Cyan Docker rig — relay / WebSocket / LAN / offline rungs. See STATUS_DOCKER_RIG.md.
#
# The rungs are Rust tests in tests/substrate_relay.rs, gated behind CYAN_RIG=1 and run
# with `--ignored` so they NEVER run in a plain `cargo test` (which stays Docker-free).
# Each rung test `docker run`s two cyan_node peers onto the right networks and drives them
# over the same stdin/stdout line protocol the in-process multi-process rig uses.
.DEFAULT_GOAL := help
COMPOSE := docker compose
NODE_IMG := cyan/node:rig
REPO_DIR := $(abspath ..)
# CYAN_RELAY_URL is read by the rung tests; default matches `iroh-relay --dev` (HTTP/3340).
export CYAN_RELAY_URL ?= http://cyan-rig-relay:3340
export CYAN_NODE_IMAGE ?= $(NODE_IMG)

help: ## list targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-18s %s\n",$$1,$$2}'

relay-info: ## build the relay image and print its --help (confirms dev ports)
	$(COMPOSE) build relay
	-docker run --rm --entrypoint /usr/local/bin/iroh-relay cyan/relay:0.95.1 --help

up: ## build relay image + create the isolation networks (relay running)
	$(COMPOSE) up -d --build relay
	$(COMPOSE) ps

build-node: ## assemble the minimal build context and build the cyan_node image
	./scripts/assemble-context.sh
	docker build -t $(NODE_IMG) -f Dockerfile.node .ctx

BOOTSTRAP_IMG := cyan/bootstrap:rig
export CYAN_BOOTSTRAP_IMAGE ?= $(BOOTSTRAP_IMG)

build-bootstrap: ## assemble the context and build the REAL xaeroflux_bootstrap image
	./scripts/assemble-bootstrap-context.sh
	docker build -t $(BOOTSTRAP_IMG) -f Dockerfile.bootstrap .ctx-bootstrap

# Each rung sets CYAN_RIG=1 and runs ONLY its named test(s) with --ignored --nocapture.
# `cargo test` runs from the repo root so it picks up the workspace.
RUN_RUNG = cd $(REPO_DIR) && CYAN_RIG=1 cargo test --test substrate_relay

test-lan: ## both peers on `lan` -> direct QUIC; snapshot syncs intact
	$(RUN_RUNG) lan_direct_snapshot_intact -- --ignored --nocapture --exact

test-relay: ## split bridges + relay -> forced relay path; snapshot syncs intact
	$(RUN_RUNG) connects_via_relay_when_direct_blocked -- --ignored --nocapture --exact

test-ws: ## split + outbound UDP dropped -> relay over WebSocket; snapshot syncs intact
	$(RUN_RUNG) connects_via_websocket_when_udp_fully_blocked -- --ignored --nocapture --exact

test-offline: ## DOCUMENTED-RED: truly-airgapped rung blocked by unconditional mDNS init
	@echo "offline_airgap_snapshot_intact is a documented-red scaffold:"
	@echo "  the engine inits MdnsDiscovery unconditionally → 'Service mdns error' on a"
	@echo "  gateway-less internal Docker net. Relayless/offline sync is proven green by"
	@echo "  test-lan (RELAY=Disabled). See STATUS_DOCKER_RIG.md 'Follow-ups'."

test-all: up build-node ## the green matrix (LAN, relay, WS)
	@set -e; for t in test-lan test-relay test-ws; do \
	  echo "### $$t"; $(MAKE) $$t; done; echo "ALL GREEN RUNGS DONE"

# ── Mesh-hardening e2e (MESH_HARDENING_SPEC §6/§7/§10/§12) ────────────────────────────────────
# Real cyan_node containers + netem/chaos (pause=offline, network disconnect=partition, tc=latency)
# driven by tests/substrate_mesh_e2e.rs; asserts on the RECEIVER's storage. All gated by CYAN_RIG=1.
RUN_MESH = cd $(REPO_DIR) && CYAN_RIG=1 cargo test --test substrate_mesh_e2e
# The runnable (green-by-construction) scenarios — the two honest-red scaffolds (cross-net bootstrap
# discovery, super-peer offline-hold) are EXCLUDED here (they unimplemented!() pending real infra).
MESH_SCENARIOS = \
  lan_mesh_forms_and_live_delta_no_infra \
  partition_then_both_edit_then_heal_converges_via_delta \
  node_offline_then_reconnect_incremental_catchup \
  lens_down_mesh_and_sync_continue \
  bootstrap_down_lan_and_superpeer_still_work \
  all_infra_down_lan_sovereign_works \
  acceptance_crud_and_continuous_delta_sync \
  acceptance_chat_live \
  acceptance_presence_roster_persists_offline \
  acceptance_airgapped_import_baseline

mesh-e2e: up build-node ## §6/§10/§12 mesh e2e: all runnable scenarios (real containers + netem)
	@set -e; for t in $(MESH_SCENARIOS); do \
	  echo "### $$t"; $(RUN_MESH) $$t -- --ignored --nocapture --exact; done; \
	  echo "MESH E2E SCENARIOS DONE"

mesh-one: ## run ONE mesh scenario: make mesh-one T=all_infra_down_lan_sovereign_works
	$(RUN_MESH) $(T) -- --ignored --nocapture --exact

# ── Bootstrap cross-net discovery e2e (STATUS_BOOTSTRAP_DISCOVERY_E2E.md) ───────────────────────
# A REAL xaeroflux_bootstrap container introduces two cyan_node peers on ISOLATED bridges (no
# pre-shared peer addrs, mDNS not carried) → cross-net mesh; plus the §5 published-config rungs.
# Each runs ONE AT A TIME (shared container names). Needs both the node + bootstrap images.
BOOTSTRAP_SCENARIOS = \
  bootstrap_seeded_cross_net_mesh \
  discovery_via_published_config_forms_cross_net_mesh \
  tampered_published_config_rejected_peer_uses_fallback \
  bootstrap_redeploy_new_id_picked_up_no_app_change

bootstrap-e2e: build-node build-bootstrap ## §5 cross-net discovery via the live xaeroflux bootstrap
	@docker network create cyan-rig_mesh_a >/dev/null 2>&1 || true
	@docker network create cyan-rig_mesh_b >/dev/null 2>&1 || true
	@set -e; for t in $(BOOTSTRAP_SCENARIOS); do \
	  echo "### $$t"; $(RUN_MESH) $$t -- --ignored --nocapture --exact; done; \
	  echo "BOOTSTRAP DISCOVERY E2E DONE"

mesh-red: ## print the remaining honest-red scenario + why it needs real infra (not faked)
	@echo "offline_peer_message_held_by_superpeer_delivered_on_return — needs the headless-cyan"
	@echo "  Lens super-peer binary (cyan-lens hold/redeliver logic is fakes-only). See STATUS."
	@echo "(bootstrap_seeded_cross_net_mesh is now GREEN — see 'make bootstrap-e2e'.)"

stress: ## run a stress scenario: make stress SCENARIO=swarm PEERS=6  (see ./stress.sh help)
	./stress.sh $(SCENARIO) $(PEERS)

logs:  ## tail the relay
	$(COMPOSE) logs -f relay
ps:    ; $(COMPOSE) ps
down:  ## stop the relay
	$(COMPOSE) down
clean: down ## stop everything + remove networks/build context
	-docker rm -f cyan-rig-peer-a cyan-rig-peer-b cyan-rig-superpeer cyan-rig-bootstrap cyan-rig-config >/dev/null 2>&1 || true
	$(COMPOSE) down -v --remove-orphans
	-docker volume rm -f cyan-rig-rdv >/dev/null 2>&1 || true
	-docker network rm cyan-rig_airgap cyan-rig_mesh_a cyan-rig_mesh_b >/dev/null 2>&1 || true
	rm -rf .ctx .ctx-bootstrap .logs
.PHONY: help relay-info up build-node build-bootstrap test-lan test-relay test-ws test-offline \
        test-all mesh-e2e mesh-one mesh-red bootstrap-e2e stress logs ps down clean
