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

logs:  ## tail the relay
	$(COMPOSE) logs -f relay
ps:    ; $(COMPOSE) ps
down:  ## stop the relay
	$(COMPOSE) down
clean: down ## stop everything + remove networks/build context
	-docker rm -f cyan-rig-peer-a cyan-rig-peer-b >/dev/null 2>&1 || true
	$(COMPOSE) down -v --remove-orphans
	-docker network rm cyan-rig_airgap >/dev/null 2>&1 || true
	rm -rf .ctx .logs
.PHONY: help relay-info up build-node test-lan test-relay test-ws test-offline test-all logs ps down clean
