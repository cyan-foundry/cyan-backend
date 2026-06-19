#!/bin/sh
# WebSocket-only rung: black-hole outbound UDP so neither direct QUIC nor QUIC-to-relay
# can work — iroh-relay must then carry packets over its HTTP/WebSocket (TCP) transport.
# Requires `--cap-add NET_ADMIN`.
#
# DNS (53/udp) is kept so name resolution still works; every other outbound UDP is dropped.
# Then exec cyan_node so it becomes PID1 and inherits the container's stdin/stdout (the
# control line-protocol channel the rig drives it over).
set -e
iptables -A OUTPUT -p udp --dport 53 -j ACCEPT
iptables -A OUTPUT -p udp -j DROP
echo "[ws-entrypoint] outbound UDP dropped (except DNS); forcing WebSocket relay" >&2
exec /usr/local/bin/cyan_node
