#!/bin/sh
# WebSocket-only rung: black-hole outbound UDP so neither direct QUIC nor QUIC-to-relay
# can work — iroh-relay must then carry packets over its HTTP/WebSocket (TCP) transport.
# Requires `--cap-add NET_ADMIN`.
#
# DNS (53/udp) is kept so name resolution still works; every other outbound UDP is dropped.
# Then exec cyan_node so it becomes PID1 and inherits the container's stdin/stdout (the
# control line-protocol channel the rig drives it over).
set -e
# Keep DNS working: Docker's embedded resolver (127.0.0.11) NATs the :53 query to a high
# port, so a bare `--dport 53` rule misses it — allow loopback + the resolver address
# explicitly. Everything else outbound-UDP (QUIC direct + QUIC-to-relay) is dropped, so
# iroh-relay must carry traffic over its HTTP/WebSocket (TCP) transport.
iptables -A OUTPUT -o lo -j ACCEPT
iptables -A OUTPUT -d 127.0.0.11 -j ACCEPT
iptables -A OUTPUT -p udp --dport 53 -j ACCEPT
iptables -A OUTPUT -p udp -j DROP
echo "[ws-entrypoint] outbound UDP dropped (DNS preserved); forcing WebSocket relay" >&2
exec /usr/local/bin/cyan_node
