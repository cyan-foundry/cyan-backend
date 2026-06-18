//! Substrate G2 ladder / G8-R / G11 — the PAID relay path (SUBSTRATE_TEST_SPEC §3, §8).
//!
//! These rungs are **NOT in-process**: to prove traffic took the relay (and the
//! WebSocket-only relay) the test must run a local `iroh-relay` fixture AND black-hole
//! direct UDP between peers (and, for WebSocket-only, the relay's UDP too). On a single
//! host that needs Linux network namespaces or the docker-compose two-node rig in
//! `cyan-local-harness/` — not a plain in-process spawn. They are scaffolded red here so
//! the names are the contract; implement them against the netns/CI rig with a `RelayFixture`
//! + `net_isolate` helper. The G11 relayed-byte meter is the oracle that proves the rung
//! was really used (relayed bytes > 0 over relay, == 0 over a direct transfer).

#![allow(unused)]

/// G2 ladder: UDP between peers black-holed; connection still forms through the local
/// relay; assert the connection type is relayed (not direct).
#[ignore = "needs RelayFixture + UDP black-hole (netns/docker rig), not in-process"]
#[tokio::test]
async fn connects_via_relay_when_direct_blocked() {
    unimplemented!("RelayFixture + net_isolate: relay-only path; see SUBSTRATE_TEST_SPEC §1/§8");
}

/// G2 worst rung: the relay's UDP is also blocked; assert the relay carries traffic over
/// its HTTP/WebSocket transport and the peers still talk.
#[ignore = "needs WebSocket-only relay rung (block relay UDP); netns/docker rig only"]
#[tokio::test]
async fn connects_via_websocket_when_udp_fully_blocked() {
    unimplemented!("RelayFixture with UDP fully blocked → relay-over-WebSocket; §1/§8");
}

/// G8-R: a 100 MB file completes intact end-to-end over the relay-only path.
#[ignore = "needs RelayFixture + RelayOnly isolation; netns/docker rig only"]
#[tokio::test]
async fn large_file_100mb_over_relay_intact() {
    unimplemented!("RelayOnly 100MB blake3 round-trip over the local relay; §3/§8");
}

/// G8-R: the same large transfer over the WebSocket-only rung (worst case must complete).
#[ignore = "needs WebsocketOnly rung; netns/docker rig only"]
#[tokio::test]
async fn large_file_over_websocket_relay_intact() {
    unimplemented!("WebsocketOnly large-file round-trip; §3/§8");
}

/// G8-R SLA: measure MB/s on the relay rung and assert ≥ an agreed (lower) floor.
#[ignore = "needs RelayFixture throughput measurement; netns/docker rig only"]
#[tokio::test]
async fn relay_path_meets_relay_throughput_floor() {
    unimplemented!("measure relay-rung throughput; the SLA we tune toward; §3/§8");
}

/// G11: after a relay transfer the per-(tenant,transfer) relayed-byte counter is > 0 and
/// equals the payload (±framing); after a *direct* transfer it is 0. The billing rail AND
/// the oracle that proves the rung was really used.
#[ignore = "needs the relayed-byte meter + RelayFixture; netns/docker rig only"]
#[tokio::test]
async fn relayed_bytes_are_metered() {
    unimplemented!("assert relayed-byte meter > 0 over relay, == 0 over direct; §3/§8");
}
