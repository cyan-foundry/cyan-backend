//! Substrate G10 — resilience / swarming (SUBSTRATE_TEST_SPEC §3).
//!
//! These drive the next build: resume a partial transfer, fetch from multiple sources,
//! survive peer churn mid-transfer, and negotiate a holder via i-have/who-has. The engine
//! does not yet implement multi-source / who-has swarming, so these are scaffolded red
//! (`#[ignore]` + `unimplemented!()`) — the names are the contract for that work.

#![allow(unused)]

/// G10: a transfer interrupted mid-stream resumes from a byte offset and verifies intact.
#[ignore = "swarming/resume not yet implemented in the engine"]
#[tokio::test]
async fn partial_transfer_resumes_from_offset() {
    unimplemented!("resume from offset (ties into the relay resume path); drives G10");
}

/// G10: a file is fetched from two sources in parallel and reassembled intact.
#[ignore = "multi-source fetch not yet implemented in the engine"]
#[tokio::test]
async fn file_fetched_from_two_sources_in_parallel() {
    unimplemented!("parallel multi-source fetch; drives G10");
}

/// G10: a transfer survives the source peer leaving mid-stream (recovers or fails cleanly).
#[ignore = "source churn handling not yet implemented in the engine"]
#[tokio::test]
async fn transfer_survives_source_peer_churn() {
    unimplemented!("source peer churn mid-transfer; drives G10");
}

/// G10: i-have/who-has negotiation picks a holder among candidates.
#[ignore = "i-have/who-has negotiation not yet implemented in the engine"]
#[tokio::test]
async fn i_have_who_has_negotiation_picks_a_holder() {
    unimplemented!("i-have/who-has holder selection; drives G10");
}
