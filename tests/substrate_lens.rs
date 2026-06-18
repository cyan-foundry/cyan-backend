//! Substrate §4 — Lens optionality (SUBSTRATE_TEST_SPEC §4).
//!
//! Lens is NOT a mesh peer — cyan-backend talks to it as a plain HTTP client
//! (`src/cyan_lens_client.rs`, reqwest → Lens REST). The only substrate-relevant property
//! is **optionality**: the mesh must work fully with Lens unreachable (offline-first).
//!
//! Scaffolded `#[ignore]` per the run plan (needs the lens-client wiring): point
//! `CYAN_LENS_URL` at a dead port, assert `CyanLensClient::is_available()` is `false`, and
//! re-run the G1/G4/G5/G6 slice (which the in-process suite already covers) asserting every
//! behavior still passes with Lens down. Lens being down must never block P2P.

#![allow(unused)]

/// The one optionality test that ships: full mesh behavior with Lens unreachable.
#[ignore = "needs CyanLensClient wiring (CYAN_LENS_URL → dead port + is_available() == false)"]
#[tokio::test]
async fn mesh_fully_functional_with_lens_unreachable() {
    unimplemented!(
        "point CYAN_LENS_URL at a dead port, assert CyanLensClient::is_available() == false, \
         then run discovery+delta+chat+file (all already green here) and assert they still pass"
    );
}
