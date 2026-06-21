//! Substrate — §6 tenant/group ISOLATION (no bleed) on the storage layer.
//!
//! SUPER_PEER_COMPLETION_SPEC §6 requires every mesh-state path to be strictly board/group/tenant
//! scoped — nothing bleeds across a board, group, or tenant. The R11 chat-bleed fix proved the
//! pattern; these are the regression guards for the REST of the mesh state the audit covered:
//! files (board+group), board-state (pins, group-scoped via the board→workspace→group chain), and
//! presence (the persisted roster, tenant=group scoped).
//!
//! Storage-level oracle: the engine DB is process-global, so these assert directly on `storage::*`
//! (the same convergence oracle the rest of the substrate suite uses). Unique ids per test keep
//! concurrently-running tests in this binary from colliding. DO NOT weaken assertions.

mod support;

use cyan_backend::models::dto::PinDTO;
use cyan_backend::storage;

/// Build a group → workspace → board chain and return `(group_id, workspace_id, board_id)`.
fn make_board(tag: &str, board_suffix: &str) -> (String, String, String) {
    let group = format!("{}-{}", support::unique_group_id(), tag);
    let ws = format!("{group}-ws");
    let board = format!("{group}-board-{board_suffix}");
    storage::group_insert_simple(&group, "Iso", "folder.fill", "#00AEEF").expect("group");
    storage::workspace_insert_simple(&ws, &group, "Main").expect("workspace");
    storage::board_insert_simple(&board, &ws, "Canvas", 1).expect("board");
    (group, ws, board)
}

/// Files are scoped by BOTH board and group: a file in board B1 never shows up in board B2's list,
/// and a file in group G2 never shows up in group G1's list. (§6 — files keyed by board/group.)
#[test]
fn files_board_scoped() {
    support::ensure_db();
    let (g1, ws1, b1) = make_board("fa", "1");
    // A second board in the SAME group, and a whole separate group.
    let b2 = format!("{g1}-board-2");
    storage::board_insert_simple(&b2, &ws1, "Canvas2", 1).expect("board2");
    let (g2, ws2, b3) = make_board("fb", "1");

    let f1 = format!("{b1}-file");
    let f2 = format!("{b2}-file");
    let f3 = format!("{b3}-file");
    storage::file_insert_simple(&f1, Some(&g1), Some(&ws1), Some(&b1), "a.pdf", "h1", 10, None, 1)
        .expect("f1");
    storage::file_insert_simple(&f2, Some(&g1), Some(&ws1), Some(&b2), "b.pdf", "h2", 10, None, 1)
        .expect("f2");
    storage::file_insert_simple(&f3, Some(&g2), Some(&ws2), Some(&b3), "c.pdf", "h3", 10, None, 1)
        .expect("f3");

    // Board scope: B1's listing is exactly [f1] — B2's file never bleeds in.
    let by_b1: Vec<String> = storage::file_list_by_board(&b1)
        .expect("list b1")
        .into_iter()
        .map(|f| f.id)
        .collect();
    assert_eq!(by_b1, vec![f1.clone()], "board B1 lists only its own file");

    // Group scope: G1 has f1 + f2; G2's file is never in G1's listing, and vice-versa.
    let mut by_g1: Vec<String> = storage::file_list_by_group(&g1)
        .expect("list g1")
        .into_iter()
        .map(|f| f.id)
        .collect();
    by_g1.sort();
    let mut expect_g1 = vec![f1.clone(), f2.clone()];
    expect_g1.sort();
    assert_eq!(by_g1, expect_g1, "group G1 lists exactly its two files");
    assert!(
        !by_g1.contains(&f3),
        "G2's file must NOT appear in G1's listing (no cross-group bleed)"
    );

    let by_g2: Vec<String> = storage::file_list_by_group(&g2)
        .expect("list g2")
        .into_iter()
        .map(|f| f.id)
        .collect();
    assert_eq!(by_g2, vec![f3], "group G2 lists exactly its own file");
}

/// Board-state (pins) is group-scoped: a pin on a board in G1 resolves to G1 (never G2) and is
/// listed only under G1's boards. (§6 — board-state keyed by board, which belongs to one group.)
#[test]
fn board_state_group_scoped() {
    support::ensure_db();
    let (g1, _ws1, b1) = make_board("pa", "1");
    let (g2, _ws2, b2) = make_board("pb", "1");

    // Pin the board in G1 only.
    storage::pin_upsert(&PinDTO {
        board_id: b1.clone(),
        tenant_id: g1.clone(),
        pinned: true,
        updated_at: 5,
    })
    .expect("pin b1");

    // The board→group chain places b1 in G1 and b2 in G2 — never crossed.
    assert_eq!(
        storage::board_get_group_id(&b1).as_deref(),
        Some(g1.as_str()),
        "board b1 resolves to group G1"
    );
    assert_eq!(
        storage::board_get_group_id(&b2).as_deref(),
        Some(g2.as_str()),
        "board b2 resolves to group G2"
    );

    // The pin is visible under G1's board, and absent under G2's board.
    let g1_pins: Vec<(String, bool)> = storage::pin_list_by_boards(std::slice::from_ref(&b1))
        .expect("pins b1")
        .into_iter()
        .map(|p| (p.board_id, p.pinned))
        .collect();
    assert_eq!(g1_pins, vec![(b1.clone(), true)], "G1's board carries the pin");

    let g2_pins = storage::pin_list_by_boards(std::slice::from_ref(&b2)).expect("pins b2");
    assert!(
        g2_pins.iter().all(|p| !p.pinned),
        "G2's board has no pin set from G1 (no cross-group board-state bleed)"
    );
}

/// Presence (the persisted roster) is tenant=group scoped: a member seen in G1 appears ONLY in
/// G1's roster, never in another group's. (§6 — presence keyed per-group.)
#[test]
fn presence_tenant_scoped() {
    support::ensure_db();
    let g1 = format!("{}-presa", support::unique_group_id());
    let g2 = format!("{}-presb", support::unique_group_id());
    let peer_a = "aaaa0000aaaa0000aaaa0000aaaa0000";
    let peer_b = "bbbb1111bbbb1111bbbb1111bbbb1111";

    storage::member_seen(&g1, peer_a, 100).expect("seen a in g1");
    storage::member_seen(&g2, peer_b, 100).expect("seen b in g2");

    let g1_members: Vec<String> = storage::group_members_list(&g1)
        .into_iter()
        .map(|(peer, _, _, _)| peer)
        .collect();
    let g2_members: Vec<String> = storage::group_members_list(&g2)
        .into_iter()
        .map(|(peer, _, _, _)| peer)
        .collect();

    assert_eq!(g1_members, vec![peer_a.to_string()], "G1's roster has only peer A");
    assert_eq!(g2_members, vec![peer_b.to_string()], "G2's roster has only peer B");
    assert!(
        !g1_members.contains(&peer_b.to_string()),
        "peer B (seen only in G2) must NOT appear in G1's roster (no cross-tenant presence bleed)"
    );
}
