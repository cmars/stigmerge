//! Integration tests for the Piece Lease Manager

use tokio::time::Duration;
use veilid_core::{BareOpaqueRecordKey, BareRecordKey, BareSharedSecret, CryptoKind, RecordKey};

use crate::piece_leases::*;
use crate::piece_map::PieceMap;

fn create_test_peer_key() -> RecordKey {
    RecordKey::new(
        CryptoKind::default(),
        BareRecordKey::new(
            BareOpaqueRecordKey::new(&[0xaa; 32]),
            Some(BareSharedSecret::new(&[0xaa; 32])),
        ),
    )
}

#[tokio::test]
async fn test_lease_request_and_grant() {
    let config = LeaseManagerConfig::default();
    let mut manager = PieceLeaseManager::new(config);

    // Set up wanted pieces
    let mut wanted = PieceMap::new();
    wanted.set(0);
    wanted.set(1);
    wanted.set(2);
    manager.set_wanted_pieces(&wanted).await;

    // Create a lease request
    let peer_key = create_test_peer_key();
    let mut have_map = PieceMap::new();
    have_map.set(1);
    manager.add_peer(&peer_key, &have_map).await;

    // Request lease
    let lease = manager
        .acquire_lease(&peer_key)
        .await
        .expect("Lease request should succeed");

    // Should be piece 1
    assert_eq!(lease.piece_index, 1);
}

#[tokio::test]
async fn test_piece_completion_success() {
    let config = LeaseManagerConfig::default();
    let mut manager = PieceLeaseManager::new(config);

    // Set up wanted pieces
    let mut wanted = PieceMap::new();
    wanted.set(2); // Use different piece index to avoid conflicts
    manager.set_wanted_pieces(&wanted).await;

    // Create a lease request
    let peer_key = create_test_peer_key();
    let mut have_map = PieceMap::new();
    have_map.set(2);
    manager.add_peer(&peer_key, &have_map).await;

    // Get lease
    let lease = manager
        .acquire_lease(&peer_key)
        .await
        .expect("Lease request should succeed");

    // Complete piece successfully
    let score = manager
        .release_piece(lease.piece_index, CompletionResult::Success)
        .await
        .unwrap();

    // Check peer ranking
    assert!(score > 1.0); // Should have increased
}

#[tokio::test]
async fn test_piece_completion_failure() {
    let config = LeaseManagerConfig::default();
    let mut manager = PieceLeaseManager::new(config);

    // Set up wanted pieces
    let mut wanted = PieceMap::new();
    wanted.set(1); // Use different piece index to avoid conflicts
    manager.set_wanted_pieces(&wanted).await;

    // Create a lease request
    let peer_key = create_test_peer_key();
    let mut have_map = PieceMap::new();
    have_map.set(1);
    manager.add_peer(&peer_key, &have_map).await;

    // Get lease
    let lease = manager.acquire_lease(&peer_key).await.unwrap();

    // Complete piece with failure
    let score = manager
        .release_piece(
            lease.piece_index,
            CompletionResult::Failure(FailureReason::VerificationFailed),
        )
        .await
        .unwrap();

    // Check that piece is still wanted and lease is removed
    assert_eq!(manager.wanted_pieces_count().await, 1);
    assert_eq!(manager.active_leases_count().await, 0);

    // Check peer ranking
    let ranking = manager.get_peer_ranking(&peer_key).await.unwrap();
    assert_eq!(ranking.successful_pieces(), 0);
    assert_eq!(ranking.failed_pieces(), 1);
    assert!(score < 1.0); // Should have decreased
}

#[tokio::test]
async fn test_lease_expiry() {
    let mut config = LeaseManagerConfig::default();
    config.lease_duration = Duration::from_millis(1);
    config.cleanup_interval = Duration::from_millis(50);

    let mut manager = PieceLeaseManager::new(config);

    // Set up wanted pieces
    let mut wanted = PieceMap::new();
    wanted.set(0);
    manager.set_wanted_pieces(&wanted).await;

    // Create a lease request
    let peer_key = create_test_peer_key();
    let mut have_map = PieceMap::new();
    have_map.set(0);
    manager.add_peer(&peer_key, &have_map).await;

    // Get lease
    let lease = manager.acquire_lease(&peer_key).await.unwrap();

    manager
        .release_piece(
            lease.piece_index,
            CompletionResult::Failure(FailureReason::Timeout),
        )
        .await
        .unwrap();

    // Check that lease was cleaned up and piece is back in wanted set
    assert_eq!(manager.active_leases_count().await, 0);
    assert_eq!(manager.wanted_pieces_count().await, 1);

    // Check peer ranking
    let ranking = manager.get_peer_ranking(&peer_key).await.unwrap();
    assert_eq!(ranking.failed_pieces(), 1); // Should count as failure
    assert!(ranking.score() < 1.0); // Should have decreased
}
