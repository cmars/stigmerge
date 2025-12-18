//! Integration tests for the Piece Lease Manager

use std::time::Instant;
use tokio::time::{sleep, Duration};
use veilid_core::{
    BareOpaqueRecordKey, BareRecordKey, BareSharedSecret, CryptoKind, RecordKey,
};

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
    let manager = PieceLeaseManager::new(config);

    // Set up wanted pieces
    let mut wanted = PieceMap::new();
    wanted.set(0);
    wanted.set(1);
    wanted.set(2);
    manager.set_wanted_pieces(wanted).await;

    // Create a lease request
    let peer_key = create_test_peer_key();
    let mut have_map = PieceMap::new();
    have_map.set(0);
    have_map.set(1);

    let request = LeaseRequest {
        remote_key: peer_key.clone(),
        have_map: &have_map,
        max_leases: 2,
        requested_at: Instant::now(),
    };

    // Request lease
    let response = manager
        .request_lease(&request)
        .await
        .expect("Lease request should succeed");

    assert!(response.granted_leases.len() > 0);

    // Check that leases were created
    assert_eq!(
        manager.active_leases_count().await,
        response.granted_leases.len()
    );
}

#[tokio::test]
async fn test_piece_completion_success() {
    let config = LeaseManagerConfig::default();
    let manager = PieceLeaseManager::new(config);

    // Set up wanted pieces
    let mut wanted = PieceMap::new();
    wanted.set(2); // Use different piece index to avoid conflicts
    manager.set_wanted_pieces(wanted).await;

    // Create a lease request
    let peer_key = create_test_peer_key();
    let mut have_map = PieceMap::new();
    have_map.set(2);

    let request = LeaseRequest {
        remote_key: peer_key.clone(),
        have_map: &have_map,
        max_leases: 1,
        requested_at: Instant::now(),
    };

    // Get lease
    let response = manager
        .request_lease(&request)
        .await
        .expect("Lease request should succeed");
    assert_eq!(response.granted_leases.len(), 1);

    let lease = &response.granted_leases[0];

    // Complete piece successfully
    manager
        .release_piece(lease.piece_index, CompletionResult::Success)
        .await
        .unwrap();

    // Check that piece is no longer wanted and lease is removed
    assert_eq!(manager.wanted_pieces_count().await, 0);
    assert_eq!(manager.active_leases_count().await, 0);

    // Check peer ranking
    let ranking = manager.get_peer_ranking(&peer_key).await.unwrap();
    assert_eq!(ranking.successful_pieces(), 1);
    assert_eq!(ranking.failed_pieces(), 0);
    assert!(ranking.score() > 1.0); // Should have increased
}

#[tokio::test]
async fn test_piece_completion_failure() {
    let config = LeaseManagerConfig::default();
    let manager = PieceLeaseManager::new(config);

    // Set up wanted pieces
    let mut wanted = PieceMap::new();
    wanted.set(1); // Use different piece index to avoid conflicts
    manager.set_wanted_pieces(wanted).await;

    // Create a lease request
    let peer_key = create_test_peer_key();
    let mut have_map = PieceMap::new();
    have_map.set(1);

    let request = LeaseRequest {
        remote_key: peer_key.clone(),
        have_map: &have_map,
        max_leases: 1,
        requested_at: Instant::now(),
    };

    // Get lease
    let response = manager.request_lease(&request).await.unwrap();
    assert_eq!(response.granted_leases.len(), 1);

    let lease = &response.granted_leases[0];

    // Complete piece with failure
    manager
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
    assert!(ranking.score() < 1.0); // Should have decreased
}

#[tokio::test]
async fn test_lease_expiry() {
    let mut config = LeaseManagerConfig::default();
    config.lease_duration = Duration::from_millis(100);
    config.cleanup_interval = Duration::from_millis(50);

    let manager = PieceLeaseManager::new(config);

    // Set up wanted pieces
    let mut wanted = PieceMap::new();
    wanted.set(0);
    manager.set_wanted_pieces(wanted).await;

    // Create a lease request
    let peer_key = create_test_peer_key();
    let mut have_map = PieceMap::new();
    have_map.set(0);

    let request = LeaseRequest {
        remote_key: peer_key.clone(),
        have_map: &have_map,
        max_leases: 1,
        requested_at: Instant::now(),
    };

    // Get lease
    let response = manager.request_lease(&request).await.unwrap();
    assert_eq!(response.granted_leases.len(), 1);

    // Wait for lease to expire
    sleep(Duration::from_millis(200)).await;

    // Trigger a cleanup by making a dummy request
    let dummy_request = LeaseRequest {
        remote_key: create_test_peer_key(),
        have_map: &PieceMap::new(),
        max_leases: 0,
        requested_at: Instant::now(),
    };
    let _ = manager.request_lease(&dummy_request).await;

    // Check that lease was cleaned up and piece is back in wanted set
    assert_eq!(manager.active_leases_count().await, 0);
    assert_eq!(manager.wanted_pieces_count().await, 1);

    // Check peer ranking
    let ranking = manager.get_peer_ranking(&peer_key).await.unwrap();
    assert_eq!(ranking.failed_pieces(), 1); // Should count as failure
    assert!(ranking.score() < 1.0); // Should have decreased
}

#[tokio::test]
async fn test_peer_capacity_limits() {
    let mut config = LeaseManagerConfig::default();
    config.max_leases_per_peer = 2;

    let manager = PieceLeaseManager::new(config);

    // Set up wanted pieces
    let mut wanted = PieceMap::new();
    wanted.set(0);
    wanted.set(1);
    wanted.set(2);
    manager.set_wanted_pieces(wanted).await;

    let peer_key = create_test_peer_key();
    let mut have_map = PieceMap::new();
    have_map.set(0);
    have_map.set(1);
    have_map.set(2);

    // First request - should get 2 leases (max for peer)
    let request1 = LeaseRequest {
        remote_key: peer_key.clone(),
        have_map: &have_map,
        max_leases: 3,
        requested_at: Instant::now(),
    };

    let response1 = manager
        .request_lease(&request1)
        .await
        .expect("lease request should succeed");
    assert_eq!(response1.granted_leases.len(), 2);

    // Second request - should be rejected due to capacity
    let request2 = LeaseRequest {
        remote_key: peer_key.clone(),
        have_map: &have_map,
        max_leases: 1,
        requested_at: Instant::now(),
    };

    let response2 = manager.request_lease(&request2).await;
    assert_eq!(
        response2.expect_err("should be at capacity"),
        Error::LeaseRejected(RejectedReason::PeerAtCapacity)
    );
}

#[tokio::test]
async fn test_stats_collection() {
    let config = LeaseManagerConfig::default();
    let manager = PieceLeaseManager::new(config);

    // Set up some state
    let mut wanted = PieceMap::new();
    wanted.set(3); // Use different piece index to avoid conflicts
    wanted.set(4);
    manager.set_wanted_pieces(wanted).await;

    let peer_key = create_test_peer_key();
    let mut have_map = PieceMap::new();
    have_map.set(3);

    let request = LeaseRequest {
        remote_key: peer_key.clone(),
        have_map: &have_map,
        max_leases: 1,
        requested_at: Instant::now(),
    };

    let response = manager.request_lease(&request).await.unwrap();

    // Complete the piece
    let lease = &response.granted_leases[0];
    manager
        .release_piece(lease.piece_index, CompletionResult::Success)
        .await
        .unwrap();

    // Get stats
    let stats = manager.get_stats().await;
    assert_eq!(stats.wanted_pieces_count, 1); // One piece still wanted
    assert_eq!(stats.active_leases_count, 0); // Lease completed
    assert_eq!(stats.peer_count, 1); // One peer
    assert_eq!(stats.total_successful_pieces, 1); // One success
    assert_eq!(stats.total_failed_pieces, 0); // No failures
}
