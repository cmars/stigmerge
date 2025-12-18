//! Standalone demonstration of the Piece Lease Manager
//! Run with: cargo run --example lease_manager_demo

use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;
use veilid_core::{
    BareOpaqueRecordKey, BareRecordKey, BareSharedSecret, CryptoKind, RecordKey,
};

use stigmerge_peer::piece_leases::*;
use stigmerge_peer::piece_map::PieceMap;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!("Starting Piece Lease Manager demonstration");

    // Create configuration
    let config = LeaseManagerConfig {
        lease_duration: Duration::from_secs(30),
        max_leases_per_peer: 3,
        peer_ranking_decay: 0.9,
        success_bonus: 0.4,
        failure_penalty: 0.7,
        cleanup_interval: Duration::from_secs(10),
        min_peer_score: 0.2,
    };

    let manager = PieceLeaseManager::new(config.clone());
    info!("Created lease manager with configuration: {:?}", config);

    // Set up wanted pieces (simulating a file with 20 pieces)
    let mut wanted = PieceMap::new();
    for i in 0..20 {
        wanted.set(i as u32);
    }
    manager.set_wanted_pieces(wanted).await;
    info!(
        "Set up {} wanted pieces",
        manager.wanted_pieces_count().await
    );

    // Create 3 peers with different capabilities
    let peer1 = RecordKey::new(
        CryptoKind::default(),
        BareRecordKey::new(
            BareOpaqueRecordKey::new(&[0x11; 32]),
            Some(BareSharedSecret::new(&[0x11; 32])),
        ),
    );
    let peer2 = RecordKey::new(
        CryptoKind::default(),
        BareRecordKey::new(
            BareOpaqueRecordKey::new(&[0x22; 32]),
            Some(BareSharedSecret::new(&[0x22; 32])),
        ),
    );
    let peer3 = RecordKey::new(
        CryptoKind::default(),
        BareRecordKey::new(
            BareOpaqueRecordKey::new(&[0x33; 32]),
            Some(BareSharedSecret::new(&[0x33; 32])),
        ),
    );

    // Set up what pieces each peer has
    let mut have1 = PieceMap::new();
    let mut have2 = PieceMap::new();
    let mut have3 = PieceMap::new();

    // Peer 1 has pieces 0-6
    for i in 0..7 {
        have1.set(i as u32);
    }

    // Peer 2 has pieces 5-12
    for i in 5..13 {
        have2.set(i as u32);
    }

    // Peer 3 has pieces 10-19
    for i in 10..20 {
        have3.set(i as u32);
    }

    info!("Peer 1 has {} pieces", have1.iter().count());
    info!("Peer 2 has {} pieces", have2.iter().count());
    info!("Peer 3 has {} pieces", have3.iter().count());

    // Show initial state
    let initial_stats = manager.get_stats().await;
    info!("Initial stats: {:?}", initial_stats);

    // Peer 1 requests leases
    info!("\n=== Peer 1 requesting leases ===");
    let request1 = LeaseRequest {
        remote_key: peer1.clone(),
        have_map: &have1,
        max_leases: 3,
        requested_at: Instant::now(),
    };

    let response1 = manager.request_lease(&request1).await?;
    info!("Peer 1 got {} leases", response1.granted_leases.len());
    for lease in &response1.granted_leases {
        info!("  Lease for piece {}", lease.piece_index());
    }

    // Peer 2 requests leases
    info!("\n=== Peer 2 requesting leases ===");
    let request2 = LeaseRequest {
        remote_key: peer2.clone(),
        have_map: &have2,
        max_leases: 2,
        requested_at: Instant::now(),
    };

    let response2 = manager.request_lease(&request2).await?;
    info!("Peer 2 got {} leases", response2.granted_leases.len());
    for lease in &response2.granted_leases {
        info!("  Lease for piece {}", lease.piece_index());
    }

    // Peer 3 requests leases
    info!("\n=== Peer 3 requesting leases ===");
    let request3 = LeaseRequest {
        remote_key: peer3.clone(),
        have_map: &have3,
        max_leases: 2,
        requested_at: Instant::now(),
    };

    let response3 = manager.request_lease(&request3).await?;
    info!("Peer 3 got {} leases", response3.granted_leases.len());
    for lease in &response3.granted_leases {
        info!("  Lease for piece {}", lease.piece_index());
    }

    // Show state after all leases granted
    let after_lease_stats = manager.get_stats().await;
    info!("\nStats after lease allocation: {:?}", after_lease_stats);

    // Simulate Peer 1 successfully completing all its pieces
    info!("\n=== Peer 1 completing pieces successfully ===");
    for lease in response1.granted_leases {
        manager
            .release_piece(lease.piece_index(), CompletionResult::Success)
            .await?;
        info!(
            "  Peer 1 completed piece {} successfully",
            lease.piece_index()
        );
    }

    // Simulate Peer 2 failing all its pieces
    info!("\n=== Peer 2 failing pieces ===");
    for lease in response2.granted_leases {
        manager
            .release_piece(
                lease.piece_index(),
                CompletionResult::Failure(FailureReason::VerificationFailed),
            )
            .await?;
        info!("  Peer 2 failed piece {}", lease.piece_index());
    }

    // Wait a bit for cleanup
    sleep(Duration::from_millis(100)).await;

    // Show final state
    let final_stats = manager.get_stats().await;
    info!("\nFinal stats: {:?}", final_stats);

    // Show peer rankings
    let ranking1 = manager.get_peer_ranking(&peer1).await;
    let ranking2 = manager.get_peer_ranking(&peer2).await;
    let ranking3 = manager.get_peer_ranking(&peer3).await;

    info!("\n=== Peer Rankings ===");
    if let Some(ranking) = ranking1 {
        info!(
            "Peer 1: score={:.2}, success={}, failed={}, current_leases={}",
            ranking.score(),
            ranking.successful_pieces(),
            ranking.failed_pieces(),
            ranking.current_leases()
        );
    }
    if let Some(ranking) = ranking2 {
        info!(
            "Peer 2: score={:.2}, success={}, failed={}, current_leases={}",
            ranking.score(),
            ranking.successful_pieces(),
            ranking.failed_pieces(),
            ranking.current_leases()
        );
    }
    if let Some(ranking) = ranking3 {
        info!(
            "Peer 3: score={:.2}, success={}, failed={}, current_leases={}",
            ranking.score(),
            ranking.successful_pieces(),
            ranking.failed_pieces(),
            ranking.current_leases()
        );
    }

    // Test lease expiry
    info!("\n=== Testing lease expiry ===");

    // Peer 3 still has active leases, let them expire
    info!("Waiting for Peer 3 leases to expire...");
    sleep(Duration::from_secs(35)).await; // Wait longer than lease_duration

    let expiry_stats = manager.get_stats().await;
    info!("Stats after lease expiry: {:?}", expiry_stats);

    let ranking3_after = manager.get_peer_ranking(&peer3).await;
    if let Some(ranking) = ranking3_after {
        info!(
            "Peer 3 after expiry: score={:.2}, success={}, failed={}, current_leases={}",
            ranking.score(),
            ranking.successful_pieces(),
            ranking.failed_pieces(),
            ranking.current_leases()
        );
    }

    // Test capacity limits
    info!("\n=== Testing capacity limits ===");

    // Peer 1 tries to get more leases (should be limited)
    let request1_more = LeaseRequest {
        remote_key: peer1.clone(),
        have_map: &have1,
        max_leases: 5,
        requested_at: Instant::now(),
    };

    let response1_more = manager.request_lease(&request1_more).await;
    info!(
        "Peer 1 requested more leases, got error: {:?}",
        response1_more.err(),
    );

    info!("\n=== Demo Complete ===");
    info!("Piece Lease Manager demonstration completed successfully!");
    info!("Key features demonstrated:");
    info!("  ✓ Lease assignment and management");
    info!("  ✓ Peer ranking with success/failure tracking");
    info!("  ✓ Capacity limits per peer");
    info!("  ✓ Automatic lease expiry and cleanup");
    info!("  ✓ Piece exclusivity (no concurrent leases for same piece)");

    Ok(())
}
