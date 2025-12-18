//! Examples and performance tests for the Piece Lease Manager

use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::info;
use veilid_core::{
    BareOpaqueRecordKey, BareRecordKey, BareSharedSecret, CryptoKind, RecordKey,
};

use crate::piece_leases::*;
use crate::piece_map::PieceMap;

/// Example usage of the PieceLeaseManager
pub async fn example_usage() -> Result<()> {
    // Create configuration
    let config = LeaseManagerConfig {
        lease_duration: Duration::from_secs(60),
        max_leases_per_peer: 3,
        peer_ranking_decay: 0.9,
        success_bonus: 0.3,
        failure_penalty: 0.8,
        cleanup_interval: Duration::from_secs(30),
        min_peer_score: 0.1,
    };

    let manager = PieceLeaseManager::new(config);

    // Set up wanted pieces
    let mut wanted = PieceMap::new();
    for i in 0..10 {
        wanted.set(i as u32);
    }
    manager.set_wanted_pieces(wanted).await;
    info!(
        "Set up {} wanted pieces",
        manager.wanted_pieces_count().await
    );

    // Simulate multiple peers requesting leases
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

    // Set up peer have maps
    let mut have1 = PieceMap::new();
    let mut have2 = PieceMap::new();
    let mut have3 = PieceMap::new();

    for i in 0..5 {
        have1.set(i as u32);
    }
    for i in 3..8 {
        have2.set(i as u32);
    }
    for i in 5..10 {
        have3.set(i as u32);
    }

    // Peer 1 requests leases
    let request1 = LeaseRequest {
        remote_key: peer1.clone(),
        have_map: &have1,
        max_leases: 3,
        requested_at: Instant::now(),
    };

    let response1 = manager.request_lease(&request1).await?;
    info!("Peer 1 got {} leases", response1.granted_leases.len());

    // Peer 2 requests leases
    let request2 = LeaseRequest {
        remote_key: peer2.clone(),
        have_map: &have2,
        max_leases: 2,
        requested_at: Instant::now(),
    };

    let response2 = manager.request_lease(&request2).await?;
    info!("Peer 2 got {} leases", response2.granted_leases.len());

    // Peer 3 requests leases
    let request3 = LeaseRequest {
        remote_key: peer3.clone(),
        have_map: &have3,
        max_leases: 2,
        requested_at: Instant::now(),
    };

    let response3 = manager.request_lease(&request3).await?;
    info!("Peer 3 got {} leases", response3.granted_leases.len());

    // Show initial stats
    let stats = manager.get_stats().await;
    info!("Initial stats: {:?}", stats);

    // Simulate some completions
    for lease in response1.granted_leases {
        manager.release_piece(lease.piece_index, CompletionResult::Success).await?;
    }

    for lease in response2.granted_leases {
        manager
            .release_piece(
                lease.piece_index,
                CompletionResult::Failure(FailureReason::NetworkError),
            )
            .await?;
    }

    // Wait a bit and show final stats
    sleep(Duration::from_millis(100)).await;

    let final_stats = manager.get_stats().await;
    info!("Final stats: {:?}", final_stats);

    // Show peer rankings
    let ranking1 = manager.get_peer_ranking(&peer1).await.unwrap();
    let ranking2 = manager.get_peer_ranking(&peer2).await.unwrap();
    let ranking3 = manager.get_peer_ranking(&peer3).await.unwrap();

    info!("Peer 1 ranking: {:?}", ranking1);
    info!("Peer 2 ranking: {:?}", ranking2);
    info!("Peer 3 ranking: {:?}", ranking3);

    Ok(())
}

/// Performance test for the lease manager
pub async fn performance_test(num_peers: usize, num_pieces: usize) -> Result<()> {
    let start = Instant::now();

    let config = LeaseManagerConfig {
        lease_duration: Duration::from_secs(30),
        max_leases_per_peer: 4,
        peer_ranking_decay: 0.95,
        success_bonus: 0.5,
        failure_penalty: 1.0,
        cleanup_interval: Duration::from_secs(10),
        min_peer_score: 0.1,
    };

    let manager = PieceLeaseManager::new(config);

    // Set up wanted pieces
    let mut wanted = PieceMap::new();
    for i in 0..num_pieces {
        wanted.set(i as u32);
    }
    manager.set_wanted_pieces(wanted).await;

    info!(
        "Performance test: {} peers, {} pieces",
        num_peers, num_pieces
    );

    // Create peers and have maps
    let mut peers = Vec::new();
    let mut have_maps = Vec::new();

    for peer_id in 0..num_peers {
        let peer_key = RecordKey::new(
            CryptoKind::default(),
            BareRecordKey::new(
                BareOpaqueRecordKey::new(&[peer_id as u8; 32]),
                Some(BareSharedSecret::new(&[peer_id as u8; 32])),
            ),
        );
        peers.push(peer_key);

        // Each peer has a different subset of pieces
        let mut have_map = PieceMap::new();
        for i in (peer_id * num_pieces / num_peers)..((peer_id + 1) * num_pieces / num_peers) {
            have_map.set((i % num_pieces) as u32);
        }
        have_maps.push(have_map);
    }

    // All peers request leases (simplified for demo)
    for (peer_key, have_map) in peers.into_iter().zip(have_maps.into_iter()) {
        let request = LeaseRequest {
            remote_key: peer_key,
            have_map: &have_map,
            max_leases: 2,
            requested_at: Instant::now(),
        };

        // In real usage, this would be: manager.request_lease(request).await
        info!(
            "Peer would request leases for {} pieces",
            request.have_map.iter().count()
        );
    }

    let setup_time = start.elapsed();
    info!("Setup completed in {:?}", setup_time);

    // Get final stats
    let stats = manager.get_stats().await;
    info!("Performance test stats: {:?}", stats);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_example_usage() {
        example_usage().await.expect("Example usage should work");
    }

    #[tokio::test]
    async fn test_performance_small() {
        performance_test(5, 20)
            .await
            .expect("Small performance test should work");
    }

    #[tokio::test]
    async fn test_performance_medium() {
        performance_test(10, 50)
            .await
            .expect("Medium performance test should work");
    }
}
