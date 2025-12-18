//! Core Piece Lease Manager functionality

use std::{collections::HashMap, sync::Arc, time::Instant};

use tokio::{sync::Mutex, task::AbortHandle};
use tracing::{debug, info, trace, warn};
use veilid_core::RecordKey;

use crate::piece_map::PieceMap;

use super::error::{Error, Result};
use super::{peer_ranking::PeerRanking, types::*};

/// Inner state for the PieceLeaseManager
struct Inner {
    /// Configuration for the lease manager
    config: LeaseManagerConfig,

    /// Pieces that still need to be downloaded
    wanted_pieces: PieceMap,

    /// Currently active leases by piece_index
    active_leases: HashMap<usize, PieceLease>,

    /// Peer performance rankings
    peer_rankings: HashMap<RecordKey, PeerRanking>,

    /// Abort handles for running tasks operating on the leased piece
    lease_handles: HashMap<usize, Vec<AbortHandle>>,

    /// Counter for generating unique request IDs
    request_id_counter: u64,
}

impl Inner {
    pub fn new(config: LeaseManagerConfig) -> Self {
        Self {
            config,
            wanted_pieces: PieceMap::new(),
            active_leases: HashMap::new(),
            peer_rankings: HashMap::new(),
            lease_handles: HashMap::new(),
            request_id_counter: 0,
        }
    }

    /// Clean up expired leases and return them to the wanted set
    #[tracing::instrument(skip_all)]
    pub async fn cleanup_expired_leases(&mut self) {
        let now = Instant::now();
        let expired_leases: Vec<_> = self
            .active_leases
            .iter()
            .filter(|(_, lease)| lease.lease_expiry <= now)
            .map(|(key, lease)| (*key, lease.clone()))
            .collect();

        if expired_leases.is_empty() {
            trace!("No expired leases");
            return;
        }

        info!("Cleaning up {} expired leases", expired_leases.len());

        for (piece_index, lease) in expired_leases {
            if let Err(err) = self
                .release_piece(PieceCompletion {
                    piece_index,
                    peer_key: lease.peer_key,
                    result: CompletionResult::Expired,
                    completed_at: now,
                })
                .await
            {
                warn!(?err, ?piece_index, "releasing piece")
            }
        }
    }

    /// Get or create a peer ranking
    pub fn get_or_create_peer_ranking(&mut self, peer_key: RecordKey) -> &mut PeerRanking {
        self.peer_rankings
            .entry(peer_key.clone())
            .or_insert_with(|| PeerRanking::new(peer_key))
    }

    /// Generate next request ID
    pub fn next_request_id(&mut self) -> u64 {
        self.request_id_counter += 1;
        self.request_id_counter
    }

    /// Select pieces for a peer based on availability and strategy
    pub fn select_pieces_for_peer(
        &self,
        available_pieces: &PieceMap,
        max_leases: usize,
    ) -> Vec<usize> {
        available_pieces
            .iter()
            .filter(|&piece_index| !self.active_leases.contains_key(&piece_index))
            .take(max_leases)
            .collect()
    }

    /// Release a piece lease (success or failure)
    pub async fn release_piece(&mut self, completion: PieceCompletion) -> Result<()> {
        let piece_index = completion.piece_index;
        let peer_key = &completion.peer_key;
        debug!(piece_index, ?peer_key, result = ?completion.result, "release piece");

        // Remove the lease if it exists
        let lease_exists = self.active_leases.remove(&completion.piece_index).is_some();

        if !lease_exists {
            warn!(piece_index, "no active lease found");
            return Err(super::Error::LeaseNotFound);
        }

        // Abort any tasks running on this piece
        if let Some(handles) = self.lease_handles.remove(&piece_index) {
            handles.iter().for_each(|handle| handle.abort());
        }

        // Update peer ranking
        let config = self.config.clone();
        let ranking = self.get_or_create_peer_ranking(completion.peer_key);
        ranking.current_leases = ranking.current_leases.saturating_sub(1);

        match &completion.result {
            CompletionResult::Success => {
                ranking.record_success(&config);
                // Remove from wanted pieces on success
                self.wanted_pieces
                    .clear(TryInto::<u32>::try_into(piece_index).unwrap());
            }
            CompletionResult::Failure(_) => {
                ranking.record_failure(&config);
                // Return to wanted pieces on failure
                self.wanted_pieces
                    .set(TryInto::<u32>::try_into(piece_index).unwrap());
            }
            CompletionResult::Expired => {
                ranking.record_expiry(&config);
                // Return to wanted pieces on expiry
                self.wanted_pieces
                    .set(TryInto::<u32>::try_into(piece_index).unwrap());
            }
        };

        Ok(())
    }
}

/// The central coordinator for piece leases and peer rankings
#[derive(Clone)]
pub struct PieceLeaseManager {
    inner: Arc<Mutex<Inner>>,
}

impl PieceLeaseManager {
    /// Create a new piece lease manager with the given configuration
    pub fn new(config: LeaseManagerConfig) -> Self {
        let inner = Inner::new(config);
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    /// Request a lease for pieces available from the given peer
    #[tracing::instrument(skip_all)]
    pub async fn request_lease<'a>(&self, request: &'a LeaseRequest<'a>) -> Result<LeaseResponse> {
        let mut inner = self.inner.lock().await;

        trace!("requesting lease");

        // Clean up expired leases first
        inner.cleanup_expired_leases().await;

        let request_id = inner.next_request_id();
        debug!(
            "Handling lease request {} from peer {:?}",
            request_id, request.remote_key
        );

        // Get or create peer ranking
        let peer_ranking = inner
            .get_or_create_peer_ranking(request.remote_key.clone())
            .clone();

        // Find available pieces
        let available_pieces = request.can_fulfill(&inner.wanted_pieces);

        if available_pieces.iter().count() == 0 {
            return Err(Error::LeaseRejected(RejectedReason::NoAvailablePieces));
        }

        if peer_ranking.current_leases >= inner.config.max_leases_per_peer {
            return Err(Error::LeaseRejected(RejectedReason::PeerAtCapacity));
        }

        // Select pieces for this peer
        let max_leases = f64::from(request.max_leases).min(f64::from(
            inner.config.max_leases_per_peer - peer_ranking.current_leases,
        ));
        let allowed_leases = (max_leases * peer_ranking.score).max(1.0) as usize;
        let selected_pieces = inner.select_pieces_for_peer(&available_pieces, allowed_leases);

        if selected_pieces.is_empty() {
            return Err(Error::LeaseRejected(RejectedReason::NoAvailablePieces));
        }

        // Create leases
        let granted_leases: Vec<PieceLease> = selected_pieces
            .into_iter()
            .map(|piece_index| PieceLease {
                piece_index,
                lease_expiry: Instant::now() + inner.config.lease_duration,
                peer_key: request.remote_key.clone(),
                granted_at: Instant::now(),
            })
            .collect();

        // Update state
        for lease in &granted_leases {
            inner
                .active_leases
                .insert(lease.piece_index(), lease.clone());
        }

        // Update peer ranking
        if let Some(ranking) = inner.peer_rankings.get_mut(&request.remote_key) {
            ranking.current_leases += granted_leases.len() as u32;
        }

        debug!(
            "Lease request {} granted {} leases",
            request_id,
            granted_leases.len()
        );

        Ok(LeaseResponse {
            request_id,
            granted_leases,
        })
    }

    pub async fn register_tasks(&self, piece_index: usize, handles: &[AbortHandle]) -> Result<()> {
        let mut inner = self.inner.lock().await;
        if inner.active_leases.contains_key(&piece_index) {
            inner
                .lease_handles
                .entry(piece_index)
                .or_insert_with(|| vec![])
                .extend_from_slice(handles);
            Ok(())
        } else {
            Err(super::Error::LeaseNotFound)
        }
    }

    /// Release a piece lease (success or failure)
    pub async fn release_piece(&self, piece_index: usize, result: CompletionResult) -> Result<()> {
        let mut inner = self.inner.lock().await;
        let peer_key = inner
            .active_leases
            .get(&piece_index)
            .map(|lease| lease.peer_key().clone())
            .ok_or(Error::LeaseNotFound)?;
        inner
            .release_piece(PieceCompletion {
                piece_index,
                peer_key,
                result,
                completed_at: Instant::now(),
            })
            .await
    }

    /// Get the number of pieces still wanted
    pub async fn wanted_pieces_count(&self) -> usize {
        let inner = self.inner.lock().await;
        inner.wanted_pieces.iter().count()
    }

    /// Get the number of active leases
    pub async fn active_leases_count(&self) -> usize {
        let inner = self.inner.lock().await;
        inner.active_leases.len()
    }

    /// Get peer ranking information
    pub async fn get_peer_ranking(&self, peer_key: &RecordKey) -> Option<PeerRanking> {
        let inner = self.inner.lock().await;
        inner.peer_rankings.get(peer_key).cloned()
    }

    /// Initialize the set of wanted pieces
    pub async fn set_wanted_pieces(&self, wanted: PieceMap) {
        let mut inner = self.inner.lock().await;
        inner.wanted_pieces = wanted;
        info!(
            "Set wanted pieces: {} pieces",
            inner.wanted_pieces.iter().count()
        );
    }

    /// Add pieces to the wanted set
    pub async fn add_wanted_pieces(&self, pieces: PieceMap) {
        let mut inner = self.inner.lock().await;
        // For each bit set in the input, set it in the wanted map
        for piece_index in pieces.iter() {
            inner.wanted_pieces.set(piece_index as u32);
        }
        info!(
            "Added {} wanted pieces, total: {}",
            pieces.iter().count(),
            inner.wanted_pieces.iter().count()
        );
    }

    /// Remove pieces from the wanted set
    pub async fn remove_wanted_pieces(&self, pieces: PieceMap) {
        let mut inner = self.inner.lock().await;
        for piece_index in pieces.iter() {
            inner.wanted_pieces.clear(piece_index as u32);
        }
        info!(
            "Removed {} wanted pieces, total: {}",
            pieces.iter().count(),
            inner.wanted_pieces.iter().count()
        );
    }

    /// Get statistics about the lease manager
    pub async fn get_stats(&self) -> LeaseManagerStats {
        let inner = self.inner.lock().await;
        let peer_count = inner.peer_rankings.len();
        let total_successful: u32 = inner
            .peer_rankings
            .values()
            .map(|r| r.successful_pieces)
            .sum();
        let total_failed: u32 = inner.peer_rankings.values().map(|r| r.failed_pieces).sum();
        let avg_score = if peer_count > 0 {
            inner.peer_rankings.values().map(|r| r.score).sum::<f64>() / peer_count as f64
        } else {
            0.0
        };

        LeaseManagerStats {
            wanted_pieces_count: inner.wanted_pieces.iter().count(),
            active_leases_count: inner.active_leases.len(),
            peer_count,
            total_successful_pieces: total_successful,
            total_failed_pieces: total_failed,
            average_peer_score: avg_score,
        }
    }
}
