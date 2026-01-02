//! Core Piece Lease Manager functionality

use std::collections::HashSet;
use std::{collections::HashMap, sync::Arc, time::Instant};

use tokio::sync::Mutex;
use veilid_core::RecordKey;

use crate::piece_map::PieceMap;

use super::error::{Error, Result};
use super::{peer_ranking::PeerRanking, types::*};

/// Inner state for the PieceLeaseManager
struct Inner {
    /// Configuration for the lease manager
    config: LeaseManagerConfig,

    /// Pieces that still need to be downloaded
    wanted: HashSet<usize>,

    /// Pieces wanted from each peer
    peer_has: HashMap<RecordKey, PieceMap>,

    /// Currently active leases by piece_index
    active_leases: HashMap<usize, PieceLease>,

    /// Peer performance rankings
    peer_rankings: HashMap<RecordKey, PeerRanking>,
}

impl Inner {
    fn new(config: LeaseManagerConfig) -> Self {
        Self {
            config,
            wanted: HashSet::new(),
            peer_has: HashMap::new(),
            active_leases: HashMap::new(),
            peer_rankings: HashMap::new(),
        }
    }

    fn want(&mut self, want_map: &PieceMap) {
        self.wanted = want_map.iter().collect();
    }

    fn add_peer(&mut self, peer_key: &RecordKey, have_map: &PieceMap) {
        self.peer_has.insert(peer_key.clone(), have_map.clone());
    }

    fn acquire_lease(&mut self, peer_key: &RecordKey) -> Result<PieceLease> {
        let piece_index = self.pop_piece(peer_key)?;
        let lease = PieceLease {
            piece_index,
            lease_expiry: Instant::now() + self.config.lease_duration,
            peer_key: peer_key.clone(),
            granted_at: Instant::now(),
        };
        self.active_leases.insert(piece_index, lease.clone());
        Ok(lease)
    }

    fn release_piece(&mut self, piece_index: usize, result: CompletionResult) -> Result<f64> {
        let active_lease = match self.active_leases.remove(&piece_index) {
            Some(lease) => lease,
            None => return Err(super::Error::LeaseNotFound),
        };

        // Update peer ranking
        let config = &self.config.clone();
        let ranking = self.get_or_create_peer_ranking(active_lease.peer_key);
        ranking.current_leases = ranking.current_leases.saturating_sub(1);

        let score = match &result {
            CompletionResult::Success => {
                let score = ranking.record_success(config);
                // Remove from wanted pieces on success
                self.wanted.remove(&piece_index);
                score
            }
            CompletionResult::Failure(_) => {
                let score = ranking.record_failure(config);
                // Return to wanted pieces on failure
                self.wanted.insert(piece_index);
                score
            }
            CompletionResult::Expired => {
                let score = ranking.record_expiry(config);
                // Return to wanted pieces on expiry
                self.wanted.insert(piece_index);
                score
            }
        };

        Ok(score)
    }

    fn pop_piece(&mut self, peer_key: &RecordKey) -> Result<usize> {
        let piece = {
            self.wanted
                .iter()
                .find(|piece_index| {
                    self.peer_has
                        .get(peer_key)
                        .map(|have_map| {
                            have_map.get(TryInto::<u32>::try_into(**piece_index).unwrap())
                        })
                        .unwrap_or(false)
                })
                .cloned()
        };
        match piece {
            Some(piece_index) => {
                self.wanted.remove(&piece_index);
                Ok(piece_index)
            }
            None => Err(Error::LeaseRejected(RejectedReason::NoMatchingPieces)),
        }
    }

    fn get_or_create_peer_ranking(&mut self, peer_key: RecordKey) -> &mut PeerRanking {
        self.peer_rankings
            .entry(peer_key.clone())
            .or_insert_with(|| PeerRanking::new(peer_key))
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

    pub async fn is_empty(&self) -> bool {
        self.inner.lock().await.wanted.is_empty()
    }

    pub async fn set_wanted_pieces(&mut self, want_map: &PieceMap) {
        self.inner.lock().await.want(want_map);
    }

    pub async fn wanted_pieces_count(&self) -> usize {
        self.inner.lock().await.wanted.len()
    }

    pub async fn active_leases_count(&self) -> usize {
        self.inner.lock().await.active_leases.len()
    }

    pub async fn get_peer_ranking(&self, peer_key: &RecordKey) -> Option<PeerRanking> {
        self.inner.lock().await.peer_rankings.get(peer_key).cloned()
    }

    pub async fn add_peer(&mut self, peer_key: &RecordKey, have_map: &PieceMap) {
        self.inner.lock().await.add_peer(peer_key, have_map)
    }

    pub async fn acquire_lease(&mut self, peer_key: &RecordKey) -> Result<PieceLease> {
        self.inner.lock().await.acquire_lease(peer_key)
    }

    pub async fn release_piece(
        &mut self,
        piece_index: usize,
        result: CompletionResult,
    ) -> Result<f64> {
        self.inner.lock().await.release_piece(piece_index, result)
    }
}
