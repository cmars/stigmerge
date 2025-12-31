//! Types for the Piece Lease Manager

use std::time::{Duration, Instant};
use veilid_core::RecordKey;

use crate::piece_map::PieceMap;

/// Configuration for the piece lease manager
#[derive(Debug, Clone)]
pub struct LeaseManagerConfig {
    /// Duration for which a lease is valid
    pub lease_duration: Duration,

    /// Maximum number of concurrent leases per peer
    pub max_leases_per_peer: u32,

    /// Exponential decay factor for peer rankings (per hour)
    pub peer_ranking_decay: f64,

    /// Bonus applied to ranking for successful pieces
    pub success_bonus: f64,

    /// Penalty applied to ranking for failed pieces
    pub failure_penalty: f64,

    /// Interval for cleanup of expired leases
    pub cleanup_interval: Duration,

    /// Minimum peer score to be considered for leases
    pub min_peer_score: f64,
}

impl Default for LeaseManagerConfig {
    fn default() -> Self {
        Self {
            lease_duration: Duration::from_secs(300), // 5 minutes
            max_leases_per_peer: 4,
            peer_ranking_decay: 0.95,
            success_bonus: 0.5,
            failure_penalty: 1.0,
            cleanup_interval: Duration::from_secs(60),
            min_peer_score: 0.2,
        }
    }
}

/// Represents exclusive rights to fetch blocks for a specific piece
#[derive(Debug, Clone)]
pub struct PieceLease {
    pub(super) piece_index: usize,
    pub(super) lease_expiry: Instant,
    pub(super) peer_key: RecordKey,
    pub(super) granted_at: Instant,
}

impl PieceLease {
    pub fn piece_index(&self) -> usize {
        self.piece_index
    }

    pub fn lease_expiry(&self) -> Instant {
        self.lease_expiry
    }

    pub fn peer_key(&self) -> &RecordKey {
        &self.peer_key
    }

    pub fn granted_at(&self) -> Instant {
        self.granted_at
    }

    pub fn is_expired(&self) -> bool {
        Instant::now() > self.lease_expiry
    }

    pub fn time_remaining(&self) -> Duration {
        self.lease_expiry.saturating_duration_since(Instant::now())
    }
}

/// Request for a piece lease
#[derive(Clone)]
pub struct LeaseRequest<'a> {
    pub remote_key: RecordKey,
    pub have_map: &'a PieceMap,
    pub max_leases: u32,
    pub requested_at: Instant,
}

impl<'a> LeaseRequest<'a> {
    pub fn can_fulfill(
        &self,
        wanted_pieces: &crate::piece_map::PieceMap,
    ) -> crate::piece_map::PieceMap {
        wanted_pieces.intersection(self.have_map)
    }
}

/// Response to a lease request
#[derive(Debug, Clone)]
pub struct LeaseResponse {
    pub request_id: u64,
    pub granted_leases: Vec<PieceLease>,
}

/// Reasons why a lease request was rejected
#[derive(Debug, Clone, PartialEq)]
pub enum RejectedReason {
    /// No leases are available to give out to the peer
    NoAvailablePieces,

    /// The peer doesn't have any pieces that are wanted
    NoMatchingPieces,

    PeerAtCapacity,
    PeerRankingTooLow,
    InvalidRequest,
}

/// Completion status for a piece
#[derive(Debug, Clone)]
pub struct PieceCompletion {
    pub piece_index: usize,
    pub peer_key: RecordKey,
    pub result: CompletionResult,
    pub completed_at: Instant,
}

/// Result of attempting to complete a piece
#[derive(Debug, Clone)]
pub enum CompletionResult {
    Success,
    Failure(FailureReason),
    Expired,
}

/// Reasons why piece completion failed
#[derive(Debug, Clone)]
pub enum FailureReason {
    VerificationFailed,
    NetworkError,
    Timeout,
    InvalidData,
}

/// Statistics about the lease manager
#[derive(Debug, Clone)]
pub struct LeaseManagerStats {
    pub wanted_pieces_count: usize,
    pub active_leases_count: usize,
    pub peer_count: usize,
    pub total_successful_pieces: u32,
    pub total_failed_pieces: u32,
    pub average_peer_score: f64,
}
