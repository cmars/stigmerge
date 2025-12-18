//! Peer ranking functionality for the Piece Lease Manager

use std::time::Instant;
use tracing::debug;
use veilid_core::RecordKey;

use super::types::LeaseManagerConfig;

/// Tracks peer performance and assigns priority scores
#[derive(Debug, Clone)]
pub struct PeerRanking {
    pub(crate) key: RecordKey,
    pub(crate) successful_pieces: u32,
    pub(crate) failed_pieces: u32,
    pub(crate) current_leases: u32,
    pub(crate) score: f64,
    pub(crate) last_updated: Instant,
}

impl PeerRanking {
    pub fn new(key: RecordKey) -> Self {
        Self {
            key,
            successful_pieces: 0,
            failed_pieces: 0,
            current_leases: 0,
            score: 1.0, // Start with neutral score
            last_updated: Instant::now(),
        }
    }

    pub fn record_success(&mut self, config: &LeaseManagerConfig) {
        self.successful_pieces += 1;
        self.update_score(config);
    }

    pub fn record_failure(&mut self, config: &LeaseManagerConfig) {
        self.failed_pieces += 1;
        self.update_score(config);
    }

    pub fn record_expiry(&mut self, config: &LeaseManagerConfig) {
        // Treat expiry as a failure but less severe
        self.failed_pieces += 1;
        self.update_score(config);
    }

    fn update_score(&mut self, config: &LeaseManagerConfig) {
        let success_rate = if self.successful_pieces + self.failed_pieces > 0 {
            self.successful_pieces as f64 / (self.successful_pieces + self.failed_pieces) as f64
        } else {
            1.0
        };

        // Apply exponential decay to old scores
        let time_factor = config.peer_ranking_decay.powf(
            self.last_updated.elapsed().as_secs_f64() / 60.0,
        );

        self.score = self.score * time_factor
            + (success_rate * config.success_bonus - (1.0 - success_rate) * config.failure_penalty);

        // Clamp score to reasonable bounds
        self.score = self.score.clamp(0.1, 10.0);
        self.last_updated = Instant::now();

        debug!(
            "Updated peer score for {:?}: {} (success_rate: {})",
            self.key, self.score, success_rate
        );
    }
}

// Public accessors for external consumers
impl PeerRanking {
    pub fn key(&self) -> &RecordKey {
        &self.key
    }

    pub fn successful_pieces(&self) -> u32 {
        self.successful_pieces
    }

    pub fn failed_pieces(&self) -> u32 {
        self.failed_pieces
    }

    pub fn current_leases(&self) -> u32 {
        self.current_leases
    }

    pub fn score(&self) -> f64 {
        self.score
    }

    pub fn last_updated(&self) -> Instant {
        self.last_updated
    }
}
