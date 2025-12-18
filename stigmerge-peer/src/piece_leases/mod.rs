//! Piece Lease Manager
//!
//! A centralized coordination system for managing piece-level leases in the stigmerge fetcher.
//! Provides exclusive access to pieces, peer ranking, and better failure handling.

mod error;
mod peer_ranking;
mod types;

// The manager module contains the core implementation
mod manager;

#[cfg(test)]
mod tests;

// Re-export all the main types for backward compatibility
pub use error::{Error, Result};
pub use manager::PieceLeaseManager;
pub use peer_ranking::PeerRanking;
pub use types::{
    CompletionResult, FailureReason, LeaseManagerConfig, LeaseManagerStats, LeaseRequest,
    LeaseResponse, PieceCompletion, PieceLease, RejectedReason,
};
