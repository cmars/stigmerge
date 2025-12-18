use std::fmt;

use crate::piece_leases::RejectedReason;

pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in the piece lease manager
#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    /// No active lease found for the specified piece
    LeaseNotFound,
    /// Lease request was rejected
    LeaseRejected(RejectedReason),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::LeaseNotFound => {
                write!(f, "No active lease found for piece")
            }
            Error::LeaseRejected(reason) => {
                write!(f, "Lease request rejected: {:?}", reason)
            }
        }
    }
}

impl std::error::Error for Error {}
