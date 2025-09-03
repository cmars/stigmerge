use std::{fmt, io};

use tokio::sync::broadcast;
use veilid_core::VeilidAPIError;

use crate::proto;

pub type Error = anyhow::Error;

pub type Result<T> = std::result::Result<T, Error>;

pub struct CancelError;

impl std::error::Error for CancelError {}

impl fmt::Display for CancelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "cancelled")
    }
}

impl fmt::Debug for CancelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "cancelled")
    }
}

pub fn as_veilid(e: &Error) -> Option<&VeilidAPIError> {
    for cause in e.chain() {
        if let Some(err) = cause.downcast_ref::<VeilidAPIError>() {
            return Some(err);
        }
    }
    None
}

pub fn as_proto(e: &Error) -> Option<&proto::Error> {
    for cause in e.chain() {
        if let Some(err) = cause.downcast_ref::<proto::Error>() {
            return Some(err);
        }
    }
    None
}

pub fn as_io(e: &Error) -> Option<&io::Error> {
    for cause in e.chain() {
        if let Some(err) = cause.downcast_ref::<io::Error>() {
            return Some(err);
        }
    }
    None
}

pub fn is_route_invalid(e: &Error) -> bool {
    match as_veilid(e) {
        Some(err) => is_veilid_route_invalid(err),
        None => false,
    }
}

pub fn is_veilid_route_invalid(err: &VeilidAPIError) -> bool {
    match err {
        VeilidAPIError::InvalidTarget { .. } => true,
        VeilidAPIError::Generic { ref message } => match message.as_str() {
            "can't reach private route any more" => true,
            "allocated route failed to test" => true,
            "allocated route could not be tested" => true,
            _ => false,
        },
        VeilidAPIError::TryAgain { ref message } => match message.as_str() {
            "unable to allocate route until we have a valid PublicInternet network class" => true,
            "not enough nodes to construct route at this time" => true,
            "unable to find unique route at this time" => true,
            "unable to assemble route until we have published peerinfo" => true,
            _ => false,
        },
        VeilidAPIError::Internal { ref message } => match message.as_str() {
            "no best key to test allocated route" => true,
            "peer info should exist for route but doesn't" => true,
            "route id does not exist" => true,
            _ => false,
        },
        _ => false,
    }
}

pub fn is_cancelled(e: &Error) -> bool {
    for cause in e.chain() {
        if cause.downcast_ref::<CancelError>().is_some() {
            return true;
        }
    }
    false
}

#[derive(Debug)]
pub struct Unrecoverable(String);

impl Unrecoverable {
    pub fn new(msg: &str) -> Unrecoverable {
        Unrecoverable(msg.to_owned())
    }
}

impl std::error::Error for Unrecoverable {}

impl fmt::Display for Unrecoverable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

pub fn is_unrecoverable(e: &Error) -> bool {
    for cause in e.chain() {
        if cause.downcast_ref::<Unrecoverable>().is_some() {
            return true;
        }
    }
    match as_veilid(e) {
        Some(err) => match err {
            &VeilidAPIError::Unimplemented { .. } => true,
            &VeilidAPIError::Shutdown => true,
            _ => false,
        },
        None => false,
    }
}

pub fn is_proto(e: &Error) -> bool {
    match as_proto(e) {
        Some(_) => true,
        _ => false,
    }
}

pub fn is_io(e: &Error) -> bool {
    match as_io(e) {
        Some(_) => true,
        _ => false,
    }
}

pub fn is_lagged<T>(result: &std::result::Result<T, broadcast::error::RecvError>) -> bool {
    match result {
        Err(broadcast::error::RecvError::Lagged(_)) => true,
        _ => false,
    }
}

/// Trait for errors that may be caused by transient conditions which may clear
/// up upon retrying.
pub trait Transient {
    fn is_transient(&self) -> bool;
}

impl Transient for VeilidAPIError {
    fn is_transient(&self) -> bool {
        if is_veilid_route_invalid(self) {
            return false;
        }

        match self {
            // Errors conditional on changing local or remote node states,
            // network conditions and other transient conditions.
            &VeilidAPIError::Timeout => true,
            &VeilidAPIError::TryAgain { .. } => true,
            &VeilidAPIError::KeyNotFound { .. } => true,
            _ => false,
        }
    }
}

impl Transient for Error {
    fn is_transient(&self) -> bool {
        match as_veilid(self) {
            // Some veilid errexamples/share_announce.rsors are retryable
            Some(err) => err.is_transient(),
            None => false,
        }
    }
}
