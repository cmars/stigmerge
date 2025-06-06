use std::{fmt, io};

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
        Some(&VeilidAPIError::InvalidTarget { .. }) => true,
        Some(e) => {
            let msg = e.to_string();
            msg.contains("could not get remote private route") || msg.contains("Invalid target")
        }
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

/// Trait for errors that may be caused by transient conditions which may clear
/// up upon retrying.
pub trait Transient {
    fn is_transient(&self) -> bool;
}

impl Transient for VeilidAPIError {
    fn is_transient(&self) -> bool {
        match self {
            // Errors conditional on changing local or remote node states,
            // network conditions and other transient conditions.
            &VeilidAPIError::Timeout => true,
            &VeilidAPIError::TryAgain { .. } => true,
            &VeilidAPIError::NoConnection { .. } => true,
            &VeilidAPIError::NotInitialized => true,
            &VeilidAPIError::KeyNotFound { .. } => true,
            &VeilidAPIError::Internal { .. } => true,

            // These errors are not likely to be transient in nature.
            &VeilidAPIError::Generic { .. } => false,
            &VeilidAPIError::Unimplemented { .. } => false,
            &VeilidAPIError::ParseError { .. } => false,
            &VeilidAPIError::InvalidArgument { .. } => false,
            &VeilidAPIError::MissingArgument { .. } => false,
            &VeilidAPIError::AlreadyInitialized => false,
            &VeilidAPIError::Shutdown => false,
            &VeilidAPIError::InvalidTarget { .. } => false,
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
