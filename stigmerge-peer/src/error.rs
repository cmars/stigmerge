use std::fmt;

use tokio::sync::broadcast;
use veilid_core::VeilidAPIError;

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
    matches!(
        as_veilid(e),
        Some(VeilidAPIError::Unimplemented { .. }) | Some(VeilidAPIError::Shutdown)
    )
}

pub fn is_lagged<T>(result: &std::result::Result<T, broadcast::error::RecvError>) -> bool {
    matches!(result, Err(broadcast::error::RecvError::Lagged(_)))
}
