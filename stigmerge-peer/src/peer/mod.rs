use std::time::Duration;
use std::{future::Future, path::Path};

use backoff::backoff::Backoff;
use backoff::ExponentialBackoff;
use tokio::sync::broadcast::Receiver;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use veilid_core::{
    CryptoKey, CryptoTyped, OperationId, Target, TimestampDuration, ValueSubkeyRangeSet,
    VeilidUpdate,
};

use stigmerge_fileindex::Index;

use crate::{proto::Header, Result};

pub type TypedKey = CryptoTyped<CryptoKey>;

pub trait Peer: Clone + Send {
    fn subscribe_veilid_update(&self) -> Receiver<VeilidUpdate>;

    fn reset(&mut self) -> impl Future<Output = Result<()>> + Send;
    fn shutdown(self) -> impl Future<Output = Result<()>> + Send;

    fn announce(
        &mut self,
        index: &Index,
    ) -> impl std::future::Future<Output = Result<(TypedKey, Target, Header)>> + Send;
    fn reannounce_route(
        &mut self,
        key: &TypedKey,
        prior_route: Option<Target>,
        index: &Index,
        header: &Header,
    ) -> impl std::future::Future<Output = Result<(Target, Header)>> + Send;

    fn resolve(
        &mut self,
        key: &TypedKey,
        root: &Path,
    ) -> impl std::future::Future<Output = Result<(Target, Header, Index)>> + Send;
    fn reresolve_route(
        &mut self,
        key: &TypedKey,
        prior_route: Option<Target>,
    ) -> impl Future<Output = Result<(Target, Header)>> + Send;

    fn request_block(
        &mut self,
        target: Target,
        piece: usize,
        block: usize,
    ) -> impl Future<Output = Result<Vec<u8>>> + Send;

    fn reply_block_contents(
        &mut self,
        call_id: OperationId,
        contents: &[u8],
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    fn watch(
        &mut self,
        key: TypedKey,
        values: ValueSubkeyRangeSet,
        period: TimestampDuration,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    fn cancel_watch(&mut self, key: &TypedKey);
}

mod veilid;
pub use veilid::Veilid;

mod observable;
pub use observable::Observable;
pub use observable::State as PeerState;

pub async fn reset_with_backoff<T: Peer>(peer: &mut T, done: &CancellationToken) -> Result<()> {
    let mut backoff = ExponentialBackoff::default();
    loop {
        match peer.reset().await {
            Ok(()) => return Ok(()),
            Err(e) => {
                if !e.is_resetable() {
                    done.cancel();
                    return Err(e);
                }
                match backoff.next_backoff() {
                    Some(delay) => sleep(delay).await,
                    None => return Err(e),
                }
            }
        }
    }
}

pub fn retry_backoff() -> ExponentialBackoff {
    let mut backoff = ExponentialBackoff::default();
    backoff.max_elapsed_time = Some(Duration::from_secs(15));
    backoff
}

pub fn reset_backoff() -> ExponentialBackoff {
    ExponentialBackoff::default()
}

macro_rules! with_backoff_retry {
    ($op:expr) => {{
        use backoff::backoff::Backoff as _;
        let mut retry_backoff = crate::peer::retry_backoff();
        let mut result = $op;
        loop {
            match result {
                Ok(_) => break,
                Err(ref e) => {
                    tracing::warn!(err = format!("{}", e));
                    match retry_backoff.next_backoff() {
                        Some(delay) => tokio::time::sleep(delay).await,
                        None => {
                            tracing::warn!("operation retries exceeded");
                            break;
                        }
                    };
                    result = $op;
                }
            }
        }
        result
    }};
}

macro_rules! with_backoff_reset {
    ($peer:expr, $op:expr) => {{
        use backoff::backoff::Backoff as _;
        let mut retry_backoff = crate::retry_backoff();
        let mut reset_backoff = crate::reset_backoff();
        let mut result = $op;
        'retry: loop {
            retry_backoff.reset();
            'operation: loop {
                match result {
                    Ok(_) => break 'retry,
                    Err(ref e) => {
                        tracing::warn!(err = format!("{}", e));
                        match retry_backoff.next_backoff() {
                            Some(delay) => tokio::time::sleep(delay).await,
                            None => {
                                break 'operation;
                            }
                        };
                        result = $op;
                    }
                }
            }
            reset_backoff.reset();
            'reset: loop {
                match $peer.reset().await {
                    Ok(()) => break 'reset,
                    Err(ref e) => {
                        if !e.is_resetable() {
                            break 'retry;
                        }
                        tracing::warn!(err = format!("{}", e));
                        match reset_backoff.next_backoff() {
                            Some(delay) => tokio::time::sleep(delay).await,
                            None => {
                                break 'retry;
                            }
                        }
                    }
                }
            }
        }
        result
    }};
}

pub(crate) use {with_backoff_reset, with_backoff_retry};
