use std::time::Duration;
use std::{future::Future, path::Path};

use backoff::ExponentialBackoff;
use tokio::sync::broadcast::Receiver;
use veilid_core::{
    CryptoKey, CryptoTyped, OperationId, Target, TimestampDuration, ValueSubkeyRangeSet,
    VeilidUpdate,
};

use stigmerge_fileindex::Index;

use crate::piece_map::PieceMap;
use crate::proto::PeerInfo;
use crate::{proto::Header, Result};

pub type TypedKey = CryptoTyped<CryptoKey>;

pub trait Node: Clone + Send {
    fn subscribe_veilid_update(&self) -> Receiver<VeilidUpdate>;

    fn reset(&mut self) -> impl Future<Output = Result<()>> + Send;
    fn shutdown(self) -> impl Future<Output = Result<()>> + Send;

    fn announce_index(
        &mut self,
        index: &Index,
    ) -> impl std::future::Future<Output = Result<(TypedKey, Target, Header)>> + Send;
    fn announce_route(
        &mut self,
        key: &TypedKey,
        prior_route: Option<Target>,
        header: &Header,
    ) -> impl std::future::Future<Output = Result<(Target, Header)>> + Send;

    fn resolve_route_index(
        &mut self,
        key: &TypedKey,
        root: &Path,
    ) -> impl std::future::Future<Output = Result<(Target, Header, Index)>> + Send;
    fn resolve_route(
        &mut self,
        key: &TypedKey,
        prior_route: Option<Target>,
    ) -> impl Future<Output = Result<(Target, Header)>> + Send;

    fn request_block(
        &mut self,
        target: Target,
        piece: usize,
        block: usize,
    ) -> impl Future<Output = Result<Option<Vec<u8>>>> + Send;

    fn reply_block_contents(
        &mut self,
        call_id: OperationId,
        contents: Option<&[u8]>,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    fn request_advertise_peer(
        &mut self,
        target: &Target,
        key: &TypedKey,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    fn watch(
        &mut self,
        key: TypedKey,
        values: ValueSubkeyRangeSet,
        period: TimestampDuration,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    fn cancel_watch(&mut self, key: &TypedKey);

    fn merge_have_map(
        &mut self,
        key: TypedKey,
        subkeys: ValueSubkeyRangeSet,
        have_map: &mut PieceMap,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    fn announce_have_map(
        &mut self,
        key: TypedKey,
        have_map: &PieceMap,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    fn resolve_have_map(
        &mut self,
        peer_key: &TypedKey,
    ) -> impl std::future::Future<Output = Result<PieceMap>> + Send;

    fn announce_peer(
        &mut self,
        payload_digest: &[u8],
        peer_key: Option<TypedKey>,
        subkey: u16,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    fn reset_peers(
        &mut self,
        payload_digest: &[u8],
        max_subkey: u16,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    fn resolve_peers(
        &mut self,
        peer_key: &TypedKey,
    ) -> impl std::future::Future<Output = Result<Vec<PeerInfo>>> + Send;
}

mod veilid;
pub use veilid::Veilid;

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
        let mut retry_backoff = crate::node::retry_backoff();
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

pub(crate) use with_backoff_retry;
