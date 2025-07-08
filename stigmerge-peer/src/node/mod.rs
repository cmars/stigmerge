use std::time::Duration;
use std::{future::Future, path::Path};

use backoff::ExponentialBackoff;
use tokio::sync::broadcast;
use veilid_core::{OperationId, Target, TypedRecordKey, ValueSubkeyRangeSet, VeilidUpdate};

use stigmerge_fileindex::Index;

use crate::piece_map::PieceMap;
use crate::proto::PeerInfo;
use crate::{proto::Header, Result};

pub trait Node: Clone + Send {
    fn subscribe_veilid_update(&self) -> broadcast::Receiver<VeilidUpdate>;

    fn reset(&mut self) -> impl Future<Output = Result<()>> + Send;
    fn shutdown(self) -> impl Future<Output = Result<()>> + Send;

    fn announce_index(
        &mut self,
        index: &Index,
    ) -> impl std::future::Future<Output = Result<(TypedRecordKey, Target, Header)>> + Send;
    fn announce_route(
        &mut self,
        key: &TypedRecordKey,
        prior_route: Option<Target>,
        header: &Header,
    ) -> impl std::future::Future<Output = Result<(Target, Header)>> + Send;

    fn resolve_route_index(
        &mut self,
        key: &TypedRecordKey,
        root: &Path,
    ) -> impl std::future::Future<Output = Result<(Target, Header, Index)>> + Send;
    fn resolve_route(
        &mut self,
        key: &TypedRecordKey,
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
        key: &TypedRecordKey,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    fn watch(
        &mut self,
        key: TypedRecordKey,
        values: ValueSubkeyRangeSet,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    fn cancel_watch(
        &mut self,
        key: &TypedRecordKey,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    fn merge_have_map(
        &mut self,
        key: TypedRecordKey,
        subkeys: ValueSubkeyRangeSet,
        have_map: &mut PieceMap,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    fn announce_have_map(
        &mut self,
        key: TypedRecordKey,
        have_map: &PieceMap,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    fn resolve_have_map(
        &mut self,
        peer_key: &TypedRecordKey,
    ) -> impl std::future::Future<Output = Result<PieceMap>> + Send;

    fn announce_peer(
        &mut self,
        payload_digest: &[u8],
        peer_key: Option<TypedRecordKey>,
        subkey: u16,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    fn known_peers(
        &mut self,
        payload_digest: &[u8],
    ) -> impl std::future::Future<Output = Result<Vec<TypedRecordKey>>> + Send;

    fn reset_peers(
        &mut self,
        payload_digest: &[u8],
        max_subkey: u16,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    fn resolve_peers(
        &mut self,
        peer_key: &TypedRecordKey,
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
