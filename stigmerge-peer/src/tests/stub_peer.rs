use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use stigmerge_fileindex::Index;
use tokio::sync::broadcast::{self, Receiver, Sender};
use veilid_core::{OperationId, Target, TimestampDuration, ValueSubkeyRangeSet, VeilidUpdate};

use crate::{error::Result, peer::TypedKey, piece_map::PieceMap, proto::PeerInfo};
use crate::{proto::Header, Peer};

#[derive(Clone)]
pub struct StubPeer {
    pub update_tx: Sender<VeilidUpdate>,
    
    pub reset_result: Arc<Mutex<dyn Fn() -> Result<()> + Send + 'static>>,
    pub shutdown_result: Arc<Mutex<dyn Fn() -> Result<()> + Send + 'static>>,
    pub announce_result:
        Arc<Mutex<dyn Fn(&Index) -> Result<(TypedKey, Target, Header)> + Send + 'static>>,
    pub reannounce_route_result: Arc<
        Mutex<
            dyn Fn(&TypedKey, Option<Target>, &Index, &Header) -> Result<(Target, Header)>
                + Send
                + 'static,
        >,
    >,
    pub resolve_result:
        Arc<Mutex<dyn Fn(&TypedKey, &Path) -> Result<(Target, Header, Index)> + Send + 'static>>,
    pub reresolve_route_result:
        Arc<Mutex<dyn Fn(&TypedKey, Option<Target>) -> Result<(Target, Header)> + Send + 'static>>,
    pub request_block_result:
        Arc<Mutex<dyn Fn(Target, usize, usize) -> Result<Vec<u8>> + Send + 'static>>,
    pub reply_block_contents_result:
        Arc<Mutex<dyn Fn(OperationId, &[u8]) -> Result<()> + Send + 'static>>,
    pub watch_result: Arc<
        Mutex<
            dyn Fn(TypedKey, ValueSubkeyRangeSet, TimestampDuration) -> Result<()> + Send + 'static,
        >,
    >,
    pub cancel_watch_result: Arc<Mutex<dyn Fn(&TypedKey) + Send + 'static>>,
    pub merge_have_map_result: Arc<
        Mutex<dyn Fn(TypedKey, ValueSubkeyRangeSet, &mut PieceMap) -> Result<()> + Send + 'static>,
    >,
    pub announce_have_map_result:
        Arc<Mutex<dyn Fn(TypedKey, &PieceMap) -> Result<()> + Send + 'static>>,
    pub resolve_peer_info_result:
        Arc<Mutex<dyn Fn(TypedKey, u16) -> Result<PeerInfo> + Send + 'static>>,
    pub announce_peer_result:
        Arc<Mutex<dyn Fn(TypedKey, Option<TypedKey>, u16) -> Result<()> + Send + 'static>>,
    pub reset_peers_result: Arc<Mutex<dyn Fn(TypedKey, u16) -> Result<()> + Send + 'static>>,
}

impl StubPeer {
    pub fn new() -> Self {
        let (update_tx, _) = broadcast::channel(16);
        StubPeer {
            update_tx,
            reset_result: Arc::new(Mutex::new(|| panic!("unexpected call to reset"))),
            shutdown_result: Arc::new(Mutex::new(|| panic!("unexpected call to shutdown"))),
            announce_result: Arc::new(Mutex::new(|_index: &Index| {
                panic!("unexpected call to announce")
            })),
            reannounce_route_result: Arc::new(Mutex::new(
                |_key: &TypedKey,
                 _prior_route: Option<Target>,
                 _index: &Index,
                 _header: &Header| {
                    panic!("unexpected call to reannounce_route")
                },
            )),
            resolve_result: Arc::new(Mutex::new(|_key: &TypedKey, _root: &Path| {
                panic!("unexpected call to resolve")
            })),
            reresolve_route_result: Arc::new(Mutex::new(
                |_key: &TypedKey, _prior_route: Option<Target>| {
                    panic!("unexpected call to reresolve_route")
                },
            )),
            request_block_result: Arc::new(Mutex::new(
                |_target: Target, _piece: usize, _block: usize| {
                    panic!("unexpected call to request_block")
                },
            )),
            reply_block_contents_result: Arc::new(Mutex::new(
                |_call_id: OperationId, _contents: &[u8]| {
                    panic!("unexpected call to reply_block_contents")
                },
            )),
            watch_result: Arc::new(Mutex::new(
                |_key: TypedKey, _values: ValueSubkeyRangeSet, _period: TimestampDuration| {
                    panic!("unexpected call to watch")
                },
            )),
            cancel_watch_result: Arc::new(Mutex::new(|_key: &TypedKey| {
                panic!("unexpected call to cancel_watch")
            })),
            merge_have_map_result: Arc::new(Mutex::new(
                |_key: TypedKey, _subkeys: ValueSubkeyRangeSet, _have_map: &mut PieceMap| {
                    panic!("unexpected call to merge_have_map")
                },
            )),
            announce_have_map_result: Arc::new(Mutex::new(
                |_key: TypedKey, _have_map: &PieceMap| {
                    panic!("unexpected call to announce_have_map")
                },
            )),
            resolve_peer_info_result: Arc::new(Mutex::new(|_key: TypedKey, _subkey: u16| {
                panic!("unexpected call to resolve_peer_info")
            })),
            announce_peer_result: Arc::new(Mutex::new(
                |_peer_map_key: TypedKey, _peer_key: Option<TypedKey>, _subkey: u16| {
                    panic!("unexpected call to announce_peer")
                },
            )),
            reset_peers_result: Arc::new(Mutex::new(
                |_peer_map_key: TypedKey, _max_subkey: u16| {
                    panic!("unexpected call to reset_peers")
                },
            )),
        }
    }
}

impl Peer for StubPeer {
    fn subscribe_veilid_update(&self) -> Receiver<VeilidUpdate> {
        self.update_tx.subscribe()
    }

    async fn reset(&mut self) -> Result<()> {
        (*(self.reset_result.lock().unwrap()))()
    }

    async fn shutdown(self) -> Result<()> {
        (*(self.shutdown_result.lock().unwrap()))()
    }

    async fn announce_index(&mut self, index: &Index) -> Result<(TypedKey, Target, Header)> {
        (*(self.announce_result.lock().unwrap()))(index)
    }

    async fn reannounce_route(
        &mut self,
        key: &TypedKey,
        prior_route: Option<Target>,
        index: &Index,
        header: &Header,
    ) -> Result<(Target, Header)> {
        (*(self.reannounce_route_result.lock().unwrap()))(key, prior_route, index, header)
    }

    async fn resolve(&mut self, key: &TypedKey, root: &Path) -> Result<(Target, Header, Index)> {
        (*(self.resolve_result.lock().unwrap()))(key, root)
    }

    async fn reresolve_route(
        &mut self,
        key: &TypedKey,
        prior_route: Option<Target>,
    ) -> Result<(Target, Header)> {
        (*(self.reresolve_route_result.lock().unwrap()))(key, prior_route)
    }

    async fn request_block(
        &mut self,
        target: Target,
        piece: usize,
        block: usize,
    ) -> Result<Vec<u8>> {
        (*(self.request_block_result.lock().unwrap()))(target, piece, block)
    }

    async fn reply_block_contents(&mut self, call_id: OperationId, contents: &[u8]) -> Result<()> {
        (*(self.reply_block_contents_result.lock().unwrap()))(call_id, contents)
    }

    async fn watch(
        &mut self,
        key: TypedKey,
        values: ValueSubkeyRangeSet,
        period: TimestampDuration,
    ) -> Result<()> {
        (*(self.watch_result.lock().unwrap()))(key, values, period)
    }

    fn cancel_watch(&mut self, key: &TypedKey) {
        (*(self.cancel_watch_result.lock().unwrap()))(key)
    }

    async fn merge_have_map(
        &mut self,
        key: TypedKey,
        subkeys: ValueSubkeyRangeSet,
        have_map: &mut PieceMap,
    ) -> Result<()> {
        (*(self.merge_have_map_result.lock().unwrap()))(key, subkeys, have_map)
    }

    async fn announce_have_map(&mut self, key: TypedKey, have_map: &PieceMap) -> Result<()> {
        (*(self.announce_have_map_result.lock().unwrap()))(key, have_map)
    }

    async fn resolve_peer_info(&mut self, key: TypedKey, subkey: u16) -> Result<PeerInfo> {
        (*(self.resolve_peer_info_result.lock().unwrap()))(key, subkey)
    }

    async fn announce_peer(
        &mut self,
        peer_map_key: TypedKey,
        peer_key: Option<TypedKey>,
        subkey: u16,
    ) -> Result<()> {
        (*(self.announce_peer_result.lock().unwrap()))(peer_map_key, peer_key, subkey)
    }

    async fn reset_peers(&mut self, peer_map_key: TypedKey, max_subkey: u16) -> Result<()> {
        (*(self.reset_peers_result.lock().unwrap()))(peer_map_key, max_subkey)
    }
}
