use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use stigmerge_fileindex::Index;
use tokio::sync::broadcast::{self, Receiver, Sender};
use veilid_core::{OperationId, Target, ValueSubkeyRangeSet, VeilidUpdate};

use crate::{error::Result, node::TypedKey, piece_map::PieceMap};
use crate::{
    proto::{Header, PeerInfo},
    Node,
};

#[derive(Clone)]
pub struct StubNode {
    pub update_tx: Sender<VeilidUpdate>,

    pub reset_result: Arc<Mutex<dyn Fn() -> Result<()> + Send + 'static>>,
    pub shutdown_result: Arc<Mutex<dyn Fn() -> Result<()> + Send + 'static>>,
    pub announce_result:
        Arc<Mutex<dyn Fn(&Index) -> Result<(TypedKey, Target, Header)> + Send + 'static>>,
    pub announce_route_result: Arc<
        Mutex<
            dyn Fn(&TypedKey, Option<Target>, &Header) -> Result<(Target, Header)> + Send + 'static,
        >,
    >,
    pub resolve_route_index_result:
        Arc<Mutex<dyn Fn(&TypedKey, &Path) -> Result<(Target, Header, Index)> + Send + 'static>>,
    pub resolve_route_result:
        Arc<Mutex<dyn Fn(&TypedKey, Option<Target>) -> Result<(Target, Header)> + Send + 'static>>,
    pub request_block_result:
        Arc<Mutex<dyn Fn(Target, usize, usize) -> Result<Option<Vec<u8>>> + Send + 'static>>,
    pub reply_block_contents_result:
        Arc<Mutex<dyn Fn(OperationId, Option<&[u8]>) -> Result<()> + Send + 'static>>,
    pub request_advertise_peer_result:
        Arc<Mutex<dyn Fn(&Target, &TypedKey) -> Result<()> + Send + 'static>>,
    pub watch_result:
        Arc<Mutex<dyn Fn(TypedKey, ValueSubkeyRangeSet) -> Result<()> + Send + 'static>>,
    pub cancel_watch_result: Arc<Mutex<dyn Fn(&TypedKey) -> Result<()> + Send + 'static>>,
    pub merge_have_map_result: Arc<
        Mutex<dyn Fn(TypedKey, ValueSubkeyRangeSet, &mut PieceMap) -> Result<()> + Send + 'static>,
    >,
    pub announce_have_map_result:
        Arc<Mutex<dyn Fn(TypedKey, &PieceMap) -> Result<()> + Send + 'static>>,
    pub resolve_have_map_result: Arc<Mutex<dyn Fn(&TypedKey) -> Result<PieceMap> + Send + 'static>>,
    pub announce_peer_result:
        Arc<Mutex<dyn Fn(&[u8], Option<TypedKey>, u16) -> Result<()> + Send + 'static>>,
    pub reset_peers_result: Arc<Mutex<dyn Fn(&[u8], u16) -> Result<()> + Send + 'static>>,
    pub resolve_peers_result:
        Arc<Mutex<dyn Fn(&TypedKey) -> Result<Vec<PeerInfo>> + Send + 'static>>,
}

impl StubNode {
    pub fn new() -> Self {
        let (update_tx, _) = broadcast::channel(16);
        StubNode {
            update_tx,
            reset_result: Arc::new(Mutex::new(|| panic!("unexpected call to reset"))),
            shutdown_result: Arc::new(Mutex::new(|| panic!("unexpected call to shutdown"))),
            announce_result: Arc::new(Mutex::new(|_index: &Index| {
                panic!("unexpected call to announce")
            })),
            announce_route_result: Arc::new(Mutex::new(
                |_key: &TypedKey, _prior_route: Option<Target>, _header: &Header| {
                    panic!("unexpected call to announce_route")
                },
            )),
            resolve_route_index_result: Arc::new(Mutex::new(|_key: &TypedKey, _root: &Path| {
                panic!("unexpected call to resolve_route_index")
            })),
            resolve_route_result: Arc::new(Mutex::new(
                |_key: &TypedKey, _prior_route: Option<Target>| {
                    panic!("unexpected call to resolve_route")
                },
            )),
            request_block_result: Arc::new(Mutex::new(
                |_target: Target, _piece: usize, _block: usize| {
                    panic!("unexpected call to request_block")
                },
            )),
            reply_block_contents_result: Arc::new(Mutex::new(
                |_call_id: OperationId, _contents: Option<&[u8]>| {
                    panic!("unexpected call to reply_block_contents")
                },
            )),
            request_advertise_peer_result: Arc::new(Mutex::new(
                |_target: &Target, _key: &TypedKey| {
                    panic!("unexpected call to request_advertised_peers")
                },
            )),
            watch_result: Arc::new(Mutex::new(
                |_key: TypedKey, _values: ValueSubkeyRangeSet| panic!("unexpected call to watch"),
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
            resolve_have_map_result: Arc::new(Mutex::new(|_peer_key: &TypedKey| {
                panic!("unexpected call to resolve_have_map")
            })),
            announce_peer_result: Arc::new(Mutex::new(
                |_payload_digest: &[u8], _peer_key: Option<TypedKey>, _subkey: u16| {
                    panic!("unexpected call to announce_peer")
                },
            )),
            reset_peers_result: Arc::new(Mutex::new(|_payload_digest: &[u8], _max_subkey: u16| {
                panic!("unexpected call to reset_peers")
            })),
            resolve_peers_result: Arc::new(Mutex::new(|_peer_key: &TypedKey| {
                panic!("unexpected call to resolve_peers")
            })),
        }
    }
}

impl Node for StubNode {
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

    async fn announce_route(
        &mut self,
        key: &TypedKey,
        prior_route: Option<Target>,
        header: &Header,
    ) -> Result<(Target, Header)> {
        (*(self.announce_route_result.lock().unwrap()))(key, prior_route, header)
    }

    async fn resolve_route_index(
        &mut self,
        key: &TypedKey,
        root: &Path,
    ) -> Result<(Target, Header, Index)> {
        (*(self.resolve_route_index_result.lock().unwrap()))(key, root)
    }

    async fn resolve_route(
        &mut self,
        key: &TypedKey,
        prior_route: Option<Target>,
    ) -> Result<(Target, Header)> {
        (*(self.resolve_route_result.lock().unwrap()))(key, prior_route)
    }

    async fn request_block(
        &mut self,
        target: Target,
        piece: usize,
        block: usize,
    ) -> Result<Option<Vec<u8>>> {
        (*(self.request_block_result.lock().unwrap()))(target, piece, block)
    }

    async fn reply_block_contents(
        &mut self,
        call_id: OperationId,
        contents: Option<&[u8]>,
    ) -> Result<()> {
        (*(self.reply_block_contents_result.lock().unwrap()))(call_id, contents)
    }

    async fn watch(&mut self, key: TypedKey, values: ValueSubkeyRangeSet) -> Result<()> {
        (*(self.watch_result.lock().unwrap()))(key, values)
    }

    async fn cancel_watch(&mut self, key: &TypedKey) -> Result<()> {
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

    async fn announce_peer(
        &mut self,
        payload_digest: &[u8],
        peer_key: Option<TypedKey>,
        subkey: u16,
    ) -> Result<()> {
        (*(self.announce_peer_result.lock().unwrap()))(payload_digest, peer_key, subkey)
    }

    async fn reset_peers(&mut self, payload_digest: &[u8], max_subkey: u16) -> Result<()> {
        (*(self.reset_peers_result.lock().unwrap()))(payload_digest, max_subkey)
    }

    async fn resolve_have_map(&mut self, peer_key: &TypedKey) -> Result<PieceMap> {
        (*(self.resolve_have_map_result.lock().unwrap()))(peer_key)
    }

    async fn resolve_peers(&mut self, peer_key: &TypedKey) -> Result<Vec<PeerInfo>> {
        (*(self.resolve_peers_result.lock().unwrap()))(peer_key)
    }

    async fn request_advertise_peer(&mut self, target: &Target, key: &TypedKey) -> Result<()> {
        (*(self.request_advertise_peer_result.lock().unwrap()))(target, key)
    }
}
