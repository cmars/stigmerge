use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use stigmerge_fileindex::Index;
use tokio::sync::broadcast::{self, Receiver, Sender};
use veilid_core::{OperationId, Target, VeilidUpdate};

use crate::{error::Result, have_map::HaveMap, peer::TypedKey};
use crate::{proto::Header, Peer};

#[derive(Clone)]
pub struct StubPeer {
    pub update_tx: Sender<VeilidUpdate>,
    pub reset_result: Arc<Mutex<dyn Fn() -> Result<()> + Send + 'static>>,
    pub shutdown_result: Arc<Mutex<dyn Fn() -> Result<()> + Send + 'static>>,
    pub announce_result:
        Arc<Mutex<dyn Fn() -> Result<(TypedKey, Target, Header)> + Send + 'static>>,
    pub reannounce_route_result: Arc<Mutex<dyn Fn() -> Result<(Target, Header)> + Send + 'static>>,
    pub resolve_result: Arc<Mutex<dyn Fn() -> Result<(Target, Header, Index)> + Send + 'static>>,
    pub reresolve_route_result: Arc<Mutex<dyn Fn() -> Result<(Target, Header)> + Send + 'static>>,
    pub request_block_result: Arc<Mutex<dyn Fn() -> Result<Vec<u8>> + Send + 'static>>,
    pub reply_block_contents_result: Arc<Mutex<dyn Fn() -> Result<()> + Send + 'static>>,
    pub watch_result: Arc<Mutex<dyn Fn() -> Result<()> + Send + 'static>>,
    pub cancel_watch_result: Arc<Mutex<dyn Fn() + Send + 'static>>,
    pub merge_have_map_result: Arc<Mutex<dyn Fn() -> Result<()> + Send + 'static>>,
    pub announce_have_map_result: Arc<Mutex<dyn Fn() -> Result<()> + Send + 'static>>,
}

impl StubPeer {
    pub fn new() -> Self {
        let (update_tx, _) = broadcast::channel(16);
        StubPeer {
            update_tx,
            reset_result: Arc::new(Mutex::new(|| panic!("unexpected call to reset"))),
            shutdown_result: Arc::new(Mutex::new(|| panic!("unexpected call to shutdown"))),
            announce_result: Arc::new(Mutex::new(|| panic!("unexpected call to announce"))),
            reannounce_route_result: Arc::new(Mutex::new(|| {
                panic!("unexpected call to reannounce_route")
            })),
            resolve_result: Arc::new(Mutex::new(|| panic!("unexpected call to resolve"))),
            reresolve_route_result: Arc::new(Mutex::new(|| {
                panic!("unexpected call to reresolve_route")
            })),
            request_block_result: Arc::new(Mutex::new(|| {
                panic!("unexpected call to request_block")
            })),
            reply_block_contents_result: Arc::new(Mutex::new(|| {
                panic!("unexpected call to reply_block_contents")
            })),
            watch_result: Arc::new(Mutex::new(|| panic!("unexpected call to watch"))),
            cancel_watch_result: Arc::new(Mutex::new(|| panic!("unexpected call to cancel_watch"))),
            merge_have_map_result: Arc::new(Mutex::new(|| {
                panic!("unexpected call to merge_have_map")
            })),
            announce_have_map_result: Arc::new(Mutex::new(|| {
                panic!("unexpected call to merge_have_map")
            })),
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

    async fn announce_index(&mut self, _index: &Index) -> Result<(TypedKey, Target, Header)> {
        (*(self.announce_result.lock().unwrap()))()
    }

    async fn reannounce_route(
        &mut self,
        _key: &TypedKey,
        _prior_route: Option<Target>,
        _index: &Index,
        _header: &Header,
    ) -> Result<(Target, Header)> {
        (*(self.reannounce_route_result.lock().unwrap()))()
    }

    async fn resolve(&mut self, _key: &TypedKey, _root: &Path) -> Result<(Target, Header, Index)> {
        (*(self.resolve_result.lock().unwrap()))()
    }

    async fn reresolve_route(
        &mut self,
        _key: &TypedKey,
        _prior_route: Option<Target>,
    ) -> Result<(Target, Header)> {
        (*(self.reresolve_route_result.lock().unwrap()))()
    }

    async fn request_block(
        &mut self,
        _target: Target,
        _piece: usize,
        _block: usize,
    ) -> Result<Vec<u8>> {
        (*(self.request_block_result.lock().unwrap()))()
    }

    async fn reply_block_contents(
        &mut self,
        _call_id: OperationId,
        _contents: &[u8],
    ) -> Result<()> {
        (*(self.reply_block_contents_result.lock().unwrap()))()
    }

    async fn watch(
        &mut self,
        _key: TypedKey,
        _values: veilid_core::ValueSubkeyRangeSet,
        _period: veilid_core::TimestampDuration,
    ) -> Result<()> {
        (*(self.watch_result.lock().unwrap()))()
    }

    fn cancel_watch(&mut self, _key: &TypedKey) {
        (*(self.cancel_watch_result.lock().unwrap()))()
    }

    async fn merge_have_map(
        &mut self,
        _key: TypedKey,
        _subkeys: veilid_core::ValueSubkeyRangeSet,
        _have_map: &mut HaveMap,
    ) -> Result<()> {
        (*(self.merge_have_map_result.lock().unwrap()))()
    }

    async fn announce_have_map(
        &mut self,
        _key: TypedKey,
        _have_map: &crate::have_map::HaveMap,
    ) -> Result<()> {
        (*(self.announce_have_map_result.lock().unwrap()))()
    }
}
