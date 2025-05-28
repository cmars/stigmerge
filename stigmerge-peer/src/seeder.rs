use std::{collections::HashMap, sync::Arc};

use stigmerge_fileindex::{BLOCK_SIZE_BYTES, PIECE_SIZE_BLOCKS};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
    select,
    sync::{broadcast, Mutex},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn, Level};
use veilid_core::VeilidUpdate;

use crate::{
    actor::{Actor, Respondable, ResponseChannel},
    node::TypedKey,
    peer_resolver,
    peer_tracker::PeerTracker,
    piece_map::PieceMap,
    proto::{self, BlockRequest, Decoder, PeerInfo},
    share_resolver,
    types::{PieceState, ShareInfo},
    Node, Result,
};

pub enum Request {
    HaveMap {
        response_tx: ResponseChannel<Response>,
    },
}

impl Respondable for Request {
    type Response = Response;

    fn set_response(&mut self, ch: ResponseChannel<Self::Response>) {
        match self {
            Request::HaveMap { response_tx } => *response_tx = ch,
        }
    }

    fn response_tx(self) -> ResponseChannel<Self::Response> {
        match self {
            Request::HaveMap { response_tx } => response_tx,
        }
    }
}

#[derive(Debug, Clone)]
pub enum Response {
    HaveMap(PieceMap),
}

pub struct Seeder<N: Node> {
    node: N,
    share: ShareInfo,
    clients: Clients,
    files: HashMap<u32, File>,
    piece_map: PieceMap,
}

impl<N: Node> Seeder<N> {
    #[tracing::instrument(skip_all)]
    pub fn new(node: N, share: ShareInfo, clients: Clients) -> Self {
        Seeder {
            node,
            share,
            clients,
            files: HashMap::new(),
            piece_map: PieceMap::new(),
        }
    }

    async fn read_block_into(&mut self, block_req: &BlockRequest, buf: &mut [u8]) -> Result<usize> {
        let fh = self.get_file_for_block(&block_req).await?;
        fh.seek(std::io::SeekFrom::Start(
            ((TryInto::<usize>::try_into(block_req.piece).unwrap()
                * PIECE_SIZE_BLOCKS
                * BLOCK_SIZE_BYTES)
                + (TryInto::<usize>::try_into(block_req.block).unwrap() * BLOCK_SIZE_BYTES))
                .try_into()
                .unwrap(),
        ))
        .await?;
        let rd = fh.read(buf).await?;
        Ok(rd)
    }

    async fn get_file_for_block(&mut self, block_req: &BlockRequest) -> Result<&mut File> {
        if !self.files.contains_key(&block_req.piece) {
            // FIXME: does not support multifile; piece -> file mapping should be handled by Index
            let file_path = self
                .share
                .root
                .join(self.share.want_index.files()[0].path());
            let fh = File::open(file_path).await?;
            self.files.insert(block_req.piece, fh);
        }
        Ok(self.files.get_mut(&block_req.piece).unwrap())
    }
}

impl<P: Node> Actor for Seeder<P> {
    type Request = Request;
    type Response = Response;

    #[tracing::instrument(skip_all, err(level = Level::TRACE), level = Level::TRACE)]
    async fn run(
        &mut self,
        cancel: CancellationToken,
        request_rx: flume::Receiver<Self::Request>,
    ) -> Result<()> {
        let mut buf = [0u8; BLOCK_SIZE_BYTES];
        loop {
            select! {
                biased;
                _ = cancel.cancelled() => {
                    return Ok(());
                }
                res = request_rx.recv_async() => {
                    let req = res?;
                    self.handle_request(req).await?;
                }
                res = self.clients.verified_rx.recv_async() => {
                    let piece_state = res?;
                    self.piece_map.set(piece_state.piece_index.try_into().unwrap());
                }
                res = self.clients.update_rx.recv() => {
                    let update = res?;
                    match update {
                        VeilidUpdate::AppCall(veilid_app_call) => {
                            trace!("app_call: {:?}", veilid_app_call);
                            let req = proto::Request::decode(veilid_app_call.message())?;
                            match req {
                                proto::Request::BlockRequest(block_req) => {
                                    if self.piece_map.get(block_req.piece) {
                                        let rd = self.read_block_into(&block_req, &mut buf).await?;
                                        self.node.reply_block_contents(veilid_app_call.id(), Some(&buf[..rd])).await?;
                                    } else {
                                        self.node.reply_block_contents(veilid_app_call.id(), None).await?;
                                    }
                                }
                                _ => {}  // Ignore other request types
                            }
                        }
                        _ => {}
                    }
                }
                res = self.clients.discovered_peers_rx.recv_async() => {
                    let (key, peer_info) = res?;
                    if !self.clients.peer_tracker.lock().await.contains(peer_info.key()) {
                        debug!("discovered peer {} at {key}", peer_info.key());
                        self.clients.share_resolver_tx.send_async(share_resolver::Request::Index{
                            response_tx: ResponseChannel::Drop,
                            key,
                            want_index_digest: Some(self.share.want_index_digest),
                            root: self.share.root.to_owned(),
                        }).await.err().map(|e| {
                            warn!("failed to resolve share for discovered peer {} at {key}: {e}", peer_info.key());
                        });
                        self.clients.peer_resolver_tx.send_async(peer_resolver::Request::Watch{
                            response_tx: ResponseChannel::Drop,
                            key: peer_info.key().to_owned(),
                        }).await.err().map(|e| {
                            warn!("failed to watch peers of discovered peer {} at {key}: {e}", peer_info.key());
                        });
                    }
                }
            }
        }
    }

    #[tracing::instrument(skip_all, err(level = Level::TRACE), level = Level::TRACE)]
    async fn handle_request(&mut self, req: Self::Request) -> Result<()> {
        match req {
            Request::HaveMap { mut response_tx } => {
                let resp = Response::HaveMap(self.piece_map.clone());
                response_tx.send(resp).await?;
                Ok(())
            }
        }
    }
}

impl<P: Node> Clone for Seeder<P> {
    fn clone(&self) -> Self {
        Self {
            node: self.node.clone(),
            share: self.share.clone(),
            clients: self.clients.clone(),
            files: HashMap::new(),
            piece_map: self.piece_map.clone(),
        }
    }
}

pub struct Clients {
    pub verified_rx: flume::Receiver<PieceState>,
    pub update_rx: broadcast::Receiver<VeilidUpdate>,
    pub peer_tracker: Arc<Mutex<PeerTracker>>,
    pub discovered_peers_rx: flume::Receiver<(TypedKey, PeerInfo)>,
    pub share_resolver_tx: flume::Sender<share_resolver::Request>,
    pub peer_resolver_tx: flume::Sender<peer_resolver::Request>,
}

impl Clone for Clients {
    fn clone(&self) -> Self {
        Self {
            verified_rx: self.verified_rx.clone(),
            update_rx: self.update_rx.resubscribe(),
            peer_tracker: self.peer_tracker.clone(),
            discovered_peers_rx: self.discovered_peers_rx.clone(),
            share_resolver_tx: self.share_resolver_tx.clone(),
            peer_resolver_tx: self.peer_resolver_tx.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        str::FromStr,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use stigmerge_fileindex::Index;
    use tokio::{sync::mpsc, time};
    use tokio_util::sync::CancellationToken;
    use veilid_core::{OperationId, TypedKey, VeilidAppCall};

    use crate::{
        actor::{OneShot, Operator, ResponseChannel},
        proto::{BlockRequest, Encoder, Header},
        tests::{temp_file, StubNode},
    };

    use super::*;

    fn test_header() -> Header {
        Header::new([0xab; 32], 42, 1, [0xcd; 99].as_slice(), None, None)
    }

    #[tokio::test]
    async fn test_seeder_block_request_for_verified_piece() {
        // Create a test file with known content
        const BLOCK_DATA: u8 = 0xa5;
        let tf = temp_file(BLOCK_DATA, BLOCK_SIZE_BYTES * PIECE_SIZE_BLOCKS * 2); // 2 pieces
        let tf_path = tf.path().to_path_buf();
        let root_dir = tf_path.parent().unwrap().to_path_buf();

        // Create a mock index
        let index = create_test_index(&tf_path).await;

        // Set up channels
        let (verified_tx, verified_rx) = flume::bounded(16);

        // Create a stub peer with mock reply_block_contents
        let mut node = StubNode::new();
        let update_tx = node.update_tx.clone();
        let reply_contents_called = Arc::new(Mutex::new(false));
        let reply_contents_data = Arc::new(Mutex::new(Vec::new()));
        let reply_contents_called_clone = reply_contents_called.clone();
        let reply_contents_data_clone = reply_contents_data.clone();

        let (replied_tx, mut replied_rx) = mpsc::channel(1);

        node.reply_block_contents_result = Arc::new(Mutex::new(
            move |_call_id: OperationId, contents: Option<&[u8]>| {
                *reply_contents_called_clone.lock().unwrap() = true;
                *reply_contents_data_clone.lock().unwrap() = match contents {
                    Some(contents) => contents.to_vec(),
                    None => vec![],
                };
                replied_tx.try_send(()).expect("replied");
                Ok(())
            },
        ));

        let fake_key =
            TypedKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");

        // Create share info
        let share_info = ShareInfo {
            key: fake_key,
            header: test_header(),
            want_index: index,
            want_index_digest: [0u8; 32],
            root: root_dir,
        };

        let (_discovered_peers_tx, discovered_peers_rx) = flume::unbounded();
        let (share_resolver_tx, _share_resolver_rx) = flume::unbounded();
        let (peer_resolver_tx, _peer_resolver_rx) = flume::unbounded();

        // Create clients
        let clients = Clients {
            verified_rx,
            update_rx: node.update_tx.subscribe(),
            peer_tracker: Arc::new(tokio::sync::Mutex::new(PeerTracker::new())),
            discovered_peers_rx,
            share_resolver_tx,
            peer_resolver_tx,
        };

        // Create cancellation token
        let cancel = CancellationToken::new();
        let mut operator = Operator::new(
            cancel.clone(),
            Seeder::new(node, share_info, clients),
            OneShot,
        );

        // First, send a verified piece notification with confirmation it's applied
        let piece_state = PieceState::new(0, 0, 0, PIECE_SIZE_BLOCKS, PIECE_SIZE_BLOCKS - 1);
        verified_tx
            .send_async(piece_state)
            .await
            .expect("send verified piece");

        // Have map should be updated. This should be a certainty with biased
        // select! behavior.
        let req = Request::HaveMap {
            response_tx: ResponseChannel::default(),
        };
        let resp = operator.call(req).await.expect("call havemap");
        match resp {
            Response::HaveMap(have_map) => {
                assert!(!have_map.is_empty(), "confirm verified block");
            }
        }

        // Send a block request for the verified piece
        let block_req = BlockRequest { piece: 0, block: 0 };
        let req = proto::Request::BlockRequest(block_req);
        let encoded_req = req.encode().expect("encode request");
        let app_call = VeilidAppCall::new(None, None, encoded_req, 42u64.into());
        update_tx
            .send(VeilidUpdate::AppCall(Box::new(app_call)))
            .expect("send app call");

        time::timeout(Duration::from_secs(1), replied_rx.recv())
            .await
            .expect("replied");

        // Cancel the seeder
        cancel.cancel();
        operator.join().await.expect("seeder task");

        // Verify reply_block_contents was called
        assert!(
            *reply_contents_called.lock().unwrap(),
            "reply_block_contents should be called"
        );

        // Verify the data returned matches what we expect
        let reply_data = reply_contents_data.lock().unwrap();
        assert_eq!(
            reply_data.len(),
            BLOCK_SIZE_BYTES,
            "should return full block"
        );
        assert!(
            reply_data.iter().all(|&b| b == BLOCK_DATA),
            "all bytes should match the pattern"
        );
    }

    #[tokio::test]
    async fn test_seeder_block_request_for_unverified_piece() {
        // Create a test file with known content
        const BLOCK_DATA: u8 = 0xa5;
        let tf = temp_file(BLOCK_DATA, BLOCK_SIZE_BYTES * PIECE_SIZE_BLOCKS * 2); // 2 pieces
        let tf_path = tf.path().to_path_buf();
        let root_dir = tf_path.parent().unwrap().to_path_buf();

        // Create a mock index
        let index = create_test_index(&tf_path).await;

        // Set up channels
        let (_verified_tx, verified_rx) = flume::bounded(16);

        // Create a stub peer with mock reply_block_contents
        let mut node = StubNode::new();
        let update_tx = node.update_tx.clone();

        let reply_contents_called = Arc::new(Mutex::new(false));
        let reply_contents_data = Arc::new(Mutex::new(Vec::new()));
        let reply_contents_called_clone = reply_contents_called.clone();
        let reply_contents_data_clone = reply_contents_data.clone();

        let (replied_tx, mut replied_rx) = mpsc::channel(1);

        node.reply_block_contents_result = Arc::new(Mutex::new(
            move |_call_id: OperationId, contents: Option<&[u8]>| {
                *reply_contents_called_clone.lock().unwrap() = true;
                *reply_contents_data_clone.lock().unwrap() = match contents {
                    Some(contents) => contents.to_vec(),
                    None => vec![],
                };
                replied_tx.try_send(()).expect("replied");
                Ok(())
            },
        ));

        let fake_key =
            TypedKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");

        // Create share info
        let share_info = ShareInfo {
            key: fake_key,
            header: test_header(),
            want_index: index,
            want_index_digest: [0u8; 32],
            root: root_dir,
        };

        let (_discovered_peers_tx, discovered_peers_rx) = flume::unbounded();
        let (share_resolver_tx, _share_resolver_rx) = flume::unbounded();
        let (peer_resolver_tx, _peer_resolver_rx) = flume::unbounded();

        // Create clients
        let clients = Clients {
            verified_rx,
            update_rx: node.update_tx.subscribe(),
            peer_tracker: Arc::new(tokio::sync::Mutex::new(PeerTracker::new())),
            discovered_peers_rx,
            share_resolver_tx,
            peer_resolver_tx,
        };

        // Create seeder
        let cancel = CancellationToken::new();
        let operator = Operator::new(
            cancel.clone(),
            Seeder::new(node, share_info, clients),
            OneShot,
        );

        // Send a block request for an unverified piece
        let block_req = BlockRequest { piece: 0, block: 0 };
        let req = proto::Request::BlockRequest(block_req);
        let encoded_req = req.encode().expect("encode request");

        let app_call = VeilidAppCall::new(None, None, encoded_req, 42u64.into());

        update_tx
            .send(VeilidUpdate::AppCall(Box::new(app_call)))
            .expect("send app call");

        time::timeout(Duration::from_secs(1), replied_rx.recv())
            .await
            .expect("replied");

        // Cancel the seeder
        cancel.cancel();

        // Verify seeder exits cleanly
        operator.join().await.expect("seeder task");

        // Verify reply_block_contents was called
        assert!(
            *reply_contents_called.lock().unwrap(),
            "reply_block_contents should be called"
        );

        // Verify empty data was returned for unverified piece
        let reply_data = reply_contents_data.lock().unwrap();
        assert_eq!(
            reply_data.len(),
            0,
            "should return empty data for unverified piece"
        );
    }

    // Helper function to create a test index
    async fn create_test_index(file_path: &PathBuf) -> Index {
        // Create index from the file using Indexer
        let indexer = stigmerge_fileindex::Indexer::from_file(file_path.as_path())
            .await
            .expect("create indexer");
        indexer.index().await.expect("create index")
    }
}
