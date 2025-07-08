use std::collections::HashMap;

use anyhow::Context;
use stigmerge_fileindex::{BLOCK_SIZE_BYTES, PIECE_SIZE_BLOCKS};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
    select,
    sync::broadcast,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};
use veilid_core::{Target, Timestamp, TimestampDuration, TypedRecordKey, VeilidUpdate};

use crate::{
    actor::{Actor, Respondable, ResponseChannel},
    error::{CancelError, Unrecoverable},
    piece_map::PieceMap,
    proto::{self, BlockRequest, Decoder},
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
    clients: Clients,
    update_rx: broadcast::Receiver<VeilidUpdate>,

    share: ShareInfo,

    files: HashMap<u32, File>,
    known_peers: HashMap<TypedRecordKey, u16>,
    advertisements: HashMap<(TypedRecordKey, TypedRecordKey), Timestamp>,
    piece_map: PieceMap,

    announce_request_tx: flume::Sender<(TypedRecordKey, u16)>,
    announce_request_rx: flume::Receiver<(TypedRecordKey, u16)>,
}

impl<N: Node> Seeder<N> {
    pub fn new(node: N, share: ShareInfo, clients: Clients) -> Self {
        let update_rx = node.subscribe_veilid_update();
        let known_peers = [(share.key, 0u16)].into_iter().collect();
        let (announce_request_tx, announce_request_rx) = flume::unbounded();
        Seeder {
            node,
            clients,
            update_rx,
            share,
            files: HashMap::new(),
            known_peers,
            advertisements: HashMap::new(),
            piece_map: PieceMap::new(),

            announce_request_tx,
            announce_request_rx,
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

    async fn advertise_peers(&mut self, key: &TypedRecordKey, target: &Target) -> Result<()> {
        self.add_known_peer(key).await?;
        for known_peer in self.known_peers.keys() {
            // Don't advertise a key to itself
            if known_peer == key {
                continue;
            }
            // Don't advertise the same peer to the same target too often
            if let Some(last_adv_time) = self
                .advertisements
                .get(&(known_peer.to_owned(), key.to_owned()))
            {
                if Timestamp::now().saturating_sub(*last_adv_time) < TimestampDuration::new_secs(30)
                {
                    trace!("advertised {known_peer} to {key} too recently");
                    continue;
                }
            }
            // Attempt to advertise
            match self.node.request_advertise_peer(target, known_peer).await {
                Ok(()) => {
                    trace!("advertised {known_peer} to {key}");
                    self.advertisements
                        .insert((known_peer.to_owned(), key.to_owned()), Timestamp::now());
                }
                Err(e) => {
                    warn!("advertise {known_peer} to {key}: {e}");
                }
            }
        }
        Ok(())
    }

    async fn add_known_peers(&mut self) -> Result<()> {
        let known_peers = self.node.known_peers(&self.share.want_index_digest).await?;
        for key in known_peers.iter() {
            self.add_known_peer(key).await?;
            trace!("loaded known peer {key}");

            self.clients
                .share_resolver_tx
                .send_async(share_resolver::Request::Index {
                    response_tx: ResponseChannel::default(),
                    key: *key,
                    want_index_digest: Some(self.share.want_index_digest),
                    root: self.share.root.to_owned(),
                })
                .await
                .context(Unrecoverable::new("send share_resolver request"))?;
        }
        Ok(())
    }

    async fn add_known_peer(&mut self, key: &TypedRecordKey) -> Result<()> {
        if let None = self.known_peers.get(key) {
            let index = self.known_peers.len().try_into().unwrap();
            self.known_peers.insert(*key, index);
            self.announce_request_tx
                .send_async((*key, index))
                .await
                .context(Unrecoverable::new("schedule peer announce"))?;
        };
        Ok(())
    }
}

impl<P: Node> Actor for Seeder<P> {
    type Request = Request;
    type Response = Response;

    async fn run(
        &mut self,
        cancel: CancellationToken,
        request_rx: flume::Receiver<Self::Request>,
    ) -> Result<()> {
        let mut buf = [0u8; BLOCK_SIZE_BYTES];
        self.add_known_peers().await?;
        loop {
            select! {
                _ = cancel.cancelled() => {
                    return Err(CancelError.into());
                }
                res = request_rx.recv_async() => {
                    let req = res.with_context(|| format!("seeder: receive request"))?;
                    self.handle_request(req).await?;
                }
                res = self.clients.verified_rx.recv() => {
                    let piece_state = res.context(Unrecoverable::new("receive verified piece update"))?;
                    self.piece_map.set(piece_state.piece_index.try_into().unwrap());
                }
                res = self.clients.share_target_rx.recv() => {
                    let (key, target) = match res {
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        res => res
                    }.context(Unrecoverable::new("receive share target update"))?;
                    debug!("share target update for {key}: {target:?}");
                    self.advertise_peers(&key, &target).await?;
                }
                res = self.update_rx.recv() => {
                    let update = res.context(Unrecoverable::new("receive veilid update"))?;
                    match update {
                        VeilidUpdate::AppCall(veilid_app_call) => {
                            let req = proto::Request::decode(veilid_app_call.message())?;
                            match req {
                                proto::Request::BlockRequest(block_req) => {
                                    trace!("app_call: {:?}", block_req);
                                    if self.piece_map.get(block_req.piece) {
                                        let rd = self.read_block_into(&block_req, &mut buf).await?;
                                        self.node.reply_block_contents(veilid_app_call.id(), Some(&buf[..rd])).await?;
                                    } else {
                                        self.node.reply_block_contents(veilid_app_call.id(), None).await?;
                                    }
                                }
                                _ => {}
                            }
                        }
                        VeilidUpdate::AppMessage(veilid_app_message) => {
                            let req = proto::Request::decode(veilid_app_message.message())?;
                            match req {
                                proto::Request::AdvertisePeer(peer_req) => {
                                    debug!("discovered advertised peer {}", peer_req.key);
                                    self.clients.share_resolver_tx.send_async(share_resolver::Request::Index{
                                        response_tx: ResponseChannel::default(),
                                        key: peer_req.key,
                                        want_index_digest: Some(self.share.want_index_digest),
                                        root: self.share.root.to_owned(),
                                    }).await.context(Unrecoverable::new("send share_resolver request"))?;
                                }
                                _ => {}
                            }
                        }
                        VeilidUpdate::Shutdown => {
                            cancel.cancel();
                        }
                        _ => {}
                    }
                }
                res = self.announce_request_rx.recv_async() => {
                    let (peer_key, subkey) = res.context(Unrecoverable::new("receive peer announce"))?;
                    match self.node.announce_peer(&self.share.want_index_digest, Some(peer_key), subkey).await {
                        Ok(_) => {
                            trace!("announce peer {peer_key} subkey {subkey}");
                        }
                        Err(e) => {
                            warn!("announce peer {peer_key} subkey {subkey}: {e}");
                            self.announce_request_tx.send((peer_key, subkey)).context(Unrecoverable::new("requeue announce peer"))?;
                        }
                    }
                }
            }
        }
    }

    async fn handle_request(&mut self, req: Self::Request) -> Result<()> {
        match req {
            Request::HaveMap { mut response_tx } => {
                let resp = Response::HaveMap(self.piece_map.clone());
                response_tx
                    .send(resp)
                    .await
                    .context(Unrecoverable::new("send response from seeder"))?;
                Ok(())
            }
        }
    }
}

pub struct Clients {
    pub verified_rx: broadcast::Receiver<PieceState>,
    pub share_resolver_tx: flume::Sender<share_resolver::Request>,
    pub share_target_rx: broadcast::Receiver<(TypedRecordKey, Target)>,
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
    use veilid_core::{OperationId, TypedRecordKey, VeilidAppCall};

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
        let (verified_tx, verified_rx) = broadcast::channel(16);

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
        node.known_peers_result = Arc::new(Mutex::new(move |_index_digest: &[u8]| Ok(vec![])));

        let fake_key = TypedRecordKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M")
            .expect("key");

        // Create share info
        let share_info = ShareInfo {
            key: fake_key,
            header: test_header(),
            want_index: index,
            want_index_digest: [0u8; 32],
            root: root_dir,
        };

        // Create clients
        let (share_resolver_tx, _share_resolver_rx) = flume::unbounded();
        let (_share_target_tx, share_target_rx) = broadcast::channel(16);
        let clients = Clients {
            verified_rx,
            share_resolver_tx,
            share_target_rx,
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
        verified_tx.send(piece_state).expect("send verified piece");

        // Have map should be updated. Retry with a backoff delay to allow
        // select! to pick up on the request.
        let mut confirmed_have_map = false;
        for i in 0..30 {
            let req = Request::HaveMap {
                response_tx: ResponseChannel::default(),
            };
            let resp = operator.call(req).await.expect("call havemap");
            match resp {
                Response::HaveMap(have_map) => {
                    if !have_map.is_empty() {
                        confirmed_have_map = true;
                        break;
                    }
                }
            }
            time::sleep(Duration::from_millis(i)).await;
        }
        assert!(confirmed_have_map, "confirm verified block");

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
        operator.join().await.expect_err("cancelled");

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
        let (_verified_tx, verified_rx) = broadcast::channel(16);

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
        node.known_peers_result = Arc::new(Mutex::new(move |_index_digest: &[u8]| Ok(vec![])));

        let fake_key = TypedRecordKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M")
            .expect("key");

        // Create share info
        let share_info = ShareInfo {
            key: fake_key,
            header: test_header(),
            want_index: index,
            want_index_digest: [0u8; 32],
            root: root_dir,
        };

        // Create clients
        let cancel = CancellationToken::new();
        let (share_resolver_tx, _share_resolver_rx) = flume::unbounded();
        let (_share_target_tx, share_target_rx) = broadcast::channel(16);
        let clients = Clients {
            verified_rx,
            share_resolver_tx,
            share_target_rx,
        };

        // Create seeder
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
        operator.join().await.expect_err("cancelled");

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
