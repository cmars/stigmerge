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
use tracing::trace;
use veilid_core::VeilidUpdate;

use crate::{
    actor::{Actor, Respondable, ResponseChannel},
    error::{CancelError, Unrecoverable},
    piece_map::PieceMap,
    proto::{self, BlockRequest, Decoder},
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
    verified_rx: broadcast::Receiver<PieceState>,

    share: ShareInfo,

    files: HashMap<u32, File>,
    piece_map: PieceMap,
}

impl<N: Node> Seeder<N> {
    pub fn new(node: N, share: ShareInfo, verified_rx: broadcast::Receiver<PieceState>) -> Self {
        Seeder {
            node,
            share,
            verified_rx,

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

    async fn run(
        &mut self,
        cancel: CancellationToken,
        request_rx: flume::Receiver<Self::Request>,
    ) -> Result<()> {
        self.files.clear();
        let mut update_rx = self.node.subscribe_veilid_update();
        let mut buf = [0u8; BLOCK_SIZE_BYTES];
        loop {
            select! {
                _ = cancel.cancelled() => {
                    return Err(CancelError.into());
                }
                res = request_rx.recv_async() => {
                    let req = res.with_context(|| format!("seeder: receive request"))?;
                    self.handle_request(req).await?;
                }
                res = self.verified_rx.recv() => {
                    let piece_state = res.context(Unrecoverable::new("receive verified piece update"))?;
                    self.piece_map.set(piece_state.piece_index.try_into().unwrap());
                }
                res = update_rx.recv() => {
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
                        VeilidUpdate::Shutdown => {
                            cancel.cancel();
                        }
                        _ => {}
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

pub struct Clients {}

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
        seeder,
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

        // Create cancellation token
        let cancel = CancellationToken::new();
        let mut operator = Operator::new(
            cancel.clone(),
            Seeder::new(node, share_info, verified_rx),
            OneShot,
        );

        // First, send a verified piece notification with confirmation it's applied
        let piece_state = PieceState::new(0, 0, 0, PIECE_SIZE_BLOCKS, PIECE_SIZE_BLOCKS - 1);
        verified_tx.send(piece_state).expect("send verified piece");

        // Have map should be updated. Retry with a backoff delay to allow
        // select! to pick up on the request.
        let mut confirmed_have_map = false;
        for i in 0..10 {
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
            time::sleep(Duration::from_secs(i)).await;
        }
        assert!(confirmed_have_map, "confirm verified block");

        // Send a block request for the verified piece
        let block_req = BlockRequest { piece: 0, block: 0 };
        let req = proto::Request::BlockRequest(block_req);
        let encoded_req = req.encode().expect("encode request");
        let app_call = VeilidAppCall::new(None, None, encoded_req, 42u64.into());

        update_tx
            .send(VeilidUpdate::AppCall(Box::new(app_call.clone())))
            .expect("send app call");
        time::timeout(Duration::from_secs(10), replied_rx.recv())
            .await
            .expect("confirm app_call");

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
        let _update_rx = update_tx.subscribe();

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

        // Create seeder
        let mut operator = Operator::new(
            cancel.clone(),
            Seeder::new(node, share_info, verified_rx),
            OneShot,
        );

        // Send a block request for an unverified piece
        let block_req = BlockRequest { piece: 0, block: 0 };
        let req = proto::Request::BlockRequest(block_req);
        let encoded_req = req.encode().expect("encode request");

        let app_call = VeilidAppCall::new(None, None, encoded_req, 42u64.into());

        // Making a synchronous call to the seeder ensures we're in the event
        // loop and reacting to app_calls.
        operator
            .call(seeder::Request::HaveMap {
                response_tx: ResponseChannel::default(),
            })
            .await
            .expect("call");

        update_tx
            .send(VeilidUpdate::AppCall(Box::new(app_call.clone())))
            .expect("send app call");
        time::timeout(Duration::from_secs(10), replied_rx.recv())
            .await
            .expect("confirm app_call");

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
