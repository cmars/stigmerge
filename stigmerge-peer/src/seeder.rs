use std::{collections::HashMap, sync::Arc, time::Duration};

use stigmerge_fileindex::{BLOCK_SIZE_BYTES, PIECE_SIZE_BLOCKS};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
    select,
    sync::Mutex,
    task::JoinSet,
    time::interval,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, trace, warn};
use veilid_core::{OperationId, VeilidAppCall};
use veilnet::{
    connection::{RoutingContext, UpdateHandler, API},
    Connection,
};

use crate::{
    error::CancelError,
    piece_verifier::{PieceStatus, PieceStatusNotifier, PieceVerifier},
    proto::{self, BlockRequest, Decoder},
    record::StableHaveMap,
    types::LocalShareInfo,
    Result, Retry,
};

pub struct Seeder<C: Connection> {
    verifier: PieceVerifier,
    verified_rx: flume::Receiver<PieceStatus>,
    have_map: StableHaveMap,

    inner: Arc<Mutex<SeederInner<C>>>,
}

impl<C: Connection + Send + Sync + 'static> Seeder<C> {
    pub async fn new(mut conn: C, share: LocalShareInfo, verifier: PieceVerifier) -> Result<Self> {
        let (status_handler, verified_rx) = PieceStatusNotifier::new();
        verifier.subscribe(Box::new(status_handler)).await;

        let have_map = StableHaveMap::new_local(&mut conn, &share.want_index).await?;

        Ok(Seeder {
            verifier,
            verified_rx,
            have_map,
            inner: Arc::new(Mutex::new(SeederInner::new(conn, share))),
        })
    }

    pub async fn run(mut self, cancel: CancellationToken, retry: Retry) -> Result<()> {
        let block_request_rx = {
            let inner = self.inner.lock().await;
            let (block_req_handler, block_request_rx) = BlockRequestHandler::new(cancel.clone());
            inner.conn.add_update_handler(Box::new(block_req_handler));
            block_request_rx
        };
        let mut tasks = JoinSet::new();
        let mut have_map_sync_interval = interval(Duration::from_secs(30));
        loop {
            select! {
                _ = cancel.cancelled() => {
                    tasks.abort_all();
                    return Err(CancelError.into());
                }
                res = self.verified_rx.recv_async() => {
                    match res {
                        Err(err) => {
                            // TODO: this shouldn't happen
                            warn!(?err, "receive verified piece update");
                            let (status_handler, verified_rx) = PieceStatusNotifier::new();
                            self.verifier.subscribe(Box::new(status_handler)).await;
                            self.verified_rx = verified_rx;
                            warn!("resubscribed to piece verifier");
                            continue;

                        }
                        Ok(piece_state) => {
                            let piece_index = piece_state.piece_index().try_into().unwrap();
                            self.have_map.update_piece(piece_index, true).await?;
                        }
                    };
                }
                res = block_request_rx.recv_async() => {
                    let (app_call_id, block_req) = res?;
                    backoff_retry!(cancel, retry, {
                        let block_req = block_req.clone();
                        trace!("app_call: {:?}", block_req);
                        if self.have_map.has_piece(block_req.piece) {
                            let mut buf = [0u8; BLOCK_SIZE_BYTES];
                            let inner = self.inner.clone();
                            let cancel = cancel.child_token();
                            tasks.spawn(async move {
                                let read_reply = async {
                                    // TODO: improve app_call handler concurrency here?
                                    let mut inner = inner.lock().await;
                                    let rd = inner.read_block_into(&block_req, &mut buf).await?;
                                    inner.reply_block_contents(app_call_id, Some(&buf[..rd])).await?;
                                    Ok::<(), anyhow::Error>(())
                                };
                                select! {
                                    _ = cancel.cancelled() => {
                                        Err(CancelError.into())
                                    }
                                    res = read_reply => {
                                        res
                                    }
                                }
                            });
                        } else {
                            self.inner.lock().await.reply_block_contents(app_call_id, None).await?;
                        }
                    }, {
                        // In the event we've gotten stuck retrying an error,
                        // flush the file handle cache for good measure.
                        self.inner.lock().await.flush_file_cache();
                    })?;
                }
                _ = have_map_sync_interval.tick() => {
                    self.have_map.sync(&mut self.inner.lock().await.conn).await?;
                }
            }
        }
    }
}

struct BlockRequestHandler {
    cancel: CancellationToken,
    block_request_tx: flume::Sender<(OperationId, BlockRequest)>,
}

impl BlockRequestHandler {
    fn new(cancel: CancellationToken) -> (Self, flume::Receiver<(OperationId, BlockRequest)>) {
        let (block_request_tx, block_request_rx) = flume::unbounded();
        (
            Self {
                cancel,
                block_request_tx,
            },
            block_request_rx,
        )
    }
}

impl UpdateHandler for BlockRequestHandler {
    fn app_call(&self, app_call: &VeilidAppCall) {
        match proto::Request::decode(app_call.message()) {
            Ok(proto::Request::BlockRequest(block_req)) => {
                let _ = self
                    .block_request_tx
                    .send((app_call.id(), block_req))
                    .map_err(|err| {
                        error!(?err, "send block request to seeder");
                        self.cancel.cancel();
                        err
                    });
            }
            Ok(_) => {}
            Err(err) => {
                warn!(?err, "invalid app_call");
            }
        }
    }
    fn shutdown(&self) {
        trace!("shutdown");
        self.cancel.cancel();
    }
}

struct SeederInner<C: Connection> {
    conn: C,
    share: LocalShareInfo,
    files: HashMap<u32, File>,
}

impl<C: Connection> SeederInner<C> {
    fn new(conn: C, share: LocalShareInfo) -> Self {
        Self {
            conn,
            share,
            files: HashMap::new(),
        }
    }

    fn flush_file_cache(&mut self) {
        self.files.clear();
    }

    async fn reply_block_contents(
        &mut self,
        call_id: OperationId,
        contents: Option<&[u8]>,
    ) -> Result<()> {
        self.conn.require_attachment().await?;
        self.conn
            .routing_context()
            .api()
            .app_call_reply(call_id, contents.unwrap_or(&[]).to_vec())
            .await?;
        Ok(())
    }

    async fn read_block_into(&mut self, block_req: &BlockRequest, buf: &mut [u8]) -> Result<usize> {
        let fh = self.get_file_for_block(block_req).await?;
        fh.seek(std::io::SeekFrom::Start(
            ((TryInto::<usize>::try_into(block_req.piece).unwrap()
                * PIECE_SIZE_BLOCKS
                * BLOCK_SIZE_BYTES)
                + (Into::<usize>::into(block_req.block) * BLOCK_SIZE_BYTES))
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

#[cfg(test)]
#[cfg(feature = "refactor")]
mod tests {
    use std::{
        path::PathBuf,
        str::FromStr,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use stigmerge_fileindex::Index;
    use tokio::{
        runtime::{Builder, RngSeed},
        sync::mpsc,
        time,
    };
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

    #[test]
    fn test_seeder_block_request_for_verified_piece() {
        let seed = RngSeed::from_bytes(b"test");

        let rt = Builder::new_current_thread()
            .enable_time()
            .rng_seed(seed) // Apply the seed for deterministic polling order
            .build_local(Default::default())
            .unwrap();

        rt.block_on(async {
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

            let fake_key =
                TypedRecordKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M")
                    .expect("key");

            // Create share info
            let share_info = LocalShareInfo {
                key: fake_key,
                header: test_header(),
                want_index: index,
                want_index_digest: [0u8; 32],
                root: root_dir,
                have_map: PieceMap::new(),
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
            let req = Request::HaveMap {
                response_tx: ResponseChannel::default(),
            };
            let resp = operator.call(req).await.expect("call havemap");
            match resp {
                Response::HaveMap(have_map) => {
                    if !have_map.is_empty() {
                        confirmed_have_map = true;
                    }
                }
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

            // Verify that the data returned matches what we expect
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
        });
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
        let share_info = LocalShareInfo {
            key: fake_key,
            header: test_header(),
            want_index: index,
            want_index_digest: [0u8; 32],
            root: root_dir,
            have_map: PieceMap::new(),
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
