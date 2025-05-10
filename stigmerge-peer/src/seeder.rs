use std::{collections::HashMap, path::PathBuf};

use stigmerge_fileindex::{Index, BLOCK_SIZE_BYTES, PIECE_SIZE_BLOCKS};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
    select,
    sync::broadcast,
};
use tokio_util::sync::CancellationToken;
use tracing::Level;
use veilid_core::VeilidUpdate;

use crate::{
    actor::{Actor, ChanServer},
    piece_map::PieceMap,
    proto::{self, BlockRequest, Decoder},
    types::{PieceState, ShareInfo},
    Node, Result,
};

#[derive(Debug, Clone)]
pub enum Request {
    HaveMap,
}

#[derive(Debug, Clone)]
pub enum Response {
    HaveMap(PieceMap),
}

pub struct Seeder<N: Node> {
    node: N,
    want_index: Index,
    root: PathBuf,
    clients: Clients,
    files: HashMap<u32, File>,
    piece_map: PieceMap,
}

impl<N: Node> Seeder<N> {
    #[tracing::instrument(skip_all)]
    pub fn new(node: N, share: ShareInfo, clients: Clients) -> Self {
        Seeder {
            node,
            want_index: share.want_index,
            root: share.root,
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
            let file_path = self.root.join(self.want_index.files()[0].path());
            let fh = File::open(file_path).await?;
            self.files.insert(block_req.piece, fh);
        }
        Ok(self.files.get_mut(&block_req.piece).unwrap())
    }
}

impl<P: Node> Actor for Seeder<P> {
    type Request = Request;
    type Response = Response;

    #[tracing::instrument(skip_all, err, level = Level::TRACE)]
    async fn run(
        &mut self,
        cancel: CancellationToken,
        mut server_ch: ChanServer<Self::Request, Self::Response>,
    ) -> Result<()> {
        let mut buf = [0u8; BLOCK_SIZE_BYTES];
        loop {
            select! { biased;
                _ = cancel.cancelled() => {
                    return Ok(());
                }
                Some(req) = server_ch.recv() => {
                    server_ch.send(self.handle(&req).await?).await?;
                }
                res = self.clients.verified_rx.recv() => {
                    let piece_state = res?;
                    self.piece_map.set(piece_state.piece_index.try_into().unwrap());
                }
                res = self.clients.update_rx.recv() => {
                    let update = res?;
                    match update {
                        VeilidUpdate::AppCall(veilid_app_call) => {
                            let req = proto::Request::decode(veilid_app_call.message())?;
                            match req {
                                proto::Request::BlockRequest(block_req) => {
                                    if self.piece_map.get(block_req.piece) {
                                        let rd = self.read_block_into(&block_req, &mut buf).await?;
                                        self.node.reply_block_contents(veilid_app_call.id(), &buf[..rd]).await?;
                                    } else {
                                        self.node.reply_block_contents(veilid_app_call.id(), &[]).await?;
                                    }
                                }
                                _ => {}  // Ignore other request types
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    #[tracing::instrument(skip_all, err, level = Level::TRACE)]
    async fn handle(&mut self, req: &Self::Request) -> Result<Self::Response> {
        match req {
            Request::HaveMap => Ok(Response::HaveMap(self.piece_map.clone())),
        }
    }
}

impl<P: Node> Clone for Seeder<P> {
    fn clone(&self) -> Self {
        Self {
            node: self.node.clone(),
            want_index: self.want_index.clone(),
            root: self.root.clone(),
            clients: self.clients.clone(),
            files: HashMap::new(),
            piece_map: self.piece_map.clone(),
        }
    }
}

pub struct Clients {
    pub verified_rx: broadcast::Receiver<PieceState>,
    pub update_rx: broadcast::Receiver<VeilidUpdate>,
}

impl Clone for Clients {
    fn clone(&self) -> Self {
        Self {
            verified_rx: self.verified_rx.resubscribe(),
            update_rx: self.update_rx.resubscribe(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };

    use tokio::{sync::mpsc, time};
    use tokio_util::sync::CancellationToken;
    use veilid_core::{OperationId, VeilidAppCall};

    use crate::{
        actor::{OneShot, Operator},
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
        let (verified_tx, _) = broadcast::channel(16);

        // Create a stub peer with mock reply_block_contents
        let mut node = StubNode::new();
        let update_tx = node.update_tx.clone();
        let reply_contents_called = Arc::new(Mutex::new(false));
        let reply_contents_data = Arc::new(Mutex::new(Vec::new()));
        let reply_contents_called_clone = reply_contents_called.clone();
        let reply_contents_data_clone = reply_contents_data.clone();

        let (replied_tx, mut replied_rx) = mpsc::channel(1);

        node.reply_block_contents_result =
            Arc::new(Mutex::new(move |_call_id: OperationId, contents: &[u8]| {
                *reply_contents_called_clone.lock().unwrap() = true;
                reply_contents_data_clone
                    .lock()
                    .unwrap()
                    .extend_from_slice(contents);
                replied_tx.try_send(()).expect("replied");
                Ok(())
            }));

        // Create share info
        let share_info = ShareInfo {
            header: test_header(),
            want_index: index,
            root: root_dir,
        };

        // Create clients
        let clients = Clients {
            verified_rx: verified_tx.subscribe(),
            update_rx: node.update_tx.subscribe(),
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

        operator
            .send(Request::HaveMap)
            .await
            .expect("requested havemap");
        let Response::HaveMap(have_map) = operator.recv().await.expect("havemap response");
        assert!(!have_map.is_empty(), "confirm verified block");

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
        operator
            .join()
            .await
            .expect("seeder task")
            .expect("seeder run");

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

        node.reply_block_contents_result =
            Arc::new(Mutex::new(move |_call_id: OperationId, contents: &[u8]| {
                *reply_contents_called_clone.lock().unwrap() = true;
                reply_contents_data_clone
                    .lock()
                    .unwrap()
                    .extend_from_slice(contents);
                replied_tx.try_send(()).expect("replied");
                Ok(())
            }));

        // Create share info
        let share_info = ShareInfo {
            header: test_header(),
            want_index: index,
            root: root_dir,
        };

        // Create clients
        let clients = Clients {
            verified_rx,
            update_rx: node.update_tx.subscribe(),
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
        let result = operator.join().await.expect("seeder task");
        assert!(
            result.is_ok(),
            "seeder should exit cleanly after handling block request"
        );

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
