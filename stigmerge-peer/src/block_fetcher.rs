use std::cmp::min;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;

use stigmerge_fileindex::{Index, BLOCK_SIZE_BYTES};
use tokio::fs::File;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::select;
use tokio::sync::{watch, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{trace, Level};
use veilid_core::Target;

use crate::actor::{Actor, ChanServer};
use crate::error::Result;
use crate::is_cancelled;
use crate::node::Node;
use crate::Error;

use super::types::FileBlockFetch;

pub struct BlockFetcher<N: Node> {
    node: N,
    want_index: Arc<RwLock<Index>>,
    root: PathBuf,
    target_update_rx: watch::Receiver<Option<Target>>,
    target: Option<Target>,
    files: HashMap<usize, File>,
}

impl<N: Node> BlockFetcher<N> {
    /// Creates a new block fetcher service.
    pub fn new(
        node: N,
        want_index: Arc<RwLock<Index>>,
        root: PathBuf,
        target_update_rx: watch::Receiver<Option<Target>>,
    ) -> Self {
        Self {
            node,
            want_index,
            root,
            target_update_rx,
            target: None,
            files: HashMap::new(),
        }
    }

    #[tracing::instrument(skip_all, err)]
    async fn fetch_block(&mut self, block: &FileBlockFetch, flush: bool) -> Result<usize> {
        // Request block from peer with retry logic
        let result = self
            .node
            .request_block(self.target.unwrap(), block.piece_index, block.block_index)
            .await?
            .ok_or(Error::msg("block not found"))?;
        // Write the block to the file
        let fh = match self.files.get_mut(&block.file_index) {
            Some(fh) => fh,
            None => {
                let path = self
                    .root
                    .join(self.want_index.read().await.files()[block.file_index].path());
                let fh = File::options()
                    .write(true)
                    .truncate(false)
                    .create(true)
                    .open(path)
                    .await?;
                self.files.insert(block.file_index, fh);
                self.files.get_mut(&block.file_index).unwrap()
            }
        };
        fh.seek(SeekFrom::Start(block.block_offset() as u64))
            .await?;
        let block_end = min(result.len(), BLOCK_SIZE_BYTES);
        fh.write_all(&result[0..block_end]).await?;
        if flush {
            fh.flush().await?;
        }
        Ok(block_end)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Request {
    Fetch { block: FileBlockFetch, flush: bool },
}

impl Request {
    fn block(&self) -> FileBlockFetch {
        match self {
            Request::Fetch { block, .. } => block.to_owned(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Response {
    Fetched {
        block: FileBlockFetch,
        length: usize,
    },
    FetchFailed {
        block: FileBlockFetch,
        error_msg: String,
    },
}

impl<P: Node + Send> Actor for BlockFetcher<P> {
    type Request = Request;
    type Response = Response;

    #[tracing::instrument(skip_all, err(level = Level::TRACE), level = Level::TRACE)]
    async fn run(
        &mut self,
        cancel: CancellationToken,
        mut server_ch: ChanServer<Self::Request, Self::Response>,
    ) -> Result<()> {
        self.target = None;
        loop {
            if let None = self.target {
                select! {
                     _ = cancel.cancelled() => {
                         return Ok(())
                     }
                     res = self.target_update_rx.changed() => {
                         if let Err(e) = res {
                             if cancel.is_cancelled() {
                                 return Ok(());
                             }
                             return Err(e.into());
                         }
                         self.target = *self.target_update_rx.borrow_and_update();
                     }
                }
            }

            select! {
                _ = cancel.cancelled() => {
                    return Ok(())
                }
                res = self.target_update_rx.changed() => {
                    if let Err(e) = res {
                        if cancel.is_cancelled() {
                            return Ok(());
                        }
                        return Err(e.into());
                    }
                    self.target = *self.target_update_rx.borrow_and_update();
                }
                req = server_ch.recv() => {
                    match req {
                        None => return Ok(()),
                        Some(req) => {
                            match self.handle(&req).await {
                                Ok(resp) => server_ch.send(resp).await?,
                                Err(e) => {
                                    if is_cancelled(&e) {
                                        return Ok(());
                                    }
                                    // Whether the fetch was successful or not, respond back to the caller.
                                    let resp = Response::FetchFailed { block: req.block(), error_msg: e.to_string() };
                                    server_ch.send(resp).await?;
                                    return Err(e);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    #[tracing::instrument(skip_all, err(level = Level::TRACE), level = Level::TRACE)]
    async fn handle(&mut self, req: &Self::Request) -> Result<Self::Response> {
        if self.target.is_none() {
            return Ok(Response::FetchFailed {
                block: req.block().to_owned(),
                error_msg: "fetch target not available yet".to_owned(),
            });
        }
        match req {
            Request::Fetch { block, flush } => {
                trace!(
                    "Fetching block: file={} piece={} block={}",
                    block.file_index,
                    block.piece_index,
                    block.block_index
                );

                // Attempt to fetch the block
                let length = self.fetch_block(block, *flush).await?;
                Ok(Response::Fetched {
                    block: block.clone(),
                    length,
                })
            }
        }
    }
}

impl<P: Node> Clone for BlockFetcher<P> {
    fn clone(&self) -> Self {
        Self {
            node: self.node.clone(),
            want_index: self.want_index.clone(),
            root: self.root.clone(),
            target_update_rx: self.target_update_rx.clone(),
            target: self.target,
            files: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, sync::Mutex};

    use stigmerge_fileindex::Indexer;
    use tokio::io::AsyncReadExt;
    use tokio_util::sync::CancellationToken;
    use veilid_core::CryptoKey;

    use crate::actor::{OneShot, Operator};
    use crate::tests::{temp_file, StubNode};
    use crate::types::FileBlockFetch;
    use crate::Error;

    use super::*;

    impl Response {
        fn block(&self) -> FileBlockFetch {
            match self {
                Response::Fetched { block, .. } => block.to_owned(),
                Response::FetchFailed { block, .. } => block.to_owned(),
            }
        }
    }

    #[tokio::test]
    async fn fetch_single_block() {
        const BLOCK_DATA: u8 = 0xa5;

        // Create a test file with known content
        let tf = temp_file(BLOCK_DATA, BLOCK_SIZE_BYTES * 2); // 2 blocks
        let tf_path = tf.path().to_path_buf();

        // Create index from the file
        let indexer = Indexer::from_file(tf_path.as_path())
            .await
            .expect("indexer");
        let index = indexer.index().await.expect("index");

        // Delete the temp file - we'll verify BlockFetcher recreates it
        drop(tf);

        // Set up BlockFetcher
        let mut node = StubNode::new();
        // Mock the block data that StubPeer will return
        node.request_block_result = Arc::new(Mutex::new(move |_, _, _| {
            Ok(Some(vec![BLOCK_DATA; BLOCK_SIZE_BYTES]))
        }));

        let (target_tx, target_rx) = watch::channel(None);
        let fetcher_root = tf_path.parent().unwrap().to_path_buf();

        // Start BlockFetcher
        let cancel = CancellationToken::new();
        let mut operator = Operator::new(
            cancel.clone(),
            BlockFetcher::new(node, Arc::new(RwLock::new(index)), fetcher_root, target_rx),
            OneShot,
        );

        // Send target update
        target_tx.send_replace(Some(Target::PrivateRoute(CryptoKey::new([0xbe; 32]))));

        // Request first block
        let block = FileBlockFetch {
            file_index: 0,
            piece_index: 0,
            block_index: 0,
            piece_offset: 0,
        };
        let req = Request::Fetch {
            block: block.clone(),
            flush: true,
        };
        operator.send(req).await.expect("send request");

        // Verify response
        let resp = operator.recv().await.expect("receive response");
        assert!(
            matches!(
                resp,
                Response::Fetched {
                    block: _,
                    length: _
                }
            ),
            "expected fetched response, got {:?}",
            resp
        );
        assert_eq!(resp.block(), block);
        if let Response::Fetched { length, .. } = resp {
            assert_eq!(length, BLOCK_SIZE_BYTES);
        } else {
            assert!(false, "expected fetched response, got {:?}", resp);
        }

        // Verify the file was recreated with correct content
        let mut recreated_file = File::open(tf_path).await.expect("open recreated file");
        let mut buf = vec![0u8; BLOCK_SIZE_BYTES];
        recreated_file
            .read_exact(&mut buf)
            .await
            .expect("read block");
        assert_eq!(buf, vec![BLOCK_DATA; BLOCK_SIZE_BYTES]);

        // Clean up
        cancel.cancel();
        operator
            .join()
            .await
            .expect("fetcher task")
            .expect("fetch ok");
    }

    #[tokio::test]
    async fn test_fetch_block_error() {
        const BLOCK_DATA: u8 = 0xa5;

        // Create a test file with known content
        let tf = temp_file(BLOCK_DATA, BLOCK_SIZE_BYTES * 2);
        let tf_path = tf.path().to_path_buf();

        // Create index from the file
        let indexer = Indexer::from_file(tf_path.as_path())
            .await
            .expect("indexer");
        let index = indexer.index().await.expect("index");

        // Delete the temp file
        drop(tf);

        // Set up BlockFetcher with error-returning stub
        let mut node = StubNode::new();
        // Mock the block request to return an error
        node.request_block_result = Arc::new(Mutex::new(
            move |_, _, _| -> crate::Result<Option<Vec<u8>>> {
                Err(Error::msg("mock block fetch error"))
            },
        ));

        let (target_tx, target_rx) = watch::channel(None);
        let fetcher_root = tf_path.parent().unwrap().to_path_buf();

        let cancel = CancellationToken::new();
        let mut operator = Operator::new(
            cancel.clone(),
            BlockFetcher::new(node, Arc::new(RwLock::new(index)), fetcher_root, target_rx),
            OneShot,
        );

        // Send target update
        target_tx.send_replace(Some(Target::PrivateRoute(CryptoKey::new([0xbe; 32]))));

        // Request first block
        let block = FileBlockFetch {
            file_index: 0,
            piece_index: 0,
            block_index: 0,
            piece_offset: 0,
        };
        let req = Request::Fetch {
            block: block.clone(),
            flush: true,
        };
        operator.send(req).await.expect("send request");

        // Verify we get a FetchFailed response with our error
        let resp = operator.recv().await.expect("receive response");
        match resp {
            Response::FetchFailed {
                block: failed_block,
                error_msg,
            } => {
                assert_eq!(failed_block, block);
                assert_eq!(error_msg, "mock block fetch error");
            }
            _ => panic!("expected FetchFailed response, got {:?}", resp),
        }

        // Clean up
        cancel.cancel();
        operator
            .join()
            .await
            .expect("fetcher task")
            .expect_err("mock block fetch error");
    }

    #[tokio::test]
    async fn test_fetch_no_target() {
        const BLOCK_DATA: u8 = 0xa5;

        // Create a test file with known content
        let tf = temp_file(BLOCK_DATA, BLOCK_SIZE_BYTES * 2); // 2 blocks
        let tf_path = tf.path().to_path_buf();

        // Create index from the file
        let indexer = Indexer::from_file(tf_path.as_path())
            .await
            .expect("indexer");
        let index = indexer.index().await.expect("index");

        // Delete the temp file - we'll verify BlockFetcher recreates it
        drop(tf);

        // Set up BlockFetcher
        let node = StubNode::new();
        let (_, target_rx) = watch::channel(None);
        let cancel = CancellationToken::new();
        let fetcher = BlockFetcher::new(node, Arc::new(RwLock::new(index)), tf_path, target_rx);
        let mut operator = Operator::new(cancel.clone(), fetcher, OneShot);

        // Request a block without sending target update first
        operator
            .send(Request::Fetch {
                block: FileBlockFetch {
                    file_index: 0,
                    piece_index: 0,
                    piece_offset: 0,
                    block_index: 0,
                },
                flush: false,
            })
            .await
            .expect("send request");

        // Client has not responded to requests
        assert_eq!(operator.recv_pending().await, 0);

        // Clean up
        cancel.cancel();
        operator
            .join()
            .await
            .expect("fetcher task")
            .expect("fetcher run, cancelled");
    }
}
