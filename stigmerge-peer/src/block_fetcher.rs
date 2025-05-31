use std::cmp::min;
use std::collections::HashMap;
use std::fmt;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use stigmerge_fileindex::{Index, BLOCK_SIZE_BYTES};
use tokio::fs::File;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::select;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::trace;
use veilid_core::Target;

use crate::actor::{Actor, Respondable, ResponseChannel};
use crate::error::{is_io, is_proto, is_route_invalid, Result, Transient};
use crate::is_cancelled;
use crate::node::{Node, TypedKey};
use crate::Error;

use super::types::FileBlockFetch;

pub struct BlockFetcher<N: Node> {
    node: N,
    want_index: Arc<RwLock<Index>>,
    root: PathBuf,
    files: HashMap<usize, File>,
}

impl<N: Node> BlockFetcher<N> {
    /// Creates a new block fetcher service.
    pub fn new(node: N, want_index: Arc<RwLock<Index>>, root: PathBuf) -> Self {
        Self {
            node,
            want_index,
            root,
            files: HashMap::new(),
        }
    }

    async fn fetch_block(
        &mut self,
        target: &Target,
        block: &FileBlockFetch,
        flush: bool,
    ) -> Result<usize> {
        // Request block from peer with retry logic
        let result = self
            .node
            .request_block(target.to_owned(), block.piece_index, block.block_index)
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

pub enum Request {
    Fetch {
        response_tx: ResponseChannel<Response>,
        share_key: TypedKey,
        target: Target,
        block: FileBlockFetch,
        flush: bool,
    },
}

impl fmt::Debug for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fetch {
                share_key,
                target,
                block,
                flush,
                ..
            } => f
                .debug_struct("Fetch")
                .field("share_key", share_key)
                .field("target", target)
                .field("block", block)
                .field("flush", flush)
                .finish(),
        }
    }
}

impl Respondable for Request {
    type Response = Response;

    fn set_response(&mut self, ch: ResponseChannel<Self::Response>) {
        match self {
            Request::Fetch { response_tx, .. } => *response_tx = ch,
        }
    }

    fn response_tx(self) -> ResponseChannel<Self::Response> {
        match self {
            Request::Fetch { response_tx, .. } => response_tx,
        }
    }
}

#[derive(Debug)]
pub enum Response {
    Fetched {
        share_key: TypedKey,
        block: FileBlockFetch,
        length: usize,
    },
    FetchFailed {
        share_key: TypedKey,
        block: FileBlockFetch,
        err: Error,
    },
}

impl<P: Node + Send> Actor for BlockFetcher<P> {
    type Request = Request;
    type Response = Response;

    async fn run(
        &mut self,
        cancel: CancellationToken,
        request_rx: flume::Receiver<Self::Request>,
    ) -> Result<()> {
        loop {
            select! {
                _ = cancel.cancelled() => {
                    return Ok(())
                }
                res = request_rx.recv_async() => {
                    let req = res.with_context(|| format!("block_fetcher: receive request"))?;
                    if let Err(e) = self.handle_request(req).await {
                        if is_cancelled(&e) {
                            return Ok(());
                        }
                        if e.is_transient() || is_route_invalid(&e) || is_proto(&e) || is_io(&e) {
                            continue;
                        }
                        return Err(e);
                    }
                }
            }
        }
    }

    async fn handle_request(&mut self, req: Self::Request) -> Result<()> {
        let (resp, mut resp_tx) = match req {
            Request::Fetch {
                share_key,
                target,
                block,
                ref flush,
                response_tx,
            } => {
                trace!(
                    "Fetching block from {}: file={} piece={} block={}",
                    share_key,
                    block.file_index,
                    block.piece_index,
                    block.block_index
                );

                // Attempt to fetch the block
                match self.fetch_block(&target, &block, *flush).await {
                    Ok(length) => (
                        Response::Fetched {
                            share_key,
                            block,
                            length,
                        },
                        response_tx,
                    ),
                    Err(e) => (
                        Response::FetchFailed {
                            share_key,
                            block,
                            err: e,
                        },
                        response_tx,
                    ),
                }
            }
        };
        resp_tx.send(resp).await?;
        Ok(())
    }

    async fn join(self) -> Result<()> {
        Ok(())
    }
}

impl<P: Node> Clone for BlockFetcher<P> {
    fn clone(&self) -> Self {
        Self {
            node: self.node.clone(),
            want_index: self.want_index.clone(),
            root: self.root.clone(),
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
    use veilid_core::{CryptoKey, CryptoTyped, CRYPTO_KIND_VLD0};

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

        let fetcher_root = tf_path.parent().unwrap().to_path_buf();

        // Start BlockFetcher
        let cancel = CancellationToken::new();
        let mut operator = Operator::new(
            cancel.clone(),
            BlockFetcher::new(node, Arc::new(RwLock::new(index)), fetcher_root),
            OneShot,
        );

        // Request first block
        let block = FileBlockFetch {
            file_index: 0,
            piece_index: 0,
            block_index: 0,
            piece_offset: 0,
        };
        let req = Request::Fetch {
            response_tx: ResponseChannel::default(),
            share_key: CryptoTyped::new(CRYPTO_KIND_VLD0, CryptoKey::new([0xbe; 32])),
            target: Target::PrivateRoute(CryptoKey::new([0xbe; 32])),
            block: block.clone(),
            flush: true,
        };
        // Verify response
        let resp = operator.call(req).await.expect("send request");
        assert!(
            matches!(
                resp,
                Response::Fetched {
                    share_key: _,
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
        operator.join().await.expect("fetcher task");
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

        let fetcher_root = tf_path.parent().unwrap().to_path_buf();

        let cancel = CancellationToken::new();
        let mut operator = Operator::new(
            cancel.clone(),
            BlockFetcher::new(node, Arc::new(RwLock::new(index)), fetcher_root),
            OneShot,
        );

        // Request first block
        let block = FileBlockFetch {
            file_index: 0,
            piece_index: 0,
            block_index: 0,
            piece_offset: 0,
        };

        // Verify we get a FetchFailed response with our error
        let resp = operator
            .call(Request::Fetch {
                response_tx: ResponseChannel::default(),
                share_key: CryptoTyped::new(CRYPTO_KIND_VLD0, CryptoKey::new([0xbe; 32])),
                target: Target::PrivateRoute(CryptoKey::new([0xbe; 32])),
                block: block.clone(),
                flush: true,
            })
            .await
            .expect("send request");
        match resp {
            Response::FetchFailed {
                share_key,
                block: failed_block,
                err,
            } => {
                assert_eq!(
                    share_key,
                    CryptoTyped::new(CRYPTO_KIND_VLD0, CryptoKey::new([0xbe; 32]))
                );
                assert_eq!(failed_block, block);
                assert_eq!(err.to_string(), "mock block fetch error");
            }
            _ => panic!("expected FetchFailed response, got {:?}", resp),
        }

        // Clean up
        cancel.cancel();
        operator.join().await.expect("fetcher task");
    }
}
