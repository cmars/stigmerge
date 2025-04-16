use std::cmp::min;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::PathBuf;

use stigmerge_fileindex::{Index, BLOCK_SIZE_BYTES};
use tokio::fs::File;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::select;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use veilid_core::Target;

use super::types::FileBlockFetch;
use crate::chan_rpc::{ChanServer, Service};
use crate::peer::Peer;
use crate::{Error, Result};

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

#[derive(Debug)]
pub enum Response {
    Fetched {
        block: FileBlockFetch,
        length: usize,
    },
    FetchFailed {
        block: FileBlockFetch,
        error: Error,
    },
}

impl Response {
    fn block(&self) -> FileBlockFetch {
        match self {
            Response::Fetched { block, .. } => block.to_owned(),
            Response::FetchFailed { block, .. } => block.to_owned(),
        }
    }
}

pub struct BlockFetcher<'a, P: Peer> {
    peer: P,
    ch: ChanServer<Request, Response>,
    want_index: &'a Index,
    root: PathBuf,
    target_update_rx: broadcast::Receiver<Target>,
    target: Option<Target>,
    files: HashMap<usize, File>,
}

impl<'a, P: Peer> BlockFetcher<'a, P> {
    pub fn new(
        peer: P,
        ch: ChanServer<Request, Response>,
        want_index: &'a Index,
        root: PathBuf,
        target_update_rx: broadcast::Receiver<Target>,
    ) -> Self {
        Self {
            peer,
            ch,
            want_index,
            root,
            target_update_rx,
            target: None,
            files: HashMap::new(),
        }
    }
}

impl<'a, P: Peer + Send> Service for BlockFetcher<'a, P> {
    type Request = Request;
    type Response = Response;

    async fn run(mut self, cancel: CancellationToken) -> Result<()> {
        self.target = None;
        loop {
            if let None = self.target {
                select! {
                     _ = cancel.cancelled() => {
                         return Ok(())
                     }
                     res = self.target_update_rx.recv() => {
                         self.target = Some(res.map_err(Error::other)?);
                     }
                }
            }

            select! {
                _ = cancel.cancelled() => {
                    return Ok(())
                }
                res = self.target_update_rx.recv() => {
                    self.target = Some(res.map_err(Error::other)?);
                }
                res = self.ch.rx.recv() => {
                    match res {
                        None => return Ok(()),
                        Some(req) => {
                            // TODO: perform this in a background task
                            let resp = self.handle(&req).await?;
                            self.ch.tx.send(resp).await.map_err(Error::other)?;
                        }
                    }
                }
            }
        }
    }

    async fn handle(&mut self, req: &Self::Request) -> Result<Self::Response> {
        if self.target.is_none() {
            return Ok(Response::FetchFailed {
                block: req.block().to_owned(),
                error: Error::msg("fetch target not available yet"),
            });
        }
        match req {
            Request::Fetch { block, flush } => {
                debug!(
                    "Fetching block: file={} piece={} block={}",
                    block.file_index, block.piece_index, block.block_index
                );

                // Attempt to fetch the block
                match self.fetch_block(block, *flush).await {
                    Ok(length) => Ok(Response::Fetched {
                        block: block.clone(),
                        length,
                    }),
                    Err(e) => {
                        warn!("Failed to fetch block: {}", e);
                        Ok(Response::FetchFailed {
                            block: block.clone(),
                            error: e,
                        })
                    }
                }
            }
        }
    }
}

impl<'a, P: Peer> BlockFetcher<'a, P> {
    async fn fetch_block(&mut self, block: &FileBlockFetch, flush: bool) -> Result<usize> {
        // Request block from peer with retry logic
        let result = self
            .peer
            .request_block(self.target.unwrap(), block.piece_index, block.block_index)
            .await?;
        // Write the block to the file
        let fh = match self.files.get_mut(&block.file_index) {
            Some(fh) => fh,
            None => {
                let path = self
                    .root
                    .join(self.want_index.files()[block.file_index].path());
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

#[cfg(test)]
mod tests {
    use std::{sync::Arc, sync::Mutex};

    use stigmerge_fileindex::Indexer;
    use tokio::io::AsyncReadExt;
    use tokio::sync::broadcast;
    use tokio_util::sync::CancellationToken;
    use veilid_core::CryptoKey;

    use crate::chan_rpc::pipe;
    use crate::fetcher2::types::FileBlockFetch;
    use crate::tests::{temp_file, StubPeer};

    use super::*;

    #[tokio::test]
    async fn fetch_single_block() {
        const BLOCK_DATA: u8 = 0xa5;

        // Create a test file with known content
        let tf = temp_file(BLOCK_DATA, BLOCK_SIZE_BYTES * 2); // 2 blocks
        let tf_path = tf.path().to_path_buf();

        // Create index from the file
        let indexer = Indexer::from_file(tf_path.clone()).await.expect("indexer");
        let index = indexer.index().await.expect("index");

        // Delete the temp file - we'll verify BlockFetcher recreates it
        drop(tf);

        // Set up BlockFetcher
        let mut peer = StubPeer::new();
        // Mock the block data that StubPeer will return
        peer.request_block_result = Arc::new(Mutex::new(move |_, _, _| {
            Ok(vec![BLOCK_DATA; BLOCK_SIZE_BYTES])
        }));

        let (mut client_ch, server_ch) = pipe(16);
        let (target_tx, target_rx) = broadcast::channel(16);
        let cancel = CancellationToken::new();

        let fetcher_cancel = cancel.clone();
        let fetcher_index = index.clone();
        let fetcher_root = tf_path.parent().unwrap().to_path_buf();

        // Start BlockFetcher
        let fetcher_task = tokio::spawn(async move {
            let fetcher =
                BlockFetcher::new(peer, server_ch, &fetcher_index, fetcher_root, target_rx);
            fetcher.run(fetcher_cancel).await
        });

        // Send target update
        target_tx
            .send(Target::PrivateRoute(CryptoKey::new([0xbe; 32])))
            .expect("send target");

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
        client_ch.tx.send(req).await.expect("send request");

        // Verify response
        let resp = client_ch.rx.recv().await.expect("receive response");
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
        fetcher_task
            .await
            .expect("fetcher task")
            .expect("fetcher run");
    }

    #[tokio::test]
    async fn test_fetch_block_error() {
        const BLOCK_DATA: u8 = 0xa5;

        // Create a test file with known content
        let tf = temp_file(BLOCK_DATA, BLOCK_SIZE_BYTES * 2);
        let tf_path = tf.path().to_path_buf();

        // Create index from the file
        let indexer = Indexer::from_file(tf_path.clone()).await.expect("indexer");
        let index = indexer.index().await.expect("index");

        // Delete the temp file
        drop(tf);

        // Set up BlockFetcher with error-returning stub
        let mut peer = StubPeer::new();
        // Mock the block request to return an error
        peer.request_block_result = Arc::new(Mutex::new(move |_, _, _| -> Result<Vec<u8>> {
            Err(Error::msg("mock block fetch error"))
        }));

        let (mut client_ch, server_ch) = pipe(16);
        let (target_tx, target_rx) = broadcast::channel(16);
        let cancel = CancellationToken::new();

        let fetcher_cancel = cancel.clone();
        let fetcher_index = index.clone();
        let fetcher_root = tf_path.parent().unwrap().to_path_buf();

        // Start BlockFetcher
        let fetcher_task = tokio::spawn(async move {
            let fetcher =
                BlockFetcher::new(peer, server_ch, &fetcher_index, fetcher_root, target_rx);
            fetcher.run(fetcher_cancel).await
        });

        // Send target update
        target_tx
            .send(Target::PrivateRoute(CryptoKey::new([0xbe; 32])))
            .expect("send target");

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
        client_ch.tx.send(req).await.expect("send request");

        // Verify we get a FetchFailed response with our error
        let resp = client_ch.rx.recv().await.expect("receive response");
        match resp {
            Response::FetchFailed {
                block: failed_block,
                error,
            } => {
                assert_eq!(failed_block, block);
                assert_eq!(
                    error.to_string(),
                    "unexpected fault: mock block fetch error"
                );
            }
            _ => panic!("expected FetchFailed response, got {:?}", resp),
        }

        // Clean up
        cancel.cancel();
        fetcher_task
            .await
            .expect("fetcher task")
            .expect("fetcher run");
    }

    #[tokio::test]
    async fn test_fetch_no_target() {
        const BLOCK_DATA: u8 = 0xa5;

        // Create a test file with known content
        let tf = temp_file(BLOCK_DATA, BLOCK_SIZE_BYTES * 2); // 2 blocks
        let tf_path = tf.path().to_path_buf();

        // Create index from the file
        let indexer = Indexer::from_file(tf_path.clone()).await.expect("indexer");
        let index = indexer.index().await.expect("index");

        // Delete the temp file - we'll verify BlockFetcher recreates it
        drop(tf);

        // Set up BlockFetcher
        let peer = StubPeer::new();
        let (client_ch, server_ch) = pipe(1);
        let (_tx, rx) = broadcast::channel(1);
        let cancel = CancellationToken::new();
        let fetcher_cancel = cancel.clone();

        // Start BlockFetcher
        let fetcher_task = tokio::spawn(async move {
            let fetcher = BlockFetcher::new(peer, server_ch, &index, tf_path, rx);
            fetcher.run(fetcher_cancel).await
        });

        // Request a block without sending target update first
        client_ch
            .tx
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

        // Client is blocked waiting for target update
        client_ch
            .tx
            .try_send(Request::Fetch {
                block: FileBlockFetch {
                    file_index: 0,
                    piece_index: 0,
                    piece_offset: 0,
                    block_index: 1,
                },
                flush: false,
            })
            .expect_err("send channel full");

        // Client has not responded to requests
        assert_eq!(client_ch.rx.len(), 0);

        // Clean up
        cancel.cancel();
        fetcher_task
            .await
            .expect("fetcher task")
            .expect("fetcher run");
    }
}
