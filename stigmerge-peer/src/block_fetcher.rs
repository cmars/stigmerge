use std::cmp::min;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;

use stigmerge_fileindex::BLOCK_SIZE_BYTES;
use tokio::fs::File;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use veilid_core::Target;
use veilnet::connection::RoutingContext;
use veilnet::Connection;

use crate::proto::{BlockRequest, Encoder, Request};
use crate::types::{PieceState, RemoteShareInfo};
use crate::{Error, Result};

use super::types::FileBlockFetch;

pub struct BlockFetcher<C: Connection> {
    conn: C,
    root: PathBuf,
    files: HashMap<usize, File>,
}

impl<C: Connection + Send + Sync> BlockFetcher<C> {
    /// Creates a new block fetcher.
    pub fn new(conn: C, root: PathBuf) -> Self {
        Self {
            conn,
            root,
            files: HashMap::new(),
        }
    }

    pub async fn fetch_block(
        &mut self,
        remote_share: Arc<RemoteShareInfo>,
        block: &FileBlockFetch,
        flush: bool,
    ) -> Result<(PieceState, usize)> {
        // Request block from peer with retry logic
        let result = self
            .request_block(
                Target::RouteId(remote_share.route_id.to_owned()),
                block.piece_index,
                block.block_index,
            )
            .await?
            .ok_or(Error::msg("block not found"))?;
        // Write the block to the file
        let fh = match self.files.get_mut(&block.file_index) {
            Some(fh) => fh,
            None => {
                let path = self.root.join(remote_share.index.files()[block.file_index].path());
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
        Ok((
            PieceState::new(
                block.file_index,
                block.piece_index,
                block.piece_offset,
                remote_share.index.payload().pieces()[block.piece_index].block_count(),
                block.block_index,
            ),
            block_end,
        ))
    }

    async fn request_block(
        &mut self,
        target: Target,
        piece: usize,
        block: usize,
    ) -> Result<Option<Vec<u8>>> {
        let routing_context = self.conn.routing_context();
        let block_req = Request::BlockRequest(BlockRequest {
            piece: piece as u32,
            block: block as u8,
        });
        let block_req_bytes = block_req.encode()?;
        let resp_bytes = routing_context.app_call(target, block_req_bytes).await?;
        Ok(if resp_bytes.is_empty() {
            None
        } else {
            Some(resp_bytes)
        })
    }
}

impl<C: Connection + Clone> Clone for BlockFetcher<C> {
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
            root: self.root.clone(),
            files: HashMap::new(),
        }
    }
}

#[cfg(feature = "refactor")]
#[cfg(test)]
mod tests {
    use std::{sync::Arc, sync::Mutex};

    use stigmerge_fileindex::Indexer;
    use tokio::io::AsyncReadExt;
    use tokio_util::sync::CancellationToken;
    use veilid_core::{CryptoKind, CryptoTyped, RecordKey, RouteId};

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
            share_key: CryptoTyped::new(CryptoKind::default(), RecordKey::new([0xbe; 32])),
            target: Target::RouteId(RouteId::new([0xbe; 32])),
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
        operator.join().await.expect_err("cancelled");
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
                share_key: CryptoTyped::new(CryptoKind::default(), RecordKey::new([0xbe; 32])),
                target: Target::RouteId(RouteId::new([0xbe; 32])),
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
                ..
            } => {
                assert_eq!(
                    share_key,
                    CryptoTyped::new(CryptoKind::default(), RecordKey::new([0xbe; 32]))
                );
                assert_eq!(failed_block, block);
                assert_eq!(err.to_string(), "mock block fetch error");
            }
            _ => panic!("expected FetchFailed response, got {:?}", resp),
        }

        // Clean up
        cancel.cancel();
        operator.join().await.expect_err("cancelled");
    }
}
