use std::{cmp::Ordering, collections::HashMap, io::SeekFrom, ops::Deref, sync::Arc};

use anyhow::Context;
use sha2::{Digest, Sha256};
use stigmerge_fileindex::{Index, BLOCK_SIZE_BYTES, PIECE_SIZE_BLOCKS, PIECE_SIZE_BYTES};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
    select,
    sync::{broadcast, RwLock},
};
use tokio_util::sync::CancellationToken;

use crate::{
    actor::{Actor, Respondable, ResponseChannel},
    error::{CancelError, Result, Unrecoverable},
    types::PieceState,
};

pub struct PieceVerifier {
    index: Arc<RwLock<Index>>,
    pending_pieces: HashMap<(usize, usize), PieceState>,
    verified_pieces: HashMap<(usize, usize), PieceState>,
    n_pieces: usize,
    verified_tx: broadcast::Sender<PieceState>,
    verified_rx: broadcast::Receiver<PieceState>,
}

impl PieceVerifier {
    pub async fn new(index: Arc<RwLock<Index>>) -> PieceVerifier {
        let (verified_tx, verified_rx) = broadcast::channel(32768);
        let pending_pieces = Self::empty_pieces(index.read().await.deref());
        let n_pieces = pending_pieces.len();
        PieceVerifier {
            index,
            pending_pieces,
            verified_pieces: HashMap::new(),
            n_pieces,
            verified_tx,
            verified_rx,
        }
    }

    pub fn subscribe_verified(&self) -> broadcast::Receiver<PieceState> {
        self.verified_rx.resubscribe()
    }

    async fn verify_piece(&self, file_index: usize, piece_index: usize) -> Result<bool> {
        let index = self.index.read().await;

        let file_spec = &index.files()[file_index];
        let mut fh = File::open(index.root().join(file_spec.path())).await?;
        let piece_spec = &index.payload().pieces()[piece_index];

        // FIXME: this is wrong for multi-file!
        // We'd need to seek relative to the file's payload slice
        fh.seek(SeekFrom::Start((piece_index * PIECE_SIZE_BYTES) as u64))
            .await?;
        let mut buf = [0u8; BLOCK_SIZE_BYTES];
        let mut digest = Sha256::new();
        for _ in 0..PIECE_SIZE_BLOCKS {
            let rd = fh.read(&mut buf[..]).await?;
            if rd == 0 {
                break;
            }
            digest.update(&buf[..rd]);
        }
        let expected_digest = piece_spec.digest();
        let actual_digest: [u8; 32] = digest.finalize().into();
        Ok(expected_digest.cmp(&actual_digest[..]) == Ordering::Equal)
    }

    fn empty_pieces(index: &Index) -> HashMap<(usize, usize), PieceState> {
        let mut result = HashMap::new();
        for (file_index, file) in index.files().iter().enumerate() {
            let n_pieces = (file.contents().length() / PIECE_SIZE_BYTES)
                + if file.contents().length() % PIECE_SIZE_BYTES > 0 {
                    1
                } else {
                    0
                };
            let starting_piece = file.contents().starting_piece();
            for piece_index in starting_piece..starting_piece + n_pieces {
                result.insert(
                    (file_index, piece_index),
                    PieceState::new(
                        file_index,
                        piece_index,
                        0,
                        index.payload().pieces()[piece_index].block_count(),
                        0,
                    ),
                );
            }
        }
        result
    }
}

pub enum Request {
    Piece {
        piece_state: PieceState,
        response_tx: ResponseChannel<Response>,
    },
}

impl Respondable for Request {
    type Response = Response;

    fn set_response(&mut self, ch: ResponseChannel<Self::Response>) {
        match self {
            Request::Piece { response_tx, .. } => *response_tx = ch,
        }
    }

    fn response_tx(self) -> ResponseChannel<Self::Response> {
        match self {
            Request::Piece { response_tx, .. } => response_tx,
        }
    }
}

impl Request {
    fn key(&self) -> (usize, usize) {
        self.piece_state().key()
    }

    fn piece_state(&self) -> PieceState {
        match self {
            Request::Piece { piece_state, .. } => piece_state.clone(),
        }
    }

    fn file_index(&self) -> usize {
        self.piece_state().file_index
    }

    fn piece_index(&self) -> usize {
        self.piece_state().piece_index
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Response {
    IncompletePiece {
        file_index: usize,
        piece_index: usize,
    },
    ValidPiece {
        file_index: usize,
        piece_index: usize,
        index_complete: bool,
    },
    InvalidPiece {
        file_index: usize,
        piece_index: usize,
    },
}

impl Response {
    pub fn index_complete(&self) -> bool {
        match self {
            Response::ValidPiece { index_complete, .. } => *index_complete,
            _ => false,
        }
    }
}

impl Actor for PieceVerifier {
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
                    return Err(CancelError.into());
                }
                res = request_rx.recv_async() => {
                    let req = res.with_context(|| format!("piece_verifier: receive request"))?;
                    self.handle_request(req).await?;
                }
            }
        }
    }

    async fn handle_request(&mut self, req: Self::Request) -> Result<()> {
        // update piece state
        let piece_state = match self.pending_pieces.remove(&req.key()) {
            Some(prior_state) => prior_state.merged(req.piece_state()),
            None => req.piece_state(),
        };

        let resp = if piece_state.is_complete() {
            // verify complete ones
            if self
                .verify_piece(req.file_index(), req.piece_index())
                .await?
            {
                self.verified_pieces.insert(req.key(), piece_state);
                self.verified_tx
                    .send(req.piece_state())
                    .context(Unrecoverable::new("notify verified piece"))?;
                Response::ValidPiece {
                    file_index: req.file_index(),
                    piece_index: req.piece_index(),
                    index_complete: self.verified_pieces.len() == self.n_pieces,
                }
            } else {
                // invalid piece, still pending
                self.pending_pieces.insert(req.key(), piece_state);
                Response::InvalidPiece {
                    file_index: req.file_index(),
                    piece_index: req.piece_index(),
                }
            }
        } else {
            // indicate incomplete ones
            self.pending_pieces.insert(req.key(), piece_state);
            Response::IncompletePiece {
                file_index: req.file_index(),
                piece_index: req.piece_index(),
            }
        };

        let mut response_tx = req.response_tx();
        response_tx
            .send(resp)
            .await
            .context(Unrecoverable::new("send response from piece verifier"))?;

        Ok(())
    }
}

impl Clone for PieceVerifier {
    fn clone(&self) -> Self {
        PieceVerifier {
            index: self.index.clone(),
            pending_pieces: self.pending_pieces.clone(),
            verified_pieces: self.verified_pieces.clone(),
            n_pieces: self.n_pieces,
            verified_tx: self.verified_tx.clone(),
            verified_rx: self.verified_rx.resubscribe(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Seek, Write};
    use stigmerge_fileindex::Indexer;
    use tokio_util::sync::CancellationToken;

    use crate::{
        actor::{OneShot, Operator, ResponseChannel},
        tests::temp_file,
    };

    use super::*;

    #[tokio::test]
    async fn verify_index() {
        const NUM_PIECES: usize = 3;

        // Create a test file with known content
        let tf = temp_file(0xa5u8, PIECE_SIZE_BYTES * NUM_PIECES); // 3 pieces

        // Create index from the file
        let indexer = Indexer::from_file(tf.path()).await.expect("indexer");

        // Set up verifier
        let cancel = CancellationToken::new();
        let mut operator = Operator::new(
            cancel.clone(),
            PieceVerifier::new(Arc::new(RwLock::new(indexer.index().await.expect("index")))).await,
            OneShot,
        );

        for piece_index in 0..NUM_PIECES {
            // Building up to piece validation
            for block_index in 0..PIECE_SIZE_BLOCKS - 1 {
                let piece_state =
                    PieceState::new(0, piece_index, 0, PIECE_SIZE_BLOCKS, block_index);
                let req = Request::Piece {
                    piece_state,
                    response_tx: ResponseChannel::default(),
                };
                let resp = operator.call(req).await.expect("call request");
                assert_eq!(
                    resp,
                    Response::IncompletePiece {
                        file_index: 0,
                        piece_index: piece_index
                    }
                );
            }
            // Piece complete & verified
            let piece_state =
                PieceState::new(0, piece_index, 0, PIECE_SIZE_BLOCKS, PIECE_SIZE_BLOCKS - 1);
            let req = Request::Piece {
                piece_state,
                response_tx: ResponseChannel::default(),
            };
            let resp = operator.call(req).await.expect("call request");
            assert_eq!(
                resp,
                Response::ValidPiece {
                    file_index: 0,
                    piece_index: piece_index,
                    index_complete: piece_index == 2, // last piece will complete the index
                }
            );
        }

        // shut down verifier
        cancel.cancel();
        operator.join().await.expect_err("cancelled");
    }

    #[tokio::test]
    async fn verify_invalid_piece() {
        const CORRUPT_PIECE_INDEX: usize = 0;
        const VALID_PIECE_INDEX: usize = 1;

        // Create a test file with known content
        let mut tf = temp_file(0xa5u8, PIECE_SIZE_BYTES * 2); // 2 pieces

        // Create index from the file
        let indexer = Indexer::from_file(tf.path()).await.expect("indexer");

        // Set up verifier
        let cancel = CancellationToken::new();
        let mut operator = Operator::new(
            cancel.clone(),
            PieceVerifier::new(Arc::new(RwLock::new(indexer.index().await.expect("index")))).await,
            OneShot,
        );

        // Building up to corrupt piece validation
        for block_index in 0..PIECE_SIZE_BLOCKS - 1 {
            let piece_state =
                PieceState::new(0, CORRUPT_PIECE_INDEX, 0, PIECE_SIZE_BLOCKS, block_index);
            let req = Request::Piece {
                piece_state,
                response_tx: ResponseChannel::default(),
            };
            let resp = operator.call(req).await.expect("call request");
            assert_eq!(
                resp,
                Response::IncompletePiece {
                    file_index: 0,
                    piece_index: CORRUPT_PIECE_INDEX,
                }
            );
        }

        // Corrupt the first piece after indexing
        {
            let f = tf.as_file_mut();
            f.seek(SeekFrom::Start(0)).expect("seek");
            f.write_all(&[0xFFu8; 1024]).expect("write");
            f.flush().expect("flush");
        }

        // Validate corrupted piece
        let piece_state = PieceState::new(
            0,
            CORRUPT_PIECE_INDEX,
            0,
            PIECE_SIZE_BLOCKS,
            PIECE_SIZE_BLOCKS - 1,
        );
        let req = Request::Piece {
            piece_state,
            response_tx: ResponseChannel::default(),
        };
        let resp = operator.call(req).await.expect("call request");
        assert_eq!(
            resp,
            Response::InvalidPiece {
                file_index: 0,
                piece_index: CORRUPT_PIECE_INDEX,
            }
        );

        // Build up to validation of intact piece
        for block_index in 0..PIECE_SIZE_BLOCKS - 1 {
            let piece_state =
                PieceState::new(0, VALID_PIECE_INDEX, 0, PIECE_SIZE_BLOCKS, block_index);
            let req = Request::Piece {
                piece_state,
                response_tx: ResponseChannel::default(),
            };
            let resp = operator.call(req).await.expect("call request");
            assert_eq!(
                resp,
                Response::IncompletePiece {
                    file_index: 0,
                    piece_index: VALID_PIECE_INDEX,
                }
            );
        }

        // Validate other intact piece
        let piece_state = PieceState::new(
            0,
            VALID_PIECE_INDEX,
            0,
            PIECE_SIZE_BLOCKS,
            PIECE_SIZE_BLOCKS - 1,
        );
        let req = Request::Piece {
            piece_state,
            response_tx: ResponseChannel::default(),
        };
        let resp = operator.call(req).await.expect("call request");
        assert_eq!(
            resp,
            Response::ValidPiece {
                file_index: 0,
                piece_index: VALID_PIECE_INDEX,
                // Important: index_complete is false because of the prior corrupt piece
                index_complete: false,
            }
        );

        // shut down verifier
        cancel.cancel();
        operator.join().await.expect_err("cancelled");
    }

    #[tokio::test]
    async fn verify_non_aligned_file() {
        const NUM_PIECES: usize = 2;

        // Create a test file that's not aligned to piece size
        let non_aligned_size = PIECE_SIZE_BYTES + (PIECE_SIZE_BYTES / 2); // 1.5 pieces
        let tf = temp_file(0xa5u8, non_aligned_size);
        let test_path = std::env::temp_dir().join(tf.path());

        // Create index from the file
        let indexer = Indexer::from_file(test_path.as_path())
            .await
            .expect("indexer");

        // Set up verifier
        let cancel = CancellationToken::new();
        let mut operator = Operator::new(
            cancel.clone(),
            PieceVerifier::new(Arc::new(RwLock::new(indexer.index().await.expect("index")))).await,
            OneShot,
        );

        let piece_index = 0;
        for block_index in 0..PIECE_SIZE_BLOCKS - 1 {
            let piece_state = PieceState::new(0, piece_index, 0, PIECE_SIZE_BLOCKS, block_index);
            let req = Request::Piece {
                piece_state,
                response_tx: ResponseChannel::default(),
            };
            let resp = operator.call(req).await.expect("call request");
            assert_eq!(
                resp,
                Response::IncompletePiece {
                    file_index: 0,
                    piece_index: piece_index,
                }
            );
        }

        // Verify first complete piece
        let piece_state =
            PieceState::new(0, piece_index, 0, PIECE_SIZE_BLOCKS, PIECE_SIZE_BLOCKS - 1);
        let req = Request::Piece {
            piece_state,
            response_tx: ResponseChannel::default(),
        };
        let resp = operator.call(req).await.expect("call request");
        assert_eq!(
            resp,
            Response::ValidPiece {
                file_index: 0,
                piece_index,
                index_complete: piece_index == NUM_PIECES - 1
            }
        );

        let piece_index = 1;
        for block_index in 0..14 {
            let piece_state = PieceState::new(0, piece_index, 0, 16, block_index);
            let req = Request::Piece {
                piece_state,
                response_tx: ResponseChannel::default(),
            };
            let resp = operator.call(req).await.expect("call request");
            assert_eq!(
                resp,
                Response::IncompletePiece {
                    file_index: 0,
                    piece_index: piece_index,
                },
                "{piece_index} {block_index}"
            );
        }

        // Verify second complete piece
        let piece_state = PieceState::new(0, piece_index, 0, 16, 15);
        let req = Request::Piece {
            piece_state,
            response_tx: ResponseChannel::default(),
        };
        let resp = operator.call(req).await.expect("call request");
        assert_eq!(
            resp,
            Response::ValidPiece {
                file_index: 0,
                piece_index,
                index_complete: piece_index == NUM_PIECES - 1
            }
        );

        // shut down verifier
        cancel.cancel();
        operator.join().await.expect_err("cancelled");
    }
}
