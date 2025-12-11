use std::{cmp::Ordering, collections::HashMap, io::SeekFrom, ops::Deref, sync::Arc};

use sha2::{Digest, Sha256};
use stigmerge_fileindex::{Index, BLOCK_SIZE_BYTES, PIECE_SIZE_BLOCKS, PIECE_SIZE_BYTES};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
    sync::{Mutex, RwLock},
};
use tracing::warn;

use crate::{error::Result, types::PieceState};

#[derive(Clone)]
pub struct PieceVerifier {
    inner: Arc<Mutex<PieceVerifierInner>>,
}

impl PieceVerifier {
    pub async fn new(index: Arc<RwLock<Index>>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(PieceVerifierInner::new(index).await)),
        }
    }

    pub async fn subscribe(&self, handler: Box<dyn PieceStatusHandler + Send + Sync>) {
        let mut inner = self.inner.lock().await;
        inner.add_status_handler(handler);
    }

    pub async fn update_piece(&self, piece_state: PieceState) -> Result<()> {
        let mut inner = self.inner.lock().await;
        let status = inner.handle_update(piece_state).await?;
        inner.handlers.piece_status(&status)
    }

    pub async fn is_complete(&self) -> bool {
        let inner = self.inner.lock().await;
        inner.verified_pieces.len() == inner.n_pieces
    }
}

pub trait PieceStatusHandler {
    fn piece_status(&self, status: &PieceStatus) -> Result<()>;
}

pub struct PieceStatusNotifier {
    verified_tx: flume::Sender<PieceStatus>,
}

impl PieceStatusNotifier {
    pub fn new() -> (Self, flume::Receiver<PieceStatus>) {
        let (verified_tx, verified_rx) = flume::unbounded();
        (Self { verified_tx }, verified_rx)
    }
}

impl PieceStatusHandler for PieceStatusNotifier {
    fn piece_status(&self, status: &PieceStatus) -> Result<()> {
        self.verified_tx.send(status.clone())?;
        Ok(())
    }
}

#[derive(Default)]
struct HandlerChain {
    handlers: Vec<Box<dyn PieceStatusHandler + Send + Sync>>,
}

impl HandlerChain {
    fn add(&mut self, handler: Box<dyn PieceStatusHandler + Send + Sync>) {
        self.handlers.push(handler);
    }
}

impl PieceStatusHandler for HandlerChain {
    fn piece_status(&self, status: &PieceStatus) -> Result<()> {
        for handler in self.handlers.iter() {
            handler.piece_status(status)?;
        }
        Ok(())
    }
}

struct PieceVerifierInner {
    index: Arc<RwLock<Index>>,
    pending_pieces: HashMap<(usize, usize), PieceState>,
    verified_pieces: HashMap<(usize, usize), PieceState>,
    n_pieces: usize,
    handlers: HandlerChain,
}

impl PieceVerifierInner {
    async fn new(index: Arc<RwLock<Index>>) -> Self {
        let pending_pieces = Self::empty_pieces(index.read().await.deref());
        let n_pieces = pending_pieces.len();
        Self {
            index,
            pending_pieces,
            verified_pieces: HashMap::new(),
            n_pieces,
            handlers: HandlerChain::default(),
        }
    }

    fn add_status_handler(&mut self, handler: Box<dyn PieceStatusHandler + Send + Sync>) {
        self.handlers.add(handler);
    }

    async fn handle_update(&mut self, piece_state: PieceState) -> Result<PieceStatus> {
        // update piece state
        let piece_state = match self.pending_pieces.remove(&piece_state.key()) {
            Some(prior_state) => prior_state.merged(piece_state),
            None => piece_state,
        };

        let status = if piece_state.is_complete() {
            // verify complete ones
            match self
                .verify_piece(piece_state.file_index, piece_state.piece_index)
                .await
            {
                Ok(true) => {
                    self.verified_pieces.insert(piece_state.key(), piece_state);
                    PieceStatus::ValidPiece {
                        file_index: piece_state.file_index,
                        piece_index: piece_state.piece_index,
                        index_complete: self.verified_pieces.len() == self.n_pieces,
                    }
                }
                Ok(false) => {
                    // invalid piece, still pending
                    self.pending_pieces.insert(piece_state.key(), piece_state);
                    PieceStatus::InvalidPiece {
                        file_index: piece_state.file_index,
                        piece_index: piece_state.piece_index,
                    }
                }
                Err(err) => {
                    // error verifying piece in local file, back to pending
                    self.pending_pieces.insert(piece_state.key(), piece_state);
                    warn!(?err, "verifying piece");
                    return Err(err);
                }
            }
        } else {
            // indicate incomplete ones
            self.pending_pieces.insert(piece_state.key(), piece_state);
            PieceStatus::IncompletePiece {
                file_index: piece_state.file_index,
                piece_index: piece_state.piece_index,
            }
        };

        Ok(status)
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

#[derive(Clone, Debug, PartialEq)]
pub enum PieceStatus {
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

impl PieceStatus {
    pub fn piece_index(&self) -> usize {
        match self {
            PieceStatus::IncompletePiece { piece_index, .. } => *piece_index,
            PieceStatus::ValidPiece { piece_index, .. } => *piece_index,
            PieceStatus::InvalidPiece { piece_index, .. } => *piece_index,
        }
    }

    pub fn index_complete(&self) -> bool {
        match self {
            PieceStatus::ValidPiece { index_complete, .. } => *index_complete,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Seek, Write};
    use stigmerge_fileindex::Indexer;

    use crate::tests::temp_file;

    use super::*;

    #[tokio::test]
    async fn verify_index() {
        const NUM_PIECES: usize = 3;

        // Create a test file with known content
        let tf = temp_file(0xa5u8, PIECE_SIZE_BYTES * NUM_PIECES); // 3 pieces

        // Create index from the file
        let indexer = Indexer::from_file(tf.path()).await.expect("indexer");

        // Set up verifier
        let verifier =
            PieceVerifier::new(Arc::new(RwLock::new(indexer.index().await.expect("index")))).await;
        let (notifier, status_rx) = PieceStatusNotifier::new();
        verifier.subscribe(Box::new(notifier)).await;

        for piece_index in 0..NUM_PIECES {
            // Building up to piece validation
            for block_index in 0..PIECE_SIZE_BLOCKS - 1 {
                let piece_state =
                    PieceState::new(0, piece_index, 0, PIECE_SIZE_BLOCKS, block_index);
                verifier
                    .update_piece(piece_state)
                    .await
                    .expect("update piece");

                let status = status_rx.recv().expect("receive status");
                assert_eq!(
                    status,
                    PieceStatus::IncompletePiece {
                        file_index: 0,
                        piece_index: piece_index
                    }
                );
            }
            // Piece complete & verified
            let piece_state =
                PieceState::new(0, piece_index, 0, PIECE_SIZE_BLOCKS, PIECE_SIZE_BLOCKS - 1);
            verifier
                .update_piece(piece_state)
                .await
                .expect("update piece");

            let status = status_rx.recv().expect("receive status");
            assert_eq!(
                status,
                PieceStatus::ValidPiece {
                    file_index: 0,
                    piece_index: piece_index,
                    index_complete: piece_index == 2, // last piece will complete the index
                }
            );
        }
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
        let verifier =
            PieceVerifier::new(Arc::new(RwLock::new(indexer.index().await.expect("index")))).await;
        let (notifier, status_rx) = PieceStatusNotifier::new();
        verifier.subscribe(Box::new(notifier)).await;

        // Building up to corrupt piece validation
        for block_index in 0..PIECE_SIZE_BLOCKS - 1 {
            let piece_state =
                PieceState::new(0, CORRUPT_PIECE_INDEX, 0, PIECE_SIZE_BLOCKS, block_index);
            verifier
                .update_piece(piece_state)
                .await
                .expect("update piece");

            let status = status_rx.recv().expect("receive status");
            assert_eq!(
                status,
                PieceStatus::IncompletePiece {
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
        verifier
            .update_piece(piece_state)
            .await
            .expect("update piece");

        let status = status_rx.recv().expect("receive status");
        assert_eq!(
            status,
            PieceStatus::InvalidPiece {
                file_index: 0,
                piece_index: CORRUPT_PIECE_INDEX,
            }
        );

        // Build up to validation of intact piece
        for block_index in 0..PIECE_SIZE_BLOCKS - 1 {
            let piece_state =
                PieceState::new(0, VALID_PIECE_INDEX, 0, PIECE_SIZE_BLOCKS, block_index);
            verifier
                .update_piece(piece_state)
                .await
                .expect("update piece");

            let status = status_rx.recv().expect("receive status");
            assert_eq!(
                status,
                PieceStatus::IncompletePiece {
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
        verifier
            .update_piece(piece_state)
            .await
            .expect("update piece");

        let status = status_rx.recv().expect("receive status");
        assert_eq!(
            status,
            PieceStatus::ValidPiece {
                file_index: 0,
                piece_index: VALID_PIECE_INDEX,
                // Important: index_complete is false because of the prior corrupt piece
                index_complete: false,
            }
        );
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
        let verifier =
            PieceVerifier::new(Arc::new(RwLock::new(indexer.index().await.expect("index")))).await;
        let (notifier, status_rx) = PieceStatusNotifier::new();
        verifier.subscribe(Box::new(notifier)).await;

        let piece_index = 0;
        for block_index in 0..PIECE_SIZE_BLOCKS - 1 {
            let piece_state = PieceState::new(0, piece_index, 0, PIECE_SIZE_BLOCKS, block_index);
            verifier
                .update_piece(piece_state)
                .await
                .expect("update piece");

            let status = status_rx.recv().expect("receive status");
            assert_eq!(
                status,
                PieceStatus::IncompletePiece {
                    file_index: 0,
                    piece_index: piece_index,
                }
            );
        }

        // Verify first complete piece
        let piece_state =
            PieceState::new(0, piece_index, 0, PIECE_SIZE_BLOCKS, PIECE_SIZE_BLOCKS - 1);
        verifier
            .update_piece(piece_state)
            .await
            .expect("update piece");

        let status = status_rx.recv().expect("receive status");
        assert_eq!(
            status,
            PieceStatus::ValidPiece {
                file_index: 0,
                piece_index,
                index_complete: piece_index == NUM_PIECES - 1
            }
        );

        let piece_index = 1;
        for block_index in 0..14 {
            let piece_state = PieceState::new(0, piece_index, 0, 16, block_index);
            verifier
                .update_piece(piece_state)
                .await
                .expect("update piece");

            let status = status_rx.recv().expect("receive status");
            assert_eq!(
                status,
                PieceStatus::IncompletePiece {
                    file_index: 0,
                    piece_index: piece_index,
                },
                "{piece_index} {block_index}"
            );
        }

        // Verify second complete piece
        let piece_state = PieceState::new(0, piece_index, 0, 16, 15);
        verifier
            .update_piece(piece_state)
            .await
            .expect("update piece");

        let status = status_rx.recv().expect("receive status");
        assert_eq!(
            status,
            PieceStatus::ValidPiece {
                file_index: 0,
                piece_index,
                index_complete: piece_index == NUM_PIECES - 1
            }
        );
    }
}
