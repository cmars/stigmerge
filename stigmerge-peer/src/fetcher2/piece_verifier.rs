use std::{cmp::Ordering, collections::HashMap, io::SeekFrom};

use sha2::{Digest, Sha256};
use stigmerge_fileindex::{Index, BLOCK_SIZE_BYTES, PIECE_SIZE_BLOCKS, PIECE_SIZE_BYTES};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
    select,
};
use tokio_util::sync::CancellationToken;

use super::types::PieceState;
use crate::Result;
use crate::{
    chan_rpc::{ChanServer, Service},
    Error, Peer,
};

#[derive(Clone, Debug, PartialEq)]
pub enum Request {
    Piece(PieceState),
}

impl Request {
    fn key(&self) -> (usize, usize) {
        self.piece_state().key()
    }

    fn piece_state(&self) -> PieceState {
        match self {
            Request::Piece(state) => state.to_owned(),
        }
    }

    fn file_index(&self) -> usize {
        self.piece_state().file_index
    }

    fn piece_index(&self) -> usize {
        self.piece_state().piece_index
    }

    fn is_complete(&self) -> bool {
        self.piece_state().is_complete()
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
    fn index_complete(&self) -> bool {
        match self {
            Response::ValidPiece { index_complete, .. } => *index_complete,
            _ => false,
        }
    }
}

pub(super) struct PieceVerifier<'a, P: Peer> {
    peer: P,
    index: &'a Index,
    ch: ChanServer<Request, Response>,
    piece_states: HashMap<(usize, usize), PieceState>,
    verified_pieces: usize,
}

impl<'a, P: Peer> Service for PieceVerifier<'a, P> {
    type Request = Request;
    type Response = Response;

    async fn run(mut self, cancel: CancellationToken) -> Result<()> {
        loop {
            select! {
                _ = cancel.cancelled() => {
                    return Ok(());
                }
                res = self.ch.rx.recv() => {
                    match res {
                        None => return Ok(()),
                        Some(req) => {
                            let resp = self.handle(&req).await?;
                            self.ch.tx.send(resp.clone()).await.map_err(Error::other)?;
                            if resp.index_complete() {
                                cancel.cancel();
                            }
                        }
                    }
                }
            }
        }
    }

    async fn handle(&mut self, req: &Self::Request) -> Result<Self::Response> {
        // update piece state
        let prior_state = match self.piece_states.get_mut(&req.key()) {
            Some(prior_state) => {
                *prior_state = prior_state.merged(req.piece_state());
                prior_state
            }
            None => {
                self.piece_states.insert(req.key(), req.piece_state());
                self.piece_states.get_mut(&req.key()).unwrap()
            }
        };
        if prior_state.is_complete() {
            // verify complete ones
            if self
                .verify_piece(req.file_index(), req.piece_index())
                .await?
            {
                self.verified_pieces += 1;
                Ok(Response::ValidPiece {
                    file_index: req.file_index(),
                    piece_index: req.piece_index(),
                    index_complete: self.verified_pieces == self.index.payload().pieces().len(),
                })
            } else {
                Ok(Response::InvalidPiece {
                    file_index: req.file_index(),
                    piece_index: req.piece_index(),
                })
            }
        } else {
            // indicate incomplete ones
            Ok(Response::IncompletePiece {
                file_index: req.file_index(),
                piece_index: req.piece_index(),
            })
        }
    }
}

impl<'a, P: Peer> PieceVerifier<'a, P> {
    pub fn new(peer: P, index: &'a Index, ch: ChanServer<Request, Response>) -> PieceVerifier<'a, P> {
        PieceVerifier {
            peer,
            index,
            ch,
            piece_states: HashMap::new(),
            verified_pieces: 0,
        }
    }

    async fn verify_piece(&self, file_index: usize, piece_index: usize) -> Result<bool> {
        let file_spec = &self.index.files()[file_index];
        let mut fh = File::open(self.index.root().join(file_spec.path())).await?;
        let piece_spec = &self.index.payload().pieces()[piece_index];

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
}

#[cfg(test)]
mod tests {
    use std::io::{Seek, Write};
    use stigmerge_fileindex::Indexer;
    use tokio_util::sync::CancellationToken;

    use crate::{
        chan_rpc::pipe,
        tests::{temp_file, StubPeer},
    };

    use super::*;

    #[tokio::test]
    async fn verify_index() {
        const NUM_PIECES: usize = 3;

        // Create a test file with known content
        let tf = temp_file(0xa5u8, PIECE_SIZE_BYTES * NUM_PIECES); // 3 pieces

        // Create index from the file
        let indexer = Indexer::from_file(tf.path().to_path_buf())
            .await
            .expect("indexer");

        // Set up verifier
        let peer = StubPeer::new();
        let (mut client_ch, server_ch) = pipe(16);
        let cancel = CancellationToken::new();
        let verifier_cancel = cancel.clone();
        let verifier_task = tokio::spawn(async move {
            let index = indexer.index().await.expect("index");
            let verifier = PieceVerifier::new(peer, &index, server_ch);
            verifier.run(verifier_cancel).await
        });

        for piece_index in 0..NUM_PIECES {
            // Building up to piece validation
            for block_index in 0..PIECE_SIZE_BLOCKS - 1 {
                let piece_state =
                    PieceState::new(0, piece_index, 0, PIECE_SIZE_BLOCKS, block_index);
                let req = Request::Piece(piece_state);
                client_ch.tx.send(req).await.expect("send request");
                let resp = client_ch.rx.recv().await.expect("receive response");
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
            let req = Request::Piece(piece_state);
            client_ch.tx.send(req).await.expect("send request");
            let resp = client_ch.rx.recv().await.expect("receive response");
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
        verifier_task
            .await
            .expect("verifier task")
            .expect("verifier run");
    }

    #[tokio::test]
    async fn verify_invalid_piece() {
        const CORRUPT_PIECE_INDEX: usize = 0;
        const VALID_PIECE_INDEX: usize = 1;

        // Create a test file with known content
        let mut tf = temp_file(0xa5u8, PIECE_SIZE_BYTES * 2); // 2 pieces

        // Create index from the file
        let indexer = Indexer::from_file(tf.path().to_path_buf())
            .await
            .expect("indexer");

        // Set up verifier
        let peer = StubPeer::new();
        let (mut client_ch, server_ch) = pipe(16);
        let cancel = CancellationToken::new();
        let verifier_cancel = cancel.clone();
        let verifier_task = tokio::spawn(async move {
            let index = indexer.index().await.expect("index");
            let verifier = PieceVerifier::new(peer, &index, server_ch);
            verifier.run(verifier_cancel).await
        });

        // Building up to corrupt piece validation
        for block_index in 0..PIECE_SIZE_BLOCKS - 1 {
            let piece_state =
                PieceState::new(0, CORRUPT_PIECE_INDEX, 0, PIECE_SIZE_BLOCKS, block_index);
            let req = Request::Piece(piece_state);
            client_ch.tx.send(req).await.expect("send request");
            let resp = client_ch.rx.recv().await.expect("receive response");
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
        let req = Request::Piece(piece_state);
        client_ch.tx.send(req).await.expect("send request");
        let resp = client_ch.rx.recv().await.expect("receive response");
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
            let req = Request::Piece(piece_state);
            client_ch.tx.send(req).await.expect("send request");
            let resp = client_ch.rx.recv().await.expect("receive response");
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
        let req = Request::Piece(piece_state);
        client_ch.tx.send(req).await.expect("send request");
        let resp = client_ch.rx.recv().await.expect("receive response");
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
        verifier_task
            .await
            .expect("verifier task")
            .expect("verifier run");
    }

    #[tokio::test]
    async fn verify_non_aligned_file() {
        const NUM_PIECES: usize = 2;

        // Create a test file that's not aligned to piece size
        let non_aligned_size = PIECE_SIZE_BYTES + (PIECE_SIZE_BYTES / 2); // 1.5 pieces
        let tf = temp_file(0xa5u8, non_aligned_size);
        let test_path = std::env::temp_dir().join(tf.path());

        // Create index from the file
        let indexer = Indexer::from_file(test_path.clone())
            .await
            .expect("indexer");

        // Set up verifier
        let peer = StubPeer::new();
        let (mut client_ch, server_ch) = pipe(16);
        let cancel = CancellationToken::new();
        let verifier_cancel = cancel.clone();
        let verifier_task = tokio::spawn(async move {
            let index = indexer.index().await.expect("index");
            let verifier = PieceVerifier::new(peer, &index, server_ch);
            verifier.run(verifier_cancel).await
        });

        for piece_index in 0..NUM_PIECES {
            // Building up to piece validation
            for block_index in 0..PIECE_SIZE_BLOCKS - 1 {
                let piece_state = PieceState::new(0, piece_index, 0, PIECE_SIZE_BLOCKS, block_index);
                let req = Request::Piece(piece_state);
                client_ch.tx.send(req).await.expect("send request");
                let resp = client_ch.rx.recv().await.expect("receive response");
                assert_eq!(
                    resp,
                    Response::IncompletePiece {
                        file_index: 0,
                        piece_index: piece_index,
                    }
                );
            }

            // Verify first complete piece
            let piece_state = PieceState::new(0, piece_index, 0, PIECE_SIZE_BLOCKS, PIECE_SIZE_BLOCKS - 1);
            let req = Request::Piece(piece_state);
            client_ch.tx.send(req).await.expect("send request");
            let resp = client_ch.rx.recv().await.expect("receive response");
            assert_eq!(
                resp,
            Response::ValidPiece {
                file_index: 0,
                piece_index,
                index_complete: piece_index == NUM_PIECES - 1
            }
        );
        }

        // shut down verifier
        cancel.cancel();
        verifier_task
            .await
            .expect("verifier task")
            .expect("verifier run");
    }
}
