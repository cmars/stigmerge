use stigmerge_fileindex::{Index, Indexer, BLOCK_SIZE_BYTES};
use tokio::select;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::warn;
use veilid_core::Target;

use crate::types::{FileBlockFetch, PieceState, ShareInfo};
use crate::{chan_rpc::ChanClient, have_announcer};
use crate::{block_fetcher, piece_verifier, Error, Result};

pub struct Fetcher {
    clients: Clients,

    want_index: Index,
    have_index: Index,

    state: State,
    status_tx: broadcast::Sender<Status>,
}

pub struct Clients {
    pub target_update_rx: broadcast::Receiver<Target>,
    pub block_fetcher: ChanClient<block_fetcher::Request, block_fetcher::Response>,
    pub piece_verifier: ChanClient<piece_verifier::Request, piece_verifier::Response>,
    pub have_announcer: ChanClient<have_announcer::Request, have_announcer::Response>,
}

pub enum State {
    Indexing,
    Planning,
    Fetching,
    Done,
}

#[derive(Clone)]
pub enum Status {
    IndexProgress { position: u64, length: u64 },
    DigestProgress { position: u64, length: u64 },
    FetchProgress { count: i64, length: u64 },
    VerifyProgress { position: u64, length: u64 },
    Done,
}

impl Fetcher {
    pub fn new(share: ShareInfo, clients: Clients) -> Fetcher {
        Fetcher {
            clients,
            have_index: share.want_index.empty(),
            want_index: share.want_index,
            state: State::Indexing,
            status_tx: broadcast::channel(16).0,
        }
    }

    pub async fn run(mut self, cancel: CancellationToken) -> Result<()> {
        loop {
            self.state = match self.state {
                State::Indexing => self.index(cancel.clone()).await?,
                State::Planning => self.plan(cancel.clone()).await?,
                State::Fetching => self.fetch(cancel.clone()).await?,
                State::Done => return Ok(()),
            }
        }
    }

    async fn index(&mut self, cancel: CancellationToken) -> Result<State> {
        let indexer = Indexer::from_wanted(&self.want_index).await?;

        // Set status updates while indexing
        let mut index_progress = indexer.subscribe_index_progress();
        let mut digest_progress = indexer.subscribe_digest_progress();
        let status_tx = self.status_tx.clone();
        let status_cancel = cancel.child_token();
        let status_task_cancel = status_cancel.clone();
        let status_task = tokio::spawn(async move {
            loop {
                select! {
                    _ = status_cancel.cancelled() => {
                        break;
                    }
                    res = index_progress.changed() => {
                        res.map_err(Error::other)?;
                        let progress = index_progress.borrow_and_update();
                        status_tx.send(
                            Status::IndexProgress{
                                position: progress.position,
                                length: progress.length,
                            }).unwrap_or_else(|_|{ warn!("no status subscribers"); 0});
                    }
                    res = digest_progress.changed() => {
                        res.map_err(Error::other)?;
                        let progress = digest_progress.borrow_and_update();
                        status_tx.send(
                            Status::DigestProgress{
                                position: progress.position,
                                length: progress.length,
                            }).unwrap_or_else(|_|{ warn!("no status subscribers"); 0});
                    }
                }
            }
            Ok::<(), Error>(())
        });

        // Index the file
        self.have_index = indexer.index().await?;

        // Stop status updates
        status_task_cancel.cancel();
        status_task.await.map_err(Error::other)??;

        // Ready for planning
        Ok(State::Planning)
    }

    async fn plan(&mut self, cancel: CancellationToken) -> Result<State> {
        let diff = self.want_index.diff(&self.have_index);
        let mut want_length = 0;
        let total_length = self.want_index.payload().length();
        for want_block in diff.want {
            select! {
                _ = cancel.cancelled() => {
                    return Err(Error::cancelled("plan cancelled"))
                }
                res = self.clients.block_fetcher.tx.send(block_fetcher::Request::Fetch {
                    block: FileBlockFetch {
                        file_index: want_block.file_index,
                        piece_index: want_block.piece_index,
                        piece_offset: want_block.piece_offset,
                        block_index: want_block.block_index,
                    },
                    flush: false,
                }) => {
                    res.map_err(Error::other)?;
                    want_length += want_block.block_length;
                }
            }
        }
        let mut have_length = 0;
        for have_block in diff.have {
            select! {
                _ = cancel.cancelled() => {
                    return Err(Error::cancelled("plan cancelled"))
                }
                res = self.clients.piece_verifier.tx.send(piece_verifier::Request::Piece(PieceState::new(
                    have_block.file_index,
                    have_block.piece_index,
                    have_block.piece_offset,
                    self.want_index.payload().pieces()[have_block.piece_index].block_count(),
                    have_block.block_index,
                ))) => {
                    res.map_err(Error::other)?;
                    have_length += have_block.block_length;
                }
            }
        }
        self.status_tx
            .send(Status::FetchProgress {
                count: have_length.try_into().unwrap(),
                length: total_length.try_into().unwrap(),
            })
            .unwrap_or_else(|_| {
                warn!("no status subscribers");
                0
            });
        Ok(if want_length == 0 {
            State::Done
        } else {
            State::Fetching
        })
    }

    async fn fetch(&mut self, cancel: CancellationToken) -> Result<State> {
        let mut verified_pieces = 0u64;
        let total_pieces = self.want_index.payload().pieces().len().try_into().unwrap();
        let total_length = self.want_index.payload().length();
        loop {
            select! {
                _ = cancel.cancelled() => {
                    return Err(Error::cancelled("fetch cancelled"))
                }
                res = self.clients.block_fetcher.rx.recv() => {
                    match res {
                        // An empty fetcher channel means we've received a
                        // response for all block requests. However, some of
                        // these might fail to validate.
                        None => continue,
                        Some(block_fetcher::Response::Fetched { block, length }) => {
                            // Send progress to subscribers
                            self.status_tx.send(
                                Status::FetchProgress{
                                    count: length.try_into().unwrap(),
                                    length: total_length.try_into().unwrap(),
                                }).unwrap_or_else(|_| { warn!("no status subscribers"); 0 });

                            // Update verifier
                            self.clients.piece_verifier.tx.send(piece_verifier::Request::Piece(PieceState::new(
                                block.file_index,
                                block.piece_index,
                                block.piece_offset,
                                self.want_index.payload().pieces()[block.piece_index].block_count(),
                                block.block_index,
                            )))
                            .await
                            .map_err(Error::other)?;
                        }
                        Some(block_fetcher::Response::FetchFailed { block, error }) => {
                            warn!("failed to fetch block: {}", error);
                            self.clients.block_fetcher.tx.send(
                                block_fetcher::Request::Fetch {
                                    block: block.clone(),
                                    flush: false
                                }).await.map_err(Error::other)?;
                        }
                    }
                }
                res = self.clients.piece_verifier.rx.recv() => {
                    match res {
                        None => continue,
                        Some(piece_verifier::Response::ValidPiece { file_index:_, piece_index, index_complete }) => {
                            verified_pieces += 1u64;

                            // Update verify progress
                            self.status_tx.send(
                                Status::VerifyProgress{
                                    length: total_pieces,
                                    position: verified_pieces
                                }).unwrap_or_else(|_| { warn!("no status subscribers"); 0 });

                            // Update have map
                            self.clients.have_announcer.tx.send(
                                have_announcer::Request::Set {
                                    piece_index: piece_index.try_into().unwrap(),
                                }).await.map_err(Error::other)?;

                            if index_complete {
                                return Ok(State::Done);
                            }
                        }
                        Some(piece_verifier::Response::InvalidPiece { file_index, piece_index }) => {
                            let piece_length = self.want_index.payload().pieces()[piece_index].length();
                            warn!(file_index, piece_index, piece_length, "invalid piece");

                            // Rewind the fetch progress status by the piece length
                            self.status_tx.send(
                                Status::FetchProgress{
                                    length: total_length.try_into().unwrap(),
                                    count: (0-piece_length).try_into().unwrap()
                                }).unwrap_or_else(|_| { warn!("no status subscribers"); 0 });

                            // Re-fetch all the blocks in the failed piece
                            let piece_blocks = piece_length / BLOCK_SIZE_BYTES + if piece_length % BLOCK_SIZE_BYTES > 0 { 1 } else { 0 };
                            for block_index in 0..piece_blocks  {
                                self.clients.block_fetcher.tx.send(
                                    block_fetcher::Request::Fetch {
                                        block: FileBlockFetch {
                                           file_index,
                                           piece_index,
                                           piece_offset: 0,
                                           block_index
                                        },
                                        flush: false,
                                    }).await.map_err(Error::other)?;
                            }
                        }
                        Some(piece_verifier::Response::IncompletePiece { .. }) => {}
                    }
                }
            }
        }
    }
}
