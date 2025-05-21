use std::ops::Deref;

use stigmerge_fileindex::{Index, Indexer, BLOCK_SIZE_BYTES};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::{select, try_join};
use tokio_util::sync::CancellationToken;
use tracing::{warn, Level};

use crate::types::{FileBlockFetch, PieceState, ShareInfo};
use crate::{actor::Operator, have_announcer};
use crate::{block_fetcher, piece_verifier, Error, Result};

pub struct Fetcher {
    clients: Clients,

    want_index: Index,
    have_index: Index,

    state: State,
    status_tx: watch::Sender<Status>,
    status_rx: watch::Receiver<Status>,

    fetch_tasks: JoinSet<Result<()>>,
    fetch_tx: flume::Sender<block_fetcher::Response>,
    fetch_rx: flume::Receiver<block_fetcher::Response>,

    verify_tx: flume::Sender<piece_verifier::Response>,
    verify_rx: flume::Receiver<piece_verifier::Response>,
}

pub struct Clients {
    pub block_fetcher: Operator<block_fetcher::Request>,
    pub piece_verifier: Operator<piece_verifier::Request>,
    pub have_announcer: Operator<have_announcer::Request>,
}

#[derive(Debug)]
pub enum State {
    Indexing,
    Planning,
    Fetching,
    Done,
}

#[derive(Clone, Debug)]
pub enum Status {
    NotStarted,
    IndexProgress { position: u64, length: u64 },
    DigestProgress { position: u64, length: u64 },
    FetchProgress { position: u64, length: u64 },
    VerifyProgress { position: u64, length: u64 },
    Done,
}

impl Status {
    fn position(&self) -> Option<u64> {
        match self {
            Status::NotStarted => None,
            Status::IndexProgress { position, .. } => Some(*position),
            Status::DigestProgress { position, .. } => Some(*position),
            Status::FetchProgress { position, .. } => Some(*position),
            Status::VerifyProgress { position, .. } => Some(*position),
            Status::Done => None,
        }
    }
}

impl Fetcher {
    #[tracing::instrument(skip_all, fields(share))]
    pub fn new(share: ShareInfo, clients: Clients) -> Fetcher {
        let (status_tx, status_rx) = watch::channel(Status::NotStarted);
        let (fetch_tx, fetch_rx) = flume::unbounded();
        let (verify_tx, verify_rx) = flume::unbounded();
        Fetcher {
            clients,
            have_index: share.want_index.empty(),
            want_index: share.want_index,
            state: State::Indexing,
            status_tx,
            status_rx,
            fetch_tasks: JoinSet::new(),
            fetch_tx,
            fetch_rx,
            verify_tx,
            verify_rx,
        }
    }

    #[tracing::instrument(skip_all)]
    pub fn subscribe_fetcher_status(&self) -> watch::Receiver<Status> {
        self.status_rx.clone()
    }

    #[tracing::instrument(skip_all, err(level = Level::TRACE), level = Level::TRACE)]
    pub async fn run(mut self, cancel: CancellationToken) -> Result<()> {
        loop {
            self.state = match self.state {
                State::Indexing => self.index(cancel.clone()).await?,
                State::Planning => self.plan(cancel.clone()).await?,
                State::Fetching => self.fetch(cancel.clone()).await?,
                State::Done => return self.join().await,
            }
        }
    }

    #[tracing::instrument(skip_all, err, ret)]
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
                        res?;
                        let progress = index_progress.borrow_and_update();
                        status_tx.send_replace(
                            Status::IndexProgress{
                                position: progress.position,
                                length: progress.length,
                            });
                    }
                    res = digest_progress.changed() => {
                        res?;
                        let progress = digest_progress.borrow_and_update();
                        status_tx.send_replace(
                            Status::DigestProgress{
                                position: progress.position,
                                length: progress.length,
                            });
                    }
                }
            }
            Ok::<(), Error>(())
        });

        // Index the file
        self.have_index = indexer.index().await?;

        // Stop status updates
        status_task_cancel.cancel();
        status_task.await??;

        // Ready for planning
        Ok(State::Planning)
    }

    #[tracing::instrument(skip_all, err, ret)]
    async fn plan(&mut self, cancel: CancellationToken) -> Result<State> {
        let diff = self.want_index.diff(&self.have_index);
        let mut want_length = 0;
        let total_length = self.want_index.payload().length();
        for want_block in diff.want {
            select! {
                _ = cancel.cancelled() => {
                    self.fetch_tasks.abort_all();
                    cancel.cancel();
                    return Err(Error::msg("plan cancelled"));
                }
                res = self.clients.block_fetcher.defer(
                    block_fetcher::Request::Fetch {
                        response_tx: None,
                        block: FileBlockFetch {
                            file_index: want_block.file_index,
                            piece_index: want_block.piece_index,
                            piece_offset: want_block.piece_offset,
                            block_index: want_block.block_index,
                        },
                        flush: true,
                    }, self.fetch_tx.clone(),
                ) => {
                    res?;
                    want_length += want_block.block_length;
                }
            }
        }
        let mut have_length = 0;
        for have_block in diff.have {
            select! {
                _ = cancel.cancelled() => {
                    return Err(Error::msg("plan cancelled"))
                }
                res = self.clients.piece_verifier.defer(piece_verifier::Request::Piece {
                    piece_state: PieceState::new(
                        have_block.file_index,
                        have_block.piece_index,
                        have_block.piece_offset,
                        self.want_index.payload().pieces()[have_block.piece_index].block_count(),
                        have_block.block_index,
                    ),
                    response_tx: None,
                }, self.verify_tx.clone()) => {
                    res?;
                    have_length += have_block.block_length;
                }
            }
        }
        self.status_tx.send_replace(Status::FetchProgress {
            position: have_length.try_into().unwrap(),
            length: total_length.try_into().unwrap(),
        });
        Ok(if want_length == 0 {
            self.status_tx.send_replace(Status::Done);
            State::Done
        } else {
            State::Fetching
        })
    }

    #[tracing::instrument(skip_all, err, ret)]
    async fn fetch(&mut self, cancel: CancellationToken) -> Result<State> {
        let mut verified_pieces = 0u64;
        let mut fetched_bytes = self.status_tx.borrow().deref().position().unwrap_or(0);
        let total_pieces = self.want_index.payload().pieces().len().try_into().unwrap();
        let total_length = self.want_index.payload().length();
        loop {
            select! {
                _ = cancel.cancelled() => {
                    return Ok(State::Done)
                }
                res = self.fetch_rx.recv_async() => {
                    match res {
                        // An empty fetcher channel means we've received a
                        // response for all block requests. However, some of
                        // these might fail to validate.
                        Err(_) => continue,
                        Ok(block_fetcher::Response::Fetched { block, length }) => {
                            fetched_bytes += TryInto::<u64>::try_into(length).unwrap();

                            // Send progress to subscribers
                            self.status_tx.send_replace(
                                Status::FetchProgress{
                                    position: fetched_bytes.try_into().unwrap(),
                                    length: total_length.try_into().unwrap(),
                                });

                            // Update verifier
                            self.clients.piece_verifier.defer(piece_verifier::Request::Piece {
                                piece_state: PieceState::new(
                                    block.file_index,
                                    block.piece_index,
                                    block.piece_offset,
                                    self.want_index.payload().pieces()[block.piece_index].block_count(),
                                    block.block_index,
                                ),
                                response_tx: None,
                            }, self.verify_tx.clone()).await?;
                        }
                        Ok(block_fetcher::Response::FetchFailed { block, error_msg }) => {
                            warn!("failed to fetch block: {}", error_msg);
                            self.clients.block_fetcher.defer(block_fetcher::Request::Fetch {
                                response_tx: None,
                                block: block.clone(),
                                flush: false
                            }, self.fetch_tx.clone()).await?;
                        }
                    }
                }
                res = self.verify_rx.recv_async() => {
                    match res? {
                        piece_verifier::Response::ValidPiece { file_index:_, piece_index, index_complete } => {
                            verified_pieces += 1u64;

                            // Update verify progress
                            self.status_tx.send_replace(
                                Status::VerifyProgress{
                                    length: total_pieces,
                                    position: verified_pieces
                                });

                            // Update have map
                            self.clients.have_announcer.send(
                                have_announcer::Request::Set {
                                    response_tx: None,
                                    piece_index: piece_index.try_into().unwrap(),
                                }).await?;

                            if index_complete {
                                self.status_tx.send_replace(Status::Done);
                                return Ok(State::Done);
                            }
                        }
                        piece_verifier::Response::InvalidPiece { file_index, piece_index } => {
                            let piece_length = self.want_index.payload().pieces()[piece_index].length();
                            warn!(file_index, piece_index, piece_length, "invalid piece");

                            fetched_bytes -= TryInto::<u64>::try_into(piece_length).unwrap();

                            // Rewind the fetch progress status by the piece length
                            self.status_tx.send_replace(
                                Status::FetchProgress{
                                    length: total_length.try_into().unwrap(),
                                    position: fetched_bytes,
                                });

                            // Re-fetch all the blocks in the failed piece
                            let piece_blocks = piece_length / BLOCK_SIZE_BYTES + if piece_length % BLOCK_SIZE_BYTES > 0 { 1 } else { 0 };
                            for block_index in 0..piece_blocks  {
                                self.clients.block_fetcher.defer(
                                    block_fetcher::Request::Fetch {
                                        response_tx: None,
                                        block: FileBlockFetch {
                                           file_index,
                                           piece_index,
                                           piece_offset: 0,
                                           block_index
                                        },
                                        flush: false,
                                    }, self.fetch_tx.clone()).await?;
                            }
                        }
                        piece_verifier::Response::IncompletePiece { .. } => {}
                    }
                }
            }
        }
    }

    #[tracing::instrument(skip_all, err)]
    async fn join(self) -> Result<()> {
        try_join!(
            self.clients.piece_verifier.join(),
            self.clients.block_fetcher.join(),
            self.clients.have_announcer.join(),
        )?;
        Ok(())
    }
}
