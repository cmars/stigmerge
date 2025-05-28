use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use backoff::backoff::Backoff;
use stigmerge_fileindex::{Index, Indexer, BLOCK_SIZE_BYTES};
use tokio::sync::{watch, Mutex};
use tokio::{select, try_join};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn, Level};
use veilid_core::Target;

use crate::actor::ResponseChannel;
use crate::node::TypedKey;
use crate::peer_tracker::PeerTracker;
use crate::types::{FileBlockFetch, PieceState, ShareInfo};
use crate::{actor::Operator, have_announcer};
use crate::{block_fetcher, peer_resolver, piece_verifier, share_resolver, Error, Node, Result};

pub struct Fetcher<N: Node> {
    node: N,
    clients: Clients,

    have_index: Index,
    share: ShareInfo,

    state: State,
    status_tx: watch::Sender<Status>,
    status_rx: watch::Receiver<Status>,

    pending_fetch_tx: flume::Sender<FileBlockFetch>,
    pending_fetch_rx: flume::Receiver<FileBlockFetch>,

    fetch_resp_tx: flume::Sender<block_fetcher::Response>,
    fetch_resp_rx: flume::Receiver<block_fetcher::Response>,

    verify_resp_tx: flume::Sender<piece_verifier::Response>,
    verify_resp_rx: flume::Receiver<piece_verifier::Response>,
}

pub struct Clients {
    pub block_fetcher: Operator<block_fetcher::Request>,
    pub piece_verifier: Operator<piece_verifier::Request>,
    pub have_announcer: Operator<have_announcer::Request>,
    pub share_resolver: Operator<share_resolver::Request>,
    pub share_target_rx: flume::Receiver<(TypedKey, Target)>,
    pub peer_resolver: Operator<peer_resolver::Request>,
    pub peer_tracker: Arc<Mutex<PeerTracker>>,
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

impl<N: Node> Fetcher<N> {
    #[tracing::instrument(skip_all, fields(share))]
    pub fn new(node: N, share: ShareInfo, clients: Clients) -> Self {
        let (status_tx, status_rx) = watch::channel(Status::NotStarted);
        let (pending_fetch_tx, pending_fetch_rx) = flume::unbounded();
        let (fetch_resp_tx, fetch_resp_rx) = flume::unbounded();
        let (verify_resp_tx, verify_resp_rx) = flume::unbounded();
        Fetcher {
            node,
            clients,
            have_index: share.want_index.empty(),
            share,
            state: State::Indexing,

            status_tx,
            status_rx,

            pending_fetch_tx,
            pending_fetch_rx,

            fetch_resp_tx,
            fetch_resp_rx,

            verify_resp_tx,
            verify_resp_rx,
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
        let indexer = Indexer::from_wanted(&self.share.want_index).await?;

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
        let diff = self.share.want_index.diff(&self.have_index);
        let mut want_length = 0;
        let total_length = self.share.want_index.payload().length();
        for want_block in diff.want {
            select! {
                _ = cancel.cancelled() => {
                    cancel.cancel();
                    return Err(Error::msg("plan cancelled"));
                }
                res = self.pending_fetch_tx.send_async(
                        FileBlockFetch {
                            file_index: want_block.file_index,
                            piece_index: want_block.piece_index,
                            piece_offset: want_block.piece_offset,
                            block_index: want_block.block_index,
                        }) => {
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
                        self.share.want_index.payload().pieces()[have_block.piece_index].block_count(),
                        have_block.block_index,
                    ),
                    response_tx: ResponseChannel::default(),
                }, self.verify_resp_tx.clone()) => {
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
        let total_pieces = self
            .share
            .want_index
            .payload()
            .pieces()
            .len()
            .try_into()
            .unwrap();
        let total_length = self.share.want_index.payload().length();
        let mut no_peers_backoff = backoff::ExponentialBackoffBuilder::new()
            .with_max_interval(Duration::from_secs(5))
            .build();
        loop {
            select! {
                _ = cancel.cancelled() => {
                    return Ok(State::Done)
                }
                res = self.pending_fetch_rx.recv_async() => {
                    let block = res?;
                    let peer_tracker = self.clients.peer_tracker.lock().await;
                    let (share_key, target) = match peer_tracker.share_target(&block)? {
                        Some((share_key, target)) => (share_key, target),
                        None => {
                            match no_peers_backoff.next_backoff() {
                                Some(duration) => {
                                    warn!("no peers available, retrying in {:?}", duration);
                                    tokio::time::sleep(duration).await;
                                }
                                None => {
                                    return Err(Error::msg("no peers available"));
                                }
                            }
                            continue;
                        },
                    };
                    self.clients.block_fetcher.defer(block_fetcher::Request::Fetch{
                        response_tx: ResponseChannel::default(),
                        share_key: share_key.to_owned(),
                        target: target.to_owned(),
                        block,
                        flush: self.pending_fetch_rx.is_empty(),
                    }, self.fetch_resp_tx.clone()).await?;
                }
                res = self.fetch_resp_rx.recv_async() => {
                    let mut peer_tracker = self.clients.peer_tracker.lock().await;
                    match res {
                        // An empty fetcher channel means we've received a
                        // response for all block requests. However, some of
                        // these might fail to validate.
                        Err(_) => continue,
                        Ok(block_fetcher::Response::Fetched { share_key, block, length }) => {
                            // Update peer stats with success
                            peer_tracker.fetch_ok(&share_key).await;

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
                                    self.share.want_index.payload().pieces()[block.piece_index].block_count(),
                                    block.block_index,
                                ),
                                response_tx: ResponseChannel::default(),
                            }, self.verify_resp_tx.clone()).await?;
                        }
                        Ok(block_fetcher::Response::FetchFailed { share_key, block, err }) => {
                            warn!("failed to fetch block: {:?}", err);
                            // Update peer stats with failure
                            if let Some(target) = peer_tracker.fetch_err(&share_key, err).await {
                                self.clients.share_resolver.call(share_resolver::Request::Header {
                                    response_tx: ResponseChannel::default(),
                                    key: share_key,
                                    prior_target: Some(target),
                                }).await?;
                            }
                            self.pending_fetch_tx.send_async(block).await?;
                        }
                    }
                }
                res = self.verify_resp_rx.recv_async() => {
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
                                    response_tx: ResponseChannel::default(),
                                    piece_index: piece_index.try_into().unwrap(),
                                }).await?;

                            if index_complete {
                                self.status_tx.send_replace(Status::Done);
                                return Ok(State::Done);
                            }
                        }
                        piece_verifier::Response::InvalidPiece { file_index, piece_index } => {
                            let piece_length = self.share.want_index.payload().pieces()[piece_index].length();
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
                                // TODO: spread these requests across peers
                                // TODO: update peer stats, bad pieces should be penalized
                                self.pending_fetch_tx.send_async(
                                    FileBlockFetch {
                                        file_index,
                                        piece_index,
                                        piece_offset: 0,
                                        block_index
                                    }).await?;
                            }
                        }
                        piece_verifier::Response::IncompletePiece { .. } => {}
                    }
                }
                res = self.clients.share_target_rx.recv_async() => {
                    let (key, target) = res?;
                    debug!("share target update for {key}: {target:?}");
                    if let None = self.clients.peer_tracker.lock().await.update(key, target.to_owned()).await {
                        // Never seen this peer before, advertise ourselves to it
                        self.node.request_advertise_peer(&target, &self.share.key).await?;
                        debug!("advertised our share {} to target {:?}", self.share.key, target);
                    }
                }

                // Resolve newly discovered peers
            }
        }
    }

    #[tracing::instrument(skip_all, err)]
    async fn join(self) -> Result<()> {
        try_join!(
            self.clients.piece_verifier.join(),
            self.clients.block_fetcher.join(),
            self.clients.have_announcer.join(),
            self.clients.share_resolver.join(),
            self.clients.peer_resolver.join(),
        )?;
        Ok(())
    }
}
