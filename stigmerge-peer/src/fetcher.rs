use std::ops::Deref;
use std::time::Duration;

use anyhow::Context;
use backoff::backoff::Backoff;
use stigmerge_fileindex::{Index, Indexer, BLOCK_SIZE_BYTES};
use tokio::sync::watch;
use tokio::time::interval;
use tokio::{select, try_join};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};
use veilid_core::{Target, TypedRecordKey};

use crate::actor::ResponseChannel;
use crate::error::{CancelError, Unrecoverable};
use crate::peer_tracker::PeerTracker;
use crate::types::{FileBlockFetch, PieceState, ShareInfo};
use crate::{actor::Operator, have_announcer};
use crate::{
    block_fetcher, is_cancelled, peer_resolver, piece_verifier, proto, share_resolver, Node, Result,
};

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

    peer_tracker: PeerTracker,
}

pub struct Clients {
    pub block_fetcher: Operator<block_fetcher::Request>,
    pub piece_verifier: Operator<piece_verifier::Request>,
    pub have_announcer: Operator<have_announcer::Request>,
    pub share_resolver: Operator<share_resolver::Request>,
    pub share_target_rx: flume::Receiver<(TypedRecordKey, Target)>,
    pub peer_resolver: Operator<peer_resolver::Request>,
    pub discovered_peers_rx: flume::Receiver<(TypedRecordKey, proto::PeerInfo)>,
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
    IndexProgress {
        position: u64,
        length: u64,
    },
    DigestProgress {
        position: u64,
        length: u64,
    },
    FetchProgress {
        fetch_position: u64,
        fetch_length: u64,
        verify_position: u64,
        verify_length: u64,
    },
    Done,
}

impl Status {
    fn position(&self) -> Option<u64> {
        match self {
            Status::NotStarted => None,
            Status::IndexProgress { position, .. } => Some(*position),
            Status::DigestProgress { position, .. } => Some(*position),
            Status::FetchProgress {
                fetch_position: position,
                ..
            } => Some(*position),
            Status::Done => None,
        }
    }
}

impl<N: Node> Fetcher<N> {
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

            peer_tracker: PeerTracker::new(),
        }
    }

    pub fn subscribe_fetcher_status(&self) -> watch::Receiver<Status> {
        self.status_rx.clone()
    }

    pub async fn run(mut self, cancel: CancellationToken) -> Result<()> {
        let mut run_res = Ok(());
        loop {
            let cancel_iter = cancel.clone();
            let res = match self.state {
                State::Indexing => self.index(cancel_iter).await,
                State::Planning => self.plan(cancel_iter).await,
                State::Fetching => self.fetch(cancel_iter).await,
                State::Done => {
                    info!("done");
                    cancel.cancelled().await;
                    break;
                }
            };
            match res {
                Ok(state) => self.state = state,
                Err(e) => {
                    if !is_cancelled(&e) {
                        error!("{e}");
                    }
                    run_res = Err(e);
                    break;
                }
            }
        }
        self.join().await?;
        run_res
    }

    async fn index(&mut self, cancel: CancellationToken) -> Result<State> {
        debug!("indexing");
        let indexer = Indexer::from_wanted(&self.share.want_index)
            .await
            .context(Unrecoverable::new("indexer"))?;

        // Set status updates while indexing
        let mut index_progress = indexer.subscribe_index_progress();
        let mut digest_progress = indexer.subscribe_digest_progress();
        let status_tx = self.status_tx.clone();
        let status_cancel = cancel.child_token();
        let status_task_cancel = status_cancel.clone();
        let status_task: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async move {
            loop {
                select! {
                    _ = status_cancel.cancelled() => {
                        return Ok(());
                    }
                    res = index_progress.changed() => {
                        res.context(Unrecoverable::new("receive index progress"))?;
                        let progress = index_progress.borrow_and_update();
                        status_tx.send_replace(
                            Status::IndexProgress{
                                position: progress.position,
                                length: progress.length,
                            });
                    }
                    res = digest_progress.changed() => {
                        res.context(Unrecoverable::new("receive digest progress"))?;
                        let progress = digest_progress.borrow_and_update();
                        status_tx.send_replace(
                            Status::DigestProgress{
                                position: progress.position,
                                length: progress.length,
                            });
                    }
                }
            }
        });

        // Index the file
        self.have_index = indexer
            .index()
            .await
            .context(Unrecoverable::new("index local share"))?;

        // Stop status updates
        status_task_cancel.cancel();
        match status_task.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => debug!("status progress: {e}"),
            Err(e) => debug!("status progress task: {e}"),
        };

        // Ready for planning
        Ok(State::Planning)
    }

    async fn plan(&mut self, cancel: CancellationToken) -> Result<State> {
        debug!("planning");
        let diff = self.share.want_index.diff(&self.have_index);
        let mut want_length = 0;
        let total_length = self.share.want_index.payload().length();
        for want_block in diff.want {
            select! {
                _ = cancel.cancelled() => {
                    cancel.cancel();
                    return Err(CancelError.into());
                }
                res = self.pending_fetch_tx.send_async(
                        FileBlockFetch {
                            file_index: want_block.file_index,
                            piece_index: want_block.piece_index,
                            piece_offset: want_block.piece_offset,
                            block_index: want_block.block_index,
                        }) => {
                    res.context(Unrecoverable::new("receive pending block fetch"))?;
                    want_length += want_block.block_length;
                }
            }
        }
        let mut have_length = 0;
        for have_block in diff.have {
            select! {
                _ = cancel.cancelled() => {
                    return Err(CancelError.into());
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
                    res.context(Unrecoverable::new("defer verification of local share piece"))?;
                    have_length += have_block.block_length;
                }
            }
        }
        self.status_tx.send_modify(|status| {
            *status = Status::FetchProgress {
                fetch_position: have_length.try_into().unwrap(),
                fetch_length: total_length.try_into().unwrap(),
                verify_position: 0,
                verify_length: TryInto::<u64>::try_into(
                    self.share.want_index.payload().pieces().len(),
                )
                .unwrap(),
            };
        });
        Ok(if want_length == 0 {
            self.status_tx.send_replace(Status::Done);
            State::Done
        } else {
            State::Fetching
        })
    }

    async fn fetch(&mut self, cancel: CancellationToken) -> Result<State> {
        debug!("fetching");
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
            // TODO: make the max elapsed time configurable
            //.with_max_elapsed_time(None)
            .build();
        let mut refresh_routes_interval = interval(Duration::from_secs(30));
        refresh_routes_interval.reset();
        loop {
            select! {
                biased;
                _ = cancel.cancelled() => {
                    return Err(CancelError.into())
                }
                res = self.clients.share_target_rx.recv_async() => {
                    let (key, target) = res.context(Unrecoverable::new("receive share target update"))?;
                    debug!("share target update for {key}: {target:?}");
                    match self.peer_tracker.update(key, target.to_owned()).await {
                        None => {
                            // Never seen this peer before, advertise ourselves to it
                            // TODO: this be handled by a WithVeilidConnection-run actor
                            if let Err(e) = self.node.request_advertise_peer(
                                &target,
                                &self.share.key).await.with_context(|| format!("advertising our share to new peer")) {
                                    warn!("{e}");
                            }
                            refresh_routes_interval.reset();
                        }
                        Some(prior_target) => {
                            if prior_target != target {
                                refresh_routes_interval.reset();
                            }
                        }
                    }
                }
                _ = refresh_routes_interval.tick() => {
                    self.refresh_share_targets().await?;
                }
                res = self.verify_resp_rx.recv_async() => {
                    match res.context(Unrecoverable::new("receive piece verification"))? {
                        piece_verifier::Response::ValidPiece { file_index: _, piece_index, index_complete } => {
                            verified_pieces += 1u64;

                            // Update verify progress
                            self.status_tx.send_modify(|status| {
                                match status {
                                    Status::FetchProgress { verify_position, verify_length, .. } => {
                                        *verify_length = total_pieces;
                                        *verify_position = verified_pieces;
                                    }
                                    _ => {}
                                }
                            });

                            // Update have map
                            self.clients.have_announcer.send(
                                have_announcer::Request::Set {
                                    response_tx: ResponseChannel::default(),
                                    piece_index: piece_index.try_into().unwrap(),
                                }).await.context(Unrecoverable::new("send verified piece to have-map announcer"))?;

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
                            self.status_tx.send_modify(|status| {
                                match status {
                                    Status::FetchProgress{ fetch_position, fetch_length, .. } => {
                                        *fetch_length = total_length.try_into().unwrap();
                                        *fetch_position = fetched_bytes;
                                    }
                                    _ => {}
                                };
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
                                    }).await.context(Unrecoverable::new("enqueue block fetch for invalid piece"))?;
                            }
                        }
                        piece_verifier::Response::IncompletePiece { .. } => {}
                    }
                }
                res = self.pending_fetch_rx.recv_async() => {
                    let block = res.context(Unrecoverable::new("receive pending block fetch"))?;
                    let (share_key, target) = match match self.peer_tracker.share_target(&block).await {
                        Ok(Some((share_key, target))) => Some((share_key, target)),
                        Ok(None) => None,
                        Err(e) => {
                            warn!("choose share target: {}", e);
                            None
                        }
                    } {
                        Some((share_key, target)) => {
                            no_peers_backoff.reset();
                            (share_key, target)
                        }
                        None => {
                            match no_peers_backoff.next_backoff() {
                                Some(duration) => {
                                    warn!("no peers available, retrying in {:?}", duration);
                                    select! {
                                        _ = cancel.cancelled() => {
                                            return Err(CancelError.into())
                                        }
                                        _ = tokio::time::sleep(duration) => {}
                                    }
                                }
                                None => {
                                    return Err(
                                        anyhow::anyhow!("no peers available after {:?}", no_peers_backoff.get_elapsed_time())
                                    ).context(Unrecoverable::new("share target"));
                                }
                            }
                            continue;
                        }
                    };
                    self.clients.block_fetcher.defer(block_fetcher::Request::Fetch{
                        response_tx: ResponseChannel::default(),
                        share_key: share_key.to_owned(),
                        target: target.to_owned(),
                        block,
                        flush: true,
                    }, self.fetch_resp_tx.clone()).await.context(Unrecoverable::new("defer block fetch"))?;
                }

                // Resolve newly discovered peers
                res = self.clients.discovered_peers_rx.recv_async() => {
                    let (key, peer_info) = res.context(Unrecoverable::new("receive discovered peer"))?;
                    debug!("discovered peer {} from {}", peer_info.key(), key);
                    if !self.peer_tracker.contains(peer_info.key()) {
                        self.clients.share_resolver.call(share_resolver::Request::Index{
                            response_tx: ResponseChannel::default(),
                            key,
                            want_index_digest: Some(self.share.want_index_digest),
                            root: self.share.root.to_owned(),
                        }).await.err().map(|e| {
                            warn!("failed to resolve share for discovered peer {} at {key}: {e}", peer_info.key());
                        });
                        self.clients.peer_resolver.call(peer_resolver::Request::Watch{
                            response_tx: ResponseChannel::default(),
                            key: peer_info.key().to_owned(),
                        }).await.err().map(|e| {
                            warn!("failed to watch peers of discovered peer {} at {key}: {e}", peer_info.key());
                        });
                    }
                }

                res = self.fetch_resp_rx.recv_async() => {
                    match res.with_context(|| format!("fetcher: receive block fetch response")) {
                        // An empty fetcher channel means we've received a
                        // response for all block requests. However, some of
                        // these might fail to validate.
                        Err(e) => {
                            trace!("receive block fetch response: {e}");
                        },
                        Ok(block_fetcher::Response::Fetched { share_key, block, length }) => {
                            refresh_routes_interval.reset();

                            // Update peer stats with success
                            self.peer_tracker.fetch_ok(&share_key).await;

                            fetched_bytes += TryInto::<u64>::try_into(length).unwrap();

                            // Send progress to subscribers
                            self.status_tx.send_modify(|status| {
                                match status {
                                    Status::FetchProgress{ fetch_position, fetch_length, .. } => {
                                        *fetch_position= fetched_bytes.try_into().unwrap();
                                        *fetch_length= total_length.try_into().unwrap();
                                    }
                                    _ => {}
                                };
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
                            }, self.verify_resp_tx.clone()).await.context(
                                Unrecoverable::new("defer piece verification for fetched block"))?;
                        }
                        Ok(block_fetcher::Response::FetchFailed { share_key, block, err, .. }) => {
                            trace!("fetch block failed: {:?}", err);
                            // Update peer stats with failure
                            self.peer_tracker.fetch_err(&share_key, &err).await;
                            self.pending_fetch_tx.send_async(block).await.context(
                                Unrecoverable::new("enqueue failed block fetch"))?;
                        }
                    }
                }
            }
        }
    }

    async fn refresh_share_targets(&mut self) -> Result<()> {
        let share_keys = self
            .peer_tracker
            .keys()
            .map(|k| k.to_owned())
            .collect::<Vec<_>>();
        for share_key in share_keys {
            debug!("refresh target for share key {share_key}");
            let resp = self
                .clients
                .share_resolver
                .call(share_resolver::Request::Header {
                    response_tx: ResponseChannel::default(),
                    key: share_key.to_owned(),
                    prior_target: None,
                })
                .await
                .context(Unrecoverable::new("call share resolver"))?;
            match resp {
                share_resolver::Response::Header { target, .. } => {
                    self.peer_tracker.update(share_key.to_owned(), target).await;
                }
                other => {
                    warn!("unexpected share resolver response: {:?}", other);
                }
            }
        }
        Ok(())
    }

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
