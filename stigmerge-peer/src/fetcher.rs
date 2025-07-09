use std::collections::HashMap;
use std::time::Duration;

use anyhow::Context;
use backoff::backoff::Backoff;
use stigmerge_fileindex::{Index, Indexer, BLOCK_SIZE_BYTES};
use tokio::sync::{broadcast, watch, MutexGuard};
use tokio::time::{interval, sleep};
use tokio::{select, try_join};
use tokio_util::sync::CancellationToken;
use tracing::{debug, enabled, error, info, trace, warn, Level};
use veilid_core::{Target, TypedRecordKey, VeilidUpdate};

use crate::actor::{ConnectionState, ConnectionStateHandle, ResponseChannel};
use crate::error::{CancelError, Transient, Unrecoverable};
use crate::peer_tracker::PeerTracker;
use crate::types::{FileBlockFetch, PieceState, ShareInfo};
use crate::{actor::Operator, have_announcer};
use crate::{
    block_fetcher, is_cancelled, is_unrecoverable, piece_verifier, share_resolver, Error, Node,
    Result,
};

pub struct Fetcher<N: Node> {
    node: N,
    clients: Clients,

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

    update_rx: broadcast::Receiver<VeilidUpdate>,

    fetched_block_peer: HashMap<FileBlockFetch, TypedRecordKey>,
}

pub struct Clients {
    pub block_fetcher: Operator<block_fetcher::Request>,
    pub piece_verifier: Operator<piece_verifier::Request>,
    pub have_announcer: Operator<have_announcer::Request>,
    pub share_resolver: Operator<share_resolver::Request>,
    pub share_target_rx: broadcast::Receiver<(TypedRecordKey, Target)>,
}

#[derive(Debug)]
pub enum State {
    Indexing,
    Planning { have_index: Index },
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
        fetch_position: i64,
        fetch_length: u64,
        verify_position: u64,
        verify_length: u64,
    },
    Done,
}

impl<N: Node> Fetcher<N> {
    pub fn new(node: N, share: ShareInfo, clients: Clients) -> Self {
        let (status_tx, status_rx) = watch::channel(Status::NotStarted);
        let (pending_fetch_tx, pending_fetch_rx) = flume::unbounded();
        let (fetch_resp_tx, fetch_resp_rx) = flume::unbounded();
        let (verify_resp_tx, verify_resp_rx) = flume::unbounded();
        let update_rx = node.subscribe_veilid_update();
        Fetcher {
            node,
            clients,
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

            update_rx,

            fetched_block_peer: HashMap::new(),
        }
    }

    pub fn subscribe_fetcher_status(&self) -> watch::Receiver<Status> {
        self.status_rx.clone()
    }

    pub async fn run(
        mut self,
        cancel: CancellationToken,
        conn_state: ConnectionStateHandle,
    ) -> Result<()> {
        let mut run_res = Ok(());
        let mut exp_backoff = backoff::ExponentialBackoffBuilder::new()
            .with_max_interval(Duration::from_secs(5))
            .build();
        let mut retries = 0;
        loop {
            let conn_rx = {
                let st = conn_state.lock().await;
                if !*st.connected.borrow() {
                    self.disconnected(cancel.clone(), st).await?;
                    continue;
                }
                st.subscribe()
            };

            let cancel_iter = cancel.clone();
            let res = match self.state {
                State::Indexing => self.index(cancel_iter).await,
                State::Planning { ref have_index } => {
                    self.plan(cancel_iter, have_index.clone()).await
                }
                State::Fetching => self.fetch(cancel_iter, conn_rx).await,
                State::Done => {
                    info!("done");
                    cancel.cancelled().await;
                    break;
                }
            };
            match res {
                Ok(state) => self.state = state,
                Err(e) => {
                    debug!("{e}");
                    if is_cancelled(&e) {
                        break;
                    }
                    if e.is_transient() {
                        sleep(
                            exp_backoff
                                .next_backoff()
                                .unwrap_or(exp_backoff.max_interval),
                        )
                        .await;
                        retries += 1;
                        if retries < crate::actor::MAX_TRANSIENT_RETRIES {
                            continue;
                        }
                        warn!("too many transient errors");
                    } else if is_unrecoverable(&e) {
                        cancel.cancel();
                        error!("{e}");
                        run_res = Err(e);
                        break;
                    } else if is_cancelled(&e) {
                        break;
                    }
                    {
                        if let Ok(st) = conn_state.try_lock() {
                            info!("marking disconnected, resetting");
                            self.node.reset().await?;
                            st.disconnect();
                            exp_backoff.reset();
                            retries = 0;
                        }
                    }
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
        let have_index = indexer
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
        Ok(State::Planning { have_index })
    }

    async fn plan(&mut self, cancel: CancellationToken, have_index: Index) -> Result<State> {
        debug!("planning");
        let diff = self.share.want_index.diff(&have_index);
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

    async fn fetch(
        &mut self,
        cancel: CancellationToken,
        mut conn_rx: watch::Receiver<bool>,
    ) -> Result<State> {
        debug!("fetching");
        let total_pieces = self
            .share
            .want_index
            .payload()
            .pieces()
            .len()
            .try_into()
            .unwrap();
        let total_length: u64 = self.share.want_index.payload().length().try_into().unwrap();

        let mut refresh_routes_interval = interval(Duration::from_secs(30));

        loop {
            select! {
                _ = cancel.cancelled() => {
                    return Err(CancelError.into())
                }
                res = conn_rx.changed() => {
                    res.context(Unrecoverable::new("connection changed"))?;
                    if !*conn_rx.borrow() {
                        return Err(Error::msg("connection lost"));
                    }
                }
                res = self.clients.share_target_rx.recv() => {
                    let (key, target) = res.context(Unrecoverable::new("receive share target update"))?;
                    debug!("share target update for {key}: {target:?}");
                    match self.peer_tracker.update(key, target.to_owned()) {
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
                    if enabled!(Level::TRACE) {
                        for share_key in self.peer_tracker.keys() {
                            trace!("peer score for {share_key}: {:?}",
                                self.peer_tracker.status(share_key).map(|status| status.score()));
                        }
                    }
                }
                res = self.verify_resp_rx.recv_async() => {
                    match res.context(Unrecoverable::new("receive piece verification"))? {
                        piece_verifier::Response::ValidPiece { file_index: _, piece_index, index_complete } => {

                            // Update verify progress
                            self.status_tx.send_modify(|status| {
                                match status {
                                    Status::FetchProgress { verify_position, verify_length, .. } => {
                                        *verify_length = total_pieces;
                                        *verify_position += 1u64;
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

                            // Rewind the fetch progress status by the piece length
                            self.status_tx.send_modify(|status| {
                                match status {
                                    Status::FetchProgress{ fetch_position, fetch_length, .. } => {
                                        *fetch_length = total_length;
                                        *fetch_position -= TryInto::<i64>::try_into(piece_length).unwrap();
                                    }
                                    _ => {}
                                };
                            });
                            // Re-fetch all the blocks in the failed piece
                            let piece_blocks = piece_length / BLOCK_SIZE_BYTES + if piece_length % BLOCK_SIZE_BYTES > 0 { 1 } else { 0 };
                            for block_index in 0..piece_blocks  {
                                let block = FileBlockFetch {
                                        file_index,
                                        piece_index,
                                        piece_offset: 0,
                                        block_index
                                    } ;
                                if let Some(block_peer) = self.fetched_block_peer.remove(&block) {
                                    // Attribute a block fetch error to the peer whence it was fetched, if known.
                                    // This could cause cooperating peers to be temporarily unfairly penalized when
                                    // contributing to a piece with a defector.
                                    //
                                    // However, in subsequent pieces the defector should be caught and more severely
                                    // downgraded, since all the blocks would be attributed to the defector.
                                    self.peer_tracker.fetch_err(&block_peer, &Error::msg("invalid piece"));
                                }
                                self.pending_fetch_tx.send_async(block).await.context(
                                    Unrecoverable::new("enqueue block fetch for invalid piece"))?;
                            }
                        }
                        piece_verifier::Response::IncompletePiece { .. } => {}
                    }
                }
                res = self.pending_fetch_rx.recv_async() => {
                    let block = res.context(Unrecoverable::new("receive pending block fetch"))?;
                    match match self.peer_tracker.share_target(&block) {
                        Ok(Some((share_key, target))) => Some((share_key, target)),
                        Ok(None) => None,
                        Err(e) => {
                            warn!("choose share target: {}", e);
                            None
                        }
                    } {
                        Some((share_key, target)) => {
                            self.clients.block_fetcher.defer(block_fetcher::Request::Fetch{
                                response_tx: ResponseChannel::default(),
                                share_key: share_key.to_owned(),
                                target: target.to_owned(),
                                block,
                                flush: true,
                            }, self.fetch_resp_tx.clone()).await.context(Unrecoverable::new("defer block fetch"))?;
                        }
                        None => {
                            // We can't dispatch the block, re-queue the block for fetching when conditions improve.
                            self.pending_fetch_tx.send_async(block).await.context(Unrecoverable::new("send pending block fetch"))?;
                            warn!("no peers available");
                        }
                    };
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
                            self.peer_tracker.fetch_ok(&share_key);

                            // Send progress to subscribers
                            self.status_tx.send_modify(|status| {
                                match status {
                                    Status::FetchProgress{ fetch_position, fetch_length, .. } => {
                                        *fetch_position += TryInto::<i64>::try_into(length).unwrap();
                                        *fetch_length = total_length;
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
                            // Attribute this block to the peer whence it was fetched.
                            self.fetched_block_peer.insert(block, share_key);
                        }
                        Ok(block_fetcher::Response::FetchFailed { share_key, block, err, .. }) => {
                            trace!("fetch block from {share_key} failed: {:?}", err);
                            // Update peer stats with block fetch failure.
                            self.peer_tracker.fetch_err(&share_key, &err);
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
                    self.peer_tracker.update(share_key.to_owned(), target);
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
        )?;
        Ok(())
    }

    async fn disconnected<'a>(
        &mut self,
        cancel: CancellationToken,
        state: MutexGuard<'a, ConnectionState>,
    ) -> Result<()> {
        let mut reset_interval = interval(Duration::from_secs(120));
        loop {
            select! {
                _ = cancel.cancelled() => {
                    trace!("cancelled");
                    return Err(CancelError.into())
                }
                res = self.update_rx.recv() => {
                    let update = res?;
                    match update {
                        VeilidUpdate::Attachment(veilid_state_attachment) => {
                            if veilid_state_attachment.public_internet_ready {
                                info!("connected: {:?}", veilid_state_attachment);
                                state.connect();
                                return Ok(())
                            }
                            info!("disconnected: {:?}", veilid_state_attachment);
                        }
                        VeilidUpdate::Shutdown => {
                            cancel.cancel();
                        }
                        _ => {}
                    }
                }
                _ = reset_interval.tick() => {
                    warn!("resetting connection");
                    self.node.reset().await?;
                }
            }
        }
    }
}
