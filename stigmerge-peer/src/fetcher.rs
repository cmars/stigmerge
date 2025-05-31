use std::collections::HashMap;
use std::ops::Deref;
use std::time::Duration;

use anyhow::Context;
use backoff::backoff::Backoff;
use moka::future::Cache;
use stigmerge_fileindex::{Index, Indexer, BLOCK_SIZE_BYTES};
use tokio::sync::watch;
use tokio::{select, try_join};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use veilid_core::Target;

use crate::actor::{Actor, Respondable, ResponseChannel};
use crate::error::is_route_invalid;
use crate::node::TypedKey;
use crate::types::{FileBlockFetch, PieceState, ShareInfo};
use crate::{actor::Operator, have_announcer};
use crate::{
    block_fetcher, peer_resolver, piece_verifier, proto, share_resolver, Error, Node, Result,
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

#[derive(Debug, Clone)]
struct PeerStatus {
    fetch_ok_count: u32,
    fetch_err_count: u32,
    is_invalid_target: bool,
}

impl PeerStatus {
    fn score(&self) -> i32 {
        if self.is_invalid_target {
            i32::MIN
        } else {
            TryInto::<i32>::try_into(self.fetch_ok_count).unwrap()
                - TryInto::<i32>::try_into(self.fetch_err_count).unwrap()
        }
    }
}

impl Default for PeerStatus {
    fn default() -> Self {
        PeerStatus {
            fetch_ok_count: 0,
            fetch_err_count: 0,
            is_invalid_target: false,
        }
    }
}

pub struct PeerTracker {
    targets: HashMap<TypedKey, Target>,
    peer_status: Cache<TypedKey, PeerStatus>,
}

const MAX_TRACKED_PEERS: u64 = 64;

impl PeerTracker {
    pub fn new() -> Self {
        PeerTracker {
            targets: HashMap::new(),
            peer_status: Cache::builder()
                .time_to_idle(Duration::from_secs(60))
                .max_capacity(MAX_TRACKED_PEERS)
                .build(),
        }
    }

    pub async fn update(&mut self, key: TypedKey, target: Target) -> Option<Target> {
        self.reset(&key).await;
        self.targets.insert(key, target)
    }

    pub fn contains(&self, key: &TypedKey) -> bool {
        self.targets.contains_key(key)
    }

    pub async fn fetch_ok(&mut self, key: &TypedKey) {
        let mut status = match self.peer_status.get(&key).await {
            Some(status) => status,
            None => PeerStatus::default(),
        };
        status.fetch_err_count = 0;
        status.is_invalid_target = false;
        status.fetch_ok_count += 1;
        self.peer_status.insert(key.clone(), status).await;
    }

    pub async fn fetch_err(&mut self, key: &TypedKey, err: Error) -> Option<Target> {
        let mut status = match self.peer_status.get(&key).await {
            Some(status) => status,
            None => PeerStatus::default(),
        };
        let is_invalid_target = is_route_invalid(&err);
        let is_newly_invalid = status.is_invalid_target != is_invalid_target;

        status.fetch_err_count += 1;
        status.is_invalid_target = is_invalid_target;
        self.peer_status.insert(key.clone(), status).await;

        if is_newly_invalid {
            self.targets.get(key).map(|target| target.to_owned())
        } else {
            None
        }
    }

    pub async fn reset(&mut self, key: &TypedKey) {
        self.peer_status.remove(key).await;
    }

    pub fn share_target(&self, _block: &FileBlockFetch) -> Result<Option<(&TypedKey, &Target)>> {
        // TODO: factor in have_map and block
        let mut peers: Vec<(TypedKey, PeerStatus)> = self
            .peer_status
            .iter()
            .map(|(key, status)| (*key, status))
            .collect();
        peers.sort_by(|(_, l_status), (_, r_status)| r_status.score().cmp(&l_status.score()));
        if !peers.is_empty() {
            if peers[0].1.score() < -1024 {
                return Err(Error::msg("peer score dropped below minimum threshold"));
            }
            return Ok(self.targets.get_key_value(&peers[0].0));
        }
        if self.targets.is_empty() {
            Ok(None)
        } else {
            Ok(self
                .targets
                .iter()
                .nth(rand::random::<usize>() % self.targets.len()))
        }
    }
}

pub struct Clients {
    pub block_fetcher: Operator<block_fetcher::Request>,
    pub piece_verifier: Operator<piece_verifier::Request>,
    pub have_announcer: Operator<have_announcer::Request>,
    pub share_resolver: Operator<share_resolver::Request>,
    pub share_target_rx: flume::Receiver<(TypedKey, Target)>,
    pub peer_resolver: Operator<peer_resolver::Request>,
    pub discovered_peers_rx: flume::Receiver<(TypedKey, proto::PeerInfo)>,
}

#[derive(Clone, Debug)]
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
                    res.with_context(|| format!("fetcher: enqueue block fetch"))?;
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
                    res.with_context(|| format!("fetcher: defer verify piece"))?;
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
                    let block = res.with_context(|| format!("fetcher: receive pending block fetch"))?;
                    let (share_key, target) = match self.peer_tracker.share_target(&block).with_context(
                            || format!("fetcher: choose share target"))? {
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
                    }, self.fetch_resp_tx.clone()).await.with_context(|| format!("fetcher: defer block fetch"))?;
                }
                res = self.fetch_resp_rx.recv_async() => {
                    match res.with_context(|| format!("fetcher: receive block fetch response")) {
                        // An empty fetcher channel means we've received a
                        // response for all block requests. However, some of
                        // these might fail to validate.
                        Err(_) => continue,
                        Ok(block_fetcher::Response::Fetched { share_key, block, length }) => {
                            // Update peer stats with success
                            self.peer_tracker.fetch_ok(&share_key).await;

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
                            }, self.verify_resp_tx.clone()).await.with_context(
                                || format!("fetcher: defer verifying piece for fetched block"))?;
                        }
                        Ok(block_fetcher::Response::FetchFailed { share_key, block, err }) => {
                            warn!("failed to fetch block: {:?}", err);
                            // Update peer stats with failure
                            if let Some(target) = self.peer_tracker.fetch_err(&share_key, err).await {
                                self.clients.share_resolver.call(share_resolver::Request::Header {
                                    response_tx: ResponseChannel::default(),
                                    key: share_key,
                                    prior_target: Some(target),
                                }).await.with_context(|| format!("fetcher: re-resolve share target"))?;
                            }
                            self.pending_fetch_tx.send_async(block).await.with_context(
                                || format!("fetcher: requeue failed block fetch"))?;
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
                                }).await.with_context(|| format!("fetcher: update have_map with verified piece"))?;

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
                                    }).await.with_context(|| format!("fetcher: re-fetch block for invalid piece"))?;
                            }
                        }
                        piece_verifier::Response::IncompletePiece { .. } => {}
                    }
                }
                res = self.clients.share_target_rx.recv_async() => {
                    let (key, target) = res.with_context(|| format!("fetcher: receive share target update"))?;
                    debug!("share target update for {key}: {target:?}");
                    if let None = self.peer_tracker.update(key, target.to_owned()).await {
                        // Never seen this peer before, advertise ourselves to it
                        self.node.request_advertise_peer(&target, &self.share.key).await.with_context(|| format!("fetch: advertising ourselves to new peer"))?;
                    }
                }

                // Resolve newly discovered peers
                res = self.clients.discovered_peers_rx.recv_async() => {
                    let (key, peer_info) = res.with_context(|| format!("fetcher: receive discovered peer"))?;
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
            }
        }
    }

    async fn next(&mut self, cancel: CancellationToken) -> Result<State> {
        let state = match self.state {
            State::Indexing => self
                .index(cancel.clone())
                .await
                .with_context(|| format!("indexing"))?,
            State::Planning => self
                .plan(cancel.clone())
                .await
                .with_context(|| format!("planning"))?,
            State::Fetching => self
                .fetch(cancel.clone())
                .await
                .with_context(|| format!("fetching"))?,
            State::Done => {
                cancel.cancelled().await;
                State::Done
            }
        };
        Ok(state)
    }
}

pub enum Request {
    State {
        response_tx: ResponseChannel<Response>,
    },
    ShareInfo {
        response_tx: ResponseChannel<Response>,
    },
}

impl Respondable for Request {
    type Response = Response;

    fn set_response(&mut self, ch: ResponseChannel<Self::Response>) {
        match self {
            Request::State { response_tx, .. } => *response_tx = ch,
            Request::ShareInfo { response_tx, .. } => *response_tx = ch,
        }
    }

    fn response_tx(self) -> ResponseChannel<Self::Response> {
        match self {
            Request::State { response_tx, .. } => response_tx,
            Request::ShareInfo { response_tx, .. } => response_tx,
        }
    }
}

pub enum Response {
    State { state: State },
    ShareInfo { share_info: ShareInfo },
}

impl<N: Node> Actor for Fetcher<N> {
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
                    return Ok(())
                }
                res = self.next(cancel.child_token()) => {
                    let state = res?;
                    self.state = state;
                }
                res = request_rx.recv_async() => {
                    let req = res.with_context(|| format!("receive request"))?;
                    self.handle_request(req).await?;
                }
            }
        }
    }

    async fn handle_request(&mut self, req: Self::Request) -> Result<()> {
        let (resp, mut response_tx) = match req {
            Request::State { response_tx } => (
                Response::State {
                    state: self.state.clone(),
                },
                response_tx,
            ),
            Request::ShareInfo { response_tx } => (
                Response::ShareInfo {
                    share_info: self.share.clone(),
                },
                response_tx,
            ),
        };
        response_tx
            .send(resp)
            .await
            .with_context(|| format!("fetcher: send response"))
    }

    async fn join(self) -> Result<()> {
        try_join!(
            self.clients.piece_verifier.join(),
            self.clients.block_fetcher.join(),
            self.clients.have_announcer.join(),
            self.clients.share_resolver.join(),
            self.clients.peer_resolver.join(),
        )
        .with_context(|| format!("fetcher: join operators"))?;
        Ok(())
    }
}
