use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use stigmerge_fileindex::{Index, Indexer, BLOCK_SIZE_BYTES};
use tokio::select;
use tokio::sync::watch;
use tokio::task::{AbortHandle, JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{info, instrument, trace, warn};
use veilid_core::{RecordKey, Timestamp};
use veilnet::Connection;

use crate::block_fetcher::BlockFetcher;
use crate::content_addressable::ContentAddressable;
use crate::error::{CancelError, Unrecoverable};
use crate::piece_verifier::{PieceStatus, PieceStatusNotifier, PieceVerifier};
use crate::share_resolver::{ShareNotifier, ShareResolver};
use crate::types::{FileBlockFetch, LocalShareInfo, PieceState, RemoteShareInfo};
use crate::{piece_verifier, Result, Retry};

pub struct Fetcher<C: Connection> {
    conn: C,
    share: LocalShareInfo,

    // State and notifications
    state: State,
    status_tx: watch::Sender<Status>,
    status_rx: watch::Receiver<Status>,

    piece_verifier: PieceVerifier,

    // Blocks to be fetched
    pending_blocks_tx: flume::Sender<FileBlockFetch>,
    pending_blocks_rx: flume::Receiver<FileBlockFetch>,

    remote_share_rx: flume::Receiver<crate::types::RemoteShareInfo>,

    piece_verified_rx: flume::Receiver<piece_verifier::PieceStatus>,
    share_resolver: ShareResolver<C>,
}

#[derive(Debug)]
pub enum State {
    Indexing,
    Planning { have_index: Index },
    Fetching,
    Done,
}

impl Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            State::Indexing => write!(f, "Indexing"),
            State::Planning { have_index } => match have_index.files().len() {
                0 => write!(f, "Planning"),
                n => write!(
                    f,
                    "Planning {}{}",
                    have_index.files()[0].path().to_str().unwrap_or(""),
                    if n > 1 { "..." } else { "" },
                ),
            },
            State::Fetching => write!(f, "Fetching"),
            State::Done => write!(f, "Done"),
        }
    }
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

impl<C: Connection + Clone + Send + Sync + 'static> Fetcher<C> {
    pub async fn new(
        conn: C,
        share: LocalShareInfo,
        piece_verifier: PieceVerifier,
        share_resolver: ShareResolver<C>,
    ) -> Self {
        let (status_tx, status_rx) = watch::channel(Status::NotStarted);
        let (pending_blocks_tx, pending_blocks_rx) = flume::unbounded();

        let (share_notifier, remote_share_rx) = ShareNotifier::new("fetcher");
        share_resolver.subscribe(Box::new(share_notifier)).await;

        let (piece_notifier, piece_verified_rx) = PieceStatusNotifier::new();
        piece_verifier.subscribe(Box::new(piece_notifier)).await;
        Fetcher {
            conn,
            share,

            state: State::Indexing,
            status_tx,
            status_rx,

            piece_verifier,

            pending_blocks_tx,
            pending_blocks_rx,

            remote_share_rx,

            piece_verified_rx,
            share_resolver,
        }
    }

    pub fn subscribe_fetcher_status(&self) -> watch::Receiver<Status> {
        self.status_rx.clone()
    }

    #[instrument(skip_all)]
    pub async fn run(mut self, cancel: CancellationToken, retry: Retry) -> Result<()> {
        loop {
            info!(state = %self.state);
            if matches!(self.state, State::Done) {
                return Ok(());
            }
            backoff_retry!(
                cancel,
                retry,
                {
                    let cancel_iter = cancel.clone();
                    self.state = match self.state {
                        // TODO: map permanent errors
                        State::Indexing => self
                            .index(cancel_iter)
                            .await
                            .map_err(backoff::Error::Permanent)?,
                        State::Planning { ref have_index } => self
                            .plan(cancel_iter, have_index.clone())
                            .await
                            .map_err(backoff::Error::Permanent)?,
                        State::Fetching => self.fetch(cancel_iter).await?,
                        State::Done => {
                            info!("done");
                            cancel.cancelled().await;
                            return Ok(());
                        }
                    };
                },
                {
                    self.conn.reset().await?;
                }
            )?;
        }
    }

    #[instrument(skip_all, err)]
    async fn index(&mut self, cancel: CancellationToken) -> Result<State> {
        let indexer = Indexer::from_wanted(&self.share.want_index)
            .await
            .context(Unrecoverable::new("indexer"))?;

        // Set status updates while indexing
        let mut index_progress = indexer.subscribe_index_progress();
        let mut digest_progress = indexer.subscribe_digest_progress();
        let status_tx = self.status_tx.clone();
        let status_cancel = cancel.child_token();
        let status_task_cancel = status_cancel.clone();
        let status_task: JoinHandle<Result<()>> = tokio::spawn(async move {
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
            Ok(Err(e)) => warn!("status progress: {e}"),
            Err(e) => warn!("status progress task: {e}"),
        };

        // Ready for planning
        Ok(State::Planning { have_index })
    }

    #[instrument(skip_all, err, fields(
        root = ?have_index.root(),
        file_names = ?have_index.files().iter().map(|f| f.path()).collect::<Vec<&std::path::Path>>(),
    ))]
    async fn plan(&mut self, _cancel: CancellationToken, have_index: Index) -> Result<State> {
        let diff = self.share.want_index.diff(&have_index);
        let mut want_length = 0;
        let total_length = self.share.want_index.payload().length();
        for want_block in diff.want {
            self.pending_blocks_tx.send(FileBlockFetch {
                file_index: want_block.file_index,
                piece_index: want_block.piece_index,
                piece_offset: want_block.piece_offset,
                block_index: want_block.block_index,
            })?;
            want_length += want_block.block_length;
        }
        let mut have_length = 0;
        for have_block in diff.have {
            self.piece_verifier
                .update_piece(PieceState::new(
                    have_block.file_index,
                    have_block.piece_index,
                    have_block.piece_offset,
                    self.share.want_index.payload().pieces()[have_block.piece_index].block_count(),
                    have_block.block_index,
                ))
                .await?;
            have_length += have_block.block_length;
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

    #[instrument(skip_all, err)]
    async fn fetch(&mut self, cancel: CancellationToken) -> Result<State> {
        let mut tasks = JoinSet::new();
        let mut task_shares = HashMap::new();
        let mut share_tasks: HashMap<RecordKey, AbortHandle> = HashMap::new();
        let task_cancel = cancel.child_token();
        loop {
            select! {
                            _ = cancel.cancelled() => {
                                for handle in share_tasks.values() {
                                    handle.abort();
                                }
                                tasks
                                    .join_all()
                                    .await
                                    .into_iter()
                                    .collect::<Result<()>>()?;
                                if self.piece_verifier.is_complete().await && self.pending_blocks_rx.is_empty() {
                                    return Ok(State::Done);
                                }
                                return Err(CancelError.into());
                            }
                            res = self.remote_share_rx.recv_async() => {
                                // TODO: This could be improved to self-moderate, limiting
                                // the number of remote shares being fetched from, evicting
                                // known unproductive fetchpools to try new ones. As it
                                // stands now, we'd promiscuously fetch from every remote
                                // peer we discover up to the max, which for a popular
                                // share, could exhaust compute & memory.
                                let mut remote_share = res?;

                                // Shouldn't happen, but ignore self-resolves.
                                if self.share.key == remote_share.key {
                                    trace!("ignoring self-resolved share");
                                    continue;
                                }

                                // Ignore remote shares that have the wrong index.
                                let remote_index_digest = remote_share.index.digest()?;
                                if remote_index_digest != self.share.want_index_digest {
                                    warn!(
                                        key = ?remote_share.key,
                                        remote_index_digest = hex::encode(remote_index_digest),
                                        want_index_digest = hex::encode(self.share.want_index_digest),
                                        "remote share does not match wanted index",
                                    );
                                    continue;
                                }
                                if !share_tasks.contains_key(&remote_share.key) {
                                    let remote_share_key = remote_share.key;
                                    let pool = FetchPool::new(FetchPoolParams {
                                        conn: self.conn.clone(),
                                        local_share: self.share.clone(),
                                        remote_share: Arc::new(remote_share),
                                        block_channels: BlockChannels {
                                            pending_blocks_tx: self.pending_blocks_tx.clone(),
                                            pending_blocks_rx: self.pending_blocks_rx.clone(),
                                        },
                                        status_tx: self.status_tx.clone(),
                                        piece_verifier: self.piece_verifier.clone(),
                                        share_resolver: self.share_resolver.clone(),
                                    });
                                    {
                                        let task_cancel = task_cancel.child_token();
                                        let handle = tasks.spawn(pool.run(task_cancel));
                                        task_shares.insert(handle.id(), remote_share_key.clone());
                                        share_tasks.insert(remote_share_key, handle);
                                    }
                                }
                            }
                            res = tasks.join_next_with_id() => {
                                let (id, res) = match res {
                                    Some(res) => res?,
                                    None => continue,
                                };
                                trace!(?res, ?id, "pool exited");
                                let remote_share_key = match task_shares.get(&id) {
                                    Some(key) => key.clone(),
                                    None => {
                                        trace!(?id, "no tasks scheduled");
                                        continue;
                                    }
                                };
                                share_tasks.remove(&remote_share_key);

                                let remote_share = self.share_resolver.add_share(&remote_share_key).await?;
                                let pool = FetchPool::new(FetchPoolParams {
                                    conn: self.conn.clone(),
                                    local_share: self.share.clone(),
                                    remote_share: Arc::new(remote_share),
                                    block_channels: BlockChannels {
                                        pending_blocks_tx: self.pending_blocks_tx.clone(),
                                        pending_blocks_rx: self.pending_blocks_rx.clone(),
                                    },
                                    status_tx: self.status_tx.clone(),
                                    piece_verifier: self.piece_verifier.clone(),
                                    share_resolver: self.share_resolver.clone(),
                                });
                                {
                                    let task_cancel = task_cancel.child_token();
                                    let handle = tasks.spawn(pool.run(task_cancel));
                                    task_shares.insert(handle.id(), remote_share_key.clone());
                                    share_tasks.insert(remote_share_key, handle);
                                }
                            }
                            res = self.piece_verified_rx.recv_async() => {
                                let piece_status = res?;
                                match piece_status {
                                    PieceStatus::ValidPiece{ index_complete, .. } => {
                                        self.status_tx.send_modify(|status| {
                                            if let Status::FetchProgress { verify_position, .. } = status {
                                                *verify_position += 1;
                                            }
                                        });
                                        if index_complete {
                                            task_cancel.cancel();
                                            return Ok(State::Done);
                                        }
                                    }
                                    PieceStatus::InvalidPiece{ file_index, piece_index } => {
                                        // Re-queue all the blocks in the failed piece
                                        let piece_length = self.share.want_index.payload().pieces()[piece_index].length();
                                        let piece_blocks = piece_length / BLOCK_SIZE_BYTES + if !piece_length.is_multiple_of(BLOCK_SIZE_BYTES) { 1 } else { 0 };
                                        for block_index in 0..piece_blocks  {
                                            let block = FileBlockFetch {
                                                file_index,
                                                piece_index,
                                                piece_offset: 0,
                                                block_index
                                            };
                                            self.pending_blocks_tx.send_async(block).await?;
                                            // TODO: punish peer for excessive bad blocks?
                                        }
                                        self.status_tx.send_modify(|status| {
                                            if let Status::FetchProgress { fetch_position, .. } = status {
                                                let piece_length = TryInto::<i64>::try_into(piece_length).unwrap();
                                                if *fetch_position > piece_length {
                                                    *fetch_position -= piece_length;
                                                } else {
                                                    *fetch_position = 0;
                                                }
                                            }
                                        });

                                    }
                                    PieceStatus::IncompletePiece { .. } => {}
                                }
                            }
                        }
        }
    }
}

struct FetchPool<C: Connection + Clone> {
    conn: C,
    local_share: LocalShareInfo,
    remote_share: Arc<RemoteShareInfo>,
    tasks: JoinSet<Result<()>>,

    pending_blocks_tx: flume::Sender<FileBlockFetch>,
    pending_blocks_rx: flume::Receiver<FileBlockFetch>,
    status_tx: watch::Sender<Status>,
    piece_verifier: PieceVerifier,
    share_resolver: ShareResolver<C>,
}

/// Parameters for creating a FetchPool
struct FetchPoolParams<C: Connection + Clone> {
    conn: C,
    local_share: LocalShareInfo,
    remote_share: Arc<RemoteShareInfo>,
    block_channels: BlockChannels,
    status_tx: watch::Sender<Status>,
    piece_verifier: PieceVerifier,
    share_resolver: ShareResolver<C>,
}

/// Channel pair for block communication
struct BlockChannels {
    pending_blocks_tx: flume::Sender<FileBlockFetch>,
    pending_blocks_rx: flume::Receiver<FileBlockFetch>,
}

impl<C: Connection + Clone + Send + Sync + 'static> FetchPool<C> {
    fn new(params: FetchPoolParams<C>) -> Self {
        Self {
            conn: params.conn,
            local_share: params.local_share,
            remote_share: params.remote_share,
            tasks: JoinSet::new(),
            pending_blocks_tx: params.block_channels.pending_blocks_tx,
            pending_blocks_rx: params.block_channels.pending_blocks_rx,
            status_tx: params.status_tx,
            piece_verifier: params.piece_verifier,
            share_resolver: params.share_resolver,
        }
    }

    const MAX_REMOTE_TASKS: usize = 64;

    async fn run(mut self, cancel: CancellationToken) -> Result<()> {
        let (fetch_success_tx, fetch_success_rx) = flume::unbounded();
        let mut consecutive_good_fetches = 0;

        loop {
            if self.tasks.is_empty() {
                let task = FetchTask::new(FetchTaskParams {
                    conn: self.conn.clone(),
                    local_root: self.local_share.root.to_path_buf(),
                    remote_share: self.remote_share.clone(),
                    block_channels: BlockChannels {
                        pending_blocks_tx: self.pending_blocks_tx.clone(),
                        pending_blocks_rx: self.pending_blocks_rx.clone(),
                    },
                    status_tx: self.status_tx.clone(),
                    fetch_success_tx: fetch_success_tx.clone(),
                    piece_verifier: self.piece_verifier.clone(),
                });
                trace!(share_key = ?self.remote_share.key, "spawning fetch task");
                self.tasks.spawn(task.run(cancel.child_token()));
            }
            select! {
                _ = cancel.cancelled() => {
                    return self.tasks
                        .join_all()
                        .await
                        .into_iter()
                        .collect::<Result<()>>();
                }
                res = self.tasks.join_next() => {
                    match res {
                        Some(Ok(Ok(()))) => {
                            if self.pending_blocks_rx.is_empty() {
                                return Ok(());
                            }
                        }
                        Some(res) => {
                            trace!(share_key = ?self.remote_share.key, ?res, "task exited");
                        }
                        None => {
                            trace!(share_key = ?self.remote_share.key, "no tasks scheduled");
                        }
                    }
                    consecutive_good_fetches = 0;
                }
                res = fetch_success_rx.recv_async(), if self.tasks.len() < Self::MAX_REMOTE_TASKS => {
                    if res? {
                        consecutive_good_fetches += 1;
                    } else {
                        consecutive_good_fetches = 0;
                        continue;
                    }
                    if consecutive_good_fetches >= self.tasks.len() * 2 {
                        for _ in 0..self.tasks.len() {
                            let task = FetchTask::new(FetchTaskParams {
                                conn: self.conn.clone(),
                                local_root: self.local_share.root.to_path_buf(),
                                remote_share: self.remote_share.clone(),
                                block_channels: BlockChannels {
                                    pending_blocks_tx: self.pending_blocks_tx.clone(),
                                    pending_blocks_rx: self.pending_blocks_rx.clone(),
                                },
                                status_tx: self.status_tx.clone(),
                                fetch_success_tx: fetch_success_tx.clone(),
                                piece_verifier: self.piece_verifier.clone(),
                            });
                            trace!(share_key = ?self.remote_share.key, "spawning fetch task");
                            self.tasks.spawn(task.run(cancel.child_token()));
                        }
                    }
                }
            }
            if consecutive_good_fetches == 0 && self.tasks.is_empty() {
                if let Err(err) = self
                    .share_resolver
                    .refresh_share(&self.remote_share.route_id)
                    .await
                {
                    warn!(
                        ?err,
                        key = ?self.remote_share.key,
                        route_id = ?self.remote_share.route_id,
                        "refreshing share",
                    );
                }

                // Cooldown so we don't thrash while network is down?
                // TODO: exp backoff until we get a good fetch?
                trace!(key = ?self.remote_share.key, "all fetch tasks have exited");
                return Ok(());
            }
        }
    }
}

struct FetchTask<C: Connection> {
    conn: C,
    local_root: PathBuf,
    remote_share: Arc<RemoteShareInfo>,
    pending_blocks_tx: flume::Sender<FileBlockFetch>,
    pending_blocks_rx: flume::Receiver<FileBlockFetch>,
    status_tx: watch::Sender<Status>,
    fetch_success_tx: flume::Sender<bool>,
    piece_verifier: PieceVerifier,
}

/// Parameters for creating a FetchTask
struct FetchTaskParams<C: Connection> {
    conn: C,
    local_root: PathBuf,
    remote_share: Arc<RemoteShareInfo>,
    block_channels: BlockChannels,
    status_tx: watch::Sender<Status>,
    fetch_success_tx: flume::Sender<bool>,
    piece_verifier: PieceVerifier,
}

impl<C: Connection + Clone + Send + Sync> FetchTask<C> {
    fn new(params: FetchTaskParams<C>) -> Self {
        Self {
            conn: params.conn,
            local_root: params.local_root,
            remote_share: params.remote_share,
            pending_blocks_tx: params.block_channels.pending_blocks_tx,
            pending_blocks_rx: params.block_channels.pending_blocks_rx,
            status_tx: params.status_tx,
            fetch_success_tx: params.fetch_success_tx,
            piece_verifier: params.piece_verifier,
        }
    }

    async fn run(self, cancel: CancellationToken) -> Result<()> {
        let mut block_fetcher = BlockFetcher::new(self.conn, self.local_root.clone());
        let mut errors = 0;
        loop {
            select! {
                _ = cancel.cancelled() => {
                    return Err(CancelError.into());
                }
                res = self.pending_blocks_rx.recv_async() => {
                    let block_fetch = res?;
                    if !self.remote_share.have_map.get(TryInto::<u32>::try_into(block_fetch.piece_index).unwrap()) {
                        // We don't have this piece, requeue it.
                        // Nack the fetch, for pool task accounting. Remote lacks these blocks.
                        self.fetch_success_tx.send_async(false).await?;
                        // Re-queue the block fetch.
                        self.pending_blocks_tx.send_async(block_fetch).await?;
                        return Ok(());
                    } else {
                        match block_fetcher.fetch_block(&self.remote_share.index, &self.remote_share.route_id, &block_fetch, true).await {
                            Ok((piece_state, fetch_len)) => {
                                self.status_tx.send_modify(|status| {
                                    if let Status::FetchProgress { fetch_position, .. } = status {
                                        *fetch_position += TryInto::<i64>::try_into(fetch_len).unwrap();
                                    }
                                });
                                self.piece_verifier.update_piece(piece_state).await?;
                                self.fetch_success_tx.send_async(true).await?;
                                errors = 0;
                            }
                            Err(err) => {
                                trace!(?err, "fetch block");
                                // Nack the fetch, for pool task accounting.
                                self.fetch_success_tx.send_async(false).await?;
                                // Re-queue the block fetch.
                                self.pending_blocks_tx.send_async(block_fetch).await?;

                                errors += 1;
                                if errors > 10 {
                                    return Err(err);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
