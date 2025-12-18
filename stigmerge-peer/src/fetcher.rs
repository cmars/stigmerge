use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use backoff::{backoff::Backoff, ExponentialBackoff};
use stigmerge_fileindex::{Index, Indexer};
use tokio::select;
use tokio::sync::watch;
use tokio::task::{AbortHandle, JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument, trace, warn};
use veilid_core::RecordKey;
use veilnet::Connection;

use crate::block_fetcher::BlockFetcher;
use crate::content_addressable::ContentAddressable;
use crate::error::{CancelError, Unrecoverable};
use crate::piece_leases::{
    self, CompletionResult, FailureReason, LeaseManagerConfig, LeaseRequest, PieceLease, PieceLeaseManager, RejectedReason
};
use crate::piece_map::PieceMap;
use crate::piece_verifier::{PieceStatus, PieceStatusNotifier, PieceVerifier};
use crate::share_resolver::{ShareNotifier, ShareResolver};
use crate::types::{FileBlockFetch, LocalShareInfo, PieceState, RemoteShareInfo};
use crate::{piece_verifier, Error, Result, Retry};

pub struct Fetcher<C: Connection> {
    conn: C,
    share: LocalShareInfo,

    // State and notifications
    state: State,
    status_tx: watch::Sender<Status>,
    status_rx: watch::Receiver<Status>,

    piece_verifier: PieceVerifier,

    remote_share_rx: flume::Receiver<crate::types::RemoteShareInfo>,

    piece_verified_rx: flume::Receiver<piece_verifier::PieceStatus>,
    share_resolver: ShareResolver<C>,

    // Piece lease management
    piece_lease_manager: PieceLeaseManager,
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

        let (share_notifier, remote_share_rx) = ShareNotifier::new("fetcher");
        share_resolver.subscribe(Box::new(share_notifier)).await;

        let (piece_notifier, piece_verified_rx) = PieceStatusNotifier::new();
        piece_verifier.subscribe(Box::new(piece_notifier)).await;

        // Initialize piece lease manager with default config
        let lease_config = LeaseManagerConfig::default();
        let piece_lease_manager = PieceLeaseManager::new(lease_config);

        Fetcher {
            conn,
            share,

            state: State::Indexing,
            status_tx,
            status_rx,

            piece_verifier,

            remote_share_rx,

            piece_verified_rx,
            share_resolver,
            piece_lease_manager,
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

        // Initialize wanted pieces in lease manager
        let mut wanted_pieces = PieceMap::new();
        for want_block in &diff.want {
            wanted_pieces.set(TryInto::<u32>::try_into(want_block.piece_index).unwrap());
            want_length += want_block.block_length;
        }
        self.piece_lease_manager
            .set_wanted_pieces(wanted_pieces)
            .await;
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
                    if self.piece_verifier.is_complete().await && self.piece_lease_manager.wanted_pieces_count().await == 0 {
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
                        let remote_share_key = remote_share.key.clone();
                        let pool = FetchPool::new(FetchPoolParams {
                            conn: self.conn.clone(),
                            local_share: self.share.clone(),
                            remote_share: Arc::new(remote_share),
                            piece_verifier: self.piece_verifier.clone(),
                            piece_lease_manager: self.piece_lease_manager.clone(),
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
                        piece_verifier: self.piece_verifier.clone(),
                        piece_lease_manager: self.piece_lease_manager.clone(),
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
                        PieceStatus::ValidPiece{ index_complete, piece_index, .. } => {
                            let piece_length = self.share.want_index.payload().pieces()[piece_index].length();
                            if let Err(err) = self.piece_lease_manager.release_piece(
                                piece_index,
                                CompletionResult::Success,
                            ).await {
                                warn!(?err, ?piece_index, "failed to release valid piece");
                            }
                            self.status_tx.send_modify(|status| {
                                if let Status::FetchProgress { verify_position, fetch_position, .. } = status {
                                    *verify_position += 1;
                                    *fetch_position += TryInto::<i64>::try_into(piece_length).unwrap();
                                }
                            });
                            if index_complete {
                                task_cancel.cancel();
                                return Ok(State::Done);
                            }
                        }
                        PieceStatus::InvalidPiece{ piece_index, .. } => {
                            // Re-queue all the blocks in the failed piece
                            if let Err(err) = self.piece_lease_manager.release_piece(
                                piece_index,
                                CompletionResult::Failure(FailureReason::VerificationFailed),
                            ).await {
                                warn!(?err, ?piece_index, "failed to release invalid piece");
                            }
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

    piece_verifier: PieceVerifier,
    piece_lease_manager: PieceLeaseManager,
}

/// Parameters for creating a FetchPool
struct FetchPoolParams<C: Connection + Clone> {
    conn: C,
    local_share: LocalShareInfo,
    remote_share: Arc<RemoteShareInfo>,
    piece_verifier: PieceVerifier,
    piece_lease_manager: PieceLeaseManager,
}

impl<C: Connection + Clone + Send + Sync + 'static> FetchPool<C> {
    fn new(params: FetchPoolParams<C>) -> Self {
        Self {
            conn: params.conn,
            local_share: params.local_share,
            remote_share: params.remote_share,
            tasks: JoinSet::new(),
            piece_verifier: params.piece_verifier,
            piece_lease_manager: params.piece_lease_manager,
        }
    }

    const MAX_REMOTE_TASKS: u32 = 64;

    #[tracing::instrument(skip_all, err)]
    async fn run(mut self, cancel: CancellationToken) -> Result<()> {
        debug!(share_key = ?self.remote_share.key, "starting fetch pool");
        let mut backoff = ExponentialBackoff::default();
        backoff.initial_interval = Duration::from_millis(50);

        loop {
            select! {
                biased;
                _ = cancel.cancelled() => {
                    // Cancel all active lease tasks
                    self.tasks.abort_all();
                    return Err(CancelError.into());
                }
                res = self.tasks.join_next(), if !self.tasks.is_empty() => {
                    let res = if let Some(res) = res { res } else { continue };
                    match res {
                        Ok(Ok(())) => {
                            trace!(share_key = ?self.remote_share.key, "lease task completed successfully");
                        }
                        Ok(Err(e)) => {
                            trace!(share_key = ?self.remote_share.key, ?e, "lease task failed");
                        }
                        Err(e) => {
                            trace!(share_key = ?self.remote_share.key, ?e, "lease task panicked");
                        }
                    };
                }
                res = async {
                    trace!("requesting lease");
                    self.piece_lease_manager.request_lease(
                        &LeaseRequest {
                            remote_key: self.remote_share.key.clone(),
                            have_map: &self.remote_share.have_map,
                            max_leases: Self::MAX_REMOTE_TASKS,
                            requested_at: Instant::now(),
                        },
                    ).await
                } => {
                    trace!(is_ok = res.is_ok(), "response from lease manager");
                    match res {
                        Ok(resp) => {
                            debug!(share_key = ?self.remote_share.key, leases = resp.granted_leases.len(), "got leases");
                            for lease in resp.granted_leases {
                                self.fetch_lease(lease, cancel.clone()).await?;
                            }
                        }
                        Err(err) => {
                            match err {
                                piece_leases::Error::LeaseRejected(RejectedReason::PeerAtCapacity) => backoff.reset(),
                                _ => {}
                            };
                            warn!(?err);
                            let delay = backoff
                                .next_backoff()
                                .ok_or(Error::msg("max attempts reached"))?;
                            tokio::time::sleep(delay).await;
                            debug!(?delay, "resuming");
                        }
                    }
                }
            }
        }
    }

    /// Fetch a leased piece
    async fn fetch_lease(
        &mut self,
        lease: PieceLease,
        cancel: CancellationToken,
    ) -> Result<()> {
        let task_cancel = cancel.child_token();

        // Generate blocks for the leased piece
        let file_index = self
            .local_share
            .want_index
            .file_index_for_piece(lease.piece_index());
        let piece_info = &self.local_share.want_index.payload().pieces()[lease.piece_index()];

        let (blocks_tx, blocks_rx) = flume::bounded(piece_info.block_count());

        for block_index in 0..piece_info.block_count() {
            blocks_tx.send(FileBlockFetch {
                file_index,
                piece_index: lease.piece_index(),
                piece_offset: 0,
                block_index,
            })?;
        }

        // Five concurrent block fetchers
        let mut tasks = JoinSet::new();
        for _ in 0..5 {
            let blocks_rx = blocks_rx.clone();
            let cancel = task_cancel.clone();
            let conn = self.conn.clone();
            let local_root = self.local_share.root.clone();
            let remote_share = self.remote_share.clone();
            let piece_verifier = self.piece_verifier.clone();
            let handle = tasks.spawn(async move {
                let mut block_fetcher = BlockFetcher::new(conn, local_root);
                loop {
                    select! {
                        _ = cancel.cancelled() => {
                            return Err(CancelError.into());
                        }
                        res = blocks_rx.recv_async() => {
                            let block_fetch = res?;
                            match block_fetcher.fetch_block(remote_share.clone(), &block_fetch, true).await {
                                Ok((piece_state, _)) => {
                                    piece_verifier.update_piece(piece_state).await?;
                                }
                                Err(err) => {
                                    return Err(err);
                                }
                            }
                            if blocks_rx.is_empty() {
                                return Ok(());
                            }
                        }
                    }
                }

            });
            self.piece_lease_manager.register_tasks(lease.piece_index(), &[handle]).await?;
        }

        let res = tasks
            .join_all()
            .await
            .into_iter()
            .filter(|res| res.is_err())
            .next();
        res.unwrap_or(Ok(()))
    }
}
