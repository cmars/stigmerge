use std::{ops::Deref, path::PathBuf, sync::Arc};

use anyhow::{Error, Result};
use path_absolutize::Absolutize;
use stigmerge_fileindex::{Indexer, Progress};
use stigmerge_peer::{
    content_addressable::ContentAddressable, fetcher, peer_gossip::PeerGossip, piece_verifier,
    proto::Digest, seeder, share_announcer, share_resolver, types::LocalShareInfo, CancelError,
    Retry,
};
use tokio::{
    select,
    sync::{broadcast, watch, RwLock},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};
use veilid_core::TypedRecordKey;
use veilnet::Connection;

#[derive(Debug)]
pub enum Mode {
    Seed {
        /// Local file to seed.
        path: PathBuf,
    },
    Fetch {
        /// Local directory where the fetched file(s) will be placed.
        root: PathBuf,

        /// Content digest of the share index (not the payload itself).
        ///
        /// The index digest is used to authenticate other peers advertising
        /// that they offer the same payload. If their share DHT has an index
        /// that doesn't match, they're not.
        want_index_digest: Option<Digest>,

        /// Share key(s) used to bootstrap into the swarm of peers sharing this
        /// content. At least one is required.
        share_keys: Vec<TypedRecordKey>,
    },
}

impl Mode {
    pub fn share_keys(&self) -> &[TypedRecordKey] {
        match self {
            Mode::Seed { .. } => &[],
            Mode::Fetch { share_keys, .. } => share_keys,
        }
    }
}

#[derive(Clone, Debug)]
pub enum Event {
    /// Share info has been published to the Veilid DHT.
    ShareInfo(Box<LocalShareInfo>),

    /// Fetch status has changed.
    FetcherStatus(fetcher::Status),

    /// Seeder is indexing and verify the local copy of the share. For a large
    /// local share, this can take a little while, so progress is reported.
    SeederLoading {
        index_progress: stigmerge_fileindex::Progress,
        verify_progress: stigmerge_fileindex::Progress,
    },

    /// Seeder is available to service block requests. The share may still be
    /// incomplete. FetcherStatus(fetcher::Status::Done) indicates the share is
    /// complete.
    SeederAvailable,
}

pub struct Share<C: Connection> {
    conn: C,
    mode: Mode,
    retry: Retry,

    events_tx: broadcast::Sender<Event>,
    events_rx: broadcast::Receiver<Event>,

    pub(crate) tasks: JoinSet<Result<()>>,
}

const SHARE_EVENTS_CAPACITY: usize = 131072;

impl<C: Connection + Clone + Send + Sync + 'static> Share<C> {
    pub fn new(conn: C, mode: Mode) -> Result<Self> {
        let (events_tx, events_rx) = broadcast::channel(SHARE_EVENTS_CAPACITY);
        let share = Self {
            conn,
            mode,
            retry: Retry::default(),
            events_tx,
            events_rx,
            tasks: JoinSet::new(),
        };
        Ok(share)
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<Event> {
        self.events_rx.resubscribe()
    }

    pub async fn start(&mut self, cancel: CancellationToken) -> Result<()> {
        let root = match self.mode {
            Mode::Seed { ref path } => path
                .absolutize()?
                .parent()
                .ok_or(Error::msg("cannot determine parent directory"))?
                .to_path_buf(),
            Mode::Fetch { ref root, .. } => root.to_owned(),
        };

        // Set up share resolver
        let (share_resolver, resolver_task) = share_resolver::ShareResolver::new_task(
            cancel.clone(),
            self.retry.clone(),
            self.conn.clone(),
            &root,
        );
        self.tasks.spawn(async move {
            resolver_task.await??;
            Ok(())
        });

        // Resolve or create index based on mode
        let mut want_index = None;
        match &self.mode {
            Mode::Fetch {
                share_keys,
                want_index_digest,
                ..
            } => {
                for share_key in share_keys.iter() {
                    let want_index_digest: Option<[u8; 32]> = match want_index_digest {
                        Some(digest_string) => {
                            let digest = hex::decode(digest_string)?;
                            Some(
                                digest
                                    .try_into()
                                    .map_err(|_| Error::msg("Invalid digest length"))?,
                            )
                        }
                        None => None,
                    };

                    // Resolve the index from the bootstrap peer
                    let mut remote_share = share_resolver.add_share(share_key).await?;
                    let remote_index_digest = remote_share.index.digest()?;

                    // Verify the index matches what we want
                    if let Some(want_digest) = want_index_digest {
                        if remote_index_digest != want_digest {
                            anyhow::bail!(
                                "remote share does not match wanted index digest: expected {}, got {}",
                                hex::encode(&want_digest[..]),
                                hex::encode(&remote_index_digest[..])
                            );
                        }
                    }

                    want_index.get_or_insert(remote_share.index);
                }
            }
            Mode::Seed { path } => {
                let indexer = Indexer::from_file(path).await?;
                self.tasks.spawn(Self::send_indexer_progress(
                    cancel.clone(),
                    indexer.subscribe_index_progress(),
                    indexer.subscribe_digest_progress(),
                    self.events_tx.clone(),
                ));
                let index = indexer.index().await?;
                want_index.get_or_insert(index);
            }
        };
        let index = want_index.ok_or(Error::msg("failed to resolve index"))?;

        // For seeding, announce our own share
        let share = {
            let share_announcer = share_announcer::ShareAnnouncer::new(
                cancel.clone(),
                self.retry.clone(),
                self.conn.clone(),
                index.clone(),
            )
            .await?;
            let share_info = share_announcer.share_info().await;
            debug!(index_digest = hex::encode(share_info.want_index_digest));
            {
                let cancel = cancel.clone();
                self.tasks.spawn(async move {
                    let res = share_announcer.run().await;
                    cancel.cancel();
                    res
                });
            }

            self.events_tx
                .send(Event::ShareInfo(Box::new(share_info.clone())))?;
            share_info
        };

        // Set up peer gossip
        let peer_gossip =
            PeerGossip::new(self.conn.clone(), share.clone(), share_resolver.clone()).await?;
        {
            let retry = self.retry.clone();
            let cancel = cancel.clone();
            self.tasks.spawn(async move {
                peer_gossip.run(cancel, retry).await.map_err(|err| {
                    error!(?err, "peer gossip task");
                    err
                })
            });
        }

        // Set up piece verifier
        let shared_index = Arc::new(RwLock::new(index.clone()));
        let piece_verifier = piece_verifier::PieceVerifier::new(shared_index.clone()).await;

        // All peers are seeders
        let seeder =
            seeder::Seeder::new(self.conn.clone(), share.clone(), piece_verifier.clone()).await;
        self.tasks
            .spawn(seeder.run(cancel.clone(), self.retry.clone()));

        match &self.mode {
            Mode::Seed { .. } => {
                // Verify all pieces to mark them as available for seeding
                for (piece_index, piece) in index.payload().pieces().iter().enumerate() {
                    for block_index in 0..piece.block_count() {
                        let piece_state = stigmerge_peer::types::PieceState::new(
                            0,
                            piece_index,
                            0,
                            piece.block_count(),
                            block_index,
                        );
                        piece_verifier.update_piece(piece_state).await?;
                    }
                }

                self.events_tx.send(Event::SeederAvailable)?;
            }
            Mode::Fetch { .. } => {
                // Set up fetcher for fetching mode
                let fetcher = fetcher::Fetcher::new(
                    self.conn.clone(),
                    share.clone(),
                    piece_verifier,
                    share_resolver.clone(),
                )
                .await;

                // Add any remote shares we resolved
                for share_key in self.mode.share_keys() {
                    share_resolver.add_share(share_key).await?;
                }

                self.tasks.spawn(Self::send_fetch_progress(
                    cancel.clone(),
                    fetcher.subscribe_fetcher_status(),
                    self.events_tx.clone(),
                ));
                self.tasks
                    .spawn(fetcher.run(cancel.clone(), self.retry.clone()));
            }
        }

        Ok(())
    }

    pub async fn join(self) -> Result<()> {
        self.tasks
            .join_all()
            .await
            .into_iter()
            .collect::<Result<(), _>>()
    }

    async fn send_indexer_progress(
        cancel: CancellationToken,
        mut subscribe_index_progress: watch::Receiver<stigmerge_fileindex::Progress>,
        mut subscribe_digest_progress: watch::Receiver<stigmerge_fileindex::Progress>,
        events_tx: broadcast::Sender<Event>,
    ) -> Result<()> {
        let mut index_progress = Progress::default();
        let mut verify_progress = Progress::default();
        loop {
            select! {
                _ = cancel.cancelled() => {
                    return Err(CancelError.into());
                }
                res = subscribe_index_progress.changed() => {
                    res?;
                    let progress = subscribe_index_progress.borrow_and_update();
                    progress.clone_into(&mut index_progress);
                    events_tx.send(Event::SeederLoading{
                        index_progress,
                        verify_progress,
                    })?;
                }
                res = subscribe_digest_progress.changed() => {
                    res?;
                    let progress = subscribe_digest_progress.borrow_and_update();
                    progress.clone_into(&mut verify_progress);
                    events_tx.send(Event::SeederLoading{
                        index_progress,
                        verify_progress,
                    })?;
                    if progress.length == progress.position {
                        return Ok(());
                    }
                }
            }
        }
    }

    async fn send_fetch_progress(
        cancel: CancellationToken,
        mut subscribe_fetcher_status: watch::Receiver<fetcher::Status>,
        events_tx: broadcast::Sender<Event>,
    ) -> Result<()> {
        loop {
            select! {
                _ = cancel.cancelled() => {
                    return Err(CancelError.into());
                }
                res = subscribe_fetcher_status.changed() => {
                    res?;
                    let progress = subscribe_fetcher_status.borrow_and_update();
                    events_tx.send(Event::FetcherStatus(progress.clone()))?;
                    if let &fetcher::Status::Done = progress.deref() {
                        return Ok(());
                    }
                }
            }
        }
    }
}
