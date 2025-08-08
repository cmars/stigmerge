use std::{ops::Deref, path::PathBuf, sync::Arc};

use anyhow::{Error, Result};
use path_absolutize::Absolutize;
use stigmerge_fileindex::{Indexer, Progress};
use stigmerge_peer::{
    actor::{
        ConnectionStateHandle, Operator, ResponseChannel, UntilCancelled, WithVeilidConnection,
    },
    block_fetcher,
    content_addressable::ContentAddressable,
    fetcher, have_announcer, peer_gossip, piece_verifier,
    proto::Digest,
    seeder, share_announcer, share_resolver,
    types::ShareInfo,
    CancelError, Node,
};
use tokio::{
    select,
    sync::{broadcast, watch, RwLock},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use veilid_core::TypedRecordKey;

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

pub struct Config {
    pub n_fetchers: u8,
}

impl Default for Config {
    fn default() -> Self {
        Config { n_fetchers: 0 }
    }
}

#[derive(Clone, Debug)]
pub enum Event {
    /// Share info has been published to the Veilid DHT.
    ShareInfo(ShareInfo),

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

pub struct Share<N: Node> {
    node: N,
    conn_state: ConnectionStateHandle,
    mode: Mode,
    config: Config,

    events_tx: broadcast::Sender<Event>,
    events_rx: broadcast::Receiver<Event>,

    tasks: JoinSet<Result<()>>,
}

impl<N: Node + Send + Sync + 'static> Share<N> {
    pub fn new(
        node: N,
        conn_state: ConnectionStateHandle,
        mode: Mode,
        config: Config,
    ) -> Result<Self> {
        let (events_tx, events_rx) = broadcast::channel(1024);
        let share = Self {
            node,
            conn_state,
            mode,
            config,
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
        let share_resolver_actor = share_resolver::ShareResolver::new(self.node.clone());
        let fetcher_share_target_rx = share_resolver_actor.subscribe_target();
        let seeder_share_target_rx = share_resolver_actor.subscribe_target();
        let mut share_resolver_op = Operator::new(
            cancel.clone(),
            share_resolver_actor,
            WithVeilidConnection::new(self.node.clone(), self.conn_state.clone()),
        );
        let share_resolver_tx = share_resolver_op.request_tx.clone();

        // Resolve bootstrap share keys and want_index_digest
        let mut want_index = None;
        match &self.mode {
            Mode::Fetch {
                share_keys,
                want_index_digest,
                ..
            } => {
                for share_key in share_keys.iter() {
                    let want_index_digest = match want_index_digest {
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
                    let index = match share_resolver_op
                        .call(share_resolver::Request::Index {
                            response_tx: ResponseChannel::default(),
                            key: share_key.clone(),
                            want_index_digest,
                            root: root.clone(),
                        })
                        .await?
                    {
                        share_resolver::Response::Index { index, .. } => index,
                        share_resolver::Response::BadIndex { .. } => {
                            anyhow::bail!("Bad index")
                        }
                        share_resolver::Response::NotAvailable { err_msg, .. } => {
                            anyhow::bail!(err_msg)
                        }
                        _ => anyhow::bail!("Unexpected response"),
                    };
                    want_index.get_or_insert(index);
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
        let mut index = want_index.ok_or(Error::msg("failed to resolve index"))?;
        let index_digest = index.digest()?;

        // Announce our own share of the index
        let mut share_announcer_op = Operator::new(
            cancel.clone(),
            share_announcer::ShareAnnouncer::new(self.node.clone(), index.clone()),
            WithVeilidConnection::new(self.node.clone(), self.conn_state.clone()),
        );
        let (share_key, share_header) = match share_announcer_op
            .call(share_announcer::Request::Announce {
                response_tx: ResponseChannel::default(),
            })
            .await?
        {
            share_announcer::Response::Announce { key, header, .. } => (key, header),
            share_announcer::Response::NotAvailable => {
                anyhow::bail!("failed to announce share")
            }
        };
        self.tasks.spawn(share_announcer_op.join());

        let share = ShareInfo {
            key: share_key,
            want_index: index.clone(),
            want_index_digest: index_digest,
            root,
            header: share_header.clone(),
        };
        self.events_tx.send(Event::ShareInfo(share.clone()))?;

        let piece_verifier_actor =
            piece_verifier::PieceVerifier::new(Arc::new(RwLock::new(index.clone()))).await;
        let verified_rx = piece_verifier_actor.subscribe_verified();
        let piece_verifier_op = Operator::new(cancel.clone(), piece_verifier_actor, UntilCancelled);

        // Announce our own have-map as we fetch, at our announced share's have-map key
        let have_announcer_op = Operator::new(
            cancel.clone(),
            // TODO: should use the share key publicly; hide this from the actor / op interface
            have_announcer::HaveAnnouncer::new(
                self.node.clone(),
                share_header.have_map().unwrap().key().clone(),
            ),
            WithVeilidConnection::new(self.node.clone(), self.conn_state.clone()),
        );

        // Set up fetcher dependencies
        let block_fetcher_op = Operator::new_clone_pool(
            cancel.clone(),
            block_fetcher::BlockFetcher::new(
                self.node.clone(),
                Arc::new(RwLock::new(index.clone())),
                index.root().to_path_buf(),
            ),
            WithVeilidConnection::new(self.node.clone(), self.conn_state.clone()),
            self.config.n_fetchers.into(),
        );

        let fetcher_clients = fetcher::Clients {
            block_fetcher: block_fetcher_op,
            piece_verifier: piece_verifier_op,
            have_announcer: have_announcer_op,
            share_resolver: share_resolver_op,
            share_target_rx: fetcher_share_target_rx,
        };

        // Create and run fetcher
        let fetcher_inst = fetcher::Fetcher::new(self.node.clone(), share.clone(), fetcher_clients);
        self.tasks.spawn(Self::send_fetch_progress(
            cancel.clone(),
            fetcher_inst.subscribe_fetcher_status(),
            self.events_tx.clone(),
        ));
        self.tasks
            .spawn(fetcher_inst.run(cancel.clone(), self.conn_state.clone()));

        let gossip_op = Operator::new(
            cancel.clone(),
            peer_gossip::PeerGossip::new(
                self.node.clone(),
                share.clone(),
                share_resolver_tx,
                seeder_share_target_rx,
            ),
            WithVeilidConnection::new(self.node.clone(), self.conn_state.clone()),
        );
        self.tasks.spawn(gossip_op.join());

        // Set up seeder
        let seeder = seeder::Seeder::new(self.node.clone(), share.clone(), verified_rx);
        let seeder_op = Operator::new(
            cancel.clone(),
            seeder,
            WithVeilidConnection::new(self.node.clone(), self.conn_state.clone()),
        );
        self.tasks.spawn(seeder_op.join());

        self.events_tx.send(Event::SeederAvailable)?;

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
