use std::{sync::Arc, time::Duration};

use anyhow::{bail, Error, Result};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use path_absolutize::Absolutize;
use stigmerge_fileindex::Indexer;
use tokio::{
    select,
    sync::{watch, RwLock},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use veilid_core::TypedKey;

use stigmerge_peer::{
    actor::{ConnectionState, Operator, UntilCancelled, WithVeilidConnection},
    block_fetcher::BlockFetcher,
    content_addressable::ContentAddressable,
    fetcher::{self, Clients as FetcherClients, Fetcher},
    have_announcer::HaveAnnouncer,
    new_routing_context,
    node::{Node, Veilid},
    piece_verifier::PieceVerifier,
    seeder::{self, Seeder},
    share_announcer::{self, ShareAnnouncer},
    share_resolver::{self, ShareResolver},
    types::ShareInfo,
};
use tracing::{debug, error, info};

use crate::{cli::Commands, initialize_stderr_logging, initialize_ui_logging, Cli};

pub struct App {
    cli: Cli,
    multi_progress: MultiProgress,
}

impl App {
    pub fn new(cli: Cli) -> Result<App> {
        Ok(App {
            cli,
            multi_progress: MultiProgress::new(),
        })
    }

    #[tracing::instrument(skip_all)]
    pub async fn run(&mut self) -> Result<()> {
        self.multi_progress
            .println(format!("🐝 stigmerge {}", env!("CARGO_PKG_VERSION")))?;

        if self.cli.version() {
            return Ok(());
        }

        if self.cli.no_ui() {
            initialize_stderr_logging()
        } else {
            initialize_ui_logging(self.multi_progress.clone());
        }

        // Set up Veilid node
        let state_dir = self.cli.state_dir()?;
        let (routing_context, update_tx, _) = new_routing_context(&state_dir, None).await?;
        let node = Veilid::new(routing_context, update_tx).await?;

        let res = self.run_with_node(node.clone()).await;
        let _ = node.shutdown().await;
        if let Err(e) = res {
            error!(err = e.to_string());
            return Err(e);
        }
        Ok(())
    }

    #[tracing::instrument(skip_all, err)]
    async fn run_with_node<T: Node + Sync + Send + 'static>(&self, node: T) -> Result<()> {
        let mut tasks = JoinSet::new();

        // Set up cancellation token
        let cancel = CancellationToken::new();

        // Set up connection state
        let conn_state_inner = ConnectionState::new();
        let mut conn_state_rx = conn_state_inner.subscribe();
        let conn_state = Arc::new(tokio::sync::Mutex::new(conn_state_inner));

        // Set up connection status progress bar
        let conn_progress_bar = self.multi_progress.add(ProgressBar::new_spinner());
        conn_progress_bar.set_message("Connecting to Veilid network");
        conn_progress_bar.set_prefix("📶");
        conn_progress_bar.enable_steady_tick(Duration::from_millis(100));

        // Monitor connection state
        let conn_cancel = cancel.clone();
        tasks.spawn(async move {
            loop {
                select! {
                    _ = conn_cancel.cancelled() => {
                        return Ok::<(), Error>(());
                    }
                    res = conn_state_rx.changed() => {
                        res?;
                        if *conn_state_rx.borrow() {
                            conn_progress_bar.disable_steady_tick();
                            conn_progress_bar.set_style(ProgressStyle::with_template("{prefix} {msg}")?);
                            conn_progress_bar.set_message("Connected to Veilid network");
                        } else {
                            conn_progress_bar.enable_steady_tick(Duration::from_millis(100));
                            conn_progress_bar.set_style(ProgressStyle::with_template("{spinner} {msg}")?);
                            conn_progress_bar.set_message("Disconnected from Veilid network");
                        }
                    }
                }
            }
        });

        // Set up ctrl-c handler
        let ctrl_c_cancel = cancel.clone();
        tasks.spawn(async move {
            loop {
                tokio::select! {
                    _ = ctrl_c_cancel.cancelled() => {
                        return Ok::<(), Error>(())
                    }
                    _ = tokio::signal::ctrl_c() => {
                        info!("Received ctrl-c, shutting down...");
                        ctrl_c_cancel.cancel();
                        return Ok(())
                    }
                }
            }
        });

        let root = match self.cli.commands {
            Commands::Fetch {
                ref output_path, ..
            } => output_path.into(),
            Commands::Seed { ref path } => path
                .absolutize()?
                .parent()
                .ok_or(Error::msg("expected parent directory"))?
                .to_path_buf(),
            _ => bail!("unexpected subcommand"),
        };

        debug!("root: {}", root.to_string_lossy());

        // Set up share resolver
        let share_resolver = ShareResolver::new(node.clone());
        let target_rx = share_resolver.subscribe_target();
        let mut share_resolve_op = Operator::new(
            cancel.clone(),
            share_resolver,
            WithVeilidConnection::new(node.clone(), conn_state.clone()),
        );

        // Resolve bootstrap share keys and want_index_digest
        let mut want_index = None;
        match &self.cli.commands {
            Commands::Fetch {
                share_keys,
                index_digest,
                ..
            } => {
                for share_key_str in share_keys.iter() {
                    debug!("resolving share key: {share_key_str}");
                    let share_key: TypedKey = share_key_str.parse()?;
                    let want_index_digest = match index_digest {
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
                    share_resolve_op
                        .send(share_resolver::Request::Index {
                            key: share_key.clone(),
                            want_index_digest,
                            root: root.clone(),
                        })
                        .await?;
                    let index = match share_resolve_op.recv().await {
                        Some(share_resolver::Response::Index { index, .. }) => index,
                        Some(share_resolver::Response::BadIndex { .. }) => {
                            anyhow::bail!("Bad index")
                        }
                        Some(share_resolver::Response::NotAvailable { err_msg, .. }) => {
                            anyhow::bail!(err_msg)
                        }
                        _ => anyhow::bail!("Unexpected response"),
                    };
                    want_index.get_or_insert(index);
                }
            }
            Commands::Seed { path } => {
                let indexer = Indexer::from_file(path).await?;
                self.add_seed_indexer_progress(
                    &cancel,
                    &mut tasks,
                    indexer.subscribe_index_progress(),
                    indexer.subscribe_digest_progress(),
                )?;
                let mut index = indexer.index().await?;
                info!(index_digest = hex::encode(index.digest()?));
                want_index.get_or_insert(index);
            }
            c => bail!("unexpected subcommand: {:?}", c),
        };
        let index = want_index.ok_or(Error::msg("failed to resolve index"))?;
        debug!("resolved index");

        // Announce our own share of the index
        let mut share_announce_op = Operator::new(
            cancel.clone(),
            ShareAnnouncer::new(node.clone(), index.clone()),
            WithVeilidConnection::new(node.clone(), conn_state.clone()),
        );
        share_announce_op
            .send(share_announcer::Request::Announce)
            .await?;
        let (share_key, share_header) = match share_announce_op.recv().await {
            Some(share_announcer::Response::Announce { key, header, .. }) => (key, header),
            Some(share_announcer::Response::NotAvailable) => {
                anyhow::bail!("failed to announce share")
            }
            None => todo!(),
        };

        info!("announced share, key: {share_key}");

        let piece_verifier = PieceVerifier::new(Arc::new(RwLock::new(index.clone())));
        let verified_rx = piece_verifier.subscribe_verified();
        let piece_verifier_op = Operator::new(cancel.clone(), piece_verifier, UntilCancelled);

        // Announce our own have-map as we fetch, at our announced share's have-map key
        let have_announcer = Operator::new(
            cancel.clone(),
            // TODO: should use the share key publicly; hide this from the actor / op interface
            HaveAnnouncer::new(node.clone(), share_header.have_map().unwrap().key().clone()),
            WithVeilidConnection::new(node.clone(), conn_state.clone()),
        );

        let share = ShareInfo {
            want_index: index.clone(),
            root,
            header: share_header.clone(),
        };

        // Set up fetcher dependencies
        let block_fetcher = Operator::new_clone_pool(
            cancel.clone(),
            BlockFetcher::new(
                node.clone(),
                Arc::new(RwLock::new(index.clone())),
                index.root().to_path_buf(),
                target_rx,
            ),
            WithVeilidConnection::new(node.clone(), conn_state.clone()),
            self.cli.fetchers,
        );

        let fetcher_clients = FetcherClients {
            block_fetcher,
            piece_verifier: piece_verifier_op,
            have_announcer,
        };

        // Create and run fetcher
        info!("Starting fetch...");

        let fetcher = Fetcher::new(share.clone(), fetcher_clients);
        self.add_fetch_progress(&cancel, &mut tasks, fetcher.subscribe_fetcher_status())?;
        tasks.spawn(fetcher.run(cancel.clone()));

        // Set up seeder
        let seeder_clients = seeder::Clients {
            update_rx: node.subscribe_veilid_update(),
            verified_rx,
        };

        let seeder = Seeder::new(node.clone(), share.clone(), seeder_clients);
        let seeder_op = Operator::new(
            cancel.clone(),
            seeder,
            WithVeilidConnection::new(node.clone(), conn_state.clone()),
        );
        tasks.spawn(async move { seeder_op.join().await? });

        info!("Seeding until ctrl-c...");
        let seed_progress = self.multi_progress.add(ProgressBar::new_spinner());
        seed_progress.set_style(ProgressStyle::with_template("{prefix} {msg}")?);
        seed_progress.set_prefix("🌱");
        seed_progress.set_message(format!(
            "Seeding {}{} to {}",
            index
                .files()
                .first()
                .map(|f| f.path().to_string_lossy())
                .unwrap(),
            if index.files().len() > 1 { "..." } else { "" },
            share_key.to_string()
        ));

        // Keep seeding until ctrl-c
        select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Received ctrl-c, shutting down...");
                cancel.cancel();
            }
            _ = tasks.join_all() => {
                info!("tasks complete");
            }
        }

        Ok(())
    }

    fn add_fetch_progress(
        &self,
        cancel: &CancellationToken,
        tasks: &mut JoinSet<Result<()>>,
        mut subscribe_fetcher_status: watch::Receiver<fetcher::Status>,
    ) -> Result<()> {
        let fetch_progress = self.multi_progress.add(ProgressBar::new_spinner());
        fetch_progress.set_style(ProgressStyle::with_template(
            "{msg} {wide_bar} {binary_bytes}/{binary_total_bytes}",
        )?);
        let progress_cancel = cancel.clone();
        tasks.spawn(async move {
            loop {
                select! {
                    _ = progress_cancel.cancelled() => {
                        return Ok(())
                    }
                    res = subscribe_fetcher_status.changed() => {
                        res?;
                        match *subscribe_fetcher_status.borrow_and_update() {
                            fetcher::Status::IndexProgress { position, length } => {
                                fetch_progress.set_message("Indexing");
                                fetch_progress.set_position(position);
                                fetch_progress.set_length(length);
                            }
                            fetcher::Status::DigestProgress { position, length } => {
                                fetch_progress.set_message("Comparing");
                                fetch_progress.set_position(position);
                                fetch_progress.set_length(length);
                            }
                            fetcher::Status::FetchProgress { position, length } => {
                                fetch_progress.set_message("Fetching");
                                fetch_progress.set_position(position);
                                fetch_progress.set_length(length);
                            }
                            fetcher::Status::VerifyProgress { position, length } => {
                                fetch_progress.set_message("Comparing");
                                fetch_progress.set_position(position);
                                fetch_progress.set_length(length);
                            }
                            fetcher::Status::Done => {
                                fetch_progress.finish_with_message("Fetch complete");
                            }
                            _ => {}
                        }
                    }
                }
            }
        });
        Ok(())
    }

    fn add_seed_indexer_progress(
        &self,
        cancel: &CancellationToken,
        tasks: &mut JoinSet<Result<()>>,
        mut subscribe_index_progress: watch::Receiver<stigmerge_fileindex::Progress>,
        mut subscribe_digest_progress: watch::Receiver<stigmerge_fileindex::Progress>,
    ) -> Result<()> {
        let indexer_progress = self.multi_progress.add(ProgressBar::new_spinner());
        indexer_progress.set_style(ProgressStyle::with_template(
            "{wide_bar} {binary_bytes}/{binary_total_bytes}",
        )?);
        let verifier_progress = self.multi_progress.add(ProgressBar::new_spinner());
        verifier_progress.set_style(ProgressStyle::with_template(
            "{wide_bar} {binary_bytes}/{binary_total_bytes}",
        )?);
        let indexer_cancel = cancel.clone();
        tasks.spawn(async move {
            loop {
                select! {
                    _ = indexer_cancel.cancelled() => {
                        return Ok(())
                    }
                    res = subscribe_index_progress.changed() => {
                        res?;
                        let progress = subscribe_index_progress.borrow_and_update();
                        if progress.length == progress.position {
                            indexer_progress.finish_and_clear();
                            return Ok(());
                        }
                        indexer_progress.set_message("Indexing");
                        indexer_progress.set_length(progress.length);
                        indexer_progress.set_position(progress.position);
                    }
                }
            }
        });
        let verifier_cancel = cancel.clone();
        tasks.spawn(async move {
            loop {
                select! {
                    _ = verifier_cancel.cancelled() => {
                        return Ok(());
                    }
                    res = subscribe_digest_progress.changed() => {
                        res?;
                        let progress = subscribe_digest_progress.borrow_and_update();
                        if progress.length == progress.position {
                            verifier_progress.finish_and_clear();
                            return Ok(());
                        }
                        verifier_progress.set_message("Verifying");
                        verifier_progress.set_length(progress.length);
                        verifier_progress.set_position(progress.position);
                    }
                }
            }
        });
        Ok(())
    }
}
