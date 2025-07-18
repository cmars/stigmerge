use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Error, Result};
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use stigmerge_peer::{
    actor::ConnectionState,
    fetcher::{self},
    new_routing_context,
    node::{Node, Veilid},
    CancelError,
};
use tokio::{fs::File, io::AsyncWriteExt, select, spawn, sync::broadcast, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};

use crate::{
    initialize_stdout_logging, initialize_ui_logging,
    share::{Event, Share},
    Cli,
};

pub struct App {
    cli: Cli,
    multi_progress: MultiProgress,
}

impl App {
    pub fn new(cli: Cli) -> Result<App> {
        let no_ui = cli.no_ui();
        Ok(App {
            cli,
            multi_progress: MultiProgress::with_draw_target(if no_ui {
                ProgressDrawTarget::hidden()
            } else {
                ProgressDrawTarget::stderr()
            }),
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        self.multi_progress
            .println(format!("🐝 stigmerge {}", env!("CARGO_PKG_VERSION")))?;

        if self.cli.version() {
            return Ok(());
        }

        if self.cli.no_ui() {
            initialize_stdout_logging()
        } else {
            initialize_ui_logging(self.multi_progress.clone());
        }

        // Set up Veilid node
        let state_dir = self.cli.state_dir()?;
        debug!(state_dir);
        let (routing_context, update_rx) = new_routing_context(&state_dir, None).await?;
        let node = Veilid::new(routing_context, update_rx).await?;

        // Set up cancellation token
        let cancel = CancellationToken::new();

        // Set up ctrl-c handler
        let ctrl_c_cancel = cancel.clone();
        let ctrl_c_node = node.clone();
        spawn(async move {
            select! {
                _ = ctrl_c_cancel.cancelled() => {}
                _ = tokio::signal::ctrl_c() => {
                    info!("Received ctrl-c, shutting down...");
                }
            }
            ctrl_c_cancel.cancel();
            ctrl_c_node.shutdown().await?;
            trace!("Ctrl-C handler completed");
            Ok::<(), Error>(())
        });

        let res = self.run_with_node(cancel, node.clone()).await;
        trace!("run_with_node completed");
        if let Err(e) = node.shutdown().await {
            warn!("{e}");
        }
        if let Err(e) = res {
            error!("{e}");
            return Err(e);
        }
        Ok(())
    }

    async fn run_with_node<T: Node + Sync + Send + 'static>(
        &self,
        cancel: CancellationToken,
        node: T,
    ) -> Result<()> {
        let mut tasks = JoinSet::new();

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
                        return Err(CancelError.into());
                    }
                    res = conn_state_rx.changed() => {
                        res?;
                        if *conn_state_rx.borrow_and_update() {
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

        let (share_mode, share_config) = self.cli.commands.share_args()?;
        let mut share = Share::new(node, conn_state, share_mode, share_config)?;

        self.add_state_file_handler(&cancel, &mut tasks, share.subscribe_events())?;
        self.add_fetch_progress(&cancel, &mut tasks, share.subscribe_events())?;
        self.add_seed_indexer_progress(&cancel, &mut tasks, share.subscribe_events())?;

        share.start(cancel.clone()).await?;
        tasks.spawn(share.join());
        tasks
            .join_all()
            .await
            .into_iter()
            .collect::<Result<(), _>>()
    }

    fn add_fetch_progress(
        &self,
        cancel: &CancellationToken,
        tasks: &mut JoinSet<Result<()>>,
        mut events: broadcast::Receiver<Event>,
    ) -> Result<()> {
        let fetch_progress = self.multi_progress.add(ProgressBar::new_spinner());
        fetch_progress.set_style(ProgressStyle::with_template(
            "{msg} {wide_bar} {binary_bytes}/{binary_total_bytes}",
        )?);
        let verify_progress = self.multi_progress.add(ProgressBar::new_spinner());
        verify_progress.set_style(ProgressStyle::with_template("{msg} {bar:40} {pos}/{len}")?);
        let progress_cancel = cancel.clone();
        tasks.spawn(async move {
            loop {
                select! {
                    _ = progress_cancel.cancelled() => {
                        return Err(CancelError.into());
                    }
                    res = events.recv() => {
                        let fetcher_status = match res? {
                            Event::FetcherStatus(status) => status,
                            _ => continue,
                        };
                        match fetcher_status {
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
                            fetcher::Status::FetchProgress { fetch_position, fetch_length, verify_position, verify_length } => {
                                fetch_progress.set_message("Fetching ");
                                fetch_progress.set_position(if fetch_position > 0 { fetch_position.try_into().unwrap() } else { 0 });
                                fetch_progress.set_length(fetch_length);
                                verify_progress.set_message("Verifying");
                                verify_progress.set_position(verify_position);
                                verify_progress.set_length(verify_length);
                            }
                            fetcher::Status::Done => {
                                fetch_progress.finish_with_message("Fetch complete");
                                verify_progress.finish_and_clear();
                                return Ok(());
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
        mut events: broadcast::Receiver<Event>,
    ) -> Result<()> {
        let indexer_progress = self.multi_progress.add(ProgressBar::new_spinner());
        indexer_progress.set_style(ProgressStyle::with_template(
            "{wide_bar} {binary_bytes}/{binary_total_bytes}",
        )?);
        let verifier_progress = self.multi_progress.add(ProgressBar::new_spinner());
        verifier_progress.set_style(ProgressStyle::with_template(
            "{wide_bar} {binary_bytes}/{binary_total_bytes}",
        )?);
        let progress_cancel = cancel.clone();
        tasks.spawn(async move {
            loop {
                select! {
                    _ = progress_cancel.cancelled() => {
                        return Err(CancelError.into());
                    }
                    res = events.recv() => {
                        let (index_progress, verify_progress) = match res? {
                            Event::SeederLoading{ index_progress, verify_progress } => {
                                (index_progress, verify_progress)
                            }
                            _ => continue,
                        };
                        if index_progress.length == index_progress.position {
                            indexer_progress.finish_and_clear();
                        } else {
                            indexer_progress.set_message("Indexing");
                            indexer_progress.set_length(index_progress.length);
                            indexer_progress.set_position(index_progress.position);
                        }
                        if verify_progress.length == verify_progress.position {
                            verifier_progress.finish_and_clear();
                            return Ok(())
                        }
                        verifier_progress.set_message("Verifying");
                        verifier_progress.set_length(verify_progress.length);
                        verifier_progress.set_position(verify_progress.position);
                    }
                }
            }
        });
        Ok(())
    }

    fn add_state_file_handler(
        &self,
        cancel: &CancellationToken,
        tasks: &mut JoinSet<std::result::Result<(), Error>>,
        mut events: broadcast::Receiver<Event>,
    ) -> Result<()> {
        let handler_cancel = cancel.clone();
        let state_dir = self.cli.state_dir()?;

        let seed_progress = self.multi_progress.add(ProgressBar::new_spinner());

        tasks.spawn(async move {
            loop {
                select! {
                    _ = handler_cancel.cancelled() => {
                        return Err(CancelError.into());
                    }
                    res = events.recv() => {
                        let share_info = match res? {
                            Event::ShareInfo(share_info) => share_info,
                            _ => continue,
                        };

                        // Write state files to state_dir, especially useful for
                        // providing share info to other processes.
                        Self::write_state_file(&state_dir, "index_digest",
                            hex::encode(&share_info.want_index_digest)).await?;
                        Self::write_state_file(&state_dir, "share_key",
                            share_info.key.to_string()).await?;

                        // Display the share info.
                        seed_progress.set_style(ProgressStyle::with_template("{prefix} {msg}")?);
                        seed_progress.set_prefix("🌱");
                        seed_progress.set_message(format!(
                            "Seeding {}{} to {}",
                            share_info.want_index
                                .files()
                                .first()
                                .map(|f| f.path().to_string_lossy())
                                .unwrap(),
                            if share_info.want_index.files().len() > 1 { "..." } else { "" },
                            share_info.key.to_string()
                        ));
                    }
                }
            }
        });
        Ok(())
    }

    async fn write_state_file(state_dir: &str, key: &str, value: String) -> Result<()> {
        let state_file = PathBuf::from(state_dir).join(key);
        File::create(state_file)
            .await?
            .write_all(value.as_bytes())
            .await?;
        Ok(())
    }
}
