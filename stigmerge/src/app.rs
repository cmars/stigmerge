use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Error, Result};
use color_eyre::owo_colors::OwoColorize;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use stigmerge_fileindex::Indexer;
use tokio::{select, spawn, sync::RwLock};
use tokio_util::sync::CancellationToken;
use veilid_core::TypedKey;

use stigmerge_peer::{
    actor::{ConnectionState, Operator, UntilCancelled, WithVeilidConnection},
    block_fetcher::BlockFetcher,
    content_addressable::ContentAddressable,
    fetcher::{self, Clients as FetcherClients, Fetcher, Status as FetcherStatus},
    have_announcer::HaveAnnouncer,
    is_cancelled, new_routing_context,
    node::{Node, Veilid},
    piece_verifier::{self, PieceVerifier},
    proto::Digest,
    seeder::{self, Seeder},
    share_announcer::{self, ShareAnnouncer},
    share_resolver::{self, ShareResolver},
    types::{PieceState, ShareInfo},
    Error as StigmergeError,
};
use tracing::info;

use crate::{cli::Commands, initialize_stderr_logging, initialize_ui_logging, Cli};

pub struct App {
    cli: Cli,
    spinner_style: ProgressStyle,
    bar_style: ProgressStyle,
    bytes_style: ProgressStyle,
    msg_style: ProgressStyle,
}

impl App {
    pub fn new(cli: Cli) -> Result<App> {
        Ok(App {
            cli,
            spinner_style: ProgressStyle::with_template("{prefix:.bold.dim} {spinner} {wide_msg}")?
                .tick_chars("⣾⣷⣯⣟⡿⢿⣻⣽"),
            bar_style: ProgressStyle::with_template(
                "[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}",
            )?,
            bytes_style: ProgressStyle::with_template(
                "[{elapsed_precise}] {wide_bar:.cyan/blue} {bytes}/{total_bytes} {msg}",
            )?,
            msg_style: ProgressStyle::with_template("{prefix:.bold} {msg}")?,
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        let m = MultiProgress::new();
        m.println(format!("🦇 stigmerge {}", env!("CARGO_PKG_VERSION")))?;

        if self.cli.version() {
            return Ok(());
        }

        if self.cli.no_ui() {
            initialize_stderr_logging()
        } else {
            initialize_ui_logging(m.clone());
        }

        // Set up Veilid node
        let state_dir = self.cli.state_dir()?;
        let (routing_context, update_tx, _) = new_routing_context(&state_dir, None).await?;
        let node = Veilid::new(routing_context, update_tx).await?;

        // Set up cancellation token
        let cancel = CancellationToken::new();

        // Set up connection state
        let conn_state = Arc::new(tokio::sync::Mutex::new(ConnectionState::new()));

        // Set up connection status progress bar
        let conn_progress_bar = m.add(ProgressBar::new(0u64));
        conn_progress_bar.set_style(self.spinner_style.clone());
        conn_progress_bar.set_prefix("🟥");
        conn_progress_bar.set_message("Connecting to Veilid network");
        conn_progress_bar.enable_steady_tick(Duration::from_millis(100));

        // Monitor connection state
        //let conn_state_clone = conn_state.clone();
        //let conn_cancel = cancel.clone();
        //let conn_msg_style = self.msg_style.clone();
        // TODO: add a status sender to ConnectionState, subscribe and update spinner here
        //spawn(async move {
        //    loop {
        //        select! {
        //            _ = conn_cancel.cancelled() => {
        //                return Ok::<(), Error>(());
        //            }
        //            _ = tokio::time::sleep(Duration::from_millis(100)) => {
        //                let state = conn_state_clone.lock().await;
        //                // Check if connected - we'll just wait a bit and assume connection
        //                // since we can't directly access the private field
        //                if state.is_connected() {
        //                    conn_progress_bar.set_style(conn_msg_style.clone());
        //                    conn_progress_bar.set_prefix("🟢");
        //                    conn_progress_bar.disable_steady_tick();
        //                    conn_progress_bar.finish_with_message("Connected to Veilid network");
        //                    return Ok(());
        //                }
        //            }
        //        }
        //    }
        //});

        // Set up ctrl-c handler
        let ctrl_c_cancel = cancel.clone();
        let canceller = tokio::spawn(async move {
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

        // Run the appropriate command
        let result = match &self.cli.commands {
            Commands::Fetch {
                dht_key: share_key,
                root,
            } => {
                self.do_fetch(m, node.clone(), conn_state, cancel.clone(), share_key, root)
                    .await
            }
            Commands::Seed { file } => {
                self.do_seed(m, node.clone(), conn_state, cancel.clone(), file)
                    .await
            }
            _ => {
                cancel.cancel();
                Err(Error::msg("invalid command"))
            }
        };

        // Wait for canceller to complete
        let _ = canceller.await?;

        // Shutdown node
        let _ = node.shutdown().await;

        Ok(result?)
    }

    async fn do_fetch(
        &self,
        m: MultiProgress,
        node: Veilid,
        conn_state: Arc<tokio::sync::Mutex<ConnectionState>>,
        cancel: CancellationToken,
        share_key: &str,
        root: &str,
    ) -> Result<()> {
        // Set up share resolver
        let share_resolver = ShareResolver::new(node.clone());
        let target_rx = share_resolver.subscribe_target();
        let mut share_resolve_op = Operator::new(
            cancel.clone(),
            share_resolver,
            WithVeilidConnection::new(
                WithVeilidConnection::new(UntilCancelled, node.clone(), conn_state.clone()),
                node.clone(),
                conn_state.clone(),
            ),
        );

        // Parse share key
        let share_key: TypedKey = share_key.parse()?;

        // Resolve the share
        share_resolve_op
            .send(share_resolver::Request::Header {
                key: share_key.clone(),
                prior_target: None,
            })
            .await?;

        let (header, want_index_digest) = match share_resolve_op.recv().await {
            Some(share_resolver::Response::Header { header, .. }) => {
                let digest: Digest = header.payload_digest().into();
                (header, digest)
            }
            Some(share_resolver::Response::NotAvailable { err_msg, .. }) => {
                return Err(Error::msg(format!("Failed to resolve share: {}", err_msg)));
            }
            _ => return Err(Error::msg("Unexpected response from share resolver")),
        };

        // Resolve the index
        let want_index_digest: Digest = want_index_digest
            .try_into()
            .map_err(|_| Error::msg("Invalid digest length"))?;

        share_resolve_op
            .send(share_resolver::Request::Index {
                key: share_key.clone(),
                want_index_digest,
                root: PathBuf::from(root),
            })
            .await?;

        let want_index = match share_resolve_op.recv().await {
            Some(share_resolver::Response::Index { index, .. }) => index,
            Some(share_resolver::Response::BadIndex { .. }) => {
                return Err(Error::msg("Bad index"));
            }
            Some(share_resolver::Response::NotAvailable { err_msg, .. }) => {
                return Err(Error::msg(format!("Failed to resolve index: {}", err_msg)));
            }
            _ => return Err(Error::msg("Unexpected response from share resolver")),
        };

        // Create share info
        let share = ShareInfo {
            want_index: want_index.clone(),
            root: PathBuf::from(root),
            header: header.clone(),
        };

        // Set up block fetcher
        let block_fetcher = Operator::new_clone_pool(
            cancel.clone(),
            BlockFetcher::new(
                node.clone(),
                Arc::new(RwLock::new(want_index.clone())),
                PathBuf::from(root),
                target_rx,
            ),
            WithVeilidConnection::new(UntilCancelled, node.clone(), conn_state.clone()),
            50, // Number of concurrent fetchers
        );

        // Set up piece verifier
        let piece_verifier = PieceVerifier::new(Arc::new(RwLock::new(want_index.clone())));
        let verified_rx = piece_verifier.subscribe_verified();
        let piece_verifier_op = Operator::new(cancel.clone(), piece_verifier, UntilCancelled);

        // Set up have announcer
        let have_announcer = Operator::new(
            cancel.clone(),
            HaveAnnouncer::new(node.clone(), header.have_map().unwrap().key().clone()),
            WithVeilidConnection::new(UntilCancelled, node.clone(), conn_state.clone()),
        );

        // Set up fetcher clients
        let fetcher_clients = FetcherClients {
            block_fetcher,
            piece_verifier: piece_verifier_op,
            have_announcer,
        };

        // Create fetcher
        let fetcher = Fetcher::new(share.clone(), fetcher_clients);
        let mut status_rx = fetcher.subscribe_fetcher_status();

        // Set up progress bars
        let fetch_progress = m.add(
            ProgressBar::new(0u64)
                .with_style(self.spinner_style.clone())
                .with_prefix("📥"),
        );

        let file_path = if let Some(file) = want_index.files().first() {
            file.path().to_string_lossy().to_string()
        } else {
            "unknown".to_string()
        };

        fetch_progress.set_message(format!(
            "Fetching {} into {}",
            file_path.bold().bright_cyan(),
            PathBuf::from(root).canonicalize()?.to_string_lossy()
        ));

        let fetch_progress_bar = m.add(ProgressBar::new(0u64));
        fetch_progress_bar.set_style(self.bytes_style.clone());
        fetch_progress_bar.set_message("Fetching blocks");

        let verify_progress_bar = m.add(ProgressBar::new(0u64));
        verify_progress_bar.set_style(self.bar_style.clone());
        verify_progress_bar.set_message("Verifying blocks");

        // Monitor fetcher status
        let status_cancel = cancel.clone();
        let status_fetch_progress = fetch_progress.clone();
        let m_fetch = m.clone();
        spawn(async move {
            loop {
                select! {
                    _ = status_cancel.cancelled() => {
                        return Ok::<(), Error>(());
                    }
                    status = status_rx.recv() => {
                        match status {
                            Ok(FetcherStatus::IndexProgress { position, length }) => {
                                status_fetch_progress.update(|pb| {
                                    pb.set_len(length);
                                    pb.set_pos(position);
                                });
                                status_fetch_progress.set_message("Indexing");
                            }
                            Ok(FetcherStatus::DigestProgress { position, length }) => {
                                status_fetch_progress.update(|pb| {
                                    pb.set_len(length);
                                    pb.set_pos(position);
                                });
                                status_fetch_progress.set_message("Comparing");
                            }
                            Ok(FetcherStatus::FetchProgress { count, length }) => {
                                fetch_progress_bar.update(|pb| {
                                    pb.set_len(length);
                                    pb.set_pos(count as u64);
                                });
                                if count as u64 == length {
                                    fetch_progress_bar.finish_with_message("Fetch complete");
                                } else {
                                    fetch_progress_bar.set_message("Fetching");
                                }

                            }
                            Ok(FetcherStatus::VerifyProgress { position, length }) => {
                                verify_progress_bar.update(|pb| {
                                    pb.set_len(length);
                                    pb.set_pos(position);
                                });
                                if position == length {
                                    verify_progress_bar.finish_with_message("Verified");
                                }
                            }
                            Ok(FetcherStatus::Done) => {
                                status_fetch_progress.finish_with_message("✅ Fetch complete");
                                let _ = m_fetch.println("✅ Fetch complete");
                                return Ok(());
                            }
                            Err(e) => {
                                status_fetch_progress.finish_with_message("❌ Fetch failed");
                                let _ = m_fetch.println(format!("❌ Fetch failed: {}", e));
                                return Err(Error::msg(format!("Fetch failed: {}", e)));
                            }
                        }
                    }
                }
            }
        });

        // Run the fetcher
        let fetcher_task = spawn(fetcher.run(cancel.clone()));

        // Set up seeder to seed as we fetch
        let seeder_clients = seeder::Clients {
            update_rx: node.subscribe_veilid_update(),
            verified_rx,
        };

        let seeder = Seeder::new(node.clone(), share.clone(), seeder_clients);
        let _seeder_op = Operator::new(
            cancel.clone(),
            seeder,
            WithVeilidConnection::new(UntilCancelled, node.clone(), conn_state.clone()),
        );

        // Wait for fetcher to complete
        match fetcher_task.await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => {
                if is_cancelled(&e) {
                    fetch_progress.finish_with_message("❌ Fetch cancelled");
                    let _ = m.println("❌ Fetch cancelled");
                    Ok(())
                } else {
                    fetch_progress.finish_with_message("❌ Fetch failed");
                    let _ = m.println(format!("❌ Fetch failed: {}", e));
                    Err(Error::msg(format!("Fetch failed: {}", e)))
                }
            }
            Err(e) => {
                fetch_progress.finish_with_message("❌ Fetch failed");
                let _ = m.println(format!("❌ Fetch failed: {}", e));
                Err(Error::msg(format!("Fetch task failed: {}", e)))
            }
        }
    }

    async fn do_seed(
        &self,
        m: MultiProgress,
        node: Veilid,
        conn_state: Arc<tokio::sync::Mutex<ConnectionState>>,
        cancel: CancellationToken,
        file: &str,
    ) -> Result<()> {
        // Create indexer from file
        let file_path = PathBuf::from(file);
        let indexer = Indexer::from_file(file_path.clone()).await?;

        // Set up progress bars for indexing
        self.add_index_progress_bars(&indexer, &m, cancel.clone());

        // Get the index
        let mut index = indexer.index().await?;
        let index_digest = index.digest()?;

        // Get the root directory
        let root_dir = match file_path.parent() {
            Some(dir) => dir.to_path_buf(),
            None => PathBuf::from("."),
        };

        // Set up share announcer
        let mut share_announce_op = Operator::new(
            cancel.clone(),
            ShareAnnouncer::new(node.clone(), index.clone()),
            WithVeilidConnection::new(UntilCancelled, node.clone(), conn_state.clone()),
        );

        // Announce the share
        share_announce_op
            .send(share_announcer::Request::Announce)
            .await?;

        let (share_key, header) = match share_announce_op.recv().await {
            Some(share_announcer::Response::Announce { key, header, .. }) => (key, header),
            Some(share_announcer::Response::NotAvailable) => {
                return Err(Error::msg("Failed to announce share"));
            }
            _ => return Err(Error::msg("Unexpected response from share announcer")),
        };

        // Create share info
        let share = ShareInfo {
            want_index: index.clone(),
            root: root_dir.clone(),
            header: header.clone(),
        };

        // Set up piece verifier
        let shared_index = Arc::new(RwLock::new(index.clone()));
        let piece_verifier = PieceVerifier::new(shared_index.clone());
        let verified_rx = piece_verifier.subscribe_verified();
        let mut piece_verifier_op = Operator::new(cancel.clone(), piece_verifier, UntilCancelled);

        // Set up seeder clients
        let seeder_clients = seeder::Clients {
            update_rx: node.subscribe_veilid_update(),
            verified_rx,
        };

        // Create seeder
        let seeder = Seeder::new(node.clone(), share.clone(), seeder_clients);
        let seeder_op = Operator::new(
            cancel.clone(),
            seeder,
            WithVeilidConnection::new(UntilCancelled, node.clone(), conn_state.clone()),
        );

        // Set up progress bars
        let seed_progress = m.add(
            ProgressBar::new(0u64)
                .with_style(self.msg_style.clone())
                .with_prefix("🌱"),
        );

        seed_progress.set_message(format!(
            "Seeding {} to {}",
            file.bold().bright_cyan(),
            share_key.to_string().bold().bright_cyan()
        ));

        let info_progress = m.add(
            ProgressBar::new(0u64)
                .with_style(self.msg_style.clone())
                .with_prefix("🎁"),
        );

        info_progress.set_message(format!(
            "Anyone may download with {}",
            format!("stigmerge fetch {}", share_key)
                .bold()
                .bright_magenta()
        ));

        // Verify all pieces in the index
        let verify_progress_bar = m.add(ProgressBar::new(0u64));
        verify_progress_bar.set_style(self.bar_style.clone());
        verify_progress_bar.set_message("Verifying pieces");

        let total_pieces = index.payload().pieces().len();
        verify_progress_bar.set_length(total_pieces as u64);

        let mut verified_count = 0;
        for (piece_index, piece) in index.payload().pieces().iter().enumerate() {
            for block_index in 0..piece.block_count() {
                let req = piece_verifier::Request::Piece(PieceState::new(
                    0,
                    piece_index,
                    0,
                    piece.block_count(),
                    block_index,
                ));
                piece_verifier_op.send(req).await?;
                let resp = piece_verifier_op.recv().await;

                if let Some(piece_verifier::Response::ValidPiece { .. }) = resp {
                    if block_index == piece.block_count() - 1 {
                        verified_count += 1;
                        verify_progress_bar.set_position(verified_count);
                    }
                }
            }
        }

        verify_progress_bar.finish_with_message("All pieces verified");

        info!(
            "Seeding {} with digest {}",
            share_key.to_string(),
            hex::encode(index_digest)
        );

        // Wait for ctrl-c or other cancellation
        select! {
            _ = cancel.cancelled() => {
                seed_progress.finish_with_message("Seeding stopped");
                Ok(())
            }
            _ = tokio::signal::ctrl_c() => {
                seed_progress.finish_with_message("Seeding stopped");
                cancel.cancel();
                Ok(())
            }
        }
    }

    fn add_index_progress_bars(
        &self,
        indexer: &Indexer,
        m: &MultiProgress,
        cancel: CancellationToken,
    ) {
        let progress_cancel = cancel.clone();
        let mut index_progress_rx = indexer.subscribe_index_progress();
        let mut digest_progress_rx = indexer.subscribe_digest_progress();
        let index_progress_bar = m.add(ProgressBar::new(0u64));
        index_progress_bar.set_style(self.bytes_style.clone());
        index_progress_bar.set_message("Indexing share");
        let digest_progress_bar = m.add(ProgressBar::new(0u64));
        digest_progress_bar.set_style(self.bytes_style.clone());
        digest_progress_bar.set_message("Calculating content digest");
        let index_multi_bar = m.clone();
        spawn(async move {
            loop {
                select! {
                    _ = progress_cancel.cancelled() => {
                        return Ok::<(), Error>(())
                    }
                    index_result = index_progress_rx.changed() => {
                        index_result?;
                        let index_progress = index_progress_rx.borrow_and_update();
                        index_progress_bar.update(|pb| {
                            pb.set_len(index_progress.length);
                            pb.set_pos(index_progress.position);
                        });
                        if index_progress.position == index_progress.length {
                            index_progress_bar.finish_with_message("Indexed");
                            index_multi_bar.remove(&index_progress_bar);
                        }
                    }
                    digest_result = digest_progress_rx.changed() => {
                        digest_result?;
                        let digest_progress = digest_progress_rx.borrow_and_update();
                        digest_progress_bar.update(|pb| {
                            pb.set_len(digest_progress.length);
                            pb.set_pos(digest_progress.position);
                        });
                        if digest_progress.position == digest_progress.length {
                            digest_progress_bar.finish_with_message("Digest complete");
                            index_multi_bar.remove(&digest_progress_bar);
                        }
                    }
                }
            }
        });
    }
}
