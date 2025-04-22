use std::{path::PathBuf, time::Duration};

use anyhow::{Error, Result};
use backoff::{backoff::Backoff, ExponentialBackoff};
use color_eyre::owo_colors::OwoColorize;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use stigmerge_fileindex::Indexer;
use tokio::{select, spawn, time::sleep};
use tokio_util::sync::CancellationToken;

use stigmerge_peer::{new_routing_context, Fetcher, Observable, Node, PeerState, Seeder, Veilid};
use tracing::error;

use crate::{cli::Commands, initialize_stderr_logging, initialize_ui_logging, Cli};

pub struct App {
    cli: Cli,
    spinner_style: ProgressStyle,
    bar_style: ProgressStyle,
    bytes_style: ProgressStyle,
    msg_style: ProgressStyle,
}

type AppPeer = Observable<Veilid>;

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
        let _ = m.println(format!("🦇 stigmerge {}", env!("CARGO_PKG_VERSION")));

        if self.cli.version() {
            return Ok(());
        }

        if self.cli.no_ui() {
            initialize_stderr_logging()
        } else {
            initialize_ui_logging(m.clone());
        }

        let mut peer = self.new_peer().await?;
        let cancel = CancellationToken::new();

        let mut peer_progress_rx = peer.subscribe_peer_progress();
        let peer_progress_bar = m.add(ProgressBar::new(0u64));
        peer_progress_bar.set_style(self.spinner_style.clone());
        peer_progress_bar.set_prefix("🟥");
        peer_progress_bar.enable_steady_tick(Duration::from_millis(100));
        let peer_progress_cancel = cancel.clone();
        let (peer_spinner_style, peer_msg_style) =
            (self.spinner_style.clone(), self.msg_style.clone());
        spawn(async move {
            loop {
                select! {
                    _ = peer_progress_cancel.cancelled() => {
                        return Ok::<(), Error>(());
                    }
                    peer_result = peer_progress_rx.changed() => {
                        peer_result?;
                        let peer_progress = peer_progress_rx.borrow_and_update();
                        if let PeerState::Connected = peer_progress.state {
                            peer_progress_bar.set_style(peer_msg_style.clone());
                            peer_progress_bar.set_prefix("🟢");
                            peer_progress_bar.disable_steady_tick();
                            peer_progress_bar.finish_with_message("Connected to Veilid network");
                            continue;
                        } else if peer_progress_bar.is_finished() {
                            peer_progress_bar.reset();
                            peer_progress_bar.set_style(peer_spinner_style.clone());
                            peer_progress_bar.enable_steady_tick(Duration::from_millis(100));
                            peer_progress_bar.set_prefix("🟥");
                        }
                        let (prefix, message) = match peer_progress.state {
                            PeerState::Starting => ("🟥","Starting peer"),
                            PeerState::Connecting => ("🔴","Connecting to Veilid network"),
                            PeerState::Announcing => ("🟡","Announcing share"),
                            PeerState::Resolving => ("🟡","Resolving share"),
                            _ => continue,
                        };
                        peer_progress_bar.set_prefix(prefix);
                        peer_progress_bar.set_message(message);
                        peer_progress_bar.update(|pb| {
                            pb.set_len(peer_progress.length);
                            pb.set_pos(peer_progress.position);
                        });
                    }
                }
            }
        });

        let mut backoff = ExponentialBackoff::default();
        loop {
            let result = peer.reset().await;
            if let Err(e) = result {
                if !e.is_resetable() {
                    error!(err = format!("{}", e), "not resetable");
                    cancel.cancel();
                    return Err(e.into());
                }
                match backoff.next_backoff() {
                    Some(delay) => sleep(delay).await,
                    None => {
                        cancel.cancel();
                        return Err(Error::msg("peer reset retries exceeded"));
                    }
                }
            } else {
                break;
            }
        }

        let ctrl_c_cancel = cancel.clone();
        let canceller = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = ctrl_c_cancel.cancelled() => {
                        return Ok::<(), Error>(())
                    }
                    _ = tokio::signal::ctrl_c() => {
                        ctrl_c_cancel.cancel();
                        return Ok(())
                    }
                }
            }
        });

        let result = match self.cli.commands {
            Commands::Fetch {
                dht_key: ref share_key,
                ref root,
            } => self.do_fetch(m, peer, cancel, share_key, root).await,
            Commands::Seed { ref file } => self.do_seed(m, peer, cancel, file).await,
            _ => {
                cancel.cancel();
                Err(Error::msg("invalid command"))
            }
        };
        let _ = canceller.await?;
        Ok(result?)
    }

    async fn new_peer(&self) -> Result<AppPeer> {
        let (routing_context, update_tx, _) =
            new_routing_context(&self.cli.state_dir()?, None).await?;
        let peer = Observable::new(Veilid::new(routing_context, update_tx).await?);
        Ok(peer)
    }

    async fn do_fetch(
        &self,
        m: MultiProgress,
        peer: AppPeer,
        cancel: CancellationToken,
        share_key: &str,
        root: &str,
    ) -> Result<()> {
        let mut fetcher = Fetcher::from_dht(peer.clone(), share_key, root).await?;

        // Index an existing partial fetch.
        let indexer = Indexer::from_wanted(fetcher.want_index()).await?;
        self.add_index_progress_bars(&indexer, &m, cancel.clone());

        // Resume a prior fetch
        fetcher.set_have_index(indexer.index().await?);

        let fetch_progress = m.add(
            ProgressBar::new(0u64)
                .with_style(self.spinner_style.clone())
                .with_prefix("📥"),
        );
        fetch_progress.set_message(format!(
            "Fetching {} into {}",
            fetcher.file().bold().bright_cyan(),
            PathBuf::from(root).canonicalize()?.to_string_lossy()
        ));
        let fetch_progress_spinner = fetch_progress.clone();

        let progress_cancel = cancel.clone();
        let mut fetch_progress_rx = fetcher.subscribe_fetch_progress();
        let mut verify_progress_rx = fetcher.subscribe_verify_progress();
        let fetch_progress_bar = m.add(ProgressBar::new(0u64));
        fetch_progress_bar.set_style(self.bytes_style.clone());
        fetch_progress_bar.set_message("Fetching blocks");
        let verify_progress_bar = m.add(ProgressBar::new(0u64));
        verify_progress_bar.set_style(self.bar_style.clone());
        verify_progress_bar.set_message("Verifying blocks");
        spawn(async move {
            loop {
                select! {
                    _ = progress_cancel.cancelled() => {
                        return Ok::<(), Error>(())
                    }
                    fetch_result = fetch_progress_rx.changed() => {
                        fetch_result?;
                        let fetch_progress = fetch_progress_rx.borrow_and_update();
                        fetch_progress_spinner.update(|pb| {
                            pb.set_len(fetch_progress.length);
                            pb.set_pos(fetch_progress.position);
                        });
                        fetch_progress_bar.update(|pb| {
                            pb.set_len(fetch_progress.length);
                            pb.set_pos(fetch_progress.position);
                        });
                        if fetch_progress.position == fetch_progress.length {
                            fetch_progress_bar.finish_with_message("Fetch complete");
                        }
                    }
                    verify_result = verify_progress_rx.changed() => {
                        verify_result?;
                        let verify_progress = verify_progress_rx.borrow_and_update();
                        verify_progress_bar.update(|pb| {
                            pb.set_len(verify_progress.length);
                            pb.set_pos(verify_progress.position);
                        });
                        if verify_progress.position == verify_progress.length {
                            verify_progress_bar.finish_with_message("Verified");
                        }
                    }
                }
            }
        });

        let fetch_result = fetcher.fetch(cancel.clone()).await;
        let msg = match fetch_result {
            Ok(()) => "✅ Fetch complete",
            Err(stigmerge_peer::Error::Fault(stigmerge_peer::Unexpected::Cancelled)) => {
                "❌ Fetch cancelled"
            }
            Err(_) => "❌ Fetch failed",
        };
        fetch_progress.finish_with_message(msg);

        cancel.cancel();
        peer.shutdown().await?;

        let _ = m.println(msg);
        Ok(())
    }

    async fn do_seed(
        &self,
        m: MultiProgress,
        peer: AppPeer,
        cancel: CancellationToken,
        file: &str,
    ) -> Result<()> {
        let indexer = Indexer::from_file(file.into()).await?;

        self.add_index_progress_bars(&indexer, &m, cancel.clone());

        let seeder = Seeder::new(peer.clone(), indexer.index().await?).await?;
        let share_key = seeder.share_key();
        let seed_progress = m.add(
            ProgressBar::new(0u64)
                .with_style(self.msg_style.clone())
                .with_prefix("🌱"),
        );
        seed_progress.set_message(format!(
            "Seeding {} to {}",
            file.bold().bright_cyan(),
            share_key.clone().bold().bright_cyan()
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
        seeder.seed(cancel.clone()).await?;
        seed_progress.finish();

        cancel.cancel();
        peer.shutdown().await?;

        Ok(())
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
