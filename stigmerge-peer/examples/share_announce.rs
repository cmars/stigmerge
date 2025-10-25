//! Example: announce a file
#![recursion_limit = "256"]

use std::path::PathBuf;

use clap::Parser;
use stigmerge_fileindex::Indexer;
use stigmerge_peer::new_connection;
use stigmerge_peer::share_announcer::ShareAnnouncer;
use stigmerge_peer::Error;
use stigmerge_peer::Retry;
use tokio::{select, spawn};
use tokio_util::sync::CancellationToken;
use tracing::info;
use veilnet::Connection;

/// Share announce CLI arguments
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the file to announce
    #[arg(help = "Path to the file to announce")]
    file: PathBuf,
}

#[tokio::main]
async fn main() -> std::result::Result<(), Error> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let file = args.file;

    // Index the file
    let indexer = Indexer::from_file(file.as_path()).await?;
    let index = indexer.index().await?;

    // Set up Veilid peer
    let state_dir = tempfile::tempdir()?;
    let conn = new_connection(state_dir.path().to_str().unwrap(), None).await?;
    let cancel = CancellationToken::new();
    let retry = Retry::default();

    let share_announcer = ShareAnnouncer::new(cancel.clone(), retry, conn.clone(), index).await?;
    let share_info = share_announcer.share_info().await;
    let announcer_task = spawn(share_announcer.run());

    info!(key = ?share_info.key, "announced share");

    spawn(async move {
        select! {
            _ = tokio::signal::ctrl_c() => {
                conn.close().await?;
                cancel.cancel();
            }
        }
        Ok::<(), veilnet::connection::Error>(())
    });

    announcer_task
        .await
        .expect("announce task")
        .expect("announce run");
    Ok(())
}
