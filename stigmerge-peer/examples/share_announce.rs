//! Example: announce a file
#![recursion_limit = "256"]

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use stigmerge_fileindex::Indexer;
use stigmerge_peer::actor::{ConnectionState, Operator, ResponseChannel, WithVeilidConnection};
use stigmerge_peer::new_routing_context;
use stigmerge_peer::node::Veilid;
use stigmerge_peer::share_announcer::{self, ShareAnnouncer};
use stigmerge_peer::Error;
use tokio::select;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::info;

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
    //let root = file
    //    .parent()
    //    .unwrap_or_else(|| std::path::Path::new("."))
    //    .to_path_buf();

    // Index the file
    let indexer = Indexer::from_file(file.as_path()).await?;
    let index = indexer.index().await?;

    let state_dir = tempfile::tempdir()?;

    // Set up Veilid peer
    let (routing_context, update_rx) =
        new_routing_context(state_dir.path().to_str().unwrap(), None).await?;
    let node = Veilid::new(routing_context, update_rx).await?;

    let cancel = CancellationToken::new();
    let conn_state = Arc::new(Mutex::new(ConnectionState::new()));

    // Announce the share
    let mut announce_op = Operator::new(
        cancel.clone(),
        ShareAnnouncer::new(node.clone(), index.clone()),
        WithVeilidConnection::new(node.clone(), conn_state.clone()),
    );

    let (key, target, _header) = match announce_op
        .call(share_announcer::Request::Announce {
            response_tx: ResponseChannel::default(),
        })
        .await?
    {
        share_announcer::Response::Announce {
            key,
            target,
            header,
        } => (key, target, header),
        _ => anyhow::bail!("Announce failed"),
    };
    info!("announced: key={key}, target={target:?}");

    select! {
        _ = tokio::signal::ctrl_c() => {
            cancel.cancel();
        }
    }

    announce_op.join().await.expect("announce task");
    Ok(())
}
