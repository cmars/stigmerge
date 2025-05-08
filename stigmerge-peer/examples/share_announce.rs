//! Example: announce a file

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;

/// Share announce CLI arguments
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the file to announce
    #[arg(help = "Path to the file to announce")]
    file: PathBuf,
}

use tokio::select;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::info;

use stigmerge_fileindex::Indexer;
use stigmerge_peer::actor::{ConnectionState, OneShot, Operator, WithVeilidConnection};
use stigmerge_peer::new_routing_context;
use stigmerge_peer::node::Veilid;
use stigmerge_peer::share_announcer::{self, ShareAnnouncer};
use stigmerge_peer::Error;

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
    let (routing_context, update_tx, _) =
        new_routing_context(state_dir.path().to_str().unwrap(), None).await?;
    let node = Veilid::new(routing_context, update_tx).await?;

    let cancel = CancellationToken::new();
    let conn_state = Arc::new(Mutex::new(ConnectionState::new()));

    // Announce the share
    let mut announce_op = Operator::new(
        cancel.clone(),
        ShareAnnouncer::new(node.clone(), index.clone()),
        WithVeilidConnection::new(OneShot, node.clone(), conn_state.clone()),
    );
    announce_op.send(share_announcer::Request::Announce).await?;

    let resp = announce_op.recv().await;
    let (key, target, _header) = match resp {
        Some(share_announcer::Response::Announce {
            key,
            target,
            header,
        }) => (key, target, header),
        _ => anyhow::bail!("Announce failed"),
    };
    info!("announced: key={key}, target={target:?}");

    select! {
        _ = tokio::signal::ctrl_c() => {
            cancel.cancel();
        }
    }

    announce_op
        .join()
        .await
        .expect("announce task")
        .expect("announce run");
    Ok(())
}
