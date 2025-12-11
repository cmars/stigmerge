//! Example: announce and seed a file
#![recursion_limit = "256"]

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use stigmerge_fileindex::Indexer;
use stigmerge_peer::peer_gossip::PeerGossip;
//use stigmerge_peer::peer_gossip::PeerGossip;
use tokio::select;
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::info;

use stigmerge_peer::seeder::Seeder;
use stigmerge_peer::share_announcer::ShareAnnouncer;
use stigmerge_peer::share_resolver::ShareResolver;
use stigmerge_peer::types::PieceState;
use stigmerge_peer::{new_connection, piece_verifier};
use stigmerge_peer::{Error, Retry};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

/// Seeder announcer CLI arguments
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the file to seed
    #[arg(help = "Path to the file to seed")]
    file: PathBuf,
}

#[tokio::main]
async fn main() -> std::result::Result<(), Error> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_level(true)
                .with_ansi(false)
                .with_writer(std::io::stdout),
        )
        .with(EnvFilter::builder().from_env_lossy())
        .init();

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

    let mut tasks = JoinSet::new();

    // Announce the share
    let share_announcer =
        ShareAnnouncer::new(cancel.clone(), retry.clone(), conn.clone(), index.clone()).await?;
    let share_info = share_announcer.share_info().await;
    tasks.spawn(share_announcer.run());

    info!(key = ?share_info.key, want_index = hex::encode(share_info.want_index_digest), "announced share");

    let shared_index = Arc::new(RwLock::new(index.clone()));

    // Set up the verifier
    let verifier = piece_verifier::PieceVerifier::new(shared_index.clone()).await;

    // Set up share resolver
    let (share_resolver, resolver_task) =
        ShareResolver::new_task(cancel.clone(), retry.clone(), conn.clone(), index.root());
    tasks.spawn(async move {
        resolver_task.await??;
        Ok(())
    });

    let peer_gossip = PeerGossip::new(conn.clone(), share_info.clone(), share_resolver).await?;
    tasks.spawn(peer_gossip.run(cancel.clone(), retry.clone()));

    // Set up the seeder

    //let gossip_op = Operator::new(
    //    cancel.clone(),
    //    PeerGossip::new(
    //        node.clone(),
    //        share.clone(),
    //        share_resolver_op.request_tx.clone(),
    //        share_target_rx,
    //    ),
    //    WithVeilidConnection::new(node.clone(), conn_state.clone()),
    //);

    let seeder = Seeder::new(conn, share_info, verifier.clone()).await?;
    tasks.spawn(seeder.run(cancel.clone(), retry.clone()));

    // Verify the index, notifying seeder of verified pieces
    for (piece_index, piece) in index.payload().pieces().iter().enumerate() {
        for block_index in 0..piece.block_count() {
            let piece_state = PieceState::new(0, piece_index, 0, piece.block_count(), block_index);
            verifier.update_piece(piece_state).await?;
        }
    }

    // Query the seeder's have map
    #[cfg(feature = "refactor")]
    {
        let seeder::Response::HaveMap(have_map) = seeder_op
            .call(seeder::Request::HaveMap {
                response_tx: ResponseChannel::default(),
            })
            .await?;

        info!(
            "seeding {} {} {have_map:?}",
            key.to_string(),
            hex::encode(index_digest)
        );
    }

    info!("seeding");

    select! {
        _ = tokio::signal::ctrl_c() => {
            cancel.cancel();
        }
    }

    tasks
        .join_all()
        .await
        .into_iter()
        .collect::<anyhow::Result<()>>()?;

    Ok(())
}
