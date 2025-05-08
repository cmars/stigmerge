//! Example: announce and seed a file

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;

/// Seeder announcer CLI arguments
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the file to seed
    #[arg(help = "Path to the file to seed")]
    file: PathBuf,
}

use stigmerge_peer::seeder::{self, Seeder};
use stigmerge_peer::types::{PieceState, ShareInfo};
use tokio::select;
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::info;

use stigmerge_fileindex::Indexer;
use stigmerge_peer::actor::{
    ConnectionState, OneShot, Operator, UntilCancelled, WithVeilidConnection,
};
use stigmerge_peer::content_addressable::ContentAddressable;
use stigmerge_peer::node::Veilid;
use stigmerge_peer::share_announcer::{self, ShareAnnouncer};
use stigmerge_peer::Error;
use stigmerge_peer::{new_routing_context, piece_verifier, Node};

#[tokio::main]
async fn main() -> std::result::Result<(), Error> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let file = args.file;
    let root = file
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .to_path_buf();

    // Index the file
    let indexer = Indexer::from_file(file.as_path()).await?;
    let mut index = indexer.index().await?;
    let index_digest = index.digest()?;

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
        WithVeilidConnection::new(UntilCancelled, node.clone(), conn_state.clone()),
    );
    announce_op.send(share_announcer::Request::Announce).await?;

    let resp = announce_op.recv().await;
    let (key, target, header) = match resp {
        Some(share_announcer::Response::Announce {
            key,
            target,
            header,
        }) => (key, target, header),
        _ => anyhow::bail!("Announce failed"),
    };
    info!("announced: key={key}, target={target:?}");

    let shared_index = Arc::new(RwLock::new(index.clone()));

    // Set up the verifier
    let verifier = piece_verifier::PieceVerifier::new(shared_index.clone());
    let verified_rx = verifier.subscribe_verified();
    let mut verifier_op = Operator::new(cancel.clone(), verifier, OneShot);

    // Set up the seeder
    let share = ShareInfo {
        header: header.clone(),
        want_index: index.clone(),
        root: root.clone(),
    };
    let seeder_clients = seeder::Clients {
        update_rx: node.subscribe_veilid_update(),
        verified_rx,
    };

    let mut seeder_op = Operator::new(
        cancel.clone(),
        Seeder::new(node.clone(), share, seeder_clients),
        WithVeilidConnection::new(UntilCancelled, node.clone(), conn_state.clone()),
    );

    // Verify the index, notifying seeder of verified pieces
    for (piece_index, piece) in index.payload().pieces().iter().enumerate() {
        for block_index in 0..piece.block_count() {
            let req = piece_verifier::Request::Piece(PieceState::new(
                0,
                piece_index,
                0,
                piece.block_count(),
                block_index,
            ));
            verifier_op.send(req).await?;
            let resp = verifier_op.recv().await;
            info!("verifier: {resp:?}");
        }
    }

    // Query the seeder's have map
    seeder_op.send(seeder::Request::HaveMap).await?;
    let seeder::Response::HaveMap(have_map) = seeder_op.recv().await.unwrap();

    info!(
        "seeding {} {} {have_map:?}",
        key.to_string(),
        hex::encode(index_digest)
    );

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
    seeder_op
        .join()
        .await
        .expect("seeder task")
        .expect("seeder run");
    Ok(())
}
