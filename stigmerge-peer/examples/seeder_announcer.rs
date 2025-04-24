//! Example: announce a file using a real Veilid peer
// Usage: cargo run --example share_and_seed -- <FILE>

use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use stigmerge_peer::fetcher::{self, Fetcher};
use stigmerge_peer::seeder::{self, Seeder};
use stigmerge_peer::types::ShareInfo;
use tokio::sync::{Mutex, RwLock};
use tokio::{select, spawn};
use tokio_util::sync::CancellationToken;
use tracing::info;

use stigmerge_fileindex::Indexer;
use stigmerge_peer::actor::{
    ConnectionState, OneShot, Operator, UntilCancelled, WithVeilidConnection,
};
use stigmerge_peer::node::Veilid;
use stigmerge_peer::share_announcer::{self, ShareAnnouncer};
use stigmerge_peer::{block_fetcher, have_announcer, share_resolver, Error};
use stigmerge_peer::{new_routing_context, piece_verifier, Node};

#[tokio::main]
async fn main() -> std::result::Result<(), Error> {
    tracing_subscriber::fmt::init();

    let file = env::args().nth(1).expect("usage: <prog> <FILE>");
    let file = PathBuf::from(file);
    let root = file
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .to_path_buf();

    // Index the file
    let indexer = Indexer::from_file(file.clone()).await?;
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
    let (key, target, header) = match resp {
        Some(share_announcer::Response::Announce {
            key,
            target,
            header,
        }) => (key, target, header),
        _ => anyhow::bail!("Announce failed"),
    };
    info!("announced: key={key}, target={target:?}");

    let resolver = share_resolver::ShareResolver::new(node.clone());
    let target_update_rx = resolver.subscribe_target();

    let shared_index = Arc::new(RwLock::new(index.clone()));

    // Verify the local share
    let verifier = piece_verifier::PieceVerifier::new(shared_index.clone());

    // Set up the seeder
    let share = ShareInfo {
        header: header.clone(),
        want_index: index.clone(),
        root: root.clone(),
    };
    let seeder_clients = seeder::Clients {
        update_rx: node.subscribe_veilid_update(),
        verified_rx: verifier.subscribe_verified(),
    };

    let fetcher = Fetcher::new(
        share.clone(),
        fetcher::Clients {
            block_fetcher: Operator::new(
                cancel.clone(),
                block_fetcher::BlockFetcher::new(
                    node.clone(),
                    shared_index.clone(),
                    root.clone(),
                    target_update_rx,
                ),
                WithVeilidConnection::new(UntilCancelled, node.clone(), conn_state.clone()),
            ),
            piece_verifier: Operator::new(
                cancel.clone(),
                verifier,
                WithVeilidConnection::new(UntilCancelled, node.clone(), conn_state.clone()),
            ),
            have_announcer: Operator::new(
                cancel.clone(),
                have_announcer::HaveAnnouncer::new(
                    node.clone(),
                    header.have_map().unwrap().key().clone(),
                ),
                WithVeilidConnection::new(UntilCancelled, node.clone(), conn_state.clone()),
            ),
        },
    );
    spawn(fetcher.run(cancel.clone()));

    Operator::new(
        cancel.clone(),
        Seeder::new(node.clone(), share, seeder_clients),
        WithVeilidConnection::new(UntilCancelled, node.clone(), conn_state.clone()),
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
    Ok(())
}
