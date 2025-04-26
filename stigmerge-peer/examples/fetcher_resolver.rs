//! Example: fetch a file
//! Usage: cargo run --example fetcher_resolver -- <WANT_INDEX_DIGEST> <SHARE_KEY> [DOWNLOAD_DIR]

use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use stigmerge_peer::actor::UntilCancelled;
use tokio::select;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::info;

use stigmerge_peer::actor::{ConnectionState, OneShot, Operator, WithVeilidConnection};
use stigmerge_peer::block_fetcher::BlockFetcher;
use stigmerge_peer::fetcher::{Clients, Fetcher};
use stigmerge_peer::have_announcer::HaveAnnouncer;
use stigmerge_peer::new_routing_context;
use stigmerge_peer::node::Veilid;
use stigmerge_peer::piece_verifier::PieceVerifier;
use stigmerge_peer::share_resolver::{self, ShareResolver};
use stigmerge_peer::types::ShareInfo;
use stigmerge_peer::Error;
use veilid_core::TypedKey;

#[tokio::main]
async fn main() -> std::result::Result<(), Error> {
    tracing_subscriber::fmt::init();

    // Parse command line arguments
    let want_index_digest = env::args()
        .nth(1)
        .expect("usage: <prog> <WANT_INDEX_DIGEST> <SHARE_KEY> [DOWNLOAD_DIR]");
    let share_key = env::args()
        .nth(2)
        .expect("usage: <prog> <WANT_INDEX_DIGEST> <SHARE_KEY> [DOWNLOAD_DIR]");
    let download_dir = env::args().nth(3).unwrap_or_else(|| ".".to_string());
    let download_dir = PathBuf::from(download_dir);

    let state_dir = tempfile::tempdir()?;

    // Set up Veilid node
    let (routing_context, update_tx, _) =
        new_routing_context(state_dir.path().to_str().unwrap(), None).await?;
    let node = Veilid::new(routing_context, update_tx).await?;

    let cancel = CancellationToken::new();
    let conn_state = Arc::new(Mutex::new(ConnectionState::new()));

    // Set up share resolver
    let share_resolver = ShareResolver::new(node.clone());
    let target_rx = share_resolver.subscribe_target();
    let mut resolve_op = Operator::new(
        cancel.clone(),
        share_resolver,
        WithVeilidConnection::new(OneShot, node.clone(), conn_state.clone()),
    );

    // Parse share key and want_index_digest
    let key: TypedKey = share_key.parse()?;
    let want_index_digest = hex::decode(want_index_digest)?;
    let want_index_digest: [u8; 32] = want_index_digest
        .try_into()
        .map_err(|_| Error::msg("Invalid digest length"))?;

    // Resolve the index
    resolve_op
        .send(share_resolver::Request::Index {
            key: key.clone(),
            want_index_digest,
            root: download_dir.clone(),
        })
        .await?;

    let (header, index, target) = match resolve_op.recv().await {
        Some(share_resolver::Response::Index {
            header,
            index,
            target,
            ..
        }) => (header, index, target),
        Some(share_resolver::Response::BadIndex { .. }) => anyhow::bail!("Bad index"),
        Some(share_resolver::Response::NotAvailable { err_msg, .. }) => anyhow::bail!(err_msg),
        _ => anyhow::bail!("Unexpected response"),
    };

    // Set up fetcher dependencies
    let want_index = Arc::new(RwLock::new(index.clone()));
    let block_fetcher = Operator::new(
        cancel.clone(),
        BlockFetcher::new(
            node.clone(),
            want_index.clone(),
            download_dir.clone(),
            target_rx,
        ),
        WithVeilidConnection::new(UntilCancelled, node.clone(), conn_state.clone()),
    );

    let piece_verifier = Operator::new(
        cancel.clone(),
        PieceVerifier::new(want_index.clone()),
        UntilCancelled,
    );

    let have_announcer = Operator::new(
        cancel.clone(),
        HaveAnnouncer::new(node.clone(), header.have_map().unwrap().key().clone()),
        WithVeilidConnection::new(UntilCancelled, node.clone(), conn_state.clone()),
    );

    let share = ShareInfo {
        want_index: index,
        root: download_dir,
        header,
    };

    let clients = Clients {
        block_fetcher,
        piece_verifier,
        have_announcer,
    };

    // Create and run fetcher
    let fetcher = Fetcher::new(share, clients);

    info!("Starting fetch...");

    select! {
        res = fetcher.run(cancel.clone()) => {
            res?;
            info!("Fetch complete!");
            cancel.cancel();
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Cancelled");
            cancel.cancel();
        }
    }

    Ok(())
}
