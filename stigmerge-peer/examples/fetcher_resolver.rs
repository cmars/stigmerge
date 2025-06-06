//! Example: fetch a file
#![recursion_limit = "256"]

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;

/// Fetcher resolver CLI arguments
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The index digest to fetch
    #[arg(help = "The index digest to fetch")]
    want_index_digest: String,

    /// The share key to fetch from
    #[arg(help = "The share key to fetch from")]
    share_key: String,

    /// Directory to download files to
    #[arg(default_value = ".", help = "Directory to download files to")]
    download_dir: PathBuf,
}

use stigmerge_peer::actor::ResponseChannel;
use stigmerge_peer::actor::UntilCancelled;
use stigmerge_peer::peer_resolver::PeerResolver;
use tokio::select;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::info;

use stigmerge_peer::actor::{ConnectionState, Operator, WithVeilidConnection};
use stigmerge_peer::block_fetcher::BlockFetcher;
use stigmerge_peer::fetcher::{Clients, Fetcher};
use stigmerge_peer::have_announcer::HaveAnnouncer;
use stigmerge_peer::new_routing_context;
use stigmerge_peer::node::Veilid;
use stigmerge_peer::piece_verifier::PieceVerifier;
use stigmerge_peer::share_resolver::{self, ShareResolver};
use stigmerge_peer::types::ShareInfo;
use stigmerge_peer::Error;
use veilid_core::TypedRecordKey;

#[tokio::main]
async fn main() -> std::result::Result<(), Error> {
    tracing_subscriber::fmt::init();

    // Parse command line arguments
    let args = Args::parse();
    let want_index_digest = args.want_index_digest;
    let share_key = args.share_key;
    let download_dir = args.download_dir;

    let state_dir = tempfile::tempdir()?;

    // Set up Veilid node
    let (routing_context, update_rx) =
        new_routing_context(state_dir.path().to_str().unwrap(), None).await?;
    let node = Veilid::new(routing_context, update_rx).await?;

    let cancel = CancellationToken::new();
    let conn_state = Arc::new(Mutex::new(ConnectionState::new()));

    // Set up share resolver
    let share_resolver = ShareResolver::new(node.clone());
    let share_target_rx = share_resolver.subscribe_target();
    let mut share_resolver_op = Operator::new(
        cancel.clone(),
        share_resolver,
        WithVeilidConnection::new(node.clone(), conn_state.clone()),
    );

    // Parse share key and want_index_digest
    let key: TypedRecordKey = share_key.parse()?;
    let want_index_digest = hex::decode(want_index_digest)?;
    let want_index_digest: [u8; 32] = want_index_digest
        .try_into()
        .map_err(|_| Error::msg("Invalid digest length"))?;

    // Resolve the index
    let (header, index) = match share_resolver_op
        .call(share_resolver::Request::Index {
            response_tx: ResponseChannel::default(),
            key: key.clone(),
            want_index_digest: Some(want_index_digest),
            root: download_dir.clone(),
        })
        .await?
    {
        share_resolver::Response::Index { header, index, .. } => (header, index),
        share_resolver::Response::BadIndex { .. } => anyhow::bail!("Bad index"),
        share_resolver::Response::NotAvailable { err_msg, .. } => anyhow::bail!(err_msg),
        _ => anyhow::bail!("Unexpected response"),
    };

    // Set up fetcher dependencies
    let want_index = Arc::new(RwLock::new(index.clone()));
    let block_fetcher = Operator::new_clone_pool(
        cancel.clone(),
        BlockFetcher::new(node.clone(), want_index.clone(), download_dir.clone()),
        WithVeilidConnection::new(node.clone(), conn_state.clone()),
        50,
    );

    let piece_verifier = Operator::new(
        cancel.clone(),
        PieceVerifier::new(want_index.clone()),
        UntilCancelled,
    );

    let have_announcer = Operator::new(
        cancel.clone(),
        HaveAnnouncer::new(node.clone(), header.have_map().unwrap().key().clone()),
        WithVeilidConnection::new(node.clone(), conn_state.clone()),
    );

    let share = ShareInfo {
        key,
        want_index: index,
        want_index_digest,
        root: download_dir,
        header,
    };

    let peer_resolver = PeerResolver::new(node.clone());
    let discovered_peers_rx = peer_resolver.subscribe_discovered_peers();
    let peer_resolver_op = Operator::new(
        cancel.clone(),
        peer_resolver,
        WithVeilidConnection::new(node.clone(), conn_state),
    );

    let clients = Clients {
        block_fetcher,
        piece_verifier,
        have_announcer,
        share_resolver: share_resolver_op,
        share_target_rx,
        peer_resolver: peer_resolver_op,
        discovered_peers_rx,
    };

    // Create and run fetcher
    let fetcher = Fetcher::new(node.clone(), share, clients);

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
