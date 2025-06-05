//! Example: fetch a file and keep seeding it
//! This example doesn't support swarm fetching from gossip-discovered peers.
//! It also isn't have-map aware.
//!
//! Those will be built in followup examples, as we incrementally build up to a
//! fully-autonomous swarming peer.
//!
//! However, you can daisy-chain syncers off an initial share with this example, like this:
//! - Peer 1 seeds a file at share_key_1
//! - Peer 2 fetches from share_key_1, publishing at share_key_2
//! - Peer 3 fetches from share_key_2, etc...
#![recursion_limit = "256"]

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;

/// Syncer CLI arguments
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The index digest to fetch
    #[arg(help = "The index digest to fetch")]
    want_index_digest: Option<String>,

    #[arg(long = "seed", short = 's', help = "File to seed")]
    seed_path: Option<PathBuf>,

    #[arg(default_value = ".", help = "Directory containing file share")]
    share_path: PathBuf,

    #[arg(long = "peer", short = 'p', help = "Remote share key to fetch from")]
    share_keys: Vec<String>,

    #[arg(
        default_value = "50",
        long = "fetchers",
        help = "Number of concurrent fetchers"
    )]
    fetchers: usize,
}

use path_absolutize::Absolutize;
use stigmerge_fileindex::Indexer;
use stigmerge_peer::actor::{ResponseChannel, UntilCancelled};
use stigmerge_peer::content_addressable::ContentAddressable;
use stigmerge_peer::peer_resolver::PeerResolver;
use stigmerge_peer::share_announcer::{self, ShareAnnouncer};
use tokio::select;
use tokio::spawn;
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use stigmerge_peer::actor::{ConnectionState, Operator, WithVeilidConnection};
use stigmerge_peer::block_fetcher::BlockFetcher;
use stigmerge_peer::fetcher::{Clients as FetcherClients, Fetcher};
use stigmerge_peer::have_announcer::HaveAnnouncer;
use stigmerge_peer::new_routing_context;
use stigmerge_peer::node::{Node, Veilid};
use stigmerge_peer::piece_verifier::PieceVerifier;
use stigmerge_peer::seeder::{self, Seeder};
use stigmerge_peer::share_resolver::{self, ShareResolver};
use stigmerge_peer::types::ShareInfo;
use stigmerge_peer::{Error, Result};
use veilid_core::TypedKey;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let state_dir = tempfile::tempdir()?;

    // Set up Veilid node
    let (routing_context, update_rx) =
        new_routing_context(state_dir.path().to_str().unwrap(), None).await?;
    let node = Veilid::new(routing_context, update_rx).await?;

    let res = run(node.clone()).await;
    let _ = node.shutdown().await;
    if let Err(e) = res {
        error!(err = e.to_string());
        return Err(e);
    }
    Ok(())
}

async fn run<T: Node + Sync + Send + 'static>(node: T) -> Result<()> {
    // Parse command line arguments
    let args = Args::parse();

    if args.seed_path.is_some() && args.want_index_digest.is_some() {
        return Err(Error::msg("cannot use --seed with index digest"));
    }

    let root = match args.seed_path {
        Some(ref path) => path
            .absolutize()?
            .parent()
            .ok_or(Error::msg("expected parent directory"))?
            .to_path_buf(),
        None => args.share_path,
    };

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

    // Resolve bootstrap share keys and want_index_digest
    let mut want_index = None;
    if let Some(ref want_index_digest) = args.want_index_digest {
        for share_key_str in args.share_keys.iter() {
            let share_key: TypedKey = share_key_str.parse()?;
            let want_index_digest = hex::decode(want_index_digest.clone())?;
            let want_index_digest: [u8; 32] = want_index_digest
                .try_into()
                .map_err(|_| Error::msg("Invalid digest length"))?;
            // Resolve the index from the bootstrap peer
            let index = match share_resolver_op
                .call(share_resolver::Request::Index {
                    response_tx: ResponseChannel::default(),
                    key: share_key.clone(),
                    want_index_digest: Some(want_index_digest),
                    root: root.clone(),
                })
                .await?
            {
                share_resolver::Response::Index { index, .. } => index,
                share_resolver::Response::BadIndex { .. } => anyhow::bail!("Bad index"),
                share_resolver::Response::NotAvailable { err_msg, .. } => {
                    anyhow::bail!(err_msg)
                }
                _ => anyhow::bail!("Unexpected response"),
            };
            want_index.get_or_insert(index);
        }
    }
    let mut index = match want_index {
        Some(index) => index,
        None => {
            // If an index wasn't resolved, and we didn't bail on an error,
            // then assume a want_index_digest and share_keys weren't provided.
            // So we're a lone seeder, starting a new share.
            let indexer = Indexer::from_file(
                args.seed_path
                    .ok_or(Error::msg("expected seed path"))?
                    .as_path(),
            )
            .await?;
            indexer.index().await?
        }
    };
    let index_digest = index.digest()?;
    info!(index_digest = hex::encode(index_digest));

    // Announce our own share of the index
    let mut share_announce_op = Operator::new(
        cancel.clone(),
        ShareAnnouncer::new(node.clone(), index.clone()),
        WithVeilidConnection::new(node.clone(), conn_state.clone()),
    );
    let (share_key, share_header) = match share_announce_op
        .call(share_announcer::Request::Announce {
            response_tx: ResponseChannel::default(),
        })
        .await?
    {
        share_announcer::Response::Announce {
            key,
            target: _,
            header,
        } => (key, header),
        share_announcer::Response::NotAvailable => anyhow::bail!("failed to announce share"),
    };

    info!("announced share, key: {share_key}");

    // Set up fetcher dependencies
    let block_fetcher = Operator::new_clone_pool(
        cancel.clone(),
        BlockFetcher::new(
            node.clone(),
            Arc::new(RwLock::new(index.clone())),
            index.root().to_path_buf(),
        ),
        WithVeilidConnection::new(node.clone(), conn_state.clone()),
        args.fetchers,
    );

    let piece_verifier = PieceVerifier::new(Arc::new(RwLock::new(index.clone())));
    let verified_rx = piece_verifier.subscribe_verified();
    let piece_verifier_op = Operator::new(cancel.clone(), piece_verifier, UntilCancelled);

    // Announce our own have-map as we fetch, at our announced share's have-map key
    let have_announcer = Operator::new(
        cancel.clone(),
        // TODO: should use the share key publicly; hide this from the actor / op interface
        HaveAnnouncer::new(node.clone(), share_header.have_map().unwrap().key().clone()),
        WithVeilidConnection::new(node.clone(), conn_state.clone()),
    );

    let share = ShareInfo {
        key: share_key,
        want_index: index.clone(),
        want_index_digest: index_digest,
        root,
        header: share_header.clone(),
    };

    let peer_resolver = PeerResolver::new(node.clone());
    let discovered_peers_rx = peer_resolver.subscribe_discovered_peers();
    let peer_resolver_op = Operator::new(
        cancel.clone(),
        peer_resolver,
        WithVeilidConnection::new(node.clone(), conn_state.clone()),
    );

    let fetcher_clients = FetcherClients {
        block_fetcher,
        piece_verifier: piece_verifier_op,
        have_announcer,
        share_resolver: share_resolver_op,
        share_target_rx,
        peer_resolver: peer_resolver_op,
        discovered_peers_rx,
    };

    // Set up seeder
    let seeder_clients = seeder::Clients {
        update_rx: node.subscribe_veilid_update(),
        verified_rx,
    };

    let seeder = Seeder::new(node.clone(), share.clone(), seeder_clients);
    let seeder_op = Operator::new(
        cancel.clone(),
        seeder,
        WithVeilidConnection::new(node.clone(), conn_state),
    );

    // Create and run fetcher
    let fetcher = Fetcher::new(node.clone(), share.clone(), fetcher_clients);

    info!("Starting fetch...");

    // Run the fetcher until completion
    let fetcher_task = spawn(fetcher.run(cancel.clone()));

    info!("Seeding until ctrl-c...");

    // Keep seeding until ctrl-c
    select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received ctrl-c, shutting down...");
            cancel.cancel();
        }
        join_res = fetcher_task => {
            join_res.expect("fetcher task").expect("fetcher done");
            info!("fetch complete, key={share_key}");
        }
        join_res = seeder_op.join() => {
            join_res.expect("seeder task");
        }
    }

    Ok(())
}
