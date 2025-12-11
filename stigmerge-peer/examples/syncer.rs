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

use clap::Parser;
use path_absolutize::Absolutize;
use stigmerge_fileindex::Indexer;
use stigmerge_peer::content_addressable::ContentAddressable;
use stigmerge_peer::fetcher::Fetcher;
use stigmerge_peer::peer_gossip::PeerGossip;
use stigmerge_peer::piece_verifier::PieceVerifier;
use stigmerge_peer::seeder::Seeder;
use stigmerge_peer::share_announcer::ShareAnnouncer;
use stigmerge_peer::share_resolver::ShareResolver;
use stigmerge_peer::{new_connection, Error, Retry};
use tokio::select;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use veilid_core::TypedRecordKey;
use veilnet::Connection;

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

#[tokio::main]
async fn main() -> std::result::Result<(), Error> {
    tracing_subscriber::fmt::init();

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

    // Set up Veilid connection
    let state_dir = tempfile::tempdir()?;
    let conn = new_connection(state_dir.path().to_str().unwrap(), None).await?;
    let cancel = CancellationToken::new();
    let retry = Retry::default();

    let mut tasks = JoinSet::new();

    // Set up share resolver for bootstrap
    let (share_resolver, resolver_task) =
        ShareResolver::new_task(cancel.clone(), retry.clone(), conn.clone(), &root);
    tasks.spawn(async move {
        resolver_task.await??;
        Ok(())
    });

    // Resolve bootstrap share keys and want_index_digest
    let mut index = None;
    if let Some(ref want_index_digest) = args.want_index_digest {
        for share_key_str in args.share_keys.iter() {
            let share_key: TypedRecordKey = share_key_str.parse()?;
            let want_index_digest = hex::decode(want_index_digest.clone())?;
            let want_index_digest: [u8; 32] = want_index_digest
                .try_into()
                .map_err(|_| Error::msg("Invalid digest length"))?;

            // Resolve the index from the bootstrap peer
            let mut remote_share = share_resolver.add_share(&share_key).await?;
            if remote_share.index.digest()? == want_index_digest {
                index = Some(remote_share.index);
                break;
            } else {
                return Err(Error::msg(
                    "remote share does not match wanted index digest",
                ));
            }
        }
    }

    let mut index = match index {
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
    let share_announcer =
        ShareAnnouncer::new(cancel.clone(), retry.clone(), conn.clone(), index.clone()).await?;
    let share_info = share_announcer.share_info().await;
    tasks.spawn(share_announcer.run());

    info!("announced share, key: {:?}", share_info.key);

    // Set up piece verifier
    let piece_verifier =
        PieceVerifier::new(std::sync::Arc::new(tokio::sync::RwLock::new(index.clone()))).await;

    // Set up seeder
    let seeder = Seeder::new(conn.clone(), share_info.clone(), piece_verifier.clone()).await?;
    tasks.spawn(seeder.run(cancel.clone(), retry.clone()));

    // Add bootstrap shares for fetching
    for share_key_str in args.share_keys.iter() {
        let share_key: TypedRecordKey = share_key_str.parse()?;
        let _ = share_resolver.add_share(&share_key).await?;
    }

    let peer_gossip =
        PeerGossip::new(conn.clone(), share_info.clone(), share_resolver.clone()).await?;
    tasks.spawn(peer_gossip.run(cancel.clone(), retry.clone()));

    // Create and run fetcher
    let fetcher = Fetcher::new(
        conn.clone(),
        share_info.clone(),
        piece_verifier.clone(),
        share_resolver,
    )
    .await;

    info!("Starting fetch...");

    // Run the fetcher until completion
    let fetcher_future = async {
        let retry = Retry::default();
        fetcher.run(cancel.clone(), retry).await
    };

    info!("Seeding until ctrl-c...");

    // Keep seeding until ctrl-c
    select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received ctrl-c, shutting down...");
            cancel.cancel();
        }
        fetch_res = fetcher_future => {
            match fetch_res {
                Ok(()) => info!("fetch complete, key={:?}", share_info.key),
                Err(e) => {
                    error!("fetch failed: {:?}", e);
                    cancel.cancel();
                }
            }
        }
    }

    // Clean up
    tasks.spawn(async move { conn.close().await.map_err(|e| e.into()) });
    tasks
        .join_all()
        .await
        .into_iter()
        .collect::<anyhow::Result<()>>()?;
    Ok(())
}
