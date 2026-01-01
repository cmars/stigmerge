//! Example: fetch a file using the new refactored veilnet-based APIs
#![recursion_limit = "256"]

use std::path::PathBuf;

use clap::Parser;
use stigmerge_peer::content_addressable::ContentAddressable;
use stigmerge_peer::fetcher::Fetcher;
use stigmerge_peer::peer_gossip::PeerGossip;
use stigmerge_peer::piece_verifier::PieceVerifier;
use stigmerge_peer::share_resolver::ShareResolver;
use stigmerge_peer::{new_connection, Error, Retry};
use tokio::select;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;
use veilid_core::RecordKey;
use veilnet::Connection;

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

    // Parse command line arguments
    let args = Args::parse();
    let want_index_digest = args.want_index_digest;
    let share_key = args.share_key;
    let download_dir = args.download_dir;

    // Set up Veilid connection
    let state_dir = tempfile::tempdir()?;
    let conn = new_connection(state_dir.path().to_str().unwrap(), None).await?;
    let cancel = CancellationToken::new();
    let retry = Retry::default();

    let mut tasks = JoinSet::new();

    // Set up share resolver
    let (share_resolver, resolver_task) =
        ShareResolver::new_task(cancel.clone(), retry.clone(), conn.clone(), &download_dir);
    tasks.spawn(async move {
        resolver_task.await??;
        Ok(())
    });

    // Parse share key and want_index_digest
    let key: RecordKey = share_key.parse()?;
    let want_index_digest = hex::decode(want_index_digest)?;
    let want_index_digest: [u8; 32] = want_index_digest
        .try_into()
        .map_err(|_| Error::msg("Invalid digest length"))?;

    // Resolve the index from the remote share
    let mut remote_share = share_resolver.add_share(&key).await?;
    let remote_index_digest = remote_share.index.digest()?;

    // Verify the index matches what we want
    if remote_index_digest != want_index_digest {
        warn!(
            remote_index_digest = hex::encode(remote_index_digest),
            want_index_digest = hex::encode(want_index_digest),
        );
        return Err(Error::msg(
            "remote share does not match wanted index digest",
        ));
    }

    info!(
        index_digest = hex::encode(remote_share.index.digest()?),
        "resolved remote share"
    );

    // Create local share info
    let share_info = stigmerge_peer::types::LocalShareInfo {
        key: key.clone(),
        header: remote_share.header.clone(),
        want_index: remote_share.index.clone(),
        want_index_digest,
        root: download_dir,
    };

    // Set up piece verifier
    let piece_verifier = PieceVerifier::new(std::sync::Arc::new(tokio::sync::RwLock::new(
        remote_share.index.clone(),
    )))
    .await;

    let peer_gossip =
        PeerGossip::new(conn.clone(), share_info.clone(), share_resolver.clone()).await?;
    tasks.spawn(peer_gossip.run(cancel.clone(), retry.clone()));

    // Create and run fetcher
    let fetcher = Fetcher::new(
        conn.clone(),
        share_info.clone(),
        piece_verifier.clone(),
        share_resolver.clone(),
        vec![remote_share],
    )
    .await;

    info!("Starting fetch...");

    select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received ctrl-c, shutting down...");
            cancel.cancel();
        }
        fetch_res = fetcher.run(cancel.clone(), retry) => {
            match fetch_res {
                Ok(()) => info!("fetch complete, key={:?}", share_info.key),
                Err(e) => {
                    error!("fetch failed: {:?}", e);
                    cancel.cancel();
                    return Err(e);
                }
            }
        }
    }

    cancel.cancel();
    conn.close().await?;

    tasks
        .join_all()
        .await
        .into_iter()
        .collect::<anyhow::Result<()>>()?;

    Ok(())
}
