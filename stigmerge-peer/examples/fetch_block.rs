//! Example: fetch a single block from a share
#![recursion_limit = "256"]

use std::path::PathBuf;

use clap::Parser;
use stigmerge_peer::new_connection;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::info;
use veilid_core::RecordKey;

use stigmerge_peer::block_fetcher::BlockFetcher;
use stigmerge_peer::share_resolver::ShareResolver;
use stigmerge_peer::types::FileBlockFetch;
use stigmerge_peer::Error;
use stigmerge_peer::Retry;
use veilnet::Connection;

/// Fetch block CLI arguments
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// The share key to fetch from
    #[arg(help = "The share key to fetch from")]
    share_key: String,

    /// File index in the share
    #[arg(long, default_value = "0", help = "File index in the share")]
    file_index: usize,

    /// Piece index in the file
    #[arg(long, default_value = "0", help = "Piece index in the file")]
    piece_index: usize,

    /// Block index in the piece
    #[arg(long, default_value = "0", help = "Block index in the piece")]
    block_index: usize,

    /// Piece offset
    #[arg(long, default_value = "0", help = "Piece offset")]
    piece_offset: usize,

    /// Output file path
    #[arg(short, long, help = "Output file path")]
    output: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> std::result::Result<(), Error> {
    tracing_subscriber::fmt::init();

    // Parse command line arguments
    let args = Args::parse();
    let share_key = args.share_key;
    let file_index = args.file_index;
    let piece_index = args.piece_index;
    let block_index = args.block_index;
    let piece_offset = args.piece_offset;

    // Determine output file path
    let output_path = args.output.unwrap_or_else(|| {
        PathBuf::from(format!(
            "block_f{}_p{}_b{}.bin",
            file_index, piece_index, block_index
        ))
    });

    // Set up Veilid peer
    let state_dir = tempfile::tempdir()?;
    let conn = new_connection(state_dir.path().to_str().unwrap(), None).await?;
    let cancel = CancellationToken::new();
    let retry = Retry::default();

    let mut tasks = JoinSet::new();

    // Set up share resolver
    let (share_resolver, resolver_task) = ShareResolver::new_task(
        cancel.clone(),
        retry.clone(),
        conn.clone(),
        &output_path.clone(),
    );
    tasks.spawn(async move {
        resolver_task.await??;
        Ok(())
    });

    // Parse share key
    let key: RecordKey = share_key.parse()?;

    // Resolve the header and index
    let remote_share = share_resolver.add_share(&key).await?;

    info!(?key, "resolved remote share");

    // Create the block fetcher
    let mut block_fetcher = BlockFetcher::new(
        conn.clone(),
        remote_share.index.clone(),
        output_path.clone(),
    );

    // Create the block fetch request
    let block = FileBlockFetch {
        file_index,
        piece_index,
        block_index,
        piece_offset,
    };

    info!(
        "Fetching block: file_index={}, piece_index={}, block_index={}, piece_offset={}",
        file_index, piece_index, block_index, piece_offset
    );

    let (_, length) = block_fetcher
        .fetch_block(&remote_share.route_id, &block, true)
        .await?;
    info!("Successfully fetched block of length: {} bytes", length);

    info!("Block written to: {}", output_path.display());

    // Clean up
    cancel.cancel();
    tasks.spawn(async { conn.close().await.map_err(|e| e.into()) });
    tasks
        .join_all()
        .await
        .into_iter()
        .collect::<anyhow::Result<()>>()?;
    Ok(())
}
