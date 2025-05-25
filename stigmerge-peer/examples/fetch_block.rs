//! Example: fetch a single block from a share
#![recursion_limit = "256"]

use std::path::PathBuf;
use std::sync::Arc;

use backoff::backoff::Backoff;
use clap::Parser;

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

use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use tokio::sync::Mutex;
use tokio::try_join;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use stigmerge_peer::actor::{ConnectionState, Operator, ResponseChannel, WithVeilidConnection};
use stigmerge_peer::block_fetcher::{self, BlockFetcher};
use stigmerge_peer::new_routing_context;
use stigmerge_peer::node::Veilid;
use stigmerge_peer::share_resolver::{self, ShareResolver};
use stigmerge_peer::types::FileBlockFetch;
use stigmerge_peer::Error;
use veilid_core::TypedKey;

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

    let state_dir = tempfile::tempdir()?;

    // Set up Veilid node
    let (routing_context, update_rx) =
        new_routing_context(state_dir.path().to_str().unwrap(), None).await?;
    let node = Veilid::new(routing_context, update_rx).await?;

    let cancel = CancellationToken::new();
    let conn_state = Arc::new(Mutex::new(ConnectionState::new()));

    // Set up share resolver
    let share_resolver = ShareResolver::new(node.clone());
    let mut share_resolver_op = Operator::new(
        cancel.clone(),
        share_resolver,
        WithVeilidConnection::new(node.clone(), conn_state.clone()),
    );

    // Parse share key
    let key: TypedKey = share_key.parse()?;

    // Create a temporary directory for the block fetcher
    let temp_dir = tempfile::tempdir()?;
    let download_dir = temp_dir.path().to_path_buf();

    // Resolve the header and index
    let (_header, index, target) = match share_resolver_op
        .call(share_resolver::Request::Index {
            response_tx: ResponseChannel::default(),
            key: key.clone(),
            want_index_digest: None, // We don't verify the index digest
            root: download_dir.clone(),
        })
        .await?
    {
        share_resolver::Response::Index {
            header,
            index,
            target,
            ..
        } => (header, index, target),
        share_resolver::Response::BadIndex { .. } => anyhow::bail!("Bad index"),
        share_resolver::Response::NotAvailable { err_msg, .. } => anyhow::bail!(err_msg),
        _ => anyhow::bail!("Unexpected response"),
    };

    info!("Resolved header and index for share key: {}", key);

    // Create the block fetcher
    let want_index = Arc::new(tokio::sync::RwLock::new(index));
    let mut block_fetcher_op = Operator::new(
        cancel.clone(),
        BlockFetcher::new(node.clone(), want_index.clone(), download_dir),
        WithVeilidConnection::new(node.clone(), conn_state.clone()),
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

    let mut fetch_backoff = backoff::ExponentialBackoff::default();
    loop {
        // Fetch the block
        let response = block_fetcher_op
            .call(block_fetcher::Request::Fetch {
                response_tx: ResponseChannel::default(),
                share_key: key.clone(),
                target,
                block: block.clone(),
                flush: true,
            })
            .await?;

        // Process the response
        match response {
            block_fetcher::Response::Fetched { length, .. } => {
                info!("Successfully fetched block of length: {} bytes", length);

                // Get the path to the fetched block
                let index_guard = want_index.read().await;
                let file_path = index_guard.files()[file_index].path().to_path_buf(); // Clone the path
                drop(index_guard); // Release the lock
                let fetched_file_path = temp_dir.path().join(file_path);

                // Read the fetched block and write it to the output file
                let mut fetched_file = File::open(&fetched_file_path).await?;
                let mut output_file = File::create(&output_path).await?;

                // Copy the block data to the output file
                let mut buffer = vec![0u8; length];
                tokio::io::AsyncReadExt::read_exact(&mut fetched_file, &mut buffer).await?;
                output_file.write_all(&buffer).await?;
                output_file.flush().await?;

                info!("Block written to: {}", output_path.display());
                break;
            }
            block_fetcher::Response::FetchFailed { err, .. } => {
                let delay = fetch_backoff
                    .next_backoff()
                    .ok_or(anyhow::anyhow!("out of retries"))?;
                warn!("Failed to fetch block: {}, delaying {:?}", err, delay);
                tokio::time::sleep(delay).await;
            }
        }
    }

    // Clean up
    cancel.cancel();
    try_join!(share_resolver_op.join(), block_fetcher_op.join())?;

    Ok(())
}
