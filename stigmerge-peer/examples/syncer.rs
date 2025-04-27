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

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;

/// Syncer CLI arguments
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

use stigmerge_peer::actor::UntilCancelled;
use stigmerge_peer::share_announcer::{self, ShareAnnouncer};
use tokio::select;
use tokio::spawn;
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::info;

use stigmerge_peer::actor::{ConnectionState, Operator, WithVeilidConnection};
use stigmerge_peer::block_fetcher::BlockFetcher;
use stigmerge_peer::fetcher::{Clients as FetcherClients, Fetcher};
use stigmerge_peer::have_announcer::HaveAnnouncer;
use stigmerge_peer::node::{Node, Veilid};
use stigmerge_peer::piece_verifier::PieceVerifier;
use stigmerge_peer::seeder::{self, Seeder};
use stigmerge_peer::share_resolver::{self, ShareResolver};
use stigmerge_peer::types::ShareInfo;
use stigmerge_peer::Error;
use stigmerge_peer::{new_routing_context, peer_resolver};
use veilid_core::TypedKey;

#[tokio::main]
async fn main() -> std::result::Result<(), Error> {
    tracing_subscriber::fmt::init();

    // Parse command line arguments
    let args = Args::parse();
    let want_index_digest = args.want_index_digest;
    let bootstrap_key = args.share_key;
    let download_dir = args.download_dir;

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
        WithVeilidConnection::new(
            WithVeilidConnection::new(UntilCancelled, node.clone(), conn_state.clone()),
            node.clone(),
            conn_state.clone(),
        ),
    );

    // Parse bootstrap key and want_index_digest
    let bootstrap_key: TypedKey = bootstrap_key.parse()?;
    let want_index_digest = hex::decode(want_index_digest)?;
    let want_index_digest: [u8; 32] = want_index_digest
        .try_into()
        .map_err(|_| Error::msg("Invalid digest length"))?;

    // Resolve the index from the bootstrap peer
    resolve_op
        .send(share_resolver::Request::Index {
            key: bootstrap_key.clone(),
            want_index_digest,
            root: download_dir.clone(),
        })
        .await?;
    let (resolved_header, want_index) = match resolve_op.recv().await {
        Some(share_resolver::Response::Index {
            header,
            index,
            target: _,
            ..
        }) => (header, index),
        Some(share_resolver::Response::BadIndex { .. }) => anyhow::bail!("Bad index"),
        Some(share_resolver::Response::NotAvailable { err_msg, .. }) => anyhow::bail!(err_msg),
        _ => anyhow::bail!("Unexpected response"),
    };

    // Announce our own share of the index
    let mut announce_op = Operator::new(
        cancel.clone(),
        ShareAnnouncer::new(node.clone(), want_index.clone()),
        WithVeilidConnection::new(UntilCancelled, node.clone(), conn_state.clone()),
    );
    announce_op.send(share_announcer::Request::Announce).await?;
    let (share_key, share_header) = match announce_op.recv().await {
        Some(share_announcer::Response::Announce {
            key,
            target: _,
            header,
        }) => (key, header),
        Some(share_announcer::Response::NotAvailable) => anyhow::bail!("failed to announce share"),
        None => todo!(),
    };

    info!("announced: key={share_key}");

    // TODO: get peer_announce actor working -- seems to be broken on misaligned subkeys
    // Announce our relationship to the bootstrap peer
    //let mut peer_announce_op = Operator::new(
    //    cancel.clone(),
    //    peer_announcer::PeerAnnouncer::new(node.clone(), share_key),
    //    WithVeilidConnection::new(UntilCancelled, node.clone(), conn_state.clone()),
    //);
    //peer_announce_op
    //    .send(peer_announcer::Request::Announce {
    //        key: bootstrap_key.clone(),
    //    })
    //    .await?;
    //let resp = peer_announce_op
    //    .recv()
    //    .await
    //    .expect("peer_announcer response");
    //info!("peer_announcer: {:?}", resp);

    // Resolve new peers announced by the bootstrap
    let mut peer_resolver_op = Operator::new(
        cancel.clone(),
        peer_resolver::PeerResolver::new(node.clone()),
        WithVeilidConnection::new(UntilCancelled, node.clone(), conn_state.clone()),
    );
    peer_resolver_op
        .send(peer_resolver::Request::Watch {
            key: bootstrap_key.clone(),
        })
        .await?;
    let resp = peer_resolver_op
        .recv()
        .await
        .expect("peer_resolver response");
    info!("peer_resolver: {:?}", resp);

    // Set up fetcher dependencies
    let block_fetcher = Operator::new(
        cancel.clone(),
        BlockFetcher::new(
            node.clone(),
            Arc::new(RwLock::new(want_index.clone())),
            download_dir.clone(),
            target_rx,
        ),
        WithVeilidConnection::new(UntilCancelled, node.clone(), conn_state.clone()),
    );

    let piece_verifier = PieceVerifier::new(Arc::new(RwLock::new(want_index.clone())));
    let verified_rx = piece_verifier.subscribe_verified();
    let piece_verifier_op = Operator::new(cancel.clone(), piece_verifier, UntilCancelled);

    // Announce our own have-map as we fetch, at our announced share's have-map key
    let have_announcer = Operator::new(
        cancel.clone(),
        HaveAnnouncer::new(node.clone(), share_header.have_map().unwrap().key().clone()),
        WithVeilidConnection::new(UntilCancelled, node.clone(), conn_state.clone()),
    );

    let share = ShareInfo {
        want_index: want_index.clone(),
        root: download_dir.clone(),
        header: resolved_header.clone(),
    };

    let fetcher_clients = FetcherClients {
        block_fetcher,
        piece_verifier: piece_verifier_op,
        have_announcer,
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
        WithVeilidConnection::new(UntilCancelled, node.clone(), conn_state.clone()),
    );

    // Create and run fetcher
    let fetcher = Fetcher::new(share.clone(), fetcher_clients);

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
        res = peer_resolver_op.recv() => {
            if let Some(resp) = res {
                info!("peer_resolver watch: {:?}", resp);
            }
        }
    }

    seeder_op.join().await?.expect("seeder task");
    Ok(())
}
