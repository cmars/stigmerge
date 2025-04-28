//! Example: announce peers for a file and resolve their peers

use std::path::PathBuf;
use std::str::FromStr;

use clap::Parser;

/// Peer announce CLI arguments
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the file to announce
    #[arg(help = "Path to the file to announce")]
    file: PathBuf,

    /// List of peer share keys to announce
    #[arg(help = "List of peer share keys to announce")]
    peer_keys: Vec<String>,
}

use stigmerge_peer::share_resolver::{self, ShareResolver};
use tokio::select;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use veilid_core::TypedKey;

use stigmerge_fileindex::Indexer;
use stigmerge_peer::actor::{ConnectionState, Operator, UntilCancelled, WithVeilidConnection};
use stigmerge_peer::node::Veilid;
use stigmerge_peer::peer_announcer::{self, PeerAnnouncer};
use stigmerge_peer::share_announcer::{self, ShareAnnouncer};
use stigmerge_peer::Error;
use stigmerge_peer::{new_routing_context, peer_resolver};

#[tokio::main]
async fn main() -> std::result::Result<(), Error> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let file = args.file;

    // Index the file
    let indexer = Indexer::from_file(file.clone()).await?;
    let index = indexer.index().await?;

    let state_dir = tempfile::tempdir()?;

    // Set up Veilid peer
    let (routing_context, update_tx, _) =
        new_routing_context(state_dir.path().to_str().unwrap(), None).await?;
    let node = Veilid::new(routing_context, update_tx).await?;

    let cancel = CancellationToken::new();
    let conn_state = std::sync::Arc::new(Mutex::new(ConnectionState::new()));

    // For resolving discovered peers
    let mut share_resolver_op = Operator::new(
        cancel.clone(),
        ShareResolver::new(node.clone()),
        WithVeilidConnection::new(UntilCancelled, node.clone(), conn_state.clone()),
    );

    // First announce our share to get our own share key
    let mut share_announcer_op = Operator::new(
        cancel.clone(),
        ShareAnnouncer::new(node.clone(), index.clone()),
        WithVeilidConnection::new(UntilCancelled, node.clone(), conn_state.clone()),
    );
    share_announcer_op
        .send(share_announcer::Request::Announce)
        .await?;
    let resp = share_announcer_op.recv().await;
    let (share_key, target, header) = match resp {
        Some(share_announcer::Response::Announce {
            key,
            target,
            header,
        }) => (key, target, header),
        _ => anyhow::bail!("Announce failed"),
    };
    info!("announced share: key={share_key}, target={target:?}");

    // Create peer announcer for our share
    let mut peer_announcer_op = Operator::new(
        cancel.clone(),
        PeerAnnouncer::new(node.clone(), &header.payload_digest()),
        WithVeilidConnection::new(UntilCancelled, node.clone(), conn_state.clone()),
    );

    // Create peer resolver to watch other shares
    let mut peer_resolver_op = Operator::new(
        cancel.clone(),
        peer_resolver::PeerResolver::new(node.clone()),
        WithVeilidConnection::new(UntilCancelled, node.clone(), conn_state.clone()),
    );

    // Announce initial peer keys
    for peer_share_key_str in args.peer_keys {
        let peer_share_key = TypedKey::from_str(&peer_share_key_str)
            .map_err(|e| anyhow::anyhow!("invalid peer share key {}: {}", peer_share_key_str, e))?;

        peer_announcer_op
            .send(peer_announcer::Request::Announce {
                key: peer_share_key.clone(),
            })
            .await?;

        match peer_announcer_op.recv().await {
            Some(peer_announcer::Response::Ok) => {
                info!("announce peer {peer_share_key}: ok");
            }
            Some(peer_announcer::Response::Err { err_msg }) => {
                info!("announce peer {peer_share_key}: {err_msg}");
                continue;
            }
            other => {
                anyhow::bail!("announce peer {peer_share_key}: unexpected response: {other:?}")
            }
        }

        peer_resolver_op
            .send(peer_resolver::Request::Watch {
                key: peer_share_key.clone(),
            })
            .await?;

        // Resolve the peer's share
        share_resolver_op
            .send(share_resolver::Request::Header {
                key: peer_share_key,
                prior_target: None,
            })
            .await?;
    }

    loop {
        select! {
                _ = cancel.cancelled() => {
                    break;
                }
                res = share_resolver_op.recv() => {
                    match res {
                        Some(share_resolver::Response::Header{ key, header: _, target: _ }) => {
                            peer_announcer_op.send(peer_announcer::Request::Announce { key: key.clone() }).await?;
                            match peer_announcer_op.recv().await {
                                Some(peer_announcer::Response::Ok) => {
                                    info!("announce peer: {key}: ok");
                                }
                                Some(peer_announcer::Response::Err {err_msg}) => {
                                    warn!("announce peer: {key}: {err_msg}");
                                }
                                None => todo!(),
                            }
                        }
                        other => {
                            warn!("share_resolver: {:?}", other);
                        }
                    }
                }
                res = peer_resolver_op.recv() => {
                    match res {
                        Some(peer_resolver::Response::Resolve{ key, peers }) => {
                            for (peer_key, info) in peers.iter() {
                                info!("peer resolver: {key} updated with {peer_key} {info:?}");
                                peer_announcer_op.send(peer_announcer::Request::Announce {key: peer_key.to_owned()}).await?;
                                match peer_announcer_op.recv().await {
                                    Some(peer_announcer::Response::Ok) => {
                                        info!("announce peer: {key}: ok");
                                    }
                                    Some(peer_announcer::Response::Err {err_msg}) => {
                                        warn!("announce peer: {key}: {err_msg}");
                                    }
                                    None => todo!(),
                                }
                            }
                        }
                        other => {
                            warn!("peer_resolver: {:?}", other);
                        }
                    }
                }
            _ = tokio::signal::ctrl_c() => {
                cancel.cancel();
            }
        }
    }

    share_announcer_op
        .join()
        .await
        .expect("announce task")
        .expect("announce run");

    peer_announcer_op
        .join()
        .await
        .expect("peer announcer task")
        .expect("peer announcer run");

    Ok(())
}
