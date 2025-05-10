//! Example: announce peers for a file and resolve their peers
#![recursion_limit = "256"]

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

use stigmerge_peer::content_addressable::ContentAddressable;
use stigmerge_peer::proto::{AdvertisePeerRequest, Decoder};
use stigmerge_peer::share_resolver::{self, ShareResolver};
use tokio::select;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use veilid_core::{TypedKey, VeilidUpdate};

use stigmerge_fileindex::Indexer;
use stigmerge_peer::actor::{ConnectionState, Operator, WithVeilidConnection};
use stigmerge_peer::node::Veilid;
use stigmerge_peer::peer_announcer::{self, PeerAnnouncer};
use stigmerge_peer::share_announcer::{self, ShareAnnouncer};
use stigmerge_peer::{new_routing_context, peer_resolver};
use stigmerge_peer::{proto, Error, Node};

#[tokio::main]
async fn main() -> std::result::Result<(), Error> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let file = args.file;

    // Index the file
    let indexer = Indexer::from_file(file.as_path()).await?;
    let mut index = indexer.index().await?;
    let want_index_digest = index.digest()?;

    let state_dir = tempfile::tempdir()?;

    // Set up Veilid peer
    let (routing_context, update_tx, _) =
        new_routing_context(state_dir.path().to_str().unwrap(), None).await?;
    let mut node = Veilid::new(routing_context, update_tx).await?;

    let cancel = CancellationToken::new();
    let conn_state = std::sync::Arc::new(Mutex::new(ConnectionState::new()));

    // For resolving discovered peers
    let mut share_resolver_op = Operator::new(
        cancel.clone(),
        ShareResolver::new(node.clone()),
        WithVeilidConnection::new(node.clone(), conn_state.clone()),
    );

    // First announce our share to get our own share key
    let mut share_announcer_op = Operator::new(
        cancel.clone(),
        ShareAnnouncer::new(node.clone(), index.clone()),
        WithVeilidConnection::new(node.clone(), conn_state.clone()),
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
        WithVeilidConnection::new(node.clone(), conn_state.clone()),
    );

    // Create peer resolver to watch other shares
    let mut peer_resolver_op = Operator::new(
        cancel.clone(),
        peer_resolver::PeerResolver::new(node.clone()),
        WithVeilidConnection::new(node.clone(), conn_state.clone()),
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

    let mut update_rx = node.subscribe_veilid_update();

    let (advertise_tx, advertise_rx) = flume::unbounded();
    const MAX_ADVERTISE_ATTEMPTS: u8 = 3;

    loop {
        select! {
                _ = cancel.cancelled() => {
                    break;
                }
                res = share_resolver_op.recv() => {
                    match res {
                        Some(share_resolver::Response::Header{ key, header: _, target }) => {
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
                            advertise_tx.send_async((target, key, 0)).await?;
                        }
                        Some(share_resolver::Response::Index{ key, header: _, index: _, target }) => {
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
                            advertise_tx.send_async((target, key, 0)).await?;
                        }
                        other => {
                            warn!("share_resolver: {:?}", other);
                        }
                    }
                }
                res = advertise_rx.recv_async() => {
                    let (target, key, attempt) = res?;
                    match node.request_advertise_peer(&target, &share_key).await {
                        Ok(()) => info!("advertised our share {share_key} to {key}"),
                        Err(e) => {
                            warn!("failed to advertise to {key}: {} (attempt {attempt})", e.to_string());
                            if attempt < MAX_ADVERTISE_ATTEMPTS {
                                advertise_tx.send_async((target, key, attempt+1)).await?;
                            }
                        }
                    };
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
                res = update_rx.recv() => {
                    let update = res?;
                    match update {
                        VeilidUpdate::AppMessage(app_msg) => {
                            let req = proto::Request::decode(app_msg.message())?;
                            match req {
                                proto::Request::AdvertisePeer(AdvertisePeerRequest{ key }) => {
                                    info!("received advertise request from {key}");
                                    share_resolver_op.send(share_resolver::Request::Index {
                                        key, want_index_digest: Some(want_index_digest), root: index.root().to_path_buf(),
                                    }).await?;
                                }
                                _ => {}  // Ignore other request types
                            }

                        }
                        VeilidUpdate::Shutdown => {
                            cancel.cancel();
                        }
                        _ => {}
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
