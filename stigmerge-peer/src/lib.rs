#![recursion_limit = "256"]

pub mod actor;
pub mod block_fetcher;
pub mod content_addressable;
mod error;
pub mod have_announcer;
pub mod have_resolver;
pub mod node;
pub mod peer_gossip;
mod peer_tracker;
mod piece_map;
pub mod piece_verifier;
pub mod proto;
pub mod share_announcer;
pub mod share_resolver;
pub mod types;

pub mod fetcher;
pub mod seeder;

pub mod veilid_config;

use std::sync::Arc;

use tokio::sync::broadcast;
use tracing::warn;
use veilid_core::{RoutingContext, VeilidUpdate};

pub use error::{is_cancelled, is_unrecoverable, CancelError, Error, Result};
pub use node::{Node, Veilid};

#[cfg(test)]
pub mod tests;

pub async fn new_routing_context(
    state_dir: &str,
    ns: Option<String>,
) -> Result<(RoutingContext, broadcast::Receiver<VeilidUpdate>)> {
    let (update_tx, update_rx) = broadcast::channel(1024);

    // Configure Veilid core
    let update_callback = Arc::new(move |change: VeilidUpdate| {
        if let Err(e) = update_tx.send(change) {
            warn!("dispatching veilid update: {:?}", e);
        }
    });
    let config_state_path = Arc::new(state_dir.to_owned());
    let config_ns = Arc::new(ns.to_owned());
    let config_callback = Arc::new(move |key| {
        veilid_config::callback((*config_state_path).to_owned(), (*config_ns).clone(), key)
    });

    // Start Veilid API
    let api: veilid_core::VeilidAPI =
        veilid_core::api_startup(update_callback, config_callback).await?;
    api.attach().await?;

    let routing_context = api.routing_context()?;
    Ok((routing_context, update_rx))
}
