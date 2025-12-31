#![recursion_limit = "256"]

#[macro_use]
pub mod retry;

pub mod block_fetcher;
pub mod content_addressable;
mod error;
pub mod peer_gossip;
pub mod piece_leases;
pub mod piece_map;
pub mod piece_verifier;
pub mod proto;
pub mod record;

pub mod share_announcer;
pub mod share_resolver;
pub mod types;

pub mod fetcher;
pub mod seeder;

pub mod veilid_config;

pub use error::{is_cancelled, is_lagged, is_unrecoverable, CancelError, Error, Result};
pub use retry::Retry;
use veilnet::{connection::veilid, Connection};

#[cfg(test)]
pub mod tests;

pub async fn new_connection(
    state_dir: &str,
    ns: Option<String>,
) -> Result<impl Connection + Clone + Send + Sync> {
    let conn = veilid::connection::Connection::new_config(veilid_config::get_config(
        state_dir.to_string(),
        ns,
    ))
    .await?;
    Ok(conn)
}
