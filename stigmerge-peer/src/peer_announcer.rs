use std::collections::HashMap;

use tokio::select;
use tokio_util::sync::CancellationToken;

use crate::{chan_rpc::{ChanServer, Service}, peer::TypedKey, Error, Peer, Result};

/// Peer-map announcer request messages.
pub(super) enum Request {
    /// Announce a known remote peer in good standing.
    Announce { key: TypedKey },

    /// Redact a known peer, it may be unavailable or defective
    /// from the point of view of this peer.
    Redact { key: TypedKey },

    /// Clear all peer announcements.
    Reset,
}

/// Peer-map announcer response message, just an acknowledgement or error.
pub type Response = Result<()>;

/// The peer-announcer service handles requests to announce or redact remote
/// peers known by this peer.
///
/// Each remote peer is encoded to a separate subkey. Redacted peers are
/// marked with empty contents.
pub(super) struct PeerAnnouncer<P: Peer> {
    peer: P,
    key: TypedKey,
    ch: ChanServer<Request, Response>,
    peer_indexes: HashMap<TypedKey, usize>,
    peers: Vec<Option<TypedKey>>,
}

impl<P: Peer> Service for PeerAnnouncer<P> {
    type Request = Request;
    type Response = Response;

    async fn run(mut self, cancel: CancellationToken) -> Result<()> {
        loop {
            select! {
                _ = cancel.cancelled() => {
                    return Ok(())
                }
                res = self.ch.rx.recv() => {
                    let req = match res {
                        None => return Ok(()),
                        Some(req) => req,
                    };
                    let resp = self.handle(&req).await?;
                    self.ch.tx.send(resp).await.map_err(Error::other)?;
                }
            }
        }
    }

    async fn handle(&mut self, req: &Self::Request) -> Result<Self::Response> {
        Ok(match req {
            Request::Announce { key } => {
                if !self.peer_indexes.contains_key(key) {
                    let index = self.assign_peer_index(*key);
                    self.peer
                        .announce_peer(self.key.to_owned(), Some(*key), index)
                        .await?;
                }
                Ok(())
            }
            Request::Redact { key } => {
                if let Some(index) = self.peer_indexes.get(key) {
                    let subkey = TryInto::<u16>::try_into(*index).unwrap();
                    self.peer
                        .announce_peer(self.key.to_owned(), None, subkey)
                        .await?;
                    self.peers[*index] = None;
                    self.peer_indexes.remove(key);
                }
                Ok(())
            }
            Request::Reset => {
                let max_subkey: u16 = (self.peers.len() - 1).try_into().unwrap();
                self.peer
                    .reset_peers(self.key.to_owned(), max_subkey)
                    .await?;
                self.peers.clear();
                self.peer_indexes.clear();
                Ok(())
            }
        })
    }
}

impl<P: Peer> PeerAnnouncer<P> {
    /// Create a new peer_announcer service.
    pub(super) fn new(peer: P, key: TypedKey, ch: ChanServer<Request, Response>) -> Self {
        Self {
            peer,
            key,
            ch,
            peer_indexes: HashMap::new(),
            peers: vec![],
        }
    }

    fn assign_peer_index(&mut self, key: TypedKey) -> u16 {
        for (i, maybe_key) in self.peers.iter_mut().enumerate() {
            if let None = maybe_key {
                *maybe_key = Some(key);
                return i.try_into().unwrap();
            }
        }
        self.peers.push(Some(key));
        (self.peers.len() - 1).try_into().unwrap()
    }
}
