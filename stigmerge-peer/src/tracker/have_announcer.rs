use std::{ops::Deref, sync::Arc, time::Duration};

use tokio::{select, sync::RwLock};
use tokio_util::sync::CancellationToken;

use crate::{chan_rpc::ChanServer, peer::TypedKey, piece_map::PieceMap, Error, Peer, Result};

/// Have-map announcer request messages.
pub(super) enum Request {
    /// Announce that this peer has a given piece.
    Set { piece_index: u32 },

    /// Announce that this peer does not have a given piece.
    Clear { piece_index: u32 },

    /// Announce that this peer has no pieces.
    Reset,
}

/// Have-map announcer response message, just an acknowledgement or error.
pub type Response = Result<()>;

/// The have-announcer service handles requests for announcing the share pieces
/// verified by this peer. The have map is written to the DHT at a have-map
/// reference key, as an uncompressed bitmap of contiguous bits, indexed by
/// piece index: a set bit (1) indicating the peer has the piece, a clear bit
/// (0) indicating the peer does not have the piece.
pub(super) struct Service<P: Peer> {
    peer: P,
    key: TypedKey,
    ch: ChanServer<Request, Response>,
    pieces_map: Arc<RwLock<PieceMap>>,
}

impl<P: Peer> Service<P> {
    /// Create a new have_announcer service.
    pub(super) fn new(peer: P, key: TypedKey, ch: ChanServer<Request, Response>) -> Self {
        Self {
            peer,
            key,
            ch,
            pieces_map: Arc::new(RwLock::new(PieceMap::new())),
        }
    }

    /// Run the service until cancelled.
    pub async fn run(mut self, cancel: CancellationToken) -> Result<()> {
        let mut changed = false;
        let mut interval = tokio::time::interval(Duration::from_secs(60));
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
                    let resp = self.handle(&req).await;
                    changed = true;
                    self.ch.tx.send(resp).await.map_err(Error::other)?;
                }
                _ = interval.tick() => {
                    if changed {
                        let have_map = self.pieces_map.read().await;
                        self.peer.announce_have_map(self.key.to_owned(), have_map.deref()).await?;
                        changed = false;
                    }
                }
            }
        }
    }

    /// Handle a have_announcer request, provide a response.
    async fn handle(&mut self, req: &Request) -> Result<()> {
        let mut pieces_map = self.pieces_map.write().await;
        Ok(match req {
            Request::Set { piece_index } => {
                pieces_map.set(*piece_index);
            }
            Request::Clear { piece_index } => {
                pieces_map.clear(*piece_index);
            }
            Request::Reset => {
                pieces_map.reset();
            }
        })
    }
}
