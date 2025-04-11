use std::{collections::HashMap, sync::Arc};

use tokio::{select, sync::RwLock};
use tokio_util::sync::CancellationToken;
use veilid_core::{TimestampDuration, ValueSubkeyRangeSet};

use crate::{
    chan_rpc::{ChanServer, Service},
    peer::TypedKey,
    proto::{Decoder, PeerInfo},
    Error, Peer, Result,
};

pub(super) enum Request {
    /// Resolve the peer information stored at the given peer map key. This will
    /// result in a response containing the peer info, or a not available
    /// indicator.
    ///
    /// The peer map key and subkeys come from the main share header.
    Resolve { key: TypedKey, subkeys: u16 },

    /// Watch for peer updates at the given peer map key. This will result in a
    /// series of peer info responses being sent as the peers change.
    Watch { key: TypedKey },

    /// Cancel the watch on the peer map key.
    CancelWatch { key: TypedKey },
}

impl Request {
    fn key(&self) -> &TypedKey {
        match self {
            Request::Resolve { key, subkeys: _ } => key,
            Request::Watch { key } => key,
            Request::CancelWatch { key } => key,
        }
    }
}

pub(super) enum Response {
    NotAvailable {
        key: TypedKey,
        err: Error,
    },
    Resolve {
        key: TypedKey,
        peers: HashMap<TypedKey, PeerInfo>,
    },
    Watching {
        key: TypedKey,
    },
    WatchCancelled {
        key: TypedKey,
    },
}

/// The peer_resolver service handles requests for remote peer maps, which
/// indicate which other peers a remote peer knows about.
pub(super) struct PeerResolver<P: Peer> {
    peer: P,
    ch: ChanServer<Request, Response>,
    peer_maps: HashMap<TypedKey, Arc<RwLock<HashMap<TypedKey, PeerInfo>>>>,
}

impl<P: Peer> Service for PeerResolver<P> {
    type Request = Request;
    type Response = Response;

    /// Run the service until cancelled.
    async fn run(mut self, cancel: CancellationToken) -> Result<()> {
        let mut updates = self.peer.subscribe_veilid_update();
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
                    match self.handle(&req).await {
                        Ok(resp) => {
                            self.ch.tx.send(resp).await.map_err(Error::other)?
                        }
                        Err(err) => self.ch.tx.send(Response::NotAvailable{key: req.key().clone(), err}).await.map_err(Error::other)?,
                    }
                }
                res = updates.recv() => {
                    let update = res.map_err(Error::other)?;
                    match update {
                        veilid_core::VeilidUpdate::ValueChange(ch) => {
                            if let Some(data) = ch.value {
                                if let Ok(peer_info) = PeerInfo::decode(data.data()) {
                                    self.ch.tx.send(Response::Resolve{
                                        key: ch.key,
                                        peers: [(peer_info.key().to_owned(), peer_info)].into_iter().collect::<HashMap<_,_>>()
                                    }).await.map_err(Error::other)?;
                                }
                            }
                            todo!();
                        }
                        veilid_core::VeilidUpdate::Shutdown => {
                            return Ok(());
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    /// Handle a peer_resolver request, provide a response.
    async fn handle(&mut self, req: &Request) -> Result<Response> {
        Ok(match req {
            Request::Resolve { key, subkeys } => {
                let mut result = HashMap::new();
                for subkey in 0u16..*subkeys {
                    if let Ok(peer_info) = self.peer.resolve_peer_info(key.to_owned(), subkey).await
                    {
                        result.insert(peer_info.key().clone(), peer_info);
                    }
                }
                Response::Resolve {
                    key: key.to_owned(),
                    peers: result,
                }
            }
            Request::Watch { key } => {
                self.peer
                    .watch(
                        key.to_owned(),
                        ValueSubkeyRangeSet::full(),
                        TimestampDuration::new_secs(60),
                    )
                    .await?;
                Response::Watching {
                    key: key.to_owned(),
                }
            }
            Request::CancelWatch { key } => {
                self.peer.cancel_watch(key);
                Response::WatchCancelled {
                    key: key.to_owned(),
                }
            }
        })
    }
}

impl<P> PeerResolver<P>
where
    P: Peer,
{
    /// Create a new peer_resolver service.
    pub(super) fn new(peer: P, ch: ChanServer<Request, Response>) -> Self {
        Self {
            peer,
            ch,
            peer_maps: HashMap::new(),
        }
    }

}
