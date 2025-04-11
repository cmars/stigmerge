use std::{collections::HashMap, sync::Arc};

use tokio::{select, sync::RwLock};
use tokio_util::sync::CancellationToken;
use veilid_core::{TimestampDuration, ValueSubkeyRangeSet};

use crate::{chan_rpc::{ChanServer, Service}, peer::TypedKey, piece_map::PieceMap, Error, Peer, Result};

/// Have-map resolver request messages.
pub(super) enum Request {
    /// Resolve the have-map key to get a map of which pieces the remote peer has.
    Resolve { key: TypedKey, subkeys: u16 },

    /// Watch the peer's have-map for changes.
    Watch { key: TypedKey },

    /// Cancel the watch on the peer's have-map.
    CancelWatch { key: TypedKey },
}

impl Request {
    /// Get the have-map key specified in the request.
    fn key(&self) -> &TypedKey {
        match self {
            Request::Resolve { key, subkeys: _ } => key,
            Request::Watch { key } => key,
            Request::CancelWatch { key } => key,
        }
    }
}

/// Have-map resolver response messages.
pub(super) enum Response {
    /// Have-map is not available at the given key, with error cause.
    NotAvailable { key: TypedKey, err: Error },

    /// Have-map response.
    Resolve {
        key: TypedKey,
        pieces: Arc<RwLock<PieceMap>>,
    },

    /// Acknowledge that the have-map at the remote peer key is being monitored
    /// for changes, with an automatically-renewed watch.
    Watching { key: TypedKey },

    /// Acknowledge that the watch on the have-map at remote peer key has been cancelled.
    WatchCancelled { key: TypedKey },
}

/// The have_resolver service handles requests for remote peer have-maps, which
/// indicate what pieces of a share the peer might have.
///
/// This service operates on the have-map reference keys indicated in the main
/// share DHT header (subkey 0) as haveMapRef.
pub(super) struct HaveResolver<P: Peer> {
    peer: P,
    ch: ChanServer<Request, Response>,
    pieces_maps: HashMap<TypedKey, Arc<RwLock<PieceMap>>>,
}

impl<P: Peer> Service for HaveResolver<P> {
    type Request = Request;
    type Response = Response;

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
                        Err(err) => {
                            self.ch.tx.send(Response::NotAvailable { key: req.key().to_owned(), err }).await.map_err(Error::other)?
                        }
                    }
                }
                res = updates.recv() => {
                    let update = res.map_err(Error::other)?;
                    match update {
                        veilid_core::VeilidUpdate::ValueChange(ch) => {
                            let have_map_lock = self.assert_have_map(&ch.key);
                            {
                                let mut have_map = have_map_lock.write().await;
                                self.peer.merge_have_map(ch.key, ch.subkeys, &mut *have_map).await?;
                            }
                            self.ch.tx.send(Response::Resolve{ key: ch.key, pieces: have_map_lock }).await.map_err(Error::other)?;
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

    async fn handle(&mut self, req: &Request) -> Result<Response> {
        Ok(match req {
            Request::Resolve { key, subkeys } => {
                let have_map_lock = self.assert_have_map(key);
                {
                    let mut have_map = have_map_lock.write().await;
                    self.peer
                        .merge_have_map(
                            key.to_owned(),
                            ValueSubkeyRangeSet::single_range(0, (*subkeys - 1).into()),
                            &mut *have_map,
                        )
                        .await?;
                }
                Response::Resolve {
                    key: key.to_owned(),
                    pieces: have_map_lock,
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

impl<P> HaveResolver<P>
where
    P: Peer,
{
    /// Create a new have_resolver service with the given peer.
    pub(super) fn new(peer: P, ch: ChanServer<Request, Response>) -> Self {
        Self {
            peer,
            ch,
            pieces_maps: HashMap::new(),
        }
    }

    /// Get or create a local have-map tracking what the remote peer has.
    ///
    /// Updates are merged with this local copy as it's initially fetched and
    /// then watched for updates.
    fn assert_have_map(&mut self, key: &TypedKey) -> Arc<RwLock<PieceMap>> {
        if let Some(value) = self.pieces_maps.get(key) {
            return value.to_owned();
        }
        let value = Arc::new(RwLock::new(PieceMap::new()));
        self.pieces_maps.insert(key.to_owned(), value.to_owned());
        value
    }
}
