use std::{collections::HashMap, sync::Arc};

use tokio::{select, sync::RwLock};
use tokio_util::sync::CancellationToken;
use veilid_core::{TimestampDuration, ValueSubkeyRangeSet};

use crate::{chan_rpc::ChanServer, have_map::HaveMap, peer::TypedKey, Error, Peer, Result};

pub(super) enum Request {
    Resolve { key: TypedKey },
    Watch { key: TypedKey },
    Remove { key: TypedKey },
}

impl Request {
    fn key(&self) -> &TypedKey {
        match self {
            Request::Resolve { key } => key,
            Request::Watch { key } => key,
            Request::Remove { key } => key,
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
        pieces: Arc<RwLock<HaveMap>>,
    },
    Watching {
        key: TypedKey,
    },
    Removed {
        key: TypedKey,
    },
}

pub(super) struct Service<P: Peer> {
    peer: P,
    ch: ChanServer<Request, Response>,
    pieces_maps: HashMap<TypedKey, Arc<RwLock<HaveMap>>>,
}

impl<P> Service<P>
where
    P: Peer,
{
    pub(super) fn new(peer: P, ch: ChanServer<Request, Response>) -> Self {
        Self {
            peer,
            ch,
            pieces_maps: HashMap::new(),
        }
    }

    pub async fn run(mut self, cancel: CancellationToken) -> Result<()> {
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

    fn assert_have_map(&mut self, key: &TypedKey) -> Arc<RwLock<HaveMap>> {
        if let Some(value) = self.pieces_maps.get(key) {
            return value.to_owned();
        }
        let value = Arc::new(RwLock::new(HaveMap::new()));
        self.pieces_maps.insert(key.to_owned(), value.to_owned());
        value
    }

    async fn handle(&mut self, req: &Request) -> Result<Response> {
        Ok(match req {
            Request::Resolve { key } => {
                let have_map_lock = self.assert_have_map(key);
                {
                    let mut have_map = have_map_lock.write().await;
                    self.peer
                        .merge_have_map(key.to_owned(), ValueSubkeyRangeSet::full(), &mut *have_map)
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
            Request::Remove { key } => {
                self.peer.cancel_watch(key);
                Response::Removed {
                    key: key.to_owned(),
                }
            }
        })
    }
}
