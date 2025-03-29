use std::{ops::Deref, sync::Arc, time::Duration};

use tokio::{select, sync::RwLock};
use tokio_util::sync::CancellationToken;

use crate::{chan_rpc::ChanServer, have_map::HaveMap, peer::TypedKey, Error, Peer, Result};

pub(super) enum Request {
    Set { piece_index: u32 },
    Clear { piece_index: u32 },
    Reset,
}

pub type Response = Result<()>;

pub(super) struct Service<P: Peer> {
    peer: P,
    key: TypedKey,
    ch: ChanServer<Request, Response>,
    pieces_map: Arc<RwLock<HaveMap>>,
}

impl<P: Peer> Service<P> {
    pub(super) fn new(peer: P, key: TypedKey, ch: ChanServer<Request, Response>) -> Self {
        Self {
            peer,
            key,
            ch,
            pieces_map: Arc::new(RwLock::new(HaveMap::new())),
        }
    }

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
