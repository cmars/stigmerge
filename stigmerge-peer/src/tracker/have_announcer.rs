use tokio::select;
use tokio_util::sync::CancellationToken;

use crate::{peer::TypedKey, Error, Peer, Result};

use super::ChanServer;

pub(super) enum Request {
    Set { key: TypedKey, piece_index: u32 },
    Clear { key: TypedKey, piece_index: u32 },
    ClearAll { key: TypedKey },
}

impl Request {
    fn key(&self) -> &TypedKey {
        match self {
            Request::Set {
                key,
                piece_index: _,
            } => key,
            Request::Clear {
                key,
                piece_index: _,
            } => key,
            Request::ClearAll { key } => key,
        }
    }
}

pub type Response = Result<()>;

pub(super) struct Service<P: Peer> {
    peer: P,
    ch: ChanServer<Request, Response>,
}

impl<P: Peer> Service<P> {
    pub(super) fn new(peer: P, ch: ChanServer<Request, Response>) -> Self {
        Self { peer, ch }
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
                    let resp = self.handle(&req).await;
                    self.ch.tx.send(resp).await.map_err(Error::other)?;
                }
            }
        }
    }

    async fn handle(&mut self, req: &Request) -> Result<()> {
        Ok(match req {
            Request::Set { key, piece_index } => {
                todo!();
            }
            Request::Clear { key, piece_index } => {
                todo!();
            }
            Request::ClearAll { key } => {
                todo!();
            }
        })
    }
}
