use std::path::PathBuf;

use sha2::{Digest as _, Sha256};
use stigmerge_fileindex::Index;
use tokio::select;
use tokio_util::sync::CancellationToken;
use veilid_core::{Target, TimestampDuration, ValueSubkeyRangeSet};

use crate::{
    peer::TypedKey,
    proto::{Digest, Encoder, Header},
    Error, Peer, Result,
};

use super::ChanServer;

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
        pieces: roaring::RoaringBitmap,
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
}

impl<P> Service<P>
where
    P: Peer,
{
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
                            // TODO: keep track of have_map per key
                            self.peer.merge_have_map(ch.key, ch.subkeys, todo!()).await?;
                            self.ch.tx.send(todo!()).await.map_err(Error::other)?;
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
            Request::Resolve { key } => todo!(),
            Request::Watch { key } => todo!(),
            Request::Remove { key } => todo!(),
        })
    }
}
