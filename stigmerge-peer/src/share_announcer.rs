use stigmerge_fileindex::Index;
use tokio::select;
use tokio_util::sync::CancellationToken;
use veilid_core::Target;

use crate::{
    chan_rpc::{ChanServer, Service},
    peer::TypedKey,
    proto::Header,
    Error, Peer, Result,
};

pub enum Request {
    Announce,
}

pub enum Response {
    NotAvailable,
    Announce {
        key: TypedKey,
        target: Target,
        header: Header,
    },
}

pub struct ShareAnnouncer<P: Peer> {
    peer: P,
    ch: ChanServer<Request, Response>,
    index: Index,

    share: Option<ShareAnnounce>,
}

struct ShareAnnounce {
    key: TypedKey,
    target: Target,
    header: Header,
}

impl<P: Peer> Service for ShareAnnouncer<P> {
    type Request = Request;

    type Response = Response;

    async fn run(mut self, cancel: CancellationToken) -> crate::Result<()> {
        loop {
            match self.share {
                None => {
                    self.announce().await?;
                }
                Some(ShareAnnounce { .. }) => {
                    select! {
                        _ = cancel.cancelled() => {
                            return Ok(());
                        }
                        res = self.ch.rx.recv() => {
                            if let Some(req) = res {
                                let resp = self.handle(&req).await?;
                                self.ch.tx.send(resp).await.map_err(Error::other)?;
                            }
                        }
                    }
                }
            }
        }
    }

    async fn handle(&mut self, req: &Self::Request) -> Result<Self::Response> {
        match req {
            Request::Announce => {
                if let Some(ShareAnnounce {
                    key,
                    target,
                    header,
                }) = &mut self.share
                {
                    let (updated_target, updated_header) = self
                        .peer
                        .reannounce_route(key, Some(*target), &self.index, header)
                        .await?;
                    *target = updated_target;
                    *header = updated_header;
                    return Ok(Response::Announce {
                        key: *key,
                        target: *target,
                        header: header.clone(),
                    });
                }
            }
        }
        Ok(Response::NotAvailable)
    }
}

impl<P: Peer> ShareAnnouncer<P> {
    pub fn new(peer: P, ch: ChanServer<Request, Response>, index: Index) -> ShareAnnouncer<P> {
        ShareAnnouncer {
            peer,
            ch,
            index,
            share: None,
        }
    }

    async fn announce(&mut self) -> Result<()> {
        let (key, target, header) = self.peer.announce_index(&self.index).await?;
        self.share = Some(ShareAnnounce {
            key: key.clone(),
            target: target.clone(),
            header: header.clone(),
        });
        self.ch
            .tx
            .send(Response::Announce {
                key,
                target,
                header,
            })
            .await
            .map_err(Error::other)?;
        Ok(())
    }
}
