use stigmerge_fileindex::Index;
use tokio::{select, sync::broadcast};
use tokio_util::sync::CancellationToken;
use veilid_core::VeilidUpdate;

use crate::{chan_rpc::ChanClient, peer_resolver, piece_map::PieceMap, proto::{Decoder, Request}, Error, Peer, Result};

use super::types::PieceState;

pub struct Seeder<P: Peer> {
    peer: P,
    want_index: Index,
    clients: Clients,
}

impl<P: Peer> Seeder<P> {
    pub fn new(peer: P, want_index: Index, clients: Clients) -> Result<Self> {
        Ok(Seeder {
            peer,
            want_index,
            clients,
        })
    }

    pub async fn run(&mut self, cancel: CancellationToken) -> Result<()> {
        let mut piece_map = PieceMap::new();
        loop {
            select! {
                _ = cancel.cancelled() => {
                    return Ok(());
                }
                res = self.clients.verified_rx.recv() => {
                    let piece_state = res.map_err(Error::other)?;
                    piece_map.set(piece_state.piece_index.try_into().unwrap());
                }
                res = self.clients.update_rx.recv() => {
                    let update = res.map_err(Error::other)?;
                    match update {
                        VeilidUpdate::AppCall(veilid_app_call) => {
                            let req = Request::decode(veilid_app_call.message()).map_err(Error::RemoteProtocol)?;
                            match req {
                                Request::BlockRequest(block_req) => {
                                    if piece_map.get(block_req.piece) {
                                        let contents = &[];
                                        self.peer.reply_block_contents(veilid_app_call.id(), contents).await?;
                                    } else {
                                        // TODO: nack somehow
                                    }
                                }
                                _ => {}  // Ignore other request types
                            }
                        }
                        VeilidUpdate::Shutdown => todo!(),
                        _ => {}
                    }
                }
            }
        }
    }
}

pub struct Clients {
    pub verified_rx: broadcast::Receiver<PieceState>,
    pub update_rx: broadcast::Receiver<VeilidUpdate>,
    pub peer_resolver: ChanClient<peer_resolver::Request, peer_resolver::Response>,
}