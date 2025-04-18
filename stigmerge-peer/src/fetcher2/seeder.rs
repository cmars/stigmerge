use std::{collections::HashMap, path::PathBuf};

use stigmerge_fileindex::{Index, BLOCK_SIZE_BYTES, PIECE_SIZE_BLOCKS};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
    select,
    sync::broadcast,
};
use tokio_util::sync::CancellationToken;
use veilid_core::VeilidUpdate;

use crate::{
    piece_map::PieceMap,
    proto::{BlockRequest, Decoder, Request},
    Error, Peer, Result,
};

use super::types::{PieceState, ShareInfo};

pub struct Seeder<P: Peer> {
    peer: P,
    want_index: Index,
    root: PathBuf,
    clients: Clients,
    files: HashMap<u32, File>,
}

impl<P: Peer> Seeder<P> {
    pub fn new(peer: P, share: ShareInfo, clients: Clients) -> Result<Self> {
        Ok(Seeder {
            peer,
            want_index: share.want_index,
            root: share.root,
            clients,
            files: HashMap::new(),
        })
    }

    pub async fn run(&mut self, cancel: CancellationToken) -> Result<()> {
        let mut piece_map = PieceMap::new();
        let mut buf = [0u8; BLOCK_SIZE_BYTES];
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
                                        let rd = self.read_block_into(&block_req, &mut buf).await?;
                                        self.peer.reply_block_contents(veilid_app_call.id(), &buf[..rd]).await?;
                                    } else {
                                        self.peer.reply_block_contents(veilid_app_call.id(), &[]).await?;
                                    }
                                }
                                _ => {}  // Ignore other request types
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    async fn read_block_into(&mut self, block_req: &BlockRequest, buf: &mut [u8]) -> Result<usize> {
        let fh = self.get_file_for_block(&block_req).await?;
        fh.seek(std::io::SeekFrom::Start(
            ((TryInto::<usize>::try_into(block_req.piece).unwrap()
                * PIECE_SIZE_BLOCKS
                * BLOCK_SIZE_BYTES)
                + (TryInto::<usize>::try_into(block_req.block).unwrap() * BLOCK_SIZE_BYTES))
                .try_into()
                .unwrap(),
        ))
        .await?;
        let rd = fh.read(buf).await?;
        Ok(rd)
    }

    async fn get_file_for_block(&mut self, block_req: &BlockRequest) -> Result<&mut File> {
        if !self.files.contains_key(&block_req.piece) {
            // FIXME: does not support multifile; piece -> file mapping should be handled by Index
            let file_path = self.root.join(self.want_index.files()[0].path());
            let fh = File::open(file_path).await?;
            self.files.insert(block_req.piece, fh);
        }
        Ok(self.files.get_mut(&block_req.piece).unwrap())
    }
}

pub struct Clients {
    pub verified_rx: broadcast::Receiver<PieceState>,
    pub update_rx: broadcast::Receiver<VeilidUpdate>,
}
