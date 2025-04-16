use std::cmp::{min, Ordering};
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use flume::{unbounded, Receiver, Sender};
use sha2::{Digest, Sha256};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::select;
use tokio::sync::{watch, RwLock};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument, trace, warn};
use veilid_core::{Target, TypedKey};

use stigmerge_fileindex::{Index, BLOCK_SIZE_BYTES, PIECE_SIZE_BLOCKS, PIECE_SIZE_BYTES};

use crate::error::Unexpected;
use crate::peer::{with_backoff_reset, with_backoff_retry, Peer};
use crate::proto::Header;
use crate::{reset_with_backoff, Error, Result};

const N_FETCHERS: u8 = 20;

pub struct Fetcher<P: Peer> {
    peer: P,
    share_key: TypedKey,
    route: Arc<RwLock<(Target, Header)>>,
    have_index: Index,
    want_index: Index,
    fetch_progress_tx: watch::Sender<Progress>,
    verify_progress_tx: watch::Sender<Progress>,
}

#[derive(Clone)]
pub struct Progress {
    pub length: u64,
    pub position: u64,
}

impl Default for Progress {
    fn default() -> Self {
        Progress {
            length: 0u64,
            position: 0u64,
        }
    }
}

impl<P: Peer> Clone for Fetcher<P> {
    fn clone(&self) -> Self {
        Self {
            peer: self.peer.clone(),
            share_key: self.share_key.clone(),
            route: Arc::clone(&self.route),
            have_index: self.have_index.clone(),
            want_index: self.want_index.clone(),
            fetch_progress_tx: self.fetch_progress_tx.clone(),
            verify_progress_tx: self.verify_progress_tx.clone(),
        }
    }
}

impl<P: Peer + Clone + 'static> Fetcher<P> {
    pub async fn from_dht(mut peer: P, share_key_str: &str, root: &str) -> Result<Fetcher<P>> {
        let share_key = TypedKey::from_str(share_key_str)?;
        let root_path_buf = PathBuf::from_str(root).unwrap();
        let (target, header, want_index) =
            with_backoff_reset!(peer, peer.resolve(&share_key, &root_path_buf).await)?;
        let (fetch_progress_tx, _) = watch::channel(Progress {
            length: want_index.payload().length() as u64,
            position: 0u64,
        });
        let (verify_progress_tx, _) = watch::channel(Progress {
            length: want_index.payload().pieces().len() as u64,
            position: 0u64,
        });

        Ok(Fetcher {
            peer,
            share_key,
            route: Arc::new(RwLock::new((target, header))),
            have_index: want_index.empty(),
            want_index,
            fetch_progress_tx,
            verify_progress_tx,
        })
    }

    pub fn set_have_index(&mut self, have_index: Index) {
        self.have_index = have_index;
    }

    pub fn want_index(&self) -> &Index {
        return &self.want_index;
    }

    pub fn subscribe_fetch_progress(&self) -> watch::Receiver<Progress> {
        self.fetch_progress_tx.subscribe()
    }

    pub fn subscribe_verify_progress(&self) -> watch::Receiver<Progress> {
        self.verify_progress_tx.subscribe()
    }

    pub fn file(&self) -> String {
        self.want_index
            .files()
            .iter()
            .map(|f| f.path().to_str().unwrap_or("").to_string())
            .collect::<Vec<String>>()
            .join(",")
    }

    pub fn digest(&self) -> String {
        hex::encode(self.want_index.payload().digest())
    }

    pub async fn fetch(mut self, cancel: CancellationToken) -> Result<()> {
        let (fetch_tx, fetch_rx) = unbounded();
        let (verify_tx, verify_rx) = unbounded();

        let done = CancellationToken::new();
        self.enqueue_blocks(fetch_tx.clone(), verify_tx.clone(), done.clone())
            .await?;
        let mut verify_tasks = JoinSet::new();
        verify_tasks.spawn(Self::verify_blocks(
            self.want_index.clone(),
            cancel.clone(),
            done.clone(),
            fetch_tx.clone(),
            verify_rx.clone(),
            self.verify_progress_tx.clone(),
        ));
        let mut fetch_tasks = JoinSet::new();
        for i in 0..N_FETCHERS {
            fetch_tasks.spawn(self.clone().fetch_blocks(
                cancel.clone(),
                done.clone(),
                fetch_tx.clone(),
                fetch_rx.clone(),
                verify_tx.clone(),
                i,
            ));
        }
        let mut result = Ok(());
        let mut verify_progress_rx = self.verify_progress_tx.subscribe();
        loop {
            select! {
                _ = done.cancelled() => {
                    break;
                }
                verify_status = verify_tasks.join_next() => {
                    if verify_rx.is_empty() {
                        // If we verified all the blocks, forget any prior
                        // errors, they were temporary.
                        result = Ok(());
                    }
                    match verify_status {
                        None => {
                            done.cancel();
                            break
                        }
                        Some(Err(e)) => {
                            warn!(err = format!("{}", e));
                            if let Ok(_) = result {
                                result = Err(Error::other(e));
                            }
                            done.cancel();
                            break;
                        }
                        Some(Ok(Err(e))) => {
                            if let Ok(_) = result {
                                result = Err(e);
                            }
                            done.cancel();
                            break;
                        }
                        _ => {}
                    };
                }
                verify_progress_result = verify_progress_rx.changed() => {
                    if let Err(_) = verify_progress_result {
                        continue;
                    }
                    // A block was verified, meaning we're still making progress.
                    // Re-spawn fetchers that might have exited on temporary errors.
                    if !fetch_rx.is_empty() && fetch_tasks.len() < N_FETCHERS as usize {
                        for i in fetch_tasks.len() as u8..N_FETCHERS {
                            fetch_tasks.spawn(self.clone().fetch_blocks(
                                cancel.clone(),
                                done.clone(),
                                fetch_tx.clone(),
                                fetch_rx.clone(),
                                verify_tx.clone(),
                                i,
                            ));
                        }
                    }
                }
                fetch_status = fetch_tasks.join_next() => {
                    match fetch_status {
                        None => {
                            // Are we done fetching? Or do we need to reset / retry / restart fetchers?
                            if fetch_rx.is_empty() {
                                continue;
                            }

                            // Reset the peer
                            reset_with_backoff(&mut self.peer, &done).await?;

                            // Attempt to re-resolve the route
                            let mut route = self.route.write().await;
                            match with_backoff_retry!(self.peer.reresolve_route(&self.share_key, None).await) {
                                Ok((target, header)) => *route = (target, header),
                                Err(e) => warn!(err = format!("{}", e), "failed to re-resolve route"),
                            };
                            drop(route);

                            // Relaunch fetchers
                            for i in 0..N_FETCHERS {
                                fetch_tasks.spawn(self.clone().fetch_blocks(
                                    cancel.clone(),
                                    done.clone(),
                                    fetch_tx.clone(),
                                    fetch_rx.clone(),
                                    verify_tx.clone(),
                                    i,
                                ));
                            }
                        }
                        Some(Err(e)) => {
                            // Problem with the job itself, shut down
                            warn!(err = format!("{}", e));
                            if let Ok(_) = result {
                                result = Err(Error::other(e));
                            }
                            done.cancel();
                            break;
                        }
                        Some(Ok(Err(e))) => {
                            if let Ok(_) = result {
                                result = Err(e);
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        if !verify_tasks.is_empty() {
            verify_tasks.abort_all();
        }
        if !fetch_tasks.is_empty() {
            fetch_tasks.abort_all();
        }

        result
    }

    async fn fetch_blocks(
        mut self,
        cancel: CancellationToken,
        done: CancellationToken,
        fetch_block_tx: Sender<FileBlockFetch>,
        fetch_block_rx: Receiver<FileBlockFetch>,
        verify_tx: Sender<PieceState>,
        task_id: u8,
    ) -> Result<()> {
        let mut fh_map: HashMap<usize, File> = HashMap::new();
        loop {
            select! {
                recv_fetch = fetch_block_rx.recv_async() => {
                    let fetch = match recv_fetch {
                        Ok(fetch) => fetch,
                        Err(e) => {
                            // This is practically impossible given we have a sender in scope here for retries...
                            warn!(err = format!("{}", e), "all fetch block senders have been dropped");
                            return Ok(())
                        }
                    };
                    trace!(task_id, fetch = format!("{:?}", fetch));
                    let fetch_result: Result<()> = async {
                        // Request block from peer
                        let target = self.route.read().await.0.clone();
                        let result = with_backoff_retry!(self.peer.request_block(
                            target.clone(),
                            fetch.piece_index,
                            fetch.block_index,
                        ).await);
                        if let Err(e) = result {
                            let mut route = self.route.write().await;
                            if e.is_route_invalid() {
                                let (target, header) = with_backoff_reset!(self.peer, self.peer.reresolve_route(&self.share_key, Some(target)).await)?;
                                *route = (target, header);
                            }
                            return Err(e);
                        }
                        let mut block = result?;

                        // Write the block to the file
                        let fh = match fh_map.get_mut(&fetch.file_index) {
                            Some(fh) => fh,
                            None => {
                                let path = self.want_index.root().join(self.want_index.files()[fetch.file_index].path());
                                let fh = File::options().write(true).truncate(false).create(true).open(path).await?;
                                fh_map.insert(fetch.file_index, fh);
                                fh_map.get_mut(&fetch.file_index).unwrap()
                            }
                        };
                        fh.seek(SeekFrom::Start(fetch.block_offset() as u64)).await?;
                        let block_end = min(block.len(), BLOCK_SIZE_BYTES);
                        fh.write_all(block[fetch.piece_offset..block_end].as_mut()).await?;
                        self.fetch_progress_tx.send_modify(|p| {
                            p.position += block_end as u64;
                        });
                        verify_tx.send_async(PieceState::new(
                            fetch.file_index,
                            fetch.piece_index,
                            fetch.piece_offset,
                            self.want_index.payload().pieces()[fetch.piece_index].block_count(),
                            fetch.block_index)).await.map_err(Error::cancelled)?;
                        Ok(())
                    }.await;
                    match fetch_result {
                        Ok(()) => {}
                        Err(ref e) => {
                            warn!(err = format!("{}", e), "fetch block failed, queued for retry");
                            fetch_block_tx.send_async(fetch).await.map_err(Error::cancelled)?;
                            if !e.is_route_invalid() {
                                return fetch_result;
                            }
                        }
                    };
                }
                _ = done.cancelled() => {
                    return Ok(())
                }
                _ = cancel.cancelled() => {
                    return Err(Error::Fault(Unexpected::Cancelled))
                }
            }
        }
    }

    async fn enqueue_blocks(
        &self,
        fetch_tx: Sender<FileBlockFetch>,
        verify_tx: Sender<PieceState>,
        done: CancellationToken,
    ) -> Result<()> {
        let diff = self.want_index.diff(&self.have_index);
        let mut want_length = 0;
        for want_block in diff.want {
            fetch_tx
                .send(FileBlockFetch {
                    file_index: want_block.file_index,
                    piece_index: want_block.piece_index,
                    piece_offset: want_block.piece_offset,
                    block_index: want_block.block_index,
                })
                .map_err(Error::cancelled)?;
            want_length += want_block.block_length;
        }
        let mut have_length = 0;
        for have_block in diff.have {
            verify_tx
                .send(PieceState::new(
                    have_block.file_index,
                    have_block.piece_index,
                    have_block.piece_offset,
                    self.want_index.payload().pieces()[have_block.piece_index].block_count(),
                    have_block.block_index,
                ))
                .map_err(Error::other)?;
            self.fetch_progress_tx
                .send_modify(|p| p.position += have_block.block_length as u64);
            have_length += have_block.block_length;
        }
        debug!(want_length, have_length);
        // If we have everything we need, flag the fetch as done. Otherwise
        // fetch_block workers will hang on channels.
        if want_length == 0 {
            done.cancel()
        }
        Ok(())
    }

    async fn verify_blocks(
        index: Index,
        cancel: CancellationToken,
        done: CancellationToken,
        fetch_block_tx: Sender<FileBlockFetch>,
        verify_rx: Receiver<PieceState>,
        verify_progress_tx: watch::Sender<Progress>,
    ) -> Result<()> {
        let mut piece_states: HashMap<(usize, usize), PieceState> = HashMap::new();
        let mut verified_pieces = 0;
        loop {
            select! {
                recv_verify = verify_rx.recv_async() => {
                    // select on verify receiver
                    let mut to_verify = match recv_verify {
                        Ok(verify) => verify,
                        Err(e) => {
                            trace!(err = format!("{}", e), "all verify senders have been dropped");
                            return Ok(())
                        }
                    };
                    // update piece state
                    if let Some(prior_state) = piece_states.get_mut(&to_verify.key()) {
                        to_verify = prior_state.merged(to_verify);
                        *prior_state = to_verify;
                    } else {
                        piece_states.insert(to_verify.key(), to_verify);
                    }
                    if to_verify.is_complete() {
                        // verify complete ones
                        if Self::verify_piece(&index, to_verify.file_index, to_verify.piece_index).await? {
                            trace!(file_index = to_verify.file_index, piece_index = to_verify.piece_index, "digest verified");
                            verified_pieces += 1;
                            verify_progress_tx.send_modify(|p| {
                                p.position += 1;
                            });
                            if verified_pieces == index.payload().pieces().len() {
                                info!("all pieces verified");
                                done.cancel();
                                return Ok(())
                            }
                        } else {
                            // re-queue fetch if invalid
                            warn!(file_index = to_verify.file_index, piece_index = to_verify.piece_index, "invalid digest, retrying");
                            piece_states.insert(to_verify.key(), PieceState::empty(
                                to_verify.file_index,
                                to_verify.piece_index,
                                to_verify.piece_offset,
                                index.payload().pieces()[to_verify.piece_index].block_count(),
                            ));
                            for block_index in 0..32  {
                                fetch_block_tx.send_async(FileBlockFetch{
                                    file_index: to_verify.file_index,
                                    piece_index: to_verify.piece_index,
                                    piece_offset: to_verify.piece_offset,
                                    block_index,
                                }).await.map_err(Error::cancelled)?;
                            }
                        }
                    }
                }
                _ = cancel.cancelled() => {
                    return Err(Error::Fault(Unexpected::Cancelled))
                }
            }
        }
    }

    #[instrument(skip(index), err)]
    async fn verify_piece(index: &Index, file_index: usize, piece_index: usize) -> Result<bool> {
        let file_spec = &index.files()[file_index];
        let mut fh = File::open(index.root().join(file_spec.path())).await?;
        let piece_spec = &index.payload().pieces()[piece_index];

        fh.seek(SeekFrom::Start((piece_index * PIECE_SIZE_BYTES) as u64))
            .await?;
        let mut buf = [0u8; BLOCK_SIZE_BYTES];
        let mut digest = Sha256::new();
        for _ in 0..PIECE_SIZE_BLOCKS {
            let rd = fh.read(&mut buf[..]).await?;
            if rd == 0 {
                break;
            }
            digest.update(&buf[..rd]);
        }
        let actual_digest: [u8; 32] = digest.finalize().into();
        Ok(piece_spec.digest().cmp(&actual_digest[..]) == Ordering::Equal)
    }
}

#[derive(Debug)]
struct FileBlockFetch {
    file_index: usize,
    piece_index: usize,
    piece_offset: usize,
    block_index: usize,
}

impl FileBlockFetch {
    fn block_offset(&self) -> usize {
        (self.piece_index * PIECE_SIZE_BYTES)
            + self.piece_offset
            + (self.block_index * BLOCK_SIZE_BYTES)
    }
}

#[derive(Clone, Copy)]
struct PieceState {
    file_index: usize,
    piece_index: usize,
    piece_offset: usize,
    block_count: usize,
    blocks: u32,
}

impl PieceState {
    fn new(
        file_index: usize,
        piece_index: usize,
        piece_offset: usize,
        block_count: usize,
        block_index: usize,
    ) -> PieceState {
        PieceState {
            file_index,
            piece_index,
            piece_offset,
            block_count,
            blocks: 1 << block_index,
        }
    }

    fn empty(
        file_index: usize,
        piece_index: usize,
        piece_offset: usize,
        block_count: usize,
    ) -> PieceState {
        PieceState {
            file_index,
            piece_index,
            piece_offset,
            block_count,
            blocks: 0u32,
        }
    }

    fn key(&self) -> (usize, usize) {
        (self.file_index, self.piece_index)
    }

    fn is_complete(&self) -> bool {
        match self.block_count {
            0 => true,
            PIECE_SIZE_BLOCKS => self.blocks == 0xffffffff,
            _ => {
                let mask = 1 << self.block_count - 1;
                self.blocks & mask == mask
            }
        }
    }

    fn merged(&mut self, other: PieceState) -> PieceState {
        if self.file_index != other.file_index || self.piece_index != other.piece_index {
            panic!("attempt to merge mismatched pieces");
        }
        self.blocks |= other.blocks;
        self.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        path::Path,
        sync::{Arc, Mutex},
    };

    use stigmerge_fileindex::Indexer;
    use veilid_core::{TimestampDuration, VeilidStateAttachment, VeilidUpdate};

    use crate::{
        proto::Encoder,
        tests::{temp_file, StubPeer},
        Observable,
    };

    use super::*;

    #[tokio::test]
    async fn from_dht_ok() {
        let tf = temp_file(0xa5u8, 1048576);
        let indexer = Indexer::from_file(std::env::temp_dir().join(tf.path()).into())
            .await
            .expect("indexer");
        let index = indexer.index().await.expect("index");

        let mut stub_peer = StubPeer::new();
        let update_tx = stub_peer.update_tx.clone();
        stub_peer.reset_result = Arc::new(Mutex::new(move || Ok(())));
        stub_peer.resolve_result = Arc::new(Mutex::new(move |_key: &TypedKey, _path: &Path| {
            let index_internal = index.clone();
            let route_key =
                TypedKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").unwrap();
            let index_bytes = index_internal.encode().expect("encode index");
            Ok((
                Target::PrivateRoute(route_key.value),
                Header::from_index(
                    &index_internal,
                    index_bytes.as_slice(),
                    &[0xde, 0xad, 0xbe, 0xef],
                ),
                index_internal,
            ))
        }));
        stub_peer.request_block_result =
            Arc::new(Mutex::new(move |_, _, _| Ok(vec![0xa5u8; 32768])));
        let rp = Observable::new(stub_peer);

        // Simulate getting connected to network, normally track_node_state
        // would set this when the node comes online.
        update_tx
            .send(VeilidUpdate::Attachment(Box::new(VeilidStateAttachment {
                state: veilid_core::AttachmentState::AttachedGood,
                public_internet_ready: true,
                local_network_ready: true,
                attached_uptime: None,
                uptime: TimestampDuration::new(0),
            })))
            .expect("send veilid update");

        let tdir = tempfile::TempDir::new().expect("tempdir");
        let fetcher = Fetcher::from_dht(
            rp,
            "VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M",
            tdir.path().to_str().unwrap(),
        )
        .await
        .expect("from_dht");
        let cancel = CancellationToken::new();
        fetcher.fetch(cancel).await.expect("fetch");
        // Simulate a shutdown so that track_node_state exits.
        update_tx
            .send(VeilidUpdate::Shutdown)
            .expect("send veilid update");
    }
}
