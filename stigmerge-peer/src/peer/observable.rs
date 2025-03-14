use std::{fmt::Display, path::Path, time::Duration};

use stigmerge_fileindex::Index;
use tokio::{
    select,
    sync::{broadcast::Receiver, watch},
    time::interval,
};
use tracing::{debug, instrument, warn};
use veilid_core::{OperationId, Target, TimestampDuration, VeilidUpdate};

use crate::{error::Error, error::NodeState, error::Result, proto::Header, Peer};

use super::TypedKey;

pub struct Observable<P: Peer> {
    peer: P,
    node_state_rx: watch::Receiver<NodeState>,
    peer_progress_tx: watch::Sender<Progress>,
}

#[derive(Clone, Debug)]
pub struct Progress {
    pub state: State,
    pub length: u64,
    pub position: u64,
}

#[derive(Clone, Debug)]
pub enum State {
    Starting,
    Connecting,
    Announcing,
    Resolving,
    Connected,
    Down,
}

impl Default for Progress {
    fn default() -> Self {
        Progress {
            state: State::Starting,
            length: 0u64,
            position: 0u64,
        }
    }
}

impl<P: Peer + 'static> Observable<P> {
    const DEFAULT_RESET_TIMEOUT: Duration = Duration::from_secs(180);

    pub fn new(peer: P) -> Observable<P> {
        let updates = peer.subscribe_veilid_update();
        let (tx, rx) = watch::channel(NodeState::APINotStarted);
        let (peer_progress_tx, _) = watch::channel(Progress::default());
        tokio::spawn(Self::track_node_state(updates, tx));
        Observable {
            peer,
            node_state_rx: rx,
            peer_progress_tx,
        }
    }

    async fn track_node_state(
        mut updates: Receiver<VeilidUpdate>,
        tx: tokio::sync::watch::Sender<NodeState>,
    ) -> Result<()> {
        loop {
            let update = updates.recv().await.map_err(Error::other)?;
            match update {
                VeilidUpdate::Attachment(ref attachment) => {
                    let updated_state = NodeState::from(attachment);
                    debug!(
                        state = format!("{}", updated_state),
                        attachment = format!("{:?}", attachment)
                    );
                    tx.send_modify(|current_state| {
                        *current_state = updated_state;
                    });
                }
                VeilidUpdate::Shutdown => {
                    debug!(state = format!("{}", NodeState::APIShuttingDown));
                    tx.send(NodeState::APIShuttingDown).map_err(Error::other)?;
                    break;
                }
                _ => {}
            }
        }
        Ok::<(), Error>(())
    }

    pub fn subscribe_peer_progress(&self) -> watch::Receiver<Progress> {
        self.peer_progress_tx.subscribe()
    }

    fn update_progress(peer_progress_tx: &watch::Sender<Progress>, state: State) {
        warn_err(peer_progress_tx.send(Progress {
            state,
            length: 0u64,
            position: 0u64,
        }));
    }
}

impl<P: Peer + Sync + 'static> Peer for Observable<P> {
    fn subscribe_veilid_update(&self) -> Receiver<VeilidUpdate> {
        self.peer.subscribe_veilid_update()
    }

    #[instrument(skip(self), level = "debug", err)]
    async fn reset(&mut self) -> Result<()> {
        Self::update_progress(&self.peer_progress_tx, State::Connecting);
        self.peer.reset().await?;
        let mut timer = interval(Self::DEFAULT_RESET_TIMEOUT);
        timer.tick().await; // discard immediate tick
        loop {
            select! {
                wait_result = self.node_state_rx.wait_for(|ns| ns.is_connected()) => {
                    if let Ok(_) = wait_result {
                        Self::update_progress(&self.peer_progress_tx, State::Connected);
                        return Ok(());
                    }
                    continue;
                }
                _ = timer.tick() => {
                    Self::update_progress(&self.peer_progress_tx, State::Down);
                    return Err(Error::ResetTimeout);
                }
            }
        }
    }

    #[instrument(skip(self), level = "debug", err)]
    async fn shutdown(self) -> Result<()> {
        Self::update_progress(&self.peer_progress_tx, State::Down);
        self.peer.shutdown().await
    }

    #[instrument(skip(self, index), level = "debug", err)]
    async fn announce(&mut self, index: &Index) -> Result<(TypedKey, Target, Header)> {
        self.peer.announce(index).await
    }

    #[instrument(skip(self, index, header), level = "debug", err)]
    async fn reannounce_route(
        &mut self,
        key: &TypedKey,
        prior_route: Option<Target>,
        index: &Index,
        header: &Header,
    ) -> Result<(Target, Header)> {
        self.peer
            .reannounce_route(key, prior_route, index, header)
            .await
    }

    #[instrument(skip(self), level = "debug", err)]
    async fn resolve(&mut self, key: &TypedKey, root: &Path) -> Result<(Target, Header, Index)> {
        self.peer.resolve(key, root).await
    }

    #[instrument(skip(self), level = "debug", err)]
    async fn reresolve_route(
        &mut self,
        key: &TypedKey,
        prior_route: Option<Target>,
    ) -> Result<(Target, Header)> {
        self.peer.reresolve_route(key, prior_route).await
    }

    #[instrument(skip(self), level = "trace", err)]
    async fn request_block(
        &mut self,
        target: Target,
        piece: usize,
        block: usize,
    ) -> Result<Vec<u8>> {
        self.peer.request_block(target, piece, block).await
    }

    #[instrument(skip(self, contents), level = "trace", err)]
    async fn reply_block_contents(&mut self, call_id: OperationId, contents: &[u8]) -> Result<()> {
        self.peer.reply_block_contents(call_id, contents).await
    }

    async fn watch(
        &mut self,
        key: TypedKey,
        values: veilid_core::ValueSubkeyRangeSet,
        period: TimestampDuration,
    ) -> Result<()> {
        self.peer.watch(key, values, period).await
    }

    fn cancel_watch(&mut self, key: &TypedKey) {
        self.peer.cancel_watch(key);
    }
}

fn warn_err<T, E: Display>(result: std::result::Result<T, E>) {
    if let Err(e) = result {
        warn!(err = format!("{}", e));
    }
}

impl<P: Peer> Clone for Observable<P> {
    fn clone(&self) -> Self {
        Observable {
            peer: self.peer.clone(),
            node_state_rx: self.node_state_rx.clone(),
            peer_progress_tx: self.peer_progress_tx.clone(),
        }
    }
}
