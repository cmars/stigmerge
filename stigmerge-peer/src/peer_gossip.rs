use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use anyhow::Context;
use tokio::{select, sync::broadcast};
use tokio_utils::MultiRateLimiter;
use tracing::{debug, trace, warn};
use veilid_core::{Target, Timestamp, TimestampDuration, TypedRecordKey, VeilidUpdate};

use crate::{
    actor::{Actor, Respondable, ResponseChannel},
    error::Unrecoverable,
    proto::{self, Decoder},
    share_resolver,
    types::ShareInfo,
    CancelError, Node, Result,
};

pub struct PeerGossip<N: Node> {
    node: N,
    share: ShareInfo,
    share_resolver_tx: flume::Sender<share_resolver::Request>,
    share_target_rx: broadcast::Receiver<(TypedRecordKey, Target)>,

    known_peers: HashMap<TypedRecordKey, u16>,
    advertisements: HashMap<(TypedRecordKey, TypedRecordKey), Timestamp>,
    blocked_peers: HashSet<TypedRecordKey>,

    announce_request_tx: flume::Sender<(TypedRecordKey, u16)>,
    announce_request_rx: flume::Receiver<(TypedRecordKey, u16)>,

    resolve_request_tx: flume::Sender<TypedRecordKey>,
    resolve_request_rx: flume::Receiver<TypedRecordKey>,

    share_resolver_resp_tx: flume::Sender<share_resolver::Response>,
    share_resolver_resp_rx: flume::Receiver<share_resolver::Response>,
}

impl<N: Node> PeerGossip<N> {
    pub fn new(
        node: N,
        share: ShareInfo,
        share_resolver_tx: flume::Sender<share_resolver::Request>,
        share_target_rx: broadcast::Receiver<(TypedRecordKey, Target)>,
    ) -> Self {
        let (announce_request_tx, announce_request_rx) = flume::unbounded();
        let (resolve_request_tx, resolve_request_rx) = flume::unbounded();
        let (share_resolver_resp_tx, share_resolver_resp_rx) = flume::unbounded();

        Self {
            node,
            share,
            share_resolver_tx,
            share_target_rx,

            known_peers: HashMap::new(),
            advertisements: HashMap::new(),
            blocked_peers: HashSet::new(),

            announce_request_tx,
            announce_request_rx,
            resolve_request_tx,
            resolve_request_rx,
            share_resolver_resp_tx,
            share_resolver_resp_rx,
        }
    }

    async fn advertise_peers(&mut self, key: &TypedRecordKey, target: &Target) -> Result<()> {
        self.add_known_peer(key).await?;
        for known_peer in self.known_peers.keys() {
            // Don't advertise a key to itself
            if known_peer == key {
                continue;
            }
            // Don't advertise the same peer to the same target too often
            if let Some(last_adv_time) = self
                .advertisements
                .get(&(known_peer.to_owned(), key.to_owned()))
            {
                if Timestamp::now().saturating_sub(*last_adv_time) < TimestampDuration::new_secs(30)
                {
                    trace!("advertised {known_peer} to {key} too recently");
                    continue;
                }
            }
            // Attempt to advertise
            match self.node.request_advertise_peer(target, known_peer).await {
                Ok(()) => {
                    trace!("advertised {known_peer} to {key}");
                    self.advertisements
                        .insert((known_peer.to_owned(), key.to_owned()), Timestamp::now());
                }
                Err(e) => {
                    warn!("advertise {known_peer} to {key}: {e}");
                }
            }
        }
        Ok(())
    }

    async fn add_known_peers(&mut self) -> Result<()> {
        let known_peers = self
            .node
            .known_peers(&self.share.want_index.payload().digest())
            .await?;
        trace!("known peers: {known_peers:?}");
        for key in known_peers.iter() {
            self.add_known_peer(key).await?;
        }
        Ok(())
    }

    async fn add_known_peer(&mut self, key: &TypedRecordKey) -> Result<()> {
        if self.blocked_peers.contains(key) {
            return Ok(());
        }
        if let None = self.known_peers.get(key) {
            let index = self.known_peers.len().try_into().unwrap();
            self.known_peers.insert(*key, index);
            self.announce_request_tx
                .send_async((*key, index))
                .await
                .context(Unrecoverable::new("schedule peer announce"))?;
            self.resolve_request_tx
                .send_async(*key)
                .await
                .context(Unrecoverable::new("schedule peer resolve"))?;
            self.share_resolver_tx
                .send_async(share_resolver::Request::Index {
                    response_tx: ResponseChannel::default(),
                    key: *key,
                    want_index_digest: Some(self.share.want_index_digest),
                    root: self.share.root.to_owned(),
                })
                .await
                .context(Unrecoverable::new("send share_resolver request"))?;
            trace!("added known peer {key}");
        };
        Ok(())
    }

    async fn resolve_peers(&mut self, key: &TypedRecordKey) -> Result<()> {
        for peer_info in self.node.resolve_peers(&key).await? {
            trace!("discovered peer {} from {key}", peer_info.key());
            self.add_known_peer(peer_info.key()).await?;
        }
        Ok(())
    }
}

pub enum Request {
    Announce {
        response_tx: ResponseChannel<Response>,
        key: TypedRecordKey,
    },
}

impl Respondable for Request {
    type Response = Response;

    fn set_response(&mut self, tx: ResponseChannel<Self::Response>) {
        match self {
            Request::Announce { response_tx, .. } => *response_tx = tx,
        }
        todo!()
    }

    fn response_tx(self) -> ResponseChannel<Self::Response> {
        match self {
            Request::Announce { response_tx, .. } => response_tx,
        }
    }
}

pub type Response = ();

impl<N: Node> Actor for PeerGossip<N> {
    type Request = Request;
    type Response = Response;

    async fn run(
        &mut self,
        cancel: tokio_util::sync::CancellationToken,
        request_rx: flume::Receiver<Self::Request>,
    ) -> Result<()> {
        self.add_known_peers().await?;
        let mut update_rx = self.node.subscribe_veilid_update();
        let limiter = MultiRateLimiter::new(Duration::from_secs(5));
        loop {
            select! {
                _ = cancel.cancelled() => {
                    return Err(CancelError.into());
                }
                res = request_rx.recv_async() => {
                    let req = res.context(Unrecoverable::new("receive request"))?;
                    self.handle_request(req).await?;
                }
                res = self.share_target_rx.recv() => {
                    let (key, target) = match res {
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        res => res
                    }.context(Unrecoverable::new("receive share target update"))?;
                    debug!("share target update for {key}: {target:?}");
                    self.advertise_peers(&key, &target).await?;
                }
                res = update_rx.recv() => {
                    let update = res.context(Unrecoverable::new("receive veilid update"))?;
                    match update {
                        VeilidUpdate::AppMessage(veilid_app_message) => {
                            let req = proto::Request::decode(veilid_app_message.message())?;
                            match req {
                                proto::Request::AdvertisePeer(peer_req) => {
                                    debug!("discovered advertised peer {}", peer_req.key);
                                    self.share_resolver_tx.send_async(share_resolver::Request::Index{
                                        response_tx: self.share_resolver_resp_tx.clone().into(),
                                        key: peer_req.key,
                                        want_index_digest: Some(self.share.want_index_digest),
                                        root: self.share.root.to_owned(),
                                    }).await.context(Unrecoverable::new("send share_resolver request"))?;
                                }
                                _ => {}
                            }
                        }
                        VeilidUpdate::Shutdown => {
                            cancel.cancel();
                        }
                        _ => {}
                    }
                }
                res = limiter.throttle("announce", || self.announce_request_rx.recv_async()) => {
                    let (peer_key, subkey) = res.context(Unrecoverable::new("receive peer announce"))?;
                    match self.node.announce_peer(&self.share.want_index.payload().digest(), Some(peer_key), subkey).await {
                        Ok(_) => {
                            trace!("announce peer {peer_key} subkey {subkey}");
                        }
                        Err(e) => {
                            warn!("announce peer {peer_key} subkey {subkey}: {e}");
                            self.announce_request_tx.send_async((peer_key, subkey)).await.context(Unrecoverable::new("requeue announce peer"))?;
                        }
                    }
                }
                res = limiter.throttle("resolve", || self.resolve_request_rx.recv_async()) => {
                    let peer_key = res.context(Unrecoverable::new("receive peer resolve"))?;
                    match self.resolve_peers(&peer_key).await {
                        Ok(_) => {
                            trace!("resolve peer {peer_key}");
                        }
                        Err(e) => {
                            warn!("resolve peer {peer_key}: {e}");
                            self.resolve_request_tx.send_async(peer_key).await.context(Unrecoverable::new("requeue announce peer"))?;
                        }
                    }
                }
                res = self.share_resolver_resp_rx.recv_async() => {
                    let resp = res.context(Unrecoverable::new("receive share resolver response"))?;
                    match resp {
                        share_resolver::Response::BadIndex { key } => {
                            if let Some(blocked_subkey) = self.known_peers.remove(&key) {
                                self.announce_request_tx.send_async(
                                    (self.share.key, blocked_subkey)).await.context(Unrecoverable::new("replace blocked peer with share key"))?;
                            }
                            self.blocked_peers.insert(key);
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    async fn handle_request(&mut self, req: Self::Request) -> Result<()> {
        match req {
            Request::Announce {
                mut response_tx,
                key,
            } => {
                self.add_known_peer(&key).await?;
                response_tx
                    .send(())
                    .await
                    .context(Unrecoverable::new("respond to announce request"))?;
            }
        }
        Ok(())
    }
}
