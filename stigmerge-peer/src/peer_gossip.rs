use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::{select, sync::Mutex, time::interval};
use tokio_util::sync::CancellationToken;
use tracing::{error, instrument, trace, warn};
use veilid_core::{RecordKey, RouteId, Target, Timestamp, TimestampDuration, VeilidAppMessage};
use veilnet::{
    connection::{RoutingContext, UpdateHandler},
    Connection,
};

use crate::{
    proto::{self, AdvertisePeerRequest, Decoder, Encoder, PeerInfo, Request},
    record::{StablePeersRecord, StableShareRecord},
    share_resolver::{ShareNotifier, ShareResolver},
    types::LocalShareInfo,
    CancelError, Result, Retry,
};

pub struct PeerGossip<C: Connection> {
    inner: Arc<Mutex<PeerGossipInner<C>>>,
    advertise_peer_rx: flume::Receiver<AdvertisePeerRequest>,
    share_changed_rx: flume::Receiver<crate::types::RemoteShareInfo>,
}

impl<C: Connection + Send + Sync + 'static> PeerGossip<C> {
    pub async fn new(
        conn: C,
        share: LocalShareInfo,
        share_resolver: ShareResolver<C>,
    ) -> Result<Self> {
        let (share_notifier, share_changed_rx) = ShareNotifier::new("peer_gossip");
        share_resolver.subscribe(Box::new(share_notifier)).await;
        let (advertise_peer, advertise_peer_rx) = AdvertisePeerRequestHandler::new();
        conn.add_update_handler(Box::new(advertise_peer));
        let inner = Arc::new(Mutex::new(
            PeerGossipInner::new(conn, share, share_resolver).await?,
        ));
        Ok(Self {
            share_changed_rx,
            advertise_peer_rx,
            inner,
        })
    }

    pub async fn advertise_self(&self) -> Result<()> {
        self.inner.lock().await.advertise_self().await
    }

    pub async fn advertise_peer(&self, key: &RecordKey, route_id: &RouteId) -> Result<()> {
        self.inner
            .lock()
            .await
            .advertise_remote_peer(key, route_id)
            .await
    }

    pub async fn run(self, cancel: CancellationToken, retry: Retry) -> Result<()> {
        let mut peer_reannounce_interval = interval(Duration::from_secs(300));
        loop {
            select! {
                _ = cancel.cancelled() => {
                    return Err(CancelError.into());
                }
                res = self.share_changed_rx.recv_async() => {
                    trace!(is_ok = ?res.is_ok(), "receive from share_changed");
                    let remote_share = res?;
                    let mut inner = self.inner.lock().await;
                    if inner.share.want_index_digest == remote_share.index_digest {
                        if let Err(err) = backoff_retry!(cancel, retry, {
                            inner.advertise_remote_peer(&remote_share.key, &remote_share.route_id).await?;
                            trace!(key = ?remote_share.key, "advertise remote peer")
                        }) {
                            warn!(?err, key = ?remote_share.key, "failed to advertise remote peer");
                        }
                    }
                }
                res = self.advertise_peer_rx.recv_async() => {
                    trace!(is_ok = ?res.is_ok(), "receive from advertised_peer");
                    let adv_peer_req = res?;
                    let mut inner = self.inner.lock().await;
                    if let Err(err) = backoff_retry!(cancel, retry, {
                        inner.add_known_peer(&adv_peer_req.key).await?;
                    }) {
                        warn!(?err, key = ?adv_peer_req.key, "failed to add advertised peer");
                    }
                }
                _ = peer_reannounce_interval.tick() => {
                    if let Err(err) = self.advertise_self().await {
                        warn!(?err, "reannounce self to known peers");
                    }
                    trace!(after = ?peer_reannounce_interval.period(), "reannounced self to known peers");
                }
            }
        }
    }
}

struct PeerGossipInner<C: Connection> {
    conn: C,
    share: LocalShareInfo,
    share_resolver: ShareResolver<C>,

    peers_record: StablePeersRecord,
    advertisements: HashMap<(RecordKey, RecordKey), Timestamp>,
}

impl<C: Connection + Send + Sync + 'static> PeerGossipInner<C> {
    async fn new(
        mut conn: C,
        share: LocalShareInfo,
        share_resolver: ShareResolver<C>,
    ) -> Result<Self> {
        let peers_record = StablePeersRecord::new_local(&mut conn, &share.want_index).await?;
        Ok(Self {
            conn,
            share,
            share_resolver,

            peers_record,
            advertisements: HashMap::new(),
        })
    }

    async fn advertise_self(&mut self) -> Result<()> {
        let peers: Vec<RecordKey> = self
            .peers_record
            .known_peers()
            .map(|(k, _)| k)
            .cloned()
            .collect();
        for known_peer in peers.iter() {
            let remote_share = self.share_resolver.add_share(known_peer).await?;
            let key = self.share.key.clone();

            // Don't advertise the same peer to the same target too often
            if let Some(last_adv_time) = self
                .advertisements
                .get(&(known_peer.to_owned(), key.to_owned()))
            {
                // TODO: make this configurable or adaptive or something?
                if Timestamp::now().duration_since(*last_adv_time) < TimestampDuration::new_secs(30)
                {
                    trace!("advertised {known_peer} to {key} too recently");
                    continue;
                }
            }
            // Attempt to advertise
            match self
                .request_advertise_peer(&remote_share.route_id, &key)
                .await
            {
                Ok(()) => {
                    trace!("advertised {key} (self) to {known_peer}");
                    self.advertisements
                        .insert((known_peer.to_owned(), key.to_owned()), Timestamp::now());
                }
                Err(err) => warn!(?err, ?known_peer, ?key, "advertise peer"),
            }
        }
        Ok(())
    }

    async fn advertise_remote_peer(&mut self, key: &RecordKey, route_id: &RouteId) -> Result<()> {
        self.add_known_peer(key).await?;
        let peers: Vec<RecordKey> = self
            .peers_record
            .known_peers()
            .map(|(k, _)| k)
            .cloned()
            .collect();
        for known_peer in peers {
            // Don't advertise a key to itself
            if &known_peer == key {
                continue;
            }
            // Don't advertise the same peer to the same target too often
            if let Some(last_adv_time) = self
                .advertisements
                .get(&(known_peer.to_owned(), key.to_owned()))
            {
                // TODO: make this configurable or adaptive or something?
                if Timestamp::now().duration_since(*last_adv_time) < TimestampDuration::new_secs(30)
                {
                    trace!("advertised {known_peer} to {key} too recently");
                    continue;
                }
            }
            // Attempt to advertise
            match self.request_advertise_peer(route_id, &known_peer).await {
                Ok(()) => {
                    trace!("advertised {known_peer} to {key}");
                    self.advertisements
                        .insert((known_peer.to_owned(), key.to_owned()), Timestamp::now());
                }
                Err(err) => warn!(?err, ?known_peer, ?key, "advertise peer"),
            }
        }
        Ok(())
    }

    async fn add_known_peer(&mut self, key: &RecordKey) -> Result<()> {
        if key == &self.share.key {
            return Ok(());
        }
        if !self.peers_record.has_peer(key) {
            let remote_share = self.share_resolver.add_share(key).await?;
            if remote_share.index_digest != self.share.want_index_digest {
                warn!(
                    ?key,
                    want_index_digest = ?self.share.want_index_digest,
                    remote_index_digest = ?remote_share.index_digest,
                    "rejected peer, index does not match local share",
                );
            }
            self.peers_record.update_peer(&mut self.conn, key).await?;
            if let Err(err) = self
                .request_advertise_peer(&remote_share.route_id, &self.share.key.clone())
                .await
            {
                warn!(?err, remote_share_key = ?remote_share.key, "advertise self to remote peer");
            }
            for peer in self.resolve_peers(key).await?.iter() {
                if peer.key() == &self.share.key {
                    continue;
                }
                let remote_share_info = self.share_resolver.add_share(peer.key()).await?;
                if remote_share_info.index_digest != self.share.want_index_digest {
                    self.share_resolver
                        .remove_share(&remote_share_info.key)
                        .await?;
                }
            }
        }
        trace!("added known peer {key}");
        Ok(())
    }

    async fn request_advertise_peer(&mut self, route_id: &RouteId, key: &RecordKey) -> Result<()> {
        let routing_context = self.conn.routing_context();
        let req = Request::AdvertisePeer(AdvertisePeerRequest { key: key.clone() });
        let req_bytes = req.encode()?;
        routing_context
            .app_message(Target::RouteId(route_id.clone()), req_bytes)
            .await?;
        Ok(())
    }

    #[instrument(skip(self))]
    async fn resolve_peers(&mut self, peer_key: &RecordKey) -> Result<Vec<PeerInfo>> {
        let header = StableShareRecord::read_header(&mut self.conn, peer_key).await?;
        let mut remote_peers_rec = match header.peer_map() {
            Some(peer_map) => StablePeersRecord::new_remote(&mut self.conn, peer_map.key()).await?,
            None => {
                warn!(?peer_key, "does not publish peers");
                return Ok(vec![]);
            }
        };
        remote_peers_rec.load_peers(&mut self.conn).await?;
        trace!(peers_key = ?remote_peers_rec.key());
        Ok(remote_peers_rec
            .known_peers()
            .map(|(_key, (_subkey, peer_info))| peer_info)
            .cloned()
            .collect())
    }
}

struct AdvertisePeerRequestHandler {
    advertise_peer_tx: flume::Sender<AdvertisePeerRequest>,
}

impl AdvertisePeerRequestHandler {
    fn new() -> (Self, flume::Receiver<AdvertisePeerRequest>) {
        let (advertise_peer_tx, advertise_peer_rx) = flume::unbounded();
        (Self { advertise_peer_tx }, advertise_peer_rx)
    }
}

impl UpdateHandler for AdvertisePeerRequestHandler {
    fn app_message(&self, app_message: &VeilidAppMessage) {
        match proto::Request::decode(app_message.message()) {
            Ok(proto::Request::AdvertisePeer(adv_peer_req)) => {
                trace!(route_id = ?app_message.route_id(), peer_share_key = ?adv_peer_req.key);
                let _ = self.advertise_peer_tx.send(adv_peer_req).map_err(|err| {
                    error!(?err, "send advertise request to peer_gossip");
                    err
                });
            }
            Ok(_) => {}
            Err(err) => {
                warn!(?err, "invalid app_call");
            }
        }
    }
}
