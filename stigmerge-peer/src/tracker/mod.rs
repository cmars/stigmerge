use std::collections::HashMap;

use roaring::RoaringBitmap;
use stigmerge_fileindex::Index;
use tokio::{sync::mpsc, task::JoinSet};
use tokio_util::sync::CancellationToken;
use veilid_core::{Target, Timestamp};

use crate::{
    peer::TypedKey,
    proto::{Digest, Header},
    Peer, Result,
};

mod share_resolver;

pub struct Tracker<P: Peer> {
    peer: P,

    /// Digest of the wanted index.
    want_index_digest: Digest,

    /// The wanted index, once resolved with a peer.
    /// It's authenticated by matching the index digest.
    want_index: Option<Index>,

    /// Collection of known remote peers.
    remote_peers: HashMap<TypedKey, RemotePeerInfo>,

    share_client: ChanClient<share_resolver::Request, share_resolver::Response>,

    cancel: CancellationToken,

    tasks: JoinSet<Result<()>>,
}

impl<P: Peer + Clone + 'static> Tracker<P> {
    pub fn new(peer: P, want_index_digest: Digest, share_keys: Vec<TypedKey>) -> Tracker<P> {
        let cancel = CancellationToken::new();
        let mut tasks = JoinSet::new();

        let (share_client, share_server) = chan_rpc(32);
        let share_svc = share_resolver::Service::new(peer.clone(), share_server);
        let share_cancel = cancel.clone();
        tasks.spawn(async move { share_svc.run(share_cancel).await });

        Self::new_with_services(
            peer,
            want_index_digest,
            share_keys,
            cancel,
            tasks,
            share_client,
        )
    }

    /// Internal constructor, useful for mocking services in tests.
    fn new_with_services(
        peer: P,
        want_index_digest: [u8; 32],
        share_keys: Vec<veilid_core::CryptoTyped<veilid_core::CryptoKey>>,
        cancel: CancellationToken,
        tasks: JoinSet<Result<()>>,
        share_client: ChanClient<share_resolver::Request, share_resolver::Response>,
    ) -> Tracker<P> {
        Tracker {
            peer,
            want_index_digest,
            want_index: None,
            remote_peers: share_keys
                .iter()
                .map(|k| (k.clone(), RemotePeerInfo::new(*k)))
                .collect(),
            share_client,
            cancel,
            tasks,
        }
    }

    pub async fn shutdown(&mut self) -> Result<()> {
        self.cancel.cancel();
        // TODO: cleaner more graceful shutdown?
        self.tasks.shutdown().await;
        Ok(())
    }
}

struct RemotePeerInfo {
    share_key: TypedKey,

    havemap_key: Option<TypedKey>,
    peermap_key: Option<TypedKey>,

    /// Last known private route and share DHT header for this peer.
    route: Option<(Target, Header)>,

    /// Last known advertisted
    /// May be updated by watches on an active peer.
    havemap: Option<RoaringBitmap>,

    /// Last known advertised peermap.
    /// May be updated by watches on an active peer.
    peermap: Vec<AdvertisedPeer>,

    /// When the Peer's havemap was last synchronized.
    havemap_updated_at: Option<Timestamp>,

    /// When the Peer's peermap was last synchronized.
    peermap_updated_at: Option<Timestamp>,

    /// When the Peer last provided a block response over the private route.
    last_block_response: Option<Timestamp>,

    /// This peer advertises a havemap DHT key that isn't available.
    missing_havemap: bool,

    /// This peer advertises a peermap DHT key that isn't available.
    missing_peermap: bool,

    /// This peer's share DHT key isn't available.
    missing_share: bool,

    /// This peer has a share DHT, but the values unmarshal to an invalid index.
    bad_index: bool,

    /// This peer provided bad blocks.
    bad_blocks: bool,
}

impl RemotePeerInfo {
    fn new(share_key: TypedKey) -> Self {
        Self {
            share_key,
            havemap_key: None,
            peermap_key: None,
            route: None,
            havemap: None,
            peermap: vec![],
            havemap_updated_at: None,
            peermap_updated_at: None,
            last_block_response: None,
            missing_havemap: false,
            missing_peermap: false,
            missing_share: false,
            bad_index: false,
            bad_blocks: false,
        }
    }
}

struct AdvertisedPeer {
    share_key: TypedKey,
    updated_at: Timestamp,
}

struct ChanClient<Request, Response> {
    tx: mpsc::Sender<Request>,
    rx: mpsc::Receiver<Response>,
}

struct ChanServer<Request, Response> {
    tx: mpsc::Sender<Response>,
    rx: mpsc::Receiver<Request>,
}

fn chan_rpc<Request, Response>(
    capacity: usize,
) -> (ChanClient<Request, Response>, ChanServer<Request, Response>) {
    let (client_tx, client_rx) = mpsc::channel(capacity);
    let (server_tx, server_rx) = mpsc::channel(capacity);
    (
        ChanClient {
            tx: client_tx,
            rx: server_rx,
        },
        ChanServer {
            tx: server_tx,
            rx: client_rx,
        },
    )
}
