use std::collections::HashMap;

use roaring::RoaringBitmap;
use stigmerge_fileindex::Index;
use veilid_core::{Target, Timestamp};

use crate::{
    peer::ShareKey,
    proto::{Digest, Header},
};

struct Tracker {
    /// Digest of the wanted index.
    want_index_digest: Digest,

    /// The wanted index, once resolved with a peer.
    /// It's authenticated by matching the index digest.
    want_index: Option<Index>,

    /// Collection of known remote peers.
    peers: HashMap<ShareKey, RemotePeerInfo>,
}

impl Tracker {
    fn new(want_index_digest: Digest, share_keys: Vec<ShareKey>) -> Tracker {
        Tracker {
            want_index_digest,
            want_index: None,
            peers: share_keys
                .iter()
                .map(|k| (k.clone(), RemotePeerInfo::new(*k)))
                .collect(),
        }
    }
}

struct RemotePeerInfo {
    share_key: ShareKey,

    havemap_key: Option<ShareKey>,
    peermap_key: Option<ShareKey>,

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
    fn new(share_key: ShareKey) -> Self {
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

    fn need_havemap_dht(&self) -> bool {
        if let None = self.havemap_key {
            true
        } else {
            false
        }
    }

    fn need_havemap(&self) -> bool {
        if let None = self.havemap {
            true
        } else {
            false
        }
    }
}

struct AdvertisedPeer {
    share_key: ShareKey,
    updated_at: Timestamp,
}
