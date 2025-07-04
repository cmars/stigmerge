use std::{collections::HashMap, fmt};

use anyhow::Context;
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace};
use veilid_core::{TypedRecordKey, VeilidUpdate};

use crate::{
    actor::{Actor, Respondable, ResponseChannel},
    error::{CancelError, Unrecoverable},
    proto::{self, Decoder},
    Node, Result,
};

/// The peer-announcer service handles requests to announce or redact remote
/// peers known by this peer.
///
/// Each remote peer is encoded to a separate subkey. Redacted peers are
/// marked with empty contents.
#[derive(Clone)]
pub struct PeerAnnouncer<N: Node> {
    node: N,
    payload_digest: Vec<u8>,
    peer_indexes: HashMap<TypedRecordKey, usize>,
    peers: Vec<Option<TypedRecordKey>>,
    max_peers: u16,
}

pub const DEFAULT_MAX_PEERS: u16 = 32;

impl<N: Node> PeerAnnouncer<N> {
    /// Create a new peer_announcer service.
    pub fn new(node: N, payload_digest: &[u8]) -> Self {
        Self {
            node,
            payload_digest: payload_digest.to_vec(),
            peer_indexes: HashMap::new(),
            peers: vec![],
            max_peers: DEFAULT_MAX_PEERS,
        }
    }

    fn assign_peer_index(&mut self, key: TypedRecordKey) -> u16 {
        for (i, maybe_key) in self.peers.iter_mut().enumerate() {
            if let None = maybe_key {
                *maybe_key = Some(key);

                self.peer_indexes.insert(key, i);
                return i.try_into().unwrap();
            }
        }
        self.peers.push(Some(key));
        let i = self.peers.len() - 1;
        self.peer_indexes.insert(key, i);
        i.try_into().unwrap()
    }
}

/// Peer-map announcer request messages.
pub enum Request {
    /// Announce a known remote peer in good standing.
    Announce {
        response_tx: ResponseChannel<Response>,
        key: TypedRecordKey,
    },

    /// Redact a known peer, it may be unavailable or defective
    /// from the point of view of this peer.
    Redact {
        response_tx: ResponseChannel<Response>,
        key: TypedRecordKey,
    },

    /// Clear all peer announcements.
    Reset {
        response_tx: ResponseChannel<Response>,
    },
}

impl fmt::Debug for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Announce {
                response_tx: _,
                key,
            } => f.debug_struct("Announce").field("key", key).finish(),
            Self::Redact {
                response_tx: _,
                key,
            } => f.debug_struct("Redact").field("key", key).finish(),
            Self::Reset { response_tx: _ } => f.debug_struct("Reset").finish(),
        }
    }
}

impl Respondable for Request {
    type Response = Response;

    fn set_response(&mut self, ch: ResponseChannel<Self::Response>) {
        match self {
            Request::Announce { response_tx, .. } => *response_tx = ch,
            Request::Redact { response_tx, .. } => *response_tx = ch,
            Request::Reset { response_tx } => *response_tx = ch,
        }
    }

    fn response_tx(self) -> ResponseChannel<Self::Response> {
        match self {
            Request::Announce { response_tx, .. } => response_tx,
            Request::Redact { response_tx, .. } => response_tx,
            Request::Reset { response_tx } => response_tx,
        }
    }
}

/// Peer-map announcer response message, just an acknowledgement or error.
#[derive(Clone, Debug, PartialEq)]
pub enum Response {
    Ok,
    Err { err_msg: String },
}

impl<P: Node> Actor for PeerAnnouncer<P> {
    type Request = Request;
    type Response = Response;

    async fn run(
        &mut self,
        cancel: CancellationToken,
        request_rx: flume::Receiver<Self::Request>,
    ) -> Result<()> {
        let mut update_rx = self.node.subscribe_veilid_update();
        self.node
            .reset_peers(&self.payload_digest, self.max_peers)
            .await?;
        loop {
            select! {
                _ = cancel.cancelled() => {
                    return Err(CancelError.into());
                }
                res = request_rx.recv_async() => {
                    let req = res.with_context(|| format!("peer_announcer: receive request"))?;
                    self.handle_request(req).await?;
                }
                res = update_rx.recv() => {
                    let update = res.with_context(|| format!("peer_announcer: receive veilid update"))?;
                    match update {
                        VeilidUpdate::AppCall(veilid_app_call) => {
                            trace!("app_call: {:?}", veilid_app_call);
                            let req = proto::Request::decode(veilid_app_call.message())?;
                            match req {
                                proto::Request::AdvertisePeer(peer_req) => {
                                    self.handle_request(Request::Announce {
                                        response_tx: ResponseChannel::Drop,
                                        key: peer_req.key,
                                    }).await?;
                                    debug!("announced peer: {}", peer_req.key);
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
            }
        }
    }

    async fn handle_request(&mut self, req: Self::Request) -> Result<()> {
        match req {
            Request::Announce {
                key,
                mut response_tx,
            } => {
                let resp = if !self.peer_indexes.contains_key(&key) {
                    let index = self.assign_peer_index(key);
                    match self
                        .node
                        .announce_peer(&self.payload_digest, Some(key), index)
                        .await
                    {
                        Ok(_) => Response::Ok,
                        Err(e) => Response::Err {
                            err_msg: e.to_string(),
                        },
                    }
                } else {
                    Response::Ok
                };

                response_tx
                    .send(resp)
                    .await
                    .with_context(|| "peer_announcer: send response")?;
            }
            Request::Redact {
                key,
                mut response_tx,
            } => {
                let resp = if let Some(index) = self.peer_indexes.get(&key) {
                    let subkey = TryInto::<u16>::try_into(*index).unwrap();
                    match self
                        .node
                        .announce_peer(&self.payload_digest, None, subkey)
                        .await
                    {
                        Ok(_) => {
                            self.peers[*index] = None;
                            self.peer_indexes.remove(&key);
                            Response::Ok
                        }
                        Err(e) => Response::Err {
                            err_msg: e.to_string(),
                        },
                    }
                } else {
                    Response::Ok
                };

                response_tx
                    .send(resp)
                    .await
                    .with_context(|| "send response from peer announcer")?;
            }
            Request::Reset { mut response_tx } => {
                let resp = match self
                    .node
                    .reset_peers(&self.payload_digest, self.max_peers)
                    .await
                {
                    Ok(_) => {
                        self.peers.clear();
                        self.peer_indexes.clear();
                        Response::Ok
                    }
                    Err(e) => Response::Err {
                        err_msg: e.to_string(),
                    },
                };

                response_tx
                    .send(resp)
                    .await
                    .context(Unrecoverable::new("send response from peer_announcer"))?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        str::FromStr,
        sync::{Arc, Mutex, RwLock},
    };

    use tokio_util::sync::CancellationToken;
    use veilid_core::TypedRecordKey;

    use crate::{
        actor::{OneShot, Operator, ResponseChannel},
        peer_announcer::{PeerAnnouncer, Request, DEFAULT_MAX_PEERS},
        tests::StubNode,
    };

    #[tokio::test]
    async fn test_peer_announcer_announces_peer() {
        // Create a stub peer with a recording announce_peer_result
        let mut node = StubNode::new();
        let recorded_payload_digest = Arc::new(RwLock::new(None::<Vec<u8>>));
        let recorded_peer_key = Arc::new(RwLock::new(None::<TypedRecordKey>));
        let recorded_index = Arc::new(RwLock::new(None::<u16>));

        let recorded_payload_digest_clone = recorded_payload_digest.clone();
        let recorded_peer_key_clone = recorded_peer_key.clone();
        let recorded_index_clone = recorded_index.clone();

        let recorded_resets = Arc::new(RwLock::new(0u32));
        let recorded_resets_clone = recorded_resets.clone();
        node.reset_peers_result = Arc::new(Mutex::new(move |_payload_digest: &[u8], _subkeys| {
            let mut count = recorded_resets_clone.write().unwrap();
            *count += 1;
            Ok(())
        }));

        node.announce_peer_result = Arc::new(Mutex::new(
            move |payload_digest: &[u8], peer_key: Option<TypedRecordKey>, index: u16| {
                *recorded_payload_digest_clone.write().unwrap() = Some(payload_digest.to_vec());
                *recorded_peer_key_clone.write().unwrap() = peer_key;
                *recorded_index_clone.write().unwrap() = Some(index);
                Ok(())
            },
        ));

        // Create test data
        let payload_digest = vec![0xab; 32];
        let peer_key = TypedRecordKey::from_str("VLD0:dDHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M")
            .expect("key");

        // Create peer announcer
        let cancel = CancellationToken::new();
        let peer_announcer = PeerAnnouncer::new(node.clone(), &payload_digest);
        let mut operator = Operator::new(cancel.clone(), peer_announcer, OneShot);

        // Send an Announce request
        let req = Request::Announce {
            response_tx: ResponseChannel::default(),
            key: peer_key.clone(),
        };
        operator.call(req).await.expect("call");

        // Verify the peer was announced correctly
        let recorded_payload_digest = recorded_payload_digest.read().unwrap();
        let recorded_peer_key = recorded_peer_key.read().unwrap();
        let recorded_index = recorded_index.read().unwrap();

        assert!(
            recorded_payload_digest.is_some(),
            "Payload digest was not recorded"
        );
        assert!(recorded_peer_key.is_some(), "Peer key was not recorded");
        assert!(recorded_index.is_some(), "Index was not recorded");
        assert_eq!(recorded_payload_digest.as_ref(), Some(&payload_digest));
        assert_eq!(recorded_peer_key.as_ref(), Some(&peer_key));
        assert_eq!(recorded_index.as_ref(), Some(&0));

        assert_eq!(*recorded_resets.read().unwrap(), 1u32);

        // Clean up
        cancel.cancel();
        operator.join().await.expect_err("cancelled");
    }

    #[tokio::test]
    async fn test_peer_announcer_redacts_peer() {
        // Create a stub peer with recording announce_peer_result
        let mut node = StubNode::new();
        let recorded_payload_digest = Arc::new(RwLock::new(None::<Vec<u8>>));
        let recorded_peer_key = Arc::new(RwLock::new(None::<TypedRecordKey>));
        let recorded_index = Arc::new(RwLock::new(None::<u16>));

        let recorded_payload_digest_clone = recorded_payload_digest.clone();
        let recorded_peer_key_clone = recorded_peer_key.clone();
        let recorded_index_clone = recorded_index.clone();

        let recorded_resets = Arc::new(RwLock::new(0u32));
        let recorded_resets_clone = recorded_resets.clone();
        node.reset_peers_result = Arc::new(Mutex::new(move |_payload_digest: &[u8], _subkeys| {
            let mut count = recorded_resets_clone.write().unwrap();
            *count += 1;
            Ok(())
        }));

        node.announce_peer_result = Arc::new(Mutex::new(
            move |payload_digest: &[u8], peer_key: Option<TypedRecordKey>, index: u16| {
                *recorded_payload_digest_clone.write().unwrap() = Some(payload_digest.to_vec());
                *recorded_peer_key_clone.write().unwrap() = peer_key;
                *recorded_index_clone.write().unwrap() = Some(index);
                Ok(())
            },
        ));

        // Create test data
        let payload_digest = vec![0xab; 32];
        let peer_key = TypedRecordKey::from_str("VLD0:dDHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M")
            .expect("key");

        // Create peer announcer
        let cancel = CancellationToken::new();
        let peer_announcer = PeerAnnouncer::new(node.clone(), &payload_digest);
        let mut operator = Operator::new(cancel.clone(), peer_announcer, OneShot);

        // First announce a peer
        let req_announce = Request::Announce {
            response_tx: ResponseChannel::default(),
            key: peer_key.clone(),
        };
        operator.call(req_announce).await.expect("call");

        // Then redact it
        let req_redact = Request::Redact {
            response_tx: ResponseChannel::default(),
            key: peer_key.clone(),
        };
        operator.call(req_redact).await.expect("call");

        // Verify the peer was redacted correctly
        let recorded_payload_digest = recorded_payload_digest.read().unwrap();
        let recorded_peer_key = recorded_peer_key.read().unwrap();
        let recorded_index = recorded_index.read().unwrap();

        assert!(
            recorded_payload_digest.is_some(),
            "Payload digest was not recorded"
        );
        assert!(recorded_peer_key.is_none(), "Peer key was not recorded");
        assert!(recorded_index.is_some(), "Index was not recorded");
        assert_eq!(recorded_payload_digest.as_ref(), Some(&payload_digest));
        assert_eq!(recorded_peer_key.as_ref(), None); // Redacted peer has None
        assert_eq!(recorded_index.as_ref(), Some(&0));

        assert_eq!(*recorded_resets.read().unwrap(), 1u32);

        // Clean up
        cancel.cancel();
        operator.join().await.expect_err("cancelled");
    }

    #[tokio::test]
    async fn test_peer_announcer_resets_peers() {
        // Create a stub peer with recording reset_peers_result
        let mut node = StubNode::new();
        let recorded_payload_digest = Arc::new(RwLock::new(None::<Vec<u8>>));
        let recorded_max_peers = Arc::new(RwLock::new(None::<u16>));

        let recorded_payload_digest_clone = recorded_payload_digest.clone();
        let recorded_max_peers_clone = recorded_max_peers.clone();

        node.reset_peers_result =
            Arc::new(Mutex::new(move |payload_digest: &[u8], max_peers: u16| {
                *recorded_payload_digest_clone.write().unwrap() = Some(payload_digest.to_vec());
                *recorded_max_peers_clone.write().unwrap() = Some(max_peers);
                Ok(())
            }));

        // Create test data
        let payload_digest = vec![0xab; 32];

        // Create peer announcer
        let cancel = CancellationToken::new();
        let peer_announcer = PeerAnnouncer::new(node.clone(), &payload_digest);
        let mut operator = Operator::new(cancel.clone(), peer_announcer, OneShot);

        // Send a Reset request
        let req = Request::Reset {
            response_tx: ResponseChannel::default(),
        };
        operator.call(req).await.expect("call");

        // Verify the peers were reset correctly
        let recorded_payload_digest = recorded_payload_digest.read().unwrap();
        let recorded_max_peers = recorded_max_peers.read().unwrap();

        assert!(
            recorded_payload_digest.is_some(),
            "Payload digest was not recorded"
        );
        assert!(recorded_max_peers.is_some(), "Max peers was not recorded");
        assert_eq!(recorded_payload_digest.as_ref(), Some(&payload_digest));
        assert_eq!(recorded_max_peers.as_ref(), Some(&DEFAULT_MAX_PEERS));

        // Clean up
        cancel.cancel();
        operator.join().await.expect_err("cancelled");
    }
}
