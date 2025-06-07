use std::collections::HashMap;

use anyhow::Context;
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace};
use veilid_core::{TypedRecordKey, ValueSubkeyRangeSet, VeilidUpdate};

use crate::{
    actor::{Actor, Respondable, ResponseChannel},
    error::{CancelError, Unrecoverable},
    proto::{self, Decoder, PeerInfo},
    Node, Result,
};

/// The peer_resolver service handles requests for remote peer maps, which
/// indicate which other peers a remote peer knows about.
pub struct PeerResolver<N: Node> {
    node: N,

    // Mapping from peer-map key to share key, used in watches
    peer_to_share_map: HashMap<TypedRecordKey, TypedRecordKey>,

    discovered_peer_tx: flume::Sender<(TypedRecordKey, PeerInfo)>,
    discovered_peer_rx: flume::Receiver<(TypedRecordKey, PeerInfo)>,
}

impl<P> PeerResolver<P>
where
    P: Node,
{
    /// Create a new peer_resolver service.
    pub fn new(node: P) -> Self {
        let (discovered_peer_tx, discovered_peer_rx) = flume::unbounded();
        Self {
            node,
            peer_to_share_map: HashMap::new(),
            discovered_peer_tx,
            discovered_peer_rx,
        }
    }

    pub fn subscribe_discovered_peers(&self) -> flume::Receiver<(TypedRecordKey, PeerInfo)> {
        self.discovered_peer_rx.clone()
    }
}

pub enum Request {
    /// Resolve the known peers for the peer at the given share key. This will
    /// result in a response containing the peer info, or a "not available"
    /// response.
    Resolve {
        response_tx: ResponseChannel<Response>,
        key: TypedRecordKey,
    },

    /// Watch for changes in known peers at the given share key. This will
    /// result in a series of resolve responses being sent as the peers change.
    Watch {
        response_tx: ResponseChannel<Response>,
        key: TypedRecordKey,
    },

    /// Cancel the peer watch on the share key.
    CancelWatch {
        response_tx: ResponseChannel<Response>,
        key: TypedRecordKey,
    },
}

impl Respondable for Request {
    type Response = Response;

    fn set_response(&mut self, ch: ResponseChannel<Self::Response>) {
        match self {
            Request::Resolve { response_tx, .. } => *response_tx = ch,
            Request::Watch { response_tx, .. } => *response_tx = ch,
            Request::CancelWatch { response_tx, .. } => *response_tx = ch,
        }
    }

    fn response_tx(self) -> ResponseChannel<Self::Response> {
        match self {
            Request::Resolve { response_tx, .. } => response_tx,
            Request::Watch { response_tx, .. } => response_tx,
            Request::CancelWatch { response_tx, .. } => response_tx,
        }
    }
}

#[derive(Clone, Debug)]
pub enum Response {
    NotAvailable {
        key: TypedRecordKey,
        err_msg: String,
    },
    Resolve {
        key: TypedRecordKey,
        peers: HashMap<TypedRecordKey, PeerInfo>,
    },
    Watching {
        key: TypedRecordKey,
    },
    WatchCancelled {
        key: TypedRecordKey,
    },
}

impl<P: Node> Actor for PeerResolver<P> {
    type Request = Request;
    type Response = Response;

    /// Run the service until cancelled.
    async fn run(
        &mut self,
        cancel: CancellationToken,
        request_rx: flume::Receiver<Self::Request>,
    ) -> Result<()> {
        let mut update_rx = self.node.subscribe_veilid_update();
        loop {
            select! {
                _ = cancel.cancelled() => {
                    return Err(CancelError.into());
                }
                res = request_rx.recv_async() => {
                    let req = res.with_context(|| format!("peer_resolver: receive request"))?;
                    self.handle_request(req).await?;
                }
                res = update_rx.recv() => {
                    let update = res.with_context(|| format!("peer_resolver: receive veilid update"))?;
                    match update {
                        VeilidUpdate::AppCall(veilid_app_call) => {
                            trace!("app_call: {:?}", veilid_app_call);
                            let req = proto::Request::decode(veilid_app_call.message())?;
                            match req {
                                proto::Request::AdvertisePeer(peer_req) => {
                                    self.handle_request(Request::Watch {
                                        response_tx: ResponseChannel::Drop,
                                        key: peer_req.key,
                                    }).await?;
                                    debug!("watching advertised peer: {}", peer_req.key);
                                }
                                _ => {}
                            }
                        }
                        VeilidUpdate::ValueChange(ch) => {
                            if let Some(data) = ch.value {
                                let share_key = match self.peer_to_share_map.get(&ch.key) {
                                    Some(key) => key,
                                    None => continue,
                                };
                                if data.data_size() > 0 {
                                    if let Ok(peer_info) = PeerInfo::decode(data.data()) {
                                        self.discovered_peer_tx.send_async((share_key.to_owned(), peer_info)).await
                                            .with_context(|| format!("peer_resolver: send discovered peer"))?;
                                    }
                                }
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

    /// Handle a peer_resolver request, provide a response.
    async fn handle_request(&mut self, req: Self::Request) -> Result<()> {
        match req {
            Request::Resolve {
                key,
                mut response_tx,
            } => {
                let resp = match self.node.resolve_peers(&key).await {
                    Ok(peers) => Response::Resolve {
                        key: key.to_owned(),
                        peers: peers
                            .into_iter()
                            .map(|peer| (peer.key().to_owned(), peer))
                            .collect::<HashMap<TypedRecordKey, PeerInfo>>(),
                    },
                    Err(err) => Response::NotAvailable {
                        key: key.to_owned(),
                        err_msg: err.to_string(),
                    },
                };

                response_tx
                    .send(resp)
                    .await
                    .with_context(|| "peer_resolver: send response")?;
            }
            Request::Watch {
                key,
                mut response_tx,
            } => {
                let resp = match self.node.resolve_route(&key, None).await {
                    Ok((_, header)) => {
                        if let Some(peer_map_ref) = header.peer_map() {
                            info!("watch: peer_map key {}", peer_map_ref.key());
                            self.peer_to_share_map
                                .insert(peer_map_ref.key().to_owned(), key.to_owned());
                            match self
                                .node
                                .watch(peer_map_ref.key().to_owned(), ValueSubkeyRangeSet::full())
                                .await
                            {
                                Ok(_) => Response::Watching {
                                    key: key.to_owned(),
                                },
                                Err(err) => Response::NotAvailable {
                                    key: key.to_owned(),
                                    err_msg: err.to_string(),
                                },
                            }
                        } else {
                            Response::NotAvailable {
                                key: key.to_owned(),
                                err_msg: format!("peer {key} does not publish peers"),
                            }
                        }
                    }
                    Err(err) => Response::NotAvailable {
                        key: key.to_owned(),
                        err_msg: err.to_string(),
                    },
                };

                response_tx
                    .send(resp)
                    .await
                    .with_context(|| "peer_resolver: send response")?;
            }
            Request::CancelWatch {
                key,
                mut response_tx,
            } => {
                let resp = match self.node.resolve_route(&key, None).await {
                    Ok((_, header)) => {
                        if let Some(peer_map_ref) = header.peer_map() {
                            self.peer_to_share_map.remove(peer_map_ref.key());
                            match self.node.cancel_watch(peer_map_ref.key()).await {
                                Ok(_) => (),
                                Err(_) => (), // Ignore errors when cancelling watch
                            }
                        }
                        Response::WatchCancelled {
                            key: key.to_owned(),
                        }
                    }
                    Err(_) => {
                        // Even if we can't resolve the route, we still want to acknowledge the cancel
                        Response::WatchCancelled {
                            key: key.to_owned(),
                        }
                    }
                };

                response_tx
                    .send(resp)
                    .await
                    .context(Unrecoverable::new("send response from peer resolver"))?;
            }
        }

        Ok(())
    }
}

impl<P: Node> Clone for PeerResolver<P> {
    fn clone(&self) -> Self {
        Self {
            node: self.node.clone(),
            peer_to_share_map: self.peer_to_share_map.clone(),
            discovered_peer_tx: self.discovered_peer_tx.clone(),
            discovered_peer_rx: self.discovered_peer_rx.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        str::FromStr,
        sync::{Arc, Mutex, RwLock},
    };

    use tokio_util::sync::CancellationToken;
    use veilid_core::{PublicKey, RouteId, TypedRecordKey, ValueSubkeyRangeSet};

    use crate::{
        actor::{OneShot, Operator, ResponseChannel},
        peer_resolver::{PeerResolver, Request, Response},
        proto::{Encoder, PeerInfo},
        tests::StubNode,
    };

    #[tokio::test]
    async fn test_peer_resolver_resolves_peers() {
        // Create a stub peer with a recording resolve_peers_result
        let mut node = StubNode::new();
        let recorded_key = Arc::new(RwLock::new(None));

        let recorded_key_clone = recorded_key.clone();

        // Create a test peer key to return
        let test_peer_key =
            TypedRecordKey::from_str("VLD0:dDHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M")
                .expect("key");
        let test_peer_info = PeerInfo::new(test_peer_key.clone());

        node.resolve_peers_result = Arc::new(Mutex::new(move |key: &TypedRecordKey| {
            *recorded_key_clone.write().unwrap() = Some(key.to_owned());
            Ok(vec![test_peer_info.clone()])
        }));

        // Create a test key and channel
        let test_key = TypedRecordKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M")
            .expect("key");

        // Create peer resolver
        let cancel = CancellationToken::new();
        let peer_resolver = PeerResolver::new(node);
        let mut operator = Operator::new(cancel.clone(), peer_resolver, OneShot);

        // Send a Resolve request
        let req = Request::Resolve {
            response_tx: ResponseChannel::default(),
            key: test_key.clone(),
        };
        let resp = operator.call(req).await.expect("call");

        // Verify the response
        match resp {
            Response::Resolve { key, peers } => {
                assert_eq!(key, test_key);
                assert_eq!(peers.len(), 1);
                assert!(peers.contains_key(&test_peer_key));
                let peer_info = peers.get(&test_peer_key).unwrap();
                assert_eq!(peer_info.key(), &test_peer_key);
            }
            other => panic!("Unexpected response: {:?}", other),
        }

        // Verify the resolve was called correctly
        let recorded_key = recorded_key.read().unwrap();

        assert!(recorded_key.is_some(), "Key was not recorded");
        assert_eq!(recorded_key.unwrap(), test_key);

        // Clean up
        cancel.cancel();
        operator.join().await.expect("task");
    }

    #[tokio::test]
    async fn test_peer_resolver_watches_peers() {
        // Create a stub peer with a recording watch_result
        let mut node = StubNode::new();
        let recorded_key = Arc::new(RwLock::new(None));
        let recorded_subkeys = Arc::new(RwLock::new(None));

        let recorded_key_clone = recorded_key.clone();
        let recorded_subkeys_clone = recorded_subkeys.clone();

        // Setup the resolve_route_result to return a header with a peer_map
        let peer_map_key =
            TypedRecordKey::from_str("VLD0:eCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M")
                .expect("key");
        let peer_map_ref = crate::proto::PeerMapRef::new(peer_map_key.clone(), 0);
        let header = crate::proto::Header::new(
            [0u8; 32],          // payload_digest
            0,                  // payload_length
            0,                  // subkeys
            &[],                // route_data
            None,               // have_map
            Some(peer_map_ref), // peer_map
        );

        node.resolve_route_result = Arc::new(Mutex::new(
            move |_key: &TypedRecordKey, _prior_route: Option<veilid_core::Target>| {
                Ok((
                    veilid_core::Target::PrivateRoute(RouteId::new([0u8; 32])),
                    header.clone(),
                ))
            },
        ));

        node.watch_result = Arc::new(Mutex::new(
            move |key: TypedRecordKey, subkeys: ValueSubkeyRangeSet| {
                *recorded_key_clone.write().unwrap() = Some(key);
                *recorded_subkeys_clone.write().unwrap() = Some(subkeys);
                Ok(())
            },
        ));

        // Create a test key and channel
        let test_key = TypedRecordKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M")
            .expect("key");

        // Create peer resolver
        let cancel = CancellationToken::new();
        let peer_resolver = PeerResolver::new(node.clone());
        let mut operator = Operator::new(cancel.clone(), peer_resolver, OneShot);

        // Send a Watch request
        let req = Request::Watch {
            response_tx: ResponseChannel::default(),
            key: test_key.clone(),
        };
        let resp = operator.call(req).await.expect("call");

        // Verify the response
        match resp {
            Response::Watching { key } => {
                assert_eq!(key, test_key);
            }
            other => panic!("Unexpected response: {:?}", other),
        }

        // Verify the watch was called correctly
        let recorded_key = recorded_key.read().unwrap();
        let recorded_subkeys = recorded_subkeys.read().unwrap();

        assert!(recorded_key.is_some(), "Key was not recorded");
        assert!(recorded_subkeys.is_some(), "Subkeys were not recorded");
        assert_eq!(recorded_key.as_ref().unwrap(), &peer_map_key);
        assert_eq!(
            recorded_subkeys.as_ref().unwrap(),
            &ValueSubkeyRangeSet::full()
        );

        // Clean up
        cancel.cancel();
        operator.join().await.expect("task");
    }

    #[tokio::test]
    async fn test_peer_resolver_cancels_watch() {
        // Create a stub peer with a recording cancel_watch_result
        let mut node = StubNode::new();
        let recorded_key = Arc::new(RwLock::new(None));

        let recorded_key_clone = recorded_key.clone();
        node.cancel_watch_result = Arc::new(Mutex::new(move |key: &TypedRecordKey| {
            *recorded_key_clone.write().unwrap() = Some(key.clone());
            Ok(())
        }));

        // Setup the resolve_route_result to return a header with a peer_map
        let peer_map_key =
            TypedRecordKey::from_str("VLD0:eCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M")
                .expect("key");
        let peer_map_ref = crate::proto::PeerMapRef::new(peer_map_key.clone(), 0);
        let header = crate::proto::Header::new(
            [0u8; 32],          // payload_digest
            0,                  // payload_length
            0,                  // subkeys
            &[],                // route_data
            None,               // have_map
            Some(peer_map_ref), // peer_map
        );

        node.resolve_route_result = Arc::new(Mutex::new(
            move |_key: &TypedRecordKey, _prior_route: Option<veilid_core::Target>| {
                Ok((
                    veilid_core::Target::PrivateRoute(RouteId::new([0u8; 32])),
                    header.clone(),
                ))
            },
        ));

        // Create a test key and channel
        let test_key = TypedRecordKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M")
            .expect("key");

        // Create peer resolver
        let cancel = CancellationToken::new();
        let peer_resolver = PeerResolver::new(node.clone());
        let mut operator = Operator::new(cancel.clone(), peer_resolver, OneShot);

        // Send a CancelWatch request
        let req = Request::CancelWatch {
            response_tx: ResponseChannel::default(),
            key: test_key.clone(),
        };
        let resp = operator.call(req).await.expect("call");

        // Verify the response
        match resp {
            Response::WatchCancelled { key } => {
                assert_eq!(key, test_key);
            }
            other => panic!("Unexpected response: {:?}", other),
        }

        // Verify the cancel was called correctly
        let recorded_key = recorded_key.read().unwrap();
        assert!(recorded_key.is_some(), "Key was not recorded");
        assert_eq!(recorded_key.as_ref().unwrap(), &peer_map_key);

        // Clean up
        cancel.cancel();
        operator.join().await.expect("task");
    }

    #[tokio::test]
    async fn test_peer_resolver_handles_value_changes() {
        // Create a stub peer
        let mut node = StubNode::new();

        node.watch_result = Arc::new(Mutex::new(
            move |_key: TypedRecordKey, _subkeys: ValueSubkeyRangeSet| Ok(()),
        ));

        // Create a test key and channel
        let test_key = TypedRecordKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M")
            .expect("key");
        let peer_key = TypedRecordKey::from_str("VLD0:dDHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M")
            .expect("key");

        // Setup the resolve_route_result to return a header with a peer_map
        let peer_map_key =
            TypedRecordKey::from_str("VLD0:eCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M")
                .expect("key");
        let peer_map_ref = crate::proto::PeerMapRef::new(peer_map_key.clone(), 1);
        let header = crate::proto::Header::new(
            [0u8; 32],                 // payload_digest
            42,                        // payload_length
            1,                         // subkeys
            &[0xde, 0xad, 0xbe, 0xef], // route_data
            None,                      // have_map
            Some(peer_map_ref),        // peer_map
        );

        node.resolve_route_result = Arc::new(Mutex::new(
            move |_key: &TypedRecordKey, _prior_route: Option<veilid_core::Target>| {
                Ok((
                    veilid_core::Target::PrivateRoute(RouteId::new([0u8; 32])),
                    header.clone(),
                ))
            },
        ));

        // Create peer resolver
        let update_tx = node.update_tx.clone();
        let cancel = CancellationToken::new();
        let peer_resolver = PeerResolver::new(node);
        let mut operator = Operator::new(cancel.clone(), peer_resolver, OneShot);

        // Send a request and receive a response, to make sure the task is
        // running. That's important: if we're not in the run loop, the update
        // send may fail to broadcast.
        let req = Request::Watch {
            response_tx: ResponseChannel::default(),
            key: test_key.clone(),
        };
        let resp = operator.call(req).await.expect("call");

        // Verify the response
        match resp {
            Response::Watching { key } => {
                assert_eq!(key, test_key);
            }
            other => panic!("Unexpected response: {:?}", other),
        }

        // Create a PeerInfo object to include in the value change
        let peer_info = PeerInfo::new(peer_key.clone());
        let encoded_peer_info = peer_info.encode().expect("encode peer info");

        // Simulate a value change notification
        let signing_key = PublicKey::new([0xbe; veilid_core::CRYPTO_KEY_LENGTH]);
        let value_data =
            veilid_core::ValueData::new(encoded_peer_info, signing_key).expect("new value data");

        let change = veilid_core::VeilidValueChange {
            key: peer_map_key.clone(),
            subkeys: ValueSubkeyRangeSet::single_range(0, 0),
            value: Some(value_data),
            count: 1,
        };

        update_tx
            .send(veilid_core::VeilidUpdate::ValueChange(Box::new(change)))
            .expect("send value change");

        // We can't verify the response since we're using call() now

        // Clean up
        cancel.cancel();
        operator.join().await.expect("task");
    }
}
