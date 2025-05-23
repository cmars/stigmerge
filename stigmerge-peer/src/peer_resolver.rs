use std::collections::HashMap;

use tokio::{select, sync::broadcast};
use tokio_util::sync::CancellationToken;
use tracing::{info, Level};
use veilid_core::ValueSubkeyRangeSet;

use crate::{
    actor::{Actor, ChanServer},
    node::TypedKey,
    proto::{Decoder, PeerInfo},
    Node, Result,
};

/// The peer_resolver service handles requests for remote peer maps, which
/// indicate which other peers a remote peer knows about.
pub struct PeerResolver<N: Node> {
    node: N,
    updates: broadcast::Receiver<veilid_core::VeilidUpdate>,

    // Mapping from peer-map key to share key, used in watches
    peer_to_share_map: HashMap<TypedKey, TypedKey>,
}

impl<P> PeerResolver<P>
where
    P: Node,
{
    /// Create a new peer_resolver service.
    pub fn new(node: P) -> Self {
        let updates = node.subscribe_veilid_update();
        Self {
            node,
            updates,
            peer_to_share_map: HashMap::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Request {
    /// Resolve the known peers for the peer at the given share key. This will
    /// result in a response containing the peer info, or a "not available"
    /// response.
    Resolve { key: TypedKey },

    /// Watch for changes in known peers at the given share key. This will
    /// result in a series of resolve responses being sent as the peers change.
    Watch { key: TypedKey },

    /// Cancel the peer watch on the share key.
    CancelWatch { key: TypedKey },
}

impl Request {
    fn key(&self) -> &TypedKey {
        match self {
            Request::Resolve { key } => key,
            Request::Watch { key } => key,
            Request::CancelWatch { key } => key,
        }
    }
}

#[derive(Clone, Debug)]
pub enum Response {
    NotAvailable {
        key: TypedKey,
        err_msg: String,
    },
    Resolve {
        key: TypedKey,
        peers: HashMap<TypedKey, PeerInfo>,
    },
    Watching {
        key: TypedKey,
    },
    WatchCancelled {
        key: TypedKey,
    },
}

impl<P: Node> Actor for PeerResolver<P> {
    type Request = Request;
    type Response = Response;

    /// Run the service until cancelled.
    #[tracing::instrument(skip_all, err, level = Level::TRACE)]
    async fn run(
        &mut self,
        cancel: CancellationToken,
        mut server_ch: ChanServer<Self::Request, Self::Response>,
    ) -> Result<()> {
        loop {
            select! {
                _ = cancel.cancelled() => {
                    return Ok(())
                }
                res = server_ch.recv() => {
                    let req = match res {
                        None => return Ok(()),
                        Some(req) => req,
                    };
                    match self.handle(&req).await {
                        Ok(resp) => {
                            server_ch.send(resp).await?
                        }
                        Err(err) => server_ch.send(Response::NotAvailable{key: req.key().clone(), err_msg: err.to_string()}).await?,
                    }
                }
                res = self.updates.recv() => {
                    let update = res?;
                    match update {
                        veilid_core::VeilidUpdate::ValueChange(ch) => {
                            if let Some(data) = ch.value {
                                let share_key = match self.peer_to_share_map.get(&ch.key) {
                                    Some(key) => key,
                                    None => continue,
                                };
                                if data.data_size() > 0 {
                                    if let Ok(peer_info) = PeerInfo::decode(data.data()) {
                                        server_ch.send(Response::Resolve{
                                            key: *share_key,
                                            peers: [(peer_info.key().to_owned(), peer_info)].into_iter().collect::<HashMap<_,_>>()
                                        }).await?;
                                    }
                                }
                            }
                        }
                        veilid_core::VeilidUpdate::Shutdown => {
                            return Ok(());
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    /// Handle a peer_resolver request, provide a response.
    #[tracing::instrument(skip_all, err(level = Level::TRACE), level = Level::TRACE)]
    async fn handle(&mut self, req: &Request) -> Result<Response> {
        Ok(match req {
            Request::Resolve { key } => {
                let peers = self.node.resolve_peers(key).await?;
                Response::Resolve {
                    key: key.to_owned(),
                    peers: peers
                        .into_iter()
                        .map(|peer| (peer.key().to_owned(), peer))
                        .collect::<HashMap<TypedKey, PeerInfo>>(),
                }
            }
            Request::Watch { key } => {
                let (_, header) = self.node.resolve_route(key, None).await?;
                if let Some(peer_map_ref) = header.peer_map() {
                    info!("watch: peer_map key {}", peer_map_ref.key());
                    self.peer_to_share_map
                        .insert(peer_map_ref.key().to_owned(), key.to_owned());
                    self.node
                        .watch(peer_map_ref.key().to_owned(), ValueSubkeyRangeSet::full())
                        .await?;
                    Response::Watching {
                        key: key.to_owned(),
                    }
                } else {
                    Response::NotAvailable {
                        key: key.to_owned(),
                        err_msg: format!("peer {key} does not publish peers"),
                    }
                }
            }
            Request::CancelWatch { key } => {
                let (_, header) = self.node.resolve_route(key, None).await?;
                if let Some(peer_map_ref) = header.peer_map() {
                    self.peer_to_share_map.remove(peer_map_ref.key());
                    self.node.cancel_watch(peer_map_ref.key()).await?;
                }
                Response::WatchCancelled {
                    key: key.to_owned(),
                }
            }
        })
    }
}

impl<P: Node> Clone for PeerResolver<P> {
    fn clone(&self) -> Self {
        Self {
            node: self.node.clone(),
            updates: self.updates.resubscribe(),
            peer_to_share_map: self.peer_to_share_map.clone(),
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
    use veilid_core::{TypedKey, ValueSubkeyRangeSet};

    use crate::{
        actor::{OneShot, Operator},
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
            TypedKey::from_str("VLD0:dDHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");
        let test_peer_info = PeerInfo::new(test_peer_key.clone());

        node.resolve_peers_result = Arc::new(Mutex::new(move |key: &TypedKey| {
            *recorded_key_clone.write().unwrap() = Some(key.to_owned());
            Ok(vec![test_peer_info.clone()])
        }));

        // Create a test key and channel
        let test_key =
            TypedKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");

        // Create peer resolver
        let cancel = CancellationToken::new();
        let peer_resolver = PeerResolver::new(node);
        let mut operator = Operator::new(cancel.clone(), peer_resolver, OneShot);

        // Send a Resolve request
        let req = Request::Resolve {
            key: test_key.clone(),
        };
        operator.send(req).await.unwrap();

        // Verify the response
        match operator.recv().await.expect("recv") {
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
        operator.join().await.expect("task").expect("run");
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
            TypedKey::from_str("VLD0:eCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");
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
            move |_key: &TypedKey, _prior_route: Option<veilid_core::Target>| {
                Ok((
                    veilid_core::Target::PrivateRoute(veilid_core::CryptoKey::new([0u8; 32])),
                    header.clone(),
                ))
            },
        ));

        node.watch_result = Arc::new(Mutex::new(
            move |key: TypedKey, subkeys: ValueSubkeyRangeSet| {
                *recorded_key_clone.write().unwrap() = Some(key);
                *recorded_subkeys_clone.write().unwrap() = Some(subkeys);
                Ok(())
            },
        ));

        // Create a test key and channel
        let test_key =
            TypedKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");

        // Create peer resolver
        let cancel = CancellationToken::new();
        let peer_resolver = PeerResolver::new(node.clone());
        let mut operator = Operator::new(cancel.clone(), peer_resolver, OneShot);

        // Send a Watch request
        let req = Request::Watch {
            key: test_key.clone(),
        };
        operator.send(req).await.unwrap();

        // Verify the response
        match operator.recv().await.expect("recv") {
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
        operator.join().await.expect("task").expect("run");
    }

    #[tokio::test]
    async fn test_peer_resolver_cancels_watch() {
        // Create a stub peer with a recording cancel_watch_result
        let mut node = StubNode::new();
        let recorded_key = Arc::new(RwLock::new(None));

        let recorded_key_clone = recorded_key.clone();
        node.cancel_watch_result = Arc::new(Mutex::new(move |key: &TypedKey| {
            *recorded_key_clone.write().unwrap() = Some(key.clone());
            Ok(())
        }));

        // Setup the resolve_route_result to return a header with a peer_map
        let peer_map_key =
            TypedKey::from_str("VLD0:eCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");
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
            move |_key: &TypedKey, _prior_route: Option<veilid_core::Target>| {
                Ok((
                    veilid_core::Target::PrivateRoute(veilid_core::CryptoKey::new([0u8; 32])),
                    header.clone(),
                ))
            },
        ));

        // Create a test key and channel
        let test_key =
            TypedKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");

        // Create peer resolver
        let cancel = CancellationToken::new();
        let peer_resolver = PeerResolver::new(node.clone());
        let mut operator = Operator::new(cancel.clone(), peer_resolver, OneShot);

        // Send a CancelWatch request
        let req = Request::CancelWatch {
            key: test_key.clone(),
        };
        operator.send(req).await.unwrap();

        // Verify the response
        match operator.recv().await.expect("recv") {
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
        operator.join().await.expect("task").expect("run");
    }

    #[tokio::test]
    async fn test_peer_resolver_handles_value_changes() {
        // Create a stub peer
        let mut node = StubNode::new();

        node.watch_result = Arc::new(Mutex::new(
            move |_key: TypedKey, _subkeys: ValueSubkeyRangeSet| Ok(()),
        ));

        // Create a test key and channel
        let test_key =
            TypedKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");
        let peer_key =
            TypedKey::from_str("VLD0:dDHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");

        // Setup the resolve_route_result to return a header with a peer_map
        let peer_map_key =
            TypedKey::from_str("VLD0:eCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");
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
            move |_key: &TypedKey, _prior_route: Option<veilid_core::Target>| {
                Ok((
                    veilid_core::Target::PrivateRoute(veilid_core::CryptoKey::new([0u8; 32])),
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
            key: test_key.clone(),
        };
        operator.send(req).await.unwrap();
        let resp = operator.recv().await;
        assert!(matches!(resp, Some(_)), "{:?}", resp);

        // Create a PeerInfo object to include in the value change
        let peer_info = PeerInfo::new(peer_key.clone());
        let encoded_peer_info = peer_info.encode().expect("encode peer info");

        // Simulate a value change notification
        let crypto_key = veilid_core::CryptoKey::new([0xbe; veilid_core::CRYPTO_KEY_LENGTH]);
        let value_data =
            veilid_core::ValueData::new(encoded_peer_info, crypto_key).expect("new value data");

        let change = veilid_core::VeilidValueChange {
            key: peer_map_key.clone(),
            subkeys: ValueSubkeyRangeSet::single_range(0, 0),
            value: Some(value_data),
            count: 1,
        };

        update_tx
            .send(veilid_core::VeilidUpdate::ValueChange(Box::new(change)))
            .expect("send value change");

        // Verify the response - we should receive a Resolve response with the peer info
        match operator.recv().await.expect("recv") {
            Response::Resolve { key, peers } => {
                assert_eq!(key, test_key);
                assert_eq!(peers.len(), 1);
                assert!(peers.contains_key(&peer_key));
                let received_peer_info = peers.get(&peer_key).unwrap();
                assert_eq!(received_peer_info.key(), &peer_key);
            }
            other => panic!("Unexpected response: {:?}", other),
        }

        // Clean up
        cancel.cancel();
        operator.join().await.expect("task").expect("run");
    }
}
