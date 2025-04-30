use std::collections::HashMap;

use tokio::{select, sync::broadcast};
use tokio_util::sync::CancellationToken;
use tracing::info;
use veilid_core::{TimestampDuration, ValueSubkeyRangeSet, VeilidUpdate};

use crate::{
    actor::{Actor, ChanServer},
    node::TypedKey,
    piece_map::PieceMap,
    Node, Result,
};

/// The have_resolver service handles requests for remote peer have-maps, which
/// indicate what pieces of a share the peer might have.
///
/// This service operates on the have-map reference keys indicated in the main
/// share DHT header (subkey 0) as haveMapRef.
pub struct HaveResolver<N: Node> {
    node: N,
    updates: broadcast::Receiver<VeilidUpdate>,

    // Mapping from have-map key to share key, used in watches
    have_to_share_map: HashMap<TypedKey, TypedKey>,
}

impl<P> HaveResolver<P>
where
    P: Node,
{
    /// Create a new have_resolver service with the given peer.
    pub(super) fn new(node: P) -> Self {
        let updates = node.subscribe_veilid_update();
        Self {
            node,
            updates,
            have_to_share_map: HashMap::new(),
        }
    }
}

/// Have-map resolver request messages.
#[derive(Clone, Debug)]
pub enum Request {
    /// Resolve the have-map for the specified share key, to get a map of which
    /// pieces the remote peer has.
    Resolve { key: TypedKey },

    /// Watch the have-map for the specified share key for changes.
    Watch { key: TypedKey },

    /// Cancel the watch on the have-map share.
    CancelWatch { key: TypedKey },
}

impl Request {
    /// Get the share key specified in the request.
    fn key(&self) -> &TypedKey {
        match self {
            Request::Resolve { key } => key,
            Request::Watch { key } => key,
            Request::CancelWatch { key } => key,
        }
    }
}

/// Have-map resolver response messages.
#[derive(Clone, Debug)]
pub enum Response {
    /// Have-map is not available at the given key, with error cause.
    NotAvailable { key: TypedKey, err_msg: String },

    /// Have-map response.
    Resolve { key: TypedKey, pieces: PieceMap },

    /// Acknowledge that the have-map at the remote share key is being monitored
    /// for changes, with an automatically-renewed watch.
    Watching { key: TypedKey },

    /// Acknowledge that the watch on the have-map at remote peer key has been cancelled.
    WatchCancelled { key: TypedKey },
}

impl<P: Node> Actor for HaveResolver<P> {
    type Request = Request;
    type Response = Response;

    #[tracing::instrument(skip_all, err)]
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
                        Err(err) => {
                            server_ch.send(Response::NotAvailable { key: req.key().to_owned(), err_msg: err.to_string() }).await?
                        }
                    }
                }
                res = self.updates.recv() => {
                    let update = res?;
                    match update {
                        VeilidUpdate::ValueChange(ch) => {
                            let share_key = match self.have_to_share_map.get(&ch.key) {
                                Some(key) => key,
                                None => continue,
                            };
                            let have_map = self.node.resolve_have_map(share_key).await?;
                            server_ch.send(Response::Resolve{ key: share_key.to_owned(), pieces: have_map}).await?;
                        }
                        VeilidUpdate::Shutdown => {
                            return Ok(());
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    async fn handle(&mut self, req: &Request) -> Result<Response> {
        Ok(match req {
            Request::Resolve { key } => {
                let have_map = self.node.resolve_have_map(key).await?;
                Response::Resolve {
                    key: key.to_owned(),
                    pieces: have_map,
                }
            }
            Request::Watch { key } => {
                let (_, header) = self.node.resolve_route(key, None).await?;
                if let Some(have_map_ref) = header.have_map() {
                    self.have_to_share_map
                        .insert(have_map_ref.key().to_owned(), key.to_owned());
                    info!("watch: have map key {}", have_map_ref.key());
                    self.node
                        .watch(
                            have_map_ref.key().to_owned(),
                            ValueSubkeyRangeSet::full(),
                            TimestampDuration::new_secs(60),
                        )
                        .await?;
                    Response::Watching {
                        key: key.to_owned(),
                    }
                } else {
                    Response::NotAvailable {
                        key: key.to_owned(),
                        err_msg: format!("peer {key} does not publish a have map"),
                    }
                }
            }
            Request::CancelWatch { key } => {
                let (_, header) = self.node.resolve_route(key, None).await?;
                if let Some(have_map_ref) = header.have_map() {
                    self.have_to_share_map.remove(have_map_ref.key());
                    self.node.cancel_watch(have_map_ref.key());
                }
                Response::WatchCancelled {
                    key: key.to_owned(),
                }
            }
        })
    }
}

impl<P: Node + 'static> Clone for HaveResolver<P> {
    fn clone(&self) -> Self {
        Self {
            node: self.node.clone(),
            updates: self.updates.resubscribe(),
            have_to_share_map: self.have_to_share_map.clone(),
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
    use veilid_core::{
        CryptoKey, Target, TimestampDuration, ValueData, ValueSubkeyRangeSet, VeilidUpdate,
        VeilidValueChange, CRYPTO_KEY_LENGTH, CRYPTO_KIND_VLD0,
    };

    use crate::{
        actor::{OneShot, Operator},
        have_resolver::{HaveResolver, Request, Response},
        node::TypedKey,
        proto::{HaveMapRef, Header},
        tests::StubNode,
    };

    #[tokio::test]
    async fn test_have_resolver_resolves_have_map() {
        // Create a stub peer with a recording merge_have_map_result
        let mut node = StubNode::new();
        let recorded_key = Arc::new(RwLock::new(None));

        let recorded_key_clone = recorded_key.clone();

        node.resolve_have_map_result = Arc::new(Mutex::new(move |key: &TypedKey| {
            *recorded_key_clone.write().unwrap() = Some(key.to_owned());
            Ok(vec![42].into())
        }));

        // Create a test key and channel
        let test_key =
            TypedKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");

        // Create have resolver
        let cancel = CancellationToken::new();
        let have_resolver = HaveResolver::new(node.clone());
        let mut operator = Operator::new(cancel.clone(), have_resolver, OneShot);

        // Send a Resolve request
        let req = Request::Resolve {
            key: test_key.clone(),
        };
        operator.send(req).await.unwrap();

        // Verify the response
        match operator.recv().await.expect("recv") {
            Response::Resolve { key, pieces } => {
                assert_eq!(key, test_key);
                assert_eq!(Into::<Vec<u8>>::into(pieces), vec![42]);
            }
            other => panic!("Unexpected response: {:?}", other),
        }

        // Verify the merge was called correctly
        let recorded_key = recorded_key.read().unwrap();

        assert!(recorded_key.is_some(), "Key was not recorded");
        assert_eq!(recorded_key.unwrap(), test_key);

        // Clean up
        cancel.cancel();
        operator.join().await.expect("task").expect("run");
    }

    #[tokio::test]
    async fn test_have_resolver_watches_have_map() {
        // Create a stub peer with a recording watch_result
        let mut node = StubNode::new();
        let recorded_key = Arc::new(RwLock::new(None));
        let recorded_subkeys = Arc::new(RwLock::new(None));
        let recorded_duration = Arc::new(RwLock::new(None));

        let recorded_key_clone = recorded_key.clone();
        let recorded_subkeys_clone = recorded_subkeys.clone();
        let recorded_duration_clone = recorded_duration.clone();

        let have_map_key = TypedKey::new(CRYPTO_KIND_VLD0, CryptoKey::new([0xa5; 32]));
        let have_map_key_clone = have_map_key.clone();
        node.resolve_route_result = Arc::new(Mutex::new(
            move |_key: &TypedKey, _prior_route: Option<Target>| {
                Ok((
                    Target::PrivateRoute(CryptoKey::new([0xa5; 32])),
                    Header::new(
                        [0xab; 32],
                        42,
                        1,
                        [0xcd; 99].as_slice(),
                        Some(HaveMapRef::new(have_map_key_clone, 1)),
                        None,
                    ),
                ))
            },
        ));

        node.watch_result = Arc::new(Mutex::new(
            move |key: TypedKey, subkeys: ValueSubkeyRangeSet, duration: TimestampDuration| {
                *recorded_key_clone.write().unwrap() = Some(key);
                *recorded_subkeys_clone.write().unwrap() = Some(subkeys);
                *recorded_duration_clone.write().unwrap() = Some(duration);
                Ok(())
            },
        ));

        // Create a test key and channel
        let test_key =
            TypedKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");

        // Create have resolver
        let cancel = CancellationToken::new();
        let have_resolver = HaveResolver::new(node.clone());
        let mut operator = Operator::new(cancel.clone(), have_resolver, OneShot);

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
        let recorded_duration = recorded_duration.read().unwrap();

        assert!(recorded_key.is_some(), "Key was not recorded");
        assert!(recorded_subkeys.is_some(), "Subkeys were not recorded");
        assert!(recorded_duration.is_some(), "Duration was not recorded");
        assert_eq!(recorded_key.as_ref().unwrap(), &have_map_key);
        assert_eq!(
            recorded_subkeys.as_ref().unwrap(),
            &ValueSubkeyRangeSet::full()
        );
        assert_eq!(
            recorded_duration.as_ref().unwrap(),
            &TimestampDuration::new_secs(60)
        );

        // Clean up
        cancel.cancel();
        operator.join().await.expect("task").expect("run");
    }

    #[tokio::test]
    async fn test_have_resolver_cancels_watch() {
        // Create a stub peer with a recording cancel_watch_result
        let mut node = StubNode::new();
        let recorded_key = Arc::new(RwLock::new(None));

        let have_map_key = TypedKey::new(CRYPTO_KIND_VLD0, CryptoKey::new([0xa5; 32]));

        let recorded_key_clone = recorded_key.clone();
        node.cancel_watch_result = Arc::new(Mutex::new(move |key: &TypedKey| {
            *recorded_key_clone.write().unwrap() = Some(key.clone());
        }));

        let have_map_key_clone = have_map_key.clone();
        node.resolve_route_result = Arc::new(Mutex::new(
            move |_key: &TypedKey, _prior_route: Option<Target>| {
                Ok((
                    Target::PrivateRoute(CryptoKey::new([0xa5; 32])),
                    Header::new(
                        [0xab; 32],
                        42,
                        1,
                        [0xcd; 99].as_slice(),
                        Some(HaveMapRef::new(have_map_key_clone, 1)),
                        None,
                    ),
                ))
            },
        ));

        // Create a test key and channel
        let test_key =
            TypedKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");

        // Create have resolver
        let cancel = CancellationToken::new();
        let have_resolver = HaveResolver::new(node.clone());
        let mut operator = Operator::new(cancel.clone(), have_resolver, OneShot);

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
        assert_eq!(recorded_key.as_ref().unwrap(), &have_map_key);

        // Clean up
        cancel.cancel();
        operator.join().await.expect("task").expect("run");
    }

    #[tokio::test]
    async fn test_have_resolver_handles_value_changes() {
        // Create a stub peer with a recording merge_have_map_result
        let mut node = StubNode::new();
        let recorded_key = Arc::new(RwLock::new(None));

        let recorded_key_clone = recorded_key.clone();

        // Mock resolve_route (called when setting up watch)
        let have_map_key =
            TypedKey::from_str("VLD0:eCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");
        let have_map_key_clone = have_map_key.clone();
        node.resolve_route_result = Arc::new(Mutex::new(
            move |_key: &TypedKey, _target: Option<Target>| {
                let peer_map_ref = crate::proto::HaveMapRef::new(have_map_key_clone, 1);
                let header = crate::proto::Header::new(
                    [0u8; 32],          // payload_digest
                    0,                  // payload_length
                    0,                  // subkeys
                    &[],                // route_data
                    Some(peer_map_ref), // peer_map
                    None,               // have_map
                );
                Ok((Target::PrivateRoute(CryptoKey::new([0u8; 32])), header))
            },
        ));

        // Mock watch
        node.watch_result = Arc::new(Mutex::new(
            move |_key: TypedKey, _subkeys: ValueSubkeyRangeSet, _duration: TimestampDuration| {
                Ok(())
            },
        ));

        // Mock resolve_have_map (called when watch fires)
        node.resolve_have_map_result = Arc::new(Mutex::new(move |key: &TypedKey| {
            *recorded_key_clone.write().unwrap() = Some(key.to_owned());
            Ok(vec![42].into())
        }));

        let test_key =
            TypedKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");
        let update_tx = node.update_tx.clone();
        let cancel = CancellationToken::new();
        let have_resolver = HaveResolver::new(node);
        let mut operator = Operator::new(cancel.clone(), have_resolver, OneShot);

        // Send a request and receive a response, to make sure the task is
        // running. That's important: if we're not in the run loop, the update
        // send may fail to broadcast.
        operator
            .send(Request::Watch {
                key: test_key.clone(),
            })
            .await
            .unwrap();
        assert!(matches!(operator.recv().await, Some(_)));

        // Simulate a value change notification
        let change = VeilidValueChange {
            key: have_map_key,
            subkeys: ValueSubkeyRangeSet::single_range(0, 99),
            value: Some(
                ValueData::new(b"foo".to_vec(), CryptoKey::new([0xbe; CRYPTO_KEY_LENGTH]))
                    .expect("new value data"),
            ),
            count: 1,
        };
        update_tx
            .send(VeilidUpdate::ValueChange(Box::new(change)))
            .expect("send value change");

        // Verify the response
        match operator.recv().await.expect("recv") {
            Response::Resolve { key, pieces } => {
                assert_eq!(key, test_key);
                assert_eq!(Into::<Vec<u8>>::into(pieces), vec![42]);
            }
            other => panic!("Unexpected response: {:?}", other),
        };

        // Verify the merge was called correctly
        let recorded_key = recorded_key.read().unwrap();

        assert!(recorded_key.is_some(), "Key was not recorded");
        assert_eq!(recorded_key.as_ref().unwrap(), &test_key);

        // Clean up
        cancel.cancel();
        operator.join().await.expect("task").expect("run");
    }
}
