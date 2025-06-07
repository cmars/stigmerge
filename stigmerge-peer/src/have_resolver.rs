use std::{collections::HashMap, fmt};

use anyhow::Context;
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use veilid_core::{TypedRecordKey, ValueSubkeyRangeSet, VeilidUpdate};

use crate::{
    actor::{Actor, Respondable, ResponseChannel},
    error::CancelError,
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

    // Mapping from have-map key to share key, used in watches
    have_to_share_map: HashMap<TypedRecordKey, TypedRecordKey>,

    // Sender for watched have-map updates
    have_map_tx: flume::Sender<(TypedRecordKey, PieceMap)>,
    have_map_rx: flume::Receiver<(TypedRecordKey, PieceMap)>,
}

impl<P> HaveResolver<P>
where
    P: Node,
{
    /// Create a new have_resolver service with the given peer.
    pub fn new(node: P) -> Self {
        let (have_map_tx, have_map_rx) = flume::unbounded();
        Self {
            node,
            have_to_share_map: HashMap::new(),
            have_map_tx,
            have_map_rx,
        }
    }

    pub fn subscribe_have_map(&self) -> flume::Receiver<(TypedRecordKey, PieceMap)> {
        self.have_map_rx.clone()
    }
}

/// Have-map resolver request messages.
pub enum Request {
    /// Resolve the have-map for the specified share key, to get a map of which
    /// pieces the remote peer has.
    Resolve {
        response_tx: ResponseChannel<Response>,
        key: TypedRecordKey,
    },

    /// Watch the have-map for the specified share key for changes.
    Watch {
        response_tx: ResponseChannel<Response>,
        key: TypedRecordKey,
    },

    /// Cancel the watch on the have-map share.
    CancelWatch {
        response_tx: ResponseChannel<Response>,
        key: TypedRecordKey,
    },
}

impl fmt::Debug for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Resolve {
                response_tx: _,
                key,
            } => f.debug_struct("Resolve").field("key", key).finish(),
            Self::Watch {
                response_tx: _,
                key,
            } => f.debug_struct("Watch").field("key", key).finish(),
            Self::CancelWatch {
                response_tx: _,
                key,
            } => f.debug_struct("CancelWatch").field("key", key).finish(),
        }
    }
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

/// Have-map resolver response messages.
#[derive(Clone, Debug)]
pub enum Response {
    /// Have-map is not available at the given key, with error cause.
    NotAvailable {
        key: TypedRecordKey,
        err_msg: String,
    },

    /// Have-map response.
    Resolve {
        key: TypedRecordKey,
        pieces: PieceMap,
    },

    /// Acknowledge that the have-map at the remote share key is being monitored
    /// for changes, with an automatically-renewed watch.
    Watching { key: TypedRecordKey },

    /// Acknowledge that the watch on the have-map at remote peer key has been cancelled.
    WatchCancelled { key: TypedRecordKey },
}

impl<P: Node> Actor for HaveResolver<P> {
    type Request = Request;
    type Response = Response;

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
                    let req = res.with_context(|| format!("have_resolver: receive request"))?;
                    self.handle_request(req).await?;
                }
                res = update_rx.recv() => {
                    let update = res.with_context(|| format!("have_resolver: receive veilid update"))?;
                    match update {
                        VeilidUpdate::ValueChange(ch) => {
                            match self.have_to_share_map.get(&ch.key) {
                                Some(share_key) => {
                                    let have_map = self.node.resolve_have_map(&ch.key).await?;
                                    self.have_map_tx.send_async((share_key.to_owned(), have_map)).await.with_context(
                                        || format!("have_resolver: send have map"))?;
                                }
                                None => continue,
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
        let (resp, mut resp_tx) = match req {
            Request::Resolve { key, response_tx } => match self.node.resolve_have_map(&key).await {
                Ok(have_map) => (
                    Response::Resolve {
                        key: key.clone(),
                        pieces: have_map,
                    },
                    response_tx,
                ),
                Err(err) => (
                    Response::NotAvailable {
                        key: key.clone(),
                        err_msg: err.to_string(),
                    },
                    response_tx,
                ),
            },
            Request::Watch { key, response_tx } => {
                match self.node.resolve_route(&key, None).await {
                    Ok((_, header)) => {
                        if let Some(have_map_ref) = header.have_map() {
                            self.have_to_share_map
                                .insert(have_map_ref.key().to_owned(), key.to_owned());
                            info!("watch: have map key {}", have_map_ref.key());
                            match self
                                .node
                                .watch(have_map_ref.key().to_owned(), ValueSubkeyRangeSet::full())
                                .await
                            {
                                Ok(_) => (
                                    Response::Watching {
                                        key: key.to_owned(),
                                    },
                                    response_tx,
                                ),
                                Err(err) => (
                                    Response::NotAvailable {
                                        key: key.to_owned(),
                                        err_msg: err.to_string(),
                                    },
                                    response_tx,
                                ),
                            }
                        } else {
                            (
                                Response::NotAvailable {
                                    key: key.to_owned(),
                                    err_msg: format!("peer {key} does not publish a have map"),
                                },
                                response_tx,
                            )
                        }
                    }
                    Err(err) => (
                        Response::NotAvailable {
                            key: key.to_owned(),
                            err_msg: err.to_string(),
                        },
                        response_tx,
                    ),
                }
            }
            Request::CancelWatch { key, response_tx } => {
                match self.node.resolve_route(&key, None).await {
                    Ok((_, header)) => {
                        if let Some(have_map_ref) = header.have_map() {
                            self.have_to_share_map.remove(have_map_ref.key());
                            if let Err(e) = self.node.cancel_watch(have_map_ref.key()).await {
                                warn!("cancel watch: {}", e);
                            }
                        }
                        (
                            Response::WatchCancelled {
                                key: key.to_owned(),
                            },
                            response_tx,
                        )
                    }
                    Err(_) => {
                        // Even if we can't resolve the route, we still want to acknowledge the cancel
                        (
                            Response::WatchCancelled {
                                key: key.to_owned(),
                            },
                            response_tx,
                        )
                    }
                }
            }
        };
        resp_tx
            .send(resp)
            .await
            .with_context(|| "have_resolver: send response")?;

        Ok(())
    }
}

impl<P: Node + 'static> Clone for HaveResolver<P> {
    fn clone(&self) -> Self {
        Self {
            node: self.node.clone(),
            have_to_share_map: self.have_to_share_map.clone(),
            have_map_tx: self.have_map_tx.clone(),
            have_map_rx: self.have_map_rx.clone(),
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
        CryptoKind, PublicKey, RecordKey, RouteId, Target, TypedRecordKey, ValueData,
        ValueSubkeyRangeSet, VeilidUpdate, VeilidValueChange, CRYPTO_KEY_LENGTH,
    };

    use crate::{
        actor::{OneShot, Operator, ResponseChannel},
        have_resolver::{HaveResolver, Request, Response},
        proto::{HaveMapRef, Header},
        tests::StubNode,
    };

    #[tokio::test]
    async fn test_have_resolver_resolves_have_map() {
        // Create a stub peer with a recording merge_have_map_result
        let mut node = StubNode::new();
        let recorded_key = Arc::new(RwLock::new(None));

        let recorded_key_clone = recorded_key.clone();

        node.resolve_have_map_result = Arc::new(Mutex::new(move |key: &TypedRecordKey| {
            *recorded_key_clone.write().unwrap() = Some(key.to_owned());
            Ok(vec![42].into())
        }));

        // Create a test key and channel
        let test_key = TypedRecordKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M")
            .expect("key");

        // Create have resolver
        let cancel = CancellationToken::new();
        let have_resolver = HaveResolver::new(node.clone());
        let mut operator = Operator::new(cancel.clone(), have_resolver, OneShot);

        // Resolve request
        match operator
            .call(Request::Resolve {
                response_tx: ResponseChannel::default(),
                key: test_key.clone(),
            })
            .await
            .expect("call")
        {
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
        operator.join().await.expect("task");
    }

    #[tokio::test]
    async fn test_have_resolver_watches_have_map() {
        // Create a stub peer with a recording watch_result
        let mut node = StubNode::new();
        let recorded_key = Arc::new(RwLock::new(None));
        let recorded_subkeys = Arc::new(RwLock::new(None));

        let recorded_key_clone = recorded_key.clone();
        let recorded_subkeys_clone = recorded_subkeys.clone();

        let have_map_key = TypedRecordKey::new(CryptoKind::default(), RecordKey::new([0xa5; 32]));
        let have_map_key_clone = have_map_key.clone();
        node.resolve_route_result = Arc::new(Mutex::new(
            move |_key: &TypedRecordKey, _prior_route: Option<Target>| {
                Ok((
                    Target::PrivateRoute(RouteId::new([0xa5; 32])),
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
            move |key: TypedRecordKey, subkeys: ValueSubkeyRangeSet| {
                *recorded_key_clone.write().unwrap() = Some(key);
                *recorded_subkeys_clone.write().unwrap() = Some(subkeys);
                Ok(())
            },
        ));

        // Create a test key and channel
        let test_key = TypedRecordKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M")
            .expect("key");

        // Create have resolver
        let cancel = CancellationToken::new();
        let have_resolver = HaveResolver::new(node.clone());
        let mut operator = Operator::new(cancel.clone(), have_resolver, OneShot);

        // Send a Watch request with a response channel
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
        assert_eq!(recorded_key.as_ref().unwrap(), &have_map_key);
        assert_eq!(
            recorded_subkeys.as_ref().unwrap(),
            &ValueSubkeyRangeSet::full()
        );

        // Clean up
        cancel.cancel();
        operator.join().await.expect("task");
    }

    #[tokio::test]
    async fn test_have_resolver_cancels_watch() {
        // Create a stub peer with a recording cancel_watch_result
        let mut node = StubNode::new();
        let recorded_key = Arc::new(RwLock::new(None));

        let have_map_key = TypedRecordKey::new(CryptoKind::default(), RecordKey::new([0xa5; 32]));

        let recorded_key_clone = recorded_key.clone();
        node.cancel_watch_result = Arc::new(Mutex::new(move |key: &TypedRecordKey| {
            *recorded_key_clone.write().unwrap() = Some(key.clone());
            Ok(())
        }));

        let have_map_key_clone = have_map_key.clone();
        node.resolve_route_result = Arc::new(Mutex::new(
            move |_key: &TypedRecordKey, _prior_route: Option<Target>| {
                Ok((
                    Target::PrivateRoute(RouteId::new([0xa5; 32])),
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
        let test_key = TypedRecordKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M")
            .expect("key");

        // Create have resolver
        let cancel = CancellationToken::new();
        let have_resolver = HaveResolver::new(node.clone());
        let mut operator = Operator::new(cancel.clone(), have_resolver, OneShot);

        // Send a CancelWatch request with a response channel
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
        assert_eq!(recorded_key.as_ref().unwrap(), &have_map_key);

        // Clean up
        cancel.cancel();
        operator.join().await.expect("task");
    }

    #[tokio::test]
    async fn test_have_resolver_handles_value_changes() {
        // Create a stub peer with a recording merge_have_map_result
        let mut node = StubNode::new();
        let recorded_key = Arc::new(RwLock::new(None));

        let recorded_key_clone = recorded_key.clone();

        // Mock resolve_route (called when setting up watch)
        let have_map_key =
            TypedRecordKey::from_str("VLD0:eCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M")
                .expect("key");
        let have_map_key_clone = have_map_key.clone();
        node.resolve_route_result = Arc::new(Mutex::new(
            move |_key: &TypedRecordKey, _target: Option<Target>| {
                let have_map_ref = crate::proto::HaveMapRef::new(have_map_key_clone, 1);
                let header = crate::proto::Header::new(
                    [0u8; 32],          // payload_digest
                    0,                  // payload_length
                    0,                  // subkeys
                    &[],                // route_data
                    Some(have_map_ref), // have_map
                    None,               // peer_map
                );
                Ok((Target::PrivateRoute(RouteId::new([0u8; 32])), header))
            },
        ));

        // Mock watch
        node.watch_result = Arc::new(Mutex::new(
            move |_key: TypedRecordKey, _subkeys: ValueSubkeyRangeSet| Ok(()),
        ));

        // Mock resolve_have_map (called when watch fires)
        node.resolve_have_map_result = Arc::new(Mutex::new(move |key: &TypedRecordKey| {
            *recorded_key_clone.write().unwrap() = Some(key.to_owned());
            Ok(vec![42].into())
        }));

        let test_key = TypedRecordKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M")
            .expect("key");
        let update_tx = node.update_tx.clone();
        let cancel = CancellationToken::new();
        let have_resolver = HaveResolver::new(node);
        let have_map_rx = have_resolver.subscribe_have_map();
        let mut operator = Operator::new(cancel.clone(), have_resolver, OneShot);

        // Send a request and receive a response, to make sure the task is
        // running. That's important: if we're not in the run loop, the update
        // send may fail to broadcast.
        match operator
            .call(Request::Watch {
                response_tx: ResponseChannel::default(),
                key: test_key.clone(),
            })
            .await
            .expect("call")
        {
            Response::Watching { key } => {
                assert_eq!(key, test_key);
            }
            other => panic!("Unexpected response: {:?}", other),
        }

        // Simulate a value change notification
        let change = VeilidValueChange {
            key: have_map_key,
            subkeys: ValueSubkeyRangeSet::single_range(0, 99),
            value: Some(
                ValueData::new(b"foo".to_vec(), PublicKey::new([0xbe; CRYPTO_KEY_LENGTH]))
                    .expect("new value data"),
            ),
            count: 1,
        };
        update_tx
            .send(VeilidUpdate::ValueChange(Box::new(change)))
            .expect("send value change");

        // Expect a have_map update
        let (share_key, _) = have_map_rx.recv_async().await.expect("recv have_map");
        assert_eq!(share_key, test_key);

        // Verify the merge was called correctly
        let recorded_key = recorded_key.read().unwrap();

        assert!(recorded_key.is_some(), "Key was not recorded");
        assert_eq!(recorded_key.as_ref().unwrap(), &have_map_key);

        // Clean up
        cancel.cancel();
        operator.join().await.expect("task");
    }
}
