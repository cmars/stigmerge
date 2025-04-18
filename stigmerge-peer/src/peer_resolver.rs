use std::{collections::HashMap, sync::Arc};

use tokio::{select, sync::RwLock};
use tokio_util::sync::CancellationToken;
use veilid_core::{TimestampDuration, ValueSubkeyRangeSet};

use crate::{
    chan_rpc::{ChanServer, Service},
    peer::TypedKey,
    proto::{Decoder, PeerInfo},
    Error, Peer, Result,
};

#[derive(Debug)]
pub enum Request {
    /// Resolve the peer information stored at the given peer map key. This will
    /// result in a response containing the peer info, or a not available
    /// indicator.
    ///
    /// The peer map key and subkeys come from the main share header (PeerMap).
    Resolve { key: TypedKey, subkeys: u16 },

    /// Watch for peer updates at the given peer map key. This will result in a
    /// series of peer info responses being sent as the peers change.
    Watch { key: TypedKey },

    /// Cancel the watch on the peer map key.
    CancelWatch { key: TypedKey },
}

impl Request {
    fn key(&self) -> &TypedKey {
        match self {
            Request::Resolve { key, subkeys: _ } => key,
            Request::Watch { key } => key,
            Request::CancelWatch { key } => key,
        }
    }
}

#[derive(Debug)]
pub enum Response {
    NotAvailable {
        key: TypedKey,
        err: Error,
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

/// The peer_resolver service handles requests for remote peer maps, which
/// indicate which other peers a remote peer knows about.
pub struct PeerResolver<P: Peer> {
    peer: P,
    ch: ChanServer<Request, Response>,
    peer_maps: HashMap<TypedKey, Arc<RwLock<HashMap<TypedKey, PeerInfo>>>>,
}

impl<P: Peer> Service for PeerResolver<P> {
    type Request = Request;
    type Response = Response;

    /// Run the service until cancelled.
    async fn run(mut self, cancel: CancellationToken) -> Result<()> {
        let mut updates = self.peer.subscribe_veilid_update();
        loop {
            select! {
                _ = cancel.cancelled() => {
                    return Ok(())
                }
                res = self.ch.rx.recv() => {
                    let req = match res {
                        None => return Ok(()),
                        Some(req) => req,
                    };
                    match self.handle(&req).await {
                        Ok(resp) => {
                            self.ch.tx.send(resp).await.map_err(Error::other)?
                        }
                        Err(err) => self.ch.tx.send(Response::NotAvailable{key: req.key().clone(), err}).await.map_err(Error::other)?,
                    }
                }
                res = updates.recv() => {
                    let update = res.map_err(Error::other)?;
                    match update {
                        veilid_core::VeilidUpdate::ValueChange(ch) => {
                            if let Some(data) = ch.value {
                                if let Ok(peer_info) = PeerInfo::decode(data.data()) {
                                    self.ch.tx.send(Response::Resolve{
                                        key: ch.key,
                                        peers: [(peer_info.key().to_owned(), peer_info)].into_iter().collect::<HashMap<_,_>>()
                                    }).await.map_err(Error::other)?;
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
    async fn handle(&mut self, req: &Request) -> Result<Response> {
        Ok(match req {
            Request::Resolve { key, subkeys } => {
                let mut result = HashMap::new();
                for subkey in 0u16..*subkeys {
                    if let Ok(peer_info) = self.peer.resolve_peer_info(key.to_owned(), subkey).await
                    {
                        result.insert(peer_info.key().clone(), peer_info);
                    }
                }
                Response::Resolve {
                    key: key.to_owned(),
                    peers: result,
                }
            }
            Request::Watch { key } => {
                self.peer
                    .watch(
                        key.to_owned(),
                        ValueSubkeyRangeSet::full(),
                        TimestampDuration::new_secs(60),
                    )
                    .await?;
                Response::Watching {
                    key: key.to_owned(),
                }
            }
            Request::CancelWatch { key } => {
                self.peer.cancel_watch(key);
                Response::WatchCancelled {
                    key: key.to_owned(),
                }
            }
        })
    }
}

impl<P> PeerResolver<P>
where
    P: Peer,
{
    /// Create a new peer_resolver service.
    pub fn new(peer: P, ch: ChanServer<Request, Response>) -> Self {
        Self {
            peer,
            ch,
            peer_maps: HashMap::new(),
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
    use veilid_core::{TimestampDuration, TypedKey, ValueSubkeyRangeSet};

    use crate::{
        chan_rpc::{pipe, Service},
        error::Result,
        peer_resolver::{PeerResolver, Request, Response},
        proto::{Encoder, PeerInfo},
        tests::StubPeer,
    };

    #[tokio::test]
    async fn test_peer_resolver_resolves_peers() -> Result<()> {
        // Create a stub peer with a recording resolve_peer_info_result
        let mut stub_peer = StubPeer::new();
        let recorded_key = Arc::new(RwLock::new(None));
        let recorded_subkey = Arc::new(RwLock::new(None));

        let recorded_key_clone = recorded_key.clone();
        let recorded_subkey_clone = recorded_subkey.clone();

        // Create a test peer key to return
        let test_peer_key =
            TypedKey::from_str("VLD0:dDHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");
        let test_peer_info = PeerInfo::new(test_peer_key.clone());

        stub_peer.resolve_peer_info_result =
            Arc::new(Mutex::new(move |key: TypedKey, subkey: u16| {
                *recorded_key_clone.write().unwrap() = Some(key);
                *recorded_subkey_clone.write().unwrap() = Some(subkey);
                Ok(test_peer_info.clone())
            }));

        // Create a test key and channel
        let test_key =
            TypedKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");
        let (mut client_ch, server_ch) = pipe(16);

        // Create peer resolver
        let peer_resolver = PeerResolver::new(stub_peer, server_ch);

        // Create cancellation token
        let cancel = CancellationToken::new();
        let cancel_task = cancel.clone();

        // Spawn the peer resolver service
        let handle = tokio::spawn(async move { peer_resolver.run(cancel_task).await });

        // Send a Resolve request
        let req = Request::Resolve {
            key: test_key.clone(),
            subkeys: 1,
        };
        client_ch.tx.send(req).await.unwrap();

        // Verify the response
        match client_ch.rx.recv().await.expect("recv") {
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
        let recorded_subkey = recorded_subkey.read().unwrap();

        assert!(recorded_key.is_some(), "Key was not recorded");
        assert!(recorded_subkey.is_some(), "Subkey was not recorded");
        assert_eq!(recorded_key.as_ref().unwrap(), &test_key);
        assert_eq!(recorded_subkey.as_ref().unwrap(), &0);

        // Clean up
        cancel.cancel();
        handle.await.unwrap().unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_peer_resolver_watches_peers() -> Result<()> {
        // Create a stub peer with a recording watch_result
        let mut stub_peer = StubPeer::new();
        let recorded_key = Arc::new(RwLock::new(None));
        let recorded_subkeys = Arc::new(RwLock::new(None));
        let recorded_duration = Arc::new(RwLock::new(None));

        let recorded_key_clone = recorded_key.clone();
        let recorded_subkeys_clone = recorded_subkeys.clone();
        let recorded_duration_clone = recorded_duration.clone();

        stub_peer.watch_result = Arc::new(Mutex::new(
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
        let (mut client_ch, server_ch) = pipe(16);

        // Create peer resolver
        let peer_resolver = PeerResolver::new(stub_peer.clone(), server_ch);

        // Create cancellation token
        let cancel = CancellationToken::new();
        let cancel_task = cancel.clone();

        // Spawn the peer resolver service
        let handle = tokio::spawn(async move { peer_resolver.run(cancel_task).await });

        // Send a Watch request
        let req = Request::Watch {
            key: test_key.clone(),
        };
        client_ch.tx.send(req).await.unwrap();

        // Verify the response
        match client_ch.rx.recv().await.expect("recv") {
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
        assert_eq!(recorded_key.as_ref().unwrap(), &test_key);
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
        handle.await.unwrap().unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_peer_resolver_cancels_watch() -> Result<()> {
        // Create a stub peer with a recording cancel_watch_result
        let mut stub_peer = StubPeer::new();
        let recorded_key = Arc::new(RwLock::new(None));

        let recorded_key_clone = recorded_key.clone();
        stub_peer.cancel_watch_result = Arc::new(Mutex::new(move |key: &TypedKey| {
            *recorded_key_clone.write().unwrap() = Some(key.clone());
        }));

        // Create a test key and channel
        let test_key =
            TypedKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");
        let (mut client_ch, server_ch) = pipe(16);

        // Create peer resolver
        let peer_resolver = PeerResolver::new(stub_peer.clone(), server_ch);

        // Create cancellation token
        let cancel = CancellationToken::new();
        let cancel_task = cancel.clone();

        // Spawn the peer resolver service
        let handle = tokio::spawn(async move { peer_resolver.run(cancel_task).await });

        // Send a CancelWatch request
        let req = Request::CancelWatch {
            key: test_key.clone(),
        };
        client_ch.tx.send(req).await.unwrap();

        // Verify the response
        match client_ch.rx.recv().await.expect("recv") {
            Response::WatchCancelled { key } => {
                assert_eq!(key, test_key);
            }
            other => panic!("Unexpected response: {:?}", other),
        }

        // Verify the cancel was called correctly
        let recorded_key = recorded_key.read().unwrap();
        assert!(recorded_key.is_some(), "Key was not recorded");
        assert_eq!(recorded_key.as_ref().unwrap(), &test_key);

        // Clean up
        cancel.cancel();
        handle.await.unwrap().unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_peer_resolver_handles_value_changes() -> Result<()> {
        // Create a stub peer
        let mut stub_peer = StubPeer::new();

        // Create a test key and channel
        let test_key =
            TypedKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");
        let peer_key =
            TypedKey::from_str("VLD0:dDHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");
        let (mut client_ch, server_ch) = pipe(16);

        let recorded_resolve_peer_info = Arc::new(RwLock::new(0u32));
        let recorded_resolve_peer_info_clone = recorded_resolve_peer_info.clone();
        let stub_peer_key = peer_key.clone();
        stub_peer.resolve_peer_info_result =
            Arc::new(Mutex::new(move |_key: TypedKey, _subkey: u16| {
                let mut count = recorded_resolve_peer_info_clone.write().unwrap();
                *count += 1;
                Ok(PeerInfo::new(stub_peer_key))
            }));

        // Create peer resolver
        let update_tx = stub_peer.update_tx.clone();
        let peer_resolver = PeerResolver::new(stub_peer, server_ch);

        // Create cancellation token
        let cancel = CancellationToken::new();
        let cancel_task = cancel.clone();

        // Spawn the peer resolver service
        let handle = tokio::spawn(async move { peer_resolver.run(cancel_task).await });

        // Send a request and receive a response, to make sure the task is
        // running. That's important: if we're not in the run loop, the update
        // send may fail to broadcast.
        let req = Request::Resolve {
            key: test_key.clone(),
            subkeys: 1,
        };
        client_ch.tx.send(req).await.unwrap();
        assert!(matches!(client_ch.rx.recv().await, Some(_)));

        // Create a PeerInfo object to include in the value change
        let peer_info = PeerInfo::new(peer_key.clone());
        let encoded_peer_info = peer_info.encode().expect("encode peer info");

        // Simulate a value change notification
        let crypto_key = veilid_core::CryptoKey::new([0xbe; veilid_core::CRYPTO_KEY_LENGTH]);
        let value_data =
            veilid_core::ValueData::new(encoded_peer_info, crypto_key).expect("new value data");

        let change = veilid_core::VeilidValueChange {
            key: test_key.clone(),
            subkeys: ValueSubkeyRangeSet::single_range(0, 0),
            value: Some(value_data),
            count: 1,
        };

        update_tx
            .send(veilid_core::VeilidUpdate::ValueChange(Box::new(change)))
            .expect("send value change");

        // Verify the response - we should receive a Resolve response with the peer info
        match client_ch.rx.recv().await.expect("recv") {
            Response::Resolve { key, peers } => {
                assert_eq!(key, test_key);
                assert_eq!(peers.len(), 1);
                assert!(peers.contains_key(&peer_key));
                let received_peer_info = peers.get(&peer_key).unwrap();
                assert_eq!(received_peer_info.key(), &peer_key);
            }
            other => panic!("Unexpected response: {:?}", other),
        }

        assert_eq!(*recorded_resolve_peer_info.read().unwrap(), 1u32);

        // Clean up
        cancel.cancel();
        handle.await.unwrap().unwrap();

        Ok(())
    }
}
