use std::{collections::HashMap, sync::Arc};

use tokio::{select, sync::RwLock};
use tokio_util::sync::CancellationToken;
use veilid_core::{TimestampDuration, ValueSubkeyRangeSet};

use crate::{
    chan_rpc::{ChanServer, Service},
    peer::TypedKey,
    piece_map::PieceMap,
    Error, Peer, Result,
};

/// Have-map resolver request messages.
#[derive(Debug)]
pub enum Request {
    /// Resolve the have-map key to get a map of which pieces the remote peer has.
    Resolve { key: TypedKey, subkeys: u16 },

    /// Watch the peer's have-map for changes.
    Watch { key: TypedKey },

    /// Cancel the watch on the peer's have-map.
    CancelWatch { key: TypedKey },
}

impl Request {
    /// Get the have-map key specified in the request.
    fn key(&self) -> &TypedKey {
        match self {
            Request::Resolve { key, subkeys: _ } => key,
            Request::Watch { key } => key,
            Request::CancelWatch { key } => key,
        }
    }
}

/// Have-map resolver response messages.
#[derive(Debug)]
pub enum Response {
    /// Have-map is not available at the given key, with error cause.
    NotAvailable { key: TypedKey, err: Error },

    /// Have-map response.
    Resolve {
        key: TypedKey,
        pieces: Arc<RwLock<PieceMap>>,
    },

    /// Acknowledge that the have-map at the remote peer key is being monitored
    /// for changes, with an automatically-renewed watch.
    Watching { key: TypedKey },

    /// Acknowledge that the watch on the have-map at remote peer key has been cancelled.
    WatchCancelled { key: TypedKey },
}

/// The have_resolver service handles requests for remote peer have-maps, which
/// indicate what pieces of a share the peer might have.
///
/// This service operates on the have-map reference keys indicated in the main
/// share DHT header (subkey 0) as haveMapRef.
pub struct HaveResolver<P: Peer> {
    peer: P,
    ch: ChanServer<Request, Response>,
    pieces_maps: HashMap<TypedKey, Arc<RwLock<PieceMap>>>,
}

impl<P: Peer> Service for HaveResolver<P> {
    type Request = Request;
    type Response = Response;

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
                        Err(err) => {
                            self.ch.tx.send(Response::NotAvailable { key: req.key().to_owned(), err }).await.map_err(Error::other)?
                        }
                    }
                }
                res = updates.recv() => {
                    let update = res.map_err(Error::other)?;
                    match update {
                        veilid_core::VeilidUpdate::ValueChange(ch) => {
                            let have_map_lock = self.assert_have_map(&ch.key);
                            {
                                let mut have_map = have_map_lock.write().await;
                                self.peer.merge_have_map(ch.key, ch.subkeys, &mut *have_map).await?;
                            }
                            self.ch.tx.send(Response::Resolve{ key: ch.key, pieces: have_map_lock }).await.map_err(Error::other)?;
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

    async fn handle(&mut self, req: &Request) -> Result<Response> {
        Ok(match req {
            Request::Resolve { key, subkeys } => {
                let have_map_lock = self.assert_have_map(key);
                {
                    let mut have_map = have_map_lock.write().await;
                    self.peer
                        .merge_have_map(
                            key.to_owned(),
                            ValueSubkeyRangeSet::single_range(0, (*subkeys - 1).into()),
                            &mut *have_map,
                        )
                        .await?;
                }
                Response::Resolve {
                    key: key.to_owned(),
                    pieces: have_map_lock,
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

impl<P> HaveResolver<P>
where
    P: Peer,
{
    /// Create a new have_resolver service with the given peer.
    pub(super) fn new(peer: P, ch: ChanServer<Request, Response>) -> Self {
        Self {
            peer,
            ch,
            pieces_maps: HashMap::new(),
        }
    }

    /// Get or create a local have-map tracking what the remote peer has.
    ///
    /// Updates are merged with this local copy as it's initially fetched and
    /// then watched for updates.
    fn assert_have_map(&mut self, key: &TypedKey) -> Arc<RwLock<PieceMap>> {
        if let Some(value) = self.pieces_maps.get(key) {
            return value.to_owned();
        }
        let value = Arc::new(RwLock::new(PieceMap::new()));
        self.pieces_maps.insert(key.to_owned(), value.to_owned());
        value
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
        CryptoKey, TimestampDuration, ValueData, ValueSubkeyRangeSet, VeilidUpdate,
        VeilidValueChange, CRYPTO_KEY_LENGTH,
    };

    use crate::{
        chan_rpc::{pipe, Service},
        error::Result,
        have_resolver::{HaveResolver, Request, Response},
        peer::TypedKey,
        piece_map::PieceMap,
        tests::StubPeer,
    };

    #[tokio::test]
    async fn test_have_resolver_resolves_have_map() -> Result<()> {
        // Create a stub peer with a recording merge_have_map_result
        let mut stub_peer = StubPeer::new();
        let recorded_key = Arc::new(RwLock::new(None));
        let recorded_subkeys = Arc::new(RwLock::new(None));

        let recorded_key_clone = recorded_key.clone();
        let recorded_subkeys_clone = recorded_subkeys.clone();

        stub_peer.merge_have_map_result = Arc::new(Mutex::new(
            move |key: TypedKey, subkeys: ValueSubkeyRangeSet, _have_map: &mut PieceMap| {
                *recorded_key_clone.write().unwrap() = Some(key);
                *recorded_subkeys_clone.write().unwrap() = Some(subkeys);
                Ok(())
            },
        ));

        // Create a test key and channel
        let test_key =
            TypedKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");
        let (mut client_ch, server_ch) = pipe(16);

        // Create have resolver
        let have_resolver = HaveResolver::new(stub_peer.clone(), server_ch);

        // Create cancellation token
        let cancel = CancellationToken::new();
        let cancel_task = cancel.clone();

        // Spawn the have resolver service
        let handle = tokio::spawn(async move { have_resolver.run(cancel_task).await });

        // Send a Resolve request
        let req = Request::Resolve {
            key: test_key.clone(),
            subkeys: 100,
        };
        client_ch.tx.send(req).await.unwrap();

        // Verify the response
        match client_ch.rx.recv().await.expect("recv") {
            Response::Resolve { key, pieces } => {
                assert_eq!(key, test_key);
                let _pieces = pieces.read().await; // Just verify we can access it
            }
            other => panic!("Unexpected response: {:?}", other),
        }

        // Verify the merge was called correctly
        let recorded_key = recorded_key.read().unwrap();
        let recorded_subkeys = recorded_subkeys.read().unwrap();

        assert!(recorded_key.is_some(), "Key was not recorded");
        assert!(recorded_subkeys.is_some(), "Subkeys were not recorded");
        assert_eq!(recorded_key.as_ref().unwrap(), &test_key);
        assert_eq!(
            recorded_subkeys.as_ref().unwrap(),
            &ValueSubkeyRangeSet::single_range(0, 99)
        );

        // Clean up
        cancel.cancel();
        handle.await.unwrap().unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_have_resolver_watches_have_map() -> Result<()> {
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

        // Create have resolver
        let have_resolver = HaveResolver::new(stub_peer.clone(), server_ch);

        // Create cancellation token
        let cancel = CancellationToken::new();
        let cancel_task = cancel.clone();

        // Spawn the have resolver service
        let handle = tokio::spawn(async move { have_resolver.run(cancel_task).await });

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
    async fn test_have_resolver_cancels_watch() -> Result<()> {
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

        // Create have resolver
        let have_resolver = HaveResolver::new(stub_peer.clone(), server_ch);

        // Create cancellation token
        let cancel = CancellationToken::new();
        let cancel_task = cancel.clone();

        // Spawn the have resolver service
        let handle = tokio::spawn(async move { have_resolver.run(cancel_task).await });

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
    async fn test_have_resolver_handles_value_changes() -> Result<()> {
        // Create a stub peer with a recording merge_have_map_result
        let mut stub_peer = StubPeer::new();
        let recorded_key = Arc::new(RwLock::new(None));
        let recorded_subkeys = Arc::new(RwLock::new(None));

        let recorded_key_clone = recorded_key.clone();
        let recorded_subkeys_clone = recorded_subkeys.clone();

        stub_peer.merge_have_map_result = Arc::new(Mutex::new(
            move |key: TypedKey, subkeys: ValueSubkeyRangeSet, _have_map: &mut PieceMap| {
                *recorded_key_clone.write().unwrap() = Some(key);
                *recorded_subkeys_clone.write().unwrap() = Some(subkeys);
                Ok(())
            },
        ));

        // Create a test key and channel
        let test_key =
            TypedKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");
        let (mut client_ch, server_ch) = pipe(16);

        // Create have resolver
        let update_tx = stub_peer.update_tx.clone();
        let have_resolver = HaveResolver::new(stub_peer, server_ch);

        // Create cancellation token
        let cancel = CancellationToken::new();
        let cancel_task = cancel.clone();

        // Spawn the have resolver service
        let handle = tokio::spawn(async move { have_resolver.run(cancel_task).await });

        // Send a request and receive a response, to make sure the task is
        // running. That's important: if we're not in the run loop, the update
        // send may fail to broadcast.
        client_ch
            .tx
            .send(Request::Resolve {
                key: test_key.clone(),
                subkeys: 100,
            })
            .await
            .unwrap();
        assert!(matches!(client_ch.rx.recv().await, Some(_)));

        // Simulate a value change notification
        let change = VeilidValueChange {
            key: test_key.clone(),
            subkeys: ValueSubkeyRangeSet::single_range(0, 99),
            value: Some(
                ValueData::new(b"foo".to_vec(), CryptoKey::new([0xbe; CRYPTO_KEY_LENGTH]))
                    .expect("new value data"),
            ),
            count: 1,
        };
        update_tx
            .send(veilid_core::VeilidUpdate::ValueChange(Box::new(change)))
            .expect("send value change");

        // Verify the response
        match client_ch.rx.recv().await.expect("recv") {
            Response::Resolve { key, pieces } => {
                assert_eq!(key, test_key);
                let _pieces = pieces.read().await; // Just verify we can access it
            }
            other => panic!("Unexpected response: {:?}", other),
        }

        // Verify the merge was called correctly
        let recorded_key = recorded_key.read().unwrap();
        let recorded_subkeys = recorded_subkeys.read().unwrap();

        assert!(recorded_key.is_some(), "Key was not recorded");
        assert!(recorded_subkeys.is_some(), "Subkeys were not recorded");
        assert_eq!(recorded_key.as_ref().unwrap(), &test_key);
        assert_eq!(
            recorded_subkeys.as_ref().unwrap(),
            &ValueSubkeyRangeSet::single_range(0, 99)
        );

        // Clean up
        cancel.cancel();
        handle.await.unwrap().unwrap();

        Ok(())
    }
}
