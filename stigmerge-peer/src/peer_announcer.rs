use std::collections::HashMap;

use tokio::select;
use tokio_util::sync::CancellationToken;

use crate::{
    chan_rpc::{ChanServer, Service},
    peer::TypedKey,
    Error, Peer, Result,
};

/// Peer-map announcer request messages.
pub enum Request {
    /// Announce a known remote peer in good standing.
    Announce { key: TypedKey },

    /// Redact a known peer, it may be unavailable or defective
    /// from the point of view of this peer.
    Redact { key: TypedKey },

    /// Clear all peer announcements.
    Reset,
}

/// Peer-map announcer response message, just an acknowledgement or error.
pub type Response = Result<()>;

/// The peer-announcer service handles requests to announce or redact remote
/// peers known by this peer.
///
/// Each remote peer is encoded to a separate subkey. Redacted peers are
/// marked with empty contents.
pub struct PeerAnnouncer<P: Peer> {
    peer: P,
    key: TypedKey,
    ch: ChanServer<Request, Response>,
    peer_indexes: HashMap<TypedKey, usize>,
    peers: Vec<Option<TypedKey>>,
    max_peers: u16,
}

impl<P: Peer> Service for PeerAnnouncer<P> {
    type Request = Request;
    type Response = Response;

    async fn run(mut self, cancel: CancellationToken) -> Result<()> {
        self.peer
            .reset_peers(self.key.to_owned(), self.max_peers)
            .await?;
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
                    let resp = self.handle(&req).await?;
                    self.ch.tx.send(resp).await.map_err(Error::other)?;
                }
            }
        }
    }

    async fn handle(&mut self, req: &Self::Request) -> Result<Self::Response> {
        Ok(match req {
            Request::Announce { key } => {
                if !self.peer_indexes.contains_key(key) {
                    let index = self.assign_peer_index(*key);
                    self.peer
                        .announce_peer(self.key.to_owned(), Some(*key), index)
                        .await?;
                }
                Ok(())
            }
            Request::Redact { key } => {
                if let Some(index) = self.peer_indexes.get(key) {
                    let subkey = TryInto::<u16>::try_into(*index).unwrap();
                    self.peer
                        .announce_peer(self.key.to_owned(), None, subkey)
                        .await?;
                    self.peers[*index] = None;
                    self.peer_indexes.remove(key);
                }
                Ok(())
            }
            Request::Reset => {
                self.peer
                    .reset_peers(self.key.to_owned(), self.max_peers)
                    .await?;
                self.peers.clear();
                self.peer_indexes.clear();
                Ok(())
            }
        })
    }
}

pub const DEFAULT_MAX_PEERS: u16 = 32;

impl<P: Peer> PeerAnnouncer<P> {
    /// Create a new peer_announcer service.
    pub(super) fn new(peer: P, key: TypedKey, ch: ChanServer<Request, Response>) -> Self {
        Self {
            peer,
            key,
            ch,
            peer_indexes: HashMap::new(),
            peers: vec![],
            max_peers: DEFAULT_MAX_PEERS,
        }
    }

    fn assign_peer_index(&mut self, key: TypedKey) -> u16 {
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

#[cfg(test)]
mod tests {
    use std::{
        str::FromStr,
        sync::{Arc, Mutex, RwLock},
    };

    use tokio_util::sync::CancellationToken;
    use veilid_core::TypedKey;

    use crate::{
        chan_rpc::{pipe, Service},
        error::Result,
        peer_announcer::{PeerAnnouncer, Request},
        tests::StubPeer,
    };

    #[tokio::test]
    async fn test_peer_announcer_announces_peer() -> Result<()> {
        // Create a stub peer with a recording announce_peer_result
        let mut stub_peer = StubPeer::new();
        let recorded_key = Arc::new(RwLock::new(None));
        let recorded_peer_key = Arc::new(RwLock::new(None));
        let recorded_index = Arc::new(RwLock::new(None));

        let recorded_key_clone = recorded_key.clone();
        let recorded_peer_key_clone = recorded_peer_key.clone();
        let recorded_index_clone = recorded_index.clone();

        let recorded_resets = Arc::new(RwLock::new(0u32));
        let recorded_resets_clone = recorded_resets.clone();
        stub_peer.reset_peers_result = Arc::new(Mutex::new(move |_key, _subkeys| {
            let mut count = recorded_resets_clone.write().unwrap();
            *count += 1;
            Ok(())
        }));

        stub_peer.announce_peer_result = Arc::new(Mutex::new(
            move |key: TypedKey, peer_key: Option<TypedKey>, index: u16| {
                *recorded_key_clone.write().unwrap() = Some(key);
                *recorded_peer_key_clone.write().unwrap() = peer_key;
                *recorded_index_clone.write().unwrap() = Some(index);
                Ok(())
            },
        ));

        // Create a test key and channel
        let test_key =
            TypedKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");
        let peer_key =
            TypedKey::from_str("VLD0:dDHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");
        let (mut client_ch, server_ch) = pipe(16);

        // Create peer announcer
        let peer_announcer = PeerAnnouncer::new(stub_peer.clone(), test_key.clone(), server_ch);

        // Create cancellation token
        let cancel = CancellationToken::new();
        let cancel_task = cancel.clone();

        // Spawn the peer announcer service
        let handle = tokio::spawn(async move { peer_announcer.run(cancel_task).await });

        // Send an Announce request
        let req = Request::Announce {
            key: peer_key.clone(),
        };
        client_ch.tx.send(req).await.unwrap();
        client_ch
            .rx
            .recv()
            .await
            .expect("recv")
            .expect("announce ok");

        // Verify the peer was announced correctly
        let recorded_key = recorded_key.read().unwrap();
        let recorded_peer_key = recorded_peer_key.read().unwrap();
        let recorded_index = recorded_index.read().unwrap();

        assert!(recorded_key.is_some(), "Key was not recorded");
        assert!(recorded_peer_key.is_some(), "Peer key was not recorded");
        assert!(recorded_index.is_some(), "Index was not recorded");
        assert_eq!(recorded_key.as_ref(), Some(&test_key));
        assert_eq!(recorded_peer_key.as_ref(), Some(&peer_key));
        assert_eq!(recorded_index.as_ref(), Some(&0));

        assert_eq!(*recorded_resets.read().unwrap(), 1u32);

        // Clean up
        cancel.cancel();
        handle.await.unwrap().unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_peer_announcer_redacts_peer() -> Result<()> {
        // Create a stub peer with recording announce_peer_result
        let mut stub_peer = StubPeer::new();
        let recorded_key = Arc::new(RwLock::new(None));
        let recorded_peer_key = Arc::new(RwLock::new(None));
        let recorded_index = Arc::new(RwLock::new(None));

        let recorded_key_clone = recorded_key.clone();
        let recorded_peer_key_clone = recorded_peer_key.clone();
        let recorded_index_clone = recorded_index.clone();

        let recorded_resets = Arc::new(RwLock::new(0u32));
        let recorded_resets_clone = recorded_resets.clone();
        stub_peer.reset_peers_result = Arc::new(Mutex::new(move |_key, _subkeys| {
            let mut count = recorded_resets_clone.write().unwrap();
            *count += 1;
            Ok(())
        }));

        stub_peer.announce_peer_result = Arc::new(Mutex::new(
            move |key: TypedKey, peer_key: Option<TypedKey>, index: u16| {
                *recorded_key_clone.write().unwrap() = Some(key);
                *recorded_peer_key_clone.write().unwrap() = peer_key;
                *recorded_index_clone.write().unwrap() = Some(index);
                Ok(())
            },
        ));

        // Create a test key and channel
        let test_key =
            TypedKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");
        let peer_key =
            TypedKey::from_str("VLD0:dDHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");
        let (mut client_ch, server_ch) = pipe(16);

        // Create peer announcer
        let peer_announcer = PeerAnnouncer::new(stub_peer.clone(), test_key.clone(), server_ch);

        // Create cancellation token
        let cancel = CancellationToken::new();
        let cancel_task = cancel.clone();

        // Spawn the peer announcer service
        let handle = tokio::spawn(async move { peer_announcer.run(cancel_task).await });

        // First announce a peer
        let req_announce = Request::Announce {
            key: peer_key.clone(),
        };
        client_ch.tx.send(req_announce).await.unwrap();
        client_ch
            .rx
            .recv()
            .await
            .expect("recv")
            .expect("announce ok");

        // Then redact it
        let req_redact = Request::Redact {
            key: peer_key.clone(),
        };
        client_ch.tx.send(req_redact).await.unwrap();
        client_ch.rx.recv().await.expect("recv").expect("redact ok");

        // Verify the peer was redacted correctly
        let recorded_key = recorded_key.read().unwrap();
        let recorded_peer_key = recorded_peer_key.read().unwrap();
        let recorded_index = recorded_index.read().unwrap();

        assert!(recorded_key.is_some(), "Key was not recorded");
        assert!(recorded_peer_key.is_none(), "Peer key was not recorded");
        assert!(recorded_index.is_some(), "Index was not recorded");
        assert_eq!(recorded_key.as_ref(), Some(&test_key));
        assert_eq!(recorded_peer_key.as_ref(), None); // Redacted peer has None
        assert_eq!(recorded_index.as_ref(), Some(&0));

        assert_eq!(*recorded_resets.read().unwrap(), 1u32);

        // Clean up
        cancel.cancel();
        handle.await.unwrap().unwrap();

        Ok(())
    }

    #[tokio::test]
    async fn test_peer_announcer_resets_peers() -> Result<()> {
        // Create a stub peer with recording reset_peers_result
        let mut stub_peer = StubPeer::new();
        let recorded_key = Arc::new(RwLock::new(None));
        let recorded_max_peers = Arc::new(RwLock::new(None));

        let recorded_key_clone = recorded_key.clone();
        let recorded_max_peers_clone = recorded_max_peers.clone();

        stub_peer.reset_peers_result =
            Arc::new(Mutex::new(move |key: TypedKey, max_peers: u16| {
                *recorded_key_clone.write().unwrap() = Some(key);
                *recorded_max_peers_clone.write().unwrap() = Some(max_peers);
                Ok(())
            }));

        // Create a test key and channel
        let test_key =
            TypedKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");
        let (mut client_ch, server_ch) = pipe(16);

        // Create peer announcer
        let peer_announcer = PeerAnnouncer::new(stub_peer.clone(), test_key.clone(), server_ch);

        // Create cancellation token
        let cancel = CancellationToken::new();
        let cancel_task = cancel.clone();

        // Spawn the peer announcer service
        let handle = tokio::spawn(async move { peer_announcer.run(cancel_task).await });

        // Send a Reset request
        let req = Request::Reset;
        client_ch.tx.send(req).await.unwrap();
        client_ch.rx.recv().await.expect("recv").expect("reset ok");

        // Verify the peers were reset correctly
        let recorded_key = recorded_key.read().unwrap();
        let recorded_max_peers = recorded_max_peers.read().unwrap();

        assert!(recorded_key.is_some(), "Key was not recorded");
        assert!(recorded_max_peers.is_some(), "Max peers was not recorded");
        assert_eq!(recorded_key.as_ref(), Some(&test_key));
        assert_eq!(recorded_max_peers.as_ref(), Some(&32)); // DEFAULT_MAX_PEERS

        // Clean up
        cancel.cancel();
        handle.await.unwrap().unwrap();

        Ok(())
    }
}
