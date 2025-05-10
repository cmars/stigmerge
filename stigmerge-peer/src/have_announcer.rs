use std::{ops::Deref, sync::Arc, time::Duration};

use tokio::{select, sync::RwLock};
use tokio_util::sync::CancellationToken;
use tracing::Level;

use crate::{
    actor::{Actor, ChanServer},
    error::Result,
    node::TypedKey,
    piece_map::PieceMap,
    Node,
};

/// The have-announcer service handles requests for announcing the share pieces
/// verified by this peer. The have map is written to the DHT at a have-map
/// reference key, as an uncompressed bitmap of contiguous bits, indexed by
/// piece index: a set bit (1) indicating the peer has the piece, a clear bit
/// (0) indicating the peer does not have the piece.
#[derive(Clone)]
pub struct HaveAnnouncer<N: Node> {
    node: N,
    key: TypedKey,
    pieces_map: Arc<RwLock<PieceMap>>,
    announce_interval: Duration,
}

impl<N: Node> HaveAnnouncer<N> {
    /// Create a new have_announcer service.
    #[tracing::instrument(skip_all)]
    pub fn new(node: N, key: TypedKey) -> Self {
        Self {
            node,
            key,
            pieces_map: Arc::new(RwLock::new(PieceMap::new())),
            announce_interval: Duration::from_secs(15),
        }
    }
}

/// Have-map announcer request messages.
#[derive(Clone, Debug)]
pub enum Request {
    /// Announce that this peer has a given piece.
    Set { piece_index: u32 },

    /// Announce that this peer does not have a given piece.
    Clear { piece_index: u32 },

    /// Announce that this peer has no pieces.
    Reset,
}

/// Have-map announcer response message, just an acknowledgement or error.
pub type Response = ();

impl<P: Node> Actor for HaveAnnouncer<P> {
    type Request = Request;
    type Response = Response;

    #[tracing::instrument(skip_all, err(level = Level::TRACE), level = Level::TRACE)]
    async fn run(
        &mut self,
        cancel: CancellationToken,
        mut server_ch: ChanServer<Self::Request, Self::Response>,
    ) -> Result<()> {
        let mut changed = false;
        let mut interval = tokio::time::interval(self.announce_interval);
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
                    let resp = self.handle(&req).await?;
                    changed = true;
                    server_ch.send(resp).await?;
                }
                _ = interval.tick() => {
                    if changed {
                        let have_map = self.pieces_map.read().await;
                        self.node.announce_have_map(self.key.to_owned(), have_map.deref()).await?;
                        changed = false;
                    }
                }
            }
        }
    }

    #[tracing::instrument(skip_all, err(level = Level::TRACE), level = Level::TRACE)]
    async fn handle(&mut self, req: &Self::Request) -> Result<Self::Response> {
        let mut pieces_map = self.pieces_map.write().await;
        Ok(match req {
            Request::Set { piece_index } => {
                pieces_map.set(*piece_index);
            }
            Request::Clear { piece_index } => {
                pieces_map.clear(*piece_index);
            }
            Request::Reset => {
                pieces_map.reset();
            }
        })
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
        actor::{OneShot, Operator},
        have_announcer::{HaveAnnouncer, Request},
        piece_map::PieceMap,
        tests::StubNode,
    };

    #[tokio::test]
    async fn test_have_announcer_announces_have_map() {
        // Create a stub peer with a recording announce_have_map_result
        let mut node = StubNode::new();
        let recorded_have_map = Arc::new(RwLock::new(None));
        let recorded_key = Arc::new(RwLock::new(None));

        let recorded_have_map_clone = recorded_have_map.clone();
        let recorded_key_clone = recorded_key.clone();

        node.announce_have_map_result =
            Arc::new(Mutex::new(move |key: TypedKey, have_map: &PieceMap| {
                *recorded_have_map_clone.write().unwrap() = Some(have_map.clone());
                *recorded_key_clone.write().unwrap() = Some(key.clone());
                Ok(())
            }));

        // Create a test key and channel
        let test_key =
            TypedKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");

        // Create have announcer with a short announce interval
        let mut have_announcer = HaveAnnouncer::new(node.clone(), test_key.clone());
        have_announcer.announce_interval = std::time::Duration::from_millis(1);
        let cancel = CancellationToken::new();
        let mut operator = Operator::new(cancel.clone(), have_announcer, OneShot);

        // Send a Set request to announce a piece
        let req = Request::Set { piece_index: 42 };
        operator.send(req).await.unwrap();
        assert_eq!(operator.recv().await.expect("recv"), ());

        // Wait for the announcement to happen
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        // Verify the have map was announced correctly
        let recorded_map = recorded_have_map.read().unwrap();
        let recorded_key = recorded_key.read().unwrap();

        assert!(recorded_map.is_some(), "Have map was not recorded");
        assert!(recorded_key.is_some(), "Key was not recorded");

        let recorded_map = recorded_map.as_ref().unwrap();
        assert_eq!(recorded_key.as_ref().unwrap(), &test_key);
        assert!(
            recorded_map.get(42),
            "Piece index 42 was not set in the have map"
        );
        assert!(!recorded_map.get(0), "Other piece indices should be clear");
        assert!(!recorded_map.get(43), "Other piece indices should be clear");

        cancel.cancel();
        operator.join().await.expect("task").expect("run");
    }

    #[tokio::test]
    async fn test_have_announcer_clears_have_map() {
        // Create a stub peer with a recording announce_have_map_result
        let mut node = StubNode::new();
        let recorded_have_map = Arc::new(RwLock::new(None));

        let recorded_have_map_clone = recorded_have_map.clone();
        node.announce_have_map_result =
            Arc::new(Mutex::new(move |_key: TypedKey, have_map: &PieceMap| {
                *recorded_have_map_clone.write().unwrap() = Some(have_map.clone());
                Ok(())
            }));

        // Create a test key and channel
        let test_key =
            TypedKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");
        let mut have_announcer = HaveAnnouncer::new(node.clone(), test_key);
        have_announcer.announce_interval = std::time::Duration::from_millis(1);
        let cancel = CancellationToken::new();
        let mut operator = Operator::new(cancel.clone(), have_announcer, OneShot);

        // First set a piece
        let req_set = Request::Set { piece_index: 42 };
        operator.send(req_set).await.unwrap();
        assert_eq!(operator.recv().await.expect("recv"), ());

        // Then clear it
        let req_clear = Request::Clear { piece_index: 42 };
        operator.send(req_clear).await.unwrap();
        assert_eq!(operator.recv().await.expect("recv"), ());

        // Wait for the announcements to happen
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        // Verify the have map was cleared
        let recorded_map = recorded_have_map.read().unwrap();
        assert!(recorded_map.is_some(), "Have map was not recorded");
        let recorded_map = recorded_map.as_ref().unwrap();
        assert!(!recorded_map.get(42), "Piece index 42 should be cleared");
        assert!(!recorded_map.get(0), "Other piece indices should be clear");
        assert!(!recorded_map.get(43), "Other piece indices should be clear");

        cancel.cancel();
        operator.join().await.expect("task").expect("run");
    }

    #[tokio::test]
    async fn test_have_announcer_resets_have_map() {
        // Create a stub peer with a recording announce_have_map_result
        let mut node = StubNode::new();
        let recorded_have_map = Arc::new(RwLock::new(None));

        let recorded_have_map_clone = recorded_have_map.clone();
        node.announce_have_map_result =
            Arc::new(Mutex::new(move |_key: TypedKey, have_map: &PieceMap| {
                *recorded_have_map_clone.write().unwrap() = Some(have_map.clone());
                Ok(())
            }));

        // Create a test key and channel
        let test_key =
            TypedKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");

        let mut have_announcer = HaveAnnouncer::new(node.clone(), test_key);
        let cancel = CancellationToken::new();
        have_announcer.announce_interval = std::time::Duration::from_millis(1);
        let mut operator = Operator::new(cancel.clone(), have_announcer, OneShot);

        // First set some pieces
        let req_set = Request::Set { piece_index: 42 };
        operator.send(req_set).await.unwrap();
        assert_eq!(operator.recv().await.expect("recv"), ());

        // Then reset the map
        let req_reset = Request::Reset;
        operator.send(req_reset).await.unwrap();
        assert_eq!(operator.recv().await.expect("recv"), ());

        // Wait for the announcements to happen
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        // Verify the have map was reset
        let recorded_map = recorded_have_map.read().unwrap();
        assert!(recorded_map.is_some(), "Have map was not recorded");
        let recorded_map = recorded_map.as_ref().unwrap();
        assert!(
            !recorded_map.get(42),
            "Piece index 42 should be cleared after reset"
        );
        assert!(!recorded_map.get(0), "Other piece indices should be clear");
        assert!(!recorded_map.get(43), "Other piece indices should be clear");

        cancel.cancel();
        operator.join().await.expect("task").expect("run");
    }
}
