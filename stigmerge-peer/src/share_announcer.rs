use stigmerge_fileindex::Index;
use tokio::{select, sync::broadcast};
use tokio_util::sync::CancellationToken;
use tracing::info;
use veilid_core::{Target, VeilidUpdate};

use crate::{actor::Actor, node::TypedKey, proto::Header, Node, Result};

pub struct ShareAnnouncer<N: Node> {
    node: N,
    index: Index,
    share: Option<ShareAnnounce>,
    update_rx: broadcast::Receiver<VeilidUpdate>,
}

#[derive(Clone)]
struct ShareAnnounce {
    key: TypedKey,
    target: Target,
    header: Header,
}

impl<N: Node> ShareAnnouncer<N> {
    pub fn new(node: N, index: Index) -> ShareAnnouncer<N> {
        let update_rx = node.subscribe_veilid_update();
        ShareAnnouncer {
            node,
            index,
            share: None,
            update_rx,
        }
    }

    async fn announce(&mut self) -> Result<Response> {
        let (key, target, header) = self.node.announce_index(&self.index).await?;
        self.share = Some(ShareAnnounce {
            key: key.clone(),
            target: target.clone(),
            header: header.clone(),
        });
        Ok(Response::Announce {
            key,
            target,
            header,
        })
    }

    async fn reannounce(&mut self) -> Result<Response> {
        match self.share.as_mut() {
            Some(ShareAnnounce {
                key,
                target,
                header,
            }) => {
                let (updated_target, updated_header) = self
                    .node
                    .reannounce_route(key, Some(*target), &self.index, header)
                    .await?;
                *target = updated_target;
                *header = updated_header;
                Ok(Response::Announce {
                    key: *key,
                    target: *target,
                    header: header.clone(),
                })
            }
            None => Ok(Response::NotAvailable),
        }
    }
}

#[derive(Clone)]
pub enum Request {
    Announce,
}

#[derive(Clone)]
pub enum Response {
    NotAvailable,
    Announce {
        key: TypedKey,
        target: Target,
        header: Header,
    },
}

impl<P: Node> Actor for ShareAnnouncer<P> {
    type Request = Request;
    type Response = Response;

    #[tracing::instrument(skip_all, err)]
    async fn run(
        &mut self,
        cancel: CancellationToken,
        mut server_ch: crate::actor::ChanServer<Self::Request, Self::Response>,
    ) -> Result<()> {
        self.share = None;
        loop {
            match self.share {
                None => {
                    let resp = self.announce().await?;
                    server_ch.send(resp).await?;
                }
                Some(ShareAnnounce { target, .. }) => {
                    select! {
                        _ = cancel.cancelled() => {
                            return Ok(());
                        }
                        res = server_ch.recv() => {
                            if let Some(req) = res {
                                let resp = self.handle(&req).await?;
                                server_ch.send(resp).await?;
                            }
                        }
                        res = self.update_rx.recv() => {
                            if let Target::PrivateRoute(ref route_id) = target {
                                let update = res?;
                                match update {
                                    VeilidUpdate::RouteChange(route_change) => {
                                        if route_change.dead_routes.contains(route_id) {
                                            info!("route changed, reannouncing");
                                            self.reannounce().await?;
                                        }
                                    },
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    async fn handle(&mut self, req: &Self::Request) -> Result<Self::Response> {
        match req {
            Request::Announce => {
                return self.reannounce().await;
            }
        }
    }
}

impl<N: Node> Clone for ShareAnnouncer<N> {
    fn clone(&self) -> Self {
        Self {
            node: self.node.clone(),
            index: self.index.clone(),
            share: self.share.clone(),
            update_rx: self.update_rx.resubscribe(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        str::FromStr,
        sync::{Arc, Mutex},
    };

    use stigmerge_fileindex::Indexer;
    use tokio_util::sync::CancellationToken;
    use veilid_core::{CryptoKey, Target, TypedKey};

    use crate::{
        actor::{Actor, OneShot, Operator},
        proto::{Encoder, Header},
        share_announcer::{self, ShareAnnouncer},
        tests::{temp_file, StubNode},
    };

    #[tokio::test]
    async fn test_announce() {
        // Create a test file and index
        let tf = temp_file(0xa5u8, 65536);
        let indexer = Indexer::from_file(std::env::temp_dir().join(tf.path()).into())
            .await
            .expect("indexer");
        let index = indexer.index().await.expect("index");

        // Create a stub peer with mock behavior
        let mut node = StubNode::new();
        let fake_key =
            TypedKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");
        let fake_target = Target::PrivateRoute(CryptoKey::new([0u8; 32]));

        // Set up the announce_result mock
        let mock_key = fake_key.clone();
        let mock_target = fake_target.clone();
        node.announce_result = Arc::new(Mutex::new(move |index: &stigmerge_fileindex::Index| {
            let index_bytes = index.encode().expect("encode index");
            let header =
                Header::from_index(index, index_bytes.as_slice(), &[0xde, 0xad, 0xbe, 0xef]);
            Ok((mock_key.clone(), mock_target.clone(), header))
        }));

        // Create the service and channels
        let cancel = CancellationToken::new();
        let mut operator = Operator::new(cancel.clone(), ShareAnnouncer::new(node, index), OneShot);

        // Wait for the initial announce response
        let announce_resp = operator.recv().await.expect("response");
        match announce_resp {
            share_announcer::Response::Announce {
                key,
                target,
                header: _,
            } => {
                assert_eq!(key, fake_key);
                assert_eq!(target, fake_target);
            }
            _ => panic!("Expected Announce response"),
        }

        // Initiate a shutdown
        cancel.cancel();

        // Service run terminates
        operator.join().await.expect("svc task").expect("svc run");
    }

    #[tokio::test]
    async fn test_reannounce() {
        // Create a test file and index
        let tf = temp_file(0xa5u8, 65536);
        let indexer = Indexer::from_file(std::env::temp_dir().join(tf.path()).into())
            .await
            .expect("indexer");
        let index = indexer.index().await.expect("index");

        // Create a stub peer with mock behavior
        let mut node = StubNode::new();
        let fake_key =
            TypedKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");
        let fake_target = Target::PrivateRoute(CryptoKey::new([0u8; 32]));
        let updated_target = Target::PrivateRoute(CryptoKey::new([1u8; 32]));

        // Set up the announce_result mock
        let mock_key = fake_key.clone();
        let mock_target = fake_target.clone();
        node.announce_result = Arc::new(Mutex::new(move |index: &stigmerge_fileindex::Index| {
            let index_bytes = index.encode().expect("encode index");
            let header =
                Header::from_index(index, index_bytes.as_slice(), &[0xde, 0xad, 0xbe, 0xef]);
            Ok((mock_key.clone(), mock_target.clone(), header))
        }));

        // Set up the reannounce_route_result mock
        let mock_updated_target = updated_target.clone();
        node.reannounce_route_result = Arc::new(Mutex::new(
            move |_key: &TypedKey,
                  _prior_route: Option<Target>,
                  index: &stigmerge_fileindex::Index,
                  _header: &Header| {
                let index_bytes = index.encode().expect("encode index");
                let updated_header =
                    Header::from_index(index, index_bytes.as_slice(), &[0xde, 0xca, 0xfb, 0xad]);
                Ok((mock_updated_target.clone(), updated_header))
            },
        ));

        // Create the service and channels
        let cancel = CancellationToken::new();
        let mut operator = Operator::new(cancel.clone(), ShareAnnouncer::new(node, index), OneShot);

        // Wait for the initial announce response
        let announce_resp = operator.recv().await.expect("response");
        match announce_resp {
            share_announcer::Response::Announce {
                key,
                target,
                header,
            } => {
                assert_eq!(key, fake_key);
                assert_eq!(target, fake_target);
                assert_eq!(header.route_data(), &[0xde, 0xad, 0xbe, 0xef])
            }
            _ => panic!("Expected Announce response"),
        }

        // Send a reannounce request
        operator
            .send(share_announcer::Request::Announce)
            .await
            .expect("send request");

        // Wait for the reannounce response
        let reannounce_resp = operator.recv().await.expect("response");
        match reannounce_resp {
            share_announcer::Response::Announce {
                key,
                target,
                header,
            } => {
                assert_eq!(key, fake_key);
                assert_eq!(target, updated_target);
                assert_eq!(header.route_data(), &[0xde, 0xca, 0xfb, 0xad])
            }
            _ => panic!("Expected Announce response"),
        }

        // Initiate a shutdown
        cancel.cancel();

        // Service run terminates
        operator.join().await.expect("svc task").expect("svc run");
    }

    #[tokio::test]
    async fn test_not_available_response() {
        // Create a test file and index
        let tf = temp_file(0xa5u8, 65536);
        let indexer = Indexer::from_file(std::env::temp_dir().join(tf.path()).into())
            .await
            .expect("indexer");
        let index = indexer.index().await.expect("index");

        // Create a stub peer with mock behavior
        let peer = StubNode::new();

        // Create the service and operator with no share announced yet
        let mut announcer = ShareAnnouncer::new(peer, index);

        // Test the handle method directly with no share set
        let response = announcer
            .handle(&share_announcer::Request::Announce)
            .await
            .expect("handle");
        assert!(matches!(response, share_announcer::Response::NotAvailable));
    }
}
