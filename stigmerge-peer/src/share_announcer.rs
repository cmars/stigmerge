use std::{fmt, time::Duration};

use anyhow::Context;
use stigmerge_fileindex::Index;
use tokio::{
    select,
    time::{interval, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;
use tracing::{info, trace};
use veilid_core::{Target, TypedRecordKey, VeilidUpdate};

use crate::{
    actor::{Actor, Respondable, ResponseChannel},
    error::{CancelError, Unrecoverable},
    proto::Header,
    Node, Result,
};

pub struct ShareAnnouncer<N: Node> {
    node: N,
    index: Index,
    share: Option<ShareAnnounce>,
}

#[derive(Clone)]
struct ShareAnnounce {
    key: TypedRecordKey,
    target: Target,
    header: Header,
}

impl<N: Node> ShareAnnouncer<N> {
    pub fn new(node: N, index: Index) -> ShareAnnouncer<N> {
        ShareAnnouncer {
            node,
            index,
            share: None,
        }
    }

    pub fn share(&self) -> Option<(&TypedRecordKey, &Target)> {
        self.share.as_ref().map(|share| (&share.key, &share.target))
    }

    async fn announce(&mut self) -> Result<()> {
        let (key, target, header) = self.node.announce_index(&self.index).await?;
        self.share = Some(ShareAnnounce {
            key: key.clone(),
            target: target.clone(),
            header: header.clone(),
        });
        Ok(())
    }

    async fn reannounce(&mut self) -> Result<Response> {
        match self.share.as_mut() {
            Some(ShareAnnounce {
                key,
                target,
                header,
            }) => {
                let (updated_target, updated_header) =
                    self.node.announce_route(key, Some(*target), header).await?;
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

pub enum Request {
    Announce {
        response_tx: ResponseChannel<Response>,
    },
}

impl fmt::Debug for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Announce { .. } => f.debug_struct("Announce").finish(),
        }
    }
}

impl Respondable for Request {
    type Response = Response;

    fn set_response(&mut self, ch: ResponseChannel<Self::Response>) {
        match self {
            Request::Announce { response_tx } => *response_tx = ch,
        }
    }

    fn response_tx(self) -> ResponseChannel<Self::Response> {
        match self {
            Request::Announce { response_tx } => response_tx,
        }
    }
}

#[derive(Clone, Debug)]
pub enum Response {
    NotAvailable,
    Announce {
        key: TypedRecordKey,
        target: Target,
        header: Header,
    },
}

const REANNOUNCE_INTERVAL_SECS: u64 = 600;

impl<P: Node> Actor for ShareAnnouncer<P> {
    type Request = Request;
    type Response = Response;

    async fn run(
        &mut self,
        cancel: CancellationToken,
        request_rx: flume::Receiver<Self::Request>,
    ) -> Result<()> {
        let mut update_rx = self.node.subscribe_veilid_update();
        let mut public_internet_ready = false;
        let mut reannounce_interval = interval(Duration::from_secs(REANNOUNCE_INTERVAL_SECS));
        reannounce_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        reannounce_interval.reset();
        loop {
            match self.share {
                None => {
                    self.announce().await?;
                    reannounce_interval.reset();
                }
                Some(ShareAnnounce { target, .. }) => {
                    select! {
                        _ = cancel.cancelled() => {
                            return Err(CancelError.into());
                        }
                        res = request_rx.recv_async() => {
                            let req = res.with_context(|| format!("share_announcer: receive request"))?;
                            self.handle_request(req).await?;
                        }
                        res = update_rx.recv() => {
                            if let Target::PrivateRoute(ref route_id) = target {
                                let update = res.with_context(|| format!("share_announcer: receive veilid update"))?;
                                match update {
                                    VeilidUpdate::RouteChange(route_change) => {
                                        trace!("current route {route_id}, dead routes {:?}", route_change.dead_routes);
                                        if route_change.dead_routes.contains(route_id) {
                                            info!("route changed, reannouncing");
                                            self.reannounce().await.with_context(|| format!("share_announcer: route changed"))?;
                                            reannounce_interval.reset();
                                        }
                                    },
                                    VeilidUpdate::Attachment(veilid_state_attachment) => {
                                        if veilid_state_attachment.public_internet_ready != public_internet_ready {
                                            if veilid_state_attachment.public_internet_ready {
                                                info!("attachment public internet ready, reannouncing");
                                                self.reannounce().await.with_context(|| format!("share_announcer: route changed"))?;
                                                reannounce_interval.reset();
                                            }
                                            public_internet_ready = veilid_state_attachment.public_internet_ready;
                                        }
                                    }
                                    VeilidUpdate::Shutdown => {
                                        cancel.cancel();
                                    }
                                    _ => {}
                                }
                            }
                        }
                        _ = reannounce_interval.tick() => {
                            info!("reannouncing after {:?}", reannounce_interval.period());
                            self.reannounce().await.with_context(|| format!("share_announcer: route changed"))?;
                        }
                    }
                }
            }
        }
    }

    async fn handle_request(&mut self, req: Self::Request) -> Result<()> {
        let response = match self.reannounce().await {
            Ok(resp) => resp,
            Err(_) => Response::NotAvailable,
        };

        req.response_tx()
            .send(response)
            .await
            .context(Unrecoverable::new("send response from share announcer"))?;

        Ok(())
    }
}

impl<N: Node + Clone> Clone for ShareAnnouncer<N> {
    fn clone(&self) -> Self {
        Self {
            node: self.node.clone(),
            index: self.index.clone(),
            share: self.share.clone(),
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
    use veilid_core::{RouteId, Target, TypedRecordKey};

    use crate::{
        actor::{OneShot, Operator, ResponseChannel},
        proto::{Encoder, Header},
        share_announcer::{self, ShareAnnouncer},
        tests::{temp_file, StubNode},
    };

    #[tokio::test]
    async fn test_announce() {
        // Create a test file and index
        let tf = temp_file(0xa5u8, 65536);
        let indexer = Indexer::from_file(std::env::temp_dir().join(tf.path()).as_path())
            .await
            .expect("indexer");
        let index = indexer.index().await.expect("index");

        // Create a stub peer with mock behavior
        let mut node = StubNode::new();
        let fake_key = TypedRecordKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M")
            .expect("key");
        let fake_target = Target::PrivateRoute(RouteId::new([0u8; 32]));

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
        let mut actor = ShareAnnouncer::new(node, index);
        actor.announce().await.expect("announce");
        assert_eq!(actor.share(), Some((&mock_key, &mock_target)));
    }

    #[tokio::test]
    async fn test_reannounce() {
        // Create a test file and index
        let tf = temp_file(0xa5u8, 65536);
        let indexer = Indexer::from_file(std::env::temp_dir().join(tf.path()).as_path())
            .await
            .expect("indexer");
        let index = indexer.index().await.expect("index");

        // Create a stub peer with mock behavior
        let mut node = StubNode::new();
        let fake_key = TypedRecordKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M")
            .expect("key");
        let fake_target = Target::PrivateRoute(RouteId::new([0u8; 32]));
        let updated_target = Target::PrivateRoute(RouteId::new([1u8; 32]));

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
        node.announce_route_result = Arc::new(Mutex::new(
            move |_key: &TypedRecordKey, _prior_route: Option<Target>, header: &Header| {
                let updated_header = header.with_route_data(vec![0xde, 0xca, 0xfb, 0xad]);
                Ok((mock_updated_target.clone(), updated_header))
            },
        ));

        // Create the service and channels
        let cancel = CancellationToken::new();
        let mut operator = Operator::new(cancel.clone(), ShareAnnouncer::new(node, index), OneShot);

        // Send a reannounce request
        match operator
            .call(share_announcer::Request::Announce {
                response_tx: ResponseChannel::default(),
            })
            .await
            .expect("send request")
        {
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
        operator.join().await.expect_err("cancelled");
    }

    // This test directly tests the reannounce method without using the Actor trait
    #[tokio::test]
    async fn test_not_available_response() {
        // Create a test file and index
        let tf = temp_file(0xa5u8, 65536);
        let indexer = Indexer::from_file(std::env::temp_dir().join(tf.path()).as_path())
            .await
            .expect("indexer");
        let index = indexer.index().await.expect("index");

        // Create a stub peer with mock behavior
        let node = StubNode::new();

        // Create the service directly without using Operator
        let mut announcer = ShareAnnouncer::new(node, index);

        // Test the reannounce method directly with no share set
        let response = announcer.reannounce().await.expect("reannounce");
        assert!(matches!(response, share_announcer::Response::NotAvailable));
    }
}
