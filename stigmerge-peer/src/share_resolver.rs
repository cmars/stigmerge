use std::{collections::HashSet, path::PathBuf};

use stigmerge_fileindex::Index;
use tokio::{select, sync::broadcast};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn, Level};
use veilid_core::{Target, ValueSubkeyRangeSet, VeilidUpdate};

use crate::{
    actor::{Actor, ChanServer},
    content_addressable::ContentAddressable,
    node::TypedKey,
    proto::{Digest, Header},
    Node, Result,
};

/// The share_resolver service maintains private routes to the route posted at remote
/// peers' share keys. It also validates that the remote peer is sharing
/// the expected index by verifying its content digest.
pub struct ShareResolver<N: Node> {
    node: N,
    target_tx: flume::Sender<(TypedKey, Target)>,
    target_rx: flume::Receiver<(TypedKey, Target)>,
    updates: broadcast::Receiver<veilid_core::VeilidUpdate>,
    watching: HashSet<TypedKey>,
}
impl<P: Node> ShareResolver<P> {
    /// Create a new share_resolver service.
    pub fn new(node: P) -> Self {
        let updates = node.subscribe_veilid_update();
        // Initialize the broadcast channel
        let (target_tx, target_rx) = flume::unbounded();
        Self {
            node,
            target_tx,
            target_rx,
            updates,
            watching: HashSet::new(),
        }
    }

    pub fn subscribe_target(&self) -> flume::Receiver<(TypedKey, Target)> {
        self.target_rx.clone()
    }
}

/// Share resolver request messages.
#[derive(Clone, Debug)]
pub enum Request {
    /// Resolve the Index located at a remote share key, with a known index
    /// digest, for merging into a local file share.
    ///
    /// If the remote share is valid, the share resolver will set up a
    /// continual, automatically-renewed watch for this share key.
    Index {
        key: TypedKey,
        want_index_digest: Option<Digest>,
        root: PathBuf,
    },

    /// Resolve the updated header for a remote share key, optionally providing
    /// a prior known private route target to release if rotated.
    ///
    /// If the remote share is valid, the share resolver will set up a
    /// continual, automatically-renewed watch for this share key.
    Header {
        key: TypedKey,
        prior_target: Option<Target>,
    },

    /// Stop watching this share key.
    Remove { key: TypedKey },
}

impl Request {
    /// Get the share key specified in the request.
    fn key(&self) -> &TypedKey {
        match self {
            Request::Index {
                key,
                want_index_digest: _,
                root: _,
            } => key,
            Request::Header {
                key,
                prior_target: _,
            } => key,
            Request::Remove { key } => key,
        }
    }
}

/// Share resolver response messages.
#[derive(Clone, Debug)]
pub enum Response {
    /// Remote share is not available at the given key, with error cause.
    NotAvailable { key: TypedKey, err_msg: String },

    /// Remote share has a bad index. This could be caused by a defective or malicious peer,
    /// or the wrong share key given for the desired index digest.
    BadIndex { key: TypedKey },

    /// Index is valid, in response to an Index request.
    Index {
        key: TypedKey,
        header: Header,
        index: Index,
        target: Target,
    },

    /// Header is updated. Could be in response to a header request, or unrequested if
    /// the watch detects a change.
    Header {
        key: TypedKey,
        header: Header,
        target: Target,
    },

    /// Acknowledgement that the share key watch has been removed.
    Remove { key: TypedKey },
}

impl Response {
    /// Get the share key for a valid usable share.
    fn valid_key(&self) -> Option<&TypedKey> {
        match self {
            Response::NotAvailable { key: _, err_msg: _ } => None,
            Response::BadIndex { key: _ } => None,
            Response::Index {
                key,
                header: _,
                index: _,
                target: _,
            } => Some(key),
            Response::Header {
                key,
                header: _,
                target: _,
            } => Some(key),
            Response::Remove { key: _ } => None,
        }
    }
}

impl<P: Node> Actor for ShareResolver<P> {
    type Request = Request;
    type Response = Response;

    #[tracing::instrument(skip_all, err(level = Level::TRACE), level = Level::TRACE)]
    async fn run(
        &mut self,
        cancel: CancellationToken,
        mut server_ch: ChanServer<Self::Request, Self::Response>,
    ) -> Result<()> {
        let mut updates = self.node.subscribe_veilid_update();
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
                            if let Some(valid_key) = resp.valid_key() {
                                // Valid usable shares are watched.
                                info!("watch: share key {valid_key}");
                                self.watching.insert(*valid_key);
                                self.node.watch(*valid_key, ValueSubkeyRangeSet::single(0)).await?;
                            } else {
                                // Invalid or unusable shares are unwatched.
                                self.watching.remove(req.key());
                                self.node.cancel_watch(req.key()).await?;
                            }
                            trace!(?resp);
                            server_ch.send(resp).await?;
                        }
                        Err(err) => {
                            warn!(?err);
                            server_ch.send(Response::NotAvailable { key: req.key().to_owned(), err_msg: err.to_string() }).await?;
                        }
                    }
                }
                res = updates.recv() => {
                    let update = res?;
                    match update {
                        VeilidUpdate::ValueChange(ch) => {
                            if !self.watching.contains(&ch.key) {
                                continue;
                            }
                            // FIXME: this may leak private routes.
                            // TODO: keep track of prior routes and pass them here?
                            let resp = self.handle(&Request::Header{ key: ch.key, prior_target: None }).await?;
                            server_ch.send(resp).await?;
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

    /// Handle a share_resolver request, provide a response.
    #[tracing::instrument(skip(self), err(level = Level::TRACE), level = Level::TRACE)]
    async fn handle(&mut self, req: &Request) -> Result<Response> {
        Ok(match req {
            Request::Index {
                key,
                want_index_digest,
                root,
            } => {
                debug!("Request::Index {key}");
                let (target, header, mut index) =
                    self.node.resolve_route_index(key, root.as_path()).await?;
                let peer_index_digest = index.digest()?;

                // If want_index_digest is None, skip verification
                // Otherwise, verify the digest matches
                let digest_matches = match want_index_digest {
                    None => true, // Skip verification if None
                    Some(digest) => peer_index_digest.as_slice() == digest,
                };

                if digest_matches {
                    let _ = self.target_tx.send((key.clone(), target.to_owned()));
                    Response::Index {
                        key: key.clone(),
                        header,
                        index,
                        target,
                    }
                } else {
                    Response::BadIndex { key: key.clone() }
                }
            }
            Request::Header {
                ref key,
                prior_target,
            } => {
                debug!("Request::Header {key}");
                let (target, header) = self.node.resolve_route(key, *prior_target).await?;
                let _ = self.target_tx.try_send((key.clone(), target.to_owned()));
                Response::Header {
                    key: key.clone(),
                    header,
                    target,
                }
            }
            Request::Remove { key } => Response::Remove {
                // No need to do anything; this response key is not valid, causing an unwatch
                key: key.clone(),
            },
        })
    }
}

impl<P: Node + Clone> Clone for ShareResolver<P> {
    fn clone(&self) -> Self {
        Self {
            node: self.node.clone(),
            target_tx: self.target_tx.clone(),
            target_rx: self.target_rx.clone(),
            updates: self.updates.resubscribe(),
            watching: self.watching.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        path::Path,
        str::FromStr,
        sync::{Arc, Mutex},
    };

    use sha2::{Digest as _, Sha256};
    use stigmerge_fileindex::Indexer;
    use tokio_util::sync::CancellationToken;
    use veilid_core::{CryptoKey, Target, TypedKey, ValueSubkeyRangeSet};

    use crate::actor::{OneShot, Operator};
    use crate::{
        proto::{Encoder, HaveMapRef, Header, PeerMapRef},
        share_resolver::{self, ShareResolver},
        tests::{temp_file, StubNode},
    };

    #[tokio::test]
    async fn test_resolve_index() {
        let tf = temp_file(0xa5u8, 65536);
        let indexer = Indexer::from_file(std::env::temp_dir().join(tf.path()).as_path())
            .await
            .expect("indexer");
        let index = indexer.index().await.expect("index");
        let mut index_digest = Sha256::new();
        index_digest.update(index.encode().expect("encode index"));
        let index_digest_bytes = index_digest.finalize();

        let mut node = StubNode::new();
        let mock_index = index.clone();
        node.resolve_route_index_result =
            Arc::new(Mutex::new(move |_key: &TypedKey, _root: &Path| {
                let index_internal = mock_index.clone();
                let index_bytes = index_internal.encode().expect("encode index");
                Ok((
                    Target::PrivateRoute(CryptoKey::new([0u8; 32])),
                    Header::from_index(
                        &index_internal,
                        index_bytes.as_slice(),
                        &[0xde, 0xad, 0xbe, 0xef],
                    ),
                    index_internal,
                ))
            }));
        node.watch_result = Arc::new(Mutex::new(
            move |_key: TypedKey, _values: ValueSubkeyRangeSet| Ok(()),
        ));
        node.cancel_watch_result = Arc::new(Mutex::new(move |_key: &TypedKey| Ok(())));

        let cancel = CancellationToken::new();
        let mut operator = Operator::new(cancel.clone(), ShareResolver::new(node), OneShot);

        let fake_key =
            TypedKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");

        // Send a bad "want index digest"
        let bad_digest: crate::proto::Digest = [0u8; 32];
        operator
            .send(share_resolver::Request::Index {
                key: fake_key,
                want_index_digest: Some(bad_digest),
                root: index.root().to_path_buf(),
            })
            .await
            .expect("send request");
        let bad_index_resp = operator.recv().await;
        assert!(matches!(
            bad_index_resp,
            Some(share_resolver::Response::BadIndex { key: _ })
        ));

        // Send a "want index digest" that matches the mock resolved index
        let digest_array: crate::proto::Digest = index_digest_bytes.into();
        operator
            .send(share_resolver::Request::Index {
                key: fake_key,
                want_index_digest: Some(digest_array),
                root: index.root().to_path_buf(),
            })
            .await
            .expect("send request");
        let good_index_resp = operator.recv().await;
        assert!(matches!(
            good_index_resp,
            Some(share_resolver::Response::Index {
                key: _,
                header: _,
                index: _,
                target: _
            })
        ));

        // Initate a shutdown
        cancel.cancel();

        // Client channel closes
        assert!(matches!(operator.recv().await, None));

        // Service run terminates
        operator.join().await.expect("join").expect("svc run");
    }

    #[tokio::test]
    async fn test_resolve_header() {
        let tf = temp_file(0xa5u8, 65536);
        let indexer = Indexer::from_file(std::env::temp_dir().join(tf.path()).as_path())
            .await
            .expect("indexer");
        let index = indexer.index().await.expect("index");
        let mut index_digest = Sha256::new();
        index_digest.update(index.encode().expect("encode index"));

        let fake_peer_map_key =
            TypedKey::from_str("VLD0:hIfQGdXUq-oO5wwzODJukR7zOGwpNznKYaFoh6uTp2A").expect("key");
        let fake_have_map_key =
            TypedKey::from_str("VLD0:rl3AyyZNFWP8GQGyY9xSnnIjCDagXzbCA47HFmsbLDU").expect("key");

        let mut node = StubNode::new();
        let mock_index = index.clone();
        let mock_peer_map_key = fake_peer_map_key.clone();
        let mock_have_map_key = fake_have_map_key.clone();
        node.resolve_route_result = Arc::new(Mutex::new(
            move |_key: &TypedKey, _prior_route: Option<Target>| {
                let index_internal = mock_index.clone();
                let index_bytes = index_internal.encode().expect("encode index");
                let header = Header::from_index(
                    &index_internal,
                    index_bytes.as_slice(),
                    &[0xde, 0xad, 0xbe, 0xef],
                )
                .with_peer_map(PeerMapRef::new(mock_peer_map_key, 1u16))
                .with_have_map(HaveMapRef::new(mock_have_map_key, 1u16));
                Ok((Target::PrivateRoute(CryptoKey::new([0u8; 32])), header))
            },
        ));
        node.watch_result = Arc::new(Mutex::new(
            move |_key: TypedKey, _values: ValueSubkeyRangeSet| Ok(()),
        ));

        let cancel = CancellationToken::new();
        let mut operator = Operator::new(cancel.clone(), ShareResolver::new(node), OneShot);

        let fake_key =
            TypedKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");

        // Send a "want index digest" that matches the mock resolved index
        operator
            .send(share_resolver::Request::Header {
                key: fake_key,
                prior_target: None,
            })
            .await
            .expect("send request");
        let header_resp = operator.recv().await;
        let (header_resp_key, header_resp_header, header_resp_target) = match header_resp {
            Some(share_resolver::Response::Header {
                key,
                header,
                target,
            }) => (key, header, target),
            _ => panic!("unexpected response"),
        };
        assert_eq!(fake_key, header_resp_key);
        assert_eq!(
            header_resp_header.have_map().map(|m| m.key()),
            Some(&fake_have_map_key)
        );
        assert_eq!(
            header_resp_header.peer_map().map(|m| m.key()),
            Some(&fake_peer_map_key)
        );
        assert_eq!(
            header_resp_target,
            Target::PrivateRoute(CryptoKey::new([0u8; 32])),
        );

        // Initate a shutdown
        cancel.cancel();

        // Client channel closes
        assert!(matches!(operator.recv().await, None));

        // Service run terminates
        operator.join().await.expect("join").expect("svc run");
    }
}
