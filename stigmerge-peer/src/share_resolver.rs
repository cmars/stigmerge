use std::{collections::HashSet, fmt, path::PathBuf};

use anyhow::Context;
use stigmerge_fileindex::Index;
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{info, trace, warn};
use veilid_core::{Target, ValueSubkeyRangeSet, VeilidUpdate};

use crate::{
    actor::{Actor, Respondable, ResponseChannel},
    content_addressable::ContentAddressable,
    error::Unrecoverable,
    node::TypedKey,
    proto::{Digest, Header},
    Node, Result,
};

/// The share_resolver service maintains private routes to the route posted at remote
/// peers' share keys. It also validates that the remote peer is sharing
/// the expected index by verifying its content digest.
pub struct ShareResolver<N: Node> {
    node: N,
    share_target_tx: flume::Sender<(TypedKey, Target)>,
    share_target_rx: flume::Receiver<(TypedKey, Target)>,
    watching: HashSet<TypedKey>,
}

impl<P: Node> ShareResolver<P> {
    /// Create a new share_resolver service.
    pub fn new(node: P) -> Self {
        let (share_target_tx, share_target_rx) = flume::unbounded();
        Self {
            node,
            share_target_tx,
            share_target_rx,
            watching: HashSet::new(),
        }
    }

    pub fn subscribe_target(&self) -> flume::Receiver<(TypedKey, Target)> {
        self.share_target_rx.clone()
    }
}

/// Share resolver request messages.
pub enum Request {
    /// Resolve the Index located at a remote share key, with a known index
    /// digest, for merging into a local file share.
    ///
    /// If the remote share is valid, the share resolver will set up a
    /// continual, automatically-renewed watch for this share key.
    Index {
        response_tx: ResponseChannel<Response>,
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
        response_tx: ResponseChannel<Response>,
        key: TypedKey,
        prior_target: Option<Target>,
    },

    /// Stop watching this share key.
    Remove {
        response_tx: ResponseChannel<Response>,
        key: TypedKey,
    },
}

impl fmt::Debug for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Index {
                response_tx: _,
                key,
                want_index_digest,
                root,
            } => f
                .debug_struct("Index")
                .field("key", key)
                .field("want_index_digest", want_index_digest)
                .field("root", root)
                .finish(),
            Self::Header {
                response_tx: _,
                key,
                prior_target,
            } => f
                .debug_struct("Header")
                .field("key", key)
                .field("prior_target", prior_target)
                .finish(),
            Self::Remove {
                response_tx: _,
                key,
            } => f.debug_struct("Remove").field("key", key).finish(),
        }
    }
}

impl Respondable for Request {
    type Response = Response;

    fn set_response(&mut self, ch: ResponseChannel<Self::Response>) {
        match self {
            Request::Index { response_tx, .. } => *response_tx = ch,
            Request::Header { response_tx, .. } => *response_tx = ch,
            Request::Remove { response_tx, .. } => *response_tx = ch,
        }
    }

    fn response_tx(self) -> ResponseChannel<Self::Response> {
        match self {
            Request::Index { response_tx, .. } => response_tx,
            Request::Header { response_tx, .. } => response_tx,
            Request::Remove { response_tx, .. } => response_tx,
        }
    }
}

impl Request {
    /// Get the share key specified in the request.
    fn key(&self) -> &TypedKey {
        match self {
            Request::Index {
                key,
                want_index_digest: _,
                root: _,
                response_tx: _,
            } => key,
            Request::Header {
                key,
                prior_target: _,
                response_tx: _,
            } => key,
            Request::Remove {
                key,
                response_tx: _,
            } => key,
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

    pub fn target(&self) -> Option<&Target> {
        match self {
            Response::NotAvailable { .. } => None,
            Response::BadIndex { .. } => None,
            Response::Index { target, .. } => Some(target),
            Response::Header { target, .. } => Some(target),
            Response::Remove { .. } => None,
        }
    }
}

impl<P: Node> Actor for ShareResolver<P> {
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
                    return Ok(())
                }
                res = request_rx.recv_async() => {
                    let req = res.with_context(|| format!("share_resolver: receive request"))?;
                    self.handle_request(req).await?;
                }
                res = update_rx.recv() => {
                    let update = res.with_context(|| format!("share_resolver: receive veilid update"))?;
                    match update {
                        VeilidUpdate::ValueChange(ch) => {
                            if !self.watching.contains(&ch.key) {
                                continue;
                            }
                            // FIXME: this may leak private routes.
                            // TODO: keep track of prior routes and pass them here?
                            if let Err(e) = self.handle_request(Request::Header {
                                response_tx: ResponseChannel::default(),
                                key: ch.key,
                                prior_target: None
                            }).await {
                                warn!("Failed to handle value change: {}", e);
                            }
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
    async fn handle_request(&mut self, req: Self::Request) -> Result<()> {
        let req_key = req.key().clone();
        let (resp, mut response_tx) = match req {
            Request::Index {
                key,
                want_index_digest,
                root,
                response_tx,
            } => {
                let (target, header, mut index) =
                    self.node.resolve_route_index(&key, root.as_path()).await?;
                let peer_index_digest = index.digest()?;

                // If want_index_digest is None, skip verification
                // Otherwise, verify the digest matches
                let digest_matches = match want_index_digest {
                    None => true, // Skip verification if None
                    Some(digest) => peer_index_digest.as_slice() == digest,
                };

                if digest_matches {
                    self.share_target_tx
                        .send_async((key.to_owned(), target.to_owned()))
                        .await?;
                    (
                        Response::Index {
                            key: key.clone(),
                            header,
                            index,
                            target,
                        },
                        response_tx,
                    )
                } else {
                    (Response::BadIndex { key: key.clone() }, response_tx)
                }
            }
            Request::Header {
                key,
                prior_target,
                response_tx,
            } => {
                let (target, header) = self.node.resolve_route(&key, prior_target).await?;
                self.share_target_tx
                    .send_async((key.to_owned(), target.to_owned()))
                    .await?;
                (
                    Response::Header {
                        key: key.clone(),
                        header,
                        target,
                    },
                    response_tx,
                )
            }
            Request::Remove { key, response_tx } => (
                Response::Remove {
                    // No need to do anything; this response key is not valid, causing an unwatch
                    key: key.clone(),
                },
                response_tx,
            ),
        };

        if let Some(valid_key) = resp.valid_key() {
            // Valid usable shares are watched.
            info!("watch: share key {valid_key}");
            self.watching.insert(*valid_key);
            self.node
                .watch(*valid_key, ValueSubkeyRangeSet::single(0))
                .await?;
        } else {
            // Invalid or unusable shares are unwatched.
            self.watching.remove(&req_key);
            self.node.cancel_watch(&req_key).await?;
        }
        trace!(?resp);

        response_tx
            .send(resp)
            .await
            .context(Unrecoverable::new("send response from share resolver"))?;
        Ok(())
    }
}

impl<P: Node + Clone> Clone for ShareResolver<P> {
    fn clone(&self) -> Self {
        Self {
            node: self.node.clone(),
            share_target_tx: self.share_target_tx.clone(),
            share_target_rx: self.share_target_rx.clone(),
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

    use crate::actor::{OneShot, Operator, ResponseChannel};
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
        let bad_index_resp = operator
            .call(share_resolver::Request::Index {
                response_tx: ResponseChannel::default(),
                key: fake_key,
                want_index_digest: Some(bad_digest),
                root: index.root().to_path_buf(),
            })
            .await
            .expect("call request");
        assert!(matches!(
            bad_index_resp,
            share_resolver::Response::BadIndex { key: _ }
        ));

        // Send a "want index digest" that matches the mock resolved index
        let digest_array: crate::proto::Digest = index_digest_bytes.into();
        let good_index_resp = operator
            .call(share_resolver::Request::Index {
                response_tx: ResponseChannel::default(),
                key: fake_key,
                want_index_digest: Some(digest_array),
                root: index.root().to_path_buf(),
            })
            .await
            .expect("call request");
        assert!(matches!(
            good_index_resp,
            share_resolver::Response::Index {
                key: _,
                header: _,
                index: _,
                target: _
            }
        ));

        // Initate a shutdown
        cancel.cancel();

        // Service run terminates
        operator.join().await.expect("join");
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
        let header_resp = operator
            .call(share_resolver::Request::Header {
                response_tx: ResponseChannel::default(),
                key: fake_key,
                prior_target: None,
            })
            .await
            .expect("call request");

        match header_resp {
            share_resolver::Response::Header {
                key,
                header,
                target,
            } => {
                assert_eq!(key, fake_key);
                assert_eq!(header.have_map().map(|m| m.key()), Some(&fake_have_map_key));
                assert_eq!(header.peer_map().map(|m| m.key()), Some(&fake_peer_map_key));
                assert_eq!(target, Target::PrivateRoute(CryptoKey::new([0u8; 32])),);
            }
            _ => panic!("unexpected response"),
        }

        // Initate a shutdown
        cancel.cancel();

        // Service run terminates
        operator.join().await.expect("join");
    }
}
