use std::path::PathBuf;

use sha2::{Digest as _, Sha256};
use stigmerge_fileindex::Index;
use tokio::{select, sync::broadcast};
use tokio_util::sync::CancellationToken;
use tracing::warn;
use veilid_core::{Target, TimestampDuration, ValueSubkeyRangeSet};

use crate::{
    chan_rpc::{ChanServer, Service},
    peer::TypedKey,
    proto::{Digest, Encoder, Header},
    Error, Peer, Result,
};

/// Share resolver request messages.
pub enum Request {
    /// Resolve the Index located at a remote share key, with a known index
    /// digest, for merging into a local file share.
    ///
    /// If the remote share is valid, the share resolver will set up a
    /// continual, automatically-renewed watch for this share key.
    Index {
        key: TypedKey,
        want_index_digest: Digest,
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
pub enum Response {
    /// Remote share is not available at the given key, with error cause.
    NotAvailable { key: TypedKey, err: Error },

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
            Response::NotAvailable { key: _, err: _ } => None,
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

/// The share_resolver service maintains private routes to the route posted at remote
/// peers' share keys. It also validates that the remote peer is sharing
/// the expected index by verifying its content digest.
pub struct ShareResolver<P: Peer> {
    peer: P,
    ch: ChanServer<Request, Response>,
    target_tx: broadcast::Sender<Target>,
}

impl<P: Peer> Service for ShareResolver<P> {
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
                            if let Some(valid_key) = resp.valid_key() {
                                // Valid usable shares are watched.
                                self.peer.watch(valid_key.clone(), ValueSubkeyRangeSet::single(0), TimestampDuration::new(60_000_000)).await?;
                            } else {
                                // Invalid or unusable shares are unwatched.
                                self.peer.cancel_watch(req.key());
                            }
                            self.ch.tx.send(resp).await.map_err(Error::other)?
                        }
                        Err(e) => {
                            self.ch.tx.send(Response::NotAvailable { key: req.key().to_owned(), err: e }).await.map_err(Error::other)?
                        }
                    }
                }
                res = updates.recv() => {
                    let update = res.map_err(Error::other)?;
                    match update {
                        veilid_core::VeilidUpdate::ValueChange(ch) => {
                            // FIXME: this may leak private routes.
                            // TODO: keep track of prior routes and pass them here?
                            let resp = self.handle(&Request::Header{ key: ch.key, prior_target: None }).await?;
                            self.ch.tx.send(resp).await.map_err(Error::other)?;
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

    /// Handle a share_resolver request, provide a response.
    async fn handle(&mut self, req: &Request) -> Result<Response> {
        Ok(match req {
            Request::Index {
                key,
                want_index_digest,
                root,
            } => {
                let (target, header, index) = self.peer.resolve(key, root.as_path()).await?;
                let mut peer_index_digest = Sha256::new();
                peer_index_digest.update(index.encode().map_err(Error::other)?);
                if peer_index_digest.finalize().as_slice() == want_index_digest {
                    self.target_tx.send(target.to_owned()).unwrap_or_else(|e| {
                        warn!("no target subscribers: {}", e);
                        0
                    });
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
                let (target, header) = self.peer.reresolve_route(key, *prior_target).await?;
                self.target_tx.send(target.to_owned()).unwrap_or_else(|e| {
                    warn!("no target subscribers: {}", e);
                    0
                });
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

const TARGET_BROADCAST_CAPACITY: usize = 16;

impl<P: Peer> ShareResolver<P> {
    /// Create a new share_resolver service.
    pub fn new(peer: P, ch: ChanServer<Request, Response>) -> Self {
        Self {
            peer,
            ch,
            target_tx: broadcast::channel(TARGET_BROADCAST_CAPACITY).0,
        }
    }

    pub fn subscribe_target(&self) -> broadcast::Receiver<Target> {
        self.target_tx.subscribe()
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
    use tokio::spawn;
    use tokio_util::sync::CancellationToken;
    use veilid_core::{CryptoKey, Target, TimestampDuration, TypedKey, ValueSubkeyRangeSet};

    use crate::chan_rpc::pipe;
    use crate::{
        chan_rpc::Service,
        proto::{Encoder, HaveMapRef, Header, PeerMapRef},
        share_resolver::{self, ShareResolver},
        tests::{temp_file, StubPeer},
    };

    #[tokio::test]
    async fn test_resolve_index() {
        let tf = temp_file(0xa5u8, 65536);
        let indexer = Indexer::from_file(std::env::temp_dir().join(tf.path()).into())
            .await
            .expect("indexer");
        let index = indexer.index().await.expect("index");
        let mut index_digest = Sha256::new();
        index_digest.update(index.encode().expect("encode index"));
        let index_digest_bytes = index_digest.finalize();

        let mut peer = StubPeer::new();
        let mock_index = index.clone();
        peer.resolve_result = Arc::new(Mutex::new(move |_key: &TypedKey, _root: &Path| {
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
        peer.watch_result = Arc::new(Mutex::new(
            move |_key: TypedKey, _values: ValueSubkeyRangeSet, _period: TimestampDuration| Ok(()),
        ));
        peer.cancel_watch_result = Arc::new(Mutex::new(move |_key: &TypedKey| {}));

        let (mut share_client, share_server) = pipe(32);
        let svc = ShareResolver::new(peer, share_server);
        let cancel = CancellationToken::new();
        let svc_task = spawn(svc.run(cancel.clone()));

        let fake_key =
            TypedKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");

        // Send a bad "want index digest"
        share_client
            .tx
            .send(share_resolver::Request::Index {
                key: fake_key,
                want_index_digest: [0u8; 32],
                root: index.root().to_path_buf(),
            })
            .await
            .expect("send request");
        let bad_index_resp = share_client.rx.recv().await;
        assert!(matches!(
            bad_index_resp,
            Some(share_resolver::Response::BadIndex { key: _ })
        ));

        // Send a "want index digest" that matches the mock resolved index
        share_client
            .tx
            .send(share_resolver::Request::Index {
                key: fake_key,
                want_index_digest: index_digest_bytes.into(),
                root: index.root().to_path_buf(),
            })
            .await
            .expect("send request");
        let good_index_resp = share_client.rx.recv().await;
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
        assert!(matches!(share_client.rx.recv().await, None));

        // Service run terminates
        svc_task.await.expect("join").expect("svc run");
    }

    #[tokio::test]
    async fn test_resolve_header() {
        let tf = temp_file(0xa5u8, 65536);
        let indexer = Indexer::from_file(std::env::temp_dir().join(tf.path()).into())
            .await
            .expect("indexer");
        let index = indexer.index().await.expect("index");
        let mut index_digest = Sha256::new();
        index_digest.update(index.encode().expect("encode index"));

        let fake_peer_map_key =
            TypedKey::from_str("VLD0:hIfQGdXUq-oO5wwzODJukR7zOGwpNznKYaFoh6uTp2A").expect("key");
        let fake_have_map_key =
            TypedKey::from_str("VLD0:rl3AyyZNFWP8GQGyY9xSnnIjCDagXzbCA47HFmsbLDU").expect("key");

        let mut peer = StubPeer::new();
        let mock_index = index.clone();
        let mock_peer_map_key = fake_peer_map_key.clone();
        let mock_have_map_key = fake_have_map_key.clone();
        peer.reresolve_route_result = Arc::new(Mutex::new(
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
        peer.watch_result = Arc::new(Mutex::new(
            move |_key: TypedKey, _values: ValueSubkeyRangeSet, _period: TimestampDuration| Ok(()),
        ));

        let (mut share_client, share_server) = pipe(32);
        let svc = ShareResolver::new(peer, share_server);
        let cancel = CancellationToken::new();
        let svc_task = spawn(svc.run(cancel.clone()));

        let fake_key =
            TypedKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");

        // Send a "want index digest" that matches the mock resolved index
        share_client
            .tx
            .send(share_resolver::Request::Header {
                key: fake_key,
                prior_target: None,
            })
            .await
            .expect("send request");
        let header_resp = share_client.rx.recv().await;
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
        assert!(matches!(share_client.rx.recv().await, None));

        // Service run terminates
        svc_task.await.expect("join").expect("svc run");
    }
}
