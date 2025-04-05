use std::path::PathBuf;

use sha2::{Digest as _, Sha256};
use stigmerge_fileindex::Index;
use tokio::select;
use tokio_util::sync::CancellationToken;
use veilid_core::{Target, TimestampDuration, ValueSubkeyRangeSet};

use crate::{
    chan_rpc::ChanServer,
    peer::TypedKey,
    proto::{Digest, Encoder, Header},
    Error, Peer, Result,
};

/// Share resolver request messages.
pub(super) enum Request {
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
pub(super) enum Response {
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
pub(super) struct Service<P: Peer> {
    peer: P,
    ch: ChanServer<Request, Response>,
}

impl<P: Peer> Service<P> {
    /// Create a new share_resolver service.
    pub(super) fn new(peer: P, ch: ChanServer<Request, Response>) -> Self {
        Self { peer, ch }
    }

    /// Run the service until cancelled.
    pub async fn run(mut self, cancel: CancellationToken) -> Result<()> {
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
                                /// Valid usable shares are watched.
                                self.peer.watch(valid_key.clone(), ValueSubkeyRangeSet::single(0), TimestampDuration::new(60_000_000)).await?;
                            } else {
                                /// Invalid or unusable shares are unwatched.
                                self.peer.cancel_watch(req.key());
                            }
                            self.ch.tx.send(resp).await.map_err(Error::other)?
                        }
                        Err(err) => self.ch.tx.send(Response::NotAvailable{key: req.key().clone(), err}).await.map_err(Error::other)?,
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

#[cfg(test)]
mod tests {
    use std::{
        str::FromStr,
        sync::{Arc, Mutex},
    };

    use sha2::{Digest as _, Sha256};
    use stigmerge_fileindex::Indexer;
    use tokio::spawn;
    use tokio_util::sync::CancellationToken;
    use veilid_core::{CryptoKey, Target, TypedKey};

    use crate::{
        proto::{Encoder, HaveMapRef, Header, PeerMapRef},
        tests::{temp_file, StubPeer},
        tracker::{
            pipe,
            share_resolver::{Request, Response, Service},
        },
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
        peer.resolve_result = Arc::new(Mutex::new(move || {
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
        peer.watch_result = Arc::new(Mutex::new(move || Ok(())));
        peer.cancel_watch_result = Arc::new(Mutex::new(move || {}));

        let (mut share_client, share_server) = pipe(32);
        let svc = Service::new(peer, share_server);
        let cancel = CancellationToken::new();
        let svc_task = spawn(svc.run(cancel.clone()));

        let fake_key =
            TypedKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");

        // Send a bad "want index digest"
        share_client
            .tx
            .send(Request::Index {
                key: fake_key,
                want_index_digest: [0u8; 32],
                root: index.root().to_path_buf(),
            })
            .await
            .expect("send request");
        let bad_index_resp = share_client.rx.recv().await;
        assert!(matches!(
            bad_index_resp,
            Some(Response::BadIndex { key: _ })
        ));

        // Send a "want index digest" that matches the mock resolved index
        share_client
            .tx
            .send(Request::Index {
                key: fake_key,
                want_index_digest: index_digest_bytes.into(),
                root: index.root().to_path_buf(),
            })
            .await
            .expect("send request");
        let good_index_resp = share_client.rx.recv().await;
        assert!(matches!(
            good_index_resp,
            Some(Response::Index {
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
        let index_digest_bytes = index_digest.finalize();

        let fake_peer_map_key =
            TypedKey::from_str("VLD0:hIfQGdXUq-oO5wwzODJukR7zOGwpNznKYaFoh6uTp2A").expect("key");
        let fake_have_map_key =
            TypedKey::from_str("VLD0:rl3AyyZNFWP8GQGyY9xSnnIjCDagXzbCA47HFmsbLDU").expect("key");

        let mut peer = StubPeer::new();
        let mock_index = index.clone();
        let mock_peer_map_key = fake_peer_map_key.clone();
        let mock_have_map_key = fake_have_map_key.clone();
        peer.reresolve_route_result = Arc::new(Mutex::new(move || {
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
        }));
        peer.watch_result = Arc::new(Mutex::new(move || Ok(())));

        let (mut share_client, share_server) = pipe(32);
        let svc = Service::new(peer, share_server);
        let cancel = CancellationToken::new();
        let svc_task = spawn(svc.run(cancel.clone()));

        let fake_key =
            TypedKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");

        // Send a "want index digest" that matches the mock resolved index
        share_client
            .tx
            .send(Request::Header {
                key: fake_key,
                prior_target: None,
            })
            .await
            .expect("send request");
        let header_resp = share_client.rx.recv().await;
        let (header_resp_key, header_resp_header, header_resp_target) = match header_resp {
            Some(Response::Header {
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
