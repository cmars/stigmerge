use stigmerge_fileindex::Index;
use tokio::select;
use tokio_util::sync::CancellationToken;
use veilid_core::Target;

use crate::{
    chan_rpc::{ChanServer, Service},
    peer::TypedKey,
    proto::Header,
    Error, Peer, Result,
};

pub enum Request {
    Announce,
}

pub enum Response {
    NotAvailable,
    Announce {
        key: TypedKey,
        target: Target,
        header: Header,
    },
}

pub struct ShareAnnouncer<P: Peer> {
    peer: P,
    ch: ChanServer<Request, Response>,
    index: Index,

    share: Option<ShareAnnounce>,
}

struct ShareAnnounce {
    key: TypedKey,
    target: Target,
    header: Header,
}

impl<P: Peer> Service for ShareAnnouncer<P> {
    type Request = Request;

    type Response = Response;

    async fn run(mut self, cancel: CancellationToken) -> crate::Result<()> {
        loop {
            match self.share {
                None => {
                    self.announce().await?;
                }
                Some(ShareAnnounce { .. }) => {
                    select! {
                        _ = cancel.cancelled() => {
                            return Ok(());
                        }
                        res = self.ch.rx.recv() => {
                            if let Some(req) = res {
                                let resp = self.handle(&req).await?;
                                self.ch.tx.send(resp).await.map_err(Error::other)?;
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
                if let Some(ShareAnnounce {
                    key,
                    target,
                    header,
                }) = &mut self.share
                {
                    let (updated_target, updated_header) = self
                        .peer
                        .reannounce_route(key, Some(*target), &self.index, header)
                        .await?;
                    *target = updated_target;
                    *header = updated_header;
                    return Ok(Response::Announce {
                        key: *key,
                        target: *target,
                        header: header.clone(),
                    });
                }
            }
        }
        Ok(Response::NotAvailable)
    }
}

impl<P: Peer> ShareAnnouncer<P> {
    pub fn new(peer: P, ch: ChanServer<Request, Response>, index: Index) -> ShareAnnouncer<P> {
        ShareAnnouncer {
            peer,
            ch,
            index,
            share: None,
        }
    }

    async fn announce(&mut self) -> Result<()> {
        let (key, target, header) = self.peer.announce_index(&self.index).await?;
        self.share = Some(ShareAnnounce {
            key: key.clone(),
            target: target.clone(),
            header: header.clone(),
        });
        self.ch
            .tx
            .send(Response::Announce {
                key,
                target,
                header,
            })
            .await
            .map_err(Error::other)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        str::FromStr,
        sync::{Arc, Mutex},
    };

    use stigmerge_fileindex::Indexer;
    use tokio::spawn;
    use tokio_util::sync::CancellationToken;
    use veilid_core::{CryptoKey, Target, TypedKey};

    use crate::{
        chan_rpc::{pipe, Service},
        proto::{Encoder, Header},
        share_announcer::{self, ShareAnnouncer},
        tests::{temp_file, StubPeer},
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
        let mut peer = StubPeer::new();
        let fake_key =
            TypedKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");
        let fake_target = Target::PrivateRoute(CryptoKey::new([0u8; 32]));

        // Set up the announce_result mock
        let mock_key = fake_key.clone();
        let mock_target = fake_target.clone();
        peer.announce_result = Arc::new(Mutex::new(move |index: &stigmerge_fileindex::Index| {
            let index_bytes = index.encode().expect("encode index");
            let header =
                Header::from_index(index, index_bytes.as_slice(), &[0xde, 0xad, 0xbe, 0xef]);
            Ok((mock_key.clone(), mock_target.clone(), header))
        }));

        // Create the service and channels
        let (mut share_client, share_server) = pipe(32);
        let svc = ShareAnnouncer::new(peer, share_server, index);
        let cancel = CancellationToken::new();
        let svc_task = spawn(svc.run(cancel.clone()));

        // Wait for the initial announce response
        let announce_resp = share_client.rx.recv().await.expect("response");
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
        svc_task.await.expect("join").expect("svc run");
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
        let mut peer = StubPeer::new();
        let fake_key =
            TypedKey::from_str("VLD0:cCHB85pEaV4bvRfywxnd2fRNBScR64UaJC8hoKzyr3M").expect("key");
        let fake_target = Target::PrivateRoute(CryptoKey::new([0u8; 32]));
        let updated_target = Target::PrivateRoute(CryptoKey::new([1u8; 32]));

        // Set up the announce_result mock
        let mock_key = fake_key.clone();
        let mock_target = fake_target.clone();
        peer.announce_result = Arc::new(Mutex::new(move |index: &stigmerge_fileindex::Index| {
            let index_bytes = index.encode().expect("encode index");
            let header =
                Header::from_index(index, index_bytes.as_slice(), &[0xde, 0xad, 0xbe, 0xef]);
            Ok((mock_key.clone(), mock_target.clone(), header))
        }));

        // Set up the reannounce_route_result mock
        let mock_updated_target = updated_target.clone();
        peer.reannounce_route_result = Arc::new(Mutex::new(
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
        let (mut share_client, share_server) = pipe(32);
        let svc = ShareAnnouncer::new(peer, share_server, index);
        let cancel = CancellationToken::new();
        let svc_task = spawn(svc.run(cancel.clone()));

        // Wait for the initial announce response
        let announce_resp = share_client.rx.recv().await.expect("response");
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
        share_client
            .tx
            .send(share_announcer::Request::Announce)
            .await
            .expect("send request");

        // Wait for the reannounce response
        let reannounce_resp = share_client.rx.recv().await.expect("response");
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
        svc_task.await.expect("join").expect("svc run");
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
        let peer = StubPeer::new();

        // Create the service and channels with no share announced yet
        let (_share_client, share_server) = pipe(32);
        let mut announcer = ShareAnnouncer::new(peer, share_server, index);

        // Test the handle method directly with no share set
        let response = announcer
            .handle(&share_announcer::Request::Announce)
            .await
            .expect("handle");
        assert!(matches!(response, share_announcer::Response::NotAvailable));
    }
}
