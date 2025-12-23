use std::{sync::Arc, time::Duration};

use stigmerge_fileindex::Index;
use tokio::{
    select,
    sync::Mutex,
    time::{interval, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument, trace, warn};
use veilid_core::{RouteId, Sequencing, Stability, VeilidRouteChange, VALID_CRYPTO_KINDS};
use veilnet::{
    connection::{RoutingContext, UpdateHandler, API},
    Connection,
};

use crate::{
    content_addressable::ContentAddressable,
    proto::{Encoder, HaveMapRef, Header, PeerMapRef},
    record::{StableHaveMap, StablePeersRecord, StableShareRecord},
    retry::Retry,
    types::LocalShareInfo,
    CancelError, Result,
};

pub struct ShareAnnouncer<C: Connection> {
    cancel: CancellationToken,
    retry: Retry,
    conn: C,
    share_announce: Arc<Mutex<ShareAnnounce>>,
    route_change_rx: flume::Receiver<Vec<RouteId>>,
}

impl<C: Connection + Send + Sync + 'static> ShareAnnouncer<C> {
    pub async fn new(
        cancel: CancellationToken,
        retry: Retry,
        mut conn: C,
        index: Index,
    ) -> Result<Self> {
        let cancel = cancel.child_token();
        let (dead_routes_handler, route_change_rx) = DeadRoutesHandler::new(cancel.clone());
        conn.add_update_handler(Box::new(dead_routes_handler));
        let share_announce = Arc::new(Mutex::new(ShareAnnounce::new(&mut conn, index).await?));
        Ok(Self {
            cancel,
            retry,
            conn,
            share_announce,
            route_change_rx,
        })
    }

    pub async fn share_info(&self) -> LocalShareInfo {
        let share_announce = self.share_announce.lock().await;
        LocalShareInfo {
            key: share_announce.share_record.share_key().clone(),
            header: share_announce.header.clone(),
            want_index_digest: share_announce.index_digest,
            root: share_announce.index.root().to_path_buf(),
            want_index: share_announce.index.clone(),
        }
    }

    const REANNOUNCE_INTERVAL_SECS: u64 = 600;

    pub async fn run(mut self) -> Result<()> {
        let mut reannounce_interval = interval(Duration::from_secs(Self::REANNOUNCE_INTERVAL_SECS));
        reannounce_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        reannounce_interval.reset();
        loop {
            select! {
                _ = self.cancel.cancelled() => {
                    return Err(CancelError.into());
                }
                res = self.route_change_rx.recv_async() => {
                    let mut share_announce = self.share_announce.lock().await;
                    let dead_routes = res?;
                    if dead_routes.contains(&share_announce.route_id) {
                        backoff_retry!(self.cancel, self.retry, {
                            let route_id = share_announce.reannounce(&mut self.conn).await.map_err(backoff::Error::transient)?;
                            debug!(?route_id, "dead route, reannounce");
                        }, {
                            self.conn.reset().await?;
                        })?;
                    }
                }
                _ = reannounce_interval.tick() => {
                    backoff_retry!(self.cancel, self.retry, {
                        let route_id = self.share_announce.lock().await.reannounce(&mut self.conn).await.map_err(backoff::Error::transient)?;
                        info!(?route_id, interval = ?reannounce_interval.period(), "reannounce");
                    }, {
                        self.conn.reset().await?;
                    })?;
                }
            }
        }
    }
}

pub struct ShareAnnounce {
    index: Index,
    index_digest: [u8; 32],
    peers_record: StablePeersRecord,
    share_record: StableShareRecord,
    route_id: RouteId,
    header: Header,
}

impl ShareAnnounce {
    pub async fn new<C: Connection + Send + Sync + 'static>(
        conn: &mut C,
        mut index: Index,
    ) -> Result<ShareAnnounce> {
        let index_digest = index.digest()?;

        // Establish and open peer & share records. These block on Veilid network attachment.
        let mut peers_record = StablePeersRecord::new_local(conn, &index).await?;
        let have_record = StableHaveMap::new_local(conn, &index).await?;
        let mut share_record = StableShareRecord::new_local(conn, &index).await?;

        let (mut header, route_id) = {
            let routing_context = conn.routing_context();
            let route_blob = routing_context
                .api()
                .new_custom_private_route(
                    &VALID_CRYPTO_KINDS,
                    Stability::LowLatency,
                    Sequencing::NoPreference,
                )
                .await?;
            let index_bytes = index.encode()?;
            (
                Header::from_index(&index, index_bytes.as_slice(), route_blob.blob.as_slice()),
                route_blob.route_id,
            )
        };

        header.set_peer_map(PeerMapRef::new(
            peers_record.key().clone(),
            StablePeersRecord::MAX_PEERS,
        ));
        if let Err(err) = peers_record.load_peers(conn).await {
            warn!(?err, peers_key = ?peers_record.key(), "failed to load peers");
        }

        header.set_have_map(HaveMapRef::new(
            have_record.key().clone(),
            have_record.subkeys(),
        ));

        share_record.write_header(conn, &header).await?;
        share_record.write_index(conn, &index).await?;

        Ok(ShareAnnounce {
            index,
            index_digest,
            peers_record,
            share_record,
            route_id,
            header,
        })
    }

    #[instrument(skip_all, err)]
    async fn reannounce<C: Connection + Send + Sync + 'static>(
        &mut self,
        conn: &mut C,
    ) -> Result<RouteId> {
        {
            conn.require_attachment().await?;
        }

        let route_id = {
            let routing_context = conn.routing_context();

            // Attempt to release a prior route
            if let Err(err) = routing_context
                .api()
                .release_private_route(self.route_id.clone())
            {
                debug!(?err, route_id = ?self.route_id, "release prior private route");
            }

            // Establish a new route
            let route_blob = routing_context
                .api()
                .new_custom_private_route(
                    &VALID_CRYPTO_KINDS,
                    Stability::LowLatency,
                    Sequencing::NoPreference,
                )
                .await?;
            // Update header with new route data
            self.header.set_route_data(route_blob.blob);
            self.route_id = route_blob.route_id.clone();
            route_blob.route_id
        };

        self.header.set_peer_map(PeerMapRef::new(
            self.peers_record.key().clone(),
            StablePeersRecord::MAX_PEERS,
        ));

        // Write updated share header
        self.share_record.write_header(conn, &self.header).await?;
        Ok(route_id)
    }
}

struct DeadRoutesHandler {
    cancel: CancellationToken,
    dead_routes_tx: flume::Sender<Vec<RouteId>>,
}

impl DeadRoutesHandler {
    fn new(cancel: CancellationToken) -> (DeadRoutesHandler, flume::Receiver<Vec<RouteId>>) {
        let (route_change_tx, route_change_rx) = flume::unbounded();
        (
            Self {
                cancel,
                dead_routes_tx: route_change_tx,
            },
            route_change_rx,
        )
    }
}

impl UpdateHandler for DeadRoutesHandler {
    fn route_change(&self, change: &VeilidRouteChange) {
        if !change.dead_routes.is_empty() {
            if let Err(err) = self.dead_routes_tx.send(change.dead_routes.to_owned()) {
                warn!(?err, "failed to send route change");
                self.cancel.cancel();
            }
        }
    }
    fn shutdown(&self) {
        trace!("shutdown");
        self.cancel.cancel();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use stigmerge_fileindex::{FileSpec, Indexer, PayloadPiece};
    use tempfile::TempDir;
    use veilid_core::{BareRouteId, RecordKey, RouteBlob, RouteId, CRYPTO_KIND_VLD0};
    use veilnet::connection::testing::{
        create_test_api, StubAPI, StubConnection, StubRoutingContext,
    };

    use crate::{
        proto::{Decoder, Header},
        share_announcer::ShareAnnounce,
        tests::temp_file,
    };

    #[tokio::test]
    async fn test_announce() {
        // Create a test file and index
        let tf = temp_file(0xa5u8, 65536);
        let indexer = Indexer::from_file(std::env::temp_dir().join(tf.path()).as_path())
            .await
            .expect("indexer");
        let index = indexer.index().await.expect("index");

        // Create a connection with mock behavior
        let temp_dir = TempDir::new().unwrap();
        let veilid_api = create_test_api(temp_dir.path()).await.unwrap();
        let mut api = StubAPI::new(veilid_api);
        api.new_custom_private_route = Arc::new(tokio::sync::Mutex::new(|_, _, _| {
            Ok(RouteBlob {
                route_id: RouteId::new(CRYPTO_KIND_VLD0, BareRouteId::new(&[0xa5u8; 32])),
                blob: b"route data".to_vec(),
            })
        }));

        let mut routing_context = StubRoutingContext::new(api);
        let set_dht_key_calls = Arc::new(std::sync::Mutex::new(vec![]));
        {
            let set_dht_key_calls = set_dht_key_calls.clone();
            routing_context.set_dht_value = Arc::new(tokio::sync::Mutex::new(
                move |key: RecordKey, subkey, data, _| {
                    set_dht_key_calls.lock().unwrap().push((key, subkey, data));
                    Ok(None)
                },
            ));
        }
        // Stub get_dht_value to return None (no existing peers)
        routing_context.get_dht_value = Arc::new(tokio::sync::Mutex::new(
            move |_key: RecordKey, _subkey, _force_refresh| Ok(None),
        ));

        let mut conn = StubConnection::new(routing_context);
        conn.require_attachment = Arc::new(tokio::sync::Mutex::new(|| Ok(())));

        let fake_route_id = RouteId::new(CRYPTO_KIND_VLD0, BareRouteId::new(&[0xa5u8; 32]));

        let announce = ShareAnnounce::new(&mut conn, index.clone())
            .await
            .expect("announce");

        let set_dht_key_calls = set_dht_key_calls.lock().unwrap();
        assert_eq!(set_dht_key_calls.len(), 2);

        assert_eq!(&set_dht_key_calls[0].0, announce.share_record.share_key());
        let announced_header = Header::decode(set_dht_key_calls[0].2.as_slice()).unwrap();
        assert_eq!(announced_header.route_data(), b"route data".as_slice());

        let (payload_pieces, file_specs) =
            <(Vec<PayloadPiece>, Vec<FileSpec>)>::decode(set_dht_key_calls[1].2.as_slice())
                .unwrap();
        assert_eq!(payload_pieces.len(), 1);
        assert_eq!(file_specs.len(), 1);
        assert_eq!(index.payload().pieces(), &payload_pieces);

        assert_eq!(announce.route_id, fake_route_id);
    }

    #[tokio::test]
    async fn test_reannounce() {
        // Create a test file and index
        let tf = temp_file(0xa5u8, 65536);
        let indexer = Indexer::from_file(std::env::temp_dir().join(tf.path()).as_path())
            .await
            .expect("indexer");
        let index = indexer.index().await.expect("index");

        // Create a connection with mock behavior
        let temp_dir = TempDir::new().unwrap();
        let veilid_api = create_test_api(temp_dir.path()).await.unwrap();
        let mut api = StubAPI::new(veilid_api);

        // Mock route creation - first for initial announce, then for reannounce
        let route_counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let route_counter_clone = route_counter.clone();
        api.new_custom_private_route = Arc::new(tokio::sync::Mutex::new(move |_, _, _| {
            let count = route_counter_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let route_id = if count == 0 {
                RouteId::new(CRYPTO_KIND_VLD0, BareRouteId::new(&[0xa5u8; 32])) // Initial route
            } else {
                RouteId::new(CRYPTO_KIND_VLD0, BareRouteId::new(&[0x5au8; 32])) // Reannounce route
            };
            let route_data = if count == 0 {
                b"initial route data".to_vec()
            } else {
                b"updated route data".to_vec()
            };
            Ok(RouteBlob {
                route_id,
                blob: route_data,
            })
        }));

        let release_private_route_calls = Arc::new(std::sync::Mutex::new(vec![]));
        {
            let release_private_route_calls = release_private_route_calls.clone();
            api.release_private_route = Arc::new(std::sync::Mutex::new(move |route_id| {
                release_private_route_calls.lock().unwrap().push(route_id);
                Ok(())
            }));
        }

        let mut routing_context = StubRoutingContext::new(api);
        let set_dht_key_calls = Arc::new(std::sync::Mutex::new(vec![]));
        {
            let set_dht_key_calls = set_dht_key_calls.clone();
            routing_context.set_dht_value = Arc::new(tokio::sync::Mutex::new(
                move |key: RecordKey, subkey, data, _| {
                    set_dht_key_calls.lock().unwrap().push((key, subkey, data));
                    Ok(None)
                },
            ));
        }
        // Stub get_dht_value to return None (no existing peers)
        routing_context.get_dht_value = Arc::new(tokio::sync::Mutex::new(
            move |_key: RecordKey, _subkey, _force_refresh| Ok(None),
        ));

        let mut conn = StubConnection::new(routing_context);
        conn.require_attachment = Arc::new(tokio::sync::Mutex::new(|| Ok(())));

        // Create initial announce
        let mut announce = ShareAnnounce::new(&mut conn, index.clone())
            .await
            .expect("announce");

        // Clear the initial calls to focus on reannounce
        set_dht_key_calls.lock().unwrap().clear();
        release_private_route_calls.lock().unwrap().clear();

        // Call reannounce
        let new_route_id = announce.reannounce(&mut conn).await.expect("reannounce");

        // Verify the old route was released
        let release_calls = release_private_route_calls.lock().unwrap();
        assert_eq!(release_calls.len(), 1);
        assert_eq!(
            release_calls[0],
            RouteId::new(CRYPTO_KIND_VLD0, BareRouteId::new(&[0xa5u8; 32]))
        ); // Initial route

        // Verify the new route ID
        assert_eq!(
            new_route_id,
            RouteId::new(CRYPTO_KIND_VLD0, BareRouteId::new(&[0x5au8; 32]))
        ); // Updated route

        // Verify the header was updated with new route data
        let set_dht_calls = set_dht_key_calls.lock().unwrap();
        assert_eq!(set_dht_calls.len(), 1); // Only header should be written during reannounce

        assert_eq!(&set_dht_calls[0].0, announce.share_record.share_key());
        let updated_header = Header::decode(set_dht_calls[0].2.as_slice()).unwrap();
        assert_eq!(
            updated_header.route_data(),
            b"updated route data".as_slice()
        );
    }
}
