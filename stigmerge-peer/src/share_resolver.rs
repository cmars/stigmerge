use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use tokio::{select, spawn, sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument, trace, warn};
use veilid_core::{RecordKey, RouteId, ValueSubkeyRangeSet, VeilidRouteChange};
use veilnet::{
    connection::{RoutingContext, UpdateHandler, API},
    Connection,
};

use crate::{
    content_addressable::ContentAddressable, error::CancelError, record::StableShareRecord,
    types::RemoteShareInfo, Error, Result, Retry,
};

#[derive(Clone)]
pub struct ShareResolver<C: Connection> {
    inner: Arc<Mutex<ShareResolverInner<C>>>,
}

struct TaskArgs<C: Connection> {
    cancel: CancellationToken,
    retry: Retry,
    dead_routes_rx: flume::Receiver<Vec<RouteId>>,
    inner: Arc<Mutex<ShareResolverInner<C>>>,
}

impl<C: Connection + Send + Sync + 'static> ShareResolver<C> {
    pub fn new_task(
        cancel: CancellationToken,
        retry: Retry,
        conn: C,
        root: &Path,
    ) -> (Self, JoinHandle<Result<()>>) {
        let (dead_routes_handler, dead_routes_rx) = DeadRoutesHandler::new(cancel.child_token());
        conn.add_update_handler(Box::new(dead_routes_handler));
        let inner = Arc::new(Mutex::new(ShareResolverInner::new(conn, root)));

        let task = spawn(Self::run(TaskArgs {
            cancel: cancel.child_token(),
            retry,
            dead_routes_rx,
            inner: inner.clone(),
        }));

        (Self { inner }, task)
    }

    pub async fn subscribe(&self, handler: Box<dyn ShareHandler + Send + Sync>) {
        self.inner.lock().await.add_share_handler(handler).await
    }

    pub async fn add_share(&self, key: &RecordKey) -> Result<RemoteShareInfo> {
        let share_info = self.inner.lock().await.resolve_and_watch(key).await?;
        Ok(share_info)
    }

    pub async fn refresh_share(&self, route_id: &RouteId) -> Result<()> {
        let mut inner = self.inner.lock().await;
        let share = {
            inner
                .remote_shares
                .get(route_id)
                .ok_or(Error::msg("route_id not found"))?
                .clone()
        };
        inner.refresh(share).await?;
        Ok(())
    }

    pub async fn remove_share(&self, key: &RecordKey) -> Result<()> {
        self.inner.lock().await.remove(key).await?;
        Ok(())
    }

    async fn run(args: TaskArgs<C>) -> Result<()> {
        loop {
            select! {
                _ = args.cancel.cancelled() => {
                    return Err(CancelError.into());
                }
                res = args.dead_routes_rx.recv_async() => {
                    let dead_routes = res?;
                    for dead_route in dead_routes.iter() {
                        let share = {
                            args.inner.lock().await.remote_shares.get(dead_route).cloned()
                        };
                        if let Some(share) = share {
                            backoff_retry!(args.cancel, args.retry, {
                                args.inner.lock().await.refresh(share.clone()).await?;
                            })?;
                        }
                    }
                }
            }
        }
    }
}

struct ShareResolverInner<C: Connection> {
    conn: C,
    root: PathBuf,
    remote_shares: HashMap<RouteId, RemoteShareInfo>,
    handlers: HandlerChain,
}

impl<C: Connection + Send + Sync + 'static> ShareResolverInner<C> {
    fn new(conn: C, root: &Path) -> Self {
        Self {
            conn,
            root: root.to_path_buf(),
            remote_shares: HashMap::new(),
            handlers: HandlerChain::default(),
        }
    }

    async fn add_share_handler(&mut self, handler: Box<dyn ShareHandler + Send + Sync>) {
        self.handlers.add(handler);
    }

    #[instrument(skip(self), err)]
    async fn resolve_and_watch(&mut self, key: &RecordKey) -> Result<RemoteShareInfo> {
        {
            self.conn.require_attachment().await?;
        }
        // Ensure record is open before reading
        let (_, header) = StableShareRecord::new_remote(&mut self.conn, key).await?;
        debug!(
            ?key,
            want_index_digest = hex::encode(header.payload_digest()),
            "read header"
        );
        let mut index =
            StableShareRecord::read_index(&mut self.conn, key, &header, self.root.as_path())
                .await?;
        let index_digest = index.digest()?;
        debug!(?key, index_digest = hex::encode(index_digest), "read index");

        let routing_context = self.conn.routing_context();
        let route_id = routing_context
            .api()
            .import_remote_private_route(header.route_data().to_vec())?;
        debug!(?key, ?route_id, "private route to remote");

        routing_context
            .watch_dht_values(
                key.clone(),
                Some(ValueSubkeyRangeSet::single(0)),
                None,
                None,
            )
            .await?;

        let share = RemoteShareInfo {
            key: key.clone(),
            header: header.clone(),
            index,
            index_digest,
            route_id,
        };
        self.remote_shares
            .insert(share.route_id.clone(), share.clone());
        self.handlers.share_changed(&share)?;
        Ok(share)
    }

    async fn refresh(&mut self, mut share: RemoteShareInfo) -> Result<()> {
        {
            self.conn.require_attachment().await?;
        }
        let header = StableShareRecord::read_header(&mut self.conn, &share.key).await?;
        if header.route_data() == share.header.route_data() {
            return Ok(());
        }

        let routing_context = self.conn.routing_context();
        let route_id = routing_context
            .api()
            .import_remote_private_route(header.route_data().to_vec())?;
        let prior_route_id = share.route_id;
        share.route_id = route_id;
        if let Err(err) = routing_context.api().release_private_route(prior_route_id) {
            warn!(?err, route_id = ?share.route_id, "release prior route");
        }

        self.remote_shares
            .insert(share.route_id.clone(), share.clone());
        self.handlers.share_changed(&share)?;
        Ok(())
    }

    async fn remove(&mut self, key: &RecordKey) -> Result<()> {
        let routing_context = self.conn.routing_context();
        if let Err(err) = routing_context
            .cancel_dht_watch(key.clone(), Some(ValueSubkeyRangeSet::single(0)))
            .await
        {
            warn!(?err, ?key, "cancel share watch");
        }

        let share = { self.remote_shares.values().find(|s| &s.key == key).cloned() };
        if let Some(share) = share {
            self.remote_shares.remove(&share.route_id);
        }
        Ok(())
    }
}

pub trait ShareHandler {
    fn share_changed(&self, share: &RemoteShareInfo) -> Result<()>;
}

pub struct ShareNotifier {
    remote_share_tx: flume::Sender<RemoteShareInfo>,
    label: String,
}

impl ShareNotifier {
    pub fn new(label: &str) -> (Self, flume::Receiver<RemoteShareInfo>) {
        let (remote_share_tx, remote_share_rx) = flume::unbounded();
        (
            Self {
                remote_share_tx,
                label: label.to_string(),
            },
            remote_share_rx,
        )
    }
}

impl ShareHandler for ShareNotifier {
    #[instrument(skip_all, fields(label = self.label), err)]
    fn share_changed(&self, share: &RemoteShareInfo) -> Result<()> {
        self.remote_share_tx.send(share.clone())?;
        Ok(())
    }
}

#[derive(Default)]
struct HandlerChain {
    handlers: Vec<Box<dyn ShareHandler + Send + Sync>>,
}

impl HandlerChain {
    fn add(&mut self, handler: Box<dyn ShareHandler + Send + Sync>) {
        self.handlers.push(handler);
    }
}

impl ShareHandler for HandlerChain {
    #[instrument(skip_all, fields(key = ?share.key))]
    fn share_changed(&self, share: &RemoteShareInfo) -> Result<()> {
        for handler in self.handlers.iter() {
            if let Err(err) = handler.share_changed(share) {
                warn!(?err);
            }
        }
        Ok(())
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
        if !change.dead_remote_routes.is_empty() {
            if let Err(err) = self
                .dead_routes_tx
                .send(change.dead_remote_routes.to_owned())
            {
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
    use std::{str::FromStr, sync::Arc};

    use sha2::{Digest as _, Sha256};
    use stigmerge_fileindex::Indexer;
    use tempfile::TempDir;
    use tokio_util::sync::CancellationToken;
    use veilid_core::{
        BarePublicKey, BareRouteId, DHTSchemaDFLT, RecordKey, RouteId, ValueSubkeyRangeSet,
        CRYPTO_KIND_VLD0,
    };
    use veilnet::connection::{
        testing::{create_test_api, StubAPI, StubConnection, StubRoutingContext},
        RoutingContext,
    };

    use crate::{
        proto::{Encoder, HaveMapRef, Header, PeerMapRef},
        retry::Retry,
        share_resolver::ShareResolver,
        tests::temp_file,
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

        // Create a connection with mock behavior
        let temp_dir = TempDir::new().unwrap();
        let veilid_api = create_test_api(temp_dir.path()).await.unwrap();
        let mut api = StubAPI::new(veilid_api);

        // Mock route import
        api.import_remote_private_route = Arc::new(std::sync::Mutex::new(|_route_data| {
            Ok(RouteId::new(CRYPTO_KIND_VLD0, BareRouteId::new(&[0u8; 32])))
        }));

        // Create a dummy public key for ValueData::new
        let dummy_public_key =
            veilid_core::PublicKey::new(CRYPTO_KIND_VLD0, BarePublicKey::new(&[0u8; 32]));

        let mock_index = index.clone();
        let get_dht_calls = Arc::new(std::sync::Mutex::new(vec![]));
        let watch_calls = Arc::new(std::sync::Mutex::new(vec![]));
        let cancel_watch_calls = Arc::new(std::sync::Mutex::new(Vec::<RecordKey>::new()));

        let mut routing_context = StubRoutingContext::new(api);
        {
            let get_dht_calls = get_dht_calls.clone();
            let dummy_public_key = dummy_public_key.clone();
            routing_context.get_dht_value = Arc::new(tokio::sync::Mutex::new(
                move |key: RecordKey, subkey: u32, _force_refresh| {
                    get_dht_calls.lock().unwrap().push((key, subkey));
                    if subkey == 0 {
                        // Return header data
                        let index_internal = mock_index.clone();
                        let index_bytes = index_internal.encode().expect("encode index");
                        let header = Header::from_index(
                            &index_internal,
                            index_bytes.as_slice(),
                            &[0xde, 0xad, 0xbe, 0xef],
                        );
                        let header_bytes = header.encode().expect("encode header");
                        Ok(Some(
                            veilid_core::ValueData::new(header_bytes, dummy_public_key.clone())
                                .unwrap(),
                        ))
                    } else {
                        // Return index data for subkey 1
                        let index_internal = mock_index.clone();
                        let index_bytes = index_internal.encode().expect("encode index");
                        Ok(Some(
                            veilid_core::ValueData::new(index_bytes, dummy_public_key.clone())
                                .unwrap(),
                        ))
                    }
                },
            ));
        }

        {
            let watch_calls = watch_calls.clone();
            routing_context.watch_dht_values = Arc::new(tokio::sync::Mutex::new(
                move |key: RecordKey, _values: Option<ValueSubkeyRangeSet>, _expiration, _count| {
                    watch_calls.lock().unwrap().push(key);
                    Ok(true) // Return true instead of ()
                },
            ));
        }

        {
            let cancel_watch_calls = cancel_watch_calls.clone();
            routing_context.cancel_dht_watch = Arc::new(tokio::sync::Mutex::new(
                move |key: RecordKey, _values: Option<ValueSubkeyRangeSet>| {
                    cancel_watch_calls.lock().unwrap().push(key);
                    Ok(true) // Return true instead of ()
                },
            ));
        }
        let mut conn = StubConnection::new(routing_context.clone());
        conn.require_attachment = Arc::new(tokio::sync::Mutex::new(|| Ok(())));
        conn.add_update_handler = Arc::new(std::sync::Mutex::new(|_| ()));

        let dht_rec = routing_context
            .create_dht_record(
                CRYPTO_KIND_VLD0,
                veilid_core::DHTSchema::DFLT(DHTSchemaDFLT::new(2).unwrap()),
                None,
            )
            .await
            .unwrap();
        let fake_key = dht_rec.key().clone();

        let cancel = CancellationToken::new();
        let retry = Retry::default();
        let (resolver, resolver_task) =
            ShareResolver::new_task(cancel.clone(), retry, conn, index.root());

        // Test adding a share - this should resolve to index
        let share_info = resolver.add_share(&fake_key).await.expect("add share");

        // Verify the resolved share info
        assert_eq!(share_info.key, fake_key);
        assert_eq!(share_info.index.root(), index.root());

        // Verify the header was created correctly
        let expected_index_bytes = index.encode().expect("encode index");
        let expected_header = Header::from_index(
            &index,
            expected_index_bytes.as_slice(),
            &[0xde, 0xad, 0xbe, 0xef],
        );
        assert_eq!(share_info.header.route_data(), expected_header.route_data());

        // Verify DHT calls were made
        let get_dht_calls = get_dht_calls.lock().unwrap();
        assert_eq!(get_dht_calls.len(), 2); // Header (subkey 0) and index (subkey 1)
        assert_eq!(get_dht_calls[0].0, fake_key);
        assert_eq!(get_dht_calls[0].1, 0); // Header subkey
        assert_eq!(get_dht_calls[1].0, fake_key);
        assert_eq!(get_dht_calls[1].1, 1); // Index subkey

        let watch_calls = watch_calls.lock().unwrap();
        assert_eq!(watch_calls.len(), 1);
        assert_eq!(watch_calls[0], fake_key);

        // Test removing the share
        resolver
            .remove_share(&fake_key)
            .await
            .expect("remove share");

        let cancel_watch_calls = cancel_watch_calls.lock().unwrap();
        assert_eq!(cancel_watch_calls.len(), 1);
        assert_eq!(cancel_watch_calls[0], fake_key);

        // Initiate a shutdown
        cancel.cancel();

        // Service run terminates
        resolver_task.await.unwrap().unwrap_err();
    }

    #[tokio::test]
    async fn test_resolve_header() {
        let tf = temp_file(0xa5u8, 65536);
        let indexer = Indexer::from_file(std::env::temp_dir().join(tf.path()).as_path())
            .await
            .expect("indexer");
        let index = indexer.index().await.expect("index");

        let fake_peer_map_key =
            RecordKey::from_str("VLD0:hIfQGdXUq-oO5wwzODJukR7zOGwpNznKYaFoh6uTp2A").expect("key");
        let fake_have_map_key =
            RecordKey::from_str("VLD0:rl3AyyZNFWP8GQGyY9xSnnIjCDagXzbCA47HFmsbLDU").expect("key");

        // Create a connection with mock behavior
        let temp_dir = TempDir::new().unwrap();
        let veilid_api = create_test_api(temp_dir.path()).await.unwrap();
        let mut api = StubAPI::new(veilid_api);

        // Mock route import
        api.import_remote_private_route = Arc::new(std::sync::Mutex::new(|_route_data| {
            Ok(RouteId::new(CRYPTO_KIND_VLD0, BareRouteId::new(&[0u8; 32])))
        }));

        // Create a dummy public key for ValueData::new
        let dummy_public_key =
            veilid_core::PublicKey::new(CRYPTO_KIND_VLD0, BarePublicKey::new(&[0u8; 32]));

        let mock_index = index.clone();
        let mock_peer_map_key = fake_peer_map_key.clone();
        let mock_have_map_key = fake_have_map_key.clone();
        let get_dht_calls = Arc::new(std::sync::Mutex::new(vec![]));
        let watch_calls = Arc::new(std::sync::Mutex::new(vec![]));

        let mut routing_context = StubRoutingContext::new(api);
        {
            let get_dht_calls = get_dht_calls.clone();
            let dummy_public_key = dummy_public_key.clone();
            routing_context.get_dht_value = Arc::new(tokio::sync::Mutex::new(
                move |key: RecordKey, subkey: u32, _force_refresh| {
                    get_dht_calls.lock().unwrap().push((key, subkey));
                    if subkey == 0 {
                        // Return header data with peer_map and have_map
                        let index_internal = mock_index.clone();
                        let index_bytes = index_internal.encode().expect("encode index");
                        let mut header = Header::from_index(
                            &index_internal,
                            index_bytes.as_slice(),
                            &[0xde, 0xad, 0xbe, 0xef],
                        );
                        header.set_peer_map(PeerMapRef::new(mock_peer_map_key.clone(), 1u16));
                        header.set_have_map(HaveMapRef::new(mock_have_map_key.clone(), 1u16));
                        let header_bytes = header.encode().expect("encode header");
                        Ok(Some(
                            veilid_core::ValueData::new(header_bytes, dummy_public_key.clone())
                                .unwrap(),
                        ))
                    } else {
                        // Return index data for subkey 1
                        let index_internal = mock_index.clone();
                        let index_bytes = index_internal.encode().expect("encode index");
                        Ok(Some(
                            veilid_core::ValueData::new(index_bytes, dummy_public_key.clone())
                                .unwrap(),
                        ))
                    }
                },
            ));
        }

        {
            let watch_calls = watch_calls.clone();
            routing_context.watch_dht_values = Arc::new(tokio::sync::Mutex::new(
                move |key: RecordKey, _values: Option<ValueSubkeyRangeSet>, _expiration, _count| {
                    watch_calls.lock().unwrap().push(key);
                    Ok(true) // Return true instead of ()
                },
            ));
        }
        let mut conn = StubConnection::new(routing_context.clone());
        conn.require_attachment = Arc::new(tokio::sync::Mutex::new(|| Ok(())));
        conn.add_update_handler = Arc::new(std::sync::Mutex::new(|_| ()));

        let cancel = CancellationToken::new();
        let retry = Retry::default();
        let (resolver, resolver_task) =
            ShareResolver::new_task(cancel.clone(), retry, conn, index.root());

        let dht_rec = routing_context
            .create_dht_record(
                CRYPTO_KIND_VLD0,
                veilid_core::DHTSchema::DFLT(DHTSchemaDFLT::new(2).unwrap()),
                None,
            )
            .await
            .unwrap();
        let fake_key = dht_rec.key().clone();

        // Test adding a share - this should resolve to header with peer_map and have_map
        let share_info = resolver.add_share(&fake_key).await.expect("add share");

        // Verify the resolved share info
        assert_eq!(share_info.key, fake_key);
        assert_eq!(share_info.index.root(), index.root());

        // Verify the header was created correctly with peer_map and have_map
        assert_eq!(
            share_info.header.have_map().map(|m| m.key()),
            Some(&fake_have_map_key)
        );
        assert_eq!(
            share_info.header.peer_map().map(|m| m.key()),
            Some(&fake_peer_map_key)
        );
        assert_eq!(
            share_info.header.have_map().map(|m| m.subkeys()),
            Some(1u16)
        );
        assert_eq!(
            share_info.header.peer_map().map(|m| m.subkeys()),
            Some(1u16)
        );

        // Verify DHT calls were made
        let get_dht_calls = get_dht_calls.lock().unwrap();
        assert_eq!(get_dht_calls.len(), 2); // Header (subkey 0) and index (subkey 1)
        assert_eq!(get_dht_calls[0].0, fake_key);
        assert_eq!(get_dht_calls[0].1, 0); // Header subkey
        assert_eq!(get_dht_calls[1].0, fake_key);
        assert_eq!(get_dht_calls[1].1, 1); // Index subkey

        let watch_calls = watch_calls.lock().unwrap();
        assert_eq!(watch_calls.len(), 1);
        assert_eq!(watch_calls[0], fake_key);

        // Initiate a shutdown
        cancel.cancel();

        // Service run terminates with either cancelled or receive error
        resolver_task.await.unwrap().unwrap_err();
    }
}
