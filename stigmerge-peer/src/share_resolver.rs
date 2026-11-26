use std::{
    collections::{HashMap, VecDeque},
    path::{Path, PathBuf},
    sync::Arc,
};

use tokio::{select, spawn, sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{instrument, trace, warn};
use veilid_core::{RecordKey, RouteId, ValueSubkeyRangeSet, VeilidRouteChange};
use veilnet::{
    connection::{RoutingContext, UpdateHandler, API},
    Connection,
};

use crate::{
    content_addressable::ContentAddressable,
    error::CancelError,
    record::{StableHaveMap, StableShareRecord},
    types::RemoteShareInfo,
    Error, Result, Retry,
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

    #[instrument(skip(self), err)]
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
        trace!(
            ?key,
            want_index_digest = hex::encode(header.payload_digest()),
            "read header"
        );
        let mut index =
            StableShareRecord::read_index(&mut self.conn, key, &header, self.root.as_path())
                .await?;
        let index_digest = index.digest()?;
        trace!(?key, index_digest = hex::encode(index_digest), "read index");

        let have_map = match header.have_map() {
            Some(have_map_ref) => {
                StableHaveMap::read_remote(&mut self.conn, have_map_ref.key(), &index).await?
            }
            None => {
                return Err(Error::msg("missing have map"));
            }
        };

        let routing_context = self.conn.routing_context();
        let route_id = routing_context
            .api()
            .import_remote_private_route(header.route_data().to_vec())?;
        trace!(?key, ?route_id, "private route to remote");

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
            have_map,
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
    fn share_changed(&mut self, share: &RemoteShareInfo) -> Result<()>;
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
    fn share_changed(&mut self, share: &RemoteShareInfo) -> Result<()> {
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
    fn share_changed(&mut self, share: &RemoteShareInfo) -> Result<()> {
        let mut to_remove = VecDeque::new();
        for (i, handler) in self.handlers.iter_mut().enumerate() {
            if let Err(err) = handler.share_changed(share) {
                warn!(?err);
                to_remove.push_back(i);
            }
        }
        if !to_remove.is_empty() {
            self.handlers = self
                .handlers
                .drain(..)
                .enumerate()
                .filter(|(i, _)| {
                    if to_remove.is_empty() {
                        return true;
                    }
                    if to_remove[0] != *i {
                        true
                    } else {
                        to_remove.pop_front();
                        false
                    }
                })
                .map(|(_, h)| h)
                .collect();
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

    use stigmerge_fileindex::Indexer;
    use tempfile::TempDir;
    use tokio_util::sync::CancellationToken;
    use veilid_core::{
        BareRouteId, DHTSchema, RecordKey, RouteId, ValueSubkeyRangeSet, CRYPTO_KIND_VLD0,
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

    // ValueData has no public constructor. Populate local records and replay their
    // values so resolver reads can be observed without contacting the network.
    async fn stub_share_reads(
        real_rc: veilid_core::RoutingContext,
        rc: &mut StubRoutingContext,
        index: &stigmerge_fileindex::Index,
        peer_map: Option<PeerMapRef>,
        calls: Arc<std::sync::Mutex<Vec<(RecordKey, u32)>>>,
    ) -> (RecordKey, RecordKey) {
        let share = rc
            .create_dht_record(CRYPTO_KIND_VLD0, DHTSchema::dflt(2).unwrap(), None)
            .await
            .unwrap()
            .key();
        let have = rc
            .create_dht_record(CRYPTO_KIND_VLD0, DHTSchema::dflt(1).unwrap(), None)
            .await
            .unwrap()
            .key();
        let index_bytes = index.encode().expect("encode index");
        let mut header = Header::from_index(index, &index_bytes, &[0xde, 0xad, 0xbe, 0xef]);
        header.set_have_map(HaveMapRef::new(have.clone(), 1));
        if let Some(peer_map) = peer_map {
            header.set_peer_map(peer_map);
        }
        let mut values = std::collections::HashMap::new();
        for (key, subkey, bytes) in [
            (&share, 0, header.encode().expect("encode header")),
            (&share, 1, index_bytes),
            (&have, 0, vec![0u8; 100]),
        ] {
            real_rc
                .set_dht_value(key.clone(), subkey, bytes, None)
                .await
                .unwrap();
            let value = real_rc
                .get_dht_value(key.clone(), subkey, false)
                .await
                .unwrap()
                .expect("local DHT value");
            values.insert((key.clone(), subkey), value);
        }
        rc.get_dht_value = Arc::new(tokio::sync::Mutex::new(move |key: RecordKey, subkey, _| {
            calls.lock().unwrap().push((key.clone(), subkey));
            Ok(values.get(&(key, subkey)).cloned())
        }));
        (share, have)
    }

    #[tokio::test]
    async fn test_resolve_index() {
        let tf = temp_file(0xa5u8, 65536);
        let indexer = Indexer::from_file(std::env::temp_dir().join(tf.path()).as_path())
            .await
            .expect("indexer");
        let index = indexer.index().await.expect("index");
        // Create a connection with mock behavior
        let temp_dir = TempDir::new().unwrap();
        let veilid_api = create_test_api(temp_dir.path()).await.unwrap();
        let real_rc = veilid_api.routing_context().unwrap();
        let mut api = StubAPI::new(veilid_api);

        // Mock route import
        api.import_remote_private_route = Arc::new(std::sync::Mutex::new(|_route_data| {
            Ok(RouteId::new(CRYPTO_KIND_VLD0, BareRouteId::new(&[0u8; 32])))
        }));

        let get_dht_calls = Arc::new(std::sync::Mutex::new(vec![]));
        let watch_calls = Arc::new(std::sync::Mutex::new(vec![]));
        let cancel_watch_calls = Arc::new(std::sync::Mutex::new(Vec::<RecordKey>::new()));

        let mut routing_context = StubRoutingContext::new(api);
        let (fake_key, _) = stub_share_reads(
            real_rc,
            &mut routing_context,
            &index,
            None,
            get_dht_calls.clone(),
        )
        .await;

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
        let mut conn = StubConnection::new(routing_context);
        conn.require_attachment = Arc::new(tokio::sync::Mutex::new(|| Ok(())));
        conn.add_update_handler = Arc::new(std::sync::Mutex::new(|_| ()));

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
        assert_eq!(get_dht_calls.len(), 3); // Header (subkey 0) and index (subkey 1), have map
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

        // Create a connection with mock behavior
        let temp_dir = TempDir::new().unwrap();
        let veilid_api = create_test_api(temp_dir.path()).await.unwrap();
        let real_rc = veilid_api.routing_context().unwrap();
        let mut api = StubAPI::new(veilid_api);

        // Mock route import
        api.import_remote_private_route = Arc::new(std::sync::Mutex::new(|_route_data| {
            Ok(RouteId::new(CRYPTO_KIND_VLD0, BareRouteId::new(&[0u8; 32])))
        }));

        let get_dht_calls = Arc::new(std::sync::Mutex::new(vec![]));
        let watch_calls = Arc::new(std::sync::Mutex::new(vec![]));

        let mut routing_context = StubRoutingContext::new(api);
        let (fake_key, fake_have_map_key) = stub_share_reads(
            real_rc,
            &mut routing_context,
            &index,
            Some(PeerMapRef::new(fake_peer_map_key.clone(), 1)),
            get_dht_calls.clone(),
        )
        .await;

        {
            let watch_calls = watch_calls.clone();
            routing_context.watch_dht_values = Arc::new(tokio::sync::Mutex::new(
                move |key: RecordKey, _values: Option<ValueSubkeyRangeSet>, _expiration, _count| {
                    watch_calls.lock().unwrap().push(key);
                    Ok(true) // Return true instead of ()
                },
            ));
        }
        let mut conn = StubConnection::new(routing_context);
        conn.require_attachment = Arc::new(tokio::sync::Mutex::new(|| Ok(())));
        conn.add_update_handler = Arc::new(std::sync::Mutex::new(|_| ()));

        let cancel = CancellationToken::new();
        let retry = Retry::default();
        let (resolver, resolver_task) =
            ShareResolver::new_task(cancel.clone(), retry, conn, index.root());

        // Test adding a share - this should resolve to header with peer_map and have_map
        let share_info = resolver.add_share(&fake_key).await.expect("add share");

        // Verify the resolved share info
        assert_eq!(share_info.key, fake_key);
        assert_eq!(share_info.index.root(), index.root());

        // Verify the header was created correctly with peer_map and have_map
        assert_eq!(
            share_info.header.have_map().map(|m| m.key()),
            Some(&fake_have_map_key),
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
        assert_eq!(get_dht_calls.len(), 3); // Header (subkey 0) and index (subkey 1), have map
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
