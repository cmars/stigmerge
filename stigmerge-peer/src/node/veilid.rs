use std::{cmp::min, path::Path, sync::Arc};

use tokio::sync::{broadcast, RwLock};
use tracing::{debug, trace, warn};
use veilid_core::{
    DHTRecordDescriptor, DHTSchema, KeyPair, OperationId, RoutingContext, Target, ValueData,
    ValueSubkeyRangeSet, VeilidAPIError, VeilidUpdate,
};

use stigmerge_fileindex::{FileSpec, Index, PayloadPiece, PayloadSpec};

use crate::{
    peer_announcer::DEFAULT_MAX_PEERS,
    piece_map::PieceMap,
    proto::{
        AdvertisePeerRequest, BlockRequest, Decoder, Encoder, HaveMapRef, Header, PeerInfo,
        PeerMapRef, Request,
    },
    Error, Result,
};

use super::{Node, TypedKey};

pub struct Veilid {
    routing_context: Arc<RwLock<RoutingContext>>,

    update_rx: broadcast::Receiver<VeilidUpdate>,
}

impl Veilid {
    pub async fn new(
        routing_context: RoutingContext,
        update_rx: broadcast::Receiver<VeilidUpdate>,
    ) -> Result<Self> {
        Ok(Veilid {
            routing_context: Arc::new(RwLock::new(routing_context)),
            update_rx,
        })
    }

    async fn open_or_create_dht_record(
        &self,
        rc: &RoutingContext,
        header: &Header,
    ) -> Result<DHTRecordDescriptor> {
        let api = rc.api();
        let ts = api.table_store()?;
        let db = ts.open("stigmerge_share_dht", 2).await?;
        let digest_key = header.payload_digest();
        let maybe_dht_key = db.load_json(0, digest_key.as_slice()).await?;
        let maybe_dht_owner_keypair = db.load_json(1, digest_key.as_slice()).await?;
        if let (Some(dht_key), Some(dht_owner_keypair)) = (maybe_dht_key, maybe_dht_owner_keypair) {
            return Ok(rc.open_dht_record(dht_key, dht_owner_keypair).await?);
        }
        let o_cnt = header.subkeys() + 1;
        debug!(o_cnt, "header subkeys");
        let dht_rec = rc
            .create_dht_record(DHTSchema::dflt(o_cnt)?, None, None)
            .await?;
        let dht_owner = KeyPair::new(
            dht_rec.owner().to_owned(),
            dht_rec
                .owner_secret()
                .ok_or(Error::msg("expected dht owner secret"))?
                .to_owned(),
        );
        db.store_json(0, digest_key.as_slice(), dht_rec.key())
            .await?;
        db.store_json(1, digest_key.as_slice(), &dht_owner).await?;
        Ok(dht_rec)
    }

    async fn open_or_create_have_map_record(
        &self,
        rc: &tokio::sync::RwLockReadGuard<'_, RoutingContext>,
        index: &Index,
    ) -> Result<(DHTRecordDescriptor, u16)> {
        let api = rc.api();
        let ts = api.table_store()?;
        let db = ts.open("stigmerge_have_map_dht", 2).await?;
        let digest_key = index.payload().digest();
        let o_cnt = PieceMap::subkeys(index.payload().pieces().len());
        let maybe_dht_key = db.load_json(0, digest_key).await?;
        let maybe_dht_owner_keypair = db.load_json(1, digest_key).await?;
        if let (Some(dht_key), Some(dht_owner_keypair)) = (maybe_dht_key, maybe_dht_owner_keypair) {
            return Ok((rc.open_dht_record(dht_key, dht_owner_keypair).await?, o_cnt));
        }

        let dht_rec = rc
            .create_dht_record(DHTSchema::dflt(o_cnt)?, None, None)
            .await?;
        let dht_owner = KeyPair::new(
            dht_rec.owner().to_owned(),
            dht_rec
                .owner_secret()
                .ok_or(Error::msg("expected dht owner secret"))?
                .to_owned(),
        );
        db.store_json(0, digest_key, dht_rec.key()).await?;
        db.store_json(1, digest_key, &dht_owner).await?;
        Ok((dht_rec, o_cnt))
    }

    async fn open_or_create_peer_map_record(
        &self,
        rc: &tokio::sync::RwLockReadGuard<'_, RoutingContext>,
        payload_digest: &[u8],
    ) -> Result<(DHTRecordDescriptor, u16)> {
        let api = rc.api();
        let ts = api.table_store()?;
        let db = ts.open("stigmerge_peer_map_dht", 2).await?;
        let o_cnt = DEFAULT_MAX_PEERS;
        let maybe_dht_key = db.load_json(0, payload_digest).await?;
        let maybe_dht_owner_keypair = db.load_json(1, payload_digest).await?;
        if let (Some(dht_key), Some(dht_owner_keypair)) = (maybe_dht_key, maybe_dht_owner_keypair) {
            return Ok((rc.open_dht_record(dht_key, dht_owner_keypair).await?, o_cnt));
        }

        let dht_rec = rc
            .create_dht_record(DHTSchema::dflt(o_cnt)?, None, None)
            .await?;
        let dht_owner = KeyPair::new(
            dht_rec.owner().to_owned(),
            dht_rec
                .owner_secret()
                .ok_or(Error::msg("expected dht owner secret"))?
                .to_owned(),
        );
        db.store_json(0, payload_digest, dht_rec.key()).await?;
        db.store_json(1, payload_digest, &dht_owner).await?;
        Ok((dht_rec, o_cnt))
    }

    async fn write_header(
        &self,
        rc: &RoutingContext,
        key: &TypedKey,
        header: &Header,
    ) -> Result<()> {
        // Encode the header
        let header_bytes = header.encode()?;
        debug!(
            header_length = header_bytes.len(),
            key = key.to_string(),
            have_map_key = header.have_map().map(|hmr| hmr.key().to_string()),
            peer_map_key = header.peer_map().map(|pmr| pmr.key().to_string()),
            "writing header"
        );

        rc.set_dht_value(key.to_owned(), 0, header_bytes, None)
            .await?;
        Ok(())
    }

    async fn write_index_bytes(
        &self,
        rc: &RoutingContext,
        dht_key: &TypedKey,
        index_bytes: &[u8],
    ) -> Result<()> {
        let mut subkey = 1; // index starts at subkey 1 (header is subkey 0)
        let mut offset = 0;
        loop {
            if offset > index_bytes.len() {
                return Ok(());
            }
            let count = min(ValueData::MAX_LEN, index_bytes.len() - offset);
            debug!(offset, count, "writing index");
            rc.set_dht_value(
                dht_key.to_owned(),
                subkey,
                index_bytes[offset..offset + count].to_vec(),
                None,
            )
            .await?;
            subkey += 1;
            offset += ValueData::MAX_LEN;
        }
    }

    async fn read_header(&self, rc: &RoutingContext, key: &TypedKey) -> Result<Header> {
        debug!(key = key.to_string());
        let subkey_value = match rc.get_dht_value(key.to_owned(), 0, true).await? {
            Some(value) => value,
            None => {
                warn!("missing header");
                return Err(VeilidAPIError::KeyNotFound {
                    key: key.to_owned(),
                }
                .into());
            }
        };
        let header = Header::decode(subkey_value.data())?;
        debug!(
            header_length = subkey_value.data_size(),
            key = key.to_string(),
            have_map_key = header.have_map().map(|hmr| hmr.key().to_string()),
            peer_map_key = header.peer_map().map(|pmr| pmr.key().to_string()),
        );
        Ok(header)
    }

    async fn read_index(
        &self,
        rc: &RoutingContext,
        key: &TypedKey,
        header: &Header,
        root: &Path,
    ) -> Result<Index> {
        let mut index_bytes = vec![];
        for i in 0..header.subkeys() {
            let subkey_value = match rc
                .get_dht_value(key.to_owned(), (i + 1) as u32, true)
                .await?
            {
                Some(value) => value,
                None => {
                    return Err(VeilidAPIError::KeyNotFound {
                        key: key.to_owned(),
                    }
                    .into())
                }
            };
            index_bytes.extend_from_slice(subkey_value.data());
        }
        let (payload_pieces, payload_files) =
            <(Vec<PayloadPiece>, Vec<FileSpec>)>::decode(index_bytes.as_slice())?;
        Ok(Index::new(
            root.to_path_buf(),
            PayloadSpec::new(
                header.payload_digest(),
                header.payload_length(),
                payload_pieces,
            ),
            payload_files,
        ))
    }

    async fn release_prior_route(&self, rc: &RoutingContext, prior_route: Option<Target>) {
        match prior_route {
            Some(Target::PrivateRoute(target)) => {
                // Ignore errors here; prior route may have already been
                // released and veilid_core logs the error.
                let _ = rc.api().release_private_route(target);
            }
            _ => {}
        }
    }
}

impl Clone for Veilid {
    fn clone(&self) -> Self {
        Veilid {
            routing_context: self.routing_context.clone(),
            update_rx: self.update_rx.resubscribe(),
        }
    }
}

impl Node for Veilid {
    fn subscribe_veilid_update(&self) -> broadcast::Receiver<VeilidUpdate> {
        self.update_rx.resubscribe()
    }

    async fn reset(&mut self) -> Result<()> {
        let rc = self.routing_context.write().await;
        if let Err(e) = rc.api().detach().await {
            trace!(err = e.to_string(), "detach failed");
        }
        rc.api().attach().await?;
        Ok(())
    }

    async fn shutdown(self) -> Result<()> {
        let rc = self.routing_context.write().await;
        rc.api().shutdown().await;
        Ok(())
    }

    async fn announce_index(&mut self, index: &Index) -> Result<(TypedKey, Target, Header)> {
        let rc = self.routing_context.read().await;
        // Serialize index to index_bytes
        let index_bytes = index.encode()?;
        let (announce_route, route_data) = rc.api().new_private_route().await?;
        let mut header = Header::from_index(index, index_bytes.as_slice(), route_data.as_slice());

        let (have_map_key, have_map_subkeys) =
            self.open_or_create_have_map_record(&rc, index).await?;
        header = header.with_have_map(HaveMapRef::new(
            have_map_key.key().to_owned(),
            have_map_subkeys,
        ));

        let (peer_map_key, peer_map_subkeys) = self
            .open_or_create_peer_map_record(&rc, index.payload().digest())
            .await?;
        header = header.with_peer_map(PeerMapRef::new(
            peer_map_key.key().to_owned(),
            peer_map_subkeys,
        ));

        let dht_rec = self.open_or_create_dht_record(&rc, &header).await?;
        let dht_key = dht_rec.key().to_owned();
        self.write_index_bytes(&rc, &dht_key, index_bytes.as_slice())
            .await?;
        self.write_header(&rc, &dht_key, &header).await?;
        trace!(header = format!("{:?}", header));
        Ok((dht_key, Target::PrivateRoute(announce_route), header))
    }

    async fn announce_route(
        &mut self,
        key: &TypedKey,
        prior_route: Option<Target>,
        header: &Header,
    ) -> Result<(Target, Header)> {
        debug!(key = key.to_string());
        let rc = self.routing_context.read().await;
        self.release_prior_route(&rc, prior_route).await;
        let (announce_route, route_data) = rc.api().new_private_route().await?;
        let header = header.with_route_data(route_data);
        self.write_header(&rc, &key, &header).await?;
        Ok((Target::PrivateRoute(announce_route), header))
    }

    async fn resolve_route_index(
        &mut self,
        key: &TypedKey,
        root: &Path,
    ) -> Result<(Target, Header, Index)> {
        let rc = self.routing_context.read().await;
        let _ = rc.open_dht_record(key.to_owned(), None).await?;
        let header = self.read_header(&rc, key).await?;
        let index = self.read_index(&rc, key, &header, &root).await?;
        let target = rc
            .api()
            .import_remote_private_route(header.route_data().to_vec())?;
        Ok((Target::PrivateRoute(target), header, index))
    }

    async fn resolve_route(
        &mut self,
        key: &TypedKey,
        prior_route: Option<Target>,
    ) -> Result<(Target, Header)> {
        debug!(key = key.to_string());
        let rc = self.routing_context.read().await;
        let _ = rc.open_dht_record(key.to_owned(), None).await?;
        self.release_prior_route(&rc, prior_route).await;
        let header = self.read_header(&rc, key).await?;
        let target = rc
            .api()
            .import_remote_private_route(header.route_data().to_vec())?;
        Ok((Target::PrivateRoute(target), header))
    }

    async fn request_block(
        &mut self,
        target: Target,
        piece: usize,
        block: usize,
    ) -> Result<Option<Vec<u8>>> {
        let rc = self.routing_context.read().await;
        let block_req = Request::BlockRequest(BlockRequest {
            piece: piece as u32,
            block: block as u8,
        });
        let block_req_bytes = block_req.encode()?;
        let resp_bytes = rc.app_call(target, block_req_bytes).await?;
        Ok(if resp_bytes.is_empty() {
            None
        } else {
            Some(resp_bytes)
        })
    }

    async fn reply_block_contents(
        &mut self,
        call_id: OperationId,
        contents: Option<&[u8]>,
    ) -> Result<()> {
        let rc = self.routing_context.read().await;
        rc.api()
            .app_call_reply(call_id, contents.unwrap_or(&[]).to_vec())
            .await?;
        Ok(())
    }

    async fn request_advertise_peer(&mut self, target: &Target, key: &TypedKey) -> Result<()> {
        let rc = self.routing_context.read().await;
        let req = Request::AdvertisePeer(AdvertisePeerRequest { key: *key });
        let req_bytes = req.encode()?;
        rc.app_message(*target, req_bytes).await?;
        Ok(())
    }

    async fn watch(&mut self, key: TypedKey, values: ValueSubkeyRangeSet) -> Result<()> {
        {
            // Ensure DHT record is open on this node
            let _ = self
                .routing_context
                .read()
                .await
                .open_dht_record(key.to_owned(), None)
                .await?;
        }
        let rc = self.routing_context.clone();
        let routing_context = rc.read().await;
        routing_context
            .watch_dht_values(key, Some(values.clone()), None, None)
            .await?;
        Ok(())
    }

    async fn cancel_watch(&mut self, key: &TypedKey) -> Result<()> {
        let rc = self.routing_context.clone();
        let routing_context = rc.read().await;
        routing_context
            .watch_dht_values(key.to_owned(), None, None, Some(0))
            .await?;
        Ok(())
    }

    async fn merge_have_map(
        &mut self,
        key: TypedKey,
        subkeys: ValueSubkeyRangeSet,
        have_map: &mut PieceMap,
    ) -> Result<()> {
        let rc = self.routing_context.read().await;
        for subkey in subkeys.iter() {
            match rc.get_dht_value(key, subkey, true).await? {
                Some(data) => {
                    have_map.write_bytes(
                        TryInto::<usize>::try_into(subkey).unwrap() * ValueData::MAX_LEN,
                        data.data(),
                    );
                }
                None => break,
            }
        }
        Ok(())
    }

    async fn announce_have_map(&mut self, key: TypedKey, have_map: &PieceMap) -> Result<()> {
        let rc = self.routing_context.read().await;
        let data = have_map.as_ref();
        let subkeys = (data.len() / ValueData::MAX_LEN)
            + if data.len() % ValueData::MAX_LEN > 0 {
                1
            } else {
                0
            };
        for subkey in 0..subkeys {
            let offset = subkey * ValueData::MAX_LEN;
            let limit = min(ValueData::MAX_LEN, data.len());
            rc.set_dht_value(
                key,
                TryInto::<u32>::try_into(subkey).unwrap(),
                data[offset..limit].to_owned(),
                None,
            )
            .await?;
        }
        Ok(())
    }

    async fn announce_peer(
        &mut self,
        payload_digest: &[u8],
        peer_key: Option<TypedKey>,
        subkey: u16,
    ) -> Result<()> {
        let rc = self.routing_context.read().await;
        let (peer_map_dht, _o_cnt) = self
            .open_or_create_peer_map_record(&rc, payload_digest)
            .await?;
        match peer_key {
            Some(peer_key) => {
                let peer_info = PeerInfo::new(peer_key);
                rc.set_dht_value(
                    peer_map_dht.key().to_owned(),
                    subkey.into(),
                    peer_info.encode()?,
                    None,
                )
                .await?;
            }
            None => {
                rc.set_dht_value(peer_map_dht.key().to_owned(), subkey.into(), vec![], None)
                    .await?;
            }
        }
        Ok(())
    }

    async fn reset_peers(&mut self, payload_digest: &[u8], max_subkey: u16) -> Result<()> {
        let rc = self.routing_context.read().await;
        let (peer_map_dht, _o_cnt) = self
            .open_or_create_peer_map_record(&rc, payload_digest)
            .await?;
        for subkey in 0..max_subkey {
            rc.set_dht_value(peer_map_dht.key().to_owned(), subkey.into(), vec![], None)
                .await?;
        }
        Ok(())
    }

    async fn resolve_have_map(&mut self, peer_key: &TypedKey) -> Result<PieceMap> {
        let (_, header) = self.resolve_route(peer_key, None).await?;
        let have_map_ref = match header.have_map() {
            Some(have_map_ref) => have_map_ref,
            None => {
                return Err(Error::msg(format!(
                    "peer {peer_key} does not publish a have map"
                )))
            }
        };

        debug!(
            peer_share_key = peer_key.to_string(),
            have_map_key = have_map_ref.key().to_string()
        );

        let rc = self.routing_context.read().await;
        let dht_rec = rc
            .open_dht_record(have_map_ref.key().to_owned(), None)
            .await?;
        let mut have_map = PieceMap::new();
        for subkey in 0..have_map_ref.subkeys() {
            match rc
                .get_dht_value(dht_rec.key().to_owned(), subkey.into(), true)
                .await?
            {
                Some(data) => {
                    have_map.write_bytes(
                        Into::<usize>::into(subkey) * ValueData::MAX_LEN,
                        data.data(),
                    );
                }
                None => {
                    return Err(Error::msg(format!(
                        "peer {peer_key} have map {} missing expected subkey value {subkey}",
                        dht_rec.key()
                    )))
                }
            };
        }
        Ok(have_map)
    }

    async fn resolve_peers(&mut self, peer_key: &TypedKey) -> Result<Vec<PeerInfo>> {
        let (_, header) = self.resolve_route(peer_key, None).await?;
        let peer_map_ref = match header.peer_map() {
            Some(peer_map_ref) => peer_map_ref,
            None => {
                return Err(Error::msg(format!(
                    "peer {peer_key} does not publish a peer map"
                )))
            }
        };

        debug!(
            peer_share_key = peer_key.to_string(),
            peer_map_key = peer_map_ref.key().to_string()
        );

        let rc = self.routing_context.read().await;
        let dht_rec = rc
            .open_dht_record(peer_map_ref.key().to_owned(), None)
            .await?;
        let mut peers = vec![];
        for subkey in 0..peer_map_ref.subkeys() {
            match rc
                .get_dht_value(dht_rec.key().to_owned(), subkey.into(), true)
                .await?
            {
                Some(data) => {
                    if data.data_size() > 0 {
                        // revoked peers are blanked out
                        let peer_info = PeerInfo::decode(data.data())?;
                        peers.push(peer_info);
                    }
                }
                None => {
                    return Err(Error::msg(format!(
                        "peer {peer_key} peer map {} missing expected subkey value {subkey}",
                        dht_rec.key()
                    )))
                }
            };
        }
        Ok(peers)
    }
}
