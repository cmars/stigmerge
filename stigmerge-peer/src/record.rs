use std::cmp::min;
use std::collections::hash_map::Iter;
use std::collections::HashMap;
use std::path::Path;

use stigmerge_fileindex::{FileSpec, Index, PayloadPiece, PayloadSpec};
use tracing::{debug, instrument, Level};
use veilid_core::{
    BareKeyPair, DHTRecordDescriptor, DHTSchema, KeyPair, RecordKey, SetDHTValueOptions, Timestamp,
    ValueData, ValueSubkeyRangeSet, CRYPTO_KIND_VLD0,
};
use veilnet::connection::{RoutingContext, API};
use veilnet::Connection;

use crate::error::Result;
use crate::piece_map::PieceMap;
use crate::proto::{Decoder, Encoder, Header, PeerInfo};
use crate::Error;

pub struct StablePublicRecord {
    dht_rec: DHTRecordDescriptor,
    key: RecordKey,
    owner_keypair: Option<KeyPair>,
}

impl std::fmt::Debug for StablePublicRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StablePublicRecord")
            .field("dht_rec", &self.dht_rec.key())
            .field(
                "owner_keypair",
                &self.owner_keypair.as_ref().map(|kp| kp.value().key()),
            )
            .finish()
    }
}

impl StablePublicRecord {
    #[instrument(skip_all, fields(table_name, db_key_id = hex::encode(db_key_id), subkeys), ret(level = Level::TRACE))]
    async fn new_local<C: Connection + Send + Sync + 'static>(
        conn: &mut C,
        table_name: &str,
        db_key_id: &[u8],
        subkeys: u16,
    ) -> Result<Self> {
        {
            conn.require_attachment().await?;
        }

        let routing_context = conn.routing_context();
        let api = routing_context.api();
        let table_store = api.table_store()?;
        let db = table_store.open(table_name, 2).await?;

        let existing_dht_key: Option<RecordKey> = db.load_json(0, db_key_id).await?;
        let existing_owner_keypair: Option<KeyPair> = db.load_json(1, db_key_id).await?;

        let dht_rec = match (existing_dht_key, existing_owner_keypair) {
            (Some(dht_key), Some(owner_keypair)) => {
                return Ok(Self {
                    dht_rec: routing_context
                        .open_dht_record(dht_key.clone(), Some(owner_keypair.clone()))
                        .await?,
                    key: dht_key,
                    owner_keypair: Some(owner_keypair),
                });
            }
            _ => {
                routing_context
                    .create_dht_record(CRYPTO_KIND_VLD0, DHTSchema::dflt(subkeys)?, None)
                    .await?
            }
        };
        let dht_owner = KeyPair::new(
            dht_rec.owner().kind(),
            BareKeyPair::new(
                dht_rec.owner().value(),
                dht_rec
                    .owner_secret()
                    .ok_or(Error::msg("expected dht owner secret"))?
                    .value(),
            ),
        );
        db.store_json(0, db_key_id, &dht_rec.key()).await?;
        db.store_json(1, db_key_id, &dht_owner).await?;
        Ok(Self {
            key: dht_rec.key(),
            dht_rec,
            owner_keypair: Some(dht_owner),
        })
    }

    #[instrument(skip(conn), ret(level = Level::TRACE))]
    async fn new_remote<C: Connection + Send + Sync + 'static>(
        conn: &mut C,
        key: &RecordKey,
    ) -> Result<Self> {
        {
            conn.require_attachment().await?;
        }
        let routing_context = conn.routing_context();
        let dht_rec = routing_context.open_dht_record(key.clone(), None).await?;
        Ok(Self {
            dht_rec,
            key: key.clone(),
            owner_keypair: None,
        })
    }

    pub fn key(&self) -> &RecordKey {
        &self.key
    }

    pub async fn read<C: Connection + Send + Sync + 'static>(
        &self,
        conn: &mut C,
        range: ValueSubkeyRangeSet,
    ) -> Result<Vec<u8>> {
        Ok(Self::read_keys(conn, self.key(), range).await?.concat())
    }

    #[instrument(skip(conn))]
    pub async fn read_keys<C: Connection + Send + Sync + 'static>(
        conn: &mut C,
        key: &RecordKey,
        range: ValueSubkeyRangeSet,
    ) -> Result<Vec<Vec<u8>>> {
        {
            conn.require_attachment().await?;
        }
        let routing_context = conn.routing_context();
        let mut result = vec![];
        for subkey in range.iter() {
            match routing_context
                .get_dht_value(key.to_owned(), subkey, true)
                .await?
            {
                Some(data) => result.push(data.data().to_vec()),
                None => break,
            }
        }
        Ok(result)
    }

    #[instrument(skip(conn, data))]
    pub async fn write_keys<C: Connection + Send + Sync + 'static>(
        conn: &mut C,
        key: &RecordKey,
        range: ValueSubkeyRangeSet,
        data: Vec<Vec<u8>>,
    ) -> Result<()> {
        {
            conn.require_attachment().await?;
        }
        let routing_context = conn.routing_context();
        for subkey in range.iter() {
            let i: usize = subkey.try_into()?;
            let key_data = if subkey < data.len().try_into()? {
                data[i].as_slice()
            } else {
                &[]
            };
            routing_context
                .set_dht_value(key.to_owned(), subkey, key_data.to_vec(), None)
                .await?;
        }
        Ok(())
    }

    #[instrument(skip(conn, data))]
    pub async fn write<C: Connection + Send + Sync + 'static>(
        &self,
        conn: &mut C,
        range: ValueSubkeyRangeSet,
        data: &[u8],
    ) -> Result<()> {
        Self::write_key(
            conn,
            self.key(),
            self.owner_keypair
                .as_ref()
                .ok_or(Error::msg("not record owner"))?,
            range,
            data,
        )
        .await
    }

    #[instrument(skip_all)]
    pub async fn write_key<C: Connection + Send + Sync + 'static>(
        conn: &mut C,
        key: &RecordKey,
        owner_keypair: &KeyPair,
        range: ValueSubkeyRangeSet,
        data: &[u8],
    ) -> Result<()> {
        {
            conn.require_attachment().await?;
        }
        let routing_context = conn.routing_context();
        let mut offset = 0;
        for subkey in range.iter() {
            if offset > data.len() {
                routing_context
                    .set_dht_value(key.to_owned(), subkey, vec![], None)
                    .await?;
                continue;
            }
            let count = min(ValueData::MAX_LEN, data.len() - offset);
            routing_context
                .set_dht_value(
                    key.to_owned(),
                    subkey,
                    data[offset..offset + count].to_vec(),
                    Some(SetDHTValueOptions {
                        writer: Some(owner_keypair.clone()),
                        ..Default::default()
                    }),
                )
                .await?;
            offset += ValueData::MAX_LEN;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct StableShareRecord {
    record: StablePublicRecord,
}

impl StableShareRecord {
    #[instrument(skip_all, fields(table_name, db_key_id = hex::encode(index.payload().digest()), subkeys), ret(level = Level::TRACE))]
    pub async fn new_local<C: Connection + Send + Sync + 'static>(
        conn: &mut C,
        index: &Index,
    ) -> Result<Self> {
        let db_key_id = index.payload().digest();
        let subkeys = PieceMap::subkeys(index.payload().pieces().len());
        Ok(Self {
            record: StablePublicRecord::new_local(
                conn,
                "stigmerge_share_dht",
                db_key_id,
                subkeys + 1,
            )
            .await?,
        })
    }

    #[instrument(skip(conn), ret(level = Level::TRACE))]
    pub async fn new_remote<C: Connection + Send + Sync + 'static>(
        conn: &mut C,
        key: &RecordKey,
    ) -> Result<(Self, Header)> {
        let record = StablePublicRecord::new_remote(conn, key).await?;
        let header = Self::read_header(conn, key).await?;
        Ok((Self { record }, header))
    }

    pub fn share_key(&self) -> &RecordKey {
        self.record.key()
    }

    #[instrument(skip(conn))]
    pub async fn read_header<C: Connection + Send + Sync + 'static>(
        conn: &mut C,
        key: &RecordKey,
    ) -> Result<Header> {
        let header_bytes =
            StablePublicRecord::read_keys(conn, key, ValueSubkeyRangeSet::single(0)).await?;
        Ok(Header::decode(header_bytes.concat().as_slice())?)
    }

    #[instrument(skip(conn, header))]
    pub async fn read_index<C: Connection + Send + Sync + 'static>(
        conn: &mut C,
        key: &RecordKey,
        header: &Header,
        root: &Path,
    ) -> Result<Index> {
        let index_bytes = StablePublicRecord::read_keys(
            conn,
            key,
            ValueSubkeyRangeSet::single_range(1, header.subkeys() as u32),
        )
        .await?;
        debug!(
            index_bytes_len = index_bytes
                .iter()
                .map(|sk_val| sk_val.len())
                .reduce(|acc, v| acc + v)
                .unwrap_or(0)
        );
        let (payload_pieces, payload_files) =
            <(Vec<PayloadPiece>, Vec<FileSpec>)>::decode(index_bytes.concat().as_slice())?;
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

    #[instrument(skip_all)]
    pub async fn write_header<C: Connection + Send + Sync + 'static>(
        &mut self,
        conn: &mut C,
        header: &Header,
    ) -> Result<()> {
        let header_bytes = header.encode()?;
        self.record
            .write(
                conn,
                ValueSubkeyRangeSet::single(0),
                header_bytes.as_slice(),
            )
            .await
    }

    #[instrument(skip_all)]
    pub async fn write_index<C: Connection + Send + Sync + 'static>(
        &mut self,
        conn: &mut C,
        index: &Index,
    ) -> Result<()> {
        let index_bytes = index.encode()?;
        let range = ValueSubkeyRangeSet::single_range(1, self.record.dht_rec.schema().max_subkey());
        self.record.write(conn, range, index_bytes.as_slice()).await
    }
}

pub struct StablePeersRecord {
    record: StablePublicRecord,
    peers: HashMap<RecordKey, (usize, PeerInfo)>,
}

impl std::fmt::Debug for StablePeersRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StablePeersRecord")
            .field("record", &self.record)
            .field("n_peers", &self.peers.len())
            .finish()
    }
}

impl StablePeersRecord {
    pub const MAX_PEERS: u16 = 512;

    #[instrument(skip_all, fields(table_name, db_key_id = hex::encode(index.payload().digest()), subkeys), ret(level = Level::TRACE))]
    pub async fn new_local<C: Connection + Send + Sync + 'static>(
        conn: &mut C,
        index: &Index,
    ) -> Result<Self> {
        let db_key_id = index.payload().digest();
        Ok(Self {
            record: StablePublicRecord::new_local(
                conn,
                "stigmerge_peers_dht",
                db_key_id,
                Self::MAX_PEERS,
            )
            .await?,
            peers: HashMap::new(),
        })
    }

    #[instrument(skip(conn), ret(level = Level::TRACE))]
    pub async fn new_remote<C: Connection + Send + Sync + 'static>(
        conn: &mut C,
        key: &RecordKey,
    ) -> Result<Self> {
        let record = StablePublicRecord::new_remote(conn, key).await?;
        Ok(Self {
            record,
            peers: HashMap::new(),
        })
    }

    pub fn key(&self) -> &RecordKey {
        self.record.dht_rec.ref_key()
    }

    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }

    #[instrument(skip_all, ret(level = Level::TRACE))]
    pub async fn load_peers<C: Connection + Send + Sync + 'static>(
        &mut self,
        conn: &mut C,
    ) -> Result<()> {
        self.peers = StablePublicRecord::read_keys(
            conn,
            self.record.key(),
            ValueSubkeyRangeSet::single_range(0, Self::MAX_PEERS.into()),
        )
        .await?
        .iter()
        // Skip empty records (were erased)
        .filter(|buf| !buf.is_empty())
        // Parse the rest
        .map(|buf| PeerInfo::decode(buf))
        // Drop the ones that failed
        .filter(|res| res.is_ok())
        // Safe to unwrap the rest
        .enumerate()
        .map(|(subkey, res)| {
            let peer_info = res.unwrap();
            (peer_info.key().clone(), (subkey, peer_info))
        })
        .collect();
        Ok(())
    }

    pub fn get_peer_info(&self, key: &RecordKey) -> Option<PeerInfo> {
        self.peers.get(key).map(|item| item.1.clone())
    }

    pub fn has_peer(&self, key: &RecordKey) -> bool {
        self.peers.contains_key(key)
    }

    pub fn known_peers(&self) -> Iter<'_, RecordKey, (usize, PeerInfo)> {
        self.peers.iter()
    }

    #[instrument(skip(self, conn), ret(level = Level::TRACE))]
    pub async fn update_peer<C: Connection + Send + Sync + 'static>(
        &mut self,
        conn: &mut C,
        key: &RecordKey,
    ) -> Result<usize> {
        let (subkey, data) = match self.peers.get_mut(key) {
            Some((subkey, peer_info)) => {
                peer_info.updated_at = Timestamp::now();
                (*subkey, peer_info.encode()?)
            }
            None => {
                let subkey = self.peers.len();
                let peer_info = PeerInfo::new(key.clone());
                let data = peer_info.encode()?;
                self.peers.insert(key.clone(), (subkey, peer_info));
                (subkey, data)
            }
        };
        self.record
            .write(
                conn,
                ValueSubkeyRangeSet::single(subkey.try_into()?),
                data.as_slice(),
            )
            .await?;
        Ok(subkey)
    }

    #[instrument(skip(self, conn), ret(level = Level::TRACE))]
    pub async fn remove_peer<C: Connection + Send + Sync + 'static>(
        &mut self,
        conn: &mut C,
        key: &RecordKey,
    ) -> Result<()> {
        if let Some((subkey, _)) = self.peers.remove(key) {
            self.record
                .write(conn, ValueSubkeyRangeSet::single(subkey.try_into()?), &[])
                .await?;
        }
        Ok(())
    }

    #[instrument(skip_all, ret(level = Level::TRACE))]
    pub async fn save_peers<C: Connection + Send + Sync + 'static>(
        &mut self,
        conn: &mut C,
    ) -> Result<()> {
        StablePublicRecord::write_keys(
            conn,
            self.record.key(),
            ValueSubkeyRangeSet::single_range(0, Self::MAX_PEERS.into()),
            self.peers
                .values()
                .map(|(_, peer_info)| peer_info.encode().unwrap_or(vec![]))
                .collect(),
        )
        .await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct StableHaveMap {
    record: StablePublicRecord,
    piece_map: PieceMap,
    dirty: bool,
    subkeys: u16,
}

impl StableHaveMap {
    #[instrument(skip_all, fields(table_name, db_key_id = hex::encode(index.payload().digest()), subkeys), ret(level = Level::TRACE))]
    pub async fn new_local<C: Connection + Send + Sync + 'static>(
        conn: &mut C,
        index: &Index,
    ) -> Result<Self> {
        let db_key_id = index.payload().digest();
        let subkeys = PieceMap::subkeys(index.payload().pieces().len());
        Ok(Self {
            record: StablePublicRecord::new_local(
                conn,
                "stigmerge_have_map_dht",
                db_key_id,
                subkeys,
            )
            .await?,
            piece_map: PieceMap::new(),
            subkeys,
            dirty: false,
        })
    }

    #[instrument(skip(conn, index), ret(level = Level::TRACE), err)]
    pub async fn read_remote<C: Connection + Send + Sync + 'static>(
        conn: &mut C,
        key: &RecordKey,
        index: &Index,
    ) -> Result<PieceMap> {
        let record = StablePublicRecord::new_remote(conn, key).await?;
        let subkeys = PieceMap::subkeys(index.payload().pieces().len());
        let piece_map_bytes = record
            .read(
                conn,
                ValueSubkeyRangeSet::single_range(0, (subkeys - 1).into()),
            )
            .await?;
        Ok(PieceMap::from(piece_map_bytes.as_slice()))
    }

    pub fn key(&self) -> &RecordKey {
        self.record.key()
    }

    pub fn subkeys(&self) -> u16 {
        self.subkeys
    }

    pub fn piece_map(&self) -> &PieceMap {
        &self.piece_map
    }

    pub fn has_piece(&self, piece_index: u32) -> bool {
        self.piece_map.get(piece_index)
    }

    #[instrument(skip_all, ret(level = Level::TRACE), err)]
    pub async fn update_piece(&mut self, piece_index: u32, have: bool) -> Result<()> {
        if self.record.owner_keypair.is_none() {
            return Err(Error::msg("dht record is not locally owned"));
        }

        if have {
            self.piece_map.set(piece_index);
        } else {
            self.piece_map.clear(piece_index);
        }

        self.dirty = true;
        Ok(())
    }

    #[instrument(skip_all, ret(level = Level::TRACE), err)]
    pub async fn sync<C: Connection + Send + Sync + 'static>(
        &mut self,
        conn: &mut C,
    ) -> Result<()> {
        if !self.dirty {
            return Ok(());
        }
        let piece_map_len = self.piece_map.as_ref().len();
        let max_subkey = piece_map_len / ValueData::MAX_LEN
            + if piece_map_len % ValueData::MAX_LEN > 0 {
                1
            } else {
                0
            };
        self.record
            .write(
                conn,
                ValueSubkeyRangeSet::single_range(
                    0,
                    TryInto::<u32>::try_into(max_subkey - 1).unwrap(),
                ),
                self.piece_map.as_ref(),
            )
            .await
    }
}
