use std::{collections::HashMap, time::Duration};

use moka::future::Cache;
use veilid_core::Target;

use crate::{error::is_route_invalid, node::TypedKey, types::FileBlockFetch, Error, Result};

#[derive(Debug, Clone)]
struct PeerStatus {
    fetch_ok_count: u32,
    fetch_err_count: u32,
    is_invalid_target: bool,
}

impl PeerStatus {
    fn score(&self) -> i32 {
        if self.is_invalid_target {
            i32::MIN
        } else {
            TryInto::<i32>::try_into(self.fetch_ok_count).unwrap()
                - TryInto::<i32>::try_into(self.fetch_err_count).unwrap()
        }
    }
}

impl Default for PeerStatus {
    fn default() -> Self {
        PeerStatus {
            fetch_ok_count: 0,
            fetch_err_count: 0,
            is_invalid_target: false,
        }
    }
}

pub struct PeerTracker {
    targets: HashMap<TypedKey, Target>,
    peer_status: Cache<TypedKey, PeerStatus>,
}

const MAX_TRACKED_PEERS: u64 = 64;

impl PeerTracker {
    pub fn new() -> Self {
        PeerTracker {
            targets: HashMap::new(),
            peer_status: Cache::builder()
                .time_to_idle(Duration::from_secs(60))
                .max_capacity(MAX_TRACKED_PEERS)
                .build(),
        }
    }

    pub async fn update(&mut self, key: TypedKey, target: Target) -> Option<Target> {
        self.reset(&key).await;
        self.targets.insert(key, target)
    }

    pub fn contains(&self, key: &TypedKey) -> bool {
        self.targets.contains_key(key)
    }

    pub async fn fetch_ok(&mut self, key: &TypedKey) {
        let mut status = match self.peer_status.get(&key).await {
            Some(status) => status,
            None => PeerStatus::default(),
        };
        status.fetch_err_count = 0;
        status.is_invalid_target = false;
        status.fetch_ok_count += 1;
        self.peer_status.insert(key.clone(), status).await;
    }

    pub async fn fetch_err(&mut self, key: &TypedKey, err: Error) -> Option<Target> {
        let mut status = match self.peer_status.get(&key).await {
            Some(status) => status,
            None => PeerStatus::default(),
        };
        let is_invalid_target = is_route_invalid(&err);
        let is_newly_invalid = status.is_invalid_target != is_invalid_target;

        status.fetch_err_count += 1;
        status.is_invalid_target = is_invalid_target;
        self.peer_status.insert(key.clone(), status).await;

        if is_newly_invalid {
            self.targets.get(key).map(|target| target.to_owned())
        } else {
            None
        }
    }

    pub async fn reset(&mut self, key: &TypedKey) {
        self.peer_status.remove(key).await;
    }

    pub fn share_target(&self, _block: &FileBlockFetch) -> Result<Option<(&TypedKey, &Target)>> {
        // TODO: factor in have_map and block
        let mut peers: Vec<(TypedKey, PeerStatus)> = self
            .peer_status
            .iter()
            .map(|(key, status)| (*key, status))
            .collect();
        peers.sort_by(|(_, l_status), (_, r_status)| r_status.score().cmp(&l_status.score()));
        if !peers.is_empty() {
            if peers[0].1.score() < -1024 {
                return Err(Error::msg("peer score dropped below minimum threshold"));
            }
            return Ok(self.targets.get_key_value(&peers[0].0));
        }
        if self.targets.is_empty() {
            Ok(None)
        } else {
            Ok(self
                .targets
                .iter()
                .nth(rand::random::<usize>() % self.targets.len()))
        }
    }
}
