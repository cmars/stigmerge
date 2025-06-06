use std::collections::{hash_map::Keys, HashMap};

use moka::future::Cache;
use tracing::debug;
use veilid_core::{Target, TypedRecordKey};

use crate::{error::is_route_invalid, types::FileBlockFetch, Error, Result};

#[derive(Debug, Clone)]
struct PeerStatus {
    fetch_ok_count: u32,
    fetch_err_count: u32,
}

impl PeerStatus {
    fn score(&self) -> i32 {
        TryInto::<i32>::try_into(self.fetch_ok_count).unwrap()
            - TryInto::<i32>::try_into(self.fetch_err_count).unwrap()
    }
}

impl Default for PeerStatus {
    fn default() -> Self {
        PeerStatus {
            fetch_ok_count: 0,
            fetch_err_count: 0,
        }
    }
}

pub struct PeerTracker {
    targets: HashMap<TypedRecordKey, Target>,
    peer_status: Cache<TypedRecordKey, PeerStatus>,
}

const MAX_TRACKED_PEERS: u64 = 64;

impl PeerTracker {
    pub fn new() -> Self {
        PeerTracker {
            targets: HashMap::new(),
            peer_status: Cache::builder().max_capacity(MAX_TRACKED_PEERS).build(),
        }
    }

    pub fn keys(&self) -> Keys<'_, TypedRecordKey, Target> {
        self.targets.keys()
    }

    pub async fn update(&mut self, key: TypedRecordKey, target: Target) -> Option<Target> {
        if !self.peer_status.contains_key(&key) {
            self.peer_status
                .insert(key.clone(), PeerStatus::default())
                .await;
        }
        self.targets.insert(key, target)
    }

    pub fn contains(&self, key: &TypedRecordKey) -> bool {
        self.targets.contains_key(key)
    }

    pub async fn fetch_ok(&mut self, key: &TypedRecordKey) {
        let mut status = match self.peer_status.get(&key).await {
            Some(status) => status,
            None => PeerStatus::default(),
        };
        status.fetch_err_count = 0;
        status.fetch_ok_count += 1;
        self.peer_status.insert(key.clone(), status).await;
    }

    pub async fn fetch_err(&mut self, key: &TypedRecordKey, err: &Error) {
        let mut status = match self.peer_status.get(&key).await {
            Some(status) => status,
            None => PeerStatus::default(),
        };
        status.fetch_err_count += if is_route_invalid(&err) { 10 } else { 1 };
        self.peer_status.insert(key.clone(), status).await;
    }

    pub async fn share_target(
        &self,
        _block: &FileBlockFetch,
    ) -> Result<Option<(&TypedRecordKey, &Target)>> {
        // TODO: factor in have_map and block
        let mut peers: Vec<(TypedRecordKey, PeerStatus)> = self
            .peer_status
            .iter()
            .map(|(key, status)| (*key, status))
            .collect();
        peers.sort_by(|(_, l_status), (_, r_status)| r_status.score().cmp(&l_status.score()));
        if !peers.is_empty() {
            return Ok(self.targets.get_key_value(&peers[0].0));
        }
        if self.targets.is_empty() {
            Ok(None)
        } else {
            let (share_key, target) = self
                .targets
                .iter()
                .nth(rand::random::<usize>() % self.targets.len())
                .unwrap();
            debug!("new status for peer key {share_key}");
            self.peer_status
                .insert(share_key.clone(), PeerStatus::default())
                .await;
            Ok(Some((share_key, target)))
        }
    }
}

#[cfg(test)]
mod tests {
    use veilid_core::{CryptoKind, RecordKey, RouteId};

    use super::*;

    // Helper function to create a TypedRecordKey for testing
    fn create_typed_key(id: u8) -> TypedRecordKey {
        let mut key_bytes = [0u8; 32];
        key_bytes[0] = id;
        TypedRecordKey::new(CryptoKind::default(), RecordKey::from(key_bytes))
    }

    // Helper function to create a Target for testing
    fn create_target(id: u8) -> Target {
        let mut key_bytes = [0u8; 32];
        key_bytes[0] = id;
        Target::PrivateRoute(RouteId::new(key_bytes))
    }

    #[tokio::test]
    async fn test_new_peer_tracker() {
        let tracker = PeerTracker::new();
        assert!(tracker.targets.is_empty());
    }

    #[tokio::test]
    async fn test_update_peer() {
        let mut tracker = PeerTracker::new();
        let key = create_typed_key(1);
        let target = create_target(1);

        // Update should return None for a new peer
        let previous = tracker.update(key.clone(), target.clone()).await;
        assert!(previous.is_none());

        // Tracker should now contain the peer
        assert!(tracker.contains(&key));

        // Update with a new target should return the old target
        let new_target = create_target(2);
        let previous = tracker.update(key.clone(), new_target.clone()).await;
        assert_eq!(previous.unwrap(), target);
    }

    #[tokio::test]
    async fn test_peer_ranking() {
        let mut tracker = PeerTracker::new();
        let key1 = create_typed_key(1);
        let key2 = create_typed_key(2);
        let target1 = create_target(1);
        let target2 = create_target(2);

        // Add two peers
        tracker.update(key1.clone(), target1.clone()).await;
        tracker.update(key2.clone(), target2.clone()).await;

        // Initially both peers have the same score
        let block = FileBlockFetch {
            file_index: 0,
            piece_index: 0,
            piece_offset: 0,
            block_index: 0,
        };

        // Increase score for key1
        tracker.fetch_ok(&key1).await;
        tracker.fetch_ok(&key1).await;

        // key1 should now be ranked higher
        let result = tracker.share_target(&block).await.unwrap();
        assert!(result.is_some());
        let (share_key, _) = result.unwrap();
        assert_eq!(share_key, &key1);
    }

    #[tokio::test]
    async fn test_update_peer_route() {
        let mut tracker = PeerTracker::new();
        let key = create_typed_key(1);
        let target1 = create_target(1);

        // Add a peer
        tracker.update(key.clone(), target1.clone()).await;

        // Update the peer's route
        let target2 = create_target(2);
        tracker.update(key.clone(), target2.clone()).await;

        // The share_target should now return the updated route
        let block = FileBlockFetch {
            file_index: 0,
            piece_index: 0,
            piece_offset: 0,
            block_index: 0,
        };

        let share_target = tracker.share_target(&block).await.unwrap();
        assert!(share_target.is_some());
        let (share_key, share_target) = share_target.unwrap();
        assert_eq!(share_key, &key);
        assert_eq!(share_target, &target2);
    }
}
