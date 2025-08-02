use std::collections::{hash_map::Keys, HashMap};

use tracing::trace;
use veilid_core::{Target, TypedRecordKey};

use crate::{error::is_route_invalid, types::FileBlockFetch, Error, Result};

#[derive(Debug, Clone)]
pub struct PeerStatus {
    fetch_ok_count: u32,
    fetch_err_count: u32,
}

impl PeerStatus {
    pub fn score(&self) -> i32 {
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
    peer_status: HashMap<TypedRecordKey, PeerStatus>,
}

const MINIMUM_PEER_SCORE: i32 = -100;

impl PeerTracker {
    pub fn new() -> Self {
        PeerTracker {
            targets: HashMap::new(),
            peer_status: HashMap::new(),
        }
    }

    pub fn keys(&self) -> Keys<'_, TypedRecordKey, Target> {
        self.targets.keys()
    }

    pub fn update(&mut self, key: TypedRecordKey, target: Target) -> Option<Target> {
        if !self.peer_status.contains_key(&key) {
            self.peer_status.insert(key.clone(), PeerStatus::default());
        }
        self.targets.insert(key, target)
    }

    #[cfg(test)]
    pub fn contains(&self, key: &TypedRecordKey) -> bool {
        self.targets.contains_key(key)
    }

    pub fn fetch_ok(&mut self, key: &TypedRecordKey) {
        if !self.peer_status.contains_key(&key) {
            self.peer_status.insert(key.clone(), PeerStatus::default());
        }
        let status = self.peer_status.get_mut(&key).unwrap();
        status.fetch_err_count = 0;
        status.fetch_ok_count += 1;
    }

    pub fn fetch_err(&mut self, key: &TypedRecordKey, err: &Error) {
        if !self.peer_status.contains_key(&key) {
            self.peer_status.insert(key.clone(), PeerStatus::default());
        }
        let status = self.peer_status.get_mut(&key).unwrap();
        status.fetch_ok_count = 0;
        status.fetch_err_count += if is_route_invalid(&err) { 10 } else { 1 };
    }

    pub fn status(&self, key: &TypedRecordKey) -> Option<PeerStatus> {
        self.peer_status.get(&key).map(|st| st.to_owned())
    }

    pub fn share_target(
        &mut self,
        _block: &FileBlockFetch,
    ) -> Result<Option<(&TypedRecordKey, &Target)>> {
        // TODO: factor in have_map and block
        let mut peers: Vec<(TypedRecordKey, PeerStatus)> = self
            .peer_status
            .iter()
            .filter(|(_, status)| status.score() >= MINIMUM_PEER_SCORE)
            .map(|(key, status)| (key.to_owned(), status.to_owned()))
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
            trace!("new status for peer key {share_key}");
            Ok(Some((share_key, target)))
        }
    }
}

#[cfg(test)]
mod tests {
    use veilid_core::{CryptoKind, RecordKey, RouteId, VeilidAPIError};

    use super::*;

    // Helper function to create a TypedRecordKey for testing
    fn create_typed_key(id: u8) -> TypedRecordKey {
        let mut key_bytes = [0u8; 32];
        key_bytes[0] = id;
        TypedRecordKey::new(CryptoKind::default(), RecordKey::new(key_bytes))
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
        let previous = tracker.update(key.clone(), target.clone());
        assert!(previous.is_none());

        // Tracker should now contain the peer
        assert!(tracker.contains(&key));

        // Update with a new target should return the old target
        let new_target = create_target(2);
        let previous = tracker.update(key.clone(), new_target.clone());
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
        tracker.update(key1.clone(), target1.clone());
        tracker.update(key2.clone(), target2.clone());

        // Initially both peers have the same score
        let block = FileBlockFetch {
            file_index: 0,
            piece_index: 0,
            piece_offset: 0,
            block_index: 0,
        };

        // Increase score for key1
        tracker.fetch_ok(&key1);
        tracker.fetch_ok(&key1);

        // key1 should now be ranked higher
        let result = tracker.share_target(&block).unwrap();
        assert!(result.is_some());
        let (share_key, _) = result.unwrap();
        assert_eq!(share_key, &key1);
    }

    #[tokio::test]
    async fn test_err_clears_ok() {
        let mut tracker = PeerTracker::new();
        let key1 = create_typed_key(1);
        let target1 = create_target(1);

        tracker.update(key1.clone(), target1.clone());

        // Route error
        tracker.fetch_err(
            &key1,
            &VeilidAPIError::InvalidTarget {
                message: "nope".to_string(),
            }
            .into(),
        );
        // Route errors are more heavily penalized
        assert_eq!(tracker.status(&key1).unwrap().score(), -10);

        // Increase score for key1
        tracker.fetch_ok(&key1);
        tracker.fetch_ok(&key1);
        // oks are counted
        assert_eq!(tracker.status(&key1).unwrap().score(), 2);

        tracker.fetch_err(&key1, &Error::msg("nope"));

        // error wipes out oks
        assert_eq!(tracker.status(&key1).unwrap().score(), -1);
    }

    #[tokio::test]
    async fn test_update_peer_route() {
        let mut tracker = PeerTracker::new();
        let key = create_typed_key(1);
        let target1 = create_target(1);

        // Add a peer
        tracker.update(key.clone(), target1.clone());

        // Update the peer's route
        let target2 = create_target(2);
        tracker.update(key.clone(), target2.clone());

        // The share_target should now return the updated route
        let block = FileBlockFetch {
            file_index: 0,
            piece_index: 0,
            piece_offset: 0,
            block_index: 0,
        };

        let share_target = tracker.share_target(&block).unwrap();
        assert!(share_target.is_some());
        let (share_key, share_target) = share_target.unwrap();
        assert_eq!(share_key, &key);
        assert_eq!(share_target, &target2);
    }
}
