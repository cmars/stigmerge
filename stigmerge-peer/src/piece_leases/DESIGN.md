# Piece Lease Manager Design

## Overview

The Piece Lease Manager is a centralized coordination system for managing piece-level leases in the stigmerge fetcher. It replaces the previous block-based channel system (`pending_blocks_tx/pending_blocks_rx`) with a more sophisticated approach that provides exclusive access to pieces, peer ranking, and better failure handling.

## Problem Statement

The current fetcher architecture has several limitations:

1. **No piece-level coordination**: Individual blocks are distributed without considering piece boundaries, leading to potential race conditions
2. **No lease management**: Multiple fetch pools could work on the same piece simultaneously
3. **No peer ranking**: All peers are treated equally regardless of performance
4. **Inefficient retry logic**: Failed pieces are re-queued at block level, causing potential thrashing
5. **Lack of exclusivity**: No mechanism to prevent conflicting writes to the same piece

## Solution Architecture

### Core Components

#### PieceLeaseManager
The central coordinator that manages piece leases and peer rankings.

```rust
pub struct PieceLeaseManager {
    inner: Arc<Mutex<PieceLeaseManagerInner>>,
}

struct PieceLeaseManagerInner {
    /// Configuration for the lease manager
    config: LeaseManagerConfig,
    
    /// Pieces that still need to be downloaded
    wanted_pieces: PieceMap,
    
    /// Currently active leases by (file_index, piece_index)
    active_leases: HashMap<(usize, usize), PieceLease>,
    
    /// Peer performance rankings
    peer_rankings: HashMap<TypedRecordKey, PeerRanking>,
    
    /// Counter for generating unique request IDs
    request_id_counter: u64,
}
```

#### PieceLease
Represents exclusive rights to fetch blocks for a specific piece.

```rust
pub struct PieceLease {
    pub piece_index: usize,
    pub file_index: usize,
    pub lease_expiry: Instant,
    pub peer_key: TypedRecordKey,
    pub granted_at: Instant,
}

impl PieceLease {
    pub fn key(&self) -> (usize, usize) {
        (self.file_index, self.piece_index)
    }
    
    pub fn is_expired(&self) -> bool {
        Instant::now() > self.lease_expiry
    }
    
    pub fn time_remaining(&self) -> Duration {
        self.lease_expiry.saturating_duration_since(Instant::now())
    }
}
```

#### PeerRanking
Tracks peer performance and assigns priority scores.

```rust
pub struct PeerRanking {
    pub key: TypedRecordKey,
    pub successful_pieces: u32,
    pub failed_pieces: u32,
    pub current_leases: u32,
    pub score: f64,
    pub last_updated: Instant,
}

impl PeerRanking {
    pub fn new(key: TypedRecordKey) -> Self {
        Self {
            key,
            successful_pieces: 0,
            failed_pieces: 0,
            current_leases: 0,
            score: 1.0, // Start with neutral score
            last_updated: Instant::now(),
        }
    }
    
    pub fn record_success(&mut self, config: &LeaseManagerConfig) {
        self.successful_pieces += 1;
        self.update_score(config);
    }
    
    pub fn record_failure(&mut self, config: &LeaseManagerConfig) {
        self.failed_pieces += 1;
        self.update_score(config);
    }
    
    fn update_score(&mut self, config: &LeaseManagerConfig) {
        let success_rate = if self.successful_pieces + self.failed_pieces > 0 {
            self.successful_pieces as f64 / (self.successful_pieces + self.failed_pieces) as f64
        } else {
            1.0
        };
        
        // Apply exponential decay to old scores
        let time_factor = config.peer_ranking_decay.powf(
            self.last_updated.elapsed().as_secs_f64() / 3600.0 // hours
        );
        
        self.score = self.score * time_factor 
                   + (success_rate * config.success_bonus 
                      - (1.0 - success_rate) * config.failure_penalty);
        
        // Clamp score to reasonable bounds
        self.score = self.score.clamp(0.1, 10.0);
        self.last_updated = Instant::now();
    }
}
```

### Communication Protocols

#### Lease Request
```rust
pub struct LeaseRequest {
    pub peer_key: TypedRecordKey,
    pub have_map: PieceMap,
    pub max_leases: usize,
    pub requested_at: Instant,
}

impl LeaseRequest {
    pub fn can_fulfill(&self, wanted_pieces: &PieceMap) -> PieceMap {
        wanted_pieces.intersection(self.have_map.clone())
    }
}
```

#### Lease Response
```rust
pub struct LeaseResponse {
    pub request_id: u64,
    pub peer_key: TypedRecordKey,
    pub granted_leases: Vec<PieceLease>,
    pub rejected_reason: Option<RejectedReason>,
}

pub enum RejectedReason {
    NoAvailablePieces,
    PeerAtCapacity,
    PeerRankingTooLow,
    InvalidRequest,
}
```

#### Piece Completion
```rust
pub struct PieceCompletion {
    pub piece_index: usize,
    pub file_index: usize,
    pub peer_key: TypedRecordKey,
    pub result: CompletionResult,
    pub completed_at: Instant,
}

pub enum CompletionResult {
    Success,
    Failure(FailureReason),
    Expired,
}

pub enum FailureReason {
    VerificationFailed,
    NetworkError,
    Timeout,
    InvalidData,
}
```

### Configuration

```rust
pub struct LeaseManagerConfig {
    /// Duration for which a lease is valid
    pub lease_duration: Duration,
    
    /// Maximum number of concurrent leases per peer
    pub max_leases_per_peer: usize,
    
    /// Exponential decay factor for peer rankings (per hour)
    pub peer_ranking_decay: f64,
    
    /// Bonus applied to ranking for successful pieces
    pub success_bonus: f64,
    
    /// Penalty applied to ranking for failed pieces
    pub failure_penalty: f64,
    
    /// Interval for cleanup of expired leases
    pub cleanup_interval: Duration,
    
    /// Minimum peer score to be considered for leases
    pub min_peer_score: f64,
}

impl Default for LeaseManagerConfig {
    fn default() -> Self {
        Self {
            lease_duration: Duration::from_secs(300), // 5 minutes
            max_leases_per_peer: 4,
            peer_ranking_decay: 0.95,
            success_bonus: 0.5,
            failure_penalty: 1.0,
            cleanup_interval: Duration::from_secs(60),
            min_peer_score: 0.2,
        }
    }
}
```

## Lease Assignment Algorithm

### 1. Request Processing
1. Validate lease request (peer not at capacity, minimum score met)
2. Calculate available pieces by intersecting peer's have_map with wanted pieces
3. If no available pieces, return rejection

### 2. Peer Selection
1. Sort available peers by ranking score (descending)
2. For highest-ranking peers, allocate pieces up to their limits
3. Consider piece distribution to avoid concentration

### 3. Piece Selection Strategy
```rust
fn select_pieces_for_peer(
    available_pieces: &PieceMap,
    peer_ranking: &PeerRanking,
    max_leases: usize,
    active_leases: &HashMap<(usize, usize), PieceLease>
) -> Vec<usize> {
    // Strategy 1: Prefer pieces that no one else is working on
    // Strategy 2: Consider peer's historical success with specific piece ranges
    // Strategy 3: Round-robin to ensure fair distribution
    
    available_pieces
        .iter()
        .filter(|&piece_index| {
            !active_leases.contains_key(&(0, piece_index)) // TODO: support file_index
        })
        .take(max_leases)
        .collect()
}
```

### 4. Lease Creation
1. Create `PieceLease` objects with expiry times
2. Update peer's current lease count
3. Store in active leases map
4. Return lease response

## Lifecycle Management

### Lease Expiry
- Expired leases are cleaned up inline when new lease requests are processed
- Expired leases are removed and pieces returned to wanted set
- Peer rankings are penalized for expired leases
- No background task needed - cleanup happens on-demand

### Piece Completion
- Successful completion: piece removed from wanted set, peer ranking increased
- Failed completion: piece returned to wanted set, peer ranking decreased
- Invalid completion (no lease): error returned

### Inline Cleanup
```rust
impl PieceLeaseManagerInner {
    /// Clean up expired leases and return them to the wanted set
    fn cleanup_expired_leases(&mut self) {
        let now = Instant::now();
        let expired_leases: Vec<_> = self.active_leases
            .iter()
            .filter(|(_, lease)| lease.lease_expiry <= now)
            .map(|(key, lease)| (*key, lease.clone()))
            .collect();
        
        if expired_leases.is_empty() {
            return;
        }
        
        info!("Cleaning up {} expired leases", expired_leases.len());
        
        for (piece_key, lease) in expired_leases {
            // Remove expired lease
            self.active_leases.remove(&piece_key);
            
            // Return piece to wanted set
            self.wanted_pieces.set(lease.piece_index as u32);
            
            // Penalize peer
            if let Some(ranking) = self.peer_rankings.get_mut(&lease.peer_key) {
                ranking.current_leases = ranking.current_leases.saturating_sub(1);
                ranking.record_expiry(&self.config);
                debug!("Penalized peer {:?} for expired lease, new score: {}", 
                       lease.peer_key, ranking.score);
            }
        }
    }
}
```

## Integration Points

### With Fetcher
```rust
// In Fetcher::new()
let lease_manager = PieceLeaseManager::new(LeaseManagerConfig::default());

// Replace pending_blocks channel
// OLD:
// let (pending_blocks_tx, pending_blocks_rx) = flume::unbounded();

// NEW:
let lease_manager = Arc::new(lease_manager);
```

### With FetchPool
```rust
impl FetchPool {
    async fn request_leases(&self) -> Result<Vec<PieceLease>> {
        let request = LeaseRequest {
            peer_key: self.remote_share.key,
            have_map: self.remote_share.have_map.clone(),
            max_leases: Self::MAX_LEASES,
            requested_at: Instant::now(),
        };
        
        let response = self.lease_manager.request_lease(request).await?;
        
        match response.granted_leases.len() {
            0 => warn!("No leases granted: {:?}", response.rejected_reason),
            n => info!("Granted {} leases to peer {:?}", n, self.remote_share.key),
        }
        
        Ok(response.granted_leases)
    }
}
```

### With PieceVerifier
```rust
// In piece verification completion handler
async fn handle_piece_verification(&self, status: PieceStatus) -> Result<()> {
    match status {
        PieceStatus::ValidPiece { file_index, piece_index, .. } => {
            let completion = PieceCompletion {
                piece_index,
                file_index,
                peer_key: self.current_lease_peer_key, // Track which peer had the lease
                result: CompletionResult::Success,
                completed_at: Instant::now(),
            };
            
            self.lease_manager.complete_piece(completion).await?;
        }
        PieceStatus::InvalidPiece { file_index, piece_index } => {
            let completion = PieceCompletion {
                piece_index,
                file_index,
                peer_key: self.current_lease_peer_key,
                result: CompletionResult::Failure(FailureReason::VerificationFailed),
                completed_at: Instant::now(),
            };
            
            self.lease_manager.complete_piece(completion).await?;
        }
        PieceStatus::IncompletePiece { .. } => {
            // Lease remains active, continue fetching
        }
    }
    
    Ok(())
}
```

## Performance Considerations

### Computational Complexity
- **Lease Assignment**: O(P + L) where P = available pieces, L = current leases
- **Peer Ranking Updates**: O(1) per update
- **Cleanup Operations**: O(L) per cleanup interval
- **Memory Usage**: O(P + N) where N = number of peers

### Optimizations
1. **Batch Operations**: Process multiple lease requests in batches
2. **Lazy Ranking Updates**: Only recalculate scores when needed
3. **Efficient Bitmap Operations**: Use PieceMap's optimized intersection
4. **Peer Caching**: Cache frequently accessed peer rankings

### Scalability
- Supports thousands of concurrent pieces
- Handles hundreds of simultaneous peers
- Efficient memory usage through bitmap operations
- Asynchronous processing prevents blocking

## Testing Strategy

### Unit Tests
- Lease assignment and expiry logic
- Peer ranking calculations
- Piece selection algorithms
- Configuration validation

### Integration Tests
- End-to-end lease lifecycle
- Multi-peer coordination
- Failure recovery scenarios
- Performance under load

### Property-Based Tests
- Lease exclusivity invariants
- Peer ranking monotonicity
- Piece set conservation
- Configuration parameter bounds

## Migration Path

### Phase 1: Standalone Implementation
- Implement PieceLeaseManager as independent component
- Comprehensive test coverage
- Performance benchmarking

### Phase 2: Parallel Integration
- Run lease manager alongside existing block channel
- Gradual migration of FetchPool instances
- A/B testing of performance

### Phase 3: Full Replacement
- Remove pending_blocks channels entirely
- Update all fetcher components to use leases
- Performance optimization and tuning

## Benefits

### Immediate
- Eliminates race conditions for piece access
- Better peer selection based on performance
- Cleaner separation of concerns

### Long-term
- More efficient resource utilization
- Better handling of unreliable peers
- Foundation for advanced strategies (e.g., piece rarity, peer specialization)

### Operational
- Reduced network overhead through better coordination
- Improved download reliability
- Better metrics and observability

## Future Enhancements

### Advanced Peer Selection
- Piece rarity-based prioritization
- Network topology awareness
- Bandwidth capacity estimation

### Adaptive Strategies
- Dynamic lease duration based on peer performance
- Machine learning for peer reputation
- Predictive piece selection

### Monitoring & Observability
- Detailed metrics collection
- Performance dashboards
- Automated alerting for anomalous behavior
