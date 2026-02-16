# Edge-First Architecture

## Summary

Leverage Streamline's single-binary advantage to create the first streaming platform designed for edge computing. Enable deployment on resource-constrained devices (Raspberry Pi, IoT gateways, vehicles, retail stores) with automatic edge-to-cloud synchronization, offline-first operation, and intelligent conflict resolution.

## Current State

- Single binary architecture already exists (~8-15MB)
- No explicit edge optimization
- No edge-to-cloud federation
- No offline-first mode
- No conflict resolution for distributed writes
- No mesh networking between edge nodes
- WASM transforms exist (useful for edge processing)

## Requirements

### Core Edge Features

| Requirement | Priority | Description |
|-------------|----------|-------------|
| Minimal Binary | P0 | <5MB edge binary with core features only |
| Offline-First | P0 | Full operation without cloud connectivity |
| Edge-Cloud Sync | P0 | Automatic synchronization when connected |
| Conflict Resolution | P0 | Handle concurrent writes at edge and cloud |
| Resource Limits | P1 | CPU/memory/storage caps for constrained devices |
| Edge Mesh | P1 | Peer-to-peer sync between edge nodes |
| Store-and-Forward | P1 | Queue data during disconnection |
| Compression | P1 | Aggressive data compression for sync |
| Delta Sync | P2 | Only sync changed data |
| Encryption | P2 | End-to-end encryption edge-to-cloud |

### Target Environments

| Environment | Memory | Storage | CPU | Network |
|-------------|--------|---------|-----|---------|
| Raspberry Pi Zero | 512MB | 8GB SD | 1 core | WiFi |
| Raspberry Pi 4 | 2-8GB | 32GB+ | 4 cores | Ethernet/WiFi |
| IoT Gateway | 256MB | 4GB | ARM | LTE/WiFi |
| Vehicle ECU | 128MB | 1GB | ARM | Cellular |
| Retail POS | 4GB | 128GB SSD | x86 | Ethernet |
| Industrial Edge | 8GB | 256GB | x86 | Ethernet |

### Data Structures

Create `src/edge/mod.rs`:

```rust
//! Edge-First Architecture Module
//!
//! Enables Streamline deployment on resource-constrained edge devices
//! with offline-first operation and cloud synchronization.

pub mod config;
pub mod sync;
pub mod conflict;
pub mod mesh;
pub mod store_forward;
pub mod resources;

/// Edge node configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeConfig {
    /// Unique edge node identifier
    pub node_id: String,
    /// Node location (for geo-routing)
    pub location: Option<GeoLocation>,
    /// Cloud endpoint for sync
    pub cloud_endpoint: Option<String>,
    /// Sync configuration
    pub sync: SyncConfig,
    /// Resource limits
    pub resources: ResourceLimits,
    /// Conflict resolution strategy
    pub conflict_resolution: ConflictStrategy,
    /// Edge mesh configuration
    pub mesh: Option<MeshConfig>,
    /// Topics to sync to cloud
    pub sync_topics: Vec<TopicSyncConfig>,
    /// Local-only topics (never sync)
    pub local_topics: Vec<String>,
}

/// Sync configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncConfig {
    /// Enable automatic sync
    pub enabled: bool,
    /// Sync interval (when connected)
    pub interval_secs: u64,
    /// Batch size for sync
    pub batch_size: usize,
    /// Compression for sync
    pub compression: CompressionMode,
    /// Max queue size before backpressure
    pub max_queue_size: usize,
    /// Retry policy
    pub retry: RetryPolicy,
    /// Bandwidth limit (bytes/sec, 0 = unlimited)
    pub bandwidth_limit: u64,
}

/// Resource limits for constrained devices
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceLimits {
    /// Max memory usage (bytes)
    pub max_memory: usize,
    /// Max storage usage (bytes)
    pub max_storage: usize,
    /// Max CPU percentage (0-100)
    pub max_cpu_percent: u8,
    /// Max open file descriptors
    pub max_file_descriptors: usize,
    /// Max concurrent connections
    pub max_connections: usize,
    /// Storage eviction policy
    pub eviction_policy: EvictionPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EvictionPolicy {
    /// Delete oldest data first
    Oldest,
    /// Delete by topic priority
    Priority { topic_priorities: HashMap<String, u8> },
    /// Delete least recently accessed
    LRU,
    /// Compact and merge segments
    Compact,
}

/// Conflict resolution strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConflictStrategy {
    /// Last write wins (by timestamp)
    LastWriteWins,
    /// First write wins
    FirstWriteWins,
    /// Cloud always wins
    CloudWins,
    /// Edge always wins
    EdgeWins,
    /// Custom merge function (WASM)
    CustomMerge { module: String },
    /// Keep both versions
    KeepBoth,
}
```

Create `src/edge/sync.rs`:

```rust
//! Edge-Cloud Synchronization
//!
//! Handles bidirectional sync between edge nodes and cloud.

/// Sync manager for edge-cloud communication
pub struct SyncManager {
    /// Edge node configuration
    config: EdgeConfig,
    /// Cloud client
    cloud_client: Option<CloudClient>,
    /// Sync state per topic
    sync_state: DashMap<String, TopicSyncState>,
    /// Pending sync queue
    pending_queue: Arc<SyncQueue>,
    /// Connection status
    connected: AtomicBool,
    /// Metrics
    metrics: SyncMetrics,
}

/// State for each synced topic
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicSyncState {
    /// Topic name
    pub topic: String,
    /// Last synced offset (per partition)
    pub last_synced: HashMap<i32, i64>,
    /// Cloud offset (per partition)
    pub cloud_offset: HashMap<i32, i64>,
    /// Pending records count
    pub pending_count: u64,
    /// Last sync timestamp
    pub last_sync: Option<DateTime<Utc>>,
    /// Sync direction
    pub direction: SyncDirection,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum SyncDirection {
    /// Edge to cloud only
    EdgeToCloud,
    /// Cloud to edge only
    CloudToEdge,
    /// Bidirectional
    Bidirectional,
}

impl SyncManager {
    /// Start the sync manager
    pub async fn start(&self) -> Result<()>;

    /// Trigger immediate sync
    pub async fn sync_now(&self) -> Result<SyncResult>;

    /// Sync a specific topic
    pub async fn sync_topic(&self, topic: &str) -> Result<TopicSyncResult>;

    /// Handle incoming cloud data
    async fn receive_from_cloud(&self, records: Vec<Record>) -> Result<()>;

    /// Send pending data to cloud
    async fn send_to_cloud(&self, topic: &str, records: Vec<Record>) -> Result<()>;

    /// Resolve conflicts between edge and cloud
    async fn resolve_conflicts(&self, conflicts: Vec<Conflict>) -> Result<Vec<Record>>;

    /// Check connection to cloud
    pub fn is_connected(&self) -> bool;

    /// Get sync status
    pub fn status(&self) -> SyncStatus;
}

/// Sync queue for store-and-forward
pub struct SyncQueue {
    /// Queue storage (persistent)
    storage: QueueStorage,
    /// Max queue size
    max_size: usize,
    /// Current size
    current_size: AtomicUsize,
}

impl SyncQueue {
    /// Enqueue record for sync
    pub async fn enqueue(&self, topic: &str, record: Record) -> Result<()>;

    /// Dequeue batch for sync
    pub async fn dequeue(&self, batch_size: usize) -> Result<Vec<QueuedRecord>>;

    /// Acknowledge synced records
    pub async fn ack(&self, ids: &[u64]) -> Result<()>;

    /// Get queue depth
    pub fn depth(&self) -> usize;
}
```

Create `src/edge/conflict.rs`:

```rust
//! Conflict Resolution
//!
//! Handles conflicts between edge and cloud writes.

/// Conflict resolver
pub struct ConflictResolver {
    strategy: ConflictStrategy,
    /// WASM runtime for custom merge (if configured)
    wasm_runtime: Option<WasmRuntime>,
}

/// A detected conflict
#[derive(Debug, Clone)]
pub struct Conflict {
    /// Topic
    pub topic: String,
    /// Partition
    pub partition: i32,
    /// Key (conflicts on same key)
    pub key: Option<Bytes>,
    /// Edge version
    pub edge_record: Record,
    /// Cloud version
    pub cloud_record: Record,
    /// Conflict type
    pub conflict_type: ConflictType,
}

#[derive(Debug, Clone, Copy)]
pub enum ConflictType {
    /// Both wrote same key
    ConcurrentWrite,
    /// Edge updated, cloud deleted
    UpdateDelete,
    /// Edge deleted, cloud updated
    DeleteUpdate,
    /// Ordering conflict
    Ordering,
}

/// Resolution result
#[derive(Debug, Clone)]
pub enum Resolution {
    /// Use edge version
    UseEdge(Record),
    /// Use cloud version
    UseCloud(Record),
    /// Use merged version
    Merged(Record),
    /// Keep both versions
    Both(Record, Record),
    /// Drop both (resolve to delete)
    Drop,
}

impl ConflictResolver {
    /// Resolve a conflict
    pub async fn resolve(&self, conflict: Conflict) -> Result<Resolution>;

    /// Batch resolve conflicts
    pub async fn resolve_batch(&self, conflicts: Vec<Conflict>) -> Result<Vec<Resolution>>;
}
```

Create `src/edge/mesh.rs`:

```rust
//! Edge Mesh Networking
//!
//! Enables peer-to-peer synchronization between edge nodes.

/// Edge mesh manager
pub struct EdgeMesh {
    /// Local node ID
    node_id: String,
    /// Known peer nodes
    peers: DashMap<String, PeerInfo>,
    /// Active connections
    connections: DashMap<String, PeerConnection>,
    /// Discovery service
    discovery: Box<dyn PeerDiscovery>,
    /// Gossip protocol handler
    gossip: GossipProtocol,
}

/// Peer information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub node_id: String,
    pub address: SocketAddr,
    pub location: Option<GeoLocation>,
    pub last_seen: DateTime<Utc>,
    pub state: PeerState,
    pub topics: Vec<String>,
}

/// Mesh configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeshConfig {
    /// Enable mesh networking
    pub enabled: bool,
    /// Discovery method
    pub discovery: DiscoveryMethod,
    /// Max peers to connect
    pub max_peers: usize,
    /// Gossip interval
    pub gossip_interval_secs: u64,
    /// Topics to replicate in mesh
    pub replicated_topics: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DiscoveryMethod {
    /// mDNS for local network
    Mdns,
    /// Static peer list
    Static { peers: Vec<String> },
    /// DNS-SD discovery
    DnsSd { domain: String },
    /// Consul/etcd
    ServiceRegistry { endpoint: String },
}

impl EdgeMesh {
    /// Start mesh networking
    pub async fn start(&self) -> Result<()>;

    /// Discover and connect to peers
    async fn discover_peers(&self) -> Result<()>;

    /// Sync topic with peer
    async fn sync_with_peer(&self, peer: &str, topic: &str) -> Result<()>;

    /// Handle incoming peer sync request
    async fn handle_peer_sync(&self, peer: &str, request: SyncRequest) -> Result<SyncResponse>;

    /// Broadcast message to mesh
    pub async fn broadcast(&self, topic: &str, record: Record) -> Result<()>;

    /// Get mesh status
    pub fn status(&self) -> MeshStatus;
}
```

Create `src/edge/resources.rs`:

```rust
//! Resource Management for Edge Devices
//!
//! Monitors and limits resource usage on constrained devices.

/// Resource manager
pub struct ResourceManager {
    limits: ResourceLimits,
    /// Current usage tracking
    usage: ResourceUsage,
    /// Storage manager
    storage: StorageManager,
    /// Eviction handler
    eviction: Box<dyn EvictionHandler>,
}

#[derive(Debug, Clone, Default)]
pub struct ResourceUsage {
    pub memory_bytes: AtomicUsize,
    pub storage_bytes: AtomicUsize,
    pub cpu_percent: AtomicU8,
    pub open_files: AtomicUsize,
    pub connections: AtomicUsize,
}

impl ResourceManager {
    /// Check if operation can proceed within limits
    pub fn can_allocate(&self, resource: ResourceType, amount: usize) -> bool;

    /// Allocate resource
    pub fn allocate(&self, resource: ResourceType, amount: usize) -> Result<()>;

    /// Release resource
    pub fn release(&self, resource: ResourceType, amount: usize);

    /// Get current usage
    pub fn usage(&self) -> ResourceUsage;

    /// Trigger eviction if needed
    pub async fn evict_if_needed(&self) -> Result<EvictionResult>;

    /// Check if limits are exceeded
    pub fn is_over_limit(&self) -> Option<ResourceType>;
}

/// Eviction handler trait
#[async_trait]
pub trait EvictionHandler: Send + Sync {
    async fn evict(&self, target_bytes: usize) -> Result<usize>;
    fn policy(&self) -> EvictionPolicy;
}
```

### Binary Size Optimization

Create edge-specific feature set:

```toml
[features]
# Minimal edge binary
edge-lite = []  # Core streaming only, no optional deps

# Edge features
edge = [
    "edge-lite",
    "edge-sync",
    "edge-mesh",
]

edge-sync = []      # Cloud sync capability
edge-mesh = []      # P2P mesh networking
edge-wasm = ["wasm-transforms"]  # WASM for edge processing
```

### CLI Commands

Add to `src/cli.rs`:

```bash
# Edge mode
streamline --edge                      # Start in edge mode
streamline --edge --config edge.yaml   # With config

# Sync management
streamline-cli edge sync status        # Show sync status
streamline-cli edge sync now           # Trigger immediate sync
streamline-cli edge sync pause         # Pause sync
streamline-cli edge sync resume        # Resume sync

# Mesh management
streamline-cli edge mesh status        # Show mesh status
streamline-cli edge mesh peers         # List connected peers
streamline-cli edge mesh join <peer>   # Join peer manually
streamline-cli edge mesh leave <peer>  # Disconnect from peer

# Resource management
streamline-cli edge resources          # Show resource usage
streamline-cli edge resources limit --memory 256MB --storage 1GB

# Diagnostics
streamline-cli edge health             # Edge health check
streamline-cli edge queue              # Show pending sync queue
```

### Implementation Tasks

1. **Task 1: Edge Module Structure**
   - Create `src/edge/mod.rs` with module structure
   - Define `EdgeConfig`, `SyncConfig`, `ResourceLimits`
   - Add `edge` feature flag
   - Files: `src/edge/mod.rs`, `src/edge/config.rs`, `Cargo.toml`
   - Acceptance: Module structure compiles

2. **Task 2: Binary Size Optimization**
   - Create `edge-lite` feature with minimal deps
   - Conditional compilation for optional features
   - Analyze and reduce binary size
   - Target: <5MB for edge-lite
   - Files: `Cargo.toml`, various modules
   - Acceptance: Edge binary <5MB

3. **Task 3: Resource Manager**
   - Implement `ResourceManager` for usage tracking
   - Add memory, storage, CPU limits
   - Implement eviction policies (LRU, oldest, priority)
   - Add resource monitoring
   - Files: `src/edge/resources.rs`
   - Acceptance: Resource limits enforced

4. **Task 4: Store-and-Forward Queue**
   - Implement persistent `SyncQueue`
   - Add backpressure when queue full
   - Implement queue compaction
   - Files: `src/edge/store_forward.rs`
   - Acceptance: Data queued during disconnection

5. **Task 5: Edge-Cloud Sync Engine**
   - Implement `SyncManager`
   - Add bidirectional sync capability
   - Implement batch sync with compression
   - Add sync state persistence
   - Handle reconnection
   - Files: `src/edge/sync.rs`
   - Acceptance: Data syncs to cloud reliably

6. **Task 6: Conflict Resolution**
   - Implement `ConflictResolver`
   - Add LWW, custom merge strategies
   - WASM integration for custom resolvers
   - Conflict logging and metrics
   - Files: `src/edge/conflict.rs`
   - Acceptance: Conflicts resolved correctly

7. **Task 7: Edge Mesh Networking**
   - Implement `EdgeMesh` for P2P sync
   - Add mDNS peer discovery
   - Implement gossip protocol
   - Add peer connection management
   - Files: `src/edge/mesh.rs`, `src/edge/discovery.rs`
   - Acceptance: Edge nodes sync with each other

8. **Task 8: Delta Sync**
   - Implement delta encoding for sync
   - Add checksum-based change detection
   - Minimize bandwidth usage
   - Files: `src/edge/delta.rs`
   - Acceptance: Only changes synced

9. **Task 9: Edge CLI Commands**
   - Add `edge` subcommand group
   - Implement sync, mesh, resource commands
   - Add edge health checks
   - Files: `src/cli.rs`, `src/cli_utils/edge_commands.rs`
   - Acceptance: CLI commands functional

10. **Task 10: Cloud Gateway**
    - Implement cloud-side sync endpoint
    - Add edge registration/authentication
    - Handle multi-edge aggregation
    - Files: `src/server/edge_gateway.rs`
    - Acceptance: Cloud receives edge data

11. **Task 11: ARM/Cross-Compilation**
    - Add cross-compilation support
    - Test on Raspberry Pi
    - Create Docker images for ARM
    - Files: `.github/workflows/`, `Dockerfile.edge`
    - Acceptance: Runs on ARM devices

12. **Task 12: Documentation & Examples**
    - Add edge deployment guide
    - Create example configurations
    - Add IoT use case examples
    - Files: `docs/edge-first.md`, `examples/edge/`
    - Acceptance: Documentation complete

## Dependencies

```toml
# Edge-specific dependencies (minimal)
# Most deps already in core

# mDNS for local discovery (optional)
mdns-sd = { version = "0.11", optional = true }

[features]
edge-lite = []  # No additional deps
edge = ["edge-sync", "edge-mesh"]
edge-sync = []
edge-mesh = ["dep:mdns-sd"]
```

## Acceptance Criteria

- [ ] Edge binary <5MB (edge-lite feature)
- [ ] Full offline operation without cloud
- [ ] Automatic sync when connectivity restored
- [ ] Conflict resolution works correctly
- [ ] Resource limits enforced on constrained devices
- [ ] Edge mesh networking functional
- [ ] Runs on Raspberry Pi Zero (512MB RAM)
- [ ] Store-and-forward queue persists across restarts
- [ ] CLI commands fully functional
- [ ] Performance: 10K msg/sec on Pi 4
- [ ] All tests pass
- [ ] Documentation complete

## Example Configuration

```yaml
# edge.yaml
edge:
  node_id: "edge-store-001"
  location:
    lat: 37.7749
    lon: -122.4194
    name: "Store #001"

  # Cloud sync
  cloud_endpoint: "https://streamline.example.com"
  sync:
    enabled: true
    interval_secs: 30
    batch_size: 1000
    compression: zstd
    bandwidth_limit: 1048576  # 1MB/s

  # Resource limits (Pi Zero)
  resources:
    max_memory: 256MB
    max_storage: 4GB
    max_cpu_percent: 80
    eviction_policy: oldest

  # Conflict resolution
  conflict_resolution: last_write_wins

  # Topics to sync
  sync_topics:
    - topic: "pos.transactions"
      direction: edge_to_cloud
    - topic: "inventory.updates"
      direction: bidirectional
    - topic: "promotions"
      direction: cloud_to_edge

  # Local-only topics
  local_topics:
    - "local.cache"
    - "local.temp"

  # Mesh (optional)
  mesh:
    enabled: true
    discovery: mdns
    max_peers: 3
    replicated_topics:
      - "inventory.updates"
```

## Use Cases

1. **Retail Edge**: POS transactions streamed from stores, promotions synced down
2. **IoT Gateway**: Sensor data aggregated and synced to cloud
3. **Vehicle Telemetry**: Driving data collected offline, synced when parked
4. **Industrial Edge**: Factory floor data processed locally, synced to cloud
5. **Remote Sites**: Oil rigs, ships, remote offices with intermittent connectivity

## Related Files

- `src/edge/` - Edge module (all files)
- `src/server/edge_gateway.rs` - Cloud-side sync endpoint
- `src/cli.rs` - CLI commands
- `Cargo.toml` - Feature flags
- `Dockerfile.edge` - ARM Docker image

## Labels

`enhancement`, `feature`, `edge`, `iot`, `game-changer`, `large`
