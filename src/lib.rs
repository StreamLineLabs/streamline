#![warn(clippy::unwrap_used)]
#![warn(clippy::expect_used)]

//! # Streamline
//!
//! Streamline is a developer-first, operationally simple streaming solution
//! designed to fill the gap between enterprise streaming platforms (Kafka, Redpanda)
//! and simple messaging systems (Redis Pub/Sub).
//!
//! ## Features
//!
//! - **Kafka Protocol Compatible**: Existing Kafka clients work unchanged
//! - **Single Binary**: One executable, no dependencies, <50MB
//! - **Zero Configuration**: `./streamline` works out of the box
//! - **Built-in Observability**: No external monitoring stack required
//! - **TLS/mTLS Support**: Secure client and inter-broker communication
//! - **SASL Authentication**: PLAIN and SCRAM-SHA-256/512 mechanisms
//! - **ACL Authorization**: Fine-grained access control
//! - **Cluster Mode**: Raft-based consensus with data replication
//!
//! ## Quick Start
//!
//! ### Running the Server
//!
//! ```bash
//! # Run with defaults (listens on 0.0.0.0:9092)
//! $ ./streamline
//!
//! # Run with custom settings
//! $ ./streamline --listen-addr 127.0.0.1:9092 --data-dir /var/lib/streamline
//!
//! # Run with TLS enabled
//! $ ./streamline --tls-enabled --tls-cert server.crt --tls-key server.key
//! ```
//!
//! ### Using with Kafka Clients
//!
//! ```bash
//! # Produce messages
//! $ kafka-console-producer --broker-list localhost:9092 --topic test
//!
//! # Consume messages
//! $ kafka-console-consumer --bootstrap-server localhost:9092 --topic test --from-beginning
//! ```
//!
//! ## Library Usage
//!
//! Streamline can be used as a library in two ways:
//!
//! ### Embedded Mode (Recommended for library usage)
//!
//! Use [`EmbeddedStreamline`] for direct programmatic access without TCP/HTTP servers:
//!
//! ```no_run
//! use streamline::{EmbeddedStreamline, Result};
//! use bytes::Bytes;
//!
//! fn main() -> Result<()> {
//!     // Create an in-memory instance
//!     let streamline = EmbeddedStreamline::in_memory()?;
//!
//!     // Create a topic
//!     streamline.create_topic("events", 3)?;
//!
//!     // Produce messages
//!     let offset = streamline.produce("events", 0, None, Bytes::from("hello world"))?;
//!     println!("Produced at offset: {}", offset);
//!
//!     // Consume messages
//!     let records = streamline.consume("events", 0, 0, 100)?;
//!     for record in records {
//!         println!("Message: {:?}", record.value);
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! ### Full Server Mode
//!
//! To embed a full Kafka-compatible server:
//!
//! ```no_run
//! use streamline::{Server, ServerConfig};
//!
//! #[tokio::main]
//! async fn main() -> streamline::Result<()> {
//!     // Use default configuration
//!     let config = ServerConfig::default();
//!
//!     // Create and run the server (None = no sharded runtime)
//!     let server = Server::new(config, None).await?;
//!     server.run().await
//! }
//! ```
//!
//! ## Architecture
//!
//! Streamline is organized into several modules:
//!
//! - [`embedded`]: Embedded mode for library usage without TCP servers
//! - [`server`]: TCP server and connection handling
//! - [`protocol`]: Kafka wire protocol implementation
//! - [`storage`]: Segment-based persistent storage
//! - [`auth`]: Authentication (SASL) and authorization (ACL)
//! - [`cluster`]: Raft-based clustering and node management
//! - [`replication`]: Data replication and ISR management
//! - [`consumer`]: Consumer groups and offset management
//! - [`config`]: Server configuration and CLI arguments
//! - [`metrics`]: Prometheus-compatible metrics
//! - [`error`]: Error types and Result alias
//!
//! ## Configuration
//!
//! Key configuration options (via CLI args or environment variables):
//!
//! | Option | Env Variable | Default | Description |
//! |--------|--------------|---------|-------------|
//! | `--listen-addr` | `STREAMLINE_LISTEN_ADDR` | `0.0.0.0:9092` | Kafka protocol address |
//! | `--http-addr` | `STREAMLINE_HTTP_ADDR` | `0.0.0.0:9094` | HTTP API address |
//! | `--data-dir` | `STREAMLINE_DATA_DIR` | `./data` | Data storage directory |
//! | `--tls-enabled` | `STREAMLINE_TLS_ENABLED` | `false` | Enable TLS |
//! | `--auth-enabled` | `STREAMLINE_AUTH_ENABLED` | `false` | Enable authentication |
//!
//! See [`ServerArgs`] for the complete list of options.
//!
//! ## Stability
//!
//! Streamline follows semantic versioning with clear stability guarantees:
//!
//! | Level | Description | Breaking Changes |
//! |-------|-------------|------------------|
//! | **Stable** | Production-ready | Only in major versions |
//! | **Beta** | Feature-complete | In minor versions with notice |
//! | **Experimental** | Work in progress | May change without notice |
//!
//! ### API Stability by Module
//!
//! | Module | Stability | Notes |
//! |--------|-----------|-------|
//! | [`server`], [`storage`], [`consumer`] | Stable | Core functionality |
//! | [`config`], [`error`] | Stable | Configuration and error types |
//! | [`protocol`] | Stable | Kafka wire protocol |
//! | [`embedded`] | Beta | Embedded SDK |
//! | [`transaction`] | Beta | Transaction support |
//! | [`cluster`], [`replication`] | Beta | Clustering features |
//! | [`analytics`] | Stable | SQL analytics (DuckDB) |
//! | [`sink`] | Experimental | Lakehouse connectors |
//!
//! See the [API Stability documentation](https://github.com/josedab/streamline/blob/main/docs/API_STABILITY.md)
//! for detailed compatibility matrices.

// Deny .unwrap() in production code to prevent panics in a streaming system.
// Test code is exempt via #[cfg(test)] and --cfg test.
#![cfg_attr(not(test), deny(clippy::unwrap_used))]

// ── Stable public API modules ──
pub mod config;
pub mod consumer;
pub mod embedded;
pub mod error;
pub mod protocol;
pub mod runtime;
pub mod server;
pub mod storage;
pub mod transaction;

// ── Internal modules exposed for binary targets (not part of public library API) ──
// These are `pub` only because Rust binaries in the same package access them
// through `use streamline::...`. Library consumers should use the re-exports below.
#[doc(hidden)]
pub mod cli_utils;
#[doc(hidden)]
pub mod contracts;
#[doc(hidden)]
pub mod debugger;
#[doc(hidden)]
pub mod marketplace;
#[doc(hidden)]
pub mod telemetry;

// ── Internal modules (crate-visible only) ──
#[cfg(feature = "kafka-connect")]
#[allow(dead_code)]
pub(crate) mod connect;
pub(crate) mod dlq;
#[allow(dead_code)]
pub(crate) mod ffi;
pub(crate) mod gateway;
pub(crate) mod lineage;
#[allow(dead_code)]
pub(crate) mod playground;
pub(crate) mod plugin;
#[allow(dead_code)]
pub(crate) mod policy;
pub(crate) mod pubsub;
pub(crate) mod replay;
pub(crate) mod smart_partition;
pub(crate) mod testing;
#[allow(dead_code)]
pub(crate) mod wasm;

// Replication requires clustering
#[cfg(feature = "clustering")]
#[allow(dead_code)]
pub(crate) mod replication;

// Metrics module - always available (provides no-ops when feature disabled)
#[allow(dead_code)]
pub(crate) mod metrics;

// Feature-gated modules
#[cfg(feature = "auth")]
pub(crate) mod audit;
#[cfg(feature = "auth")]
pub mod auth;

#[cfg(feature = "clustering")]
#[allow(dead_code)]
pub(crate) mod cluster;

#[cfg(feature = "schema-registry")]
pub mod schema;

#[cfg(feature = "telemetry")]
pub(crate) mod tracing;

#[cfg(feature = "analytics")]
#[allow(dead_code)]
pub(crate) mod analytics;

// GraphQL API (requires graphql feature)
#[cfg(feature = "graphql")]
pub mod graphql;

// SQLite query interface (requires sqlite-queries feature)
#[cfg(feature = "sqlite-queries")]
pub mod sqlite;

// Sink connectors (requires iceberg or delta-lake feature)
#[cfg(any(feature = "iceberg", feature = "delta-lake"))]
pub mod sink;

// Optional web UI module (requires "web-ui" feature)
#[cfg(feature = "web-ui")]
pub mod ui;

// StreamlineQL - SQL-like stream processing engine
#[allow(dead_code)]
pub(crate) mod streamql;

// Change Data Capture (CDC) - Universal CDC Hub
#[cfg(any(
    feature = "postgres-cdc",
    feature = "mysql-cdc",
    feature = "mongodb-cdc",
    feature = "sqlserver-cdc"
))]
#[allow(dead_code)]
pub(crate) mod cdc;

// Transport layer (QUIC, WebTransport)
#[allow(dead_code)]
pub(crate) mod transport;

// Observability with eBPF support
pub(crate) mod observability;

// Network module with XDP support (Linux only)
#[cfg(target_os = "linux")]
pub(crate) mod network;

// Auto-tuning with ML-based optimization
#[doc(hidden)]
pub mod autotuning;

// Edge-first architecture (optional - requires edge feature)
#[cfg(feature = "edge")]
#[allow(dead_code)]
pub(crate) mod edge;

// Functions-as-a-Service runtime (optional - requires faas feature)
#[cfg(feature = "faas")]
#[allow(dead_code)]
pub(crate) mod faas;

// CRDT (Conflict-free Replicated Data Types) for edge computing
#[doc(hidden)]
pub mod crdt;

// Stream Processing DSL
#[allow(dead_code)]
pub(crate) mod dsl;

// Time-Series Native Storage
#[allow(dead_code)]
pub(crate) mod timeseries;

// Smart Data Lifecycle Management
pub(crate) mod lifecycle;

// Observability Pipeline Integration
pub(crate) mod obs_pipeline;

// Multi-Tenancy Framework
#[allow(dead_code)]
pub(crate) mod multitenancy;

// Tenant isolation layer (namespace management, resource quotas, tenant-scoped auth)
pub mod tenant;

// Stateful Stream Operators
pub(crate) mod stateful;

// Streamline Cloud - Serverless Control Plane
// Always available; full functionality requires "control-plane" feature
#[doc(hidden)]
pub mod cloud;

// Managed Cloud sub-modules (experimental, behind control-plane feature)
#[cfg(feature = "control-plane")]
pub use cloud::metering::{
    AggregatedUsage, AggregationWindow, MeteringRecord, MeteringService, UsageAggregator,
};
#[cfg(feature = "control-plane")]
pub use cloud::provisioning::{
    ClusterSpec, ManagedCluster as ProvisionedCluster, ManagedClusterStatus, ProvisioningManager,
    ProvisioningPlan, ProvisioningStep,
};
#[cfg(feature = "control-plane")]
pub use cloud::tenant::{
    CloudPlan, CloudTenant, CloudTenantManager, TenantIsolation, TenantQuota as CloudTenantQuota,
};

// GitOps Configuration
#[doc(hidden)]
pub mod gitops;

// Transactional Outbox Pattern
#[allow(dead_code)]
pub(crate) mod outbox;

// Real-Time ML Feature Store (optional - featurestore feature)
#[cfg(feature = "featurestore")]
#[allow(dead_code)]
pub(crate) mod featurestore;

// MCP (Model Context Protocol) Server for AI Agents
#[allow(dead_code)]
pub(crate) mod mcp;

// Dev Server Mode (streamline dev)
#[allow(dead_code)]
pub(crate) mod devserver;

// Re-export commonly used types (feature-gated)
#[cfg(feature = "auth")]
pub use auth::{
    Acl, AclBuilder, AclFilter, AclStore, AuthSession, AuthorizationResult, Authorizer,
    AuthorizerConfig, DelegationToken, DelegationTokenConfig, DelegationTokenManager,
    DelegationTokenStats, Operation, PatternType, Permission, RbacManager, ResourceType, Role,
    RoleBinding, RolePermission, SaslMechanism, SaslPlainAuthenticator, ScramAuthenticator,
    SessionManager, TokenCredentials, TokenInfo, User, UserStore,
};

#[cfg(feature = "clustering")]
pub use cluster::{
    AutoScaler, AutoScalerConfig, AutoScalerStats, BrokerInfo, ClusterConfig, ClusterManager,
    ClusterMetadata, DrainInfo, DrainState, FailoverConfig, FailoverEvent, FailoverHandler,
    HpaMetric, HpaMetricsAdapter, InterBrokerTls, InterBrokerTlsConfig, MetricType,
    MetricsSnapshot, NodeId, NodeState, PartitionAssignment, ScaleDirection, ScalingDecision,
    ScalingPolicy, TopicAssignment,
};
pub use config::{AclConfig, AuthConfig, ServerArgs, ServerConfig, StorageConfig, WalConfig};
pub use consumer::{
    CommittedOffset, ConsumerGroup, GroupCoordinator, GroupMember, GroupState, OffsetStore,
    PartitionAssignor, RangeAssignor, RoundRobinAssignor, StickyAssignor, StickyUserData,
    TopicPartition,
};
pub use error::{ErrorHint, Result, StreamlineError};
#[cfg(feature = "clustering")]
pub use replication::{
    AcksPolicy, IsrConfig, IsrManager, ReplicaRole, ReplicationConfig, ReplicationManager,
    WatermarkManager,
};

// Re-export Failover types (requires clustering feature)
#[cfg(feature = "clustering")]
pub use replication::failover::{
    FailoverConfig as RegionFailoverConfig, FailoverEvent as RegionFailoverEvent,
    FailoverOrchestrator, FailoverReason, RegionFailoverState, RegionHealth, RegionRole,
    RoutingPreference,
};
pub use server::{QuotaConfig, QuotaManager, Server};
pub use storage::{
    Record, RecordBatch, TopicConfig, TopicManager, TopicMetadata, MESSAGE_TTL_HEADER,
};

// Re-export audit types (requires auth feature)
#[cfg(feature = "auth")]
pub use audit::{AuditConfig, AuditEvent, AuditLogEntry, AuditLogger};

// Re-export tracing types (requires telemetry feature)
#[cfg(feature = "telemetry")]
pub use tracing::{
    extract_trace_context, init_tracing, inject_trace_context, record_acl_span, record_auth_span,
    record_cluster_span, record_consumer_group_span, record_fetch_span, record_produce_span,
    record_replication_span, record_transaction_span, shutdown_tracing, TraceContext,
    TracingConfig, TracingConfigBuilder,
};

// Re-export transaction types
pub use transaction::{
    Transaction, TransactionCoordinator, TransactionPartition, TransactionState,
    TransactionTimeoutConfig, TransactionTimeoutStats,
};

// Re-export embedded mode types
pub use embedded::{EmbeddedConfig, EmbeddedStats, EmbeddedStreamline, TopicInfoResult};

// Re-export DLQ types
pub use dlq::{DlqConfig, DlqContext, DlqManager, DlqMetadata, DlqStats};

// Re-export Replay types
pub use replay::transform::{MessageMutator, MutatorChain, MutatorRule};
pub use replay::{
    RecordConfig, RecorderStats, Recording, RecordingMetadata, RecordingSegment, ReplayConfig,
    ReplayMode, ReplayerStats, TrafficRecorder, TrafficReplayer,
};

// Re-export Pub/Sub types
pub use pubsub::{ChannelStats, Message, PubSubConfig, PubSubManager, PubSubStats};

// Re-export Simple Protocol types
pub use protocol::{start_simple_server, SimpleProtocolHandler, SimpleProtocolState};

// Re-export Plugin system types
pub use plugin::{
    AuthContext, AuthResult, AuthenticationPlugin, InstalledPlugin, InterceptorContext,
    InterceptorResult, MessageInterceptor, MetricsContext, MetricsPlugin, Plugin, PluginError,
    PluginManager, PluginManagerStats, PluginManifest, PluginRegistry, PluginResult, PluginType,
};

// Re-export Schema Registry types (requires schema-registry feature)
#[cfg(feature = "schema-registry")]
pub use schema::{
    CompatibilityChecker, CompatibilityLevel, RegisteredSchema, SchemaCache, SchemaError,
    SchemaReference, SchemaRegistry, SchemaRegistryConfig, SchemaStore, SchemaType,
};

// Re-export Schema Inference types (requires schema-registry feature)
#[cfg(feature = "schema-registry")]
pub use schema::inference::{
    InferenceConfig, InferenceError, InferredField, InferredType, MigrationAction,
    MigrationSuggestion, SchemaDrift, SchemaInferrer,
};

// Re-export Kafka Connect compatibility types
#[cfg(feature = "kafka-connect")]
pub use connect::{
    ConnectConfig, ConnectInternalTopic, ConnectManager, ConnectTopicType, ConnectorState,
    TaskState,
};

// Re-export Kafka Connect REST API types
#[cfg(feature = "kafka-connect")]
pub use connect::api::{
    create_connect_router, ConfigDefinition, ConfigInfo, ConfigValidationResult, ConfigValue,
    ConnectApiState, ConnectError, ConnectorInfo, ConnectorManager, ConnectorOffsets,
    ConnectorPlugin, ConnectorStatus, ConnectorStatusResponse, ConnectorType,
    CreateConnectorRequest, OffsetEntry as ConnectOffsetEntry, RestartQuery, TaskId, TaskInfo,
    TaskStatus, WorkerInfo,
};

// Re-export CDC types (requires any CDC feature)
#[cfg(any(
    feature = "postgres-cdc",
    feature = "mysql-cdc",
    feature = "mongodb-cdc",
    feature = "sqlserver-cdc"
))]
pub use cdc::{
    CdcColumnValue, CdcConfig, CdcEvent, CdcManager, CdcOperation, CdcSource, CdcSourceInfo,
    CdcSourceMetrics, CdcSourceStatus, ChangeEvent, ColumnSchema, CompatibilityResult,
    Operation as CdcChangeOperation, SchemaCompatibility, SchemaEvolutionConfig, SchemaEvolutionStats, SchemaEvolutionTracker,
    SchemaHistory, SchemaRegistry as CdcSchemaRegistry, SchemaVersion, SourceInfo, TableSchema,
    TransactionInfo,
};
// Debezium-replacement types
#[cfg(any(
    feature = "postgres-cdc",
    feature = "mysql-cdc",
    feature = "mongodb-cdc",
    feature = "sqlserver-cdc"
))]
pub use cdc::{
    CdcHeartbeat, DdlType, HeartbeatConfig, HeartbeatMessage, SchemaChange, SchemaHistoryStore,
    SnapshotManager, SnapshotProgress, SnapshotState, SnapshotStrategy,
};

#[cfg(feature = "postgres-cdc")]
pub use cdc::{PostgresCdcConfig, PostgresCdcSource, PostgresOutputPlugin, PostgresSslMode};

#[cfg(feature = "mysql-cdc")]
pub use cdc::{BinlogPosition, MySqlCdcConfig, MySqlCdcSource};

#[cfg(feature = "mongodb-cdc")]
pub use cdc::{MongoDbCdcConfig, MongoDbCdcSource, MongoFullDocumentMode, ResumeToken};

#[cfg(feature = "sqlserver-cdc")]
pub use cdc::{LsnPosition, SqlServerCdcConfig, SqlServerCdcSource};

// Re-export WASM transformation types
pub use wasm::runtime::{
    get_wasm_runtime_info, is_wasm_transforms_available, ModuleStats, RuntimeConfig, RuntimeStats,
    WasmRuntime, WasmRuntimeInfo,
};
pub use wasm::{
    RouteResult, TransformInput, TransformOutput, TransformResult, TransformationConfig,
    TransformationRegistry, TransformationStats, TransformationType,
};

// Re-export WASM Catalog types
pub use wasm::catalog::{
    CatalogEntry, ChainErrorHandling, ChainStep, DeploymentConfig, TransformCatalog,
    TransformChain, TransformMetrics, TransformStatus, TransformVersion,
};

// Re-export WASM Function Registry types
pub use wasm::function_registry::{
    FunctionMetrics, FunctionRegistry, FunctionStatus, FunctionType, WasmFunction,
};

// Re-export WASM Built-in function types
pub use wasm::builtins::{
    list_builtins, create_builtin, BuiltinFunction, BuiltinInfo, FieldMask,
    FilterOperator as WasmFilterOperator, JsonFilter, JsonFlatten, JsonTransform, MaskStrategy,
    TimestampExtract, TimestampFormat,
};

// Re-export WASM REST API types
pub use wasm::api::{
    create_wasm_router, CreateTransformationRequest, TestTransformationRequest,
    TestTransformationResponse, TransformationInfo, UpdateTransformationRequest, WasmApiError,
    WasmApiState,
};

// Re-export Telemetry types
pub use telemetry::{
    EnabledFeatures, TelemetryCollector, TelemetryConfig, TelemetryManager, TelemetryReport,
};

// Re-export Analytics types (requires analytics feature)
#[cfg(feature = "analytics")]
pub use analytics::{
    DuckDBEngine, ExplainResult, MaterializedView, QueryOptions as AnalyticsQueryOptions,
    QueryResult as AnalyticsQueryResult, QueryResultRow, StreamTable,
};

// Re-export Sink types (requires iceberg feature)
#[cfg(feature = "iceberg")]
pub use sink::config::{
    CatalogType, IcebergSinkConfig, PartitioningConfig, PartitioningStrategy, SinkConfig, SinkType,
    TimeGranularity,
};
#[cfg(feature = "iceberg")]
pub use sink::{SinkConnector, SinkInfo, SinkManager, SinkMetrics, SinkStatus};

// Re-export Observability types
#[cfg(target_os = "linux")]
pub use observability::ebpf::{
    DiskIoEvent, EbpfConfig, EbpfManager, EbpfSummary, NetworkEvent, ProbeStatus, SyscallStats,
};
pub use observability::metrics::{
    HistogramValue, MetricDefinition, MetricSample, MetricValue, MetricsRegistry, RateCalculator,
    SummaryValue,
};
pub use observability::system::{
    DiskInfo, NetworkInterface, ResourceLimits, SystemInfo, UptimeInfo,
};
pub use observability::{
    ConnectionInfo, CpuMetrics, DiskMetrics, LatencyHistogram, LatencyHistograms, MemoryMetrics,
    NetworkMetrics, ObservabilityConfig, ObservabilityManager, ProcessMetrics, SystemMetrics,
};

// Re-export Observability Alerting types (SLO monitoring engine)
pub use observability::alerting::{
    AlertCondition as ObsAlertCondition, AlertRule as ObsAlertRule, AlertingEngine, FiredAlert,
    NotificationChannel as ObsNotificationChannel, NotificationPayload, Severity, SloDefinition,
    SloIndicator, SloStatus, SloWindow,
};

// Lakehouse-Native Streaming (optional - requires lakehouse feature)
#[cfg(feature = "lakehouse")]
pub mod lakehouse;

// AI-Native Streaming (optional - requires ai feature)
#[cfg(feature = "ai")]
pub mod ai;

// Feature Integration Pipelines (CDC → AI → Lakehouse)
#[doc(hidden)]
pub mod integration;

// Re-export Auto-tuning types
pub use autotuning::optimizer::{
    BayesianOptimizer, GeneticOptimizer, GradientDescentOptimizer, Individual, OptimizerConfig,
    OptimizerType, SimulatedAnnealingOptimizer,
};
pub use autotuning::predictor::{
    LinearRegression, NeuralNetworkPredictor, PerformancePredictor, PolynomialRegression,
    TimeSeriesForecaster,
};
pub use autotuning::workload::{
    BurstinessLevel, CachedProfile, RateBucket, SizeBucket, WorkloadAnalyzer,
    WorkloadCharacteristics, WorkloadFingerprint, WorkloadPattern, WorkloadProfileCache,
    WorkloadSample,
};
pub use autotuning::{
    AutoTuner, AutoTuningConfig, AutoTuningStats, PerformanceMetrics, TunableParameters,
    WorkloadType,
};

// Re-export Resource Governor types
pub use autotuning::governor::{
    GovernorAction, PartitionRecommendation, PressureLevel, ResourceGovernor,
    ResourceGovernorConfig, ResourceSnapshot,
};

// Re-export Lakehouse types (requires lakehouse feature)
#[cfg(feature = "lakehouse")]
pub use lakehouse::{
    ExportDestination, ExportFormat, ExportJob, ExportManager, ExportStatus, LakehouseConfig,
    LakehouseManager, MaterializedView as LakehouseMaterializedView, MaterializedViewConfig,
    MaterializedViewManager, ParquetConfig, ParquetSegment, PhysicalPlan, Predicate,
    QueryOptimizer as LakehouseQueryOptimizer, RefreshMode, RefreshResult, StatisticsManager,
    ViewDefinition, ViewStatus,
};

// Re-export AI types (requires ai feature)
#[cfg(feature = "ai")]
pub use ai::{
    AIConfig, AIManager, AnomalyConfig, AnomalyDetector, AnomalyResult, AnomalyType,
    ClassificationResult, DetectorConfig, EmbeddingConfig, EmbeddingEngine, EmbeddingResult,
    EnrichmentResult, LLMClient, LLMConfig, LLMProvider, PipelineRoutingRule, ProcessingResult,
    RoutingConfig, RoutingDecision, RoutingRule, SearchConfig, SearchEngine, SearchQuery,
    SearchResult, SemanticRouter, StreamEnricher, StreamProcessor, VectorStore,
};

// Re-export Edge types (requires edge feature)
#[cfg(feature = "edge")]
pub use edge::{
    AdaptiveConfig, CheckpointState, CompactionResult, ConflictInfo, ConflictResolution,
    ConflictResolver, ConflictStats, DiscoveryConfig, DiscoveryProtocol, DiscoveryStatsSnapshot,
    EdgeCapabilities, EdgeCluster, EdgeCompressionStrategy, EdgeConfig, EdgeDiscovery,
    EdgeNetworkConfig, EdgeNode, EdgeNodeCapabilities, EdgeNodeInfo, EdgeNodeStatus,
    EdgeOptimizationConfig, EdgeOptimizer, EdgeResourceConfig, EdgeRetentionPolicy, EdgeRuntime,
    EdgeRuntimeBuilder, EdgeStorageConfig, EdgeSyncCheckpoint, EdgeSyncConfig, EdgeSyncEngine,
    EdgeSyncMode, EvictionResult, FederationCommand, FederationManager, FederationState,
    FederationStats, HeartbeatRequest, HeartbeatResponse, MemoryRecommendation, MeshTopology,
    OptimizationStats, PartitionCheckpoint, PeerConnection, PeerConnectionState, ResolutionResult,
    ResourceAlert, ResourceMonitor, ResourceMonitorConfig, ResourceThresholds,
    StoreForwardConfig, StoreForwardEngine, StoreForwardStatus, SyncBatch, SyncDirection,
    SyncPhase, SyncPolicy, SyncProgress, SyncRecord, SyncResult, SyncState, SyncStats,
    SyncStrategy, SyncWatermark, TopicCheckpoint, TopicStorageUsage,
};

// Re-export CRDT types
pub use crdt::{
    // Compaction
    compact_crdt_records,
    // Record types
    has_crdt_header,
    headers_to_map,
    is_crdt_record,
    map_to_headers,
    // State management
    CausalConsumer,
    // Clock types
    CausalTimestamp,
    ClockComparison,
    // CRDT types
    Crdt,
    CrdtCompactionConfig,
    CrdtCompactionStats,
    CrdtCompactor,
    CrdtMergeContext,
    CrdtMetadata,
    CrdtOperation,
    CrdtRecord,
    CrdtRecordBuilder,
    CrdtStateConfig,
    CrdtStateManager,
    CrdtStateStatsSnapshot,
    CrdtType,
    CrdtValue,
    GCounter,
    HlcClockSource,
    HybridLogicalClock,
    KeySubscription,
    LWWRegister,
    MergedState,
    MultiPartitionState,
    ORSet,
    PNCounter,
    RGASequence,
    RgaElement,
    RgaId,
    StateChange,
    StateKey,
    UniqueTag,
    VectorClock,
};

// Re-export Time-Series types
pub use timeseries::{
    compression::{CompressionCodec, TimeSeriesCompressor},
    partition::{PartitionInfo, PartitionManager, PartitionState, PartitionStats, TimePartition},
    query::{
        execute_query, Aggregation, QueryBuilder, QueryFilter,
        QueryResult as TimeSeriesQueryResult, TimeRange, TimeSeriesQuery,
    },
    rollup::{
        RollupConfig, RollupFunction, RollupInterval, RollupManager, RollupPolicy, RollupResult,
        RollupStats,
    },
    store::{TimeSeriesConfig, TimeSeriesStore, TimeSeriesWriter},
    DataPoint, DataPointBatch, RetentionPolicy, TimeSeriesPoint, TimeSeriesStats,
};

// Re-export Lifecycle types
pub use lifecycle::{
    policies::{
        CompliancePolicy, CostOptimizationPolicy, DataClassification, LifecyclePolicy,
        PolicyAction, PolicyCondition, PolicyEvaluator, PolicyRule,
    },
    storage_tiers::{
        AccessFrequency, StorageTier, StorageTierConfig, TierCapabilities, TierManager,
        TierMetrics, TierStats,
    },
    transitions::{
        MigrationJob, MigrationManager, MigrationPriority, MigrationStats, MigrationStatus,
        TransitionPlan, TransitionRule,
    },
    CompressionType, DataAge, DataSegment, LifecycleAction, LifecycleConfig, LifecycleManager,
    LifecycleStats,
};

// Re-export Observability Pipeline types
pub use obs_pipeline::{
    alerts::{
        Alert, AlertCondition, AlertManager, AlertRule, AlertSeverity, AlertState, ComparisonOp,
        NotificationChannel,
    },
    exporters::{
        ExportFormat as ObsExportFormat, ExportResult, ExporterConfig, LogExporter,
        MetricsExporter, TelemetryExporter, TraceExporter,
    },
    logs::{LogAggregator, LogEntry, LogLevel, LogQuery, LogSource},
    metrics::{
        Counter as ObsCounter, Gauge as ObsGauge, Histogram as ObsHistogram,
        HistogramBucket as ObsHistogramBucket, HistogramValue as ObsHistogramValue, MetricLabel,
        MetricType as ObsMetricType, MetricValue as ObsMetricValue, MetricsCollector,
        MetricsSnapshot as ObsMetricsSnapshot,
    },
    traces::{
        Span, SpanContext, SpanEvent, SpanKind, SpanLink, SpanStatus, Trace, TraceCollector,
        TraceSampler,
    },
    ComponentHealth, HealthCheck, HealthStatus, ObsPipeline, ObsPipelineConfig, PipelineStats,
    Resource as ObsResource,
};

// Re-export Multi-Tenancy types
pub use multitenancy::{
    isolation::{IsolationLevel, IsolationPolicy, NamespaceManager, TenantNamespace},
    quotas::{
        EnforcementMode, QuotaEnforcer, QuotaLimit, QuotaPolicy, ResourceQuota,
        ResourceType as TenantResourceType, TenantQuotaStatus,
    },
    usage::{BillingPeriod, UsageAggregate, UsageMetric, UsageRecord, UsageSummary, UsageTracker},
    Tenant, TenantConfig, TenantError, TenantManager, TenantManagerConfig, TenantManagerStats,
    TenantStatus, TenantTier, TenantUpdate,
};

// Re-export Stateful Stream Operator types
pub use stateful::{
    aggregations::{
        Aggregation as StatefulAggregation, AggregationFunction, AggregationResult,
        AggregationState, AvgAggregation, CountAggregation, CountDistinctAggregation,
        MaxAggregation, MinAggregation, MultiAggregation, StdDevAggregation, SumAggregation,
    },
    checkpoints::{
        Checkpoint, CheckpointConfig, CheckpointManager, CheckpointMetadata, CheckpointStats,
        CheckpointStore, IncrementalCheckpoint, MemoryCheckpointStore,
    },
    joins::{
        IntervalJoin, JoinCondition, JoinResult, JoinStats, JoinType, StreamJoin, TemporalJoin,
    },
    state::{KeyedState, MemoryStateBackend, StateBackend, StateConfig, StateManager, StateStore},
    windows::{
        SessionWindow, SlidingWindow, TumblingWindow, Window, WindowConfig, WindowManager,
        WindowOutput, WindowTrigger, WindowType,
    },
    KeyedProcessor, LateDataHandling, OperatorConfig, OperatorError, StateBackendType,
    StatefulProcessor, StreamElement, TimeSemantics, Watermark, WatermarkStrategy,
};

// Re-export StreamQL types
pub use streamql::{
    CrossJoinOperator, DataType as StreamQLDataType, FilterOperator, HashJoinOperator,
    IntervalJoinOperator, JoinBuilder, JoinConfig, JoinSide, NestedLoopJoinOperator,
    ProjectOperator, QueryExecutor, QueryOptions as StreamQlQueryOptions, QueryPlanner,
    QueryResult as StreamQlQueryResult, StreamOperator, StreamQL, StreamQLParser, StreamResult,
    TemporalJoinOperator, Value as StreamQLValue,
};

// Re-export StreamQL ksqlDB-compatible types
pub use streamql::{
    KsqlAggregateFunction, KsqlColumnSchema, KsqlColumnType, KsqlContinuousQuery,
    KsqlContinuousQueryStats, KsqlContinuousQueryStatus, KsqlDataFormat, KsqlStatementResult,
    KsqlWindowSpec, StreamDefinition, StreamqlEngine, StreamqlStatement, TableDefinition,
};

// Re-export Testing types
pub use testing::{
    CiCdHelpers, DataGenerator, ScenarioResult, StreamMatchers, StreamlineContainerConfig,
    TestAssertions, TestConfig, TestConsumer, TestFixtures, TestHarness, TestProducer, TestRecord,
    TestScenarioBuilder, TestStreamline, TestUtils, WaitStrategy,
};

// Re-export Stream Contracts types
pub use contracts::{
    Assertion, AssertionEngine, AssertionResult, ContractRunner, ContractRunnerConfig,
    ContractSpec, FieldType, MockProducer, MockProducerConfig, StreamContract, TestReport,
    TestSuite,
};

// Re-export Debugger types
pub use debugger::{
    Breakpoint, BreakpointMatch, DebuggerConfig, EventView, MessageInspector, NavigationDirection,
    StateDiffEngine,
};

// Re-export Marketplace types
pub use marketplace::{
    ConnectorCatalogEntry, ConnectorCategory, ConnectorLifecycle, ConnectorSdk, InstalledConnector,
    MarketplaceCatalog, MarketplaceConfig, MarketplaceRegistry,
};

// Re-export Declarative Connector types
pub use marketplace::declarative::{
    ConnectorDirection, ConnectorManager as DeclarativeConnectorManager, ConnectorRuntimeState,
    ConnectorSpec, ManagedConnector, RestartPolicy,
};

// Re-export Gateway types
pub use gateway::{
    AmqpAdapter, AmqpConfig, GatewayConfig, GatewayStats, GrpcAdapter, GrpcConfig, MqttAdapter,
    MqttConfig, ProtocolGateway, ProtocolMapping,
};

// Re-export Smart Partition types
pub use smart_partition::{
    PartitionMetrics, RebalanceConfig, RebalanceMode, RebalancePlan, SkewAnalyzer,
    SkewAnalyzerConfig, SkewReport, SkewSeverity, SmartRebalancer,
};

// Re-export Playground types
pub use playground::{
    PlaygroundConfig, PlaygroundManager, PlaygroundStats, Tutorial, TutorialEngine,
};

// Re-export Lineage types
pub use lineage::{
    ConnectionEvent, ConnectionEventType, LineageConfig, LineageEdge, LineageGraph, LineageNode,
    LineageNodeType, LineageTracker,
};

// Re-export Cloud Console types
pub use cloud::console::{
    ApiKey, ApiScope, ConsoleManager, OnboardingRequest, OnboardingResponse, OnboardingStatus,
    UsageDashboard,
};
