//! Protocol handlers for Streamline
//!
//! This module contains protocol implementations for
//! Kafka protocol compatibility and native protocols.
//!
//! ## Available Protocols
//!
//! - **Kafka**: Full Kafka wire protocol compatibility for existing clients
//! - **Simple**: Redis RESP-like text protocol for easy integration
//! - **Internal**: Inter-broker communication protocol for clustering
//!
//! ## Submodules
//!
//! - `handlers`: Kafka API handlers and error codes

pub mod connection;
pub mod handlers;
#[cfg(feature = "clustering")]
pub mod internal;
pub mod kafka;
pub mod pipeline;
pub mod response_cache;
pub mod simple;

pub use connection::{ConnectionContext, RawFd};
#[cfg(feature = "clustering")]
pub use internal::{
    FetchPartition, FetchPartitionResponse, FetchTopic, FetchTopicResponse, InternalMessage,
    LeaderAndIsrPartitionResponse, LeaderAndIsrPartitionState, LeaderAndIsrRequest,
    LeaderAndIsrResponse, LiveBroker, RecordData, RecordHeader, ReplicaFetchRequest,
    ReplicaFetchResponse, StopReplicaPartition, StopReplicaPartitionResponse, StopReplicaRequest,
    StopReplicaResponse, UpdateMetadataPartitionState, UpdateMetadataRequest,
    UpdateMetadataResponse,
};
pub use kafka::{
    create_control_record_batch, KafkaHandler, CONTROL_TYPE_ABORT, CONTROL_TYPE_COMMIT,
};
// Public configuration types
pub use pipeline::PipelineConfig;
pub use response_cache::{CacheKey, CacheStats as ResponseCacheStats, ResponseCache};
pub use simple::{start_simple_server, SimpleProtocolHandler, SimpleProtocolState};
