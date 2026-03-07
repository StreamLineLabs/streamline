//! Universal Protocol Gateway
//!
//! Accepts connections via MQTT, AMQP, and gRPC protocols and routes
//! messages into Streamline topics, expanding interoperability with
//! IoT devices (MQTT), legacy enterprise systems (AMQP), and
//! cloud-native services (gRPC).
//!
//! # Architecture
//!
//! ```text
//! ┌────────────┐  ┌────────────┐  ┌────────────┐
//! │ MQTT 3.1.1 │  │ AMQP 0-9-1 │  │   gRPC     │
//! │  :1883     │  │  :5672     │  │  :9096     │
//! └─────┬──────┘  └─────┬──────┘  └─────┬──────┘
//!       │               │               │
//!       └───────────────┼───────────────┘
//!                       ▼
//!              ┌─────────────────┐
//!              │ Protocol Router │
//!              └────────┬────────┘
//!                       ▼
//!              ┌─────────────────┐
//!              │   TopicManager  │
//!              └─────────────────┘
//! ```

pub mod amqp;
pub mod grpc;
pub mod mqtt;
pub mod mqtt_handler;
pub mod router;

pub use amqp::{
    AmqpAdapter, AmqpBinding, AmqpChannel, AmqpConfig, AmqpExchange, AmqpHandlerStatsSnapshot,
    AmqpMessageProperties, AmqpPublishData, AmqpQueue, AmqpSession, ExchangeType,
};
pub use grpc::{
    GrpcAdapter, GrpcConfig, GrpcConsumeRequest, GrpcConsumeResponse, GrpcCreateTopicRequest,
    GrpcHandlerStatsSnapshot, GrpcProduceRequest, GrpcProduceResponse, GrpcSession, GrpcTopicInfo,
};
pub use mqtt::{MqttAdapter, MqttConfig};
pub use mqtt_handler::{
    ConnAckCode, MqttConnectData, MqttHandlerStatsSnapshot, MqttPacketType, MqttPublishData,
    MqttSession, MqttSessionManager, MqttSubscription, mqtt_topic_matches,
};
pub use router::{GatewayConfig, GatewayStats, ProtocolGateway, ProtocolMapping};
