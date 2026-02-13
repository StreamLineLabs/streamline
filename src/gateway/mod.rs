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
//! │ MQTT 3.1.1 │  │  AMQP 1.0  │  │   gRPC     │
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
pub mod router;

pub use amqp::{AmqpAdapter, AmqpConfig};
pub use grpc::{GrpcAdapter, GrpcConfig};
pub use mqtt::{MqttAdapter, MqttConfig};
pub use router::{GatewayConfig, GatewayStats, ProtocolGateway, ProtocolMapping};
