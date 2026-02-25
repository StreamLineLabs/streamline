//! Functions-as-a-Service (FaaS) Runtime
//!
//! A lightweight serverless runtime for stream-triggered WASM functions.
//! Functions execute in response to events on Streamline topics, enabling
//! event-driven processing without additional infrastructure.
//!
//! # Architecture
//!
//! ```text
//! ┌────────────┐     ┌─────────────┐     ┌──────────────┐
//! │ Topic      │────▶│ FaaS Engine  │────▶│ Output Topic │
//! │ (trigger)  │     │ (WASM exec)  │     │ (result)     │
//! └────────────┘     └─────────────┘     └──────────────┘
//!                          │
//!                    ┌─────┴─────┐
//!                    │ Function  │
//!                    │ Registry  │
//!                    └───────────┘
//! ```
//!
//! # Features
//!
//! - WASM-based function execution with sandboxed isolation
//! - Event trigger bindings (topic → function)
//! - Scale-to-zero: functions only consume resources when processing events
//! - Function lifecycle management (deploy, start, stop, undeploy)
//! - Chaining: output of one function feeds into another
//! - Dead letter queue for failed invocations
//! - Per-function resource limits (CPU time, memory)
//!
//! # Stability
//!
//! This module is **Experimental**. The API may change without notice.

pub mod engine;
pub mod function;
pub mod registry;
pub mod trigger;

pub use engine::{ChainErrorPolicy, ChainStep, FaasEngine, FunctionChain};
pub use function::{
    FaasFunction, FunctionConfig, FunctionMetrics, FunctionState, InvocationResult,
};
pub use registry::FunctionRegistry;
pub use trigger::{TriggerBinding, TriggerConfig, TriggerType};
