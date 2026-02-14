//! Stream Contracts & Consumer Testing Framework
//!
//! Provides contract-driven testing for event streams, enabling:
//! - Schema-based contract definitions for topics
//! - Mock event producers for reproducible testing
//! - Consumer assertion engine for validating behavior
//! - CI/CD integration with JUnit-compatible output
//!
//! # Example
//!
//! ```yaml
//! # contracts/events.yaml
//! apiVersion: streamline.io/v1
//! kind: StreamContract
//! metadata:
//!   name: user-events-contract
//! spec:
//!   topic: user-events
//!   schema:
//!     type: json
//!     required_fields:
//!       - name: user_id
//!         type: string
//!       - name: action
//!         type: string
//!       - name: timestamp
//!         type: integer
//!   invariants:
//!     - ordering: by_key
//!     - uniqueness: user_id+timestamp
//!   mock_data:
//!     count: 100
//!     template: '{"user_id":"{{uuid}}","action":"click","timestamp":{{timestamp}}}'
//! ```

pub mod assertion;
pub mod definition;
pub mod mock_producer;
pub mod runner;

pub use assertion::{Assertion, AssertionEngine, AssertionResult, AssertionType, FieldAssertion};
pub use definition::{
    ContractField, ContractInvariant, ContractSpec, FieldType, MockDataSpec, OrderingInvariant,
    StreamContract, UniquenessInvariant,
};
pub use mock_producer::{MockMessage, MockProducer, MockProducerConfig};
pub use runner::{
    ContractRunner, ContractRunnerConfig, TestOutcome, TestReport, TestResult, TestSuite,
};
