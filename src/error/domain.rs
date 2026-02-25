//! Domain-specific error types for Streamline

use thiserror::Error;

/// Structured storage error domain
#[derive(Debug, Error, Clone)]
pub enum StorageError {
    #[error("{operation}: {detail}")]
    Operation { operation: String, detail: String },
    #[error("{topic}/{partition}: {operation}: {detail}")]
    Partition {
        topic: String,
        partition: i32,
        operation: String,
        detail: String,
    },
    #[error("{0}")]
    Message(String),
}

impl StorageError {
    pub fn operation(operation: impl Into<String>, detail: impl Into<String>) -> Self {
        Self::Operation {
            operation: operation.into(),
            detail: detail.into(),
        }
    }

    pub fn partition(
        topic: impl Into<String>,
        partition: i32,
        operation: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        Self::Partition {
            topic: topic.into(),
            partition,
            operation: operation.into(),
            detail: detail.into(),
        }
    }
}

impl From<String> for StorageError {
    fn from(value: String) -> Self {
        Self::Message(value)
    }
}

impl From<&str> for StorageError {
    fn from(value: &str) -> Self {
        Self::Message(value.to_string())
    }
}

/// Structured protocol error domain
#[derive(Debug, Error, Clone)]
pub enum ProtocolError {
    #[error("{operation}: {detail}")]
    Operation { operation: String, detail: String },
    #[error("expected {expected}, got {got}")]
    Unexpected { expected: String, got: String },
    #[error("invalid {field}: {reason}")]
    InvalidField { field: String, reason: String },
    #[error("unsupported API key: {0}")]
    UnsupportedApiKey(String),
    #[error("{0}")]
    Message(String),
}

impl ProtocolError {
    pub fn operation(operation: impl Into<String>, detail: impl Into<String>) -> Self {
        Self::Operation {
            operation: operation.into(),
            detail: detail.into(),
        }
    }

    pub fn unexpected(expected: impl Into<String>, got: impl Into<String>) -> Self {
        Self::Unexpected {
            expected: expected.into(),
            got: got.into(),
        }
    }

    pub fn invalid_field(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::InvalidField {
            field: field.into(),
            reason: reason.into(),
        }
    }

    pub fn unsupported_api_key(api_key: impl Into<String>) -> Self {
        Self::UnsupportedApiKey(api_key.into())
    }
}

impl From<String> for ProtocolError {
    fn from(value: String) -> Self {
        Self::Message(value)
    }
}

impl From<&str> for ProtocolError {
    fn from(value: &str) -> Self {
        Self::Message(value.to_string())
    }
}

/// Structured configuration error domain
#[derive(Debug, Error, Clone)]
pub enum ConfigError {
    #[error("{setting}: {reason}")]
    InvalidSetting { setting: String, reason: String },
    #[error("missing {0}")]
    Missing(String),
    #[error("{0}")]
    Message(String),
}

impl ConfigError {
    pub fn invalid_setting(setting: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::InvalidSetting {
            setting: setting.into(),
            reason: reason.into(),
        }
    }

    pub fn missing(setting: impl Into<String>) -> Self {
        Self::Missing(setting.into())
    }
}

impl From<String> for ConfigError {
    fn from(value: String) -> Self {
        Self::Message(value)
    }
}

impl From<&str> for ConfigError {
    fn from(value: &str) -> Self {
        Self::Message(value.to_string())
    }
}

/// Structured server error domain
#[derive(Debug, Error, Clone)]
pub enum ServerError {
    #[error("bind failed on {address}: {reason}")]
    BindFailed { address: String, reason: String },
    #[error("shutdown: {0}")]
    ShutdownError(String),
    #[error("connection error: {0}")]
    ConnectionError(String),
    #[error("{task}: {detail}")]
    TaskFailed { task: String, detail: String },
    #[error("{operation}: {detail}")]
    Operation { operation: String, detail: String },
    #[error("{0}")]
    Message(String),
}

impl ServerError {
    pub fn bind_failed(address: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::BindFailed {
            address: address.into(),
            reason: reason.into(),
        }
    }

    pub fn shutdown(detail: impl Into<String>) -> Self {
        Self::ShutdownError(detail.into())
    }

    pub fn connection(detail: impl Into<String>) -> Self {
        Self::ConnectionError(detail.into())
    }

    pub fn task_failed(task: impl Into<String>, detail: impl Into<String>) -> Self {
        Self::TaskFailed {
            task: task.into(),
            detail: detail.into(),
        }
    }

    pub fn operation(operation: impl Into<String>, detail: impl Into<String>) -> Self {
        Self::Operation {
            operation: operation.into(),
            detail: detail.into(),
        }
    }
}

impl From<String> for ServerError {
    fn from(value: String) -> Self {
        Self::Message(value)
    }
}

impl From<&str> for ServerError {
    fn from(value: &str) -> Self {
        Self::Message(value.to_string())
    }
}

/// Structured cluster error domain
#[derive(Debug, Error, Clone)]
pub enum ClusterError {
    #[error("node not found: {0}")]
    NodeNotFound(String),
    #[error("leader election failed: {0}")]
    LeaderElectionFailed(String),
    #[error("raft error: {0}")]
    RaftError(String),
    #[error("bootstrap failed: {0}")]
    BootstrapFailed(String),
    #[error("not ready: {0}")]
    NotReady(String),
    #[error("{operation}: {detail}")]
    Operation { operation: String, detail: String },
    #[error("{0}")]
    Message(String),
}

impl ClusterError {
    pub fn node_not_found(node_id: impl Into<String>) -> Self {
        Self::NodeNotFound(node_id.into())
    }

    pub fn leader_election_failed(detail: impl Into<String>) -> Self {
        Self::LeaderElectionFailed(detail.into())
    }

    pub fn raft(detail: impl Into<String>) -> Self {
        Self::RaftError(detail.into())
    }

    pub fn bootstrap_failed(detail: impl Into<String>) -> Self {
        Self::BootstrapFailed(detail.into())
    }

    pub fn not_ready(reason: impl Into<String>) -> Self {
        Self::NotReady(reason.into())
    }

    pub fn operation(operation: impl Into<String>, detail: impl Into<String>) -> Self {
        Self::Operation {
            operation: operation.into(),
            detail: detail.into(),
        }
    }
}

impl From<String> for ClusterError {
    fn from(value: String) -> Self {
        Self::Message(value)
    }
}

impl From<&str> for ClusterError {
    fn from(value: &str) -> Self {
        Self::Message(value.to_string())
    }
}
