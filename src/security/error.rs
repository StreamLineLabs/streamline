//! Errors raised by KMS adapters.

use std::fmt;

pub type KmsResult<T> = Result<T, KmsError>;

#[derive(Debug)]
pub enum KmsError {
    KeyNotFound(String),
    InvalidSignature,
    Backend(String),
    UnsupportedAlgorithm(String),
    Configuration(String),
}

impl fmt::Display for KmsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::KeyNotFound(id) => write!(f, "kms: key not found: {id}"),
            Self::InvalidSignature => write!(f, "kms: invalid signature"),
            Self::Backend(msg) => write!(f, "kms: backend error: {msg}"),
            Self::UnsupportedAlgorithm(a) => write!(f, "kms: unsupported algorithm: {a}"),
            Self::Configuration(msg) => write!(f, "kms: configuration error: {msg}"),
        }
    }
}

impl std::error::Error for KmsError {}
