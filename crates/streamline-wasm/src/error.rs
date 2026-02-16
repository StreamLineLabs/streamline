/// Errors from the WASM transformation engine.
#[derive(Debug, thiserror::Error)]
pub enum WasmError {
    #[error("WASM configuration error: {0}")]
    Configuration(String),

    #[error("WASM storage/IO error: {0}")]
    Storage(String),

    #[error("WASM execution timeout: {0}")]
    Timeout(String),

    #[error("WASM error: {0}")]
    Wasm(String),

    #[error("WASM validation error: {0}")]
    Validation(String),

    #[error("WASM internal error: {0}")]
    Internal(String),

    #[error("Resource exhausted: {0}")]
    ResourceExhausted(String),
}

pub type Result<T> = std::result::Result<T, WasmError>;
