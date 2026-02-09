// Buffer strategies are scaffolding for hybrid/diskless storage modes (not yet fully integrated)
#![allow(dead_code)]

//! Buffer strategies for Streamline storage
//!
//! This module provides different buffering strategies for managing in-flight
//! records before they are persisted. The strategy used depends on the storage mode:
//!
//! - **Local mode**: Write-through with minimal buffering
//! - **Hybrid/Diskless mode**: Batching buffer for efficient object storage writes
//!
//! ## Buffer Types
//!
//! - [`LocalBuffer`]: Minimal buffering for immediate writes (local storage)
//! - [`BatchingBuffer`]: Accumulates records for batch writes (object storage)

mod batching;
mod local;

pub(crate) use batching::BatchingBuffer;
pub(crate) use local::LocalBuffer;

use crate::storage::backend::{BufferConfig, BufferStrategy};
use crate::storage::storage_mode::{RemoteStorageConfig, StorageMode};

/// Factory for creating buffer strategies based on storage mode
pub(crate) struct BufferFactory;

impl BufferFactory {
    /// Create a buffer strategy appropriate for the given storage mode
    pub fn create(
        mode: StorageMode,
        remote_config: Option<&RemoteStorageConfig>,
    ) -> Box<dyn BufferStrategy> {
        match mode {
            StorageMode::Local => Box::new(LocalBuffer::new()),
            StorageMode::Hybrid | StorageMode::Diskless => {
                let config = remote_config.cloned().unwrap_or_default();
                Box::new(BatchingBuffer::new(
                    config.batch_size_bytes,
                    config.batch_timeout_ms,
                ))
            }
        }
    }

    /// Create a buffer with custom configuration
    pub fn create_with_config(config: BufferConfig) -> Box<dyn BufferStrategy> {
        if config.max_size_bytes == 0 && config.max_linger_ms == 0 {
            Box::new(LocalBuffer::new())
        } else {
            Box::new(BatchingBuffer::from_config(config))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_factory_local() {
        let buffer = BufferFactory::create(StorageMode::Local, None);
        assert!(!buffer.should_flush()); // Empty buffer shouldn't flush
    }

    #[test]
    fn test_buffer_factory_diskless() {
        let remote_config = RemoteStorageConfig::default();
        let buffer = BufferFactory::create(StorageMode::Diskless, Some(&remote_config));
        assert!(!buffer.should_flush()); // Empty buffer shouldn't flush
    }

    #[test]
    fn test_buffer_factory_hybrid() {
        let remote_config = RemoteStorageConfig::default();
        let buffer = BufferFactory::create(StorageMode::Hybrid, Some(&remote_config));
        assert!(!buffer.should_flush()); // Empty buffer shouldn't flush
    }
}
