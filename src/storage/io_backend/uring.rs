//! io_uring backend for high-performance async I/O on Linux.
//!
//! `tokio-uring` file handles are thread-local and therefore cannot implement
//! the crate-wide `AsyncFile: Send + Sync` contract directly. This module keeps
//! every raw handle on one dedicated io_uring worker thread and exposes only
//! message-passing handles to the Tokio runtime.

use super::uring_advanced::{
    AdvancedUringManager, AdvancedUringManagerStats, BatchBuilder, BatchConfig, BatchOp,
    FixedFileConfig, FixedFileId, RegisteredBufferConfig, RegisteredBufferId,
};
use super::uring_registration::UringRegistrationManager;
use super::worker::{IoWorker, IoWorkerConfig, OpenMode};
use super::{AsyncFile, AsyncFileSystem, IoResult};
use crate::error::{Result, StreamlineError};
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{debug, warn};

/// A file owned by the dedicated io_uring worker.
pub struct UringFile {
    worker: Arc<IoWorker>,
    file_id: super::FileId,
    raw_fd: i32,
    path: PathBuf,
}

impl UringFile {
    fn new(worker: Arc<IoWorker>, file_id: super::FileId, raw_fd: i32, path: PathBuf) -> Self {
        Self {
            worker,
            file_id,
            raw_fd,
            path,
        }
    }

    /// Return the file path used to open this handle.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Return the worker-owned raw file descriptor.
    ///
    /// The descriptor remains valid only while this `UringFile` is alive.
    pub fn raw_fd(&self) -> i32 {
        self.raw_fd
    }
}

impl Drop for UringFile {
    fn drop(&mut self) {
        self.worker.try_close_file(self.file_id);
    }
}

#[async_trait]
impl AsyncFile for UringFile {
    async fn read_at(&self, mut buf: Vec<u8>, offset: u64) -> IoResult<usize> {
        match self.worker.read(self.file_id, offset, buf.len()).await {
            Ok(bytes) => {
                let read = bytes.len().min(buf.len());
                buf[..read].copy_from_slice(&bytes[..read]);
                (Ok(read), buf)
            }
            Err(error) => (Err(error), buf),
        }
    }

    async fn write_at(&self, buf: Vec<u8>, offset: u64) -> IoResult<usize> {
        let result = self
            .worker
            .write(self.file_id, offset, Bytes::copy_from_slice(&buf))
            .await;
        (result, buf)
    }

    async fn append(&self, buf: Vec<u8>) -> IoResult<usize> {
        let result = self
            .worker
            .append(self.file_id, Bytes::copy_from_slice(&buf))
            .await;
        (result, buf)
    }

    async fn sync_data(&self) -> Result<()> {
        self.worker.sync_data(self.file_id).await
    }

    async fn sync_all(&self) -> Result<()> {
        self.worker.sync_all(self.file_id).await
    }

    async fn size(&self) -> Result<u64> {
        self.worker.size(self.file_id).await
    }

    async fn allocate(&self, len: u64) -> Result<()> {
        self.worker.allocate(self.file_id, len).await
    }
}

/// Filesystem facade backed by one dedicated io_uring worker thread.
pub struct UringFileSystem {
    worker: std::result::Result<Arc<IoWorker>, String>,
}

impl UringFileSystem {
    /// Start an io_uring worker with the default configuration.
    pub fn new() -> Self {
        Self::with_worker_config(IoWorkerConfig::default())
    }

    /// Start an io_uring worker with an explicit configuration.
    pub fn with_worker_config(config: IoWorkerConfig) -> Self {
        let worker = IoWorker::start(config)
            .map(Arc::new)
            .map_err(|error| error.to_string());
        Self { worker }
    }

    fn worker(&self) -> Result<Arc<IoWorker>> {
        self.worker.as_ref().map(Arc::clone).map_err(|error| {
            StreamlineError::storage_msg(format!("Failed to initialize io_uring worker: {error}"))
        })
    }

    async fn open_with_mode(&self, path: &Path, mode: OpenMode) -> Result<UringFile> {
        let worker = self.worker()?;
        let path_buf = path.to_path_buf();
        let (file_id, raw_fd) = worker.open_file_with_fd(path_buf.clone(), mode).await?;
        Ok(UringFile::new(worker, file_id, raw_fd, path_buf))
    }
}

impl Default for UringFileSystem {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl AsyncFileSystem for UringFileSystem {
    type File = UringFile;

    async fn open(&self, path: &Path) -> Result<Self::File> {
        self.open_with_mode(path, OpenMode::Read).await
    }

    async fn open_rw(&self, path: &Path) -> Result<Self::File> {
        self.open_with_mode(path, OpenMode::ReadWrite).await
    }

    async fn create(&self, path: &Path) -> Result<Self::File> {
        self.open_with_mode(path, OpenMode::Create).await
    }

    async fn open_append(&self, path: &Path) -> Result<Self::File> {
        self.open_with_mode(path, OpenMode::Append).await
    }
}

/// Configuration for the enhanced io_uring facade.
#[derive(Debug, Clone)]
pub struct EnhancedUringConfig {
    /// Enable registered buffers.
    pub enable_registered_buffers: bool,
    /// Enable fixed file descriptors.
    pub enable_fixed_files: bool,
    /// Enable batch operations.
    pub enable_batching: bool,
    /// Registered buffer configuration.
    pub buffer_config: RegisteredBufferConfig,
    /// Fixed file configuration.
    pub file_config: FixedFileConfig,
    /// Batch configuration.
    pub batch_config: BatchConfig,
    /// io_uring worker queue depth.
    pub ring_size: u32,
}

impl Default for EnhancedUringConfig {
    fn default() -> Self {
        Self {
            enable_registered_buffers: true,
            enable_fixed_files: true,
            enable_batching: true,
            buffer_config: RegisteredBufferConfig::default(),
            file_config: FixedFileConfig::default(),
            batch_config: BatchConfig::default(),
            ring_size: 256,
        }
    }
}

/// An io_uring file with registered-buffer and fixed-file metadata.
pub struct EnhancedUringFile {
    inner: UringFile,
    fixed_id: Option<FixedFileId>,
    kernel_slot: Option<u32>,
    manager: Arc<AdvancedUringManager>,
    registration: Option<Arc<Mutex<UringRegistrationManager>>>,
    path_to_fixed: Arc<RwLock<HashMap<PathBuf, Vec<FixedFileId>>>>,
}

impl EnhancedUringFile {
    fn new(
        inner: UringFile,
        fixed_id: Option<FixedFileId>,
        kernel_slot: Option<u32>,
        manager: Arc<AdvancedUringManager>,
        registration: Option<Arc<Mutex<UringRegistrationManager>>>,
        path_to_fixed: Arc<RwLock<HashMap<PathBuf, Vec<FixedFileId>>>>,
    ) -> Self {
        Self {
            inner,
            fixed_id,
            kernel_slot,
            manager,
            registration,
            path_to_fixed,
        }
    }

    /// Get the fixed file ID if registration succeeded.
    pub fn fixed_id(&self) -> Option<FixedFileId> {
        self.fixed_id
    }

    /// Check whether this file has a fixed-file registry entry.
    pub fn is_fixed(&self) -> bool {
        self.fixed_id.is_some()
    }

    /// Get the original file path.
    pub fn path(&self) -> &Path {
        self.inner.path()
    }

    /// Acquire a registered buffer of at least `min_size` bytes.
    pub fn acquire_buffer(&self, min_size: usize) -> Option<RegisteredBufferId> {
        self.manager.buffer_pool().acquire(min_size).ok()
    }

    /// Release a registered buffer back to the pool.
    pub fn release_buffer(&self, id: RegisteredBufferId) {
        if let Err(error) = self.manager.buffer_pool().release(id) {
            warn!(%error, "Failed to release registered io_uring buffer");
        }
    }

    /// Read into an acquired registered buffer.
    ///
    /// The returned buffer ID remains acquired and must be released by the caller.
    pub async fn read_registered(
        &self,
        offset: u64,
        min_size: usize,
    ) -> Result<(usize, RegisteredBufferId)> {
        let buffer_id = self.acquire_buffer(min_size).ok_or_else(|| {
            StreamlineError::storage_msg("No registered buffer available".to_string())
        })?;
        let capacity = self.manager.buffer_pool().buffer(buffer_id)?.len();
        let (result, data) = self.inner.read_at(vec![0; capacity], offset).await;
        match result {
            Ok(read) => {
                self.manager
                    .buffer_pool()
                    .write_buffer(buffer_id, 0, &data[..read])?;
                Ok((read, buffer_id))
            }
            Err(error) => {
                self.release_buffer(buffer_id);
                Err(error)
            }
        }
    }

    /// Write data through the worker-owned file.
    pub async fn write_registered(&self, data: &[u8], offset: u64) -> Result<usize> {
        let (result, _) = self.inner.write_at(data.to_vec(), offset).await;
        result
    }

    /// Get the worker-owned raw file descriptor.
    pub fn raw_fd(&self) -> i32 {
        self.inner.raw_fd()
    }
}

impl Drop for EnhancedUringFile {
    fn drop(&mut self) {
        if let (Some(registration), Some(slot)) = (&self.registration, self.kernel_slot) {
            if let Err(error) = registration.lock().unregister_file(slot) {
                warn!(%error, "Failed to unregister kernel io_uring file slot");
            }
        }
        if let Some(fixed_id) = self.fixed_id {
            match self.manager.file_registry().unregister(fixed_id) {
                Ok(true) => {
                    let mut paths = self.path_to_fixed.write();
                    if let Some(ids) = paths.get_mut(self.inner.path()) {
                        ids.retain(|id| *id != fixed_id);
                        if ids.is_empty() {
                            paths.remove(self.inner.path());
                        }
                    }
                }
                Ok(false) => {}
                Err(error) => {
                    warn!(%error, "Failed to unregister fixed io_uring file");
                }
            }
        }
    }
}

#[async_trait]
impl AsyncFile for EnhancedUringFile {
    async fn read_at(&self, buf: Vec<u8>, offset: u64) -> IoResult<usize> {
        self.inner.read_at(buf, offset).await
    }

    async fn write_at(&self, buf: Vec<u8>, offset: u64) -> IoResult<usize> {
        self.inner.write_at(buf, offset).await
    }

    async fn append(&self, buf: Vec<u8>) -> IoResult<usize> {
        self.inner.append(buf).await
    }

    async fn sync_data(&self) -> Result<()> {
        self.inner.sync_data().await
    }

    async fn sync_all(&self) -> Result<()> {
        self.inner.sync_all().await
    }

    async fn size(&self) -> Result<u64> {
        self.inner.size().await
    }

    async fn allocate(&self, len: u64) -> Result<()> {
        self.inner.allocate(len).await
    }
}

/// Enhanced io_uring filesystem using the same worker-isolation boundary.
pub struct EnhancedUringFileSystem {
    inner: UringFileSystem,
    manager: Arc<AdvancedUringManager>,
    registration: Option<Arc<Mutex<UringRegistrationManager>>>,
    config: EnhancedUringConfig,
    path_to_fixed: Arc<RwLock<HashMap<PathBuf, Vec<FixedFileId>>>>,
}

impl EnhancedUringFileSystem {
    /// Create an enhanced filesystem with default settings.
    pub fn new() -> Self {
        Self::with_config(EnhancedUringConfig::default())
    }

    /// Create an enhanced filesystem with explicit settings.
    pub fn with_config(config: EnhancedUringConfig) -> Self {
        debug!(
            buffers = config.enable_registered_buffers,
            files = config.enable_fixed_files,
            batching = config.enable_batching,
            "Initializing enhanced io_uring filesystem"
        );
        let manager = Arc::new(AdvancedUringManager::from_configs(
            config.buffer_config.clone(),
            config.file_config.clone(),
        ));
        let worker_config = IoWorkerConfig {
            sq_depth: config.ring_size,
            ..Default::default()
        };
        Self {
            inner: UringFileSystem::with_worker_config(worker_config),
            manager,
            registration: None,
            config,
            path_to_fixed: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Attach optional low-level registrations to an externally owned ring.
    pub fn init_registration(&mut self, ring_fd: i32) -> Result<()> {
        let mut registration = UringRegistrationManager::new(ring_fd);
        if self.config.enable_registered_buffers {
            let mut sizes = Vec::new();
            'classes: for &size in &self.config.buffer_config.size_classes {
                for _ in 0..self.config.buffer_config.buffers_per_class {
                    if sizes.len() >= self.config.buffer_config.max_registered {
                        break 'classes;
                    }
                    sizes.push(size);
                }
            }
            registration.init_buffers(&sizes)?;
        }
        if self.config.enable_fixed_files {
            registration.init_files(self.config.file_config.max_files)?;
        }
        self.registration = Some(Arc::new(Mutex::new(registration)));
        Ok(())
    }

    /// Get the advanced buffer and file registry manager.
    pub fn manager(&self) -> &Arc<AdvancedUringManager> {
        &self.manager
    }

    /// Get an enhanced filesystem statistics snapshot.
    pub fn stats(&self) -> EnhancedUringStats {
        let manager_stats = self.manager.stats();
        EnhancedUringStats {
            registered_files: manager_stats.file_registry.registered_files,
            manager_stats,
            config: self.config.clone(),
        }
    }

    /// Create a batch builder with this filesystem's limits.
    pub fn batch_builder(&self) -> BatchBuilder {
        BatchBuilder::with_config(self.config.batch_config.clone())
    }

    /// Validate and account for a batch.
    ///
    /// Actual submission remains the responsibility of the worker-backed file
    /// operations because the batch descriptors contain registry indices, not
    /// file handles.
    pub async fn execute_batch(&self, batch: Vec<BatchOp>) -> Vec<Result<usize>> {
        if !self.config.enable_batching {
            return batch
                .into_iter()
                .map(|_| {
                    Err(StreamlineError::storage_msg(
                        "io_uring batching is disabled".to_string(),
                    ))
                })
                .collect();
        }

        self.manager.record_batch_submission(batch.len());
        batch
            .into_iter()
            .map(|operation| match operation {
                BatchOp::Read { len, .. } | BatchOp::Write { len, .. } => Ok(len),
                BatchOp::SyncData { .. } | BatchOp::SyncAll { .. } | BatchOp::Nop => Ok(0),
            })
            .collect()
    }

    /// Get the fixed-file ID currently associated with a path.
    pub fn get_fixed_id(&self, path: &Path) -> Option<FixedFileId> {
        self.path_to_fixed
            .read()
            .get(path)
            .and_then(|ids| ids.last().copied())
    }

    fn register_fixed_file(&self, path: &Path, fd: i32) -> (Option<FixedFileId>, Option<u32>) {
        if !self.config.enable_fixed_files {
            return (None, None);
        }

        let fixed_id = match self
            .manager
            .file_registry()
            .register(fd, path.to_path_buf())
        {
            Ok(id) => id,
            Err(error) => {
                warn!(%error, path = %path.display(), "Failed to register fixed io_uring file");
                return (None, None);
            }
        };
        self.path_to_fixed
            .write()
            .entry(path.to_path_buf())
            .or_default()
            .push(fixed_id);

        let kernel_slot = self.registration.as_ref().and_then(|registration| {
            match registration.lock().register_file(fd) {
                Ok(slot) => Some(slot),
                Err(error) => {
                    warn!(%error, path = %path.display(), "Failed to update kernel file registration");
                    None
                }
            }
        });
        (Some(fixed_id), kernel_slot)
    }

    async fn open_enhanced(&self, path: &Path, mode: OpenMode) -> Result<EnhancedUringFile> {
        let inner = self.inner.open_with_mode(path, mode).await?;
        let (fixed_id, kernel_slot) = self.register_fixed_file(path, inner.raw_fd());
        Ok(EnhancedUringFile::new(
            inner,
            fixed_id,
            kernel_slot,
            Arc::clone(&self.manager),
            self.registration.clone(),
            Arc::clone(&self.path_to_fixed),
        ))
    }
}

impl Default for EnhancedUringFileSystem {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl AsyncFileSystem for EnhancedUringFileSystem {
    type File = EnhancedUringFile;

    async fn open(&self, path: &Path) -> Result<Self::File> {
        self.open_enhanced(path, OpenMode::Read).await
    }

    async fn open_rw(&self, path: &Path) -> Result<Self::File> {
        self.open_enhanced(path, OpenMode::ReadWrite).await
    }

    async fn create(&self, path: &Path) -> Result<Self::File> {
        self.open_enhanced(path, OpenMode::Create).await
    }

    async fn open_append(&self, path: &Path) -> Result<Self::File> {
        self.open_enhanced(path, OpenMode::Append).await
    }
}

/// Statistics for the enhanced io_uring filesystem.
#[derive(Debug, Clone)]
pub struct EnhancedUringStats {
    /// Advanced manager statistics.
    pub manager_stats: AdvancedUringManagerStats,
    /// Number of registered fixed files.
    pub registered_files: usize,
    /// Active enhanced configuration.
    pub config: EnhancedUringConfig,
}

/// Check whether the running Linux kernel is new enough for io_uring.
pub fn is_uring_available() -> bool {
    let Ok(version) = std::fs::read_to_string("/proc/version") else {
        return false;
    };
    let Some(version) = version.split_whitespace().nth(2) else {
        return false;
    };
    let mut components = version.split('.');
    let Some(major) = components
        .next()
        .and_then(|value| value.parse::<u32>().ok())
    else {
        return false;
    };
    let Some(minor) = components
        .next()
        .and_then(|value| value.parse::<u32>().ok())
    else {
        return false;
    };
    major > 5 || (major == 5 && minor >= 11)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uring_availability() {
        let _ = is_uring_available();
    }
}
