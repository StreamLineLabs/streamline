//! I/O worker thread management for io_uring
//!
//! Since tokio-uring requires its own runtime (and is `!Sync`), we use a dedicated
//! worker thread with a message-passing interface for I/O operations.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │  Main Tokio Runtime (epoll-based)                           │
//! │                                                              │
//! │  Network I/O ──► Protocol Handler ──► Storage Layer         │
//! │                                            │                 │
//! │                                            ▼                 │
//! │                                      IoWorker Client         │
//! │                                            │                 │
//! └────────────────────────────────────────────┼─────────────────┘
//!                                              │
//!                                       (mpsc channel)
//!                                              │
//! ┌────────────────────────────────────────────▼─────────────────┐
//! │  I/O Worker Thread                                           │
//! │                                                              │
//! │  tokio-uring runtime ──► File Operations ──► Disk           │
//! │                                                              │
//! └──────────────────────────────────────────────────────────────┘
//! ```

use super::buffer_pool::IoBufferPool;
use super::types::FileId;
use crate::error::{Result, StreamlineError};
use bytes::Bytes;
use std::collections::HashMap;
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use tokio::sync::{mpsc, oneshot};
use tokio_uring::fs::{File, OpenOptions};
use tracing::{debug, error, info};

/// I/O worker configuration
#[derive(Debug, Clone)]
pub struct IoWorkerConfig {
    /// Request queue depth (default: 1024)
    pub queue_depth: usize,

    /// io_uring submission queue depth (default: 256)
    pub sq_depth: u32,

    /// Buffer pool size per size class (default: 64)
    pub buffer_pool_size: usize,

    /// Pre-allocate WAL files
    pub preallocate_wal: bool,

    /// Pre-allocation size for WAL files (default: 64MB)
    pub preallocate_size: u64,
}

impl Default for IoWorkerConfig {
    fn default() -> Self {
        Self {
            queue_depth: 1024,
            sq_depth: 256,
            buffer_pool_size: 64,
            preallocate_wal: true,
            preallocate_size: 64 * 1024 * 1024,
        }
    }
}

/// I/O request types
pub enum IoRequest {
    /// Read from a file
    Read {
        file_id: FileId,
        offset: u64,
        len: usize,
        response: oneshot::Sender<Result<Bytes>>,
    },
    /// Write to a file
    Write {
        file_id: FileId,
        offset: u64,
        data: Bytes,
        response: oneshot::Sender<Result<usize>>,
    },
    /// Append to a file
    Append {
        file_id: FileId,
        data: Bytes,
        response: oneshot::Sender<Result<usize>>,
    },
    /// Sync file data
    SyncData {
        file_id: FileId,
        response: oneshot::Sender<Result<()>>,
    },
    /// Sync file data and metadata
    SyncAll {
        file_id: FileId,
        response: oneshot::Sender<Result<()>>,
    },
    /// Get file size
    Size {
        file_id: FileId,
        response: oneshot::Sender<Result<u64>>,
    },
    /// Pre-allocate file space
    Allocate {
        file_id: FileId,
        len: u64,
        response: oneshot::Sender<Result<()>>,
    },
    /// Open a file
    OpenFile {
        path: PathBuf,
        mode: OpenMode,
        response: oneshot::Sender<Result<OpenFileLease>>,
    },
    /// Shutdown the worker
    Shutdown,
}

enum ControlRequest {
    Close {
        file_id: FileId,
        response: Option<oneshot::Sender<()>>,
    },
}

/// A delivered open result that closes the worker-owned file unless claimed.
pub struct OpenFileLease {
    file_id: FileId,
    raw_fd: i32,
    control_tx: mpsc::UnboundedSender<ControlRequest>,
    claimed: bool,
}

impl OpenFileLease {
    fn new(
        file_id: FileId,
        raw_fd: i32,
        control_tx: mpsc::UnboundedSender<ControlRequest>,
    ) -> Self {
        Self {
            file_id,
            raw_fd,
            control_tx,
            claimed: false,
        }
    }

    fn claim(mut self) -> (FileId, i32) {
        self.claimed = true;
        (self.file_id, self.raw_fd)
    }
}

impl Drop for OpenFileLease {
    fn drop(&mut self) {
        if !self.claimed {
            let _ = self.control_tx.send(ControlRequest::Close {
                file_id: self.file_id,
                response: None,
            });
        }
    }
}

/// File open mode for worker
#[derive(Debug, Clone, Copy)]
pub enum OpenMode {
    Read,
    ReadWrite,
    Create,
    Append,
}

/// I/O worker handle
pub struct IoWorker {
    tx: mpsc::Sender<IoRequest>,
    control_tx: mpsc::UnboundedSender<ControlRequest>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl IoWorker {
    /// Start a new I/O worker thread
    pub fn start(config: IoWorkerConfig) -> Result<Self> {
        let (tx, rx) = mpsc::channel(config.queue_depth);
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let worker_control_tx = control_tx.clone();

        let handle = std::thread::Builder::new()
            .name("streamline-io".to_string())
            .spawn(move || {
                // Create tokio-uring runtime in the worker thread
                let mut builder = tokio_uring::builder();
                builder.entries(config.sq_depth);
                builder.start(async move {
                    if let Err(e) = Self::run_loop(rx, control_rx, worker_control_tx, config).await
                    {
                        error!("I/O worker error: {}", e);
                    }
                });
            })
            .map_err(|e| {
                StreamlineError::Internal(format!("Failed to spawn I/O worker thread: {e}"))
            })?;

        info!("I/O worker thread started");

        Ok(Self {
            tx,
            control_tx,
            handle: Some(handle),
        })
    }

    fn apply_control<T>(files: &mut HashMap<FileId, T>, control: ControlRequest) {
        match control {
            ControlRequest::Close { file_id, response } => {
                files.remove(&file_id);
                if let Some(response) = response {
                    let _ = response.send(());
                }
            }
        }
    }

    fn drain_controls<T>(
        files: &mut HashMap<FileId, T>,
        control_rx: &mut mpsc::UnboundedReceiver<ControlRequest>,
    ) {
        while let Ok(control) = control_rx.try_recv() {
            Self::apply_control(files, control);
        }
    }

    fn finish_open<T>(
        files: &mut HashMap<FileId, T>,
        file_id: FileId,
        raw_fd: i32,
        control_tx: &mpsc::UnboundedSender<ControlRequest>,
        response: oneshot::Sender<Result<OpenFileLease>>,
    ) {
        let lease = OpenFileLease::new(file_id, raw_fd, control_tx.clone());
        if let Err(Ok(mut undelivered)) = response.send(Ok(lease)) {
            undelivered.claimed = true;
            files.remove(&file_id);
        }
    }

    /// Main worker loop
    async fn run_loop(
        mut rx: mpsc::Receiver<IoRequest>,
        mut control_rx: mpsc::UnboundedReceiver<ControlRequest>,
        control_tx: mpsc::UnboundedSender<ControlRequest>,
        config: IoWorkerConfig,
    ) -> Result<()> {
        let mut files: HashMap<FileId, File> = HashMap::new();
        let mut next_id = 0u64;
        let buffer_pool = IoBufferPool::new(
            vec![4 * 1024, 16 * 1024, 64 * 1024, 256 * 1024, 1024 * 1024],
            config.buffer_pool_size,
        );

        debug!("I/O worker loop started");

        loop {
            let request = tokio::select! {
                biased;
                control = control_rx.recv() => {
                    if let Some(control) = control {
                        Self::apply_control(&mut files, control);
                        Self::drain_controls(&mut files, &mut control_rx);
                        continue;
                    }
                    rx.recv().await
                }
                request = rx.recv() => request,
            };
            let Some(request) = request else {
                Self::drain_controls(&mut files, &mut control_rx);
                break;
            };

            match request {
                IoRequest::Read {
                    file_id,
                    offset,
                    len,
                    response,
                } => {
                    if let Some(file) = files.get(&file_id) {
                        let buf = buffer_pool.acquire(len);
                        let (result, buf) = file.read_at(buf, offset).await;
                        let bytes = result
                            .map(|n| Bytes::copy_from_slice(&buf[..n]))
                            .map_err(|e| StreamlineError::storage_msg(format!("Read failed: {e}")));
                        buffer_pool.release(buf);
                        let _ = response.send(bytes);
                    } else {
                        let _ = response.send(Err(StreamlineError::storage_msg(format!(
                            "File not found: {file_id:?}"
                        ))));
                    }
                }

                IoRequest::Write {
                    file_id,
                    offset,
                    data,
                    response,
                } => {
                    if let Some(file) = files.get(&file_id) {
                        let (result, _) = file.write_at(data.to_vec(), offset).submit().await;
                        let _ = response.send(result.map_err(StreamlineError::from));
                    } else {
                        let _ = response.send(Err(StreamlineError::storage_msg(format!(
                            "File not found: {file_id:?}"
                        ))));
                    }
                }

                IoRequest::Append {
                    file_id,
                    data,
                    response,
                } => {
                    if let Some(file) = files.get(&file_id) {
                        let result = match file.statx().await {
                            Ok(stat) => {
                                let (result, _) =
                                    file.write_at(data.to_vec(), stat.stx_size).submit().await;
                                result.map_err(StreamlineError::from)
                            }
                            Err(error) => Err(StreamlineError::from(error)),
                        };
                        let _ = response.send(result);
                    } else {
                        let _ = response.send(Err(StreamlineError::storage_msg(format!(
                            "File not found: {file_id:?}"
                        ))));
                    }
                }

                IoRequest::SyncData { file_id, response } => {
                    if let Some(file) = files.get(&file_id) {
                        let result = file.sync_data().await.map_err(StreamlineError::from);
                        let _ = response.send(result);
                    } else {
                        let _ = response.send(Err(StreamlineError::storage_msg(format!(
                            "File not found: {file_id:?}"
                        ))));
                    }
                }

                IoRequest::SyncAll { file_id, response } => {
                    if let Some(file) = files.get(&file_id) {
                        let result = file.sync_all().await.map_err(StreamlineError::from);
                        let _ = response.send(result);
                    } else {
                        let _ = response.send(Err(StreamlineError::storage_msg(format!(
                            "File not found: {file_id:?}"
                        ))));
                    }
                }

                IoRequest::Size { file_id, response } => {
                    if let Some(file) = files.get(&file_id) {
                        let result = file
                            .statx()
                            .await
                            .map(|stat| stat.stx_size)
                            .map_err(StreamlineError::from);
                        let _ = response.send(result);
                    } else {
                        let _ = response.send(Err(StreamlineError::storage_msg(format!(
                            "File not found: {file_id:?}"
                        ))));
                    }
                }

                IoRequest::Allocate {
                    file_id,
                    len,
                    response,
                } => {
                    if let Some(file) = files.get(&file_id) {
                        let result = file
                            .fallocate(0, len, 0)
                            .await
                            .map_err(StreamlineError::from);
                        let _ = response.send(result);
                    } else {
                        let _ = response.send(Err(StreamlineError::storage_msg(format!(
                            "File not found: {file_id:?}"
                        ))));
                    }
                }

                IoRequest::OpenFile {
                    path,
                    mode,
                    response,
                } => {
                    let file_result = match mode {
                        OpenMode::Read => File::open(&path).await,
                        OpenMode::ReadWrite => {
                            OpenOptions::new().read(true).write(true).open(&path).await
                        }
                        OpenMode::Create => {
                            OpenOptions::new()
                                .read(true)
                                .write(true)
                                .create(true)
                                .truncate(true)
                                .open(&path)
                                .await
                        }
                        OpenMode::Append => {
                            OpenOptions::new()
                                .read(true)
                                .write(true)
                                .create(true)
                                .append(true)
                                .open(&path)
                                .await
                        }
                    };

                    match file_result {
                        Ok(file) => {
                            let file_id = FileId::new(next_id);
                            next_id += 1;
                            let raw_fd = file.as_raw_fd();
                            files.insert(file_id, file);
                            Self::finish_open(&mut files, file_id, raw_fd, &control_tx, response);
                        }
                        Err(e) => {
                            let _ = response.send(Err(StreamlineError::storage_msg(format!(
                                "Open failed: {e}"
                            ))));
                        }
                    }
                }

                IoRequest::Shutdown => {
                    Self::drain_controls(&mut files, &mut control_rx);
                    info!("I/O worker shutting down");
                    break;
                }
            }
        }

        debug!("I/O worker loop exited");
        Ok(())
    }

    /// Send a read request
    pub async fn read(&self, file_id: FileId, offset: u64, len: usize) -> Result<Bytes> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(IoRequest::Read {
                file_id,
                offset,
                len,
                response: tx,
            })
            .await
            .map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to send read request: {e}"))
            })?;
        rx.await.map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to receive read response: {e}"))
        })?
    }

    /// Send a write request
    pub async fn write(&self, file_id: FileId, offset: u64, data: Bytes) -> Result<usize> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(IoRequest::Write {
                file_id,
                offset,
                data,
                response: tx,
            })
            .await
            .map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to send write request: {e}"))
            })?;
        rx.await.map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to receive write response: {e}"))
        })?
    }

    /// Send an append request
    pub async fn append(&self, file_id: FileId, data: Bytes) -> Result<usize> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(IoRequest::Append {
                file_id,
                data,
                response: tx,
            })
            .await
            .map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to send append request: {e}"))
            })?;
        rx.await.map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to receive append response: {e}"))
        })?
    }

    /// Send a sync data request
    pub async fn sync_data(&self, file_id: FileId) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(IoRequest::SyncData {
                file_id,
                response: tx,
            })
            .await
            .map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to send sync request: {e}"))
            })?;
        rx.await.map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to receive sync response: {e}"))
        })?
    }

    /// Send a sync all request
    pub async fn sync_all(&self, file_id: FileId) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(IoRequest::SyncAll {
                file_id,
                response: tx,
            })
            .await
            .map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to send sync request: {e}"))
            })?;
        rx.await.map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to receive sync response: {e}"))
        })?
    }

    /// Get the current file size.
    pub async fn size(&self, file_id: FileId) -> Result<u64> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(IoRequest::Size {
                file_id,
                response: tx,
            })
            .await
            .map_err(|error| {
                StreamlineError::storage_msg(format!("Failed to send size request: {error}"))
            })?;
        rx.await.map_err(|error| {
            StreamlineError::storage_msg(format!("Failed to receive size response: {error}"))
        })?
    }

    /// Pre-allocate file space.
    pub async fn allocate(&self, file_id: FileId, len: u64) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(IoRequest::Allocate {
                file_id,
                len,
                response: tx,
            })
            .await
            .map_err(|error| {
                StreamlineError::storage_msg(format!("Failed to send allocate request: {error}"))
            })?;
        rx.await.map_err(|error| {
            StreamlineError::storage_msg(format!("Failed to receive allocate response: {error}"))
        })?
    }

    /// Open a file
    pub async fn open_file(&self, path: PathBuf, mode: OpenMode) -> Result<FileId> {
        self.open_file_with_fd(path, mode)
            .await
            .map(|(file_id, _)| file_id)
    }

    /// Open a file and return both its worker ID and raw descriptor.
    pub(crate) async fn open_file_with_fd(
        &self,
        path: PathBuf,
        mode: OpenMode,
    ) -> Result<(FileId, i32)> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(IoRequest::OpenFile {
                path,
                mode,
                response: tx,
            })
            .await
            .map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to send open request: {e}"))
            })?;
        let lease = rx.await.map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to receive open response: {e}"))
        })??;
        Ok(lease.claim())
    }

    /// Queue a close independently of the bounded data-operation queue.
    pub(crate) fn try_close_file(&self, file_id: FileId) {
        let _ = self.control_tx.send(ControlRequest::Close {
            file_id,
            response: None,
        });
    }

    /// Close a file
    pub async fn close_file(&self, file_id: FileId) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.control_tx
            .send(ControlRequest::Close {
                file_id,
                response: Some(tx),
            })
            .map_err(|_| StreamlineError::storage_msg("I/O worker has stopped".to_string()))?;
        rx.await
            .map_err(|_| StreamlineError::storage_msg("I/O worker close was cancelled".to_string()))
    }

    /// Shutdown the worker
    pub async fn shutdown(mut self) -> Result<()> {
        debug!("Shutting down I/O worker");
        let _ = self.tx.send(IoRequest::Shutdown).await;

        if let Some(handle) = self.handle.take() {
            handle.join().map_err(|_| {
                StreamlineError::storage_msg("Failed to join I/O worker thread".into())
            })?;
        }

        info!("I/O worker shutdown complete");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    struct TrackedFile(Arc<AtomicUsize>);

    impl Drop for TrackedFile {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn dropped_files_close_when_the_request_queue_is_saturated() {
        struct DropFile {
            worker: Arc<IoWorker>,
            file_id: FileId,
        }

        impl Drop for DropFile {
            fn drop(&mut self) {
                self.worker.try_close_file(self.file_id);
            }
        }

        let (tx, _rx) = mpsc::channel(1);
        assert!(tx.try_send(IoRequest::Shutdown).is_ok());
        let (control_tx, mut control_rx) = mpsc::unbounded_channel();
        let worker = Arc::new(IoWorker {
            tx,
            control_tx,
            handle: None,
        });

        let mut files: HashMap<FileId, ()> = (0..1024).map(|id| (FileId::new(id), ())).collect();
        for id in 0..1024 {
            drop(DropFile {
                worker: Arc::clone(&worker),
                file_id: FileId::new(id),
            });
        }

        IoWorker::drain_controls(&mut files, &mut control_rx);
        assert!(
            files.is_empty(),
            "the unbounded close path must eventually release every file ID"
        );
    }

    #[test]
    fn cancelled_open_before_delivery_drops_the_new_file_immediately() {
        let dropped = Arc::new(AtomicUsize::new(0));
        let file_id = FileId::new(7);
        let mut files = HashMap::from([(file_id, TrackedFile(Arc::clone(&dropped)))]);
        let (control_tx, mut control_rx) = mpsc::unbounded_channel();
        let (response, receiver) = oneshot::channel();
        drop(receiver);

        IoWorker::finish_open(&mut files, file_id, 42, &control_tx, response);

        assert!(files.is_empty());
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert!(control_rx.try_recv().is_err());
    }

    #[test]
    fn cancelled_open_after_delivery_closes_the_unclaimed_file() {
        let dropped = Arc::new(AtomicUsize::new(0));
        let file_id = FileId::new(8);
        let mut files = HashMap::from([(file_id, TrackedFile(Arc::clone(&dropped)))]);
        let (control_tx, mut control_rx) = mpsc::unbounded_channel();
        let (response, receiver) = oneshot::channel();

        IoWorker::finish_open(&mut files, file_id, 43, &control_tx, response);
        assert!(files.contains_key(&file_id));

        drop(receiver);
        IoWorker::drain_controls(&mut files, &mut control_rx);

        assert!(files.is_empty());
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn repeated_unclaimed_opens_close_every_file() {
        let dropped = Arc::new(AtomicUsize::new(0));
        let mut files = HashMap::new();
        let (control_tx, mut control_rx) = mpsc::unbounded_channel();
        let mut receivers = Vec::new();

        for raw_id in 0..128 {
            let file_id = FileId::new(raw_id);
            files.insert(file_id, TrackedFile(Arc::clone(&dropped)));
            let (response, receiver) = oneshot::channel();
            IoWorker::finish_open(&mut files, file_id, raw_id as i32, &control_tx, response);
            receivers.push(receiver);
        }

        drop(receivers);
        IoWorker::drain_controls(&mut files, &mut control_rx);

        assert!(files.is_empty());
        assert_eq!(dropped.load(Ordering::SeqCst), 128);
    }

    #[test]
    fn claimed_open_remains_owned_until_explicitly_closed() {
        let dropped = Arc::new(AtomicUsize::new(0));
        let file_id = FileId::new(9);
        let mut files = HashMap::from([(file_id, TrackedFile(Arc::clone(&dropped)))]);
        let (control_tx, mut control_rx) = mpsc::unbounded_channel();
        let (response, mut receiver) = oneshot::channel();

        IoWorker::finish_open(&mut files, file_id, 44, &control_tx, response);
        let lease = receiver
            .try_recv()
            .expect("the worker must deliver the open result")
            .expect("the open result must be successful");
        assert_eq!(lease.claim(), (file_id, 44));

        IoWorker::drain_controls(&mut files, &mut control_rx);
        assert!(files.contains_key(&file_id));
        assert_eq!(dropped.load(Ordering::SeqCst), 0);

        files.remove(&file_id);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }
}
