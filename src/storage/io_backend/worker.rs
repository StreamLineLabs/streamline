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
use super::UringFile;
use crate::error::{Result, StreamlineError};
use bytes::Bytes;
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::sync::{mpsc, oneshot};
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
    /// Open a file
    OpenFile {
        path: PathBuf,
        mode: OpenMode,
        response: oneshot::Sender<Result<FileId>>,
    },
    /// Close a file
    CloseFile { file_id: FileId },
    /// Shutdown the worker
    Shutdown,
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
    handle: Option<std::thread::JoinHandle<()>>,
}

impl IoWorker {
    /// Start a new I/O worker thread
    pub fn start(config: IoWorkerConfig) -> Result<Self> {
        let (tx, rx) = mpsc::channel(config.queue_depth);

        let handle = std::thread::Builder::new()
            .name("streamline-io".to_string())
            .spawn(move || {
                // Create tokio-uring runtime in the worker thread
                tokio_uring::start(async move {
                    if let Err(e) = Self::run_loop(rx, config).await {
                        error!("I/O worker error: {}", e);
                    }
                });
            })
            .map_err(|e| StreamlineError::Internal(format!("Failed to spawn I/O worker thread: {}", e)))?;

        info!("I/O worker thread started");

        Ok(Self {
            tx,
            handle: Some(handle),
        })
    }

    /// Main worker loop
    async fn run_loop(mut rx: mpsc::Receiver<IoRequest>, config: IoWorkerConfig) -> Result<()> {
        let mut files: HashMap<FileId, UringFile> = HashMap::new();
        let mut next_id = 0u64;
        let buffer_pool = IoBufferPool::default_pool();

        debug!("I/O worker loop started");

        while let Some(request) = rx.recv().await {
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
                        let bytes =
                            result
                                .map(|n| Bytes::copy_from_slice(&buf[..n]))
                                .map_err(|e| {
                                    StreamlineError::storage_msg(format!("Read failed: {}", e))
                                });
                        buffer_pool.release(buf);
                        let _ = response.send(bytes);
                    } else {
                        let _ = response.send(Err(StreamlineError::storage_msg(format!(
                            "File not found: {:?}",
                            file_id
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
                        let (result, _) = file.write_at(data.to_vec(), offset).await;
                        let _ = response.send(result);
                    } else {
                        let _ = response.send(Err(StreamlineError::storage_msg(format!(
                            "File not found: {:?}",
                            file_id
                        ))));
                    }
                }

                IoRequest::Append {
                    file_id,
                    data,
                    response,
                } => {
                    if let Some(file) = files.get(&file_id) {
                        let (result, _) = file.append(data.to_vec()).await;
                        let _ = response.send(result);
                    } else {
                        let _ = response.send(Err(StreamlineError::storage_msg(format!(
                            "File not found: {:?}",
                            file_id
                        ))));
                    }
                }

                IoRequest::SyncData { file_id, response } => {
                    if let Some(file) = files.get(&file_id) {
                        let result = file.sync_data().await;
                        let _ = response.send(result);
                    } else {
                        let _ = response.send(Err(StreamlineError::storage_msg(format!(
                            "File not found: {:?}",
                            file_id
                        ))));
                    }
                }

                IoRequest::SyncAll { file_id, response } => {
                    if let Some(file) = files.get(&file_id) {
                        let result = file.sync_all().await;
                        let _ = response.send(result);
                    } else {
                        let _ = response.send(Err(StreamlineError::storage_msg(format!(
                            "File not found: {:?}",
                            file_id
                        ))));
                    }
                }

                IoRequest::Size { file_id, response } => {
                    if let Some(file) = files.get(&file_id) {
                        let result = file.size().await;
                        let _ = response.send(result);
                    } else {
                        let _ = response.send(Err(StreamlineError::storage_msg(format!(
                            "File not found: {:?}",
                            file_id
                        ))));
                    }
                }

                IoRequest::OpenFile {
                    path,
                    mode,
                    response,
                } => {
                    use tokio_uring::fs::{File, OpenOptions};

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
                            files.insert(file_id, UringFile::new(file, path));
                            let _ = response.send(Ok(file_id));
                        }
                        Err(e) => {
                            let _ = response.send(Err(StreamlineError::storage_msg(format!(
                                "Open failed: {}",
                                e
                            ))));
                        }
                    }
                }

                IoRequest::CloseFile { file_id } => {
                    files.remove(&file_id);
                }

                IoRequest::Shutdown => {
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
                StreamlineError::storage_msg(format!("Failed to send read request: {}", e))
            })?;
        rx.await.map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to receive read response: {}", e))
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
                StreamlineError::storage_msg(format!("Failed to send write request: {}", e))
            })?;
        rx.await.map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to receive write response: {}", e))
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
                StreamlineError::storage_msg(format!("Failed to send append request: {}", e))
            })?;
        rx.await.map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to receive append response: {}", e))
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
                StreamlineError::storage_msg(format!("Failed to send sync request: {}", e))
            })?;
        rx.await.map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to receive sync response: {}", e))
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
                StreamlineError::storage_msg(format!("Failed to send sync request: {}", e))
            })?;
        rx.await.map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to receive sync response: {}", e))
        })?
    }

    /// Open a file
    pub async fn open_file(&self, path: PathBuf, mode: OpenMode) -> Result<FileId> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(IoRequest::OpenFile {
                path,
                mode,
                response: tx,
            })
            .await
            .map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to send open request: {}", e))
            })?;
        rx.await.map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to receive open response: {}", e))
        })?
    }

    /// Close a file
    pub async fn close_file(&self, file_id: FileId) -> Result<()> {
        self.tx
            .send(IoRequest::CloseFile { file_id })
            .await
            .map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to send close request: {}", e))
            })
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
