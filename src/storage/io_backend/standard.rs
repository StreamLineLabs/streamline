//! Standard I/O backend using tokio's spawn_blocking
//!
//! This backend works on all platforms and uses the standard Rust file I/O
//! operations wrapped in `tokio::task::spawn_blocking` to avoid blocking
//! the async runtime.

use super::{AsyncFile, AsyncFileSystem, IoResult};
use crate::error::{Result, StreamlineError};
use async_trait::async_trait;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Standard file implementation using std::fs::File
pub struct StandardFile {
    file: Arc<Mutex<File>>,
    #[allow(dead_code)]
    path: PathBuf,
}

impl StandardFile {
    /// Create a new StandardFile
    pub fn new(file: File, path: PathBuf) -> Self {
        Self {
            file: Arc::new(Mutex::new(file)),
            path,
        }
    }
}

#[async_trait]
impl AsyncFile for StandardFile {
    async fn read_at(&self, mut buf: Vec<u8>, offset: u64) -> IoResult<usize> {
        let file = self.file.clone();
        let result = tokio::task::spawn_blocking(move || {
            let file = file.blocking_lock();

            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;
                match file.read_at(&mut buf, offset) {
                    Ok(n) => (Ok(n), buf),
                    Err(e) => (Err(e), buf),
                }
            }

            #[cfg(not(unix))]
            {
                use std::io::Read;
                let mut file_guard = file;
                let result = file_guard
                    .seek(SeekFrom::Start(offset))
                    .and_then(|_| file_guard.read(&mut buf));
                match result {
                    Ok(n) => (Ok(n), buf),
                    Err(e) => (Err(e), buf),
                }
            }
        })
        .await;

        match result {
            Ok((Ok(n), buf)) => (Ok(n), buf),
            Ok((Err(e), buf)) => (Err(StreamlineError::from(e)), buf),
            Err(e) => (
                Err(StreamlineError::storage_msg(format!(
                    "Task join error: {}",
                    e
                ))),
                vec![], // Can't recover buffer in join error case
            ),
        }
    }

    async fn write_at(&self, buf: Vec<u8>, offset: u64) -> IoResult<usize> {
        let file = self.file.clone();
        let result = tokio::task::spawn_blocking(move || {
            #[allow(unused_mut)]
            let mut file = file.blocking_lock();

            #[cfg(unix)]
            {
                use std::os::unix::fs::FileExt;
                match file.write_at(&buf, offset) {
                    Ok(n) => (Ok(n), buf),
                    Err(e) => (Err(e), buf),
                }
            }

            #[cfg(not(unix))]
            {
                let result = file
                    .seek(SeekFrom::Start(offset))
                    .and_then(|_| file.write(&buf));
                match result {
                    Ok(n) => (Ok(n), buf),
                    Err(e) => (Err(e), buf),
                }
            }
        })
        .await;

        match result {
            Ok((Ok(n), buf)) => (Ok(n), buf),
            Ok((Err(e), buf)) => (Err(StreamlineError::from(e)), buf),
            Err(e) => (
                Err(StreamlineError::storage_msg(format!(
                    "Task join error: {}",
                    e
                ))),
                vec![],
            ),
        }
    }

    async fn append(&self, buf: Vec<u8>) -> IoResult<usize> {
        let file = self.file.clone();
        let result = tokio::task::spawn_blocking(move || {
            let mut file = file.blocking_lock();
            let result = file.seek(SeekFrom::End(0)).and_then(|_| file.write(&buf));
            match result {
                Ok(n) => (Ok(n), buf),
                Err(e) => (Err(e), buf),
            }
        })
        .await;

        match result {
            Ok((Ok(n), buf)) => (Ok(n), buf),
            Ok((Err(e), buf)) => (Err(StreamlineError::from(e)), buf),
            Err(e) => (
                Err(StreamlineError::storage_msg(format!(
                    "Task join error: {}",
                    e
                ))),
                vec![],
            ),
        }
    }

    async fn sync_data(&self) -> Result<()> {
        let file = self.file.clone();
        tokio::task::spawn_blocking(move || file.blocking_lock().sync_data())
            .await
            .map_err(|e| StreamlineError::storage_msg(format!("Task join error: {}", e)))?
            .map_err(StreamlineError::from)
    }

    async fn sync_all(&self) -> Result<()> {
        let file = self.file.clone();
        tokio::task::spawn_blocking(move || file.blocking_lock().sync_all())
            .await
            .map_err(|e| StreamlineError::storage_msg(format!("Task join error: {}", e)))?
            .map_err(StreamlineError::from)
    }

    async fn size(&self) -> Result<u64> {
        let file = self.file.clone();
        tokio::task::spawn_blocking(move || {
            let file = file.blocking_lock();
            file.metadata().map(|m| m.len())
        })
        .await
        .map_err(|e| StreamlineError::storage_msg(format!("Task join error: {}", e)))?
        .map_err(StreamlineError::from)
    }

    async fn allocate(&self, len: u64) -> Result<()> {
        let file = self.file.clone();

        #[cfg(target_os = "linux")]
        {
            tokio::task::spawn_blocking(move || {
                use std::os::unix::io::AsRawFd;
                let file = file.blocking_lock();
                let fd = file.as_raw_fd();
                unsafe {
                    let result = libc::fallocate(fd, 0, 0, len as i64);
                    if result != 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                }
                Ok(())
            })
            .await
            .map_err(|e| StreamlineError::storage_msg(format!("Task join error: {}", e)))?
            .map_err(StreamlineError::from)
        }

        #[cfg(not(target_os = "linux"))]
        {
            // On non-Linux platforms, use set_len as a fallback
            tokio::task::spawn_blocking(move || {
                let file = file.blocking_lock();
                file.set_len(len)
            })
            .await
            .map_err(|e| StreamlineError::storage_msg(format!("Task join error: {}", e)))?
            .map_err(StreamlineError::from)
        }
    }
}

/// Standard filesystem implementation
pub struct StandardFileSystem {}

impl StandardFileSystem {
    /// Create a new StandardFileSystem
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for StandardFileSystem {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl AsyncFileSystem for StandardFileSystem {
    type File = StandardFile;

    async fn open(&self, path: &Path) -> Result<Self::File> {
        let path_buf = path.to_path_buf();
        let path_clone = path_buf.clone();

        let file = tokio::task::spawn_blocking(move || {
            OpenOptions::new()
                .read(true)
                .open(&path_clone)
                .map_err(|e| {
                    StreamlineError::storage_msg(format!(
                        "Failed to open file {:?} for reading: {}",
                        path_clone, e
                    ))
                })
        })
        .await
        .map_err(|e| StreamlineError::storage_msg(format!("Task join error: {}", e)))??;

        Ok(StandardFile::new(file, path_buf))
    }

    async fn open_rw(&self, path: &Path) -> Result<Self::File> {
        let path_buf = path.to_path_buf();
        let path_clone = path_buf.clone();

        let file = tokio::task::spawn_blocking(move || {
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path_clone)
                .map_err(|e| {
                    StreamlineError::storage_msg(format!(
                        "Failed to open file {:?} for read-write: {}",
                        path_clone, e
                    ))
                })
        })
        .await
        .map_err(|e| StreamlineError::storage_msg(format!("Task join error: {}", e)))??;

        Ok(StandardFile::new(file, path_buf))
    }

    async fn create(&self, path: &Path) -> Result<Self::File> {
        let path_buf = path.to_path_buf();
        let path_clone = path_buf.clone();

        let file = tokio::task::spawn_blocking(move || {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path_clone)
                .map_err(|e| {
                    StreamlineError::storage_msg(format!(
                        "Failed to create file {:?}: {}",
                        path_clone, e
                    ))
                })
        })
        .await
        .map_err(|e| StreamlineError::storage_msg(format!("Task join error: {}", e)))??;

        Ok(StandardFile::new(file, path_buf))
    }

    async fn open_append(&self, path: &Path) -> Result<Self::File> {
        let path_buf = path.to_path_buf();
        let path_clone = path_buf.clone();

        let file = tokio::task::spawn_blocking(move || {
            OpenOptions::new()
                .read(true)
                .create(true)
                .append(true)
                .open(&path_clone)
                .map_err(|e| {
                    StreamlineError::storage_msg(format!(
                        "Failed to open file {:?} for append: {}",
                        path_clone, e
                    ))
                })
        })
        .await
        .map_err(|e| StreamlineError::storage_msg(format!("Task join error: {}", e)))??;

        Ok(StandardFile::new(file, path_buf))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_standard_read_write() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.txt");

        let fs = StandardFileSystem::new();

        // Create and write
        let file = fs.create(&path).await.unwrap();
        let data = b"hello world".to_vec();
        let (result, _buf) = file.write_at(data, 0).await;
        let bytes_written = result.unwrap();
        assert_eq!(bytes_written, 11);

        // Sync
        file.sync_all().await.unwrap();

        // Read
        let file = fs.open(&path).await.unwrap();
        let buf = vec![0u8; 11];
        let (result, buf) = file.read_at(buf, 0).await;
        let bytes_read = result.unwrap();
        assert_eq!(bytes_read, 11);
        assert_eq!(&buf[..bytes_read], b"hello world");
    }

    #[tokio::test]
    async fn test_standard_append() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.txt");

        let fs = StandardFileSystem::new();

        // Create and append
        let file = fs.open_append(&path).await.unwrap();
        let (result, _) = file.append(b"first ".to_vec()).await;
        result.unwrap();
        let (result, _) = file.append(b"second".to_vec()).await;
        result.unwrap();

        file.sync_all().await.unwrap();

        // Read back
        let file = fs.open(&path).await.unwrap();
        let size = file.size().await.unwrap();
        assert_eq!(size, 12);

        let buf = vec![0u8; 12];
        let (result, buf) = file.read_at(buf, 0).await;
        let bytes_read = result.unwrap();
        assert_eq!(&buf[..bytes_read], b"first second");
    }

    #[tokio::test]
    async fn test_standard_size() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.txt");

        let fs = StandardFileSystem::new();
        let file = fs.create(&path).await.unwrap();

        // Empty file
        let size = file.size().await.unwrap();
        assert_eq!(size, 0);

        // Write data
        let (result, _) = file.write_at(b"test".to_vec(), 0).await;
        result.unwrap();
        file.sync_all().await.unwrap();

        let size = file.size().await.unwrap();
        assert_eq!(size, 4);
    }
}
