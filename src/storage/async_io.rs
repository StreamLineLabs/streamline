//! Async I/O helpers for storage operations
//!
//! This module provides async wrappers around blocking file I/O operations
//! using `tokio::task::spawn_blocking`. This prevents blocking the async runtime
//! when performing file operations.
//!
//! ## Why this matters
//!
//! Blocking I/O operations (like `std::fs::read`, `std::fs::write`) can block
//! the entire tokio worker thread, leading to:
//! - Thread starvation under high load
//! - Increased latency for other async tasks
//! - Potential deadlocks in single-threaded runtimes
//!
//! By using `spawn_blocking`, these operations run on a dedicated thread pool
//! designed for blocking work.

use crate::error::{Result, StreamlineError};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use tracing::warn;

/// Read a file to string asynchronously
///
/// Wraps `std::fs::read_to_string` in `spawn_blocking`.
pub async fn read_to_string_async(path: PathBuf) -> Result<String> {
    tokio::task::spawn_blocking(move || fs::read_to_string(&path).map_err(StreamlineError::from))
        .await
        .map_err(|e| StreamlineError::storage_msg(format!("Task join error: {}", e)))?
}

/// Read a file to bytes asynchronously
///
/// Wraps `std::fs::read` in `spawn_blocking`.
pub async fn read_async(path: PathBuf) -> Result<Vec<u8>> {
    tokio::task::spawn_blocking(move || fs::read(&path).map_err(StreamlineError::from))
        .await
        .map_err(|e| StreamlineError::storage_msg(format!("Task join error: {}", e)))?
}

/// Write bytes to a file asynchronously
///
/// Wraps `std::fs::write` in `spawn_blocking`.
pub async fn write_async(path: PathBuf, data: Vec<u8>) -> Result<()> {
    tokio::task::spawn_blocking(move || fs::write(&path, &data).map_err(StreamlineError::from))
        .await
        .map_err(|e| StreamlineError::storage_msg(format!("Task join error: {}", e)))?
}

/// Create directories recursively asynchronously
///
/// Wraps `std::fs::create_dir_all` in `spawn_blocking`.
pub async fn create_dir_all_async(path: PathBuf) -> Result<()> {
    tokio::task::spawn_blocking(move || fs::create_dir_all(&path).map_err(StreamlineError::from))
        .await
        .map_err(|e| StreamlineError::storage_msg(format!("Task join error: {}", e)))?
}

/// Check if a path exists asynchronously
///
/// Wraps `std::path::Path::exists` in `spawn_blocking`.
pub async fn exists_async(path: PathBuf) -> Result<bool> {
    tokio::task::spawn_blocking(move || Ok(path.exists()))
        .await
        .map_err(|e| StreamlineError::storage_msg(format!("Task join error: {}", e)))?
}

/// Remove a file asynchronously
///
/// Wraps `std::fs::remove_file` in `spawn_blocking`.
pub async fn remove_file_async(path: PathBuf) -> Result<()> {
    tokio::task::spawn_blocking(move || fs::remove_file(&path).map_err(StreamlineError::from))
        .await
        .map_err(|e| StreamlineError::storage_msg(format!("Task join error: {}", e)))?
}

/// Remove a directory and all its contents asynchronously
///
/// Wraps `std::fs::remove_dir_all` in `spawn_blocking`.
pub async fn remove_dir_all_async(path: PathBuf) -> Result<()> {
    tokio::task::spawn_blocking(move || fs::remove_dir_all(&path).map_err(StreamlineError::from))
        .await
        .map_err(|e| StreamlineError::storage_msg(format!("Task join error: {}", e)))?
}

/// Rename a file asynchronously
///
/// Wraps `std::fs::rename` in `spawn_blocking`.
pub async fn rename_async(from: PathBuf, to: PathBuf) -> Result<()> {
    tokio::task::spawn_blocking(move || fs::rename(&from, &to).map_err(StreamlineError::from))
        .await
        .map_err(|e| StreamlineError::storage_msg(format!("Task join error: {}", e)))?
}

/// Atomically write data to a file using temp file + rename pattern (async version)
///
/// This ensures the file is never partially written, even if the process
/// crashes mid-write. The sequence is:
/// 1. Write to a temp file in the same directory
/// 2. fsync the temp file to ensure data is on disk
/// 3. Rename temp file to target (atomic on POSIX)
/// 4. fsync parent directory to ensure rename is persisted
pub async fn atomic_write_async(path: PathBuf, data: Vec<u8>) -> Result<()> {
    tokio::task::spawn_blocking(move || atomic_write_blocking(&path, &data))
        .await
        .map_err(|e| StreamlineError::storage_msg(format!("Task join error: {}", e)))?
}

/// Blocking version of atomic write (used by spawn_blocking)
fn atomic_write_blocking(path: &Path, data: &[u8]) -> Result<()> {
    // Create temp file in same directory (ensures same filesystem for atomic rename)
    let parent = path.parent().ok_or_else(|| {
        StreamlineError::storage_msg(format!("Cannot get parent directory of {:?}", path))
    })?;

    let temp_name = format!(
        ".{}.tmp.{}",
        path.file_name().and_then(|n| n.to_str()).unwrap_or("data"),
        std::process::id()
    );
    let temp_path = parent.join(&temp_name);

    // Write to temp file
    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)
            .map_err(|e| {
                StreamlineError::storage_msg(format!(
                    "Failed to create temp file {:?}: {}",
                    temp_path, e
                ))
            })?;

        file.write_all(data).map_err(|e| {
            StreamlineError::storage_msg(format!(
                "Failed to write temp file {:?}: {}",
                temp_path, e
            ))
        })?;

        // fsync to ensure data is on disk before rename
        file.sync_all().map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to sync temp file {:?}: {}", temp_path, e))
        })?;
    }

    // Atomic rename
    #[cfg(unix)]
    fs::rename(&temp_path, path).map_err(|e| {
        let _ = fs::remove_file(&temp_path);
        StreamlineError::storage_msg(format!(
            "Failed to rename {:?} to {:?}: {}",
            temp_path, path, e
        ))
    })?;

    #[cfg(windows)]
    {
        if let Err(_) = fs::rename(&temp_path, path) {
            if path.exists() {
                fs::remove_file(path)?;
            }
            fs::rename(&temp_path, path)?;
        }
    }

    // Sync parent directory to ensure the rename is persisted
    if let Ok(dir) = File::open(parent) {
        if let Err(e) = dir.sync_all() {
            warn!(
                path = ?parent,
                error = %e,
                "Directory fsync failed - rename durability not guaranteed"
            );
        }
    }

    Ok(())
}

/// Sync a file to disk asynchronously
///
/// Opens the file and calls fsync.
pub async fn sync_file_async(path: PathBuf) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        let file = File::open(&path).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to open {:?} for sync: {}", path, e))
        })?;
        file.sync_all()
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to sync {:?}: {}", path, e)))
    })
    .await
    .map_err(|e| StreamlineError::storage_msg(format!("Task join error: {}", e)))?
}

/// Sync a directory to disk asynchronously
///
/// Opens the directory and calls fsync. This is important for durability
/// after rename operations.
pub async fn sync_dir_async(path: PathBuf) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        let dir = File::open(&path).map_err(|e| {
            StreamlineError::storage_msg(format!(
                "Failed to open directory {:?} for sync: {}",
                path, e
            ))
        })?;
        dir.sync_all().map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to sync directory {:?}: {}", path, e))
        })
    })
    .await
    .map_err(|e| StreamlineError::storage_msg(format!("Task join error: {}", e)))?
}

/// Read directory entries asynchronously
///
/// Returns a vector of (path, is_dir) tuples.
pub async fn read_dir_async(path: PathBuf) -> Result<Vec<(PathBuf, bool)>> {
    tokio::task::spawn_blocking(move || {
        let entries = fs::read_dir(&path).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to read directory {:?}: {}", path, e))
        })?;

        let mut result = Vec::new();
        for entry in entries.flatten() {
            let is_dir = entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false);
            result.push((entry.path(), is_dir));
        }
        Ok(result)
    })
    .await
    .map_err(|e| StreamlineError::storage_msg(format!("Task join error: {}", e)))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_read_write_async() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.txt");

        // Write
        write_async(path.clone(), b"hello world".to_vec())
            .await
            .unwrap();

        // Read
        let content = read_to_string_async(path).await.unwrap();
        assert_eq!(content, "hello world");
    }

    #[tokio::test]
    async fn test_create_dir_all_async() {
        let dir = tempdir().unwrap();
        let nested = dir.path().join("a").join("b").join("c");

        create_dir_all_async(nested.clone()).await.unwrap();
        assert!(exists_async(nested).await.unwrap());
    }

    #[tokio::test]
    async fn test_atomic_write_async() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("atomic.txt");

        atomic_write_async(path.clone(), b"atomic data".to_vec())
            .await
            .unwrap();

        let content = read_to_string_async(path).await.unwrap();
        assert_eq!(content, "atomic data");
    }

    #[tokio::test]
    async fn test_remove_file_async() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("to_remove.txt");

        write_async(path.clone(), b"data".to_vec()).await.unwrap();
        assert!(exists_async(path.clone()).await.unwrap());

        remove_file_async(path.clone()).await.unwrap();
        assert!(!exists_async(path).await.unwrap());
    }

    #[tokio::test]
    async fn test_rename_async() {
        let dir = tempdir().unwrap();
        let from = dir.path().join("from.txt");
        let to = dir.path().join("to.txt");

        write_async(from.clone(), b"data".to_vec()).await.unwrap();

        rename_async(from.clone(), to.clone()).await.unwrap();

        assert!(!exists_async(from).await.unwrap());
        assert!(exists_async(to).await.unwrap());
    }

    #[tokio::test]
    async fn test_read_dir_async() {
        let dir = tempdir().unwrap();

        // Create some files and directories
        write_async(dir.path().join("file1.txt"), b"data".to_vec())
            .await
            .unwrap();
        write_async(dir.path().join("file2.txt"), b"data".to_vec())
            .await
            .unwrap();
        create_dir_all_async(dir.path().join("subdir"))
            .await
            .unwrap();

        let entries = read_dir_async(dir.path().to_path_buf()).await.unwrap();
        assert_eq!(entries.len(), 3);

        // Count files and directories
        let dirs: Vec<_> = entries.iter().filter(|(_, is_dir)| *is_dir).collect();
        let files: Vec<_> = entries.iter().filter(|(_, is_dir)| !*is_dir).collect();

        assert_eq!(dirs.len(), 1);
        assert_eq!(files.len(), 2);
    }
}
