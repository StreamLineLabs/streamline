//! Low-level io_uring buffer and file registration
//!
//! This module provides direct integration with io_uring's registration syscalls:
//!
//! - **IORING_REGISTER_BUFFERS**: Pre-register buffers with the kernel to avoid
//!   per-I/O memory mapping overhead
//! - **IORING_REGISTER_FILES**: Pre-register file descriptors to avoid per-I/O
//!   file lookup overhead
//!
//! # Performance Impact
//!
//! Registered buffers can improve small I/O performance by 10-20% by eliminating:
//! - get_user_pages() overhead for each I/O
//! - Page pinning/unpinning for each operation
//!
//! Registered files can improve I/O performance by 5-10% by eliminating:
//! - File descriptor lookup for each operation
//! - fdget()/fdput() overhead
//!
//! # Kernel Requirements
//!
//! - Linux 5.1+ for basic io_uring support
//! - Linux 5.11+ for stable fixed buffer/file support
//! - Linux 5.19+ for buffer ring updates

#[cfg(target_os = "linux")]
use crate::error::{Result, StreamlineError};
#[cfg(target_os = "linux")]
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
#[cfg(target_os = "linux")]
use std::sync::Arc;
#[cfg(target_os = "linux")]
use tracing::{debug, error, info, warn};

// ============================================================================
// io_uring Constants (from linux/io_uring.h)
// ============================================================================

/// io_uring register opcodes
#[cfg(target_os = "linux")]
mod opcodes {
    pub const IORING_REGISTER_BUFFERS: libc::c_uint = 0;
    pub const IORING_UNREGISTER_BUFFERS: libc::c_uint = 1;
    pub const IORING_REGISTER_FILES: libc::c_uint = 2;
    pub const IORING_UNREGISTER_FILES: libc::c_uint = 3;
    pub const IORING_REGISTER_FILES_UPDATE: libc::c_uint = 6;
    pub const IORING_REGISTER_BUFFERS2: libc::c_uint = 15;
    pub const IORING_REGISTER_BUFFERS_UPDATE: libc::c_uint = 16;
}

/// io_uring syscall number (x86_64)
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
const SYS_IO_URING_REGISTER: libc::c_long = 427;

/// io_uring syscall number (aarch64)
#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
const SYS_IO_URING_REGISTER: libc::c_long = 427;

// ============================================================================
// Ring Handle Management (Linux only)
// ============================================================================

/// Handle to an io_uring ring for registration operations
///
/// This wraps the ring file descriptor obtained from io_uring_setup.
/// The handle is used for all registration syscalls.
#[cfg(target_os = "linux")]
#[derive(Debug)]
pub struct UringRingHandle {
    /// Ring file descriptor
    ring_fd: AtomicI32,
    /// Whether buffers are registered
    buffers_registered: AtomicBool,
    /// Whether files are registered
    files_registered: AtomicBool,
    /// Number of registered buffers
    num_buffers: std::sync::atomic::AtomicUsize,
    /// Number of registered files
    num_files: std::sync::atomic::AtomicUsize,
}

#[cfg(target_os = "linux")]
impl UringRingHandle {
    /// Create a new ring handle
    ///
    /// # Arguments
    ///
    /// * `ring_fd` - The file descriptor of the io_uring ring
    pub fn new(ring_fd: i32) -> Self {
        debug!("Created io_uring ring handle with fd {}", ring_fd);
        Self {
            ring_fd: AtomicI32::new(ring_fd),
            buffers_registered: AtomicBool::new(false),
            files_registered: AtomicBool::new(false),
            num_buffers: std::sync::atomic::AtomicUsize::new(0),
            num_files: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Get the ring file descriptor
    pub fn fd(&self) -> i32 {
        self.ring_fd.load(Ordering::Acquire)
    }

    /// Check if buffers are registered
    pub fn has_registered_buffers(&self) -> bool {
        self.buffers_registered.load(Ordering::Acquire)
    }

    /// Check if files are registered
    pub fn has_registered_files(&self) -> bool {
        self.files_registered.load(Ordering::Acquire)
    }

    /// Get number of registered buffers
    pub fn num_buffers(&self) -> usize {
        self.num_buffers.load(Ordering::Acquire)
    }

    /// Get number of registered files
    pub fn num_files(&self) -> usize {
        self.num_files.load(Ordering::Acquire)
    }
}

// ============================================================================
// Buffer Registration (Linux only)
// ============================================================================

/// A registered buffer that has been pinned in kernel memory
#[cfg(target_os = "linux")]
#[derive(Debug)]
pub struct RegisteredBuffer {
    /// Pointer to the buffer data
    ptr: *mut u8,
    /// Length of the buffer
    len: usize,
    /// Index in the registered buffer array
    index: u32,
}

#[cfg(target_os = "linux")]
impl RegisteredBuffer {
    /// Get the buffer index for use in io_uring operations
    pub fn index(&self) -> u32 {
        self.index
    }

    /// Get the buffer length
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// Manager for registered buffers
#[cfg(target_os = "linux")]
pub struct BufferRegistration {
    /// Ring handle
    ring: Arc<UringRingHandle>,
    /// Registered buffer iovecs (kept alive for kernel)
    iovecs: Vec<libc::iovec>,
    /// Backing buffers (kept alive for kernel)
    buffers: Vec<Vec<u8>>,
}

#[cfg(target_os = "linux")]
impl BufferRegistration {
    /// Create a new buffer registration manager
    pub fn new(ring: Arc<UringRingHandle>) -> Self {
        Self {
            ring,
            iovecs: Vec::new(),
            buffers: Vec::new(),
        }
    }

    /// Register buffers with the io_uring ring
    ///
    /// # Arguments
    ///
    /// * `sizes` - Sizes of buffers to register
    ///
    /// # Returns
    ///
    /// Vector of registered buffer handles
    pub fn register_buffers(&mut self, sizes: &[usize]) -> Result<Vec<RegisteredBuffer>> {
        if self.ring.has_registered_buffers() {
            return Err(StreamlineError::storage_msg(
                "Buffers already registered".to_string(),
            ));
        }

        // Allocate aligned buffers
        self.buffers = sizes
            .iter()
            .map(|&size| {
                // Allocate page-aligned for DMA
                let layout = std::alloc::Layout::from_size_align(size, 4096).or_else(|_| {
                    std::alloc::Layout::from_size_align(size, 8)
                }).map_err(|e| StreamlineError::storage_msg(format!(
                    "Failed to create memory layout for size {}: {}", size, e
                )))?;
                // SAFETY: alloc_zeroed is safe because the layout was successfully
                // created above with valid size and alignment. The returned pointer
                // is checked for null before use.
                let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
                if ptr.is_null() {
                    return Err(StreamlineError::storage_msg(format!(
                        "Failed to allocate aligned buffer of size {}",
                        size
                    )));
                }
                // SAFETY: Vec::from_raw_parts is safe because: (1) ptr was allocated
                // by alloc_zeroed with the same layout, (2) ptr is non-null (checked
                // above), (3) size bytes are initialized (zeroed), and (4) capacity
                // equals length, matching the allocation size.
                Ok(unsafe { Vec::from_raw_parts(ptr, size, size) })
            })
            .collect::<Result<Vec<_>>>()?;

        // Create iovec array
        self.iovecs = self
            .buffers
            .iter_mut()
            .map(|buf| libc::iovec {
                iov_base: buf.as_mut_ptr() as *mut libc::c_void,
                iov_len: buf.len(),
            })
            .collect();

        // SAFETY: io_uring_register syscall is safe because: (1) the ring fd is
        // valid (obtained from io_uring_setup), (2) iovecs points to a valid array
        // of iovec structs whose iov_base pointers reference live, owned buffers,
        // and (3) the count matches the iovecs length. Errors are checked via return value.
        let ret = unsafe {
            libc::syscall(
                SYS_IO_URING_REGISTER,
                self.ring.fd(),
                opcodes::IORING_REGISTER_BUFFERS,
                self.iovecs.as_ptr(),
                self.iovecs.len() as libc::c_uint,
            )
        };

        if ret < 0 {
            let err = std::io::Error::last_os_error();
            error!("Failed to register buffers: {}", err);
            self.buffers.clear();
            self.iovecs.clear();
            return Err(StreamlineError::storage_msg(format!(
                "IORING_REGISTER_BUFFERS failed: {}",
                err
            )));
        }

        self.ring.buffers_registered.store(true, Ordering::Release);
        self.ring
            .num_buffers
            .store(self.buffers.len(), Ordering::Release);

        info!(
            "Registered {} buffers with io_uring (total {} bytes)",
            self.buffers.len(),
            self.buffers.iter().map(|b| b.len()).sum::<usize>()
        );

        // Create handles
        Ok(self
            .buffers
            .iter()
            .enumerate()
            .map(|(i, buf)| RegisteredBuffer {
                ptr: buf.as_ptr() as *mut u8,
                len: buf.len(),
                index: i as u32,
            })
            .collect())
    }

    /// Unregister all buffers
    pub fn unregister_buffers(&mut self) -> Result<()> {
        if !self.ring.has_registered_buffers() {
            return Ok(());
        }

        // SAFETY: io_uring_register with UNREGISTER_BUFFERS is safe because the
        // ring fd is valid and buffers were previously registered. The null pointer
        // and zero count are correct for the unregister operation.
        let ret = unsafe {
            libc::syscall(
                SYS_IO_URING_REGISTER,
                self.ring.fd(),
                opcodes::IORING_UNREGISTER_BUFFERS,
                std::ptr::null::<libc::c_void>(),
                0 as libc::c_uint,
            )
        };

        if ret < 0 {
            let err = std::io::Error::last_os_error();
            warn!("Failed to unregister buffers: {}", err);
            // Continue cleanup anyway
        }

        self.ring.buffers_registered.store(false, Ordering::Release);
        self.ring.num_buffers.store(0, Ordering::Release);
        self.buffers.clear();
        self.iovecs.clear();

        debug!("Unregistered buffers from io_uring");
        Ok(())
    }

    /// Write data to a registered buffer
    ///
    /// # Safety
    ///
    /// The caller must ensure the buffer index is valid and the data fits.
    pub unsafe fn write_to_buffer(&self, index: u32, offset: usize, data: &[u8]) -> Result<()> {
        let iovec = self.iovecs.get(index as usize).ok_or_else(|| {
            StreamlineError::storage_msg(format!("Invalid buffer index: {}", index))
        })?;

        if offset + data.len() > iovec.iov_len {
            return Err(StreamlineError::storage_msg(format!(
                "Write exceeds buffer size: {} + {} > {}",
                offset,
                data.len(),
                iovec.iov_len
            )));
        }

        let dst = (iovec.iov_base as *mut u8).add(offset);
        std::ptr::copy_nonoverlapping(data.as_ptr(), dst, data.len());
        Ok(())
    }

    /// Read data from a registered buffer
    ///
    /// # Safety
    ///
    /// The caller must ensure the buffer index is valid.
    pub unsafe fn read_from_buffer(&self, index: u32, offset: usize, len: usize) -> Result<&[u8]> {
        let iovec = self.iovecs.get(index as usize).ok_or_else(|| {
            StreamlineError::storage_msg(format!("Invalid buffer index: {}", index))
        })?;

        if offset + len > iovec.iov_len {
            return Err(StreamlineError::storage_msg(format!(
                "Read exceeds buffer size: {} + {} > {}",
                offset, len, iovec.iov_len
            )));
        }

        let src = (iovec.iov_base as *const u8).add(offset);
        Ok(std::slice::from_raw_parts(src, len))
    }
}

#[cfg(target_os = "linux")]
impl Drop for BufferRegistration {
    fn drop(&mut self) {
        let _ = self.unregister_buffers();
    }
}

// ============================================================================
// File Registration (Linux only)
// ============================================================================

/// Manager for registered file descriptors
#[cfg(target_os = "linux")]
pub struct FileRegistration {
    /// Ring handle
    ring: Arc<UringRingHandle>,
    /// Registered file descriptors (-1 for empty slots)
    fds: Vec<i32>,
    /// Maximum number of files
    max_files: usize,
}

#[cfg(target_os = "linux")]
impl FileRegistration {
    /// Create a new file registration manager
    ///
    /// # Arguments
    ///
    /// * `ring` - Ring handle
    /// * `max_files` - Maximum number of files to register
    pub fn new(ring: Arc<UringRingHandle>, max_files: usize) -> Self {
        Self {
            ring,
            fds: Vec::new(),
            max_files,
        }
    }

    /// Initialize the file table with empty slots
    pub fn init_file_table(&mut self) -> Result<()> {
        if self.ring.has_registered_files() {
            return Err(StreamlineError::storage_msg(
                "Files already registered".to_string(),
            ));
        }

        // Initialize with -1 (empty slots)
        self.fds = vec![-1i32; self.max_files];

        // SAFETY: io_uring_register with REGISTER_FILES is safe because: (1) the
        // ring fd is valid, (2) fds points to a valid array of i32 values (-1 for
        // empty slots), and (3) the count matches the fds length. The kernel copies
        // the fd array. Errors are checked via return value.
        let ret = unsafe {
            libc::syscall(
                SYS_IO_URING_REGISTER,
                self.ring.fd(),
                opcodes::IORING_REGISTER_FILES,
                self.fds.as_ptr(),
                self.fds.len() as libc::c_uint,
            )
        };

        if ret < 0 {
            let err = std::io::Error::last_os_error();
            error!("Failed to register file table: {}", err);
            self.fds.clear();
            return Err(StreamlineError::storage_msg(format!(
                "IORING_REGISTER_FILES failed: {}",
                err
            )));
        }

        self.ring.files_registered.store(true, Ordering::Release);
        self.ring.num_files.store(0, Ordering::Release);

        info!(
            "Initialized io_uring file table with {} slots",
            self.max_files
        );
        Ok(())
    }

    /// Register a file descriptor in the table
    ///
    /// # Arguments
    ///
    /// * `fd` - File descriptor to register
    /// * `slot` - Slot index (or None to find first empty)
    ///
    /// # Returns
    ///
    /// The slot index where the file was registered
    pub fn register_file(&mut self, fd: i32, slot: Option<usize>) -> Result<u32> {
        if !self.ring.has_registered_files() {
            return Err(StreamlineError::storage_msg(
                "File table not initialized".to_string(),
            ));
        }

        // Find slot
        let slot_idx = match slot {
            Some(s) => {
                if s >= self.fds.len() {
                    return Err(StreamlineError::storage_msg(format!(
                        "Slot {} out of range (max {})",
                        s, self.max_files
                    )));
                }
                s
            }
            None => {
                // Find first empty slot
                self.fds.iter().position(|&f| f == -1).ok_or_else(|| {
                    StreamlineError::storage_msg("No empty slots available".to_string())
                })?
            }
        };

        // Update the slot using IORING_REGISTER_FILES_UPDATE
        #[repr(C)]
        struct io_uring_files_update {
            offset: u32,
            resv: u32,
            fds: *const i32,
        }

        let update = io_uring_files_update {
            offset: slot_idx as u32,
            resv: 0,
            fds: &fd as *const i32,
        };

        // SAFETY: io_uring_register with FILES_UPDATE is safe because: (1) the ring
        // fd is valid, (2) update is a valid #[repr(C)] struct with a pointer to the
        // fd value on the stack, and (3) the count is 1 matching the single fd. The
        // file table was previously initialized via REGISTER_FILES.
        let ret = unsafe {
            libc::syscall(
                SYS_IO_URING_REGISTER,
                self.ring.fd(),
                opcodes::IORING_REGISTER_FILES_UPDATE,
                &update as *const io_uring_files_update,
                1 as libc::c_uint,
            )
        };

        if ret < 0 {
            let err = std::io::Error::last_os_error();
            return Err(StreamlineError::storage_msg(format!(
                "IORING_REGISTER_FILES_UPDATE failed: {}",
                err
            )));
        }

        self.fds[slot_idx] = fd;
        let current = self.ring.num_files.load(Ordering::Acquire);
        self.ring.num_files.store(current + 1, Ordering::Release);

        debug!("Registered fd {} in slot {}", fd, slot_idx);
        Ok(slot_idx as u32)
    }

    /// Unregister a file from a slot
    pub fn unregister_file(&mut self, slot: u32) -> Result<()> {
        let slot_idx = slot as usize;
        if slot_idx >= self.fds.len() {
            return Err(StreamlineError::storage_msg(format!(
                "Slot {} out of range",
                slot
            )));
        }

        if self.fds[slot_idx] == -1 {
            return Ok(()); // Already empty
        }

        // Update with -1 to remove
        let empty: i32 = -1;

        #[repr(C)]
        struct io_uring_files_update {
            offset: u32,
            resv: u32,
            fds: *const i32,
        }

        let update = io_uring_files_update {
            offset: slot as u32,
            resv: 0,
            fds: &empty as *const i32,
        };

        // SAFETY: io_uring_register with FILES_UPDATE is safe because: (1) the ring
        // fd is valid, (2) update is a valid #[repr(C)] struct with a pointer to
        // the empty fd value (-1) on the stack, and (3) the count is 1. The file
        // table was previously initialized.
        let ret = unsafe {
            libc::syscall(
                SYS_IO_URING_REGISTER,
                self.ring.fd(),
                opcodes::IORING_REGISTER_FILES_UPDATE,
                &update as *const io_uring_files_update,
                1 as libc::c_uint,
            )
        };

        if ret < 0 {
            let err = std::io::Error::last_os_error();
            warn!("Failed to unregister file from slot {}: {}", slot, err);
        }

        self.fds[slot_idx] = -1;
        let current = self.ring.num_files.load(Ordering::Acquire);
        if current > 0 {
            self.ring.num_files.store(current - 1, Ordering::Release);
        }

        debug!("Unregistered file from slot {}", slot);
        Ok(())
    }

    /// Unregister all files
    pub fn unregister_all(&mut self) -> Result<()> {
        if !self.ring.has_registered_files() {
            return Ok(());
        }

        // SAFETY: io_uring_register with UNREGISTER_FILES is safe because the ring
        // fd is valid and a file table was previously registered. The null pointer
        // and zero count are correct for the unregister operation.
        let ret = unsafe {
            libc::syscall(
                SYS_IO_URING_REGISTER,
                self.ring.fd(),
                opcodes::IORING_UNREGISTER_FILES,
                std::ptr::null::<libc::c_void>(),
                0 as libc::c_uint,
            )
        };

        if ret < 0 {
            let err = std::io::Error::last_os_error();
            warn!("Failed to unregister files: {}", err);
        }

        self.ring.files_registered.store(false, Ordering::Release);
        self.ring.num_files.store(0, Ordering::Release);
        self.fds.clear();

        debug!("Unregistered all files from io_uring");
        Ok(())
    }

    /// Get the file descriptor at a slot (for debugging)
    pub fn get_fd(&self, slot: u32) -> Option<i32> {
        self.fds.get(slot as usize).copied().filter(|&fd| fd != -1)
    }

    /// Get number of registered files
    pub fn count(&self) -> usize {
        self.fds.iter().filter(|&&fd| fd != -1).count()
    }
}

#[cfg(target_os = "linux")]
impl Drop for FileRegistration {
    fn drop(&mut self) {
        let _ = self.unregister_all();
    }
}

// ============================================================================
// Combined Registration Manager (Linux only)
// ============================================================================

/// Combined manager for buffer and file registration
#[cfg(target_os = "linux")]
pub struct UringRegistrationManager {
    /// Ring handle (shared)
    ring: Arc<UringRingHandle>,
    /// Buffer registration
    buffers: Option<BufferRegistration>,
    /// File registration
    files: Option<FileRegistration>,
}

#[cfg(target_os = "linux")]
impl UringRegistrationManager {
    /// Create a new registration manager
    ///
    /// # Arguments
    ///
    /// * `ring_fd` - The io_uring ring file descriptor
    pub fn new(ring_fd: i32) -> Self {
        let ring = Arc::new(UringRingHandle::new(ring_fd));
        Self {
            ring,
            buffers: None,
            files: None,
        }
    }

    /// Get the ring handle
    pub fn ring(&self) -> &Arc<UringRingHandle> {
        &self.ring
    }

    /// Initialize buffer registration
    ///
    /// # Arguments
    ///
    /// * `sizes` - Buffer sizes to register
    pub fn init_buffers(&mut self, sizes: &[usize]) -> Result<Vec<RegisteredBuffer>> {
        let mut buf_reg = BufferRegistration::new(self.ring.clone());
        let handles = buf_reg.register_buffers(sizes)?;
        self.buffers = Some(buf_reg);
        Ok(handles)
    }

    /// Initialize file registration
    ///
    /// # Arguments
    ///
    /// * `max_files` - Maximum number of files
    pub fn init_files(&mut self, max_files: usize) -> Result<()> {
        let mut file_reg = FileRegistration::new(self.ring.clone(), max_files);
        file_reg.init_file_table()?;
        self.files = Some(file_reg);
        Ok(())
    }

    /// Register a file
    pub fn register_file(&mut self, fd: i32) -> Result<u32> {
        self.files
            .as_mut()
            .ok_or_else(|| StreamlineError::storage_msg("File table not initialized".to_string()))?
            .register_file(fd, None)
    }

    /// Unregister a file
    pub fn unregister_file(&mut self, slot: u32) -> Result<()> {
        self.files
            .as_mut()
            .ok_or_else(|| StreamlineError::storage_msg("File table not initialized".to_string()))?
            .unregister_file(slot)
    }

    /// Write to a registered buffer
    ///
    /// # Safety
    ///
    /// The buffer index must be valid.
    pub unsafe fn write_buffer(&self, index: u32, offset: usize, data: &[u8]) -> Result<()> {
        self.buffers
            .as_ref()
            .ok_or_else(|| StreamlineError::storage_msg("Buffers not registered".to_string()))?
            .write_to_buffer(index, offset, data)
    }

    /// Read from a registered buffer
    ///
    /// # Safety
    ///
    /// The buffer index must be valid.
    pub unsafe fn read_buffer(&self, index: u32, offset: usize, len: usize) -> Result<&[u8]> {
        self.buffers
            .as_ref()
            .ok_or_else(|| StreamlineError::storage_msg("Buffers not registered".to_string()))?
            .read_from_buffer(index, offset, len)
    }

    /// Get registration statistics
    pub fn stats(&self) -> RegistrationStats {
        RegistrationStats {
            ring_fd: self.ring.fd(),
            buffers_registered: self.ring.has_registered_buffers(),
            files_registered: self.ring.has_registered_files(),
            num_buffers: self.ring.num_buffers(),
            num_files: self.ring.num_files(),
            file_slots_used: self.files.as_ref().map(|f| f.count()).unwrap_or(0),
        }
    }
}

/// Registration statistics
#[derive(Debug, Clone)]
pub struct RegistrationStats {
    /// Ring file descriptor
    pub ring_fd: i32,
    /// Whether buffers are registered
    pub buffers_registered: bool,
    /// Whether file table is initialized
    pub files_registered: bool,
    /// Number of registered buffers
    pub num_buffers: usize,
    /// Number of registered files
    pub num_files: usize,
    /// Number of file slots actually in use
    pub file_slots_used: usize,
}

// ============================================================================
// Non-Linux Stubs
// ============================================================================

/// Stub ring handle for non-Linux platforms
#[cfg(not(target_os = "linux"))]
#[derive(Debug)]
pub struct UringRingHandle;

#[cfg(not(target_os = "linux"))]
impl UringRingHandle {
    pub fn new(_ring_fd: i32) -> Self {
        Self
    }

    pub fn fd(&self) -> i32 {
        -1
    }

    pub fn has_registered_buffers(&self) -> bool {
        false
    }

    pub fn has_registered_files(&self) -> bool {
        false
    }
}

/// Stub registration manager for non-Linux platforms
#[cfg(not(target_os = "linux"))]
pub struct UringRegistrationManager;

#[cfg(not(target_os = "linux"))]
impl UringRegistrationManager {
    pub fn new(_ring_fd: i32) -> Self {
        Self
    }

    pub fn stats(&self) -> RegistrationStats {
        RegistrationStats {
            ring_fd: -1,
            buffers_registered: false,
            files_registered: false,
            num_buffers: 0,
            num_files: 0,
            file_slots_used: 0,
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ring_handle_creation() {
        let handle = UringRingHandle::new(42);
        #[cfg(target_os = "linux")]
        {
            assert_eq!(handle.fd(), 42);
            assert!(!handle.has_registered_buffers());
            assert!(!handle.has_registered_files());
        }
        #[cfg(not(target_os = "linux"))]
        {
            assert_eq!(handle.fd(), -1);
        }
    }

    #[test]
    fn test_registration_stats() {
        let manager = UringRegistrationManager::new(42);
        let stats = manager.stats();

        #[cfg(target_os = "linux")]
        {
            assert_eq!(stats.ring_fd, 42);
            assert!(!stats.buffers_registered);
            assert!(!stats.files_registered);
        }
        #[cfg(not(target_os = "linux"))]
        {
            assert_eq!(stats.ring_fd, -1);
        }
    }
}
