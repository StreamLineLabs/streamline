# Rust Coding Agent Instructions

This document provides comprehensive instructions for AI coding agents working on the Streamline Rust project. Follow these guidelines to produce production-quality code that passes CI checks on the first try.

## General Guidelines

Write idiomatic Rust code that emphasizes:

- **Safety**: Leverage Rust's ownership system. Avoid `unsafe` unless absolutely necessary and document why.
- **Clarity**: Code should be self-documenting. Use meaningful names and add doc comments for public APIs.
- **Performance**: Zero-cost abstractions are preferred. Avoid unnecessary allocations and clones.
- **Error handling**: Use `Result` and `Option` properly. Never use `unwrap()` or `expect()` in production code paths.
- **Concurrency**: This project uses `tokio` for async runtime. All async code must be non-blocking.

## Writing Code

### Style and Naming Conventions

Follow standard Rust naming conventions:

- **Types** (structs, enums, traits): `PascalCase`
- **Functions and variables**: `snake_case`
- **Constants and statics**: `SCREAMING_SNAKE_CASE`
- **Lifetimes**: single lowercase letter (e.g., `'a`, `'b`) or descriptive (e.g., `'static`)

Code formatting:
- Run `cargo fmt` before committing. All code must pass `rustfmt`.
- Maximum line length: 100 characters (default rustfmt).
- Use 4 spaces for indentation (enforced by rustfmt).

Visibility:
- Make items `pub` only when necessary for the public API.
- Use `pub(crate)` for internal-only items shared across modules.
- Keep implementation details private by default.

### Error Handling Patterns

The project uses `thiserror` for error types and defines `StreamlineError` in `src/error.rs`.

**Custom error types**:

```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StreamlineError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Storage error: {0}")]
    Storage(String),
    
    #[error("Topic not found: {0}")]
    TopicNotFound(String),
}

pub type Result<T> = std::result::Result<T, StreamlineError>;
```

**Error propagation**:

```rust
// Use ? operator for propagating errors
pub fn read_record(&self) -> Result<Record> {
    let data = self.file.read()?;  // Automatically converts io::Error
    self.parse_record(data)
}

// Provide context when converting errors
pub fn open_segment(&self, id: u64) -> Result<Segment> {
    let path = self.segment_path(id);
    File::open(&path)
        .map_err(|e| StreamlineError::Storage(format!("Failed to open segment {}: {}", id, e)))?;
    // ...
}
```

**Option handling**:

```rust
// Use combinators instead of unwrap()
let value = optional_value.ok_or(StreamlineError::Storage("Value not found".into()))?;

// Use if-let or match for branching
if let Some(partition) = self.partitions.get(&partition_id) {
    partition.append(record)?;
}
```

### Memory and Performance Considerations

**Ownership and borrowing**:

```rust
// Accept references for read-only access
pub fn validate_record(&self, record: &Record) -> bool {
    record.offset >= 0 && !record.value.is_empty()
}

// Take ownership when the value is consumed
pub fn store_record(&mut self, record: Record) -> Result<()> {
    self.records.push(record);
    Ok(())
}

// Return owned data when the caller needs ownership
pub fn take_records(&mut self) -> Vec<Record> {
    std::mem::take(&mut self.records)
}
```

**Avoid unnecessary allocations**:

```rust
// Good: Use references when possible
fn process_data(data: &[u8]) -> Result<()> { /* ... */ }

// Bad: Unnecessary clone
fn process_data(data: Vec<u8>) -> Result<()> { /* ... */ }

// Use Cow when you might need to own data
use std::borrow::Cow;

fn process_string(s: Cow<str>) -> String {
    if s.contains("special") {
        s.to_uppercase()  // Allocate only when needed
    } else {
        s.into_owned()
    }
}
```

**Use efficient data structures**:

```rust
use bytes::Bytes;  // Already imported, use for zero-copy byte handling
use std::collections::HashMap;  // For key-value lookups
use std::collections::BTreeMap;  // For ordered keys

// Prefer Bytes over Vec<u8> for network/storage data
pub struct Record {
    pub value: Bytes,  // Zero-copy cloning
}
```

**Async considerations**:

```rust
// Use async/await for I/O operations
pub async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
    let encoded = self.encode_batch(&batch)?;
    self.file.write_all(&encoded).await?;
    Ok(())
}

// Don't block the runtime
// Bad:
async fn process() {
    std::thread::sleep(Duration::from_secs(1));  // Blocks executor!
}

// Good:
async fn process() {
    tokio::time::sleep(Duration::from_secs(1)).await;
}
```

### Dependency Management

**Adding dependencies**:

1. Check if a similar dependency already exists in `Cargo.toml`
2. Prefer well-maintained crates with recent updates
3. Minimize dependency count - reuse existing dependencies
4. For test-only dependencies, add to `[dev-dependencies]`

```toml
[dependencies]
tokio = { version = "1.41", features = ["full"] }

[dev-dependencies]
tempfile = "3.14"
```

**Feature flags**:

Only enable features you actually use:

```toml
# Good: Only necessary features
serde = { version = "1.0", features = ["derive"] }

# Bad: Unnecessary features increase compile time
serde = { version = "1.0", features = ["derive", "rc", "alloc"] }
```

## Testing

### Unit Test Structure and Placement

Place unit tests in the same file as the code they test, inside a `tests` module:

```rust
// src/storage/record.rs

impl Record {
    pub fn new(offset: i64, timestamp: i64, value: Bytes) -> Self {
        Self { offset, timestamp, value }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_record_creation() {
        let record = Record::new(0, 1234567890, Bytes::from("test"));
        assert_eq!(record.offset, 0);
        assert_eq!(record.timestamp, 1234567890);
        assert_eq!(record.value, Bytes::from("test"));
    }

    #[test]
    fn test_record_size() {
        let record = Record::new(0, 0, Bytes::from("hello"));
        assert_eq!(record.size(), 8 + 8 + 5);  // offset + timestamp + value
    }
    
    #[test]
    fn test_record_with_key() {
        let key = Some(Bytes::from("key"));
        let value = Bytes::from("value");
        let record = Record::new(10, 1000, key, value);
        assert!(record.key.is_some());
    }
}
```

**Async tests**:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_async_write() {
        let mut segment = Segment::new(0).await.unwrap();
        segment.append_record(record).await.unwrap();
        assert_eq!(segment.record_count(), 1);
    }
}
```

### Integration Test Organization

Place integration tests in the `tests/` directory at the project root:

```
streamline/
├── src/
├── tests/
│   ├── integration_test.rs
│   └── common/
│       └── mod.rs  (shared test utilities)
└── Cargo.toml
```

Integration tests have access to the public API only:

```rust
// tests/storage_integration.rs

use streamline::{TopicManager, Record};
use tempfile::tempdir;

#[tokio::test]
async fn test_topic_create_and_append() {
    let dir = tempdir().unwrap();
    let mut manager = TopicManager::new(dir.path()).await.unwrap();
    
    manager.create_topic("test-topic", 3).await.unwrap();
    let topic = manager.get_topic("test-topic").await.unwrap();
    
    assert_eq!(topic.partition_count(), 3);
}
```

### Common Test Commands

Run these commands frequently during development:

```bash
# Run all tests
cargo test

# Run tests for a specific module
cargo test storage::

# Run a specific test
cargo test test_record_creation

# Run tests with output visible
cargo test -- --nocapture

# Run tests in release mode (faster, for performance tests)
cargo test --release

# Run only integration tests
cargo test --test '*'

# Run only doc tests
cargo test --doc
```

## Validation

### Pre-commit Checklist

Before committing, run these commands in order. All must pass:

```bash
# 1. Format code
cargo fmt

# 2. Run clippy with pedantic lints
cargo clippy --all-targets --all-features -- -D warnings

# 3. Run all tests
cargo test

# 4. Check documentation builds
cargo doc --no-deps --document-private-items

# 5. Build in release mode
cargo build --release
```

**One-liner validation**:

```bash
cargo fmt && cargo clippy --all-targets --all-features -- -D warnings && cargo test && cargo doc --no-deps
```

### Clippy Rules to Watch For

The project treats warnings as errors in CI. Pay attention to:

**Common clippy lints**:

- `clippy::unwrap_used` - Never use `.unwrap()` in production code
- `clippy::expect_used` - Avoid `.expect()` in production code paths
- `clippy::panic` - No explicit panics in library code
- `clippy::indexing_slicing` - Use `.get()` instead of `[]` for fallible access
- `clippy::clone_on_copy` - Don't clone `Copy` types
- `clippy::unnecessary_unwrap` - Use `if let` or `match` instead
- `clippy::large_enum_variant` - Box large enum variants
- `clippy::too_many_arguments` - Refactor functions with >7 arguments

**Fix clippy suggestions automatically** when safe:

```bash
cargo clippy --fix --all-targets --all-features
```

### Documentation Requirements

**All public items must have doc comments**:

```rust
/// Represents a record in a topic partition.
///
/// Records are the fundamental unit of data in Streamline. Each record
/// has an offset, timestamp, optional key, and a value.
///
/// # Examples
///
/// ```
/// use streamline::Record;
/// use bytes::Bytes;
///
/// let record = Record::new(0, 1234567890, None, Bytes::from("hello"));
/// assert_eq!(record.offset, 0);
/// ```
pub struct Record {
    /// Offset of this record within the partition
    pub offset: i64,
    
    /// Timestamp in milliseconds since Unix epoch
    pub timestamp: i64,
    
    /// Optional key for the record
    pub key: Option<Bytes>,
    
    /// Record payload
    pub value: Bytes,
}
```

**Document complex functions**:

```rust
/// Appends a batch of records to the partition.
///
/// # Arguments
///
/// * `batch` - The record batch to append
///
/// # Returns
///
/// The base offset of the appended batch
///
/// # Errors
///
/// Returns `StreamlineError::Storage` if:
/// - The disk is full
/// - The segment file cannot be written
/// - The batch exceeds size limits
pub async fn append_batch(&mut self, batch: RecordBatch) -> Result<i64> {
    // Implementation
}
```

**Module-level documentation**:

```rust
//! Storage layer for Streamline.
//!
//! This module provides the core storage abstractions for topics, partitions,
//! and segments. Data is organized hierarchically:
//!
//! - `TopicManager` manages all topics
//! - `Topic` represents a single topic with multiple partitions
//! - `Partition` stores records in ordered segments
//! - `Segment` is the physical storage unit (a single file)
```

## Workflow

Follow this step-by-step process when implementing features:

### 1. Understand the Requirement

- Read the issue/task description carefully
- Identify affected modules and components
- Check for existing related code or patterns
- Plan the minimal change needed

### 2. Explore the Codebase

```bash
# Find relevant files
rg "pattern" src/

# View module structure
cargo modules tree

# Check dependencies of a module
cargo tree
```

### 3. Write Tests First (TDD)

- Write failing tests that describe the expected behavior
- Run `cargo test` to verify tests fail
- Keep tests simple and focused

```rust
#[test]
fn test_new_feature() {
    // Arrange
    let input = create_test_input();
    
    // Act
    let result = process(input);
    
    // Assert
    assert_eq!(result, expected);
}
```

### 4. Implement the Feature

- Make minimal changes to pass the tests
- Follow existing code patterns and style
- Add error handling and validation
- Document public APIs with doc comments

### 5. Run Local Validation

```bash
# Fast feedback loop
cargo check        # Type checking only (fast)
cargo test         # Run tests
cargo clippy       # Lint
cargo fmt          # Format
```

### 6. Check Performance Implications

For performance-critical code:

```bash
# Run benchmarks if they exist
cargo bench

# Profile with flamegraph (if needed)
cargo install flamegraph
cargo flamegraph
```

### 7. Update Documentation

- Update doc comments if behavior changes
- Update README.md if user-facing changes
- Add examples if introducing new public APIs

### 8. Final Validation

```bash
# Complete pre-commit checklist
cargo fmt && \
cargo clippy --all-targets --all-features -- -D warnings && \
cargo test && \
cargo doc --no-deps
```

### 9. Commit

```bash
git add .
git commit -m "feat: add feature description

- Detailed change 1
- Detailed change 2

Closes #123"
```

Use conventional commit prefixes:
- `feat:` - New features
- `fix:` - Bug fixes
- `docs:` - Documentation only
- `refactor:` - Code refactoring
- `test:` - Adding tests
- `perf:` - Performance improvements
- `chore:` - Maintenance tasks

## Project Structure

```
streamline/
├── Cargo.toml              # Project manifest and dependencies
├── Cargo.lock              # Locked dependency versions
├── README.md               # Project documentation
├── src/
│   ├── lib.rs             # Library root, re-exports public API
│   ├── main.rs            # Server binary entry point
│   ├── cli.rs             # CLI binary entry point
│   ├── error.rs           # Error types (StreamlineError)
│   ├── config/            # Configuration types
│   │   └── mod.rs         # ServerConfig, StorageConfig
│   ├── protocol/          # Kafka protocol implementation
│   │   ├── mod.rs
│   │   └── kafka.rs       # Protocol handling
│   ├── server/            # Server and networking
│   │   └── mod.rs         # TCP server, request handling
│   └── storage/           # Storage layer
│       ├── mod.rs         # Module exports
│       ├── record.rs      # Record and RecordBatch types
│       ├── topic.rs       # TopicManager and Topic
│       ├── partition.rs   # Partition management
│       └── segment.rs     # Segment (file) operations
├── tests/                 # Integration tests (optional)
└── target/                # Build artifacts (gitignored)
```

**Module organization principles**:

- **`lib.rs`**: Re-exports public API, provides crate-level documentation
- **`main.rs`**: Thin wrapper, initializes and runs the server
- **Modules**: Organized by domain (storage, protocol, server, config)
- **Tests**: Co-located with code in `#[cfg(test)]` modules

## Common Patterns

Use these idiomatic Rust patterns when appropriate:

### Builder Pattern

For complex types with many optional fields:

```rust
pub struct ServerConfig {
    host: String,
    port: u16,
    log_level: String,
    data_dir: PathBuf,
}

pub struct ServerConfigBuilder {
    host: Option<String>,
    port: Option<u16>,
    log_level: Option<String>,
    data_dir: Option<PathBuf>,
}

impl ServerConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }
    
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }
    
    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }
    
    pub fn build(self) -> Result<ServerConfig> {
        Ok(ServerConfig {
            host: self.host.unwrap_or_else(|| "localhost".to_string()),
            port: self.port.unwrap_or(9092),
            log_level: self.log_level.unwrap_or_else(|| "info".to_string()),
            data_dir: self.data_dir.ok_or(StreamlineError::Config("data_dir required".into()))?,
        })
    }
}
```

### Newtype Pattern

For type safety and clarity:

```rust
/// Partition ID (newtype for clarity and type safety)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PartitionId(pub i32);

impl PartitionId {
    pub fn new(id: i32) -> Result<Self> {
        if id < 0 {
            return Err(StreamlineError::Config("Partition ID must be non-negative".into()));
        }
        Ok(Self(id))
    }
}

/// Topic name (newtype to prevent string confusion)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicName(String);

impl TopicName {
    pub fn new(name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        if name.is_empty() {
            return Err(StreamlineError::Config("Topic name cannot be empty".into()));
        }
        Ok(Self(name))
    }
    
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
```

### From/Into Traits

For convenient conversions:

```rust
impl From<std::io::Error> for StreamlineError {
    fn from(err: std::io::Error) -> Self {
        StreamlineError::Io(err)
    }
}

// Enable .into() conversions
let topic_name: TopicName = "my-topic".to_string().try_into()?;

// Or implement TryFrom for fallible conversions
impl TryFrom<String> for TopicName {
    type Error = StreamlineError;
    
    fn try_from(s: String) -> Result<Self> {
        TopicName::new(s)
    }
}
```

### Default Trait

For sensible default values:

```rust
impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 9092,
            log_level: "info".to_string(),
            data_dir: PathBuf::from("./data"),
        }
    }
}

// Use: let config = ServerConfig::default();
```

### Iterator Adapters

Use iterator combinators instead of explicit loops:

```rust
// Bad: Explicit loop
let mut total = 0;
for record in records.iter() {
    total += record.size();
}

// Good: Iterator chain
let total: usize = records.iter().map(|r| r.size()).sum();

// Filter and map
let large_records: Vec<_> = records.iter()
    .filter(|r| r.size() > 1024)
    .map(|r| r.offset)
    .collect();
```

### Resource Management (RAII)

Use Drop for cleanup:

```rust
pub struct Segment {
    file: File,
    path: PathBuf,
}

impl Drop for Segment {
    fn drop(&mut self) {
        // Cleanup happens automatically
        let _ = self.file.sync_all();  // Best effort flush
    }
}
```

### Type State Pattern

For compile-time state validation:

```rust
pub struct Connection<State> {
    socket: TcpStream,
    state: PhantomData<State>,
}

pub struct Disconnected;
pub struct Connected;

impl Connection<Disconnected> {
    pub async fn connect(addr: &str) -> Result<Connection<Connected>> {
        let socket = TcpStream::connect(addr).await?;
        Ok(Connection {
            socket,
            state: PhantomData,
        })
    }
}

impl Connection<Connected> {
    pub async fn send(&mut self, data: &[u8]) -> Result<()> {
        self.socket.write_all(data).await?;
        Ok(())
    }
}
```

### Interior Mutability

When you need mutation through shared references:

```rust
use std::sync::{Arc, Mutex};

pub struct TopicManager {
    topics: Arc<Mutex<HashMap<String, Topic>>>,
}

impl TopicManager {
    pub async fn create_topic(&self, name: String) -> Result<()> {
        let mut topics = self.topics.lock().unwrap();
        topics.insert(name, Topic::new());
        Ok(())
    }
}
```

For async, prefer `tokio::sync`:

```rust
use tokio::sync::RwLock;

pub struct TopicManager {
    topics: Arc<RwLock<HashMap<String, Topic>>>,
}

impl TopicManager {
    pub async fn get_topic(&self, name: &str) -> Option<Topic> {
        let topics = self.topics.read().await;
        topics.get(name).cloned()
    }
}
```

---

## Quick Reference

**File modification checklist**:
- [ ] Code follows rustfmt style
- [ ] No clippy warnings
- [ ] Tests added/updated
- [ ] Doc comments for public items
- [ ] Error handling uses `?` operator
- [ ] No unwrap/expect in production paths
- [ ] Async functions don't block

**Before committing**:
```bash
cargo fmt && cargo clippy --all-targets -- -D warnings && cargo test
```

**Performance considerations**:
- Avoid clones (use references)
- Use `Bytes` for zero-copy data
- Prefer iterators over loops
- Don't block async runtime

**Common mistakes to avoid**:
- Using `unwrap()` or `panic!()` in library code
- Blocking operations in async code
- Unnecessary `.clone()` calls
- Missing error context
- Undocumented public APIs
- Large enum variants without boxing
