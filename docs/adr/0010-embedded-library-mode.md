# ADR-0010: Embedded Library Mode (StreamlineEngine Trait)

## Status
Accepted

## Context
Most streaming platforms (Kafka, Redpanda, Pulsar) are server-only — they
require a separate process to run. There's no "SQLite for streaming" that
can be embedded directly into an application as a library.

## Decision
Expose a stable `StreamlineEngine` trait that enables using Streamline as an
embedded library (Rust crate) and via C FFI bindings for other languages.

## API Surface

```rust
pub trait StreamlineEngine: Send + Sync {
    fn engine_create_topic(&self, name: &str, partitions: i32) -> Result<()>;
    fn engine_delete_topic(&self, name: &str) -> Result<()>;
    fn engine_list_topics(&self) -> Result<Vec<String>>;
    fn engine_produce(&self, topic: &str, partition: i32, key: Option<&[u8]>, value: &[u8]) -> Result<i64>;
    fn engine_consume(&self, topic: &str, partition: i32, offset: i64, max_records: usize) -> Result<Vec<EngineRecord>>;
    fn engine_latest_offset(&self, topic: &str, partition: i32) -> Result<i64>;
    fn engine_flush(&self) -> Result<()>;
    fn engine_version(&self) -> &str;
}
```

## C FFI

```c
StreamlineHandle* streamline_create_in_memory();
StreamlineHandle* streamline_create_persistent(const char* data_dir);
void streamline_destroy(StreamlineHandle* handle);
int streamline_create_topic(StreamlineHandle* handle, const char* topic, int partitions);
int64_t streamline_produce(StreamlineHandle* handle, const char* topic, int partition, ...);
```

## Rationale
- **Unique positioning** — No competitor offers embedded streaming
- **Testing** — Integration tests can use the library directly (no Docker)
- **Edge** — Embed in IoT devices without a separate server process
- **Language bindings** — C FFI enables Python (PyO3), Java (JNI), Node (napi-rs)

## Consequences
- **Positive**: Enables "SQLite for streaming" use case
- **Positive**: Testcontainers modules can start in <100ms (no Docker needed)
- **Positive**: C header (`include/streamline.h`) is 294 lines — small, stable API
- **Negative**: Must maintain API stability across versions
- **Negative**: All 52 unsafe FFI blocks require SAFETY documentation
- **Mitigation**: `StreamlineEngine` trait marked Stable in API_STABILITY.md;
  all unsafe blocks verified to have SAFETY comments
