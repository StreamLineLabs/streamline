# WASM Transforms — Design & Prototype

> **Status**: Experimental
> **Feature flag**: `wasm-transforms` (Cargo.toml)
> **Tracking ADR**: [ADR-026: Embedded WASM Transforms](../../../streamline-docs/docs/architecture/decisions/adr-026-wasm-transforms.md)

## Overview

Embedded WASM transforms enable server-side message processing — filter, map, enrich, and route messages as they flow through Streamline topics. User-supplied WebAssembly modules run in a sandboxed `wasmtime` runtime directly inside the server process, eliminating the need for external stream processing applications.

## Transform Specification Format

Each transform is defined by a YAML configuration that pairs a compiled WASM binary with input/output topic routing and optional configuration:

```yaml
name: mask-pii
input_topic: raw-events
output_topic: clean-events
wasm_module: transforms/mask_pii.wasm
config:
  fields_to_mask: ["email", "phone", "ssn"]
```

```yaml
name: json-to-avro
input_topic: api-logs
output_topic: api-logs-avro
wasm_module: transforms/json_to_avro.wasm
config:
  schema_registry_subject: api-logs-value
```

```yaml
name: route-by-region
input_topic: orders
output_topic: ""  # determined by the module's route function
wasm_module: transforms/region_router.wasm
config:
  route_field: "region"
  topic_prefix: "orders-"
```

### Specification Fields

| Field | Required | Description |
|---|---|---|
| `name` | Yes | Unique transform identifier (kebab-case) |
| `input_topic` | Yes | Source topic to consume from |
| `output_topic` | Yes | Destination topic (empty string if using routing) |
| `wasm_module` | Yes | Path to the compiled `.wasm` binary |
| `config` | No | Arbitrary key-value configuration passed to the module |
| `instances` | No | Number of parallel instances (default: 1) |
| `max_memory_mb` | No | Per-instance memory limit (default: 64) |
| `timeout_ms` | No | Per-message execution timeout (default: 10) |

## Transform Trait / Interface

WASM modules must export specific functions that Streamline calls during message processing.

### Required Export

```rust
/// Transform a single message. Called for every message consumed from the input topic.
/// Input: raw message bytes (key + value concatenated with a 4-byte length prefix for the key)
/// Output: transformed message bytes in the same format
#[no_mangle]
pub extern "C" fn transform(input_ptr: *const u8, input_len: u32, output_ptr: *mut u8) -> u32;
```

Simplified Rust SDK view (what the user writes):

```rust
// Transform function signature
fn transform(input: &[u8]) -> Vec<u8>;
```

### Optional Exports

```rust
// Filter function — return false to drop the message before transformation
fn filter(input: &[u8]) -> bool;

// Init function — called once when the module is loaded, receives config JSON
fn init(config: &[u8]) -> bool;

// Route function — return a topic name for content-based routing
fn route(input: &[u8]) -> String;
```

### Host-Guest Memory Protocol

WASM modules operate on linear memory shared between host and guest:

1. Host writes input bytes to guest memory at a pre-allocated buffer
2. Host calls the exported function with pointer and length
3. Guest writes output bytes to a separate output buffer
4. Host reads the output using the returned length

The `streamline-wasm` crate handles this protocol transparently through the `TransformInput` / `TransformOutput` types.

## CLI Commands

```bash
# List all deployed transforms
streamline-cli transforms list

# Deploy a new transform
streamline-cli transforms deploy \
  --name mask-pii \
  --wasm ./mask_pii.wasm \
  --input raw-events \
  --output clean-events \
  --config '{"fields_to_mask": ["email", "phone", "ssn"]}'

# Deploy from a YAML spec file
streamline-cli transforms deploy --spec transforms/mask-pii.yaml

# Check transform status
streamline-cli transforms status mask-pii

# View transform metrics (messages processed, errors, avg latency)
streamline-cli transforms metrics mask-pii

# Pause a running transform
streamline-cli transforms pause mask-pii

# Resume a paused transform
streamline-cli transforms resume mask-pii

# Undeploy a transform
streamline-cli transforms undeploy mask-pii

# Validate a WASM module without deploying
streamline-cli transforms validate ./mask_pii.wasm
```

## Performance Considerations

### WASM Compilation

`wasmtime` supports two compilation strategies:

- **Ahead-of-time (AOT)**: Compile WASM to native code at deploy time. Adds ~100ms deploy latency but eliminates per-invocation JIT overhead.
- **Just-in-time (JIT)**: Compile on first invocation. Faster deploy but slower first message.

Streamline uses **AOT compilation by default**. The compiled native code is cached alongside the WASM binary in the transform catalog.

### Instance Pooling

Creating a new WASM instance per message is expensive (~50μs). Instead, Streamline maintains an **instance pool** per transform:

```
┌─────────────────────────────────────────────┐
│              Transform: mask-pii            │
│                                             │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐   │
│  │Instance 1│ │Instance 2│ │Instance 3│   │
│  │  (busy)  │ │ (idle)   │ │ (idle)   │   │
│  └──────────┘ └──────────┘ └──────────┘   │
│                                             │
│  Pool size: min=1, max=instances config     │
│  Idle timeout: 30s                          │
└─────────────────────────────────────────────┘
```

- Instances are pre-warmed on deploy
- Pool scales between 1 and the configured `instances` count
- Idle instances are reclaimed after 30 seconds
- Each instance maintains its own linear memory (no cross-instance state leakage)

### Throughput Targets

| Transform Complexity | Target Latency (p99) | Target Throughput |
|---|---|---|
| Pass-through (no-op) | < 5μs | > 1M msg/s |
| Simple field masking | < 50μs | > 200K msg/s |
| JSON parse + transform | < 200μs | > 50K msg/s |
| Complex enrichment | < 1ms | > 10K msg/s |

### Fuel Metering

`wasmtime` fuel metering prevents runaway transforms:

- Each message gets a fuel budget proportional to `timeout_ms`
- If fuel is exhausted, the instance is terminated and the message is routed to a dead-letter topic
- Fuel consumption is tracked in `TransformationStats`

## Example Transform: PII Masking (Rust → WASM)

```rust
// mask_pii/src/lib.rs
// Compile with: cargo build --target wasm32-wasi --release

use serde_json::{Value, Map};

static mut CONFIG: Option<Vec<String>> = None;

#[no_mangle]
pub extern "C" fn init(config_ptr: *const u8, config_len: u32) -> u32 {
    let config_bytes = unsafe { std::slice::from_raw_parts(config_ptr, config_len as usize) };
    let config: Value = serde_json::from_slice(config_bytes).unwrap_or(Value::Null);

    if let Some(fields) = config.get("fields_to_mask").and_then(|v| v.as_array()) {
        let field_names: Vec<String> = fields
            .iter()
            .filter_map(|f| f.as_str().map(String::from))
            .collect();
        unsafe { CONFIG = Some(field_names); }
    }
    1 // success
}

#[no_mangle]
pub extern "C" fn transform(input_ptr: *const u8, input_len: u32, output_ptr: *mut u8) -> u32 {
    let input = unsafe { std::slice::from_raw_parts(input_ptr, input_len as usize) };

    let mut value: Value = match serde_json::from_slice(input) {
        Ok(v) => v,
        Err(_) => return 0, // pass through unparseable messages
    };

    let fields = unsafe { CONFIG.as_ref() };
    if let (Some(obj), Some(fields)) = (value.as_object_mut(), fields) {
        mask_fields(obj, fields);
    }

    let output = serde_json::to_vec(&value).unwrap_or_default();
    let len = output.len();
    unsafe {
        std::ptr::copy_nonoverlapping(output.as_ptr(), output_ptr, len);
    }
    len as u32
}

#[no_mangle]
pub extern "C" fn filter(_input_ptr: *const u8, _input_len: u32) -> u32 {
    1 // accept all messages
}

fn mask_fields(obj: &mut Map<String, Value>, fields: &[String]) {
    for field in fields {
        if let Some(val) = obj.get_mut(field) {
            if val.is_string() {
                *val = Value::String("***REDACTED***".to_string());
            }
        }
    }
}
```

Build and deploy:

```bash
# Build the WASM module
cd mask_pii
cargo build --target wasm32-wasi --release

# Deploy to Streamline
streamline-cli transforms deploy \
  --name mask-pii \
  --wasm target/wasm32-wasi/release/mask_pii.wasm \
  --input raw-events \
  --output clean-events \
  --config '{"fields_to_mask": ["email", "phone", "ssn"]}'

# Verify it's running
streamline-cli transforms status mask-pii
```

## Integration with Existing `src/wasm/` Module

The WASM transforms feature builds on the existing infrastructure in `crates/streamline-wasm/`:

```
crates/streamline-wasm/
├── src/
│   ├── api.rs               ← Transform API types (TransformInput, TransformOutput)
│   ├── runtime.rs            ← wasmtime execution engine, instance lifecycle
│   ├── catalog.rs            ← Transform catalog: registration, discovery, AOT cache
│   ├── builtins.rs           ← Built-in transforms (pass-through, logging, metrics)
│   ├── function_registry.rs  ← Host function registration for WASM imports
│   └── error.rs              ← WASM-specific error types
│
src/wasm/mod.rs               ← Re-exports from streamline-wasm into main server

Key types:
  TransformInput       → Input envelope (message bytes + metadata)
  TransformOutput      → Output envelope (transformed bytes + routing info)
  TransformResult      → Result<TransformOutput, WasmError>
  TransformationRegistry → Global registry of deployed transforms
  TransformationConfig → Per-transform YAML configuration
  TransformationStats  → Per-transform metrics (count, errors, latency)
  TransformationType   → Enum: Filter, Map, Enrich, Route
  RouteResult          → Content-based routing decision
```

### Data Flow

```
Producer → [input_topic] → Transform Pipeline → [output_topic] → Consumer
                               │
                    ┌──────────┴──────────┐
                    │  1. Consume message  │
                    │  2. Deserialize      │
                    │  3. Call filter()    │ ← optional, drop if false
                    │  4. Call transform() │
                    │  5. Call route()     │ ← optional, for routing
                    │  6. Produce to       │
                    │     output_topic     │
                    └─────────────────────┘
```

### Feature Flag Dependency Chain

```
wasm-transforms = ["dep:wasmtime", "streamline-wasm/wasm-runtime"]
faas            = ["wasm-transforms"]
full            = [..., "wasm-transforms", "faas", ...]
```

When `wasm-transforms` is disabled, all WASM types compile to no-op stubs (the `streamline-wasm` crate is always linked, but wasmtime is not).

## Open Questions

1. **Hot reload**: Can we swap a WASM module without dropping messages? Requires draining the instance pool and re-compiling.
2. **State**: Should transforms have access to a persistent key-value store (e.g., for deduplication or windowed aggregation)?
3. **Chaining**: Allow multiple transforms in sequence on a single topic, or require explicit intermediate topics?
4. **Dead letter routing**: How to handle transform failures — DLQ topic, retry with backoff, or skip?
5. **Observability**: Expose per-transform OpenTelemetry spans and Prometheus metrics via the existing metrics infrastructure.
