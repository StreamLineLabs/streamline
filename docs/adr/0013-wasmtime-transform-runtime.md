# ADR-0013: Wasmtime for Stream Transform Runtime

## Status

Accepted

## Context

Streamline supports user-defined message transformations (filtering, format conversion, field extraction, data masking) that execute inline in the message pipeline. We needed a sandboxed execution environment that:

1. Isolates user code from the server process (security boundary)
2. Supports multiple source languages (Rust, Go, AssemblyScript, C)
3. Has near-native execution performance for hot-path message processing
4. Can be loaded and unloaded dynamically without server restarts
5. Integrates with the WASM marketplace for community-contributed transforms

Candidates considered:
- **Wasmtime** — Bytecode Alliance reference WASM runtime, Rust-native
- **Wasmer** — Alternative WASM runtime with multiple compilation backends
- **V8/Deno** — JavaScript engine with WASM support
- **Native plugins** — Shared library loading (`.so`/`.dylib`)

## Decision

We chose **Wasmtime** as the WebAssembly runtime for the transform engine:

- **Security**: Strong sandboxing with capability-based security model — user transforms cannot access filesystem, network, or server memory
- **Rust-native**: First-class Rust API, developed by the Bytecode Alliance
- **Performance**: Cranelift JIT compiler provides near-native execution speed
- **Standards compliance**: Reference implementation of WASI, ensuring portable transforms
- **Ecosystem**: Largest WASM ecosystem, supports compilation from Rust, C, Go, AssemblyScript, and others

The transform engine (`crates/streamline-wasm/`) provides:
- `TransformationEngine` for managing transform lifecycle
- `WasmRuntime` for instance management and execution
- Pipeline chaining for composing multiple transforms
- Batch processing support for throughput optimization

## Consequences

### Positive

- User code is fully sandboxed — a buggy or malicious transform cannot crash the server
- Transforms are portable across architectures (compile once, run anywhere)
- Hot-reloading of transforms without server restart via the marketplace registry
- Single-message and batch processing modes optimize for different workloads

### Negative

- WASM has limited access to system resources by design — transforms that need network or filesystem access require explicit capability grants
- JIT compilation adds startup latency for first invocation of each transform
- Debugging WASM transforms is harder than native code (limited tooling for source maps)

### Neutral

- The marketplace registry (`streamline-marketplace/`) centralizes transform distribution
- Transform authors need a WASM-compatible toolchain (e.g., `wasm-pack` for Rust, `tinygo` for Go)
