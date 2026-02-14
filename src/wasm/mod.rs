//! WebAssembly (WASM) message transformation framework.
//!
//! This module re-exports from the `streamline-wasm` workspace crate,
//! which isolates the heavy `wasmtime` dependency into its own compilation unit.

// Re-export all public items from the workspace crate
pub use streamline_wasm::api;
pub use streamline_wasm::builtins;
pub use streamline_wasm::catalog;
pub use streamline_wasm::function_registry;
pub use streamline_wasm::runtime;
pub use streamline_wasm::RouteResult;
pub use streamline_wasm::TransformInput;
pub use streamline_wasm::TransformOutput;
pub use streamline_wasm::TransformResult;
pub use streamline_wasm::TransformationConfig;
pub use streamline_wasm::TransformationRegistry;
pub use streamline_wasm::TransformationStats;
pub use streamline_wasm::TransformationType;
