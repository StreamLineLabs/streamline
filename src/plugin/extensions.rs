//! Streamline Extensions SDK
//!
//! Framework for building, testing, and publishing custom connectors,
//! transforms, and protocol adapters as WASM modules.
//!
//! # Extension Lifecycle
//!
//! ```bash
//! # Create a new extension
//! streamline-cli extension create my-transform --type transform
//!
//! # Develop (generates Cargo.toml, src/lib.rs, tests/)
//! cd my-transform && cargo build --target wasm32-wasip1
//!
//! # Test locally against a Streamline container
//! streamline-cli extension test
//!
//! # Build optimized WASM
//! streamline-cli extension build
//!
//! # Publish to marketplace
//! streamline-cli extension publish --version 0.1.0
//! ```
//!
//! # Extension Types
//!
//! | Type | Trait | Description |
//! |------|-------|-------------|
//! | Transform | `StreamlineTransform` | Process messages in-flight (filter, enrich, convert) |
//! | Sink | `StreamlineSink` | Write messages to external systems |
//! | Source | `StreamlineSource` | Read from external systems into topics |

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Extension types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ExtensionType {
    Transform,
    Sink,
    Source,
}

impl std::fmt::Display for ExtensionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transform => write!(f, "transform"),
            Self::Sink => write!(f, "sink"),
            Self::Source => write!(f, "source"),
        }
    }
}

impl std::str::FromStr for ExtensionType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "transform" => Ok(Self::Transform),
            "sink" => Ok(Self::Sink),
            "source" => Ok(Self::Source),
            _ => Err(format!("Unknown extension type: '{}'. Use transform, sink, or source.", s)),
        }
    }
}

/// Manifest for a Streamline extension (extension.toml)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtensionManifest {
    pub name: String,
    pub version: String,
    pub description: String,
    pub extension_type: ExtensionType,
    pub author: String,
    pub license: String,
    pub min_streamline_version: String,
    pub categories: Vec<String>,
    pub config_schema: Vec<ConfigField>,
}

/// A configuration field that the extension accepts
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigField {
    pub name: String,
    pub description: String,
    pub field_type: String,
    pub required: bool,
    pub default: Option<String>,
}

/// Scaffold a new extension project
pub fn scaffold_extension(name: &str, ext_type: ExtensionType, output_dir: &Path) -> Result<Vec<PathBuf>, String> {
    let dir = output_dir.join(name);
    std::fs::create_dir_all(&dir).map_err(|e| format!("Failed to create directory: {}", e))?;

    let mut created_files = Vec::new();

    // Cargo.toml
    let cargo_toml = format!(r#"[package]
name = "{name}"
version = "0.1.0"
edition = "2021"
description = "A Streamline {ext_type} extension"
license = "Apache-2.0"

[lib]
crate-type = ["cdylib"]

[dependencies]
serde = {{ version = "1", features = ["derive"] }}
serde_json = "1"

[profile.release]
opt-level = "s"
lto = true
strip = true
"#, name = name, ext_type = ext_type);

    let cargo_path = dir.join("Cargo.toml");
    std::fs::write(&cargo_path, cargo_toml).map_err(|e| e.to_string())?;
    created_files.push(cargo_path);

    // src/lib.rs
    std::fs::create_dir_all(dir.join("src")).map_err(|e| e.to_string())?;

    let lib_rs = match ext_type {
        ExtensionType::Transform => format!(r#"//! {name} — Streamline Transform Extension
//!
//! Processes messages in-flight as they flow between topics.

use serde::{{Deserialize, Serialize}};
use serde_json::Value;

/// Configuration for this transform
#[derive(Debug, Deserialize)]
pub struct Config {{
    // Add your configuration fields here
}}

/// Process a single message.
///
/// Return `Some(output)` to forward the message, or `None` to filter it out.
#[no_mangle]
pub extern "C" fn transform(input_ptr: *const u8, input_len: usize) -> i64 {{
    // SAFETY: Caller guarantees input_ptr is valid for input_len bytes per the WASM FFI contract.
    let input = unsafe {{ std::slice::from_raw_parts(input_ptr, input_len) }};

    match serde_json::from_slice::<Value>(input) {{
        Ok(mut value) => {{
            // TODO: Implement your transform logic here
            // Example: add a timestamp field
            if let Some(obj) = value.as_object_mut() {{
                obj.insert(
                    "processed_by".to_string(),
                    Value::String("{name}".to_string()),
                );
            }}

            // Return the modified message
            let output = serde_json::to_vec(&value).unwrap_or_default();
            // In production: write output to WASM memory and return pointer+length
            output.len() as i64
        }}
        Err(_) => -1, // Signal error
    }}
}}

#[cfg(test)]
mod tests {{
    use super::*;

    #[test]
    fn test_transform() {{
        let input = serde_json::json!({{"event": "test", "value": 42}});
        let bytes = serde_json::to_vec(&input).unwrap();
        let result = transform(bytes.as_ptr(), bytes.len());
        assert!(result > 0);
    }}
}}
"#, name = name),

        ExtensionType::Sink => format!(r#"//! {name} — Streamline Sink Extension
//!
//! Writes messages from Streamline topics to an external system.

use serde::Deserialize;
use serde_json::Value;

/// Configuration for this sink
#[derive(Debug, Deserialize)]
pub struct Config {{
    // Add your configuration fields here (e.g., endpoint URL, auth)
}}

/// Initialize the sink with configuration.
#[no_mangle]
pub extern "C" fn init(_config_ptr: *const u8, _config_len: usize) -> i32 {{
    // TODO: Parse config, establish connections
    0 // Return 0 for success
}}

/// Write a batch of messages to the external system.
#[no_mangle]
pub extern "C" fn write(input_ptr: *const u8, input_len: usize) -> i32 {{
    // SAFETY: Caller guarantees input_ptr is valid for input_len bytes per the WASM FFI contract.
    let input = unsafe {{ std::slice::from_raw_parts(input_ptr, input_len) }};

    match serde_json::from_slice::<Vec<Value>>(input) {{
        Ok(messages) => {{
            for msg in &messages {{
                // TODO: Write each message to your external system
                let _ = msg;
            }}
            messages.len() as i32
        }}
        Err(_) => -1,
    }}
}}

/// Flush any buffered data.
#[no_mangle]
pub extern "C" fn flush() -> i32 {{
    0
}}

#[cfg(test)]
mod tests {{
    use super::*;

    #[test]
    fn test_write() {{
        let msgs = serde_json::to_vec(&vec![
            serde_json::json!({{"key": "a", "value": "1"}}),
            serde_json::json!({{"key": "b", "value": "2"}}),
        ]).unwrap();
        let result = write(msgs.as_ptr(), msgs.len());
        assert_eq!(result, 2);
    }}
}}
"#, name = name),

        ExtensionType::Source => format!(r#"//! {name} — Streamline Source Extension
//!
//! Reads data from an external system and produces to Streamline topics.

use serde::Deserialize;
use serde_json::Value;

/// Configuration for this source
#[derive(Debug, Deserialize)]
pub struct Config {{
    // Add your configuration fields here (e.g., poll URL, interval)
}}

/// Initialize the source with configuration.
#[no_mangle]
pub extern "C" fn init(_config_ptr: *const u8, _config_len: usize) -> i32 {{
    0
}}

/// Poll for new records. Returns JSON array of records.
#[no_mangle]
pub extern "C" fn poll() -> i64 {{
    // TODO: Poll your external system for new data
    let records: Vec<Value> = vec![
        serde_json::json!({{"source": "{name}", "data": "sample"}}),
    ];
    let output = serde_json::to_vec(&records).unwrap_or_default();
    output.len() as i64
}}

#[cfg(test)]
mod tests {{
    use super::*;

    #[test]
    fn test_poll() {{
        let result = poll();
        assert!(result > 0);
    }}
}}
"#, name = name),
    };

    let lib_path = dir.join("src/lib.rs");
    std::fs::write(&lib_path, lib_rs).map_err(|e| e.to_string())?;
    created_files.push(lib_path);

    // extension.toml manifest
    let manifest = format!(r#"# Streamline Extension Manifest
[extension]
name = "{name}"
version = "0.1.0"
description = "A custom Streamline {ext_type} extension"
type = "{ext_type}"
author = ""
license = "Apache-2.0"
min_streamline_version = "0.2.0"
categories = ["{ext_type}"]

# Configuration fields accepted by this extension
# [[config]]
# name = "endpoint"
# description = "Target endpoint URL"
# type = "string"
# required = true
"#, name = name, ext_type = ext_type);

    let manifest_path = dir.join("extension.toml");
    std::fs::write(&manifest_path, manifest).map_err(|e| e.to_string())?;
    created_files.push(manifest_path);

    // README.md
    let readme = format!(r#"# {name}

A Streamline **{ext_type}** extension.

## Build

```bash
# Install WASM target
rustup target add wasm32-wasip1

# Build
cargo build --target wasm32-wasip1 --release

# Optimize (optional)
wasm-opt -Os target/wasm32-wasip1/release/{name}.wasm -o {name}.wasm
```

## Test

```bash
# Unit tests
cargo test

# Integration test against Streamline
streamline-cli extension test
```

## Publish

```bash
streamline-cli extension publish --version 0.1.0
```

## Usage

```yaml
# In a connector pipeline YAML:
transforms:
  - type: wasm
    config:
      module: {name}
      version: "0.1.0"
```
"#, name = name, ext_type = ext_type);

    let readme_path = dir.join("README.md");
    std::fs::write(&readme_path, readme).map_err(|e| e.to_string())?;
    created_files.push(readme_path);

    // Makefile
    let makefile = format!(r#"TARGET = wasm32-wasip1
NAME = {name}

.PHONY: build test clean publish

build:
	cargo build --target $(TARGET) --release

test:
	cargo test
	@echo "Run 'streamline-cli extension test' for integration testing"

clean:
	cargo clean

publish: build
	streamline-cli extension publish --version $(shell grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)"/\1/')

.DEFAULT_GOAL := build
"#, name = name);

    let make_path = dir.join("Makefile");
    std::fs::write(&make_path, makefile).map_err(|e| e.to_string())?;
    created_files.push(make_path);

    // .gitignore
    std::fs::write(dir.join(".gitignore"), "/target\n*.wasm\n").map_err(|e| e.to_string())?;
    created_files.push(dir.join(".gitignore"));

    Ok(created_files)
}

/// Parse an extension manifest from a TOML file
pub fn parse_manifest(path: &Path) -> Result<ExtensionManifest, String> {
    let content = std::fs::read_to_string(path).map_err(|e| format!("Failed to read {}: {}", path.display(), e))?;
    let toml_value: toml::Value = toml::from_str(&content).map_err(|e| format!("Invalid TOML: {}", e))?;

    let ext = toml_value.get("extension").ok_or("Missing [extension] section")?;

    Ok(ExtensionManifest {
        name: ext.get("name").and_then(|v| v.as_str()).unwrap_or("unknown").to_string(),
        version: ext.get("version").and_then(|v| v.as_str()).unwrap_or("0.0.0").to_string(),
        description: ext.get("description").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        extension_type: ext.get("type").and_then(|v| v.as_str()).unwrap_or("transform").parse().unwrap_or(ExtensionType::Transform),
        author: ext.get("author").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        license: ext.get("license").and_then(|v| v.as_str()).unwrap_or("Apache-2.0").to_string(),
        min_streamline_version: ext.get("min_streamline_version").and_then(|v| v.as_str()).unwrap_or("0.2.0").to_string(),
        categories: ext.get("categories").and_then(|v| v.as_array()).map(|a| a.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect()).unwrap_or_default(),
        config_schema: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_scaffold_transform() {
        let dir = std::env::temp_dir().join("streamline-ext-test-transform");
        let _ = std::fs::remove_dir_all(&dir);

        let files = scaffold_extension("my-filter", ExtensionType::Transform, &dir).unwrap();
        assert!(files.len() >= 5);

        let cargo = std::fs::read_to_string(dir.join("my-filter/Cargo.toml")).unwrap();
        assert!(cargo.contains("my-filter"));
        assert!(cargo.contains("cdylib"));

        let lib = std::fs::read_to_string(dir.join("my-filter/src/lib.rs")).unwrap();
        assert!(lib.contains("transform"));
        assert!(lib.contains("#[no_mangle]"));

        let manifest = std::fs::read_to_string(dir.join("my-filter/extension.toml")).unwrap();
        assert!(manifest.contains("my-filter"));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_scaffold_sink() {
        let dir = std::env::temp_dir().join("streamline-ext-test-sink");
        let _ = std::fs::remove_dir_all(&dir);

        let files = scaffold_extension("my-sink", ExtensionType::Sink, &dir).unwrap();
        assert!(files.len() >= 5);

        let lib = std::fs::read_to_string(dir.join("my-sink/src/lib.rs")).unwrap();
        assert!(lib.contains("write"));
        assert!(lib.contains("flush"));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_scaffold_source() {
        let dir = std::env::temp_dir().join("streamline-ext-test-source");
        let _ = std::fs::remove_dir_all(&dir);

        let files = scaffold_extension("my-source", ExtensionType::Source, &dir).unwrap();
        assert!(files.len() >= 5);

        let lib = std::fs::read_to_string(dir.join("my-source/src/lib.rs")).unwrap();
        assert!(lib.contains("poll"));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_extension_type_parse() {
        assert_eq!("transform".parse::<ExtensionType>().unwrap(), ExtensionType::Transform);
        assert_eq!("sink".parse::<ExtensionType>().unwrap(), ExtensionType::Sink);
        assert_eq!("source".parse::<ExtensionType>().unwrap(), ExtensionType::Source);
        assert!("unknown".parse::<ExtensionType>().is_err());
    }
}
