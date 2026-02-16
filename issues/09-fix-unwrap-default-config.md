# Fix Unwrap Usage in Default Config

## Summary

Remove `.unwrap()` calls in `ServerConfig::default()` which violate the project's error handling conventions.

## Current State

`src/config/mod.rs:135-136`:
```rust
impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: DEFAULT_KAFKA_ADDR.parse().unwrap(),  // <- unwrap
            http_addr: DEFAULT_HTTP_ADDR.parse().unwrap(),    // <- unwrap
            // ...
        }
    }
}
```

Per `CLAUDE.md` coding conventions:
> **Never** use `.unwrap()` or `.expect()` in production code paths

## Solution

Since `Default` trait cannot return `Result`, and these are compile-time constants that are guaranteed valid, use `const` assertions or lazy_static:

### Option A: Const assertion (preferred)

```rust
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

const DEFAULT_KAFKA_ADDR: SocketAddr = SocketAddr::new(
    IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
    9092
);

const DEFAULT_HTTP_ADDR: SocketAddr = SocketAddr::new(
    IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
    9094
);

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: DEFAULT_KAFKA_ADDR,
            http_addr: DEFAULT_HTTP_ADDR,
            // ...
        }
    }
}
```

### Option B: Lazy static with expect (acceptable for constants)

```rust
use std::sync::LazyLock;

static DEFAULT_KAFKA_ADDR_PARSED: LazyLock<SocketAddr> = LazyLock::new(|| {
    DEFAULT_KAFKA_ADDR.parse().expect("DEFAULT_KAFKA_ADDR is invalid - this is a bug")
});
```

### Option C: Document the safety

If keeping `.unwrap()`, add safety comment:
```rust
// SAFETY: DEFAULT_KAFKA_ADDR is a valid socket address literal,
// parse cannot fail. Covered by test_default_constants().
listen_addr: DEFAULT_KAFKA_ADDR.parse().unwrap(),
```

## Implementation Tasks

1. Choose approach (recommend Option A)
2. Update constant definitions
3. Update `Default` impl
4. Ensure tests still pass
5. Run `cargo clippy` to verify

## Acceptance Criteria

- [ ] No `.unwrap()` on potentially fallible operations
- [ ] `cargo clippy --all-targets -- -D warnings` passes
- [ ] Existing tests pass
- [ ] Default config still works

## Related Files

- `src/config/mod.rs:135-136`

## Labels

`bug`, `code-quality`, `good-first-issue`
