# Replace Unsafe Unwrap Calls in Default Configuration

## Priority: Critical

## Summary

The `ServerConfig::default()` implementation uses `.unwrap()` on hardcoded string parsing. While these are technically safe because the strings are constants, using `.unwrap()` is against project conventions and could mask issues if constants are ever modified incorrectly.

## Current State

In `src/config/mod.rs`, the `Default` impl for `ServerConfig` contains:

```rust
impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            kafka_addr: DEFAULT_KAFKA_ADDR.parse().unwrap(),
            http_addr: DEFAULT_HTTP_ADDR.parse().unwrap(),
            // ... more unwraps
        }
    }
}
```

## Requirements

### Option 1: Use `.expect()` with Safety Comments (Recommended)

```rust
impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            kafka_addr: DEFAULT_KAFKA_ADDR
                .parse()
                .expect("DEFAULT_KAFKA_ADDR is a valid socket address"),
            http_addr: DEFAULT_HTTP_ADDR
                .parse()
                .expect("DEFAULT_HTTP_ADDR is a valid socket address"),
            // ...
        }
    }
}
```

### Option 2: Compile-Time Validation with `const fn`

Create const functions that validate addresses at compile time, ensuring invalid defaults cause build failures rather than runtime panics.

### Option 3: Lazy Static with Validation

Use `once_cell::sync::Lazy` to parse once at startup with proper error handling.

### Implementation Tasks

1. Identify all `.unwrap()` calls in `src/config/mod.rs`
2. Replace with `.expect()` containing safety rationale
3. Add unit test that exercises `ServerConfig::default()` explicitly
4. Review other files for similar patterns
5. Consider adding clippy lint `#![deny(clippy::unwrap_used)]` with exceptions

### Files to Review

- `src/config/mod.rs` - Main configuration
- `src/server/mod.rs` - Server defaults
- `src/storage/*.rs` - Storage defaults

### Acceptance Criteria

- [ ] No bare `.unwrap()` calls on fallible operations in production paths
- [ ] All `.expect()` calls have descriptive messages
- [ ] Unit test exercises default configuration
- [ ] Code review confirms no panicking paths in normal operation

## Related Files

- `src/config/mod.rs` - Configuration defaults
- `CLAUDE.md` - Coding conventions (documents no-unwrap rule)

## Labels

`critical`, `code-quality`, `production-readiness`
