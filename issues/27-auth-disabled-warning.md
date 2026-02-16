# Warn When Authentication is Disabled

## Priority: Quick Win

## Summary

Log a warning at startup when authentication is disabled. This helps operators notice misconfigurations before deploying to production.

## Current State

- Auth disabled by default (`--auth-enabled=false`)
- No warning logged when auth disabled
- Easy to accidentally deploy without auth

## Implementation

In `src/main.rs` or server startup:

```rust
if !config.auth.enabled {
    tracing::warn!(
        "⚠️  Authentication is DISABLED. All clients can connect without credentials. \
         Enable with --auth-enabled=true for production deployments."
    );
}

if config.auth.enabled && config.auth.allow_anonymous {
    tracing::warn!(
        "⚠️  Anonymous connections allowed. Consider disabling with --allow-anonymous=false"
    );
}

if !config.tls.enabled {
    tracing::warn!(
        "⚠️  TLS is DISABLED. Traffic is unencrypted. \
         Enable with --tls-enabled=true for production deployments."
    );
}
```

## Acceptance Criteria

- [ ] Warning logged when auth disabled
- [ ] Warning logged when TLS disabled
- [ ] Warning logged when anonymous allowed with auth enabled
- [ ] Warnings are visible and clear

## Effort

~30 minutes

## Labels

`quick-win`, `security`, `developer-experience`
