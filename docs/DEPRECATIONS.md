# Deprecations

This document tracks deprecated features, APIs, and configuration options in Streamline.

## Deprecation Policy

1. **Announcement:** Deprecated features are announced in the CHANGELOG and marked with `#[deprecated]` (Rust) or equivalent annotations in SDKs.
2. **Grace period:** Deprecated features remain functional for at least **2 minor releases** after deprecation.
3. **Removal:** Deprecated features are removed in the next minor release after the grace period expires.
4. **Migration:** Each deprecation includes a migration path to the replacement.

### Versioning Example

| Version | Action |
|---------|--------|
| 0.3.0 | Feature X deprecated, replacement Y introduced |
| 0.4.0 | Feature X still functional (grace period) |
| 0.5.0 | Feature X removed |

## Stability Levels

Features follow the stability tiers documented in [API Stability](/docs/api-stability):

| Level | Breaking Changes | Deprecation Required |
|-------|-----------------|---------------------|
| **Stable** | Major versions only | Yes, 2 minor version grace |
| **Beta** | Minor versions with notice | Yes, 1 minor version grace |
| **Experimental** | Any time | No grace period required |

## Current Deprecations

### v0.2.0

_No deprecated features in the current release._

All APIs, configuration options, and SDK interfaces in v0.2.0 are active and supported.

## Deprecated Configuration Options

_None at this time._

## Deprecated CLI Commands

_None at this time._

## Deprecated SDK APIs

_None at this time._

## How to Report Issues

If you encounter behavior that seems deprecated but isn't documented here, please open an issue:
- [Core server](https://github.com/streamlinelabs/streamline/issues)
- [SDK-specific repos](https://github.com/streamlinelabs)
