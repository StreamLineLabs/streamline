# Dead Code Audit Report

Generated: 2026-02-22

This report identifies `#[allow(dead_code)]` annotations in non-test production code.
Each should be reviewed and resolved by one of:

1. **Remove** — the code is truly unused and should be deleted
2. **Feature-gate** — the code is used only with a specific feature flag; add `#[cfg(feature = "X")]`
3. **Keep** — the code is part of a public API or needed for future use (document why)

## Summary

**Total annotations (non-test):** 209
**Files affected:** 40+
**Top priority:** lib.rs (20), cloud_api.rs (11), streamql/join.rs (8)

## Hotspots (by file)

| File | Count | Recommended Action |
|------|-------|-------------------|
| `lib.rs` | 20 | Review — likely module-level allows for experimental modules. Add `#[cfg]` gates instead. |
| `server/cloud_api.rs` | 11 | Feature-gate with `#[cfg(feature = "control-plane")]` |
| `streamql/join.rs` | 8 | Review — join operators may be scaffolded but not connected to planner. Keep if planned. |
| `ai/llm.rs` | 6 | Feature-gate with `#[cfg(feature = "ai")]` |
| `ai/anomaly.rs` | 6 | Feature-gate with `#[cfg(feature = "ai")]` |
| `edge/sync.rs` | 5 | Feature-gate with `#[cfg(feature = "edge")]` |
| `multitenancy/namespaces.rs` | 4 | Review — may be scaffolded for cloud multi-tenancy |
| `cdc/mysql.rs` | 4 | Feature-gate with `#[cfg(feature = "mysql-cdc")]` |
| `cdc/mongodb.rs` | 4 | Feature-gate with `#[cfg(feature = "mongodb-cdc")]` |
| `storage/segment_s3.rs` | 3 | Feature-gate with `#[cfg(feature = "cloud-storage")]` |
| `storage/bloom.rs` | 3 | Review — bloom filters should be active in storage reads |
| `stateful/windows.rs` | 3 | Review — windowing operators for StreamQL |
| `server/limits.rs` | 3 | Review — quota management should be active |
| `connect/runtime.rs` | 3 | Review — connector runtime should be active |

## Action Plan

### Phase 1: Quick Wins (replace `allow(dead_code)` with `#[cfg]`)
Target: CDC, AI, edge modules that are already feature-gated at module level.
These annotations are redundant if the module itself is `#[cfg]`-gated in lib.rs.

### Phase 2: Review & Remove
Target: lib.rs, cloud_api.rs, streamql/join.rs — determine if code is
genuinely unused or just not yet connected.

### Phase 3: Track
Any remaining `allow(dead_code)` should have a `// Reason: ...` comment
explaining why the dead code is intentionally kept.

## Tracking

To monitor progress:
```bash
# Count remaining annotations
grep -r 'allow(dead_code)' src/ --include='*.rs' | grep -v test | wc -l
```

Target: Reduce from 209 to <50 before v1.0 release.
