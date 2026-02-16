# Streamline Cloud Provisioner Completion

## Summary

Complete the Streamline Cloud provisioner by replacing simulated K8s API calls with structured provisioning specifications, adding provisioning status tracking with error propagation, and hardening cloud API input validation.

## Current State

- Control plane CRUD: ✅ Complete (`src/cloud/control_plane.rs`)
- Billing/metering: ✅ Complete (`src/cloud/billing.rs`)
- Autoscaler: ✅ Complete (`src/cloud/autoscaler.rs`)
- Serverless endpoints: ✅ Complete (`src/cloud/serverless.rs`)
- Cloud API handlers: ✅ Complete (`src/server/cloud_api.rs`)
- Provisioner: ❌ DRY-RUN only (`src/cloud/provisioner.rs:37`)
  - `create_namespace()`: Stub at line 285
  - `create_secrets()`: Stub at line 291
  - `create_storage()`: Stub at line 297
  - `create_deployment()`: Stub at line 303
  - `create_services()`: Stub at line 309
  - `wait_for_ready()`: Simulated sleep at line 315
  - `delete_namespace()`: Stub at line 337

## Requirements

### P0: Provisioner Infrastructure Specs
| Requirement | Priority | Description |
|-------------|----------|-------------|
| K8s manifest generation | P0 | Generate actual K8s manifests (Namespace, Secret, PVC, StatefulSet, Service) |
| Provisioning spec output | P0 | Return structured provisioning specs that can be applied |
| Readiness check | P0 | Poll-based readiness check with timeout |

### P1: Status Tracking
| Requirement | Priority | Description |
|-------------|----------|-------------|
| Provisioning stages | P1 | Track progress through each provisioning stage |
| Error propagation | P1 | Surface provisioning errors to API callers |

### P1: API Validation
| Requirement | Priority | Description |
|-------------|----------|-------------|
| Input validation | P1 | Validate tenant names, cluster sizes, regions |
| Error responses | P1 | Return structured error responses |

## Implementation Tasks

1. **K8s provisioning specs**: Generate real K8s manifests in provisioner.rs
   - Files: `src/cloud/provisioner.rs`
   - Acceptance: Provisioner generates valid K8s YAML/JSON specs

2. **Status tracking**: Add provisioning stage tracking
   - Files: `src/cloud/provisioner.rs`, `src/cloud/mod.rs`
   - Acceptance: Provisioning status queryable at each stage

3. **API validation**: Add input validation to cloud API
   - Files: `src/server/cloud_api.rs`
   - Acceptance: Invalid inputs return 400 with clear error messages

## Labels

`enhancement`, `feature`, `cloud`, `size:medium`
