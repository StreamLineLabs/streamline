# Code Quality & Architecture Refactor

## Summary
Refactor core modules to eliminate monolithic files, enforce clearer boundaries, and improve testability by replacing giant match blocks, splitting CLI/TopicManager responsibilities, and separating wiring from side effects. Breaking public API changes are allowed for this remediation effort.

## Current State
- `src/protocol/kafka.rs` is ~19,899 lines with a giant dispatch `match` and mixed concerns (`src/protocol/kafka.rs:2310-2565`).
- `src/cli.rs` is ~9,708 lines combining parsing, execution, output, history, and error UX (`src/cli.rs:157-230`, `2131-2205`).
- `TopicManager` owns WAL/checkpoint/segment config and topic lifecycle (`src/storage/topic.rs:1488-1750`).
- `build_http_router` spawns background tasks while building routes (`src/server/http.rs:141-197`).
- Entry points and helpers call `std::process::exit` (`src/main.rs:90-161`, `src/cli_cluster.rs:18-22`).
- `lib.rs` exports a broad surface area (`src/lib.rs:150-176`).
- Error variants are stringly typed for core domains (`src/error.rs:268-338`).

## Requirements

### Module Boundaries
| Requirement | Priority | Description |
|-------------|----------|-------------|
| R1 | P0 | Split `src/protocol/kafka.rs` into framing + per-API handler modules with a registry. |
| R2 | P0 | Split `src/cli.rs` into submodules with a thin `main` wrapper. |
| R3 | P0 | Decompose `TopicManager` into focused components (topic lifecycle, WAL, checkpoints, segment config). |
| R4 | P1 | Separate HTTP router wiring from background task bootstrap. |
| R5 | P1 | Reduce public API surface by making internal modules `pub(crate)` and curating re-exports. |
| R6 | P1 | Replace stringly typed error variants with domain enums. |
| R7 | P1 | Replace `std::process::exit` in helpers with `Result` propagation. |

### Data Structures
```rust
// Protocol dispatch registry
pub struct ApiHandlerRegistry { /* ApiKey -> handler */ }

// CLI modularization
pub struct CliApp { /* parse + run */ }

// Storage components
pub struct TopicCatalog { /* topic map + lifecycle */ }
pub struct WalManager { /* WAL wiring */ }
pub struct CheckpointService { /* checkpointing */ }

// HTTP bootstrap
pub struct HttpBootstrap { /* background tasks */ }

// Structured errors
pub enum StorageError { /* ... */ }
pub enum ProtocolError { /* ... */ }
```

### Implementation Tasks
1. **Protocol handler registry**
   - Objective: Replace `dispatch_request` match with registry and per-API modules.
   - Files: `src/protocol/kafka.rs`, `src/protocol/handlers/*`.
   - Acceptance: Dispatch is table-driven; per-API handlers are isolated; `kafka.rs` shrinks.

2. **CLI modularization**
   - Objective: Split parsing/commands/output/error UX/history into `src/cli/*`.
   - Files: `src/cli.rs`, `src/cli/*`, `src/cli_cluster.rs`.
   - Acceptance: `src/cli.rs` is a thin wrapper; behavior preserved.

3. **TopicManager decomposition**
   - Objective: Extract WAL/checkpoint/segment config responsibilities.
   - Files: `src/storage/topic.rs`, `src/storage/*`.
   - Acceptance: Topic lifecycle is isolated; storage components are unit-testable.

4. **HTTP bootstrap separation**
   - Objective: Move background tasks out of router construction; use typed addresses.
   - Files: `src/server/http.rs`, `src/server/mod.rs`.
   - Acceptance: Router build is pure; bootstrap owns side effects.

5. **Exit removal**
   - Objective: Replace `std::process::exit` in helpers with `Result` propagation.
   - Files: `src/main.rs`, `src/cli_cluster.rs`, `src/cli.rs`.
   - Acceptance: Entrypoints handle exits; helpers return errors.

6. **Public API curation**
   - Objective: Reduce `lib.rs` exports and make internal modules private.
   - Files: `src/lib.rs`.
   - Acceptance: Only intended stable surfaces are `pub`; breaking changes accepted.

7. **Structured error domains**
   - Objective: Introduce domain enums and update call sites.
   - Files: `src/error.rs`, dependent modules.
   - Acceptance: Core errors are structured (no stringly typed domains).

8. **Docs and tests**
   - Objective: Update architecture docs and add tests for registry + storage + CLI refactors.
   - Files: `docs/ARCHITECTURE.md`, `docs/MODULES.md`, `tests/*`.
   - Acceptance: Docs reflect new boundaries; tests pass.

## Acceptance Criteria
- [ ] `src/protocol/kafka.rs` and `src/cli.rs` reduced to thin orchestration modules.
- [ ] Protocol dispatch uses a registry (no mega-match).
- [ ] TopicManager responsibilities are split into dedicated components.
- [ ] HTTP router construction has no side effects.
- [ ] No helper uses `std::process::exit`.
- [ ] Public API surface is curated; internal modules are private.
- [ ] Structured error enums replace stringly typed domain errors.
- [ ] Docs updated and all tests pass.

## Related Files
- `src/protocol/kafka.rs` - Protocol framing and dispatch.
- `src/protocol/handlers/*` - API handlers.
- `src/cli.rs` - CLI entrypoint.
- `src/storage/topic.rs` - Topic lifecycle and storage wiring.
- `src/server/http.rs` - HTTP routing.
- `src/main.rs` - Server entrypoint.
- `src/lib.rs` - Public API surface.
- `src/error.rs` - Error types.

## Labels

`code-quality`, `refactor`, `architecture`, `large`
