# ADR-0004: Schema Registry Enhancements

## Status

Proposed

## Context

Streamline includes a basic schema registry behind the `schema-registry` feature flag. The `src/schema/` module provides Avro, Protobuf, and JSON Schema support with compatibility checking (`CompatibilityChecker`), caching (`SchemaCache` with TTL), and storage via an internal `_schemas` topic (`SchemaStore`).

The current implementation has gaps that block production adoption:

- The REST API is incomplete — missing schema deletion, mode endpoints, and reverse-lookup by ID
- Per-subject compatibility overrides are defined in `CompatibilityLevel` but not fully persisted through `SchemaStore`
- `SchemaReference` exists in the data model but reference resolution is not implemented
- `SchemaRegistryConfig.validate_on_produce` is defined but the produce path does not invoke validation
- No CLI tooling exists for schema operations

The Confluent Schema Registry REST API is the de facto standard used by all major Kafka client serializers. Compatibility with this API is required for SDK interoperability.

## Decision

Enhance the built-in schema registry to achieve Confluent Schema Registry API compatibility while adding Streamline-native features, delivered in five phases:

**Phase 1 — Confluent REST API Compatibility**: Complete the REST API surface to match Confluent Schema Registry v7.x. Add missing endpoints: `DELETE /subjects/{subject}`, `DELETE /subjects/{subject}/versions/{version}`, `GET /schemas/ids/{id}/subjects`, `GET /schemas/ids/{id}/versions`, `GET/PUT /mode`, and `DELETE /config/{subject}`. Return Confluent-compatible JSON error codes.

**Phase 2 — Schema Evolution Policies**: Wire per-subject compatibility levels through the storage layer. Persist overrides to the `_schemas` topic. Support all seven levels (NONE, BACKWARD, FORWARD, FULL, and transitive variants). Add registry modes (READWRITE, READONLY, IMPORT).

**Phase 3 — Schema References**: Implement recursive reference resolution building on the existing `SchemaReference` type. Support Avro named types, Protobuf imports, and JSON Schema `$ref`. Prevent deletion of referenced schemas. Cache resolved reference graphs.

**Phase 4 — Server-Side Validation on Produce**: Activate `validate_on_produce` in the Kafka protocol produce path. Extract schema ID from the Confluent wire format header (magic byte `0x00` + 4-byte ID). Validate payloads via `CompatibilityChecker::validate_data`. Return Kafka error `INVALID_RECORD` (87) on failure. Target < 50µs overhead with cached schemas.

**Phase 5 — CLI Tooling**: Add `streamline schema` subcommands: `list`, `get`, `versions`, `register`, `test` (dry-run compatibility), `export`, `import`, `migrate` (from external registry), and `config get/set`.

### Key Design Choices

**Storage in `_schemas` topic** rather than a separate database — maintains zero external dependencies and inherits the same replication/durability guarantees as user data. Uses log compaction to retain only the latest version per key.

**Confluent wire format preserved** — magic byte + 4-byte schema ID prefix in message payloads. All existing Kafka serializers work without modification.

**Feature-gated** — all changes remain behind the `schema-registry` feature flag. No impact on the lite edition binary.

## Consequences

### Positive

- Drop-in replacement for Confluent Schema Registry — existing clients work without changes
- Zero-config experience: enable with `schema_registry.enabled = true`, no separate process
- Server-side validation prevents schema-incompatible messages from reaching topics
- CLI tooling simplifies operational workflows and cross-registry migration
- All Streamline SDKs can use standard Kafka serializers

### Negative

- Schema validation libraries add ~2MB to the full edition binary
- Server-side validation adds per-message latency on the produce path
- Maintaining Confluent API compatibility requires tracking upstream changes
- Feature flag complexity increases (schema-registry feature interactions)

### Neutral

- All changes are additive; existing `_schemas` topic data remains valid
- Schema registry remains opt-in via compile-time feature flag
- CLI `migrate` command provides a clear migration path from external registries
