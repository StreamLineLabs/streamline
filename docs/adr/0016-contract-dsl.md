# ADR-0016: Contract DSL for Enforced Topic Contracts

## Status

Proposed (M4 P0 deliverable)

## Context

M4 (Enforced Contracts + Provenance) elevates schemas from advisory to
mandatory: producers that violate a contract are rejected at the broker.
Existing assertion code lives at `streamline/src/contracts/assertion.rs` but
lacks a user-facing authoring format.

We need a DSL that:

- Is editable by humans without a compiler.
- Compiles to a fast in-process validator (target: < 50 µs / record at p99
  for typical schemas).
- Versions cleanly so consumers can pin a contract version.
- Covers schema validation, semantic invariants (`amount > 0`,
  `currency in [...]`), and lineage assertions (`derived_from: <topic>`).

Options surveyed: JSON Schema, CUE, Rego/OPA, Avro+predicates, Protobuf+
options, Cedar.

## Decision

- **Authoring format:** YAML document with a defined top-level schema (the
  "Streamline Contract" schema, versioned).
- **Schema sub-language:** JSON Schema 2020-12 for shape validation
  (mature, well-known, decent tooling).
- **Predicates:** A small pure-expression language ("CEL-lite") for
  invariants (`record.amount > 0 && record.currency in ['USD','EUR']`).
  Deliberately not Turing-complete.
- **Compilation:** YAML → AST → bytecode evaluated by a sandboxed VM in
  `streamline/src/contracts/runner.rs`. No runtime code generation.
- **Lineage:** First-class `lineage:` block (parents + transform name);
  enforced when producers populate the standard lineage headers.

Example:

```yaml
contract: order-events
version: 3
status: enforced            # enforced | warn | bypass
schema:
  type: object
  required: [order_id, amount, currency]
  properties:
    order_id: { type: string, pattern: "^ord_" }
    amount:   { type: number }
    currency: { type: string, enum: [USD, EUR, GBP] }
invariants:
  - "record.amount > 0"
  - "record.currency in ['USD','EUR','GBP']"
lineage:
  parents: [orders.raw]
  transform: orders.normalize@v2
```

## Consequences

### Positive
- Reuses widely-known JSON Schema; lowers learning curve.
- Bytecode evaluator is auditable and benchmarkable.
- Lineage is a first-class field, enabling provenance graph generation
  (M4 P2).

### Negative
- Predicate language requires its own parser, lexer, type checker.
- Two sub-languages (JSON Schema + CEL-lite) — pedagogical surface to
  document.

### Neutral
- Contracts are themselves stored in `__contracts.<topic>` system topic and
  signed under M4 P1 (`streamline/src/security/attestation.rs`).

## References

- JSON Schema 2020-12: <https://json-schema.org/draft/2020-12/release-notes>
- CEL spec: <https://github.com/google/cel-spec>
- Existing: `streamline/src/contracts/assertion.rs`
- Internal: `MOONSHOT_PLAN.md` Feature M4
