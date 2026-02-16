# Architecture Decision Records

This directory contains Architecture Decision Records (ADRs) for the Streamline project.

ADRs document significant architectural decisions along with their context and consequences. They provide a historical record of why certain technical choices were made.

## Format

Each ADR follows this template:

- **Title**: Short descriptive name
- **Status**: Proposed, Accepted, Deprecated, Superseded
- **Context**: The situation and forces at play
- **Decision**: What we decided to do
- **Consequences**: The resulting impact, both positive and negative

## Index

| ADR | Title | Status |
|-----|-------|--------|
| [ADR-0001](0001-kafka-protocol-compatibility.md) | Kafka Protocol Compatibility | Accepted |
| [ADR-0002](0002-single-binary-architecture.md) | Single Binary Architecture | Accepted |

## Creating a New ADR

1. Copy `template.md` to a new file: `NNNN-short-title.md`
2. Fill in all sections
3. Submit a PR for review
4. Update this index
