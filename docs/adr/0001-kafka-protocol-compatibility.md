# ADR-0001: Kafka Protocol Compatibility

## Status

Accepted

## Context

Streamline aims to be a lightweight, developer-friendly streaming platform. The streaming ecosystem is dominated by Apache Kafka, which has become the de facto standard with a massive ecosystem of clients, tools, and integrations across every major programming language.

New streaming platforms face a critical adoption barrier: without client library support, developers cannot use the platform regardless of its technical merits. Building and maintaining client libraries for every language is a multi-year, multi-team effort.

We needed to decide whether to:

1. Design a custom wire protocol and build client libraries from scratch
2. Implement compatibility with an existing protocol (Kafka, AMQP, MQTT, etc.)
3. Provide only a REST/HTTP API

## Decision

We implement the Apache Kafka wire protocol as our primary interface, targeting production-level Kafka 3.x compatibility (50+ APIs).

This means any existing Kafka client library (Java, Python, Go, Node.js, Rust, .NET, C/C++, etc.) can connect to Streamline without code changes. Users can adopt Streamline by changing only a broker address.

We also expose an HTTP API on a separate port (9094) for management, health checks, and metrics — functionality that doesn't need Kafka protocol compatibility.

## Consequences

### Positive

- **Instant ecosystem access**: Every Kafka client, tool, and integration works with Streamline out of the box
- **Zero migration friction**: Existing Kafka users can evaluate Streamline by changing one connection string
- **SDK development is additive**: Our language-specific SDKs add Streamline-native features on top of standard Kafka client libraries rather than replacing them
- **Battle-tested protocol**: The Kafka wire protocol is well-documented, widely understood, and thoroughly tested by the industry

### Negative

- **Protocol complexity**: Implementing 50+ Kafka APIs with their version ranges is a significant engineering investment
- **Compatibility constraints**: Some internal design decisions are constrained by Kafka protocol semantics (e.g., partition-based ordering, offset model)
- **Version tracking burden**: As Kafka evolves its protocol, we must track and implement new API versions to maintain compatibility
- **Feature subset confusion**: Users may expect full Kafka feature parity; we must clearly document what is and isn't supported

### Neutral

- Our SDKs wrap standard Kafka client libraries (aiokafka, Sarama, kafkajs, etc.) and add Streamline-specific features — this is both a benefit (less code) and a constraint (we inherit their limitations)
