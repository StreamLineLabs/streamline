# Add Distributed Tracing Support (OpenTelemetry)

## Priority: Nice to Have

## Summary

Add OpenTelemetry distributed tracing support to enable end-to-end request tracing across clients and server. Valuable for debugging latency issues and understanding request flow in production.

## Current State

- Structured logging via `tracing` crate
- No trace context propagation
- No span creation for operations
- No integration with tracing backends (Jaeger, Zipkin, etc.)

## Requirements

### Trace Points

1. **Connection lifecycle**
   - Span: `connection` (client IP, duration)

2. **Request handling**
   - Span: `kafka.produce` (topic, partition, message count)
   - Span: `kafka.fetch` (topic, partition, offset range)
   - Span: `kafka.metadata` (topics requested)

3. **Storage operations**
   - Span: `storage.write` (segment, bytes)
   - Span: `storage.read` (segment, offset, bytes)
   - Span: `storage.index_lookup` (offset, result)

4. **Replication** (cluster mode)
   - Span: `replication.send` (follower, bytes)
   - Span: `replication.fetch` (leader, offset)

### Configuration

```bash
--tracing-enabled=true
--tracing-endpoint=http://localhost:4317  # OTLP endpoint
--tracing-service-name=streamline
--tracing-sample-rate=0.1  # 10% sampling
```

### Implementation

```rust
// Using tracing-opentelemetry
use tracing_opentelemetry::OpenTelemetryLayer;
use opentelemetry::sdk::trace::Tracer;

fn init_tracing(config: &TracingConfig) -> Result<()> {
    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(opentelemetry_otlp::new_exporter().tonic())
        .install_batch(opentelemetry::runtime::Tokio)?;

    let telemetry = OpenTelemetryLayer::new(tracer);

    tracing_subscriber::registry()
        .with(telemetry)
        .with(tracing_subscriber::fmt::layer())
        .init();

    Ok(())
}
```

### Trace Context Propagation

Support W3C Trace Context headers for Kafka clients that propagate context:
- Extract trace ID from record headers
- Propagate context to downstream operations

### Dependencies to Add

```toml
opentelemetry = "0.21"
opentelemetry-otlp = "0.14"
tracing-opentelemetry = "0.22"
```

### Implementation Tasks

1. Add OpenTelemetry dependencies
2. Create tracing initialization module
3. Add spans to request handlers
4. Add spans to storage operations
5. Add spans to replication (cluster mode)
6. Support trace context extraction from records
7. Add configuration options
8. Document tracing setup
9. Provide example Jaeger docker-compose

### Acceptance Criteria

- [ ] Traces exported to OTLP endpoint
- [ ] Request spans show full lifecycle
- [ ] Storage operation spans nested correctly
- [ ] Sampling rate configurable
- [ ] Minimal overhead when disabled
- [ ] Documentation with Jaeger example

## Related Files

- `src/main.rs` - Tracing initialization
- `src/protocol/kafka.rs` - Request handling
- `src/storage/` - Storage operations
- `Cargo.toml` - Dependencies

## Labels

`nice-to-have`, `observability`, `production-readiness`
