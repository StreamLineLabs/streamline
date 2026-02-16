# Implement Metrics and Monitoring

## Summary

Add comprehensive metrics collection and exposure for production monitoring. Currently, only logging exists with no quantitative metrics.

## Current State

- `src/server/mod.rs:124` - HTTP API port defined but not implemented ("planned")
- No metrics collected anywhere in codebase
- No health check endpoint
- Logging via `tracing` but no metrics

## Requirements

### Metrics to Collect

**Server Metrics:**
| Metric | Type | Description |
|--------|------|-------------|
| `streamline_connections_active` | Gauge | Current open connections |
| `streamline_connections_total` | Counter | Total connections accepted |
| `streamline_uptime_seconds` | Gauge | Server uptime |

**Request Metrics:**
| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `streamline_requests_total` | Counter | api_key, error | Requests by API type |
| `streamline_request_duration_seconds` | Histogram | api_key | Request latency |
| `streamline_request_size_bytes` | Histogram | api_key | Request payload size |
| `streamline_response_size_bytes` | Histogram | api_key | Response payload size |

**Storage Metrics:**
| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `streamline_topics_total` | Gauge | - | Number of topics |
| `streamline_partitions_total` | Gauge | topic | Partitions per topic |
| `streamline_records_total` | Counter | topic, partition | Records written |
| `streamline_bytes_in_total` | Counter | topic | Bytes received |
| `streamline_bytes_out_total` | Counter | topic | Bytes sent |
| `streamline_log_size_bytes` | Gauge | topic, partition | Partition size |
| `streamline_log_end_offset` | Gauge | topic, partition | Latest offset |
| `streamline_segments_total` | Gauge | topic, partition | Segment count |

**Consumer Group Metrics (when implemented):**
| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `streamline_consumer_groups_total` | Gauge | - | Active groups |
| `streamline_consumer_lag` | Gauge | group, topic, partition | Consumer lag |

### Endpoints

Implement HTTP API at configured `http_addr` (default `:9094`):

```
GET /metrics          # Prometheus format metrics
GET /health           # Health check (200 OK / 503 Unhealthy)
GET /health/live      # Liveness probe
GET /health/ready     # Readiness probe
GET /info             # Server info JSON
```

### Implementation Tasks

1. Add dependencies to `Cargo.toml`:
   ```toml
   metrics = "0.21"
   metrics-exporter-prometheus = "0.12"
   axum = "0.7"  # or hyper for HTTP
   ```

2. Create `src/metrics/mod.rs`:
   ```rust
   pub fn init_metrics() -> PrometheusHandle;
   pub fn record_request(api_key: i16, duration: Duration, error: bool);
   pub fn record_bytes_in(topic: &str, bytes: u64);
   // etc.
   ```

3. Create `src/server/http.rs`:
   - HTTP server using axum/hyper
   - `/metrics` endpoint returning Prometheus format
   - `/health` endpoints

4. Instrument `src/protocol/kafka.rs`:
   - Add timing around `process_message()`
   - Record request/response sizes
   - Count by API key

5. Instrument `src/storage/`:
   - Record writes/reads
   - Track partition sizes
   - Count segments

6. Update `src/server/mod.rs`:
   - Start HTTP server alongside Kafka listener
   - Initialize metrics on startup

### Health Check Logic

```rust
fn health_check() -> HealthStatus {
    HealthStatus {
        status: if all_checks_pass { "healthy" } else { "unhealthy" },
        checks: vec![
            Check { name: "storage", status: storage_accessible() },
            Check { name: "listener", status: listener_bound() },
        ]
    }
}
```

### Grafana Dashboard

Provide example Grafana dashboard JSON in `examples/grafana-dashboard.json`.

### Acceptance Criteria

- [ ] `/metrics` returns Prometheus format
- [ ] `/health` returns appropriate status
- [ ] Request latency tracked per API type
- [ ] Bytes in/out tracked per topic
- [ ] Partition sizes exposed
- [ ] Docker health check uses `/health/live`
- [ ] Example Grafana dashboard provided
- [ ] Minimal performance impact (<1% overhead)

### Example Metrics Output

```
# HELP streamline_requests_total Total requests by API type
# TYPE streamline_requests_total counter
streamline_requests_total{api_key="produce",error="false"} 1523
streamline_requests_total{api_key="fetch",error="false"} 8291

# HELP streamline_request_duration_seconds Request latency
# TYPE streamline_request_duration_seconds histogram
streamline_request_duration_seconds_bucket{api_key="produce",le="0.001"} 1200
streamline_request_duration_seconds_bucket{api_key="produce",le="0.01"} 1500
```

### Related Files

- `src/server/mod.rs:124` - HTTP addr already defined
- `src/protocol/kafka.rs:87-215` - Instrument request handling
- `src/storage/partition.rs` - Instrument writes
- `Dockerfile` - Update health check to use HTTP

## Labels

`enhancement`, `observability`, `production-readiness`
