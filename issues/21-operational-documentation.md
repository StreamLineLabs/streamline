# Create Operational Documentation

## Priority: Medium

## Summary

Create comprehensive operational documentation including runbooks, troubleshooting guides, and performance tuning recommendations. Essential for production deployments.

## Current State

- README.md covers installation and basic usage
- TLS_GUIDE.md covers security setup
- CONTRIBUTING.md covers development
- No operational runbook
- No troubleshooting guide
- No performance tuning guide
- No monitoring/alerting guide

## Requirements

### Documents to Create

#### 1. Operations Runbook (`docs/OPERATIONS.md`)

**Sections:**
- Deployment checklist
- Configuration reference (all options with descriptions)
- Starting/stopping procedures
- Log file locations and rotation
- Backup and restore procedures
- Upgrade procedures
- Rollback procedures

#### 2. Troubleshooting Guide (`docs/TROUBLESHOOTING.md`)

**Common Issues:**
- Server won't start
  - Port already in use
  - Permission denied on data directory
  - TLS certificate issues
- Client connection failures
  - Authentication errors
  - TLS handshake failures
  - Network connectivity
- Performance issues
  - High latency causes
  - Low throughput causes
  - Memory usage growing
- Cluster issues
  - Node not joining cluster
  - Leader election problems
  - Replication lag

**Diagnostic Commands:**
```bash
# Check server health
curl http://localhost:8080/health

# View metrics
curl http://localhost:8080/metrics | grep streamline_

# Check logs
journalctl -u streamline -f

# Test connectivity
streamline-cli topics list
```

#### 3. Performance Tuning Guide (`docs/PERFORMANCE.md`)

**Topics:**
- Hardware sizing recommendations
- Configuration tuning
  - WAL sync mode tradeoffs
  - Segment size optimization
  - Connection limits
- OS tuning
  - File descriptor limits
  - TCP settings
  - Disk I/O scheduler
- JVM client tuning (for Kafka clients)
- Benchmark results and methodology

#### 4. Monitoring Guide (`docs/MONITORING.md`)

**Topics:**
- Prometheus metrics reference
- Recommended alert rules
- Grafana dashboard examples
- Key metrics to watch
- Capacity planning metrics

**Example Alert Rules:**
```yaml
groups:
  - name: streamline
    rules:
      - alert: StreamlineDown
        expr: up{job="streamline"} == 0
        for: 1m
      - alert: HighProduceLatency
        expr: histogram_quantile(0.99, streamline_produce_latency_seconds) > 0.1
        for: 5m
      - alert: ReplicationLag
        expr: streamline_replication_lag_messages > 1000
        for: 5m
```

### Implementation Tasks

1. Create `docs/` directory
2. Write OPERATIONS.md with deployment procedures
3. Write TROUBLESHOOTING.md with common issues
4. Write PERFORMANCE.md with tuning guidance
5. Write MONITORING.md with metrics reference
6. Create example Grafana dashboard JSON
7. Create example Prometheus alert rules
8. Update README to link to docs
9. Review with someone unfamiliar with project

### Content Guidelines

- Use clear, concise language
- Include copy-paste ready commands
- Show expected output where helpful
- Include "why" not just "what"
- Keep up to date with code changes

### Acceptance Criteria

- [ ] Operations runbook complete
- [ ] Troubleshooting guide covers common issues
- [ ] Performance tuning guide with recommendations
- [ ] Monitoring guide with metrics reference
- [ ] Example Grafana dashboard provided
- [ ] Example alert rules provided
- [ ] Documentation reviewed for accuracy

## Related Files

- `README.md` - Main documentation
- `TLS_GUIDE.md` - TLS setup guide
- `src/metrics/mod.rs` - Metrics definitions

## Labels

`medium-priority`, `documentation`, `production-readiness`
