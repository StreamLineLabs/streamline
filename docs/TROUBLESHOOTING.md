# Troubleshooting

Common issues and solutions for Streamline.

## Connection Issues

### `Connection refused` on port 9092

**Cause**: Server not running or listening on a different address.

```bash
# Check if streamline is running
ps aux | grep streamline

# Start with explicit address
./streamline --listen-addr 0.0.0.0:9092

# Check if port is in use
lsof -i :9092
```

### `Connection refused` on port 9094 (HTTP API)

**Cause**: HTTP API binds to a different port or address.

```bash
# Start with explicit HTTP address
./streamline --listen-addr 0.0.0.0:9092 --http-addr 0.0.0.0:9094

# Verify health
curl http://localhost:9094/health
```

### Kafka client `UNKNOWN_TOPIC_OR_PARTITION`

**Cause**: Topic doesn't exist and auto-creation may be disabled.

```bash
# Create topic explicitly
streamline-cli topics create my-topic --partitions 3

# Or enable auto-creation
./streamline --auto-create-topics
```

## Startup Issues

### `Address already in use`

**Cause**: Another process (Kafka, another Streamline, etc.) is using port 9092.

```bash
# Find what's using the port
lsof -i :9092

# Use a different port
./streamline --listen-addr 0.0.0.0:19092
```

### `Permission denied` on data directory

**Cause**: Streamline can't write to the data directory.

```bash
# Check permissions
ls -la ./data

# Use a different directory
./streamline --data-dir /tmp/streamline-data

# Or run in-memory mode (no disk needed)
./streamline --in-memory
```

### Server starts but clients can't connect

**Cause**: Usually a hostname/advertised listener mismatch in Docker or K8s.

```bash
# In Docker, use host network or map ports:
docker run -p 9092:9092 -p 9094:9094 ghcr.io/streamlinelabs/streamline:latest

# Verify the server is accessible:
kafkacat -b localhost:9092 -L
```

## Performance Issues

### High produce latency

**Possible causes**:
1. **Disk I/O**: Switch to in-memory mode for testing: `--in-memory`
2. **Compression**: Disable if CPU-bound: `--default-compression none`
3. **WAL sync**: Reduce sync frequency for throughput: check storage config
4. **Batch size**: Clients should batch messages (most Kafka clients do this by default)

### High memory usage

```bash
# Check current memory via HTTP API
curl http://localhost:9094/info | python3 -m json.tool

# Limit segment cache
# Set storage.cache.max_size in config or reduce partition count
```

### Slow consumer groups

**Cause**: Consumer group rebalancing or offset commit overhead.

```bash
# Check consumer group status
streamline-cli groups describe my-group

# Check consumer lag
streamline-cli groups lag my-group
```

## Data Issues

### Messages appear to be lost

1. **Check retention**: Messages may have been deleted by retention policy
   ```bash
   streamline-cli topics describe my-topic
   # Look at retention_ms and retention_bytes
   ```

2. **Check offset**: Consumer may be reading from wrong offset
   ```bash
   streamline-cli consume my-topic --from-beginning
   ```

3. **In-memory mode**: Data is lost on restart when using `--in-memory`

### Corrupt segment file

```bash
# Run diagnostics
streamline-cli doctor --data-dir ./data

# Check specific topic
streamline-cli doctor --topic my-topic
```

## Docker / Kubernetes Issues

### Container exits immediately

```bash
# Check logs
docker logs streamline

# Common fix: ensure ports are mapped
docker run -p 9092:9092 -p 9094:9094 \
  -e STREAMLINE_LISTEN_ADDR=0.0.0.0:9092 \
  ghcr.io/streamlinelabs/streamline:latest
```

### Kubernetes pod CrashLoopBackOff

```bash
# Check events
kubectl describe pod streamline-0

# Check logs
kubectl logs streamline-0

# Common fix: ensure PVC is provisioned
kubectl get pvc
```

## TLS Issues

### `SSL handshake failed`

```bash
# Verify cert and key files exist and are readable
ls -la server.crt server.key

# Test TLS connection
openssl s_client -connect localhost:9092

# Start with TLS
./streamline --tls-enabled --tls-cert server.crt --tls-key server.key
```

## Getting Help

- **GitHub Issues**: https://github.com/streamlinelabs/streamline/issues
- **Discussions**: https://github.com/streamlinelabs/streamline/discussions
- **Discord**: https://discord.gg/streamlinelabs
- **Security**: security@streamline.dev (see [SECURITY.md](../SECURITY.md))

## Diagnostic Commands

```bash
# Server health
curl http://localhost:9094/health

# Server info (version, uptime, config)
curl http://localhost:9094/info

# Metrics (Prometheus format)
curl http://localhost:9094/metrics

# List all topics
streamline-cli topics list

# Full diagnostics
streamline-cli doctor
```
