# Kubernetes Deployment for Streamline Benchmarks

Deploy the benchmark suite to Kubernetes for production-like testing.

## Prerequisites

- Kubernetes cluster (1.24+)
- kubectl configured
- (Optional) Helm for monitoring stack

## Quick Start

```bash
# Deploy all systems
kubectl apply -k k8s/

# Wait for pods to be ready
kubectl wait --for=condition=ready pod -l app.kubernetes.io/part-of=streamline-benchmark \
  -n streamline-benchmark --timeout=120s

# Check status
kubectl get pods -n streamline-benchmark

# Run benchmark
kubectl apply -f k8s/benchmark-job.yaml

# Watch benchmark progress
kubectl logs -f job/benchmark -n streamline-benchmark
```

## Deployed Components

| Component | Port | Description |
|-----------|------|-------------|
| Streamline | 9092, 9094 | Kafka protocol + HTTP API |
| Kafka | 9092 | Apache Kafka (KRaft mode) |
| Redpanda | 9092, 9644 | Kafka-compatible streaming |

## Resource Limits

All systems are configured with identical limits for fair comparison:

| Resource | Request | Limit |
|----------|---------|-------|
| CPU | 1 core | 2 cores |
| Memory | 512Mi | 2Gi |

## Customization

### Adjust Resources

Edit `kustomization.yaml` to add patches:

```yaml
patches:
  - patch: |-
      - op: replace
        path: /spec/template/spec/containers/0/resources/limits/cpu
        value: "4"
    target:
      kind: Deployment
      name: streamline
```

### Change Workload

Edit `benchmark-job.yaml` environment variables:

```yaml
env:
  - name: DRIVER
    value: "driver-streamline/streamline.yaml"
  - name: WORKLOAD
    value: "streamline-comparison"  # Change workload
```

### Run Multiple Benchmarks

```bash
# Run against Streamline
kubectl create job benchmark-streamline --from=job/benchmark \
  -n streamline-benchmark

# Run against Kafka
kubectl create job benchmark-kafka --from=job/benchmark \
  -n streamline-benchmark \
  -- env DRIVER=driver-kafka/kafka.yaml
```

## Cleanup

```bash
# Remove benchmark job
kubectl delete job benchmark -n streamline-benchmark

# Remove all resources
kubectl delete -k k8s/
```

## Production Considerations

For production benchmarks:

1. **Use PersistentVolumes** - Replace `emptyDir` with PVCs for durability
2. **Resource Isolation** - Use dedicated node pools with taints/tolerations
3. **Network Policies** - Restrict traffic between benchmark components
4. **Monitoring** - Deploy Prometheus/Grafana for real-time metrics

### Example with PersistentVolume

```yaml
# Add to streamline.yaml
volumes:
  - name: data
    persistentVolumeClaim:
      claimName: streamline-data
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: streamline-data
spec:
  accessModes: ["ReadWriteOnce"]
  resources:
    requests:
      storage: 10Gi
```
