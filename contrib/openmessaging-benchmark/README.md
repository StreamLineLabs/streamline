# Streamline OpenMessaging Benchmark

This directory contains templates for integrating Streamline into the [OpenMessaging Benchmark](https://openmessaging.cloud/docs/benchmarks/) framework.

## Setup Instructions

### 1. Fork the OpenMessaging Benchmark

Fork [Redpanda's OMB fork](https://github.com/redpanda-data/openmessaging-benchmark) to your GitHub account:

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/openmessaging-benchmark.git
cd openmessaging-benchmark
```

### 2. Add the Streamline Driver

Copy the driver-streamline directory from this template:

```bash
# From the streamline repo
cp -r contrib/openmessaging-benchmark/driver-streamline /path/to/openmessaging-benchmark/

# Add to parent pom.xml modules list
# In openmessaging-benchmark/pom.xml, add to <modules>:
#   <module>driver-streamline</module>
```

### 3. Add Workloads

Copy the Streamline-specific workloads:

```bash
cp contrib/openmessaging-benchmark/workloads/*.yaml /path/to/openmessaging-benchmark/workloads/
```

### 4. Build

```bash
cd /path/to/openmessaging-benchmark
mvn clean install -DskipTests
```

## Quick Start with Makefile

```bash
# Start systems
make up                    # Start Streamline + Kafka
make up-all                # Start all messaging systems
make up-monitoring         # Start with Prometheus + Grafana

# Run benchmarks
make benchmark-quick       # Quick validation (~1 min)
make benchmark             # Default comparison
make benchmark-all         # All systems comparison

# Generate reports
make report                # Markdown report
make report-html           # HTML report with tables
make report-charts         # Generate comparison charts

# Cleanup
make clean                 # Stop and remove containers
```

## Running Benchmarks

### Using the Run Script

```bash
# Full orchestration with automatic startup and reporting
./scripts/run-benchmarks.sh --systems streamline,kafka --workload comparison

# All systems with monitoring
./scripts/run-benchmarks.sh --all --monitoring --workload ecommerce

# See all options
./scripts/run-benchmarks.sh --help
```

### Local Testing with Docker

Start all systems:

```bash
docker-compose -f docker-compose.benchmarks.yml up -d
```

With monitoring (Prometheus + Grafana):

```bash
docker-compose -f docker-compose.benchmarks.yml --profile monitoring up -d

# Open Grafana at http://localhost:3000 (admin/admin)
# Pre-configured dashboard: "Benchmark Overview"
```

Run a quick benchmark:

```bash
bin/benchmark \
    --drivers driver-streamline/streamline.yaml \
    --workloads workloads/streamline-quick.yaml
```

### Comparison Benchmark

Compare Streamline against Kafka:

```bash
bin/benchmark \
    --drivers driver-streamline/streamline.yaml,driver-kafka/kafka.yaml \
    --workloads workloads/streamline-comparison.yaml
```

### View Results

Results are saved to the current directory as JSON files:

```bash
ls *.json
cat streamline-*.json | jq .
```

### Process Results

Use the included results processor to generate comparison reports:

```bash
# Install dependencies (optional, for charts)
pip install -r scripts/requirements.txt

# Generate markdown report
python scripts/process_results.py results/*.json --output report.md

# Generate HTML report with tables
python scripts/process_results.py results/*.json --format html --output report.html

# Generate CSV for spreadsheets
python scripts/process_results.py results/*.json --format csv --output results.csv

# Generate comparison charts (requires matplotlib)
python scripts/process_results.py results/*.json --chart charts/benchmark

# Generate latency heatmap
python scripts/process_results.py results/*.json --heatmap charts/heatmap

# Include resource efficiency metrics
python scripts/process_results.py results/*.json --efficiency --cpu-cores 2 --memory-gb 2
```

### Kubernetes Deployment

Deploy to Kubernetes for production-like benchmarks:

```bash
# Deploy all systems
kubectl apply -k k8s/

# Wait for ready
kubectl wait --for=condition=ready pod -l app.kubernetes.io/part-of=streamline-benchmark \
  -n streamline-benchmark --timeout=120s

# Run benchmark job
kubectl apply -f k8s/benchmark-job.yaml

# Watch logs
kubectl logs -f job/benchmark -n streamline-benchmark
```

See `k8s/README.md` for more details.

## Workload Configurations

### Synthetic Workloads

| Workload | Duration | Use Case |
|----------|----------|----------|
| `streamline-quick.yaml` | ~1 min | Quick validation |
| `streamline-comparison.yaml` | ~10 min | Full comparison |
| `streamline-throughput.yaml` | ~30 min | Max throughput test |
| `streamline-latency.yaml` | ~10 min | Latency-focused test |
| `streamline-e2e.yaml` | ~15 min | End-to-end latency |

### Real-World Workloads

| Workload | Duration | Use Case |
|----------|----------|----------|
| `workload-ecommerce.yaml` | ~12 min | E-commerce events (orders, carts) |
| `workload-iot.yaml` | ~6 min | IoT sensor telemetry (high volume) |
| `workload-logging.yaml` | ~12 min | Log aggregation pipeline |
| `workload-trading.yaml` | ~6 min | Financial trading (ultra-low latency) |

## Metrics Collected

- **Throughput**: messages/sec, MB/sec
- **Latency**: P50, P75, P95, P99, P99.9, P99.99
- **End-to-End**: Publish-to-consume latency
- **Resource Efficiency**: msgs/CPU-core, msgs/GB-memory, MB/s per CPU

## Directory Structure

```
contrib/openmessaging-benchmark/
├── Makefile                         # Common operations
├── docker-compose.benchmarks.yml    # All systems with resource limits
├── driver-streamline/
│   ├── pom.xml                      # Maven configuration
│   ├── streamline.yaml              # Default driver config
│   ├── streamline-throughput.yaml   # Throughput-optimized config
│   ├── streamline-latency.yaml      # Latency-optimized config
│   └── src/main/java/.../
│       ├── StreamlineBenchmarkDriver.java
│       ├── StreamlineBenchmarkProducer.java
│       ├── StreamlineBenchmarkConsumer.java
│       └── StreamlineConfig.java
├── workloads/
│   ├── streamline-quick.yaml        # Quick validation (~1 min)
│   ├── streamline-comparison.yaml   # Full comparison (~10 min)
│   ├── streamline-throughput.yaml   # Max throughput test
│   ├── streamline-latency.yaml      # Latency-focused test
│   ├── streamline-e2e.yaml          # End-to-end latency
│   ├── workload-ecommerce.yaml      # E-commerce simulation
│   ├── workload-iot.yaml            # IoT telemetry simulation
│   ├── workload-logging.yaml        # Log aggregation simulation
│   └── workload-trading.yaml        # Financial trading simulation
├── monitoring/
│   ├── prometheus.yml               # Prometheus scrape config
│   └── grafana/
│       ├── provisioning/            # Auto-provisioned datasources
│       └── dashboards/              # Pre-built dashboards
├── scripts/
│   ├── run-benchmarks.sh            # Benchmark orchestration
│   ├── process_results.py           # Results processor
│   └── requirements.txt             # Python dependencies
├── k8s/
│   ├── kustomization.yaml           # Kustomize config
│   ├── namespace.yaml               # Benchmark namespace
│   ├── streamline.yaml              # Streamline deployment
│   ├── kafka.yaml                   # Kafka deployment
│   ├── redpanda.yaml                # Redpanda deployment
│   ├── benchmark-job.yaml           # Benchmark runner job
│   └── README.md                    # K8s deployment guide
└── .github/workflows/
    ├── benchmark.yml                # CI automation
    └── regression.yml               # Performance regression detection
```

## Resources

- [OpenMessaging Benchmark Documentation](https://openmessaging.cloud/docs/benchmarks/)
- [Streamline GitHub](https://github.com/josedab/streamline)
- [Kafka Benchmark Methodology](https://developer.confluent.io/learn/kafka-performance/)
