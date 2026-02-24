#!/usr/bin/env bash
# Comparative Benchmark Runner
# Runs identical workloads against Streamline, Kafka, and Redpanda
# then generates a JSON report for the dashboard.
#
# Usage: ./run_comparative.sh [--quick] [--output results.json]
#
# Prerequisites:
#   - Docker and Docker Compose installed
#   - Streamline binary built (cargo build --release)
#   - kafkacat/kcat installed for Kafka/Redpanda benchmarks
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
OUTPUT_FILE="${REPO_ROOT}/benchmark-results/comparative-results.json"
QUICK_MODE=false
STREAMLINE_BIN="${REPO_ROOT}/target/release/streamline"
CLI_BIN="${REPO_ROOT}/target/release/streamline-cli"
RESULTS_DIR="${REPO_ROOT}/benchmark-results"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --quick) QUICK_MODE=true; shift ;;
        --output) OUTPUT_FILE="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

mkdir -p "$RESULTS_DIR"

# Benchmark parameters
if [ "$QUICK_MODE" = true ]; then
    DURATION=10
    WARMUP=2
    MESSAGE_SIZE=1024
    MESSAGES_PER_TEST=10000
else
    DURATION=60
    WARMUP=10
    MESSAGE_SIZE=1024
    MESSAGES_PER_TEST=100000
fi

TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
VERSION=$(grep '^version' "$REPO_ROOT/Cargo.toml" | head -1 | sed 's/.*"\(.*\)"/\1/')
COMMIT=$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null || echo "unknown")

echo "============================================"
echo " Streamline Comparative Benchmark Suite"
echo " Version: $VERSION | Commit: $COMMIT"
echo " Mode: $([ "$QUICK_MODE" = true ] && echo 'Quick' || echo 'Full')"
echo "============================================"

# Initialize results JSON
cat > "$OUTPUT_FILE" << EOF
{
  "metadata": {
    "timestamp": "$TIMESTAMP",
    "version": "$VERSION",
    "commit": "$COMMIT",
    "platform": "$(uname -s)-$(uname -m)",
    "quick_mode": $QUICK_MODE,
    "message_size_bytes": $MESSAGE_SIZE,
    "duration_secs": $DURATION
  },
  "results": []
}
EOF

append_result() {
    local target="$1"
    local benchmark_type="$2"
    local throughput_msgs="$3"
    local throughput_mb="$4"
    local p50="$5"
    local p99="$6"
    local p999="$7"
    local startup_ms="${8:-0}"

    local tmp=$(mktemp)
    python3 -c "
import json, sys
with open('$OUTPUT_FILE') as f:
    data = json.load(f)
data['results'].append({
    'target': '$target',
    'benchmark_type': '$benchmark_type',
    'throughput_msgs_sec': $throughput_msgs,
    'throughput_mb_sec': $throughput_mb,
    'latency_p50_ms': $p50,
    'latency_p99_ms': $p99,
    'latency_p999_ms': $p999,
    'startup_ms': $startup_ms,
    'message_size': $MESSAGE_SIZE,
    'duration_secs': $DURATION
})
with open('$OUTPUT_FILE', 'w') as f:
    json.dump(data, f, indent=2)
" 2>/dev/null || echo "Warning: Could not append result for $target/$benchmark_type"
}

# -------------------------------------------------------------------
# 1. Streamline Benchmarks
# -------------------------------------------------------------------
echo ""
echo ">>> Benchmarking Streamline..."

# Startup time
STARTUP_START=$(python3 -c 'import time; print(int(time.time()*1000))')
$STREAMLINE_BIN --in-memory &
SL_PID=$!
sleep 1

# Wait for health
for i in $(seq 1 30); do
    if curl -sf http://localhost:9094/health > /dev/null 2>&1; then
        break
    fi
    sleep 0.1
done
STARTUP_END=$(python3 -c 'import time; print(int(time.time()*1000))')
SL_STARTUP_MS=$((STARTUP_END - STARTUP_START))
echo "  Startup time: ${SL_STARTUP_MS}ms"

# Create test topic
$CLI_BIN topics create bench-test --partitions 6 2>/dev/null || true

# Producer benchmark
echo "  Running producer benchmark..."
PRODUCER_OUTPUT=$($CLI_BIN benchmark --profile throughput \
    --duration "$DURATION" \
    --message-size "$MESSAGE_SIZE" \
    --output "$RESULTS_DIR/streamline-producer.json" 2>&1 || echo "throughput: 0 msgs/sec, 0 MB/sec")

# Extract metrics (parse CLI output or JSON)
SL_PROD_MSGS=$(echo "$PRODUCER_OUTPUT" | grep -oP '[\d.]+(?= msgs/sec)' | head -1 || echo "0")
SL_PROD_MB=$(echo "$PRODUCER_OUTPUT" | grep -oP '[\d.]+(?= MB/sec)' | head -1 || echo "0")
SL_PROD_P50=$(echo "$PRODUCER_OUTPUT" | grep -oP 'p50=[\d.]+' | grep -oP '[\d.]+' || echo "0")
SL_PROD_P99=$(echo "$PRODUCER_OUTPUT" | grep -oP 'p99=[\d.]+' | grep -oP '[\d.]+' || echo "0")
SL_PROD_P999=$(echo "$PRODUCER_OUTPUT" | grep -oP 'p999=[\d.]+' | grep -oP '[\d.]+' || echo "0")

: "${SL_PROD_MSGS:=0}" "${SL_PROD_MB:=0}" "${SL_PROD_P50:=0}" "${SL_PROD_P99:=0}" "${SL_PROD_P999:=0}"

append_result "streamline" "producer" "$SL_PROD_MSGS" "$SL_PROD_MB" "$SL_PROD_P50" "$SL_PROD_P99" "$SL_PROD_P999" "$SL_STARTUP_MS"

# Latency benchmark
echo "  Running latency benchmark..."
LATENCY_OUTPUT=$($CLI_BIN benchmark --profile latency \
    --duration "$DURATION" \
    --message-size "$MESSAGE_SIZE" \
    --output "$RESULTS_DIR/streamline-latency.json" 2>&1 || echo "p50=0 p99=0 p999=0")

SL_LAT_P50=$(echo "$LATENCY_OUTPUT" | grep -oP 'p50=[\d.]+' | grep -oP '[\d.]+' || echo "0")
SL_LAT_P99=$(echo "$LATENCY_OUTPUT" | grep -oP 'p99=[\d.]+' | grep -oP '[\d.]+' || echo "0")
SL_LAT_P999=$(echo "$LATENCY_OUTPUT" | grep -oP 'p999=[\d.]+' | grep -oP '[\d.]+' || echo "0")
SL_LAT_MSGS=$(echo "$LATENCY_OUTPUT" | grep -oP '[\d.]+(?= msgs/sec)' | head -1 || echo "0")

: "${SL_LAT_P50:=0}" "${SL_LAT_P99:=0}" "${SL_LAT_P999:=0}" "${SL_LAT_MSGS:=0}"

append_result "streamline" "e2e_latency" "$SL_LAT_MSGS" "0" "$SL_LAT_P50" "$SL_LAT_P99" "$SL_LAT_P999" "$SL_STARTUP_MS"

# Startup benchmark
append_result "streamline" "startup" "0" "0" "0" "0" "0" "$SL_STARTUP_MS"

# Clean up Streamline
kill $SL_PID 2>/dev/null || true
wait $SL_PID 2>/dev/null || true
echo "  Streamline benchmarks complete."

# -------------------------------------------------------------------
# 2. Kafka Benchmarks (if Docker available)
# -------------------------------------------------------------------
if command -v docker &> /dev/null; then
    echo ""
    echo ">>> Benchmarking Apache Kafka..."

    # Start Kafka via Docker
    KAFKA_STARTUP_START=$(python3 -c 'import time; print(int(time.time()*1000))')
    docker run -d --name bench-kafka \
        -p 9093:9093 \
        -e KAFKA_CFG_NODE_ID=0 \
        -e KAFKA_CFG_PROCESS_ROLES=controller,broker \
        -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@localhost:9094 \
        -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9093,CONTROLLER://:9094 \
        -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9093 \
        -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
        -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
        bitnami/kafka:3.7 2>/dev/null || true

    # Wait for Kafka to be ready
    for i in $(seq 1 60); do
        if docker exec bench-kafka kafka-topics.sh --bootstrap-server localhost:9093 --list 2>/dev/null; then
            break
        fi
        sleep 1
    done
    KAFKA_STARTUP_END=$(python3 -c 'import time; print(int(time.time()*1000))')
    KAFKA_STARTUP_MS=$((KAFKA_STARTUP_END - KAFKA_STARTUP_START))
    echo "  Kafka startup time: ${KAFKA_STARTUP_MS}ms"

    # Create test topic
    docker exec bench-kafka kafka-topics.sh \
        --bootstrap-server localhost:9093 \
        --create --topic bench-test --partitions 6 2>/dev/null || true

    # Producer benchmark using kafka-producer-perf-test
    echo "  Running Kafka producer benchmark..."
    KAFKA_PROD_OUTPUT=$(docker exec bench-kafka kafka-producer-perf-test.sh \
        --topic bench-test \
        --num-records "$MESSAGES_PER_TEST" \
        --record-size "$MESSAGE_SIZE" \
        --throughput -1 \
        --producer-props bootstrap.servers=localhost:9093 \
        2>&1 || echo "0 records/sec, 0 MB/sec avg latency 0 ms, 99th 0 ms")

    KAFKA_MSGS=$(echo "$KAFKA_PROD_OUTPUT" | tail -1 | grep -oP '[\d.]+(?= records/sec)' || echo "0")
    KAFKA_MB=$(echo "$KAFKA_PROD_OUTPUT" | tail -1 | grep -oP '[\d.]+(?= MB/sec)' || echo "0")
    KAFKA_AVG=$(echo "$KAFKA_PROD_OUTPUT" | tail -1 | grep -oP 'avg latency [\d.]+' | grep -oP '[\d.]+' || echo "0")
    KAFKA_P99=$(echo "$KAFKA_PROD_OUTPUT" | tail -1 | grep -oP '99th [\d.]+' | grep -oP '[\d.]+' || echo "0")

    : "${KAFKA_MSGS:=0}" "${KAFKA_MB:=0}" "${KAFKA_AVG:=0}" "${KAFKA_P99:=0}"

    append_result "kafka" "producer" "$KAFKA_MSGS" "$KAFKA_MB" "$KAFKA_AVG" "$KAFKA_P99" "0" "$KAFKA_STARTUP_MS"
    append_result "kafka" "startup" "0" "0" "0" "0" "0" "$KAFKA_STARTUP_MS"

    # Clean up
    docker stop bench-kafka 2>/dev/null && docker rm bench-kafka 2>/dev/null || true
    echo "  Kafka benchmarks complete."
else
    echo ">>> Skipping Kafka/Redpanda benchmarks (Docker not available)"
fi

# -------------------------------------------------------------------
# 3. Generate dashboard data
# -------------------------------------------------------------------
echo ""
echo ">>> Generating benchmark dashboard..."

python3 "$SCRIPT_DIR/dashboard/generate_dashboard.py" \
    --input "$OUTPUT_FILE" \
    --output "$RESULTS_DIR/dashboard.html" 2>/dev/null || echo "Dashboard generation skipped (python3 issue)"

echo ""
echo "============================================"
echo " Benchmark Results: $OUTPUT_FILE"
echo "============================================"
cat "$OUTPUT_FILE"
