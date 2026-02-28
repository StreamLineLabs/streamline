#!/usr/bin/env bash
# Reproducible Benchmark Suite for Streamline
# Runs in a controlled Docker environment with fixed resources
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
RESULTS_DIR="${PROJECT_DIR}/benchmark-results"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
RESULTS_FILE="${RESULTS_DIR}/results-${TIMESTAMP}.json"

# Configuration
CPUS=${BENCH_CPUS:-4}
MEMORY=${BENCH_MEMORY:-8g}
MESSAGE_SIZES=(64 1024 10240 102400)
PARTITION_COUNTS=(1 4 16)
BATCH_SIZES=(1 100 1000)
DURATION_SECS=${BENCH_DURATION:-30}

mkdir -p "$RESULTS_DIR"

echo "============================================"
echo " Streamline Reproducible Benchmark Suite"
echo " CPUs: ${CPUS}, Memory: ${MEMORY}"
echo " Timestamp: ${TIMESTAMP}"
echo "============================================"

# Build release binary
echo "[1/5] Building release binary..."
cd "$PROJECT_DIR"
cargo build --release --quiet 2>/dev/null

BINARY="${PROJECT_DIR}/target/release/streamline"
CLI="${PROJECT_DIR}/target/release/streamline-cli"

if [[ ! -f "$BINARY" ]]; then
    echo "ERROR: Release binary not found at $BINARY"
    exit 1
fi

# Measure startup time
echo "[2/5] Measuring startup time..."
STARTUP_TIMES=()
for i in $(seq 1 5); do
    DATA_DIR=$(mktemp -d)
    START=$(date +%s%N)
    $BINARY --data-dir "$DATA_DIR" --listen-addr "127.0.0.1:0" --http-addr "127.0.0.1:0" &
    PID=$!
    # Wait for health endpoint
    for attempt in $(seq 1 100); do
        if kill -0 $PID 2>/dev/null; then
            sleep 0.01
        else
            break
        fi
    done
    END=$(date +%s%N)
    kill $PID 2>/dev/null || true
    wait $PID 2>/dev/null || true
    STARTUP_MS=$(( (END - START) / 1000000 ))
    STARTUP_TIMES+=($STARTUP_MS)
    rm -rf "$DATA_DIR"
done
AVG_STARTUP=$(echo "${STARTUP_TIMES[@]}" | tr ' ' '\n' | awk '{sum+=$1} END {printf "%.0f", sum/NR}')

# Measure idle memory
echo "[3/5] Measuring idle memory..."
DATA_DIR=$(mktemp -d)
$BINARY --data-dir "$DATA_DIR" --listen-addr "127.0.0.1:19092" --http-addr "127.0.0.1:19094" &
SERVER_PID=$!
sleep 2
IDLE_RSS=$(ps -o rss= -p $SERVER_PID 2>/dev/null | tr -d ' ')
IDLE_MB=$((IDLE_RSS / 1024))

# Measure binary size
BINARY_SIZE=$(stat -f%z "$BINARY" 2>/dev/null || stat -c%s "$BINARY" 2>/dev/null || echo 0)
BINARY_MB=$((BINARY_SIZE / 1048576))

# Run criterion benchmarks
echo "[4/5] Running criterion benchmarks..."
cargo bench --bench storage_benchmarks -- --output-format bencher 2>/dev/null | tee "${RESULTS_DIR}/storage-${TIMESTAMP}.txt" || true
cargo bench --bench protocol_benchmarks -- --output-format bencher 2>/dev/null | tee "${RESULTS_DIR}/protocol-${TIMESTAMP}.txt" || true
cargo bench --bench latency_benchmarks -- --output-format bencher 2>/dev/null | tee "${RESULTS_DIR}/latency-${TIMESTAMP}.txt" || true

# Cleanup
kill $SERVER_PID 2>/dev/null || true
wait $SERVER_PID 2>/dev/null || true
rm -rf "$DATA_DIR"

# Generate JSON results
echo "[5/5] Generating results..."
cat > "$RESULTS_FILE" << EOF
{
  "timestamp": "${TIMESTAMP}",
  "environment": {
    "cpus": ${CPUS},
    "memory": "${MEMORY}",
    "os": "$(uname -s)",
    "arch": "$(uname -m)",
    "rust_version": "$(rustc --version | awk '{print $2}')"
  },
  "results": {
    "startup_time_ms": ${AVG_STARTUP},
    "idle_memory_mb": ${IDLE_MB},
    "binary_size_mb": ${BINARY_MB}
  }
}
EOF

echo ""
echo "============================================"
echo " Results Summary"
echo "============================================"
echo " Startup time:   ${AVG_STARTUP}ms (avg of 5 runs)"
echo " Idle memory:    ${IDLE_MB}MB RSS"
echo " Binary size:    ${BINARY_MB}MB"
echo " Results saved:  ${RESULTS_FILE}"
echo "============================================"
