#!/usr/bin/env bash
#
# validate-60-seconds.sh
#
# Validates the Streamline "60 seconds to first message" promise.
# This script measures the time from binary availability to successfully
# producing and consuming a message.
#
# Usage:
#   ./scripts/validate-60-seconds.sh [--release]
#
# Exit codes:
#   0 - Success (under 60 seconds)
#   1 - Failure (took too long or error occurred)
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
TEST_DIR=$(mktemp -d)
RELEASE_MODE=""

# Parse arguments
if [[ "$1" == "--release" ]]; then
    RELEASE_MODE="--release"
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}Cleaning up...${NC}"
    # Kill server if running
    if [[ -n "$SERVER_PID" ]]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    rm -rf "$TEST_DIR"
}

trap cleanup EXIT

echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}  Streamline 60-Second Validation Test${NC}"
echo -e "${YELLOW}========================================${NC}"
echo

# Record start time
START_TIME=$(date +%s.%N)

# Step 1: Build (if needed)
echo -e "${YELLOW}[Step 1/5]${NC} Building Streamline..."
cd "$PROJECT_ROOT"
if [[ -n "$RELEASE_MODE" ]]; then
    cargo build --release --bin streamline --bin streamline-cli 2>&1 | tail -1
    STREAMLINE="$PROJECT_ROOT/target/release/streamline"
    STREAMLINE_CLI="$PROJECT_ROOT/target/release/streamline-cli"
else
    cargo build --bin streamline --bin streamline-cli 2>&1 | tail -1
    STREAMLINE="$PROJECT_ROOT/target/debug/streamline"
    STREAMLINE_CLI="$PROJECT_ROOT/target/debug/streamline-cli"
fi
BUILD_TIME=$(date +%s.%N)
BUILD_ELAPSED=$(echo "$BUILD_TIME - $START_TIME" | bc)
echo -e "  Build completed in ${GREEN}${BUILD_ELAPSED}s${NC}"

# Step 2: Start server
echo -e "${YELLOW}[Step 2/5]${NC} Starting Streamline server..."
DATA_DIR="$TEST_DIR/data"
mkdir -p "$DATA_DIR"

"$STREAMLINE" --data-dir "$DATA_DIR" --listen-addr 127.0.0.1:19092 --http-addr 127.0.0.1:18080 &
SERVER_PID=$!

# Wait for server to be ready (check health endpoint)
echo -n "  Waiting for server..."
for i in {1..30}; do
    if curl -s http://127.0.0.1:18080/health > /dev/null 2>&1; then
        echo -e " ${GREEN}ready${NC}"
        break
    fi
    if [[ $i -eq 30 ]]; then
        echo -e " ${RED}timeout${NC}"
        exit 1
    fi
    sleep 0.2
    echo -n "."
done
SERVER_TIME=$(date +%s.%N)
SERVER_ELAPSED=$(echo "$SERVER_TIME - $BUILD_TIME" | bc)
echo -e "  Server started in ${GREEN}${SERVER_ELAPSED}s${NC}"

# Step 3: Create topic
echo -e "${YELLOW}[Step 3/5]${NC} Creating topic..."
"$STREAMLINE_CLI" --data-dir "$DATA_DIR" topics create test-topic --partitions 1 > /dev/null
TOPIC_TIME=$(date +%s.%N)
TOPIC_ELAPSED=$(echo "$TOPIC_TIME - $SERVER_TIME" | bc)
echo -e "  Topic created in ${GREEN}${TOPIC_ELAPSED}s${NC}"

# Step 4: Produce message
echo -e "${YELLOW}[Step 4/5]${NC} Producing message..."
TEST_MESSAGE="Hello, Streamline! Time: $(date +%s)"
"$STREAMLINE_CLI" --data-dir "$DATA_DIR" produce test-topic -m "$TEST_MESSAGE" > /dev/null
PRODUCE_TIME=$(date +%s.%N)
PRODUCE_ELAPSED=$(echo "$PRODUCE_TIME - $TOPIC_TIME" | bc)
echo -e "  Message produced in ${GREEN}${PRODUCE_ELAPSED}s${NC}"

# Step 5: Consume message
echo -e "${YELLOW}[Step 5/5]${NC} Consuming message..."
CONSUMED=$("$STREAMLINE_CLI" --data-dir "$DATA_DIR" consume test-topic --from-beginning -n 1 2>/dev/null | head -1)
CONSUME_TIME=$(date +%s.%N)
CONSUME_ELAPSED=$(echo "$CONSUME_TIME - $PRODUCE_TIME" | bc)
echo -e "  Message consumed in ${GREEN}${CONSUME_ELAPSED}s${NC}"

# Calculate total time
END_TIME=$(date +%s.%N)
TOTAL_ELAPSED=$(echo "$END_TIME - $START_TIME" | bc)

# Verify message content
echo
if [[ "$CONSUMED" == "$TEST_MESSAGE" ]]; then
    echo -e "${GREEN}Message verification: PASSED${NC}"
else
    echo -e "${RED}Message verification: FAILED${NC}"
    echo "  Expected: $TEST_MESSAGE"
    echo "  Received: $CONSUMED"
    exit 1
fi

# Summary
echo
echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}  Summary${NC}"
echo -e "${YELLOW}========================================${NC}"
echo "  Build:   ${BUILD_ELAPSED}s"
echo "  Server:  ${SERVER_ELAPSED}s"
echo "  Topic:   ${TOPIC_ELAPSED}s"
echo "  Produce: ${PRODUCE_ELAPSED}s"
echo "  Consume: ${CONSUME_ELAPSED}s"
echo -e "  ${YELLOW}────────────────────────────────────────${NC}"
printf "  Total:   %.2fs\n" "$TOTAL_ELAPSED"
echo

# Check if under 60 seconds
if (( $(echo "$TOTAL_ELAPSED < 60" | bc -l) )); then
    echo -e "${GREEN}SUCCESS: Completed in under 60 seconds!${NC}"
    exit 0
else
    echo -e "${RED}FAILED: Took longer than 60 seconds${NC}"
    exit 1
fi
