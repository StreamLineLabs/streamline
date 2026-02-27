#!/usr/bin/env bash
set -euo pipefail

# Streamline Interactive Demo
# This script demonstrates core Streamline features in ~5 minutes.
# Usage: ./scripts/demo.sh

STREAMLINE="${STREAMLINE_BIN:-./target/release/streamline}"
CLI="${CLI_BIN:-./target/release/streamline-cli}"
PORT=9092
HTTP_PORT=9094

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

step() { echo -e "\n${BLUE}━━━ Step $1: $2 ━━━${NC}\n"; }
info() { echo -e "${GREEN}▸ $*${NC}"; }
wait_key() { echo -e "${YELLOW}Press Enter to continue...${NC}"; read -r; }

cleanup() {
    info "Stopping Streamline server..."
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    rm -rf /tmp/streamline-demo-data
}
trap cleanup EXIT

echo -e "${GREEN}"
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║           🚀 Streamline Demo — The Redis of Streaming       ║"
echo "║                                                              ║"
echo "║  This demo walks through core features in ~5 minutes.       ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo -e "${NC}"

# Step 1: Start server
step 1 "Starting Streamline (single binary, zero config)"
info "Running: streamline --in-memory --playground"
$STREAMLINE --in-memory --playground --data-dir /tmp/streamline-demo-data &
SERVER_PID=$!
sleep 2
info "Server started in $(curl -s http://localhost:$HTTP_PORT/info | grep -o '"uptime_seconds":[0-9.]*' | cut -d: -f2)s"
info "Kafka protocol: localhost:$PORT"
info "HTTP API: localhost:$HTTP_PORT"
wait_key

# Step 2: Create a topic
step 2 "Creating a topic"
info "Running: streamline-cli topics create demo-orders --partitions 3"
$CLI topics create demo-orders --partitions 3 2>/dev/null || info "Topic may already exist"
$CLI topics list
wait_key

# Step 3: Produce messages
step 3 "Producing messages"
info "Running: streamline-cli produce demo-orders with template"
for i in $(seq 1 5); do
    $CLI produce demo-orders -m "{\"order_id\": $i, \"user\": \"user-$i\", \"amount\": $((RANDOM % 1000))}" -k "order-$i"
done
info "5 messages produced"
wait_key

# Step 4: Consume messages
step 4 "Consuming messages"
info "Running: streamline-cli consume demo-orders --from-beginning -n 5"
$CLI consume demo-orders --from-beginning -n 5
wait_key

# Step 5: SQL Query
step 5 "SQL on Streams (StreamQL)"
info "Running: streamline-cli query"
$CLI query "SELECT * FROM streamline_topic('demo-orders') LIMIT 3" 2>/dev/null || info "StreamQL query (requires analytics feature)"
wait_key

# Step 6: Health check
step 6 "Health & Metrics"
info "Health: $(curl -s http://localhost:$HTTP_PORT/health | head -c 100)"
info "Info: $(curl -s http://localhost:$HTTP_PORT/info | head -c 200)"
wait_key

echo -e "${GREEN}"
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║                    🎉 Demo Complete!                         ║"
echo "║                                                              ║"
echo "║  Next steps:                                                 ║"
echo "║  • Try the playground: https://play.streamline.io            ║"
echo "║  • Read the docs: https://docs.streamline.io                 ║"
echo "║  • Join Discord: https://discord.gg/streamline               ║"
echo "║  • Star on GitHub: https://github.com/streamlinelabs/streamline ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo -e "${NC}"
