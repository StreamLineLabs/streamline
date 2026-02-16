#!/usr/bin/env bash
# Streamline Cluster Setup Example
#
# This script demonstrates how to set up a 3-node Streamline cluster.
# Each node runs on a different port on localhost for demonstration purposes.
#
# In production, each node would run on a separate server with its own
# data directory and network configuration.
#
# Prerequisites:
#   - Build Streamline with clustering feature:
#     cargo build --release --features clustering
#
# Usage:
#   ./examples/cluster_setup.sh start   # Start all nodes
#   ./examples/cluster_setup.sh stop    # Stop all nodes
#   ./examples/cluster_setup.sh status  # Check cluster status

set -e

# Configuration
STREAMLINE_BIN="${STREAMLINE_BIN:-./target/release/streamline}"
BASE_DATA_DIR="${BASE_DATA_DIR:-/tmp/streamline-cluster}"

# Node configurations
# Format: node_id:kafka_port:http_port:inter_broker_port
NODES=(
    "1:9092:9094:9093"
    "2:9102:9104:9103"
    "3:9112:9114:9113"
)

# Generate seed nodes list (inter-broker addresses)
SEED_NODES=""
for node in "${NODES[@]}"; do
    IFS=':' read -r node_id kafka_port http_port ib_port <<< "$node"
    if [ -n "$SEED_NODES" ]; then
        SEED_NODES="${SEED_NODES},"
    fi
    SEED_NODES="${SEED_NODES}127.0.0.1:${ib_port}"
done

start_cluster() {
    echo "Starting Streamline cluster..."
    mkdir -p "$BASE_DATA_DIR"

    for node in "${NODES[@]}"; do
        IFS=':' read -r node_id kafka_port http_port ib_port <<< "$node"

        NODE_DATA_DIR="${BASE_DATA_DIR}/node-${node_id}"
        mkdir -p "$NODE_DATA_DIR"

        echo "Starting node $node_id (Kafka: $kafka_port, HTTP: $http_port, Inter-broker: $ib_port)..."

        $STREAMLINE_BIN \
            --node-id "$node_id" \
            --listen-addr "127.0.0.1:${kafka_port}" \
            --http-addr "127.0.0.1:${http_port}" \
            --inter-broker-addr "127.0.0.1:${ib_port}" \
            --advertised-addr "127.0.0.1:${kafka_port}" \
            --seed-nodes "$SEED_NODES" \
            --data-dir "$NODE_DATA_DIR" \
            --default-replication-factor 3 \
            --min-insync-replicas 2 \
            --log-level info \
            > "${NODE_DATA_DIR}/streamline.log" 2>&1 &

        echo $! > "${NODE_DATA_DIR}/streamline.pid"
        echo "  PID: $(cat ${NODE_DATA_DIR}/streamline.pid)"
    done

    echo ""
    echo "Cluster started! Waiting for nodes to join..."
    sleep 3

    echo ""
    echo "Cluster endpoints:"
    for node in "${NODES[@]}"; do
        IFS=':' read -r node_id kafka_port http_port ib_port <<< "$node"
        echo "  Node $node_id: kafka://127.0.0.1:${kafka_port} | http://127.0.0.1:${http_port}"
    done

    echo ""
    echo "Test with:"
    echo "  # Create a replicated topic"
    echo "  streamline-cli --broker 127.0.0.1:9092 topics create my-topic --partitions 3 --replication-factor 3"
    echo ""
    echo "  # Produce messages"
    echo "  streamline-cli --broker 127.0.0.1:9092 produce my-topic -m 'Hello, cluster!'"
    echo ""
    echo "  # Consume from any node"
    echo "  streamline-cli --broker 127.0.0.1:9102 consume my-topic --from-beginning"
}

stop_cluster() {
    echo "Stopping Streamline cluster..."

    for node in "${NODES[@]}"; do
        IFS=':' read -r node_id kafka_port http_port ib_port <<< "$node"
        PID_FILE="${BASE_DATA_DIR}/node-${node_id}/streamline.pid"

        if [ -f "$PID_FILE" ]; then
            PID=$(cat "$PID_FILE")
            if kill -0 "$PID" 2>/dev/null; then
                echo "Stopping node $node_id (PID: $PID)..."
                kill "$PID" || true
                rm -f "$PID_FILE"
            else
                echo "Node $node_id not running (stale PID file)"
                rm -f "$PID_FILE"
            fi
        else
            echo "Node $node_id: no PID file found"
        fi
    done

    echo "Cluster stopped."
}

cluster_status() {
    echo "Streamline cluster status:"
    echo ""

    for node in "${NODES[@]}"; do
        IFS=':' read -r node_id kafka_port http_port ib_port <<< "$node"
        PID_FILE="${BASE_DATA_DIR}/node-${node_id}/streamline.pid"

        if [ -f "$PID_FILE" ]; then
            PID=$(cat "$PID_FILE")
            if kill -0 "$PID" 2>/dev/null; then
                echo "  Node $node_id: RUNNING (PID: $PID)"
                # Try to get cluster info from HTTP API
                if command -v curl &> /dev/null; then
                    CLUSTER_INFO=$(curl -s "http://127.0.0.1:${http_port}/api/v1/cluster" 2>/dev/null || echo '{}')
                    if [ "$CLUSTER_INFO" != "{}" ]; then
                        echo "    Cluster: $(echo "$CLUSTER_INFO" | grep -o '"cluster_id":"[^"]*"' | cut -d'"' -f4 || echo 'unknown')"
                    fi
                fi
            else
                echo "  Node $node_id: STOPPED (stale PID file)"
            fi
        else
            echo "  Node $node_id: STOPPED"
        fi
    done
}

case "${1:-}" in
    start)
        start_cluster
        ;;
    stop)
        stop_cluster
        ;;
    status)
        cluster_status
        ;;
    restart)
        stop_cluster
        sleep 2
        start_cluster
        ;;
    *)
        echo "Usage: $0 {start|stop|status|restart}"
        exit 1
        ;;
esac
