#!/usr/bin/env bash
#
# Setup Redpanda for benchmark comparison
#
# This script starts a single-node Redpanda broker using Docker for benchmarking.
# Redpanda is a Kafka-compatible streaming platform with faster startup times.
#
# Usage:
#   ./scripts/setup_redpanda.sh [start|stop|status|clean]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REDPANDA_DIR="${PROJECT_ROOT}/.bench_redpanda"
REDPANDA_PORT=9094
REDPANDA_ADMIN_PORT=9644
CONTAINER_NAME="streamline-bench-redpanda"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

check_docker() {
    if ! command -v docker &> /dev/null; then
        log_error "Docker is required to run Redpanda. Please install Docker."
        exit 1
    fi

    if ! docker info &> /dev/null; then
        log_error "Docker daemon is not running. Please start Docker."
        exit 1
    fi
}

start_redpanda() {
    check_docker

    if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        log_warn "Redpanda is already running"
        return 0
    fi

    # Remove stopped container if exists
    docker rm -f "$CONTAINER_NAME" 2>/dev/null || true

    log_info "Starting Redpanda..."
    mkdir -p "${REDPANDA_DIR}/data"

    docker run -d \
        --name "$CONTAINER_NAME" \
        --pull=missing \
        -p "${REDPANDA_PORT}:9092" \
        -p "${REDPANDA_ADMIN_PORT}:9644" \
        -v "${REDPANDA_DIR}/data:/var/lib/redpanda/data" \
        docker.redpanda.com/redpandadata/redpanda:latest \
        redpanda start \
        --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:9092 \
        --advertise-kafka-addr internal://localhost:9092,external://localhost:${REDPANDA_PORT} \
        --pandaproxy-addr internal://0.0.0.0:8082,external://0.0.0.0:8082 \
        --advertise-pandaproxy-addr internal://localhost:8082,external://localhost:8082 \
        --schema-registry-addr internal://0.0.0.0:8081,external://0.0.0.0:8081 \
        --rpc-addr localhost:33145 \
        --advertise-rpc-addr localhost:33145 \
        --mode dev-container \
        --smp 1 \
        --memory 1G \
        --reserve-memory 0M \
        --overprovisioned \
        --default-log-level=warn

    # Wait for Redpanda to be ready
    log_info "Waiting for Redpanda to be ready..."
    for i in {1..60}; do
        if docker exec "$CONTAINER_NAME" rpk cluster health 2>/dev/null | grep -q "HEALTHY"; then
            log_success "Redpanda started on port ${REDPANDA_PORT}"
            return 0
        fi
        sleep 1
    done

    # Check if at least the port is open
    if nc -z localhost "$REDPANDA_PORT" 2>/dev/null; then
        log_success "Redpanda started on port ${REDPANDA_PORT}"
        return 0
    fi

    log_error "Redpanda failed to start"
    docker logs "$CONTAINER_NAME"
    return 1
}

stop_redpanda() {
    log_info "Stopping Redpanda..."

    if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        docker stop "$CONTAINER_NAME" 2>/dev/null || true
        docker rm "$CONTAINER_NAME" 2>/dev/null || true
        log_success "Redpanda stopped"
    else
        log_info "Redpanda is not running"
    fi
}

status_redpanda() {
    check_docker

    if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        log_success "Redpanda is running"

        echo ""
        log_info "Cluster health:"
        docker exec "$CONTAINER_NAME" rpk cluster health 2>/dev/null || true

        echo ""
        log_info "Broker info:"
        docker exec "$CONTAINER_NAME" rpk cluster info 2>/dev/null || true
    else
        log_info "Redpanda is not running"
    fi
}

clean_redpanda() {
    stop_redpanda

    log_info "Cleaning Redpanda data..."
    rm -rf "${REDPANDA_DIR}"
    log_success "Redpanda data cleaned"
}

# Create benchmark topic
create_bench_topic() {
    if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        log_error "Redpanda is not running"
        return 1
    fi

    log_info "Creating benchmark topic..."
    docker exec "$CONTAINER_NAME" rpk topic create benchmark \
        --partitions 6 \
        --config retention.ms=3600000 \
        2>/dev/null || true

    log_success "Benchmark topic created"
}

case "${1:-start}" in
    start)
        start_redpanda
        ;;
    stop)
        stop_redpanda
        ;;
    status)
        status_redpanda
        ;;
    clean)
        clean_redpanda
        ;;
    create-topic)
        create_bench_topic
        ;;
    *)
        echo "Usage: $0 [start|stop|status|clean|create-topic]"
        exit 1
        ;;
esac
