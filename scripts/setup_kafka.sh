#!/usr/bin/env bash
#
# Setup Kafka for benchmark comparison
#
# This script downloads and starts a single-node Kafka broker for benchmarking.
# Uses KRaft mode (no ZooKeeper) for simpler setup.
#
# Usage:
#   ./scripts/setup_kafka.sh [start|stop|status|clean]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
KAFKA_DIR="${PROJECT_ROOT}/.bench_kafka"
KAFKA_VERSION="3.6.1"
SCALA_VERSION="2.13"
KAFKA_PORT=9093

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

download_kafka() {
    if [[ -d "${KAFKA_DIR}/kafka" ]]; then
        log_info "Kafka already downloaded"
        return 0
    fi

    log_info "Downloading Kafka ${KAFKA_VERSION}..."
    mkdir -p "$KAFKA_DIR"
    cd "$KAFKA_DIR"

    local kafka_url="https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"

    if command -v curl &> /dev/null; then
        curl -L -o kafka.tgz "$kafka_url"
    elif command -v wget &> /dev/null; then
        wget -O kafka.tgz "$kafka_url"
    else
        log_error "Neither curl nor wget available"
        exit 1
    fi

    tar -xzf kafka.tgz
    mv "kafka_${SCALA_VERSION}-${KAFKA_VERSION}" kafka
    rm kafka.tgz

    log_success "Kafka downloaded to ${KAFKA_DIR}/kafka"
}

configure_kafka() {
    log_info "Configuring Kafka for KRaft mode..."

    local config_dir="${KAFKA_DIR}/config"
    local data_dir="${KAFKA_DIR}/data"
    mkdir -p "$config_dir" "$data_dir"

    # Generate cluster ID
    local cluster_id
    cluster_id=$("${KAFKA_DIR}/kafka/bin/kafka-storage.sh" random-uuid)

    # Create KRaft config
    cat > "${config_dir}/kraft.properties" << EOF
# KRaft server configuration for benchmarking
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9095
listeners=PLAINTEXT://:${KAFKA_PORT},CONTROLLER://:9095
advertised.listeners=PLAINTEXT://localhost:${KAFKA_PORT}
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
log.dirs=${data_dir}
num.partitions=6
default.replication.factor=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.retention.hours=1
log.segment.bytes=104857600
# Performance tuning
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
EOF

    # Format storage
    "${KAFKA_DIR}/kafka/bin/kafka-storage.sh" format \
        -t "$cluster_id" \
        -c "${config_dir}/kraft.properties" \
        --ignore-formatted

    log_success "Kafka configured"
}

start_kafka() {
    if pgrep -f "kafka.Kafka" > /dev/null 2>&1; then
        log_warn "Kafka is already running"
        return 0
    fi

    download_kafka
    configure_kafka

    log_info "Starting Kafka..."
    "${KAFKA_DIR}/kafka/bin/kafka-server-start.sh" \
        -daemon \
        "${KAFKA_DIR}/config/kraft.properties"

    # Wait for Kafka to be ready
    for i in {1..60}; do
        if nc -z localhost "$KAFKA_PORT" 2>/dev/null; then
            log_success "Kafka started on port ${KAFKA_PORT}"
            return 0
        fi
        sleep 1
    done

    log_error "Kafka failed to start"
    return 1
}

stop_kafka() {
    log_info "Stopping Kafka..."
    "${KAFKA_DIR}/kafka/bin/kafka-server-stop.sh" 2>/dev/null || true

    # Wait for shutdown
    for i in {1..30}; do
        if ! pgrep -f "kafka.Kafka" > /dev/null 2>&1; then
            log_success "Kafka stopped"
            return 0
        fi
        sleep 1
    done

    # Force kill if needed
    pkill -9 -f "kafka.Kafka" 2>/dev/null || true
    log_warn "Kafka forcefully stopped"
}

status_kafka() {
    if pgrep -f "kafka.Kafka" > /dev/null 2>&1; then
        log_success "Kafka is running on port ${KAFKA_PORT}"
        "${KAFKA_DIR}/kafka/bin/kafka-broker-api-versions.sh" \
            --bootstrap-server "localhost:${KAFKA_PORT}" 2>/dev/null | head -5 || true
    else
        log_info "Kafka is not running"
    fi
}

clean_kafka() {
    stop_kafka
    log_info "Cleaning Kafka data..."
    rm -rf "${KAFKA_DIR}"
    log_success "Kafka data cleaned"
}

case "${1:-start}" in
    start)
        start_kafka
        ;;
    stop)
        stop_kafka
        ;;
    status)
        status_kafka
        ;;
    clean)
        clean_kafka
        ;;
    *)
        echo "Usage: $0 [start|stop|status|clean]"
        exit 1
        ;;
esac
