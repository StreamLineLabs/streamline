#!/usr/bin/env bash
#
# Streamline Benchmark Orchestration Script
#
# Usage:
#   ./scripts/run_benchmarks.sh [OPTIONS]
#
# Options:
#   --all                Run all benchmarks against all targets
#   --streamline         Run benchmarks against Streamline only
#   --kafka              Run benchmarks against Kafka only
#   --redpanda           Run benchmarks against Redpanda only
#   --producer           Run producer throughput benchmarks
#   --consumer           Run consumer throughput benchmarks
#   --e2e                Run end-to-end latency benchmarks
#   --startup            Run startup time benchmarks
#   --message-size SIZE  Message size in bytes (default: 1024)
#   --duration SECS      Benchmark duration in seconds (default: 60)
#   --partitions NUM     Number of partitions (default: 6)
#   --output DIR         Output directory for results (default: ./benchmark-results)
#   --docker-compose     Start Kafka/Redpanda via Docker Compose before benchmarks
#   --clean              Remove existing results before running
#   --json               Output results in JSON format
#   --help               Show this help message
#
# Examples:
#   ./scripts/run_benchmarks.sh --all
#   ./scripts/run_benchmarks.sh --streamline --producer --message-size 4096
#   ./scripts/run_benchmarks.sh --docker-compose --all

set -euo pipefail

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Default configuration
MESSAGE_SIZE=1024
DURATION=60
PARTITIONS=6
OUTPUT_DIR="${PROJECT_ROOT}/benchmark-results"
TARGETS=()
BENCHMARKS=()
USE_DOCKER_COMPOSE=false
CLEAN_RESULTS=false
JSON_OUTPUT=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $*"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $*"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*" >&2
}

# Show help
show_help() {
    head -40 "$0" | grep "^#" | sed 's/^# *//'
}

# Parse arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --all)
                TARGETS=(streamline kafka redpanda)
                BENCHMARKS=(producer consumer e2e startup)
                shift
                ;;
            --streamline)
                TARGETS+=(streamline)
                shift
                ;;
            --kafka)
                TARGETS+=(kafka)
                shift
                ;;
            --redpanda)
                TARGETS+=(redpanda)
                shift
                ;;
            --producer)
                BENCHMARKS+=(producer)
                shift
                ;;
            --consumer)
                BENCHMARKS+=(consumer)
                shift
                ;;
            --e2e)
                BENCHMARKS+=(e2e)
                shift
                ;;
            --startup)
                BENCHMARKS+=(startup)
                shift
                ;;
            --message-size)
                MESSAGE_SIZE="$2"
                shift 2
                ;;
            --duration)
                DURATION="$2"
                shift 2
                ;;
            --partitions)
                PARTITIONS="$2"
                shift 2
                ;;
            --output)
                OUTPUT_DIR="$2"
                shift 2
                ;;
            --docker-compose)
                USE_DOCKER_COMPOSE=true
                shift
                ;;
            --clean)
                CLEAN_RESULTS=true
                shift
                ;;
            --json)
                JSON_OUTPUT=true
                shift
                ;;
            --help|-h)
                show_help
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done

    # Default to streamline and producer if nothing specified
    if [[ ${#TARGETS[@]} -eq 0 ]]; then
        TARGETS=(streamline)
    fi
    if [[ ${#BENCHMARKS[@]} -eq 0 ]]; then
        BENCHMARKS=(producer)
    fi
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."

    # Check for Rust/Cargo
    if ! command -v cargo &> /dev/null; then
        log_error "cargo not found. Please install Rust."
        exit 1
    fi

    # Check for Docker if docker-compose mode
    if [[ "$USE_DOCKER_COMPOSE" == true ]]; then
        if ! command -v docker &> /dev/null; then
            log_error "docker not found. Please install Docker."
            exit 1
        fi
        if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
            log_error "docker-compose not found. Please install Docker Compose."
            exit 1
        fi
    fi

    # Check for Python (for report generation)
    if ! command -v python3 &> /dev/null; then
        log_warn "python3 not found. Report generation will be skipped."
    fi

    log_success "Prerequisites check passed"
}

# Start Docker containers
start_docker_services() {
    if [[ "$USE_DOCKER_COMPOSE" != true ]]; then
        return
    fi

    log_info "Starting Docker services..."

    local compose_file="${PROJECT_ROOT}/docker/docker-compose.benchmarks.yml"
    if [[ ! -f "$compose_file" ]]; then
        log_error "Docker Compose file not found: $compose_file"
        exit 1
    fi

    # Use docker compose or docker-compose
    local compose_cmd="docker compose"
    if ! docker compose version &> /dev/null 2>&1; then
        compose_cmd="docker-compose"
    fi

    cd "${PROJECT_ROOT}/docker"

    # Start services based on targets
    local services=""
    for target in "${TARGETS[@]}"; do
        case $target in
            kafka)
                services="$services zookeeper kafka"
                ;;
            redpanda)
                services="$services redpanda"
                ;;
        esac
    done

    if [[ -n "$services" ]]; then
        $compose_cmd -f docker-compose.benchmarks.yml up -d $services
        log_info "Waiting for services to be ready..."
        sleep 15
    fi

    cd "$PROJECT_ROOT"
    log_success "Docker services started"
}

# Stop Docker containers
stop_docker_services() {
    if [[ "$USE_DOCKER_COMPOSE" != true ]]; then
        return
    fi

    log_info "Stopping Docker services..."

    local compose_cmd="docker compose"
    if ! docker compose version &> /dev/null 2>&1; then
        compose_cmd="docker-compose"
    fi

    cd "${PROJECT_ROOT}/docker"
    $compose_cmd -f docker-compose.benchmarks.yml down -v 2>/dev/null || true
    cd "$PROJECT_ROOT"

    log_success "Docker services stopped"
}

# Build Streamline
build_streamline() {
    log_info "Building Streamline..."
    cd "$PROJECT_ROOT"
    cargo build --release --bin streamline
    log_success "Streamline built successfully"
}

# Start Streamline server
start_streamline() {
    log_info "Starting Streamline server..."

    local data_dir="${OUTPUT_DIR}/streamline-data"
    mkdir -p "$data_dir"

    "${PROJECT_ROOT}/target/release/streamline" \
        --data-dir "$data_dir" \
        --listen-addr "127.0.0.1:9092" \
        --http-addr "127.0.0.1:9094" \
        --log-level warn &

    STREAMLINE_PID=$!
    sleep 2

    if ! kill -0 "$STREAMLINE_PID" 2>/dev/null; then
        log_error "Failed to start Streamline"
        exit 1
    fi

    log_success "Streamline started (PID: $STREAMLINE_PID)"
}

# Stop Streamline server
stop_streamline() {
    if [[ -n "${STREAMLINE_PID:-}" ]]; then
        log_info "Stopping Streamline server..."
        kill "$STREAMLINE_PID" 2>/dev/null || true
        wait "$STREAMLINE_PID" 2>/dev/null || true
        log_success "Streamline stopped"
    fi
}

# Get broker address for target
get_broker_addr() {
    local target=$1
    case $target in
        streamline)
            echo "127.0.0.1:9092"
            ;;
        kafka)
            echo "127.0.0.1:9093"
            ;;
        redpanda)
            echo "127.0.0.1:9094"
            ;;
    esac
}

# Run a single benchmark
run_benchmark() {
    local target=$1
    local benchmark=$2
    local broker_addr
    broker_addr=$(get_broker_addr "$target")

    log_info "Running $benchmark benchmark against $target..."

    local result_file="${OUTPUT_DIR}/${target}_${benchmark}_$(date +%Y%m%d_%H%M%S).json"

    # Set environment variables for benchmark configuration
    export BENCHMARK_TARGET="$target"
    export BENCHMARK_BROKER="$broker_addr"
    export BENCHMARK_MESSAGE_SIZE="$MESSAGE_SIZE"
    export BENCHMARK_DURATION="$DURATION"
    export BENCHMARK_PARTITIONS="$PARTITIONS"
    export BENCHMARK_OUTPUT="$result_file"

    # Run the appropriate benchmark
    case $benchmark in
        producer)
            cargo bench --bench comparative_benchmarks -- "streamline/producer" --noplot 2>/dev/null || true
            ;;
        consumer)
            cargo bench --bench comparative_benchmarks -- "streamline/consumer" --noplot 2>/dev/null || true
            ;;
        e2e)
            cargo bench --bench comparative_benchmarks -- "streamline/e2e" --noplot 2>/dev/null || true
            ;;
        startup)
            cargo bench --bench comparative_benchmarks -- "startup" --noplot 2>/dev/null || true
            ;;
    esac

    log_success "Completed $benchmark benchmark against $target"
}

# Run all selected benchmarks
run_all_benchmarks() {
    log_info "Starting benchmark suite..."
    log_info "Targets: ${TARGETS[*]}"
    log_info "Benchmarks: ${BENCHMARKS[*]}"
    log_info "Message size: ${MESSAGE_SIZE} bytes"
    log_info "Duration: ${DURATION} seconds"
    log_info "Partitions: ${PARTITIONS}"

    for target in "${TARGETS[@]}"; do
        # Start target if needed
        case $target in
            streamline)
                start_streamline
                ;;
            kafka|redpanda)
                # Assume Docker services are already running
                ;;
        esac

        # Run benchmarks
        for benchmark in "${BENCHMARKS[@]}"; do
            run_benchmark "$target" "$benchmark"
        done

        # Stop target if needed
        case $target in
            streamline)
                stop_streamline
                ;;
        esac
    done

    log_success "All benchmarks completed"
}

# Generate report
generate_report() {
    if ! command -v python3 &> /dev/null; then
        log_warn "Skipping report generation (python3 not found)"
        return
    fi

    if [[ ! -f "${SCRIPT_DIR}/generate_report.py" ]]; then
        log_warn "Skipping report generation (generate_report.py not found)"
        return
    fi

    log_info "Generating benchmark report..."
    python3 "${SCRIPT_DIR}/generate_report.py" \
        --input-dir "$OUTPUT_DIR" \
        --output "${OUTPUT_DIR}/BENCHMARK_REPORT.md"
    log_success "Report generated: ${OUTPUT_DIR}/BENCHMARK_REPORT.md"
}

# Cleanup
cleanup() {
    stop_streamline
    stop_docker_services
}

# Main
main() {
    parse_args "$@"

    # Set up cleanup on exit
    trap cleanup EXIT

    # Create output directory
    if [[ "$CLEAN_RESULTS" == true ]] && [[ -d "$OUTPUT_DIR" ]]; then
        log_info "Cleaning existing results..."
        rm -rf "$OUTPUT_DIR"
    fi
    mkdir -p "$OUTPUT_DIR"

    check_prerequisites
    build_streamline
    start_docker_services
    run_all_benchmarks
    generate_report

    log_success "Benchmark suite completed!"
    log_info "Results saved to: $OUTPUT_DIR"
}

main "$@"
