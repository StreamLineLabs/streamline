#!/usr/bin/env bash
#
# OpenMessaging Benchmark Runner
#
# Orchestrates running benchmarks across multiple messaging systems.
# Handles startup, warmup, benchmark execution, and result collection.
#
# Usage:
#   ./run-benchmarks.sh                           # Run default comparison
#   ./run-benchmarks.sh --systems streamline,kafka --workload quick
#   ./run-benchmarks.sh --all --workload comparison
#   ./run-benchmarks.sh --help
#

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
RESULTS_DIR="${PROJECT_DIR}/results"
COMPOSE_FILE="${PROJECT_DIR}/docker-compose.benchmarks.yml"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)

# Defaults
SYSTEMS="streamline,kafka"
WORKLOAD="streamline-comparison"
MONITORING=false
CLEAN_RESULTS=false
SKIP_STARTUP=false
WARMUP_SECONDS=30
OMB_HOME="${OMB_HOME:-}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Available systems
ALL_SYSTEMS="streamline,kafka,redpanda,pulsar,nats,rabbitmq"

# System to driver mapping
declare -A DRIVER_MAP=(
    ["streamline"]="driver-streamline/streamline.yaml"
    ["kafka"]="driver-kafka/kafka.yaml"
    ["redpanda"]="driver-redpanda/redpanda.yaml"
    ["pulsar"]="driver-pulsar/pulsar.yaml"
    ["nats"]="driver-nats-jetstream/nats-jetstream.yaml"
    ["rabbitmq"]="driver-rabbitmq/rabbitmq.yaml"
)

# System to container name mapping
declare -A CONTAINER_MAP=(
    ["streamline"]="benchmark-streamline"
    ["kafka"]="benchmark-kafka"
    ["redpanda"]="benchmark-redpanda"
    ["pulsar"]="benchmark-pulsar"
    ["nats"]="benchmark-nats"
    ["rabbitmq"]="benchmark-rabbitmq"
)

usage() {
    cat << EOF
OpenMessaging Benchmark Runner

Usage: $(basename "$0") [OPTIONS]

Options:
    -s, --systems SYSTEMS    Comma-separated list of systems to benchmark
                             Available: streamline,kafka,redpanda,pulsar,nats,rabbitmq
                             Default: streamline,kafka

    -w, --workload WORKLOAD  Workload configuration to use (without .yaml)
                             Available: streamline-quick, streamline-comparison,
                                       streamline-throughput, streamline-latency,
                                       workload-ecommerce, workload-iot, workload-logging
                             Default: streamline-comparison

    -a, --all                Benchmark all available systems

    -m, --monitoring         Enable monitoring stack (Prometheus + Grafana)

    -c, --clean              Clean previous results before running

    --skip-startup           Skip Docker container startup (assume already running)

    --warmup SECONDS         Warmup time after container startup (default: 30)

    --omb-home PATH          Path to OpenMessaging Benchmark installation
                             (required if not set via OMB_HOME env var)

    -h, --help               Show this help message

Examples:
    # Quick validation of Streamline
    $(basename "$0") -s streamline -w streamline-quick

    # Compare Streamline vs Kafka
    $(basename "$0") -s streamline,kafka -w streamline-comparison

    # Full comparison of all systems with monitoring
    $(basename "$0") --all -m -w streamline-comparison

    # Run e-commerce workload
    $(basename "$0") -s streamline,kafka,redpanda -w workload-ecommerce

Environment Variables:
    OMB_HOME    Path to OpenMessaging Benchmark installation

EOF
    exit 0
}

log() {
    echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[$(date '+%H:%M:%S')] ✓${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[$(date '+%H:%M:%S')] ⚠${NC} $1"
}

log_error() {
    echo -e "${RED}[$(date '+%H:%M:%S')] ✗${NC} $1"
}

check_dependencies() {
    local missing=()

    if ! command -v docker &> /dev/null; then
        missing+=("docker")
    fi

    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        missing+=("docker-compose")
    fi

    if [[ ${#missing[@]} -gt 0 ]]; then
        log_error "Missing dependencies: ${missing[*]}"
        exit 1
    fi

    if [[ -z "$OMB_HOME" ]]; then
        log_error "OMB_HOME not set. Please set it or use --omb-home"
        log_error "Example: export OMB_HOME=/path/to/openmessaging-benchmark"
        exit 1
    fi

    if [[ ! -d "$OMB_HOME" ]]; then
        log_error "OMB_HOME directory does not exist: $OMB_HOME"
        exit 1
    fi

    if [[ ! -f "$OMB_HOME/bin/benchmark" ]]; then
        log_error "OMB benchmark script not found. Build OMB first:"
        log_error "  cd $OMB_HOME && mvn clean install -DskipTests"
        exit 1
    fi
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -s|--systems)
                SYSTEMS="$2"
                shift 2
                ;;
            -w|--workload)
                WORKLOAD="$2"
                shift 2
                ;;
            -a|--all)
                SYSTEMS="$ALL_SYSTEMS"
                shift
                ;;
            -m|--monitoring)
                MONITORING=true
                shift
                ;;
            -c|--clean)
                CLEAN_RESULTS=true
                shift
                ;;
            --skip-startup)
                SKIP_STARTUP=true
                shift
                ;;
            --warmup)
                WARMUP_SECONDS="$2"
                shift 2
                ;;
            --omb-home)
                OMB_HOME="$2"
                shift 2
                ;;
            -h|--help)
                usage
                ;;
            *)
                log_error "Unknown option: $1"
                usage
                ;;
        esac
    done
}

docker_compose() {
    if docker compose version &> /dev/null 2>&1; then
        docker compose -f "$COMPOSE_FILE" "$@"
    else
        docker-compose -f "$COMPOSE_FILE" "$@"
    fi
}

start_containers() {
    local systems_array
    IFS=',' read -ra systems_array <<< "$SYSTEMS"

    log "Starting containers for: ${systems_array[*]}"

    # Build list of services to start
    local services=()
    for sys in "${systems_array[@]}"; do
        services+=("$sys")
    done

    # Add monitoring if enabled
    if [[ "$MONITORING" == true ]]; then
        log "Enabling monitoring stack..."
        docker_compose --profile monitoring up -d "${services[@]}" prometheus grafana cadvisor
    else
        docker_compose up -d "${services[@]}"
    fi

    log "Waiting for containers to be healthy..."
    wait_for_healthy "${systems_array[@]}"

    log "Warming up for ${WARMUP_SECONDS}s..."
    sleep "$WARMUP_SECONDS"

    log_success "All containers ready"
}

wait_for_healthy() {
    local systems=("$@")
    local max_wait=120
    local waited=0

    for sys in "${systems[@]}"; do
        local container="${CONTAINER_MAP[$sys]:-}"
        if [[ -z "$container" ]]; then
            continue
        fi

        log "  Waiting for $sys..."
        while [[ $waited -lt $max_wait ]]; do
            local health
            health=$(docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "unknown")

            if [[ "$health" == "healthy" ]]; then
                log_success "  $sys is healthy"
                break
            elif [[ "$health" == "unhealthy" ]]; then
                log_error "  $sys is unhealthy"
                docker logs --tail 20 "$container"
                exit 1
            fi

            sleep 2
            waited=$((waited + 2))
        done

        if [[ $waited -ge $max_wait ]]; then
            log_warn "  $sys health check timed out, continuing anyway..."
        fi
        waited=0
    done
}

run_benchmark() {
    local system="$1"
    local driver="${DRIVER_MAP[$system]:-}"

    if [[ -z "$driver" ]]; then
        log_error "Unknown system: $system"
        return 1
    fi

    local driver_path="$OMB_HOME/$driver"
    local workload_path="$OMB_HOME/workloads/${WORKLOAD}.yaml"

    # Check if using local workloads
    if [[ ! -f "$workload_path" ]]; then
        workload_path="${PROJECT_DIR}/workloads/${WORKLOAD}.yaml"
    fi

    if [[ ! -f "$workload_path" ]]; then
        log_error "Workload not found: ${WORKLOAD}.yaml"
        return 1
    fi

    local result_file="${RESULTS_DIR}/${TIMESTAMP}-${system}-${WORKLOAD}.json"

    log "Running benchmark: $system with $WORKLOAD"
    log "  Driver: $driver_path"
    log "  Workload: $workload_path"
    log "  Output: $result_file"

    # Run the benchmark
    if "$OMB_HOME/bin/benchmark" \
        --drivers "$driver_path" \
        --workloads "$workload_path" \
        --output "$result_file" 2>&1 | tee "${RESULTS_DIR}/${TIMESTAMP}-${system}.log"; then
        log_success "Benchmark complete for $system"
        return 0
    else
        log_error "Benchmark failed for $system"
        return 1
    fi
}

generate_report() {
    log "Generating comparison report..."

    local result_files=("${RESULTS_DIR}/${TIMESTAMP}-"*"-${WORKLOAD}.json")

    if [[ ${#result_files[@]} -eq 0 ]] || [[ ! -f "${result_files[0]}" ]]; then
        log_warn "No result files found for report generation"
        return
    fi

    # Generate reports in multiple formats
    if command -v python3 &> /dev/null; then
        local report_base="${RESULTS_DIR}/${TIMESTAMP}-report"

        python3 "${SCRIPT_DIR}/process_results.py" \
            "${result_files[@]}" \
            --format markdown \
            --output "${report_base}.md" \
            --title "Benchmark Results - ${WORKLOAD} - ${TIMESTAMP}" || true

        python3 "${SCRIPT_DIR}/process_results.py" \
            "${result_files[@]}" \
            --format html \
            --output "${report_base}.html" \
            --title "Benchmark Results - ${WORKLOAD} - ${TIMESTAMP}" || true

        # Try to generate charts
        python3 "${SCRIPT_DIR}/process_results.py" \
            "${result_files[@]}" \
            --chart "${report_base}" 2>/dev/null || true

        log_success "Reports generated:"
        log "  Markdown: ${report_base}.md"
        log "  HTML: ${report_base}.html"
    else
        log_warn "Python3 not found, skipping report generation"
    fi
}

cleanup() {
    if [[ "$SKIP_STARTUP" == false ]]; then
        log "Stopping containers..."
        docker_compose down -v 2>/dev/null || true
    fi
}

main() {
    parse_args "$@"
    check_dependencies

    # Create results directory
    mkdir -p "$RESULTS_DIR"

    # Clean previous results if requested
    if [[ "$CLEAN_RESULTS" == true ]]; then
        log "Cleaning previous results..."
        rm -rf "${RESULTS_DIR:?}"/*
    fi

    # Print configuration
    echo ""
    log "Configuration:"
    log "  Systems: $SYSTEMS"
    log "  Workload: $WORKLOAD"
    log "  Monitoring: $MONITORING"
    log "  OMB Home: $OMB_HOME"
    log "  Results: $RESULTS_DIR"
    echo ""

    # Start containers
    if [[ "$SKIP_STARTUP" == false ]]; then
        start_containers
    else
        log "Skipping container startup (--skip-startup)"
    fi

    # Parse systems into array
    local systems_array
    IFS=',' read -ra systems_array <<< "$SYSTEMS"

    # Run benchmarks
    local failed=()
    for sys in "${systems_array[@]}"; do
        if ! run_benchmark "$sys"; then
            failed+=("$sys")
        fi
        echo ""
    done

    # Generate report
    generate_report

    # Summary
    echo ""
    echo "========================================"
    log_success "Benchmark run complete!"
    log "Results saved to: $RESULTS_DIR"

    if [[ ${#failed[@]} -gt 0 ]]; then
        log_warn "Failed systems: ${failed[*]}"
    fi

    if [[ "$MONITORING" == true ]]; then
        log "Grafana available at: http://localhost:3000 (admin/admin)"
    fi
    echo "========================================"
}

# Trap cleanup on exit
trap cleanup EXIT

main "$@"
