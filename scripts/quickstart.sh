#!/usr/bin/env bash
#
# Streamline quickstart script
# One command to build, run, and initialize a local Streamline instance.
#
# Usage: ./scripts/quickstart.sh [--release] [--no-build] [--download] [--install-dir <dir>]
#

set -euo pipefail

RELEASE_MODE=""
SKIP_BUILD=false
DOWNLOAD_MODE=false
INSTALL_DIR="${STREAMLINE_INSTALL_DIR:-}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --release)
            RELEASE_MODE="--release"
            shift
            ;;
        --no-build)
            SKIP_BUILD=true
            shift
            ;;
        --download)
            DOWNLOAD_MODE=true
            shift
            ;;
        --install-dir)
            INSTALL_DIR="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: ./scripts/quickstart.sh [--release] [--no-build] [--download] [--install-dir <dir>]"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

DATA_DIR="${STREAMLINE_DATA_DIR:-./data}"
LISTEN_ADDR="${STREAMLINE_LISTEN_ADDR:-127.0.0.1:9092}"
HTTP_ADDR="${STREAMLINE_HTTP_ADDR:-127.0.0.1:9094}"

if [[ -z "$INSTALL_DIR" ]]; then
    INSTALL_DIR="$HOME/.local/bin"
fi

CLIENT_LISTEN_ADDR="$LISTEN_ADDR"
if [[ "$LISTEN_ADDR" == 0.0.0.0:* ]]; then
    CLIENT_LISTEN_ADDR="127.0.0.1:${LISTEN_ADDR#0.0.0.0:}"
fi

CLIENT_HTTP_ADDR="$HTTP_ADDR"
if [[ "$HTTP_ADDR" == 0.0.0.0:* ]]; then
    CLIENT_HTTP_ADDR="127.0.0.1:${HTTP_ADDR#0.0.0.0:}"
fi

BIN_DIR="$PROJECT_ROOT/target/debug"
if [[ -n "$RELEASE_MODE" ]]; then
    BIN_DIR="$PROJECT_ROOT/target/release"
fi

LOCAL_STREAMLINE="$BIN_DIR/streamline"
LOCAL_STREAMLINE_CLI="$BIN_DIR/streamline-cli"
USE_DOWNLOAD="$DOWNLOAD_MODE"

if [[ "$USE_DOWNLOAD" == "false" ]]; then
    if [[ "$SKIP_BUILD" == "true" ]]; then
        if [[ -x "$LOCAL_STREAMLINE" && -x "$LOCAL_STREAMLINE_CLI" ]]; then
            STREAMLINE="$LOCAL_STREAMLINE"
            STREAMLINE_CLI="$LOCAL_STREAMLINE_CLI"
        elif command -v streamline >/dev/null 2>&1 && command -v streamline-cli >/dev/null 2>&1; then
            STREAMLINE="$(command -v streamline)"
            STREAMLINE_CLI="$(command -v streamline-cli)"
        elif ! command -v cargo >/dev/null 2>&1; then
            USE_DOWNLOAD=true
        else
            echo "Built binaries not found. Run without --no-build or use --download."
            exit 1
        fi
    elif command -v cargo >/dev/null 2>&1; then
        echo "Building Streamline binaries..."
        cargo build $RELEASE_MODE --bin streamline --bin streamline-cli
        STREAMLINE="$LOCAL_STREAMLINE"
        STREAMLINE_CLI="$LOCAL_STREAMLINE_CLI"
    else
        USE_DOWNLOAD=true
    fi
fi

if [[ "$USE_DOWNLOAD" == "true" ]]; then
    echo "Downloading Streamline binaries..."
    "$PROJECT_ROOT/scripts/install.sh" --dir "$INSTALL_DIR"
    STREAMLINE="$INSTALL_DIR/streamline"
    STREAMLINE_CLI="$INSTALL_DIR/streamline-cli"
fi

if [[ ! -x "$STREAMLINE" || ! -x "$STREAMLINE_CLI" ]]; then
    echo "Streamline binaries not found. Use --download or install from source."
    exit 1
fi

mkdir -p "$DATA_DIR"

SERVER_STARTED=false
if curl -fsS "http://${CLIENT_HTTP_ADDR}/health" > /dev/null 2>&1; then
    echo "Streamline server already running at ${CLIENT_LISTEN_ADDR}"
else
    echo "Starting Streamline server..."
    "$STREAMLINE" --data-dir "$DATA_DIR" --listen-addr "$LISTEN_ADDR" --http-addr "$HTTP_ADDR" &
    SERVER_PID=$!
    SERVER_STARTED=true

    echo -n "Waiting for server"
    for _ in {1..30}; do
        if curl -fsS "http://${CLIENT_HTTP_ADDR}/health" > /dev/null 2>&1; then
            echo " ✓"
            break
        fi
        sleep 0.2
        echo -n "."
    done
    echo
    if ! curl -fsS "http://${CLIENT_HTTP_ADDR}/health" > /dev/null 2>&1; then
        echo "Server did not become ready. Check logs and try again."
        CLI_HINT="streamline-cli"
        if ! command -v streamline-cli >/dev/null 2>&1; then
            CLI_HINT="$STREAMLINE_CLI"
        fi
        echo "Troubleshooting:"
        echo "  ${CLI_HINT} doctor --server-addr ${CLIENT_LISTEN_ADDR} --http-addr ${CLIENT_HTTP_ADDR}"
        echo "  curl http://${CLIENT_HTTP_ADDR}/health"
        echo "  See docs/TROUBLESHOOTING.md"
        exit 1
    fi
fi

echo "Running quickstart setup..."
STREAMLINE_DATA_DIR="$DATA_DIR" "$STREAMLINE_CLI" quickstart \
    --no-interactive \
    --no-start-server \
    --server-addr "$CLIENT_LISTEN_ADDR" \
    --http-addr "$CLIENT_HTTP_ADDR"

echo
CLI_CMD="streamline-cli"
if ! command -v streamline-cli >/dev/null 2>&1; then
    CLI_CMD="$STREAMLINE_CLI"
fi

echo "Next steps:"
echo "  ${CLI_CMD} topics list"
echo "  ${CLI_CMD} consume orders --from-beginning"
echo "  ${CLI_CMD} top"

if [[ "$SERVER_STARTED" == "true" ]]; then
    echo
    echo "To stop the server:"
    echo "  kill ${SERVER_PID}"
fi
