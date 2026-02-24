#!/usr/bin/env bash
set -euo pipefail

# Cross-SDK Integration Test Runner
#
# Runs protocol compatibility tests against a Streamline server
# using all 7 SDK test containers.
#
# Usage:
#   ./tests/cross_sdk/run_all.sh
#   # Or specific SDKs:
#   ./tests/cross_sdk/run_all.sh java python
#
# Prerequisites:
#   - Docker and docker compose
#   - SDK repos cloned as siblings (../streamline-*-sdk/)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker_compose.yml"
ALL_SDKS="java python go node rust dotnet"
SDKS="${@:-$ALL_SDKS}"

echo "╔══════════════════════════════════════════════════╗"
echo "║  Streamline Cross-SDK Integration Test Suite     ║"
echo "╠══════════════════════════════════════════════════╣"
echo "║  SDKs: $SDKS"
echo "║  Compose: $COMPOSE_FILE"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# Phase 1: Start Streamline server
echo "🚀 Starting Streamline server..."
docker compose -f "$COMPOSE_FILE" up -d streamline
echo "   Waiting for health check..."
for i in $(seq 1 30); do
    if docker compose -f "$COMPOSE_FILE" exec -T streamline curl -sf http://localhost:9094/health >/dev/null 2>&1; then
        echo "   ✅ Server healthy (${i}s)"
        break
    fi
    sleep 1
    if [ "$i" -eq 30 ]; then
        echo "   ❌ Server failed to start"
        docker compose -f "$COMPOSE_FILE" logs streamline
        docker compose -f "$COMPOSE_FILE" down
        exit 1
    fi
done

# Phase 2: Run Rust protocol compatibility tests (in-process, fast)
echo ""
echo "🦀 Running Rust protocol compatibility tests..."
if cargo test --test cross_sdk_test 2>&1 | tail -5; then
    echo "   ✅ Protocol compatibility: PASS"
else
    echo "   ❌ Protocol compatibility: FAIL"
fi

# Phase 3: Run each SDK's integration tests
PASS=0
FAIL=0
for sdk in $SDKS; do
    service="${sdk}-sdk-test"
    echo ""
    echo "📦 Testing ${sdk} SDK..."

    if docker compose -f "$COMPOSE_FILE" run --rm "$service" 2>&1 | tail -10; then
        echo "   ✅ ${sdk}: PASS"
        PASS=$((PASS + 1))
    else
        echo "   ❌ ${sdk}: FAIL"
        FAIL=$((FAIL + 1))
    fi
done

# Phase 4: Cleanup
echo ""
echo "🧹 Cleaning up..."
docker compose -f "$COMPOSE_FILE" down

# Summary
echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║  Results: $PASS passed, $FAIL failed             "
if [ "$FAIL" -eq 0 ]; then
    echo "║  🎉 All SDK tests passed!                       ║"
else
    echo "║  ⚠️  $FAIL SDK(s) failed                         ║"
fi
echo "╚══════════════════════════════════════════════════╝"

exit "$FAIL"
