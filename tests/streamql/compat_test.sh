#!/usr/bin/env bash
set -euo pipefail

# StreamQL / ksqlDB Compatibility Test Suite
#
# Validates that Streamline's StreamQL endpoint accepts standard ksqlDB
# client requests and returns Confluent-compatible responses.
#
# Prerequisites:
#   - Streamline server running at localhost:9094
#   - curl and jq installed
#
# Usage:
#   ./tests/streamql/compat_test.sh
#   # Or with custom endpoint:
#   STREAMQL_URL=http://my-server:9094 ./tests/streamql/compat_test.sh

STREAMQL_URL="${STREAMQL_URL:-http://localhost:9094}"
PASS=0
FAIL=0
TOTAL=0

# Helper: run a test case
run_test() {
    local name="$1"
    local method="$2"
    local path="$3"
    local body="$4"
    local expected_status="$5"
    local expected_content="$6"

    TOTAL=$((TOTAL + 1))

    local status
    local response
    if [ "$method" = "GET" ]; then
        response=$(curl -s -w "\n%{http_code}" "$STREAMQL_URL$path" 2>/dev/null)
    else
        response=$(curl -s -w "\n%{http_code}" -X "$method" "$STREAMQL_URL$path" \
            -H "Content-Type: application/json" \
            -d "$body" 2>/dev/null)
    fi

    status=$(echo "$response" | tail -1)
    body_response=$(echo "$response" | sed '$d')

    if [ "$status" != "$expected_status" ]; then
        echo "  ❌ $name — expected HTTP $expected_status, got $status"
        FAIL=$((FAIL + 1))
        return
    fi

    if [ -n "$expected_content" ] && ! echo "$body_response" | grep -q "$expected_content"; then
        echo "  ❌ $name — response missing: $expected_content"
        FAIL=$((FAIL + 1))
        return
    fi

    echo "  ✅ $name"
    PASS=$((PASS + 1))
}

echo "StreamQL / ksqlDB Compatibility Test Suite"
echo "Endpoint: $STREAMQL_URL"
echo "==========================================="
echo ""

# ── Server Info ──────────────────────────────────────────
echo "📋 Server Info"
run_test "GET /info returns server info" \
    "GET" "/info" "" "200" "KsqlServerInfo"
run_test "GET /info contains version" \
    "GET" "/info" "" "200" "version"
run_test "GET /healthcheck returns healthy" \
    "GET" "/healthcheck" "" "200" "isHealthy"

# ── SQL Execution ────────────────────────────────────────
echo ""
echo "🔍 SQL Execution"
run_test "POST /sql with empty body returns 400" \
    "POST" "/sql" '{"ksql": ""}' "400" "error"
run_test "SHOW TOPICS returns success" \
    "POST" "/sql" '{"ksql": "SHOW TOPICS;"}' "200" "SUCCESS"
run_test "SHOW STREAMS returns success" \
    "POST" "/sql" '{"ksql": "SHOW STREAMS;"}' "200" "SUCCESS"
run_test "LIST TOPICS returns success" \
    "POST" "/sql" '{"ksql": "LIST TOPICS;"}' "200" "SUCCESS"
run_test "SELECT query returns success" \
    "POST" "/sql" '{"ksql": "SELECT * FROM events LIMIT 10;"}' "200" "SUCCESS"
run_test "CREATE STREAM returns success" \
    "POST" "/sql" '{"ksql": "CREATE STREAM user_events (id INT, name VARCHAR) WITH (kafka_topic='\''users'\'', value_format='\''JSON'\'');"}' "200" "SUCCESS"
run_test "CREATE TABLE returns success" \
    "POST" "/sql" '{"ksql": "CREATE TABLE user_counts AS SELECT name, COUNT(*) FROM user_events GROUP BY name;"}' "200" "SUCCESS"
run_test "DROP STREAM returns success" \
    "POST" "/sql" '{"ksql": "DROP STREAM user_events;"}' "200" "SUCCESS"

# ── Materialized Views ───────────────────────────────────
echo ""
echo "📊 Materialized Views"
run_test "CREATE MATERIALIZED VIEW returns success" \
    "POST" "/sql" '{"ksql": "CREATE MATERIALIZED VIEW order_totals AS SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id;"}' "200" "SUCCESS"
run_test "SHOW VIEWS returns success" \
    "POST" "/sql" '{"ksql": "SHOW VIEWS;"}' "200" "SUCCESS"
run_test "LIST VIEWS returns success" \
    "POST" "/sql" '{"ksql": "LIST VIEWS;"}' "200" "SUCCESS"
run_test "GET /api/v1/views returns view list" \
    "GET" "/api/v1/views" "" "200" "views"

# ── Wire Format Compatibility ─────────────────────────────
echo ""
echo "📦 Wire Format"
run_test "Response contains @type field" \
    "POST" "/sql" '{"ksql": "SHOW TOPICS;"}' "200" "@type"
run_test "Response contains statementText" \
    "POST" "/sql" '{"ksql": "SHOW TOPICS;"}' "200" "statementText"
run_test "Response contains commandStatus" \
    "POST" "/sql" '{"ksql": "SHOW TOPICS;"}' "200" "commandStatus"
run_test "Error response contains error_code" \
    "POST" "/sql" '{"ksql": "INVALID SYNTAX;"}' "400" "error_code"
run_test "streamsProperties field accepted" \
    "POST" "/sql" '{"ksql": "SHOW TOPICS;", "streamsProperties": {"auto.offset.reset": "earliest"}}' "200" "SUCCESS"

# ── Summary ──────────────────────────────────────────────
echo ""
echo "==========================================="
echo "Results: $PASS passed, $FAIL failed, $TOTAL total"
if [ "$FAIL" -eq 0 ]; then
    echo "🎉 All tests passed!"
    exit 0
else
    echo "⚠️  $FAIL test(s) failed"
    exit 1
fi
