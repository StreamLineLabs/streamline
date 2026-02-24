#!/usr/bin/env bash
# v1.0 Readiness Checker
#
# Evaluates the V1_RELEASE_CRITERIA.md checkboxes programmatically.
# Run before tagging v1.0.0 to verify all criteria are met.
#
# Usage: ./check-v1-readiness.sh [--verbose]

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$SCRIPT_DIR/../.." && pwd)"
VERBOSE=false
[ "${1:-}" = "--verbose" ] && VERBOSE=true

PASS=0
FAIL=0
WARN=0

check() {
    local desc="$1"
    local result="$2"  # "pass", "fail", "warn"
    local detail="${3:-}"

    case "$result" in
        pass) echo "  ✅ $desc"; PASS=$((PASS+1)) ;;
        fail) echo "  ❌ $desc"; [ -n "$detail" ] && echo "     $detail"; FAIL=$((FAIL+1)) ;;
        warn) echo "  ⚠️  $desc"; [ -n "$detail" ] && echo "     $detail"; WARN=$((WARN+1)) ;;
    esac
}

echo ""
echo "  ═══════════════════════════════════════════"
echo "  Streamline v1.0 Readiness Check"
echo "  ═══════════════════════════════════════════"
echo ""

# ── 1. Stability Requirements ────────────────────────────────────────────────
echo "  1. STABILITY"
echo "  ─────────────"

STABLE_MODULES="server storage consumer protocol config error embedded"
for mod in $STABLE_MODULES; do
    if [ -d "$REPO/src/$mod" ] || [ -f "$REPO/src/${mod}.rs" ] || [ -f "$REPO/src/${mod}/mod.rs" ]; then
        check "Module '$mod' exists" pass
    else
        check "Module '$mod' exists" fail "Not found"
    fi
done

# Check for deprecated items in stable modules
DEP_COUNT=$(grep -r '#\[deprecated' $REPO/src/server/ $REPO/src/storage/ $REPO/src/consumer/ $REPO/src/protocol/ $REPO/src/config/ $REPO/src/error/ $REPO/src/embedded.rs 2>/dev/null | wc -l | tr -d ' ')
check "No deprecated items in stable modules ($DEP_COUNT found)" "$([ "$DEP_COUNT" -eq 0 ] && echo pass || echo warn)" ""

# ── 2. Quality Requirements ──────────────────────────────────────────────────
echo ""
echo "  2. QUALITY"
echo "  ──────────"

TEST_COUNT=$(find "$REPO/tests" -name '*.rs' 2>/dev/null | wc -l | tr -d ' ')
check "Integration tests ≥ 50 (found: $TEST_COUNT)" "$([ "$TEST_COUNT" -ge 50 ] && echo pass || echo fail)"

BENCH_COUNT=$(find "$REPO/benches" -name '*.rs' 2>/dev/null | wc -l | tr -d ' ')
check "Benchmarks ≥ 10 (found: $BENCH_COUNT)" "$([ "$BENCH_COUNT" -ge 10 ] && echo pass || echo fail)"

# ── 3. Documentation ─────────────────────────────────────────────────────────
echo ""
echo "  3. DOCUMENTATION"
echo "  ────────────────"

REQUIRED_DOCS="docs/API_STABILITY.md docs/V1_RELEASE_CRITERIA.md docs/LAUNCH_CHECKLIST.md CHANGELOG.md SECURITY.md RELEASING.md README.md LICENSE"
for doc in $REQUIRED_DOCS; do
    if [ -f "$REPO/$doc" ]; then
        check "$doc" pass
    else
        check "$doc" fail "File not found"
    fi
done

# ── 4. Security ───────────────────────────────────────────────────────────────
echo ""
echo "  4. SECURITY"
echo "  ───────────"

if command -v cargo-audit >/dev/null 2>&1; then
    AUDIT=$(cd "$REPO" && cargo audit 2>&1 | tail -1)
    if echo "$AUDIT" | grep -q "0 vulnerabilities"; then
        check "cargo-audit: no vulnerabilities" pass
    else
        check "cargo-audit" warn "$AUDIT"
    fi
else
    check "cargo-audit installed" warn "Install with: cargo install cargo-audit"
fi

UNSAFE_COUNT=$(grep -r 'unsafe ' "$REPO/src/" --include='*.rs' 2>/dev/null | grep -v '// SAFETY:' | grep -v '#\[' | grep -v 'test' | wc -l | tr -d ' ')
check "Unsafe blocks without SAFETY comment: $UNSAFE_COUNT" "$([ "$UNSAFE_COUNT" -le 5 ] && echo pass || echo warn)"

# ── 5. CI/CD ──────────────────────────────────────────────────────────────────
echo ""
echo "  5. CI/CD"
echo "  ────────"

REQUIRED_WORKFLOWS="release.yml release-gate.yml compatibility.yml sdk-conformance.yml publish-sdks.yml lts.yml publish-benchmarks.yml"
for wf in $REQUIRED_WORKFLOWS; do
    if [ -f "$REPO/.github/workflows/$wf" ]; then
        check "Workflow: $wf" pass
    else
        check "Workflow: $wf" fail "Missing"
    fi
done

# ── 6. Infrastructure ─────────────────────────────────────────────────────────
echo ""
echo "  6. INFRASTRUCTURE"
echo "  ─────────────────"

CONNECTOR_COUNT=$(grep -c 'ConnectorEntry {' "$REPO/src/connect/hub.rs" 2>/dev/null || echo 0)
check "Connector catalog ≥ 20 (found: $CONNECTOR_COUNT)" "$([ "$CONNECTOR_COUNT" -ge 20 ] && echo pass || echo fail)"

CONFORMANCE_TESTS=$(grep -c '| [A-Z][0-9]' "$REPO/tests/cross_sdk/CONFORMANCE_SPEC.md" 2>/dev/null || echo 0)
check "Conformance spec ≥ 40 tests (found: $CONFORMANCE_TESTS)" "$([ "$CONFORMANCE_TESTS" -ge 40 ] && echo pass || echo fail)"

MODULE_COUNT=$(grep -c 'pub mod\|pub(crate) mod' "$REPO/src/lib.rs" 2>/dev/null || echo 0)
check "Core modules ≥ 50 (found: $MODULE_COUNT)" "$([ "$MODULE_COUNT" -ge 50 ] && echo pass || echo fail)"

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "  ═══════════════════════════════════════════"
TOTAL=$((PASS+FAIL+WARN))
echo "  Results: $PASS/$TOTAL passed, $FAIL failed, $WARN warnings"
echo ""

if [ "$FAIL" -eq 0 ]; then
    echo "  ✅ v1.0 READY — all required checks passed"
    echo ""
    echo "  To release:"
    echo "    git tag -a v1.0.0 -m 'Release v1.0.0'"
    echo "    git push origin v1.0.0"
    exit 0
else
    echo "  ❌ v1.0 NOT READY — $FAIL check(s) failed"
    echo ""
    echo "  Fix the failing items before releasing."
    exit 1
fi
