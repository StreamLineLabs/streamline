#!/usr/bin/env bash
# docs-consistency-check.sh - Detect stale documentation claims
#
# Checks for known patterns that indicate documentation has fallen
# out of sync with the implementation. Run in CI or before releases.
#
# Usage:
#   ./scripts/docs-consistency-check.sh
#
# Exit codes:
#   0 = all checks pass
#   1 = stale documentation found

set -euo pipefail

ERRORS=0
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

red()   { printf '\033[0;31m%s\033[0m\n' "$*"; }
green() { printf '\033[0;32m%s\033[0m\n' "$*"; }
yellow(){ printf '\033[0;33m%s\033[0m\n' "$*"; }

check_pattern() {
    local description="$1"
    local pattern="$2"
    shift 2
    local paths=("$@")

    local matches
    matches=$(grep -rn --include="*.md" "$pattern" "${paths[@]}" 2>/dev/null || true)

    if [ -n "$matches" ]; then
        red "FAIL: $description"
        echo "$matches" | while IFS= read -r line; do
            echo "  $line"
        done
        echo ""
        ERRORS=$((ERRORS + 1))
    else
        green "PASS: $description"
    fi
}

echo "=== Documentation Consistency Check ==="
echo ""

# 1. Consumer groups should not be listed as "Planned" or "not yet supported"
check_pattern \
    "Consumer groups marked as Planned/not supported (they are implemented)" \
    "Consumer groups.*[Pp]lanned\|consumer groups.*coming soon\|Consumer groups.*not yet" \
    "$ROOT_DIR/docs" "$ROOT_DIR/README.md"

# 2. Transactions should not be listed as "Planned" (they are implemented as Beta)
#    Allow "Planned" only in the context of "Exactly-Once Semantics" which is legitimately planned
check_pattern \
    "Transactions marked as Planned (they are implemented as Beta)" \
    "| Transactions.*Planned" \
    "$ROOT_DIR/docs" "$ROOT_DIR/README.md"

# 3. Implemented APIs marked as "Planned" in migration docs
check_pattern \
    "Implemented APIs marked as Planned in tables" \
    "DeleteTopics.*Planned\|OffsetCommit.*Planned\|FindCoordinator.*Planned\|JoinGroup.*Planned\|InitProducerId.*Planned" \
    "$ROOT_DIR/docs"

# 4. Old tagline "AI-Native Streaming Platform" (should be "The Redis of Streaming")
check_pattern \
    "Old tagline 'AI-Native Streaming Platform' found" \
    "AI-Native Streaming Platform" \
    "$ROOT_DIR/sdks" "$ROOT_DIR/README.md" "$ROOT_DIR/docs"

# 5. GraphQL references in SDK READMEs (GraphQL API does not exist)
check_pattern \
    "GraphQL backend references in SDK docs (GraphQL not implemented)" \
    "GraphQL backend" \
    "$ROOT_DIR/sdks"

# 6. Old GitHub org URL pattern
check_pattern \
    "Old GitHub URL github.com/streamline/streamline (should be josedab/streamline)" \
    "github\.com/streamline/streamline" \
    "$ROOT_DIR/sdks" "$ROOT_DIR/README.md" "$ROOT_DIR/docs" "$ROOT_DIR/CONTRIBUTING.md"

# 7. SECURITY.md should list current version
if ! grep -q "0.2.x" "$ROOT_DIR/SECURITY.md" 2>/dev/null; then
    red "FAIL: SECURITY.md does not list 0.2.x as supported"
    ERRORS=$((ERRORS + 1))
else
    green "PASS: SECURITY.md lists current version"
fi

echo ""
echo "=== Summary ==="
if [ "$ERRORS" -gt 0 ]; then
    red "$ERRORS check(s) failed. Fix the stale documentation above."
    exit 1
else
    green "All documentation consistency checks passed."
    exit 0
fi
