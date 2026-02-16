#!/usr/bin/env bash
#
# Generate OpenAPI 3.0.3 specification from Streamline Rust sources
#
# This script parses src/server/*_api.rs and src/server/api.rs to extract
# route definitions and generates a comprehensive OpenAPI YAML spec.
#
# Usage:
#   ./scripts/generate-openapi.sh           # Generate spec
#   ./scripts/generate-openapi.sh --check   # Verify spec is up-to-date
#

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT="$ROOT_DIR/scripts/generate_openapi.py"

# Forward all arguments to the Python script
exec python3 "$SCRIPT" "$@"
