#!/usr/bin/env bash
#
# Generate CLI help reference into docs/CLI.md
#
# Usage:
#   ./scripts/generate-cli-reference.sh [--release] [--check]
#

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="debug"
BUILD_FLAGS=()
CHECK=false

for arg in "$@"; do
    case "$arg" in
        --release)
            MODE="release"
            BUILD_FLAGS=(--release)
            ;;
        --check)
            CHECK=true
            ;;
        *)
            echo "Unknown option: $arg"
            exit 1
            ;;
    esac
done

OUT_FILE="$ROOT_DIR/docs/CLI.md"
BIN_DIR="$ROOT_DIR/target/$MODE"

echo "Building CLI binaries ($MODE)..."
if [ "${#BUILD_FLAGS[@]}" -gt 0 ]; then
    cargo build "${BUILD_FLAGS[@]}" --bin streamline --bin streamline-cli
else
    cargo build --bin streamline --bin streamline-cli
fi

TMP_FILE="$(mktemp)"

{
    printf '```text\n'
    "$BIN_DIR/streamline" --help
    printf '```\n\n'
    printf '```text\n'
    "$BIN_DIR/streamline-cli" --help
    printf '```\n'
} > "$TMP_FILE"

export TMP_FILE
export OUT_FILE
export CHECK

python3 - <<'PY'
import os
from pathlib import Path

out_file = Path(os.environ["OUT_FILE"])
tmp_file = Path(os.environ["TMP_FILE"])
check = os.environ["CHECK"] == "true"

start = "<!-- BEGIN GENERATED HELP -->"
end = "<!-- END GENERATED HELP -->"

contents = out_file.read_text()
if start not in contents or end not in contents:
    raise SystemExit("Missing generated help markers in docs/CLI.md")

before, rest = contents.split(start, 1)
_, after = rest.split(end, 1)
snippet = tmp_file.read_text().rstrip()
replacement = f"{start}\n{snippet}\n{end}"
new_contents = before + replacement + after

if check:
    if new_contents != contents:
        raise SystemExit("docs/CLI.md is out of date. Run scripts/generate-cli-reference.sh")
else:
    out_file.write_text(new_contents)
    print(f"Wrote {out_file}")
PY

rm -f "$TMP_FILE"
