#!/usr/bin/env bash
#
# Generate config template into docs/CONFIGURATION.md
#
# Usage:
#   ./scripts/generate-config-reference.sh [--release] [--check]
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

OUT_FILE="$ROOT_DIR/docs/CONFIGURATION.md"
BIN_DIR="$ROOT_DIR/target/$MODE"

echo "Building server binary ($MODE)..."
if [ "${#BUILD_FLAGS[@]}" -gt 0 ]; then
    cargo build "${BUILD_FLAGS[@]}" --bin streamline
else
    cargo build --bin streamline
fi

TMP_FILE="$(mktemp)"
{
    printf '```toml\n'
    "$BIN_DIR/streamline" --generate-config
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

start = "<!-- BEGIN GENERATED CONFIG -->"
end = "<!-- END GENERATED CONFIG -->"

contents = out_file.read_text()
if start not in contents or end not in contents:
    raise SystemExit("Missing generated config markers in docs/CONFIGURATION.md")

before, rest = contents.split(start, 1)
_, after = rest.split(end, 1)
snippet = tmp_file.read_text().rstrip()
replacement = f"{start}\n{snippet}\n{end}"
new_contents = before + replacement + after

if check:
    if new_contents != contents:
        raise SystemExit("docs/CONFIGURATION.md is out of date. Run scripts/generate-config-reference.sh")
else:
    out_file.write_text(new_contents)
    print(f"Wrote {out_file}")
PY

rm -f "$TMP_FILE"
