#!/usr/bin/env bash
# Stability-tier import-direction check.
#
# Streamline's stability tiers are documented in docs/API_STABILITY.md.
# This script enforces the unidirectional rule:
#
#   Stable  modules MUST NOT import Beta or Experimental modules.
#   Beta    modules MUST NOT import Experimental modules.
#
# Implementation:
# - Parse the "Stable" / "Beta" / "Experimental" tables in docs/API_STABILITY.md.
# - For each module in a strict tier, grep its source for `use crate::<weaker>`.
# - Compare the findings against the recorded baseline of known violations.
# - Exit non-zero on any violation that is not in the baseline, and on any
#   baseline entry that no longer corresponds to a real violation (so the
#   baseline can only ratchet downwards).
#
# Usage:
#   scripts/check_stability_tiers.sh              # enforce
#   scripts/check_stability_tiers.sh --write-baseline
#
# Portability note: the table parser deliberately uses POSIX `sed`/`grep` only.
# It previously used gawk's three-argument `match()`, which does not exist in
# mawk (the default awk on Ubuntu runners) or BSD awk (macOS). There it produced
# empty tier sets and the script exited 0 having performed no checks at all —
# a fail-open lint. Parsing failures are now hard errors.
#
# This is a Phase-1 lint — it is intentionally simple. Future hardening:
# - parse `pub use` re-exports
# - follow rename/import aliases
# - support fully-qualified paths
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC="$REPO_ROOT/src"
DOC="$REPO_ROOT/docs/API_STABILITY.md"
BASELINE="$REPO_ROOT/scripts/stability-tier-baseline.txt"

WRITE_BASELINE=0
if [[ "${1:-}" == "--write-baseline" ]]; then
  WRITE_BASELINE=1
fi

if [[ ! -f "$DOC" ]]; then
  echo "FATAL: $DOC not found"
  exit 2
fi

# Extract module names from one stability-tier table.
# Table format: | `module` | version | description |
extract_tier() {
  local heading="$1"
  sed -n "/^### ${heading}\$/,/^### /p" "$DOC" \
    | grep -E '^\| *`[^`]+` *\|' \
    | sed -E 's/^\| *`([^`]+)` *\|.*/\1/'
}

# `mapfile` is bash 4+ only; read in a loop so this also works on macOS bash 3.
read_tier_into() {
  local __name="$1"
  local __heading="$2"
  local __line
  eval "$__name=()"
  while IFS= read -r __line; do
    [[ -z "$__line" ]] && continue
    eval "$__name+=(\"\$__line\")"
  done < <(extract_tier "$__heading")
}

read_tier_into STABLE       "🟢 Stable"
read_tier_into BETA         "🟡 Beta"
read_tier_into EXPERIMENTAL "🔴 Experimental"

# Fail closed: if the document structure changed, the lint is not doing its job
# and must not report success.
if [[ ${#STABLE[@]} -eq 0 || ${#BETA[@]} -eq 0 || ${#EXPERIMENTAL[@]} -eq 0 ]]; then
  echo "FATAL: could not parse stability tiers from $DOC"
  echo "       parsed: stable=${#STABLE[@]} beta=${#BETA[@]} experimental=${#EXPERIMENTAL[@]}"
  echo "       Expected '### 🟢 Stable', '### 🟡 Beta' and '### 🔴 Experimental'"
  echo "       sections containing tables whose first column is a \`module\` name."
  exit 2
fi

join_alternation() {
  local out=""
  local item
  for item in "$@"; do
    if [[ -z "$out" ]]; then
      out="$item"
    else
      out="$out|$item"
    fi
  done
  printf '%s' "$out"
}

# A module's sources live at src/<name>.rs OR src/<name>/.
sources_for_module() {
  local m="$1"
  if [[ -d "$SRC/$m" ]]; then
    find "$SRC/$m" -name '*.rs' -type f | sort
  elif [[ -f "$SRC/$m.rs" ]]; then
    echo "$SRC/$m.rs"
  fi
}

# Emit "<tier-label> <relative/path.rs> <imported-module>" for every violation.
find_violations() {
  local label="$1"
  local module="$2"
  shift 2
  local forbidden_alt
  forbidden_alt="$(join_alternation "$@")"
  [[ -z "$forbidden_alt" ]] && return 0

  local files
  files="$(sources_for_module "$module")"
  [[ -z "$files" ]] && return 0

  # One grep per module rather than one per (module, forbidden) pair.
  # `-H` is required: without it grep omits the file name when the module has a
  # single source file (src/<name>.rs), and the parsing below would treat part
  # of the matched text as the path.
  local line file imported
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    file="${line%%:*}"
    imported="$(printf '%s' "${line#*:}" | sed -E 's/.*crate::([A-Za-z0-9_]+).*/\1/')"
    printf '%s %s %s\n' "$label" "${file#"$REPO_ROOT"/}" "$imported"
  done < <(printf '%s\n' "$files" | tr '\n' '\0' \
    | xargs -0 grep -HoE "use[[:space:]]+crate::(${forbidden_alt})(::|;)" 2>/dev/null || true)
}

collect_violations() {
  local m
  for m in "${STABLE[@]}"; do
    find_violations "stable-to-weaker" "$m" "${BETA[@]}" "${EXPERIMENTAL[@]}"
  done
  for m in "${BETA[@]}"; do
    find_violations "beta-to-experimental" "$m" "${EXPERIMENTAL[@]}"
  done
}

CURRENT="$(collect_violations | sort -u)"

if [[ "$WRITE_BASELINE" -eq 1 ]]; then
  {
    echo "# Stability-tier violation baseline — DO NOT ADD TO THIS FILE."
    echo "#"
    echo "# Format: <tier-rule> <path> <imported module>"
    echo "#"
    echo "# Each line is a pre-existing violation of the import-direction rule in"
    echo "# docs/API_STABILITY.md, recorded so the lint can fail closed on *new*"
    echo "# violations while the existing debt is paid down. Removing a line is"
    echo "# always allowed (and required once the import is gone); adding one is"
    echo "# not. Regenerate with: scripts/check_stability_tiers.sh --write-baseline"
    echo "#"
    echo "# Generated from docs/API_STABILITY.md tier tables."
    echo "$CURRENT"
  } > "$BASELINE"
  echo "Wrote baseline: $BASELINE"
  echo "Entries: $(printf '%s\n' "$CURRENT" | grep -c . || true)"
  exit 0
fi

if [[ ! -f "$BASELINE" ]]; then
  echo "FATAL: baseline $BASELINE not found."
  echo "       Generate it with: scripts/check_stability_tiers.sh --write-baseline"
  exit 2
fi

BASELINE_ENTRIES="$(grep -vE '^\s*(#|$)' "$BASELINE" | sort -u)"

# `comm` needs sorted, non-empty streams; `grep -v '^$'` drops the blank line
# printf produces for an empty variable.
NEW_VIOLATIONS="$(comm -23 \
  <(printf '%s\n' "$CURRENT" | grep -v '^$' | sort -u) \
  <(printf '%s\n' "$BASELINE_ENTRIES" | grep -v '^$' | sort -u))"
STALE_ENTRIES="$(comm -13 \
  <(printf '%s\n' "$CURRENT" | grep -v '^$' | sort -u) \
  <(printf '%s\n' "$BASELINE_ENTRIES" | grep -v '^$' | sort -u))"

status=0

if [[ -n "$(printf '%s' "$NEW_VIOLATIONS" | tr -d '[:space:]')" ]]; then
  echo "NEW stability-tier violations (not in baseline):"
  printf '%s\n' "$NEW_VIOLATIONS" | sed 's/^/  /'
  echo
  echo "A Stable module must not import a Beta/Experimental module, and a Beta"
  echo "module must not import an Experimental module."
  echo "See docs/API_STABILITY.md and ADR-0009 (feature-flag architecture)."
  status=1
fi

if [[ -n "$(printf '%s' "$STALE_ENTRIES" | tr -d '[:space:]')" ]]; then
  echo "STALE baseline entries (violation is gone — please remove them):"
  printf '%s\n' "$STALE_ENTRIES" | sed 's/^/  /'
  echo
  echo "Run: scripts/check_stability_tiers.sh --write-baseline"
  status=1
fi

if [[ $status -ne 0 ]]; then
  exit 1
fi

echo "Stability-tier check OK."
echo "Stable modules:        ${#STABLE[@]}"
echo "Beta modules:          ${#BETA[@]}"
echo "Experimental modules:  ${#EXPERIMENTAL[@]}"
echo "Baselined violations:  $(printf '%s\n' "$BASELINE_ENTRIES" | grep -c . || true)"
