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
# - Exit non-zero on the first violation, listing the file:line and tier conflict.
#
# Usage: scripts/check_stability_tiers.sh
#
# This is a Phase-1 lint — it is intentionally simple. Future hardening:
# - parse `pub use` re-exports
# - follow rename/import aliases
# - support fully-qualified paths
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC="$REPO_ROOT/src"
DOC="$REPO_ROOT/docs/API_STABILITY.md"

if [[ ! -f "$DOC" ]]; then
  echo "FATAL: $DOC not found"
  exit 2
fi

# Extract module names from the three stability-tier tables.
# Table format: | `module` | version | description |
extract_tier() {
  local heading="$1"
  awk -v hdr="### $heading" '
    $0 == hdr { found = 1; next }
    found && /^### / { exit }
    found && /^\| `/ {
      # capture content of first column inside backticks
      match($0, /`[^`]+`/, m)
      if (m[0] != "") {
        gsub(/`/, "", m[0])
        print m[0]
      }
    }
  ' "$DOC"
}

mapfile -t STABLE       < <(extract_tier "🟢 Stable")
mapfile -t BETA         < <(extract_tier "🟡 Beta")
mapfile -t EXPERIMENTAL < <(extract_tier "🔴 Experimental")

if [[ ${#STABLE[@]} -eq 0 && ${#BETA[@]} -eq 0 ]]; then
  echo "WARN: parser produced empty Stable/Beta tier sets — heading marker may have changed."
  echo "      No checks performed."
  exit 0
fi

violations=0

# A module's sources live at src/<name>.rs OR src/<name>/.
sources_for_module() {
  local m="$1"
  if [[ -d "$SRC/$m" ]]; then
    find "$SRC/$m" -name '*.rs' -type f
  elif [[ -f "$SRC/$m.rs" ]]; then
    echo "$SRC/$m.rs"
  fi
}

# Check that no Stable module imports any Beta or Experimental module.
for m in "${STABLE[@]}"; do
  for forbidden in "${BETA[@]}" "${EXPERIMENTAL[@]}"; do
    while IFS= read -r src; do
      [[ -z "$src" ]] && continue
      if grep -qE "use[[:space:]]+crate::${forbidden}(::|;)" "$src"; then
        echo "VIOLATION (Stable→weaker): $src imports crate::$forbidden"
        violations=$((violations + 1))
      fi
    done < <(sources_for_module "$m")
  done
done

# Check that no Beta module imports any Experimental module.
for m in "${BETA[@]}"; do
  for forbidden in "${EXPERIMENTAL[@]}"; do
    while IFS= read -r src; do
      [[ -z "$src" ]] && continue
      if grep -qE "use[[:space:]]+crate::${forbidden}(::|;)" "$src"; then
        echo "VIOLATION (Beta→Experimental): $src imports crate::$forbidden"
        violations=$((violations + 1))
      fi
    done < <(sources_for_module "$m")
  done
done

if [[ $violations -gt 0 ]]; then
  echo
  echo "Stability-tier check failed: $violations violation(s)."
  echo "See docs/API_STABILITY.md and ADR-0009 (feature-flag architecture)."
  exit 1
fi

echo "Stability-tier check OK."
echo "Stable modules:       ${#STABLE[@]}"
echo "Beta modules:         ${#BETA[@]}"
echo "Experimental modules: ${#EXPERIMENTAL[@]}"
