#!/usr/bin/env bash
#
# Verify that a checked-out SDK repository really is at the version a release is
# publishing, before anything is built or pushed to a registry.
#
# ── Why ────────────────────────────────────────────────────────────────────
#
# `.github/workflows/publish-sdks.yml` takes a `version` input and used it only
# to tag the Go module and to print a summary. Every other job checked out the
# SDK's default branch and published *whatever version that branch happened to
# declare*. A release of 0.4.1 could therefore publish 0.4.0 artefacts to npm,
# PyPI, Maven Central and NuGet and report success.
#
# This script is the single place that knows where each ecosystem keeps its
# authoritative version, so the workflow cannot drift per job and dry runs
# perform exactly the same check as real runs.
#
# Every check is fail-closed: a missing file, an unparsable file, zero matches
# or more than one conflicting match is an error, never an implicit pass. No
# ecosystem is allowed to claim equality without reading a real artefact.
#
# Usage:
#   scripts/release/verify-sdk-version.sh \
#     --ecosystem rust|node|python|java|dotnet|go|kotlin|wasm \
#     --dir <checkout directory> \
#     --expected <version>
#
# The SDK repositories are checked out into a subdirectory (see
# publish-sdks.yml) so that this repository's release-control scripts stay on
# disk; nothing here writes to the SDK checkout.

set -euo pipefail

ECOSYSTEM=""
DIR=""
EXPECTED=""

while [ $# -gt 0 ]; do
  case "$1" in
    --ecosystem)
      ECOSYSTEM="${2:-}"
      shift 2
      ;;
    --dir)
      DIR="${2:-}"
      shift 2
      ;;
    --expected)
      EXPECTED="${2:-}"
      shift 2
      ;;
    *)
      echo "::error::unknown argument '$1'" >&2
      exit 2
      ;;
  esac
done

for required in ECOSYSTEM DIR EXPECTED; do
  if [ -z "${!required}" ]; then
    echo "::error::--${required,,} is required" >&2
    exit 2
  fi
done

if [ ! -d "$DIR" ]; then
  echo "::error::SDK checkout '${DIR}' does not exist; refusing to claim its version matches ${EXPECTED}" >&2
  exit 1
fi

fail() {
  echo "::error::$*" >&2
  exit 1
}

need_file() {
  local path="$1" what="$2"
  [ -f "$path" ] || fail "${ECOSYSTEM} SDK: expected ${what} at '${path}', which does not exist. Without it there is no evidence the checkout is at ${EXPECTED}."
  printf '%s' "$path"
}

# Emit one `source|version` record.
emit() { printf '%s|%s\n' "$1" "$2"; }

# ── format readers ──────────────────────────────────────────────────────────

# `version = "x.y.z"` from the `[package]` table of a Cargo manifest.
#
# Read with a real TOML parser: a Cargo manifest can carry `version` keys in
# `[dependencies.*]`, `[workspace.package]` and `[profile.*]`, and picking the
# wrong one would let a mismatched SDK publish.
toml_package_version() {
  local path="$1"
  python3 - "$path" <<'PY'
import sys, tomllib
with open(sys.argv[1], "rb") as handle:
    data = tomllib.load(handle)
package = data.get("package") or {}
version = package.get("version")
if isinstance(version, dict):
    # `version.workspace = true` — the real value lives in the workspace root.
    version = ((data.get("workspace") or {}).get("package") or {}).get("version")
print(version if isinstance(version, str) else "")
PY
}

# `version = "x.y.z"` from the `[project]` (PEP 621) or `[tool.poetry]` table.
pyproject_version() {
  local path="$1"
  python3 - "$path" <<'PY'
import sys, tomllib
with open(sys.argv[1], "rb") as handle:
    data = tomllib.load(handle)
for table in (("project", "version"), ("tool", "poetry", "version")):
    node = data
    for key in table:
        if not isinstance(node, dict) or key not in node:
            node = None
            break
        node = node[key]
    if isinstance(node, str):
        print(node)
        break
PY
}

# The importable package directories the wheel ships, from whichever build
# backend the project uses. Falls back to the normalised project name.
pyproject_wheel_packages() {
  local path="$1"
  python3 - "$path" <<'PY'
import sys, tomllib
with open(sys.argv[1], "rb") as handle:
    data = tomllib.load(handle)

def dig(*keys):
    node = data
    for key in keys:
        if not isinstance(node, dict) or key not in node:
            return None
        node = node[key]
    return node

packages = (
    dig("tool", "hatch", "build", "targets", "wheel", "packages")
    or dig("tool", "setuptools", "packages")
    or dig("tool", "poetry", "packages")
)
names = []
for entry in packages or []:
    if isinstance(entry, str):
        names.append(entry)
    elif isinstance(entry, dict) and isinstance(entry.get("include"), str):
        names.append(entry["include"])
if not names:
    project = dig("project", "name") or dig("tool", "poetry", "name") or ""
    if project:
        names.append(project.replace("-", "_"))
for name in names:
    print(name.rstrip("/"))
PY
}

# The `<project>`-level `<version>`, i.e. the coordinates this POM publishes
# under — never a `<parent>`, `<dependency>` or plugin version.
pom_project_version() {
  local path="$1"
  python3 - "$path" <<'PY'
import sys, xml.etree.ElementTree as ET
root = ET.parse(sys.argv[1]).getroot()
ns = ""
if root.tag.startswith("{"):
    ns = root.tag[: root.tag.index("}") + 1]
version = root.find(f"{ns}version")
if version is None or not (version.text or "").strip():
    # Maven inherits the version from the parent when the module omits it.
    parent = root.find(f"{ns}parent")
    if parent is not None:
        version = parent.find(f"{ns}version")
print((version.text or "").strip() if version is not None else "")
PY
}

# An MSBuild property value from a props/csproj file.
msbuild_property() {
  local path="$1" property="$2"
  python3 - "$path" "$property" <<'PY'
import sys, xml.etree.ElementTree as ET
root = ET.parse(sys.argv[1]).getroot()
wanted = sys.argv[2]
values = []
for group in root.iter():
    tag = group.tag.split("}")[-1]
    if tag == wanted and (group.text or "").strip():
        values.append(group.text.strip())
print(values[0] if values else "")
PY
}

# Exactly one line matching `regex`, with capture group 1 as the version.
single_capture() {
  local path="$1" regex="$2" what="$3"
  local matches
  matches=$(sed -nE "s/${regex}/\1/p" "$path")
  local count
  count=$(printf '%s' "$matches" | grep -c . || true)
  if [ "$count" -eq 0 ]; then
    fail "${ECOSYSTEM} SDK: no ${what} found in '${path}'; cannot verify the checkout is at ${EXPECTED}."
  fi
  if [ "$count" -gt 1 ]; then
    fail "${ECOSYSTEM} SDK: ${count} conflicting ${what} declarations in '${path}': $(echo "$matches" | tr '\n' ' '). Refusing to pick one."
  fi
  printf '%s' "$matches"
}

# ── per-ecosystem authoritative sources ─────────────────────────────────────

collect_versions() {
  case "$ECOSYSTEM" in
    rust)
      # Published to crates.io straight from the manifest.
      local manifest
      manifest=$(need_file "${DIR}/Cargo.toml" "the crate manifest")
      emit "$manifest" "$(toml_package_version "$manifest")"
      ;;

    node)
      # `npm publish` uses package.json#version verbatim.
      local pkg
      pkg=$(need_file "${DIR}/package.json" "the npm manifest")
      emit "$pkg" "$(jq -er '.version' < "$pkg")"
      ;;

    python)
      # The wheel takes its version from pyproject; the runtime `__version__`
      # is what users see, so both must agree.
      local pyproject
      pyproject=$(need_file "${DIR}/pyproject.toml" "the project manifest")
      emit "$pyproject" "$(pyproject_version "$pyproject")"

      # Only the packages this project actually ships. The repository also
      # contains a separate testcontainers distribution with its own release
      # cadence; folding it in here would block a valid SDK release on an
      # unrelated package, and asserting on it would be a claim this job has no
      # authority to make.
      local package init found=0
      while IFS= read -r package; do
        init="${DIR}/${package}/__init__.py"
        [ -f "$init" ] || fail "python SDK: '${pyproject}' declares wheel package '${package}', but '${init}' does not exist."
        grep -q '^__version__' "$init" || fail "python SDK: '${init}' declares no '__version__'; the importable package must be pinned to the published version."
        found=1
        emit "$init" "$(single_capture "$init" '^__version__[[:space:]]*=[[:space:]]*"([^"]+)".*$' '__version__ assignment')"
      done < <(pyproject_wheel_packages "$pyproject")
      if [ "$found" -eq 0 ]; then
        fail "python SDK: could not determine which packages '${pyproject}' ships, so '__version__' could not be checked."
      fi
      ;;

    java)
      # Maven Central coordinates come from the reactor POMs. Every module must
      # agree with the parent, or `mvn deploy` uploads mixed versions.
      local root_pom
      root_pom=$(need_file "${DIR}/pom.xml" "the reactor POM")
      emit "$root_pom" "$(pom_project_version "$root_pom")"

      local module
      while IFS= read -r module; do
        [ -f "${DIR}/${module}/pom.xml" ] || continue
        emit "${DIR}/${module}/pom.xml" "$(pom_project_version "${DIR}/${module}/pom.xml")"
      done < <(python3 - "$root_pom" <<'PY'
import sys, xml.etree.ElementTree as ET
root = ET.parse(sys.argv[1]).getroot()
ns = root.tag[: root.tag.index("}") + 1] if root.tag.startswith("{") else ""
modules = root.find(f"{ns}modules")
for module in (modules or []):
    if (module.text or "").strip():
        print(module.text.strip())
PY
      )
      ;;

    dotnet)
      # `dotnet pack` reads the NuGet package version from this property; the
      # repository's own Directory.Build.props asserts every packable project
      # matches it.
      local props
      props=$(need_file "${DIR}/Directory.Build.props" "the shared MSBuild properties")
      local version
      version=$(msbuild_property "$props" "StreamlinePackageVersion")
      if [ -z "$version" ]; then
        version=$(msbuild_property "$props" "Version")
      fi
      emit "$props" "$version"
      ;;

    go)
      # A Go module has no manifest version — `go.mod` records the module path
      # and the language version only, and the published version *is* the git
      # tag. This SDK therefore carries an in-repo constant, and validates its
      # own release tag against it.
      #
      # Both are checked here: the constant (evidence the checkout is at the
      # release) and the tag state (evidence the tag this workflow is about to
      # create does not already point somewhere else).
      local version_go
      version_go=$(need_file "${DIR}/streamline/version.go" "the SDK version constant")
      emit "$version_go" "$(single_capture "$version_go" '^const Version = "([^"]+)".*$' 'Version constant')"

      verify_go_tag_state
      ;;

    kotlin)
      # The Gradle publication uses the root project's `version`.
      local build_file
      build_file=$(need_file "${DIR}/build.gradle.kts" "the Gradle build script")
      emit "$build_file" "$(single_capture "$build_file" '^version[[:space:]]*=[[:space:]]*"([^"]+)".*$' 'project version')"
      ;;

    wasm)
      # Published to npm from `pkg/`, which wasm-pack generates from *both*
      # manifests, so both have to be right.
      local cargo pkg
      cargo=$(need_file "${DIR}/Cargo.toml" "the crate manifest")
      emit "$cargo" "$(toml_package_version "$cargo")"
      pkg=$(need_file "${DIR}/package.json" "the npm manifest")
      emit "$pkg" "$(jq -er '.version' < "$pkg")"
      ;;

    *)
      fail "unknown ecosystem '${ECOSYSTEM}'. Add it here rather than skipping the check."
      ;;
  esac
}

# Fail if `v<expected>` already exists in the SDK checkout and points at a
# different commit than the one being released.
#
# This is the "cannot claim equality without evidence" part for Go: retagging is
# how a module proxy ends up serving one tree under two versions.
verify_go_tag_state() {
  local tag="v${EXPECTED}"
  local toplevel resolved
  # `git -C <dir>` walks *up* to the nearest repository, so a checkout that is
  # not itself a git repository would otherwise be validated against whatever
  # repository happens to contain it — including this one. Require the
  # directory to be the repository root.
  toplevel=$(git -C "$DIR" rev-parse --show-toplevel 2> /dev/null || true)
  resolved=$(cd "$DIR" && pwd -P)
  if [ -z "$toplevel" ] || [ "$(cd "$toplevel" && pwd -P)" != "$resolved" ]; then
    fail "go SDK: '${DIR}' is not the root of a git checkout, so the release tag ${tag} cannot be validated. A Go module's published version *is* its tag."
  fi

  local head
  head=$(git -C "$DIR" rev-parse HEAD)

  if git -C "$DIR" rev-parse -q --verify "refs/tags/${tag}" > /dev/null 2>&1; then
    local tagged
    tagged=$(git -C "$DIR" rev-list -n 1 "refs/tags/${tag}")
    if [ "$tagged" != "$head" ]; then
      fail "go SDK: tag ${tag} already exists at ${tagged}, but the checkout is at ${head}. Publishing would either fail or silently serve a different tree for ${EXPECTED}."
    fi
    echo "go SDK: tag ${tag} already exists at the checked-out commit — publishing is a no-op for it" >&2
  else
    echo "go SDK: tag ${tag} does not exist yet; it will be created from ${head}" >&2
  fi
}

# ── compare ─────────────────────────────────────────────────────────────────

records=$(collect_versions)

if [ -z "$records" ]; then
  fail "${ECOSYSTEM} SDK: no version sources were read from '${DIR}'. A check that reads nothing cannot prove anything."
fi

mismatches=""
checked=0
while IFS='|' read -r source found; do
  [ -n "$source" ] || continue
  checked=$((checked + 1))
  if [ -z "$found" ]; then
    mismatches+=$'\n'"  ${source}: no version could be read"
  elif [ "$found" != "$EXPECTED" ]; then
    mismatches+=$'\n'"  ${source}: ${found}"
  else
    echo "ok: ${source} declares ${found}" >&2
  fi
done <<< "$records"

if [ "$checked" -eq 0 ]; then
  fail "${ECOSYSTEM} SDK: no version sources were read from '${DIR}'."
fi

if [ -n "$mismatches" ]; then
  fail "${ECOSYSTEM} SDK in '${DIR}' is not at ${EXPECTED}:${mismatches}

The release is publishing ${EXPECTED}, so the SDK repository must be checked
out at that release. Tag/branch the SDK repository first, or pass the version
the SDK is actually at."
fi

echo "${ECOSYSTEM} SDK: all ${checked} version source(s) in '${DIR}' declare ${EXPECTED}" >&2
