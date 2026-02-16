#!/usr/bin/env bash
#
# Verify that core packages, SDKs, and docs agree on the current version.
#

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

version=$(grep -m 1 '^version = "' "$ROOT_DIR/Cargo.toml" | sed -E 's/version = "([^"]+)"/\1/')

if [[ -z "$version" ]]; then
  echo "Unable to read version from Cargo.toml"
  exit 1
fi

if ! grep -q "^## \\[$version\\]" "$ROOT_DIR/CHANGELOG.md"; then
  echo "CHANGELOG.md does not include version $version"
  exit 1
fi

docs_version=$(grep -m 1 'Documentation version:' "$ROOT_DIR/DOCS.md" | sed -E 's/.*Documentation version: ([0-9.]+).*/\1/')

if [[ "$docs_version" != "$version" ]]; then
  echo "DOCS.md version ($docs_version) does not match Cargo.toml ($version)"
  exit 1
fi

check_contains() {
  local file="$1"
  local pattern="$2"
  if ! grep -q "$pattern" "$file"; then
    echo "Version mismatch in $file (expected pattern: $pattern)"
    exit 1
  fi
}

# Core packages
check_contains "$ROOT_DIR/streamline-operator/Cargo.toml" "version = \"$version\""

# Helm and Homebrew
check_contains "$ROOT_DIR/helm/streamline/Chart.yaml" "version: $version"
check_contains "$ROOT_DIR/helm/streamline/Chart.yaml" "appVersion: \"$version\""
check_contains "$ROOT_DIR/homebrew/streamline.rb" "version \"$version\""

# Extensions
check_contains "$ROOT_DIR/extensions/vscode/package.json" "\"version\": \"$version\""

# SDK manifests
check_contains "$ROOT_DIR/sdks/rust/Cargo.toml" "version = \"$version\""
check_contains "$ROOT_DIR/sdks/nodejs/package.json" "\"version\": \"$version\""
check_contains "$ROOT_DIR/sdks/python/pyproject.toml" "version = \"$version\""
check_contains "$ROOT_DIR/sdks/python/streamline/__init__.py" "__version__ = \"$version\""
check_contains "$ROOT_DIR/sdks/python-sdk/pyproject.toml" "version = \"$version\""
check_contains "$ROOT_DIR/sdks/python-sdk/streamline_sdk/__init__.py" "__version__ = \"$version\""
check_contains "$ROOT_DIR/sdks/testcontainers-python/pyproject.toml" "version = \"$version\""
check_contains "$ROOT_DIR/sdks/testcontainers-python/streamline_testcontainers/__init__.py" "__version__ = \"$version\""
check_contains "$ROOT_DIR/sdks/dotnet/src/Streamline.Client/Streamline.Client.csproj" "<Version>$version</Version>"
check_contains "$ROOT_DIR/sdks/java/pom.xml" "<version>${version}-SNAPSHOT</version>"
check_contains "$ROOT_DIR/sdks/java/streamline-client/pom.xml" "<version>${version}-SNAPSHOT</version>"
check_contains "$ROOT_DIR/sdks/java/streamline-spring-boot-starter/pom.xml" "<version>${version}-SNAPSHOT</version>"
check_contains "$ROOT_DIR/sdks/testcontainers-java/pom.xml" "<version>$version</version>"

# Website docs
check_contains "$ROOT_DIR/website/docs/changelog.md" "## [$version]"
check_contains "$ROOT_DIR/website/docs/getting-started/quick-start.md" "Streamline v$version"
check_contains "$ROOT_DIR/website/static/openapi/streamline-api-v1.yaml" "version: \"$version\""

echo "Version sync OK: $version"
