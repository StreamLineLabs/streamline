#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'USAGE'
Usage: scripts/publish-sdks.sh --version <version> [--publish] [--ref <git-ref>]

Dispatch the gated publish-sdks.yml workflow. The wrapper never edits an SDK
manifest, builds a package, tags a repository, or contacts a package registry.

Options:
  --version <version>  Bare release version expected in every SDK manifest.
  --dry-run            Dispatch a dry run (the default).
  --publish            Request a real publish; the workflow release gate and
                       credential preflight still run before any publication.
  --ref <git-ref>      Run the workflow from a specific branch or tag.
  -h, --help           Show this help.
USAGE
}

fail() {
    printf 'publish-sdks.sh: %s\n' "$*" >&2
    exit 2
}

version=""
dry_run=true
ref=""

while (($# > 0)); do
    case "$1" in
        --version)
            (($# >= 2)) || fail "--version requires a value"
            version="$2"
            shift 2
            ;;
        --dry-run)
            dry_run=true
            shift
            ;;
        --publish)
            dry_run=false
            shift
            ;;
        --ref)
            (($# >= 2)) || fail "--ref requires a value"
            ref="$2"
            shift 2
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        *)
            fail "unknown argument: $1"
            ;;
    esac
done

[[ -n "$version" ]] || fail "--version is required"
[[ "$version" != v* ]] || fail "--version must not include the leading 'v'"
command -v gh >/dev/null 2>&1 || fail "the GitHub CLI ('gh') is required"

gh_args=(
    workflow run publish-sdks.yml
    --field "version=$version"
    --field "dry_run=$dry_run"
)
if [[ -n "$ref" ]]; then
    gh_args+=(--ref "$ref")
fi

printf 'Dispatching publish-sdks.yml for version %s (dry_run=%s)\n' "$version" "$dry_run"
exec gh "${gh_args[@]}"
