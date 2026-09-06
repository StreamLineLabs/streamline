#!/usr/bin/env bash
#
# Fail a release *before* it publishes anything, when a credential it will need
# later is missing.
#
# `publish-sdks.yml` publishes to crates.io, npm, PyPI, Maven Central and NuGet
# and then pushes the Go SDK's release tag. Every one of those is irrevocable,
# and the Go tag comes last. A credential problem discovered there leaves a
# release that is five-sixths shipped, cannot be rolled back, and cannot be
# completed by re-running the workflow either — the missing secret is a
# configuration change, not a transient failure.
#
# So the credentials that only *later* steps use are checked here, in a job that
# every publishing job depends on. This costs one runner-minute and converts an
# unrecoverable half-release into a clean "nothing happened".
#
# Nothing in this script prints a secret. Values are only ever tested for
# emptiness.
#
# Usage:
#   scripts/release/require-release-credentials.sh --mode publish|dry-run
#
# Environment:
#   GO_SDK_RELEASE_TOKEN  Cross-repository token for the Go SDK release tag.
#                         Required when --mode publish.

set -euo pipefail

MODE=""

while [ $# -gt 0 ]; do
  case "$1" in
    --mode)
      MODE="${2:-}"
      shift 2
      ;;
    *)
      echo "::error::unknown argument '$1'" >&2
      exit 2
      ;;
  esac
done

case "$MODE" in
  publish | dry-run) ;;
  *)
    echo "::error::--mode must be 'publish' or 'dry-run' (got '${MODE}')" >&2
    exit 2
    ;;
esac

log() { echo "$*" >&2; }

missing=()

# Record whether one credential is present. The value is passed in by the
# caller and never echoed — only its emptiness is reported.
require_credential() {
  local name="$1" value="$2" purpose="$3"
  if [ -z "$value" ]; then
    echo "::error::${name} is not configured. ${purpose}" >&2
    missing+=("$name")
  else
    log "  ${name}: configured"
  fi
}

if [ "$MODE" = "dry-run" ]; then
  # A dry run publishes nothing, so it must stay runnable on a fork or by a
  # maintainer who has no publishing secrets at all. Requiring them here would
  # make the one safe way to rehearse a release the hardest to run.
  log "dry run: no publishing credentials are required"
  exit 0
fi

require_credential \
  "GO_SDK_RELEASE_TOKEN" \
  "${GO_SDK_RELEASE_TOKEN:-}" \
  "A Go module's published version *is* its git tag, so releasing the Go SDK means pushing a tag to streamlinelabs/streamline-go-sdk — another repository. The workflow's automatic GITHUB_TOKEN is scoped to this repository and is granted only 'contents: read' here, so it cannot create that tag. Configure a fine-grained PAT or GitHub App installation token with 'contents: write' on streamlinelabs/streamline-go-sdk as the GO_SDK_RELEASE_TOKEN repository secret. See RELEASING.md."

if [ ${#missing[@]} -gt 0 ]; then
  echo "::error::refusing to start a release that cannot finish: ${missing[*]} not configured. Nothing has been published; configure the secret and re-run." >&2
  if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    {
      echo "### Release credential preflight — FAILED"
      echo ""
      echo "Missing repository secret(s): \`${missing[*]}\`"
      echo ""
      echo "No package was published. Configure the secret(s) and re-run."
    } >> "$GITHUB_STEP_SUMMARY"
  fi
  exit 1
fi

log "all release credentials are configured"

if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  {
    echo "### Release credential preflight"
    echo ""
    echo "All cross-repository release credentials are configured."
  } >> "$GITHUB_STEP_SUMMARY"
fi
