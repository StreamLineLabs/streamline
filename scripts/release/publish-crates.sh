#!/usr/bin/env bash
#
# Publish the Streamline workspace crates to crates.io, resumably.
#
# A crates.io publish is irrevocable and the workspace publishes four crates in
# dependency order. If the run died after two of them — a runner timeout, a
# transient 5xx, a cancelled job — the previous version of this step could not
# be re-run: `cargo publish` fails with "crate version is already uploaded" and
# the whole workflow went red with two crates published and two not.
#
# This script makes the step idempotent:
#
#   1. Before each crate it asks crates.io whether that **exact** name@version
#      already exists. A different version of the same crate is never accepted
#      as evidence — that is how a half-finished release gets mistaken for a
#      complete one.
#   2. If it exists, the publish is skipped and reported.
#   3. Whether the version was just published or was already there, the script
#      waits for it to become *resolvable from the registry* before moving on to
#      a crate that depends on it. The old code slept a fixed 30 seconds, which
#      is both too long when the index is fast and too short when it is not.
#      That wait is a two-part proof, and neither part can be answered by the
#      checkout on disk — see `wait_for_visibility`.
#   4. Anything that is not a clean 200 (exists) or 404 (does not exist) — a
#      5xx, a proxy error, a curl failure, malformed JSON — aborts. Treating an
#      unreadable answer as "not published" would republish a crate; treating it
#      as "published" would skip one.
#
# The registry token is only ever read from the environment (cargo reads
# CARGO_REGISTRY_TOKEN itself) and is never echoed, never passed on a command
# line, and never sent to the API queries below, which are unauthenticated.
#
# Usage:
#   scripts/release/publish-crates.sh --mode publish|dry-run [--order "a b c"]
#
# Environment:
#   PUBLISH_ORDER               Default crate order when --order is omitted.
#   CRATES_IO_API               API base (default https://crates.io/api/v1/crates).
#   VISIBILITY_TIMEOUT_SECONDS  Bound on the registry-visibility wait (default 300).
#   VISIBILITY_POLL_SECONDS     Poll interval (default 5).
#   CARGO / CURL                Tool overrides, used by the hermetic tests.
#   CARGO_REGISTRY_TOKEN        Required for --mode publish.

set -euo pipefail

MODE=""
ORDER="${PUBLISH_ORDER:-}"
CRATES_IO_API="${CRATES_IO_API:-https://crates.io/api/v1/crates}"
VISIBILITY_TIMEOUT_SECONDS="${VISIBILITY_TIMEOUT_SECONDS:-300}"
VISIBILITY_POLL_SECONDS="${VISIBILITY_POLL_SECONDS:-5}"
CARGO="${CARGO:-cargo}"
CURL="${CURL:-curl}"
USER_AGENT="streamline-release-publisher (https://github.com/streamlinelabs/streamline)"

while [ $# -gt 0 ]; do
  case "$1" in
    --mode)
      MODE="${2:-}"
      shift 2
      ;;
    --order)
      ORDER="${2:-}"
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

if [ -z "$ORDER" ]; then
  echo "::error::no crate order supplied (--order or PUBLISH_ORDER)" >&2
  exit 2
fi

if ! command -v jq > /dev/null 2>&1; then
  echo "::error::jq is required to read the crates.io response safely" >&2
  exit 2
fi

# The registry probe runs cargo from a scratch directory (see
# `wait_for_visibility`), where a relative CARGO override would no longer
# resolve. Anchor it now, while the current directory still means what the
# caller intended.
case "$CARGO" in
  /*) ;;
  */*) CARGO="$(cd "$(dirname "$CARGO")" && pwd)/$(basename "$CARGO")" ;;
esac

log() { echo "$*" >&2; }

# The manifest version of a workspace crate.
#
# `cargo pkgid` emits either `…#name@1.2.3` or `…#1.2.3`, depending on whether
# the package name matches its directory, so take whatever follows the last
# `#` or `@`.
crate_version() {
  local crate="$1" pkgid
  if ! pkgid=$("$CARGO" pkgid --package "$crate" 2> /dev/null); then
    echo "::error::could not determine the version of '${crate}' with cargo pkgid" >&2
    return 1
  fi
  local version="${pkgid##*[#@]}"
  if ! printf '%s' "$version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+'; then
    echo "::error::cargo pkgid returned an unusable version for '${crate}': ${pkgid}" >&2
    return 1
  fi
  printf '%s' "$version"
}

# Echo `present` or `absent` for an exact name@version on crates.io.
#
# Any other outcome is a hard failure: silence about an unreadable registry is
# how a crate gets published twice or skipped.
registry_has_exact_version() {
  local crate="$1" version="$2"
  local body status
  body="$(mktemp)"
  # shellcheck disable=SC2064  # expand $body now, not at trap time
  trap "rm -f '$body'" RETURN

  if ! status=$("$CURL" --silent --show-error --location \
    --user-agent "$USER_AGENT" \
    --max-time 30 \
    --output "$body" \
    --write-out '%{http_code}' \
    "${CRATES_IO_API}/${crate}/${version}"); then
    echo "::error::crates.io query for ${crate}@${version} failed (curl error). Refusing to guess whether it is published." >&2
    return 1
  fi

  case "$status" in
    200)
      local reported
      if ! reported=$(jq -er '.version.num' < "$body" 2> /dev/null); then
        echo "::error::crates.io returned 200 for ${crate}@${version} but no .version.num field; refusing to guess." >&2
        return 1
      fi
      if [ "$reported" != "$version" ]; then
        # crates.io answered about a different release. That is never evidence
        # that *this* version is published.
        echo "::error::crates.io answered for ${crate}@${reported} when asked about ${crate}@${version}; refusing to treat another version as sufficient." >&2
        return 1
      fi
      printf 'present'
      ;;
    404)
      # A clean not-found is the only acceptable "not published yet" answer.
      printf 'absent'
      ;;
    *)
      echo "::error::crates.io returned HTTP ${status} for ${crate}@${version}; only 200 (published) and 404 (not published) are actionable." >&2
      return 1
      ;;
  esac
}

# A scratch directory that cargo cannot resolve a package from locally.
#
# `cargo info <crate>@<version>` consults the *current workspace* first. Every
# crate this script publishes is either a member of the workspace it is run
# from (publish-crate.yml, at the repository root) or the package in
# `working-directory: sdk` (publish-sdks.yml's standalone Rust SDK job), so run
# in place the probe answers instantly from the manifest on disk and proves
# nothing whatsoever about crates.io. The probe therefore runs from a freshly
# created empty directory with no ancestor manifest, where the registry is the
# only place an answer can come from.
#
# Echoes the directory. The caller owns it and must remove it.
new_registry_probe_dir() {
  local dir ancestor
  if ! dir=$(mktemp -d "${TMPDIR:-/tmp}/streamline-registry-probe.XXXXXX"); then
    echo "::error::could not create a scratch directory for the registry visibility probe" >&2
    return 1
  fi

  # Fail closed if the scratch directory turned out to be inside a cargo
  # workspace after all (a TMPDIR pointing into a checkout, for instance).
  # Probing from there would reintroduce exactly the local-manifest shortcut
  # this directory exists to avoid.
  ancestor="$dir"
  while :; do
    if [ -e "${ancestor}/Cargo.toml" ]; then
      rm -rf "$dir"
      echo "::error::the registry visibility probe directory (${dir}) is inside a cargo workspace rooted at ${ancestor}; cargo would answer from that manifest instead of the registry. Point TMPDIR outside any checkout." >&2
      return 1
    fi
    case "$ancestor" in
      / | .) break ;;
    esac
    ancestor="$(dirname "$ancestor")"
  done

  printf '%s' "$dir"
}

# Block until `crate@version` is visible on the registry, or fail.
#
# Visibility is proved twice, because the two consumers differ:
#
#   1. crates.io reports the **exact** name@version over its API. This is the
#      authoritative record of the publish and, unlike anything cargo does, it
#      cannot be satisfied by a local checkout. Transport errors, 5xx and an
#      answer about a different version abort the release (see
#      `registry_has_exact_version`) rather than being retried as "not yet".
#   2. cargo can resolve that exact version from the registry index, run from
#      an empty directory outside any workspace. This is the precondition the
#      *next* crate in PUBLISH_ORDER actually needs: `cargo publish` of a
#      dependent resolves its `{ path, version }` dependencies from the index,
#      which lags the API.
#
# Both are polled under one deadline, and each is attempted at least once.
wait_for_visibility() {
  local crate="$1" version="$2"
  local deadline=$((SECONDS + VISIBILITY_TIMEOUT_SECONDS))
  local attempt=0 state probe rc

  while :; do
    attempt=$((attempt + 1))
    # A hard failure here (unreadable registry) aborts; only a clean 404 is
    # retried.
    state="$(registry_has_exact_version "$crate" "$version")" || return 1
    if [ "$state" = "present" ]; then
      log "  ${crate}@${version} is published on crates.io (attempt ${attempt})"
      break
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "::error::crates.io still did not report ${crate}@${version} as published after ${VISIBILITY_TIMEOUT_SECONDS}s. Refusing to continue: crates later in PUBLISH_ORDER depend on it." >&2
      return 1
    fi
    log "  waiting for crates.io to report ${crate}@${version} as published (attempt ${attempt})"
    sleep "$VISIBILITY_POLL_SECONDS"
  done

  probe="$(new_registry_probe_dir)" || return 1
  attempt=0
  rc=1
  while :; do
    attempt=$((attempt + 1))
    # `cd` into the probe directory in a subshell so the script's own working
    # directory — a workspace — is never what cargo resolves against.
    if (cd "$probe" && "$CARGO" info "${crate}@${version}") > /dev/null 2>&1; then
      log "  ${crate}@${version} is resolvable from the registry index (attempt ${attempt})"
      rc=0
      break
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "::error::${crate}@${version} was still not resolvable after ${VISIBILITY_TIMEOUT_SECONDS}s. Crates later in PUBLISH_ORDER depend on it, so publishing them now would fail; re-run this workflow once the index has caught up." >&2
      break
    fi
    log "  waiting for ${crate}@${version} to appear in the registry index (attempt ${attempt})"
    sleep "$VISIBILITY_POLL_SECONDS"
  done

  rm -rf "$probe"
  return "$rc"
}

if [ "$MODE" = "publish" ] && [ -z "${CARGO_REGISTRY_TOKEN:-}" ]; then
  echo "::error::no crates.io token configured (set CARGO_REGISTRY_TOKEN, or CRATES_IO_TOKEN); refusing to publish." >&2
  exit 1
fi

published=()
skipped=()

for crate in $ORDER; do
  version="$(crate_version "$crate")"
  log "::group::${crate} ${version}"

  state="$(registry_has_exact_version "$crate" "$version")"

  if [ "$state" = "present" ]; then
    log "  ${crate}@${version} is already on crates.io — skipping publish"
    skipped+=("${crate}@${version}")
  elif [ "$MODE" = "dry-run" ]; then
    log "  ${crate}@${version} is not on crates.io — a real run would publish it"
    log "::endgroup::"
    continue
  else
    log "  publishing ${crate}@${version}"
    # The token stays in the environment; cargo reads it from there.
    "$CARGO" publish --locked --package "$crate"
    published+=("${crate}@${version}")
  fi

  if [ "$MODE" = "publish" ]; then
    # Both branches wait: a crate that was already published in an earlier,
    # failed run still has to be resolvable before its dependents go out.
    wait_for_visibility "$crate" "$version"
  fi

  log "::endgroup::"
done

if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  {
    echo "### crates.io publish (${MODE})"
    echo ""
    if [ ${#published[@]} -gt 0 ]; then
      echo "Published: ${published[*]}"
    fi
    if [ ${#skipped[@]} -gt 0 ]; then
      echo "Already present, skipped: ${skipped[*]}"
    fi
    if [ ${#published[@]} -eq 0 ] && [ ${#skipped[@]} -eq 0 ]; then
      echo "Nothing published."
    fi
  } >> "$GITHUB_STEP_SUMMARY"
fi

log "done: ${#published[@]} published, ${#skipped[@]} already present"
