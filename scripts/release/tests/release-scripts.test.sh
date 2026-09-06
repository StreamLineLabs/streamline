#!/usr/bin/env bash
#
# Hermetic tests for the release helper scripts in scripts/release/.
#
# Nothing here touches the network, crates.io, a package registry or any sibling
# repository: `cargo` and `curl` are replaced by fakes on PATH that record what
# they were asked to do, and every SDK fixture is generated from the table in
# `make_sdk_fixture` below.
#
# Run directly, or through `cargo test --test release_workflow_test`, which
# executes this file so the suite runs in CI without a second harness.
#
# Scratch state lives under `target/` so it is gitignored and removed by
# `cargo clean`.

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
SCRIPTS="${REPO_ROOT}/scripts/release"
WORK="${REPO_ROOT}/target/release-script-tests"

RESOLVE_MODE="${SCRIPTS}/resolve-publish-mode.sh"
PUBLISH="${SCRIPTS}/publish-crates.sh"
VERIFY_SDK="${SCRIPTS}/verify-sdk-version.sh"
REQUIRE_CREDS="${SCRIPTS}/require-release-credentials.sh"

ORDER='streamline-serde-wincode streamline-wasm streamline-analytics streamline'
VERSION='0.4.0'
# A value that must never be echoed by anything under test.
FAKE_TOKEN='cio-SECRET-do-not-log-0123456789'

passed=0
failed=0
failures=()
LAST_OUT=""
LAST_STATUS=0

pass() {
  passed=$((passed + 1))
  printf '  ok   %s\n' "$1"
}

fail() {
  failed=$((failed + 1))
  failures+=("$1")
  printf '  FAIL %s\n     %s\n' "$1" "$2"
}

assert_eq() {
  local name="$1" expected="$2" actual="$3"
  if [ "$expected" = "$actual" ]; then
    pass "$name"
  else
    fail "$name" "expected: [${expected}] actual: [${actual}]"
  fi
}

assert_contains() {
  local name="$1" haystack="$2" needle="$3"
  case "$haystack" in
    *"$needle"*) pass "$name" ;;
    *) fail "$name" "expected output to contain [${needle}], got: ${haystack}" ;;
  esac
}

assert_not_contains() {
  local name="$1" haystack="$2" needle="$3"
  case "$haystack" in
    *"$needle"*) fail "$name" "output must not contain [${needle}]" ;;
    *) pass "$name" ;;
  esac
}

# ── fake tools ──────────────────────────────────────────────────────────────
#
# `cargo` and `curl` are the only external commands publish-crates.sh runs that
# reach the outside world. Both are replaced here so the tests are deterministic
# and can never publish anything.

install_fakes() {
  local bin="$1"
  mkdir -p "$bin"

  cat > "${bin}/cargo" <<'FAKE_CARGO'
#!/usr/bin/env bash
# Fake cargo. Records every invocation and answers from FAKE_STATE.
set -uo pipefail
printf '%s\n' "$*" >> "${FAKE_CARGO_LOG}"

case "${1:-}" in
  pkgid)
    # `cargo pkgid --package <crate>`
    crate="${3:-}"
    printf 'path+file:///fake/%s#%s@%s\n' "$crate" "$crate" "${FAKE_VERSION}"
    ;;
  publish)
    crate=""
    while [ $# -gt 0 ]; do
      [ "$1" = "--package" ] && crate="${2:-}"
      shift
    done
    if [ "${FAKE_PUBLISH_FAILS:-}" = "$crate" ]; then
      echo "error: fake publish failure for ${crate}" >&2
      exit 101
    fi
    # A real publish makes the version readable over the crates.io API and,
    # shortly afterwards, resolvable from the registry index.
    touch "${FAKE_STATE}/registry-${crate}-${FAKE_VERSION}"
    touch "${FAKE_STATE}/visible-${crate}-${FAKE_VERSION}"
    echo "Uploading ${crate} v${FAKE_VERSION}"
    ;;
  info)
    # `cargo info <crate>@<version>`.
    spec="${2:-}"
    crate="${spec%@*}"
    version="${spec##*@}"
    printf '%s\t%s\n' "$spec" "$PWD" >> "${FAKE_CARGO_INFO_LOG}"

    # Model cargo's local-workspace shortcut, which is the whole reason this
    # probe has to be run from somewhere else: inside a workspace, `cargo info`
    # resolves the spec from the manifests on disk and reports success
    # immediately, no matter what the registry contains. A probe run in place
    # is not a probe.
    dir="$PWD"
    while :; do
      if [ -e "${dir}/Cargo.toml" ]; then
        echo "${crate} @ ${version} (resolved from the local workspace at ${dir})"
        exit 0
      fi
      [ "$dir" = "/" ] && break
      dir="$(dirname "$dir")"
    done

    # Outside a workspace the only possible answer is the registry index.
    if [ "${FAKE_NEVER_VISIBLE:-0}" = "1" ]; then
      echo "error: could not find \`${crate}\` in registry" >&2
      exit 101
    fi
    if [ -f "${FAKE_STATE}/visible-${crate}-${version}" ]; then
      echo "${crate} @ ${version}"
      exit 0
    fi
    echo "error: could not find \`${crate}\` in registry" >&2
    exit 101
    ;;
  *)
    echo "fake cargo: unsupported subcommand '${1:-}'" >&2
    exit 2
    ;;
esac
FAKE_CARGO

  cat > "${bin}/curl" <<'FAKE_CURL'
#!/usr/bin/env bash
# Fake curl for the crates.io existence probe.
#
# Honours the subset of flags publish-crates.sh uses: --output and
# --write-out '%{http_code}'. Everything else is ignored.
set -uo pipefail

out=""
url=""
while [ $# -gt 0 ]; do
  case "$1" in
    --output) out="${2:-}"; shift 2 ;;
    --write-out) shift 2 ;;
    --user-agent | --max-time) shift 2 ;;
    --silent | --show-error | --location) shift ;;
    -*) shift ;;
    *) url="$1"; shift ;;
  esac
done

printf '%s\n' "$url" >> "${FAKE_CURL_LOG}"

# A token must never reach these unauthenticated queries.
if [ -n "${CARGO_REGISTRY_TOKEN:-}" ]; then
  case "$*" in
    *"${CARGO_REGISTRY_TOKEN}"*)
      echo "fake curl: registry token leaked into the API request" >&2
      exit 99
      ;;
  esac
fi

if [ "${FAKE_CURL_TRANSPORT_ERROR:-0}" = "1" ]; then
  echo "curl: (6) Could not resolve host" >&2
  exit 6
fi

version="${url##*/}"
rest="${url%/*}"
crate="${rest##*/}"

if [ -n "${FAKE_CURL_FORCE_STATUS:-}" ]; then
  printf '{"errors":[{"detail":"forced"}]}' > "$out"
  printf '%s' "${FAKE_CURL_FORCE_STATUS}"
  exit 0
fi

if [ -n "${FAKE_CURL_WRONG_VERSION:-}" ]; then
  printf '{"version":{"num":"%s"}}' "${FAKE_CURL_WRONG_VERSION}" > "$out"
  printf '200'
  exit 0
fi

if [ -f "${FAKE_STATE}/registry-${crate}-${version}" ]; then
  printf '{"version":{"num":"%s","crate":"%s"}}' "$version" "$crate" > "$out"
  printf '200'
else
  printf '{"errors":[{"detail":"Not Found"}]}' > "$out"
  printf '404'
fi
FAKE_CURL

  chmod +x "${bin}/cargo" "${bin}/curl"
}

# Fresh, isolated state for one scenario. Echoes the state directory.
#
# The scenario contains a `workspace/` directory with a manifest in it. Both
# real callers run publish-crates.sh from inside a cargo workspace — the
# repository root (publish-crate.yml) or the SDK checkout
# (`working-directory: sdk` in publish-sdks.yml) — and that is precisely the
# condition under which cargo answers a package query from local manifests, so
# the tests reproduce it rather than depending on wherever the suite started.
new_scenario() {
  local name="$1"
  local dir="${WORK}/${name}"
  rm -rf "$dir"
  mkdir -p "${dir}/state" "${dir}/workspace"
  cat > "${dir}/workspace/Cargo.toml" <<'FAKE_MANIFEST'
[package]
name = "streamline"
version = "0.4.0"
edition = "2021"
FAKE_MANIFEST
  install_fakes "${dir}/bin"
  : > "${dir}/cargo.log"
  : > "${dir}/cargo-info.log"
  : > "${dir}/curl.log"
  printf '%s' "$dir"
}

# Run publish-crates.sh inside a scenario.
#
# Sets LAST_OUT (merged stdout+stderr) and LAST_STATUS, so a test never has to
# rely on `$?` surviving an intervening assertion.
#
#   run_publish <scenario dir> <publish|dry-run> [VAR=value ...]
#
# `RUN_PUBLISH_CWD` overrides the working directory (default: the scenario
# workspace) and `RUN_PUBLISH_ORDER` the crate order, for the SDK scenario.
run_publish() {
  local dir="$1" mode="$2"
  shift 2
  # CARGO/CURL are set to absolute paths, not just placed first on PATH:
  # `cargo test` exports CARGO=<real cargo> and publish-crates.sh honours it, so
  # without this the suite would drive the real cargo — i.e. attempt a real
  # `cargo publish`.
  LAST_OUT=$(
    cd "${RUN_PUBLISH_CWD:-${dir}/workspace}" && env \
      PATH="${dir}/bin:${PATH}" \
      CARGO="${dir}/bin/cargo" \
      CURL="${dir}/bin/curl" \
      FAKE_STATE="${dir}/state" \
      FAKE_CARGO_LOG="${dir}/cargo.log" \
      FAKE_CARGO_INFO_LOG="${dir}/cargo-info.log" \
      FAKE_CURL_LOG="${dir}/curl.log" \
      FAKE_VERSION="$VERSION" \
      CRATES_IO_API='https://crates.io.invalid/api/v1/crates' \
      VISIBILITY_TIMEOUT_SECONDS="${VISIBILITY_TIMEOUT_SECONDS:-2}" \
      VISIBILITY_POLL_SECONDS="${VISIBILITY_POLL_SECONDS:-1}" \
      "$@" \
      bash "$PUBLISH" --mode "$mode" --order "${RUN_PUBLISH_ORDER:-$ORDER}" 2>&1
  )
  LAST_STATUS=$?
}

# Run any command, capturing merged output and status the same way.
run_capture() {
  LAST_OUT=$("$@" 2>&1)
  LAST_STATUS=$?
}

publish_log() { grep -c '^publish ' "$1/cargo.log" 2> /dev/null || true; }

published_order() {
  sed -n 's/^publish --locked --package \(.*\)$/\1/p' "$1/cargo.log" | tr '\n' ' ' | sed 's/ $//'
}

# Number of `cargo info` probes recorded, and the directories they ran in.
info_probe_count() { grep -c . "$1/cargo-info.log" 2> /dev/null || true; }
info_probe_dirs() { cut -f2 "$1/cargo-info.log" 2> /dev/null || true; }

# Echo the probe directories that are inside a cargo workspace — i.e. the ones
# where cargo would have answered from a manifest on disk instead of the
# registry. Any output at all is a bug.
info_probes_inside_a_workspace() {
  local probe dir
  while IFS= read -r probe; do
    [ -n "$probe" ] || continue
    dir="$probe"
    while :; do
      if [ -e "${dir}/Cargo.toml" ]; then
        printf '%s (workspace at %s)\n' "$probe" "$dir"
        break
      fi
      [ "$dir" = "/" ] && break
      dir="$(dirname "$dir")"
    done
  done < <(info_probe_dirs "$1")
}

# ── resolve-publish-mode.sh ─────────────────────────────────────────────────

test_resolve_publish_mode() {
  echo "resolve-publish-mode.sh"

  local out
  # A manual dispatch cannot publish, whatever it claims.
  out=$(env INVOCATION_EVENT=workflow_dispatch REQUESTED_DRY_RUN=false \
    CALLER_WORKFLOW_REF='o/r/.github/workflows/publish-crate.yml@refs/heads/main' \
    ALLOWED_PUBLISH_CALLERS='release.yml publish-sdks.yml' \
    bash "$RESOLVE_MODE" 2> /dev/null)
  assert_eq "manual dispatch with dry_run=false is still a dry run" "dry-run" "$out"

  out=$(env INVOCATION_EVENT=workflow_dispatch REQUESTED_DRY_RUN= \
    CALLER_WORKFLOW_REF='o/r/.github/workflows/publish-crate.yml@refs/heads/main' \
    ALLOWED_PUBLISH_CALLERS='release.yml publish-sdks.yml' \
    bash "$RESOLVE_MODE" 2> /dev/null)
  assert_eq "manual dispatch with no input is a dry run" "dry-run" "$out"

  # workflow_call still needs an explicit false.
  out=$(env INVOCATION_EVENT=workflow_call REQUESTED_DRY_RUN=true \
    CALLER_WORKFLOW_REF='o/r/.github/workflows/release.yml@refs/tags/v0.4.0' \
    ALLOWED_PUBLISH_CALLERS='release.yml publish-sdks.yml' \
    bash "$RESOLVE_MODE" 2> /dev/null)
  assert_eq "workflow_call with dry_run=true is a dry run" "dry-run" "$out"

  out=$(env INVOCATION_EVENT=workflow_call REQUESTED_DRY_RUN= \
    CALLER_WORKFLOW_REF='o/r/.github/workflows/release.yml@refs/tags/v0.4.0' \
    ALLOWED_PUBLISH_CALLERS='release.yml publish-sdks.yml' \
    bash "$RESOLVE_MODE" 2> /dev/null)
  assert_eq "workflow_call with an absent input is a dry run" "dry-run" "$out"

  # Only a gated caller may publish.
  out=$(env INVOCATION_EVENT=workflow_call REQUESTED_DRY_RUN=false \
    CALLER_WORKFLOW_REF='o/r/.github/workflows/release.yml@refs/tags/v0.4.0' \
    ALLOWED_PUBLISH_CALLERS='release.yml publish-sdks.yml' \
    bash "$RESOLVE_MODE" 2> /dev/null)
  assert_eq "gated caller with dry_run=false publishes" "publish" "$out"

  out=$(env INVOCATION_EVENT=workflow_call REQUESTED_DRY_RUN=false \
    CALLER_WORKFLOW_REF='o/r/.github/workflows/sneaky.yml@refs/heads/main' \
    ALLOWED_PUBLISH_CALLERS='release.yml publish-sdks.yml' \
    bash "$RESOLVE_MODE" 2> /dev/null)
  assert_eq "an ungated caller cannot publish" "dry-run" "$out"

  out=$(env INVOCATION_EVENT=workflow_call REQUESTED_DRY_RUN=false \
    CALLER_WORKFLOW_REF='o/r/.github/workflows/release.yml@refs/tags/v0.4.0' \
    ALLOWED_PUBLISH_CALLERS= \
    bash "$RESOLVE_MODE" 2>&1)
  assert_contains "an empty caller allowlist fails closed" "$out" "refusing to publish"

  run_capture env INVOCATION_EVENT=workflow_call REQUESTED_DRY_RUN=maybe \
    CALLER_WORKFLOW_REF='o/r/.github/workflows/release.yml@refs/tags/v0.4.0' \
    ALLOWED_PUBLISH_CALLERS='release.yml' \
    bash "$RESOLVE_MODE"
  assert_eq "a malformed dry_run value is an error" "1" "$LAST_STATUS"
  assert_contains "names the bad value" "$LAST_OUT" "invalid dry_run value"
}

# ── publish-crates.sh ───────────────────────────────────────────────────────

test_all_new_sequence() {
  echo "publish-crates.sh: nothing published yet"
  local dir
  dir=$(new_scenario "all-new")
  run_publish "$dir" publish CARGO_REGISTRY_TOKEN="$FAKE_TOKEN"
  assert_eq "exits successfully" "0" "$LAST_STATUS"
  assert_eq "publishes every crate in dependency order" "$ORDER" "$(published_order "$dir")"
  assert_contains "reports what it published" "$LAST_OUT" "publishing streamline-analytics@${VERSION}"
  assert_not_contains "never echoes the registry token" "$LAST_OUT" "$FAKE_TOKEN"
  # Once to decide whether to publish, once more to prove the publish landed:
  # the exact-version API is the only visibility check a local checkout cannot
  # answer, so it is re-asked after publishing rather than assumed.
  assert_eq "queries crates.io before and after publishing each crate" "8" \
    "$(wc -l < "${dir}/curl.log" | tr -d ' ')"
  assert_contains "confirms publication against the registry, not the manifest" "$LAST_OUT" \
    "streamline-analytics@${VERSION} is published on crates.io"
  assert_eq "probes the index once per crate" "4" "$(info_probe_count "$dir")"
}

# The defect this suite exists to prevent: `cargo info` consults the current
# workspace before the registry, so a probe run where the script runs is
# answered by the manifest on disk and succeeds instantly for every crate being
# released. The fake cargo above models that shortcut, so any probe that runs
# inside a workspace silently "passes" — and these assertions fail.
test_visibility_probe_runs_outside_the_workspace() {
  echo "publish-crates.sh: the visibility probe cannot be answered locally"
  local dir offenders
  dir=$(new_scenario "probe-location")
  run_publish "$dir" publish CARGO_REGISTRY_TOKEN="$FAKE_TOKEN"
  assert_eq "exits successfully" "0" "$LAST_STATUS"

  offenders="$(info_probes_inside_a_workspace "$dir")"
  assert_eq "no index probe runs inside a cargo workspace" "" "$offenders"

  # And specifically not in the directory the script itself was run from.
  assert_not_contains "the probe does not run in the publishing workspace" \
    "$(info_probe_dirs "$dir")" "${dir}/workspace"
  assert_contains "the probe reports index resolution" "$LAST_OUT" "resolvable from the registry index"
}

# `publish-sdks.yml` runs this same script with `working-directory: sdk`, where
# the standalone Rust SDK's own manifest is the thing cargo would answer from.
test_sdk_working_directory_probe() {
  echo "publish-crates.sh: standalone SDK checkout (working-directory: sdk)"
  local dir sdk
  dir=$(new_scenario "sdk-workdir")
  sdk="${dir}/workspace/sdk"
  mkdir -p "$sdk"
  cat > "${sdk}/Cargo.toml" <<'SDK_MANIFEST'
[package]
name = "streamline-client"
version = "0.4.0"
edition = "2021"
SDK_MANIFEST

  RUN_PUBLISH_CWD="$sdk" RUN_PUBLISH_ORDER='streamline-client' \
    run_publish "$dir" publish CARGO_REGISTRY_TOKEN="$FAKE_TOKEN"
  assert_eq "publishes the SDK crate" "0" "$LAST_STATUS"
  assert_eq "only the SDK crate is published" "streamline-client" "$(published_order "$dir")"
  assert_eq "the SDK probe is not answered by sdk/Cargo.toml" "" \
    "$(info_probes_inside_a_workspace "$dir")"

  # The same crate, never becoming visible in the index, must still fail —
  # exactly what the local manifest would have hidden.
  dir=$(new_scenario "sdk-workdir-never-visible")
  sdk="${dir}/workspace/sdk"
  mkdir -p "$sdk"
  cat > "${sdk}/Cargo.toml" <<'SDK_MANIFEST'
[package]
name = "streamline-client"
version = "0.4.0"
edition = "2021"
SDK_MANIFEST

  RUN_PUBLISH_CWD="$sdk" RUN_PUBLISH_ORDER='streamline-client' \
    run_publish "$dir" publish CARGO_REGISTRY_TOKEN="$FAKE_TOKEN" FAKE_NEVER_VISIBLE=1
  assert_eq "an SDK version that never appears in the index fails the job" "1" "$LAST_STATUS"
  assert_contains "names the timeout" "$LAST_OUT" "still not resolvable after 2s"
}

# A TMPDIR that points into a checkout would put the probe back inside a
# workspace. That must fail closed rather than quietly reintroduce the bug.
test_probe_dir_refuses_a_workspace_tmpdir() {
  echo "publish-crates.sh: a probe directory inside a workspace fails closed"
  local dir tmp
  dir=$(new_scenario "probe-tmpdir")
  tmp="${dir}/workspace/tmp"
  mkdir -p "$tmp"
  run_publish "$dir" publish CARGO_REGISTRY_TOKEN="$FAKE_TOKEN" TMPDIR="$tmp"
  assert_eq "refuses to probe from inside a workspace" "1" "$LAST_STATUS"
  assert_contains "says why" "$LAST_OUT" "inside a cargo workspace"
  assert_eq "stops at the first crate" "streamline-serde-wincode" "$(published_order "$dir")"
}

test_partial_rerun() {
  echo "publish-crates.sh: rerun after a partial publish"
  local dir
  dir=$(new_scenario "partial-rerun")
  # The first two crates went out before the previous run died.
  touch "${dir}/state/registry-streamline-serde-wincode-${VERSION}"
  touch "${dir}/state/registry-streamline-wasm-${VERSION}"
  # And they are already resolvable from the index.
  touch "${dir}/state/visible-streamline-serde-wincode-${VERSION}"
  touch "${dir}/state/visible-streamline-wasm-${VERSION}"

  run_publish "$dir" publish CARGO_REGISTRY_TOKEN="$FAKE_TOKEN"
  assert_eq "a rerun succeeds instead of failing on 'already uploaded'" "0" "$LAST_STATUS"
  assert_eq "only the unpublished crates are published" \
    "streamline-analytics streamline" "$(published_order "$dir")"
  assert_contains "reports the skip" "$LAST_OUT" "streamline-wasm@${VERSION} is already on crates.io"
  assert_contains "still waits for the already-published crate" "$LAST_OUT" \
    "streamline-serde-wincode@${VERSION} is resolvable from the registry index"
  assert_eq "an already-present version is probed too" "4" "$(info_probe_count "$dir")"
  assert_eq "none of those probes could be answered locally" "" \
    "$(info_probes_inside_a_workspace "$dir")"
}

# An already-published crate whose index entry is unreadable must block its
# dependents just as a freshly published one does.
test_already_present_still_waits_for_the_index() {
  echo "publish-crates.sh: an already-present version still waits for the index"
  local dir
  dir=$(new_scenario "present-but-invisible")
  # crates.io reports every version as published, but the index never serves
  # any of them.
  local crate
  for crate in $ORDER; do
    touch "${dir}/state/registry-${crate}-${VERSION}"
  done

  run_publish "$dir" publish CARGO_REGISTRY_TOKEN="$FAKE_TOKEN" FAKE_NEVER_VISIBLE=1
  assert_eq "fails instead of publishing dependents against a stale index" "1" "$LAST_STATUS"
  assert_contains "names the timeout" "$LAST_OUT" "still not resolvable after 2s"
  assert_eq "nothing was published" "0" "$(publish_log "$dir")"
}

test_other_version_is_not_enough() {
  echo "publish-crates.sh: another version is never evidence"
  local dir
  dir=$(new_scenario "wrong-version")
  run_publish "$dir" publish CARGO_REGISTRY_TOKEN="$FAKE_TOKEN" \
    FAKE_CURL_WRONG_VERSION='0.2.9'
  assert_eq "fails rather than skipping" "1" "$LAST_STATUS"
  assert_contains "explains the mismatch" "$LAST_OUT" "refusing to treat another version as sufficient"
  assert_eq "nothing was published" "0" "$(publish_log "$dir")"
}

test_visibility_timeout() {
  echo "publish-crates.sh: bounded index-visibility wait"
  local dir started elapsed
  dir=$(new_scenario "visibility-timeout")
  started=$SECONDS
  VISIBILITY_TIMEOUT_SECONDS=2 VISIBILITY_POLL_SECONDS=1 \
    run_publish "$dir" publish CARGO_REGISTRY_TOKEN="$FAKE_TOKEN" FAKE_NEVER_VISIBLE=1
  elapsed=$((SECONDS - started))
  # If the probe ran where the script runs, the fake cargo would answer from
  # the workspace manifest and this run would succeed — which is the whole
  # defect.
  assert_eq "fails when the version never becomes resolvable" "1" "$LAST_STATUS"
  assert_contains "names the timeout" "$LAST_OUT" "still not resolvable after 2s"
  assert_eq "stops at the first crate rather than publishing dependents" \
    "streamline-serde-wincode" "$(published_order "$dir")"
  assert_eq "the failing probe still ran outside any workspace" "" \
    "$(info_probes_inside_a_workspace "$dir")"
  if [ "$elapsed" -le 20 ]; then
    pass "the wait is bounded (${elapsed}s)"
  else
    fail "the wait is bounded" "took ${elapsed}s"
  fi
}

test_api_error_fails() {
  echo "publish-crates.sh: registry API errors are fatal"
  local dir
  dir=$(new_scenario "api-error")
  run_publish "$dir" publish CARGO_REGISTRY_TOKEN="$FAKE_TOKEN" \
    FAKE_CURL_FORCE_STATUS=503
  assert_eq "fails on HTTP 503" "1" "$LAST_STATUS"
  assert_contains "names the status" "$LAST_OUT" "returned HTTP 503"
  assert_eq "nothing was published" "0" "$(publish_log "$dir")"

  dir=$(new_scenario "api-transport-error")
  run_publish "$dir" publish CARGO_REGISTRY_TOKEN="$FAKE_TOKEN" \
    FAKE_CURL_TRANSPORT_ERROR=1
  assert_eq "fails when curl itself fails" "1" "$LAST_STATUS"
  assert_contains "refuses to guess" "$LAST_OUT" "Refusing to guess whether it is published"
  assert_eq "nothing was published" "0" "$(publish_log "$dir")"
}

test_dry_run_never_publishes() {
  echo "publish-crates.sh: dry run"
  local dir
  dir=$(new_scenario "dry-run")
  run_publish "$dir" dry-run CARGO_REGISTRY_TOKEN=
  assert_eq "succeeds without a token" "0" "$LAST_STATUS"
  assert_eq "runs no publish at all" "0" "$(publish_log "$dir")"
  assert_contains "still reports what a real run would do" "$LAST_OUT" "a real run would publish it"
  assert_eq "still queries crates.io for every crate" "4" "$(wc -l < "${dir}/curl.log" | tr -d ' ')"
  assert_eq "runs no visibility probe at all" "0" "$(info_probe_count "$dir")"
}

test_missing_token_fails_closed() {
  echo "publish-crates.sh: missing token"
  local dir
  dir=$(new_scenario "no-token")
  run_publish "$dir" publish CARGO_REGISTRY_TOKEN=
  assert_eq "refuses to publish" "1" "$LAST_STATUS"
  assert_contains "says why" "$LAST_OUT" "no crates.io token configured"
  assert_eq "nothing was published" "0" "$(publish_log "$dir")"
}

test_publish_failure_stops_the_chain() {
  echo "publish-crates.sh: a failed publish stops dependents"
  local dir
  dir=$(new_scenario "publish-fails")
  run_publish "$dir" publish CARGO_REGISTRY_TOKEN="$FAKE_TOKEN" \
    FAKE_PUBLISH_FAILS=streamline-wasm
  assert_eq "propagates the failure" "101" "$LAST_STATUS"
  assert_eq "did not continue past the failure" \
    "streamline-serde-wincode streamline-wasm" "$(published_order "$dir")"
}

# ── require-release-credentials.sh ──────────────────────────────────────────

# The preflight exists because the Go SDK tag — the one step that writes to
# another repository — is pushed *after* five irrevocable publications. Its
# only job is to turn "half a release and an unfixable workflow" into "nothing
# happened", so the behaviour that matters is: real runs fail closed, dry runs
# do not need the secret, and the secret never reaches the log.
test_require_release_credentials() {
  echo "require-release-credentials.sh"

  run_capture env -u GO_SDK_RELEASE_TOKEN bash "$REQUIRE_CREDS" --mode publish
  assert_eq "a real run without GO_SDK_RELEASE_TOKEN fails" "1" "$LAST_STATUS"
  assert_contains "names the missing secret" "$LAST_OUT" "GO_SDK_RELEASE_TOKEN is not configured"
  assert_contains "explains that the tag goes to another repository" "$LAST_OUT" \
    "streamlinelabs/streamline-go-sdk"
  assert_contains "says the automatic token cannot stand in" "$LAST_OUT" "GITHUB_TOKEN"
  assert_contains "makes clear nothing shipped" "$LAST_OUT" "Nothing has been published"

  # GitHub renders an unconfigured secret as the empty string, so "set but
  # empty" has to fail exactly like "unset".
  run_capture env GO_SDK_RELEASE_TOKEN= bash "$REQUIRE_CREDS" --mode publish
  assert_eq "an empty secret is treated as missing" "1" "$LAST_STATUS"

  run_capture env GO_SDK_RELEASE_TOKEN="$FAKE_TOKEN" bash "$REQUIRE_CREDS" --mode publish
  assert_eq "a configured token passes" "0" "$LAST_STATUS"
  assert_not_contains "never echoes the token" "$LAST_OUT" "$FAKE_TOKEN"

  # A dry run publishes nothing. Requiring publishing secrets for it would make
  # the one safe way to rehearse a release the hardest to run.
  run_capture env -u GO_SDK_RELEASE_TOKEN bash "$REQUIRE_CREDS" --mode dry-run
  assert_eq "a dry run needs no credentials" "0" "$LAST_STATUS"
  assert_contains "says why it is skipping the checks" "$LAST_OUT" \
    "no publishing credentials are required"

  run_capture bash "$REQUIRE_CREDS" --mode maybe
  assert_eq "an unknown mode fails closed" "2" "$LAST_STATUS"
  run_capture bash "$REQUIRE_CREDS"
  assert_eq "a missing mode fails closed" "2" "$LAST_STATUS"
  run_capture bash "$REQUIRE_CREDS" --mode publish --wat
  assert_eq "an unknown argument fails closed" "2" "$LAST_STATUS"
}

# ── verify-sdk-version.sh ───────────────────────────────────────────────────

# Build a minimal but structurally faithful SDK checkout for one ecosystem.
make_sdk_fixture() {
  local ecosystem="$1" version="$2" dir="$3"
  rm -rf "$dir"
  mkdir -p "$dir"
  case "$ecosystem" in
    rust)
      cat > "${dir}/Cargo.toml" <<EOF
[package]
# a comment between the header and the version, as the real manifest has
name = "streamline-client"
version = "${version}"
edition = "2021"

[dependencies]
serde = { version = "1.0" }
EOF
      ;;
    node)
      printf '{\n  "name": "@streamlinelabs/sdk",\n  "version": "%s"\n}\n' "$version" > "${dir}/package.json"
      ;;
    python)
      cat > "${dir}/pyproject.toml" <<EOF
[project]
name = "streamline-sdk"
version = "${version}"

[tool.hatch.build.targets.wheel]
packages = ["streamline_sdk"]
EOF
      mkdir -p "${dir}/streamline_sdk"
      printf '__version__ = "%s"\n' "$version" > "${dir}/streamline_sdk/__init__.py"
      # An unrelated distribution in the same repository must not be conflated
      # with the SDK being published.
      mkdir -p "${dir}/testcontainers/streamline_testcontainers"
      printf '__version__ = "0.1.0"\n' > "${dir}/testcontainers/streamline_testcontainers/__init__.py"
      ;;
    java)
      cat > "${dir}/pom.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>dev.streamline</groupId>
  <artifactId>streamline-java-parent</artifactId>
  <version>${version}</version>
  <packaging>pom</packaging>
  <modules>
    <module>streamline-client</module>
  </modules>
</project>
EOF
      mkdir -p "${dir}/streamline-client"
      cat > "${dir}/streamline-client/pom.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <parent>
    <groupId>dev.streamline</groupId>
    <artifactId>streamline-java-parent</artifactId>
    <version>${version}</version>
  </parent>
  <artifactId>streamline-client</artifactId>
</project>
EOF
      ;;
    dotnet)
      cat > "${dir}/Directory.Build.props" <<EOF
<Project>
  <PropertyGroup>
    <StreamlinePackageVersion>${version}</StreamlinePackageVersion>
    <Version>\$(StreamlinePackageVersion)</Version>
  </PropertyGroup>
</Project>
EOF
      ;;
    go)
      mkdir -p "${dir}/streamline"
      printf 'package streamline\n\n// Version is the semantic version of this SDK.\nconst Version = "%s"\n' \
        "$version" > "${dir}/streamline/version.go"
      printf 'module github.com/streamlinelabs/streamline-go-sdk\n\ngo 1.22\n' > "${dir}/go.mod"
      git -C "$dir" init -q
      git -C "$dir" -c user.email=t@e -c user.name=t add -A
      git -C "$dir" -c user.email=t@e -c user.name=t commit -qm fixture
      ;;
    kotlin)
      cat > "${dir}/build.gradle.kts" <<EOF
plugins {
    kotlin("jvm") version "2.0.0"
}

group = "dev.streamline"
version = "${version}"
EOF
      ;;
    wasm)
      cat > "${dir}/Cargo.toml" <<EOF
[package]
name = "streamline-wasm-sdk"
version = "${version}"
edition = "2021"
EOF
      printf '{\n  "name": "@streamlinelabs/streamline-wasm",\n  "version": "%s"\n}\n' "$version" > "${dir}/package.json"
      ;;
  esac
}

ECOSYSTEMS='rust node python java dotnet go kotlin wasm'

test_verify_sdk_version() {
  echo "verify-sdk-version.sh"
  local root="${WORK}/sdk"
  rm -rf "$root"
  mkdir -p "$root"

  local ecosystem
  for ecosystem in $ECOSYSTEMS; do
    make_sdk_fixture "$ecosystem" "$VERSION" "${root}/${ecosystem}-match"
    run_capture bash "$VERIFY_SDK" --ecosystem "$ecosystem" --dir "${root}/${ecosystem}-match" --expected "$VERSION"
    assert_eq "${ecosystem}: a matching checkout passes" "0" "$LAST_STATUS"

    make_sdk_fixture "$ecosystem" "0.2.0" "${root}/${ecosystem}-mismatch"
    run_capture bash "$VERIFY_SDK" --ecosystem "$ecosystem" --dir "${root}/${ecosystem}-mismatch" --expected "$VERSION"
    assert_eq "${ecosystem}: a stale checkout fails" "1" "$LAST_STATUS"
    assert_contains "${ecosystem}: the failure names the version it found" "$LAST_OUT" "0.2.0"
  done

  # An unrelated distribution in the Python repo must not fail the SDK release.
  run_capture bash "$VERIFY_SDK" --ecosystem python --dir "${root}/python-match" --expected "$VERSION"
  assert_not_contains "python: an unrelated package is not conflated with the SDK" \
    "$LAST_OUT" "streamline_testcontainers"

  # Missing files are errors, never silent passes.
  mkdir -p "${root}/empty"
  run_capture bash "$VERIFY_SDK" --ecosystem node --dir "${root}/empty" --expected "$VERSION"
  assert_eq "a checkout with no manifest fails" "1" "$LAST_STATUS"
  assert_contains "says what was missing" "$LAST_OUT" "does not exist"

  run_capture bash "$VERIFY_SDK" --ecosystem node --dir "${root}/nowhere" --expected "$VERSION"
  assert_eq "a missing checkout fails" "1" "$LAST_STATUS"
  assert_contains "refuses to claim a match" "$LAST_OUT" "refusing to claim"

  run_capture bash "$VERIFY_SDK" --ecosystem perl --dir "${root}/node-match" --expected "$VERSION"
  assert_eq "an unknown ecosystem fails closed" "1" "$LAST_STATUS"
  assert_contains "says to add it rather than skip it" "$LAST_OUT" "Add it here rather than skipping"

  # Go: a tag that already points somewhere else must block the release.
  local go_dir="${root}/go-retag"
  make_sdk_fixture go "$VERSION" "$go_dir"
  git -C "$go_dir" tag "v${VERSION}"
  printf '// drift\n' >> "${go_dir}/streamline/version.go"
  git -C "$go_dir" -c user.email=t@e -c user.name=t commit -qam drift
  run_capture bash "$VERIFY_SDK" --ecosystem go --dir "$go_dir" --expected "$VERSION"
  assert_eq "go: a tag pointing at a different commit fails" "1" "$LAST_STATUS"
  assert_contains "go: explains the retag risk" "$LAST_OUT" "already exists at"

  # Go: a tag at the checked-out commit is fine (a resumed release).
  local go_ok="${root}/go-tagged"
  make_sdk_fixture go "$VERSION" "$go_ok"
  git -C "$go_ok" tag "v${VERSION}"
  run_capture bash "$VERIFY_SDK" --ecosystem go --dir "$go_ok" --expected "$VERSION"
  assert_eq "go: an existing tag at HEAD passes" "0" "$LAST_STATUS"

  # Go: no git metadata means no evidence.
  local go_bare="${root}/go-bare"
  make_sdk_fixture go "$VERSION" "$go_bare"
  rm -rf "${go_bare}/.git"
  run_capture bash "$VERIFY_SDK" --ecosystem go --dir "$go_bare" --expected "$VERSION"
  assert_eq "go: a non-git checkout fails" "1" "$LAST_STATUS"
  assert_contains "go: says the tag cannot be validated" "$LAST_OUT" "not the root of a git checkout"
}

# ── run ─────────────────────────────────────────────────────────────────────

# Fail loudly unless the fake toolchain really is what the scripts will run.
#
# This is a safety interlock, not a nicety: if the real cargo were used, the
# publish scenarios below would attempt an actual `cargo publish`.
preflight() {
  echo "preflight"
  local dir probe
  dir=$(new_scenario "preflight")
  probe=$(env CARGO="${dir}/bin/cargo" FAKE_CARGO_LOG="${dir}/cargo.log" \
    FAKE_STATE="${dir}/state" FAKE_VERSION="$VERSION" \
    "${dir}/bin/cargo" pkgid --package probe-crate)
  case "$probe" in
    path+file:///fake/*)
      pass "the fake cargo is in control"
      ;;
    *)
      fail "the fake cargo is in control" "got: ${probe}"
      echo "refusing to run the publish scenarios against a real cargo" >&2
      exit 1
      ;;
  esac
}

main() {
  rm -rf "$WORK"
  mkdir -p "$WORK"

  preflight
  test_resolve_publish_mode
  test_all_new_sequence
  test_visibility_probe_runs_outside_the_workspace
  test_sdk_working_directory_probe
  test_probe_dir_refuses_a_workspace_tmpdir
  test_partial_rerun
  test_already_present_still_waits_for_the_index
  test_other_version_is_not_enough
  test_visibility_timeout
  test_api_error_fails
  test_dry_run_never_publishes
  test_missing_token_fails_closed
  test_publish_failure_stops_the_chain
  test_require_release_credentials
  test_verify_sdk_version

  echo
  echo "${passed} passed, ${failed} failed"
  if [ "$failed" -ne 0 ]; then
    printf 'failed: %s\n' "${failures[@]}"
    exit 1
  fi
  rm -rf "$WORK"
}

main "$@"
