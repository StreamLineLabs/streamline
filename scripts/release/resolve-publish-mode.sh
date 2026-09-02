#!/usr/bin/env bash
#
# Decide whether a run of .github/workflows/publish-crate.yml may publish.
#
# Prints exactly one of `publish` or `dry-run` on stdout, plus a human-readable
# explanation on stderr. Never prints a secret.
#
# ── Why this is a script and not an `if:` expression ────────────────────────
#
# The obvious guard, `if: github.event_name == 'workflow_call'`, does not work.
# Inside a *called* (reusable) workflow the `github` context belongs to the
# caller, so `github.event_name` is the caller's event — `push` for a tag
# release, `workflow_dispatch` when publish-sdks.yml is run manually. It is
# never the string `workflow_call`.
#   https://github.com/actions/runner/issues/3146
#
# So publish-crate.yml derives the invocation event instead, and this script
# consumes it:
#
#   * `workflow_dispatch` on publish-crate.yml declares **no inputs at all**.
#     There is no manual boolean to supply, so nothing manual can ask to
#     publish.
#   * `workflow_call` declares a required `dry_run` input. The workflow renders
#     it with `format('{0}', inputs.dry_run)`, which is the empty string when
#     the input is absent. String comparison is essential here: GitHub coerces
#     `null == false` to `0 == 0`, i.e. *true*, so an absent input compared
#     directly against `false` would look like an explicit "please publish".
#
# The result is that INVOCATION_EVENT is `workflow_call` if and only if the run
# came through the reusable-workflow entry point, and this script publishes only
# then, only for an explicit `dry_run=false`, and only for a caller that is
# known to run the release gate first.
#
# Inputs (environment):
#   INVOCATION_EVENT         `workflow_call` or the caller's event name.
#   REQUESTED_DRY_RUN        `true`, `false`, or empty when no input was given.
#   CALLER_WORKFLOW_REF      github.workflow_ref of the run (may be empty).
#   ALLOWED_PUBLISH_CALLERS  Space-separated workflow file names permitted to
#                            request a publish.
#
# Exit status is 0 for both outcomes; a malformed request exits non-zero.

set -euo pipefail

INVOCATION_EVENT="${INVOCATION_EVENT:-}"
REQUESTED_DRY_RUN="${REQUESTED_DRY_RUN:-}"
CALLER_WORKFLOW_REF="${CALLER_WORKFLOW_REF:-}"
ALLOWED_PUBLISH_CALLERS="${ALLOWED_PUBLISH_CALLERS:-}"

deny() {
  echo "publish mode: dry-run — $1" >&2
  echo "dry-run"
  exit 0
}

if [ "$INVOCATION_EVENT" != "workflow_call" ]; then
  deny "invoked as '${INVOCATION_EVENT:-unknown}', not through workflow_call. \
Manual runs of publish-crate.yml verify packaging only; publish through \
release.yml (tag) or publish-sdks.yml, which run the release gate first."
fi

case "$REQUESTED_DRY_RUN" in
  true)
    deny "the caller requested dry_run=true"
    ;;
  false)
    : # the only value that may proceed
    ;;
  "")
    deny "no dry_run input was supplied; publishing requires an explicit \
dry_run=false from a gated caller"
    ;;
  *)
    echo "::error::invalid dry_run value '${REQUESTED_DRY_RUN}'; expected 'true' or 'false'" >&2
    exit 1
    ;;
esac

# A gated caller is one whose workflow runs release-gate.yml before calling
# this workflow. `github.workflow_ref` looks like
# `owner/repo/.github/workflows/release.yml@refs/tags/v0.4.0`.
if [ -z "$ALLOWED_PUBLISH_CALLERS" ]; then
  echo "::error::ALLOWED_PUBLISH_CALLERS is empty; refusing to publish" >&2
  exit 1
fi

caller_file="${CALLER_WORKFLOW_REF%@*}"
caller_file="${caller_file##*/}"

if [ -z "$caller_file" ]; then
  deny "the calling workflow could not be identified from workflow_ref \
'${CALLER_WORKFLOW_REF}'"
fi

for allowed in $ALLOWED_PUBLISH_CALLERS; do
  if [ "$caller_file" = "$allowed" ]; then
    echo "publish mode: publish — called by ${caller_file} with dry_run=false" >&2
    echo "publish"
    exit 0
  fi
done

deny "'${caller_file}' is not one of the gated callers (${ALLOWED_PUBLISH_CALLERS})"
