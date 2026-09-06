# Releasing Streamline

Step-by-step checklist for creating a new Streamline release.

## Pre-Release Checklist

- [ ] All CI checks pass on `main`
- [ ] CHANGELOG.md updated with release date
- [ ] Version bumped in `Cargo.toml` **and** in all three workspace member
      crates (`crates/streamline-serde-wincode`, `crates/streamline-wasm`,
      `crates/streamline-analytics`) — they are published together and
      `tests/packaging_metadata_test.rs` enforces that they match
- [ ] `cargo test --test packaging_metadata_test` passes
- [ ] Dry-run the publish workflow: run **Publish to crates.io** via
      `workflow_dispatch` (a manual run is always a dry run — it declares no
      inputs and cannot publish)
- [ ] `cargo test --test release_scripts_test` passes (release-helper suite)
- [ ] `GO_SDK_RELEASE_TOKEN` is configured (cross-repository write on
      `streamlinelabs/streamline-go-sdk`) — the Go SDK tag is pushed after every
      other ecosystem has already published, so a missing token there is not
      recoverable by re-running; `publish-sdks.yml` checks it up front
- [ ] Version bumped in all SDK repos (8 SDKs) — `publish-sdks.yml` verifies
      each one's authoritative version against the release version before it
      builds or publishes anything, so a stale SDK fails the release rather
      than silently shipping the previous version
- [ ] Documentation updated for any new features
- [ ] `docs/API_STABILITY.md` version matrix and `SECURITY.md` supported
      versions updated
- [ ] Benchmarks run and results reviewed

## Release Steps

### 1. Tag the Core Release

```bash
# Ensure you're on main with latest changes
git checkout main && git pull

# Create annotated tag
git tag -a v0.X.0 -m "Release v0.X.0"
git push origin v0.X.0
```

This triggers the `release.yml` workflow, which runs as one chain:

1. **Release gate** (`release-gate.yml`, called as a reusable workflow):
   module stability audit + stability-tier lint, fail-closed `cargo audit` and
   `cargo deny`, Kafka 3.6/3.7/3.8 compatibility (the consumer steps assert the
   produced payload is actually returned), the full test suite, and a
   documentation completeness check. **Nothing is published if this fails.**
2. **Build** binaries for 6 targets (Linux x86/ARM/musl, macOS x86/ARM,
   Windows). Also gated: the build job declares `needs: release-gate`, so no
   release binary is produced before the gate is green.
3. **Publish to crates.io** (`publish-crate.yml`, reusable) — only after the
   gate and the builds pass. The job always runs, but for tags containing
   `alpha`/`beta`/`rc` it runs with `dry_run: true`, which verifies the package
   graph without publishing. Stable tags publish in dependency order:
   `streamline-serde-wincode` → `streamline-wasm` → `streamline-analytics` →
   `streamline`.
   This is the *only* workflow that runs `cargo publish` for this workspace.
4. **Create Release** — GitHub Release with binaries, `streamline.h`,
   `checksums.txt` (+ cosign signature and certificate), and a CycloneDX SBOM
   (`sbom.cdx.json`). SBOM generation, SBOM attestation and build-provenance
   attestation are **required** steps: if any of them fails, the release fails.
   No SPDX SBOM is produced.
5. **Homebrew** formula update via `repository_dispatch` (stable tags only).
6. **SLSA L3 provenance** for stable tags.

### Publishing to crates.io manually

**A `workflow_dispatch` of "Publish to crates.io" can never publish.** The
manual trigger declares no inputs at all: it verifies packaging, reports which
crates are already on crates.io, and stops. Publishing is only reachable through
`workflow_call` from `release.yml` (a tag) or `publish-sdks.yml`, both of which
run the release gate first, and only with an explicit `dry_run: false`.

`scripts/release/resolve-publish-mode.sh` makes that decision and every
dangerous step is gated on its output. It cannot key off
`github.event_name == 'workflow_call'`, because inside a reusable workflow the
`github` context belongs to the *caller* — the event is `push` or
`workflow_dispatch`, never `workflow_call` ([actions/runner#3146]). The
invocation is instead derived from whether the `workflow_call`-only `dry_run`
input is present at all, rendered through `format()` so that GitHub's
`null == false` coercion cannot turn "absent" into "please publish".

The crates.io token is read from the `CARGO_REGISTRY_TOKEN` repository secret,
falling back to `CRATES_IO_TOKEN` if only that one is configured. Both
`publish-crate.yml` and the SDK publishing workflow resolve the token the same
way and refuse to publish when neither secret is set.

Note the chicken-and-egg in the publish graph: the root `streamline` crate
depends on the member crates by `{ path, version }`, so a full
`cargo package`/`cargo publish` of the root crate only succeeds once the member
crates are on crates.io at the matching version. The dry run therefore verifies
the packaged file list for all four crates and fully packages only the member
crates; the real publish runs in dependency order.

[actions/runner#3146]: https://github.com/actions/runner/issues/3146

### Re-running a partially completed publish

crates.io publishes are irrevocable and the workspace publishes four crates in
order, so a run that dies half way through used to leave the release wedged:
re-running failed on "crate version is already uploaded", and finishing by hand
skipped the workflow's checks.

`scripts/release/publish-crates.sh` makes the step resumable. For each crate, in
dependency order, it:

1. asks crates.io whether that **exact** `name@version` exists. A different
   version of the same crate is never accepted as evidence;
2. skips the publish and reports it if the version is already there;
3. waits — whether it just published or found the version already present — for
   the version to become visible on the registry, so the next crate in the
   order can actually resolve it. This replaced a fixed `sleep 30`, and is
   bounded by `VISIBILITY_TIMEOUT_SECONDS`. The wait proves visibility twice:
   the exact-version crates.io API reports it, **and** `cargo info
   <crate>@<version>` resolves it *from a scratch directory outside any
   workspace*. That last detail is load-bearing. `cargo info` consults the
   current workspace before the registry, and every crate being published is a
   member of the workspace the script runs in (or, for the standalone Rust SDK
   job, the package in `working-directory: sdk`), so a probe run in place
   answers from the manifest on disk and succeeds instantly for a version that
   has not reached crates.io at all;
4. aborts on anything that is not a clean `200` or `404` — a 5xx, a proxy error,
   a curl failure or malformed JSON. Guessing in either direction either
   republishes a crate or skips one.

So the correct response to a half-finished release is simply to re-run the
workflow. `scripts/release/tests/release-scripts.test.sh` covers the all-new
sequence, the partial rerun, the visibility timeout, API errors and the manual
dispatch, all against fake `cargo`/`curl` binaries.

### 2. Verify Automated Steps

```bash
# Check release artifacts (binaries, checksums.txt(.sig/.pem), sbom.cdx.json)
gh release view v0.X.0

# Verify the checksum signature (keyless Sigstore)
cosign verify-blob \
  --certificate checksums.txt.pem \
  --signature checksums.txt.sig \
  --certificate-identity-regexp 'https://github.com/streamlinelabs/streamline/.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  checksums.txt

# Verify build provenance and the SBOM attestation
gh attestation verify streamline-v0.X.0-x86_64-unknown-linux-gnu.tar.gz \
  --repo streamlinelabs/streamline

# Check Homebrew update PR was created
gh pr list --repo streamlinelabs/homebrew-tap

# Check benchmark dashboard updated
open https://josedab.github.io/streamline/dev/bench/
```

### 3. Publish SDKs

Use the **Publish All SDKs** workflow (`publish-sdks.yml`) rather than
publishing by hand. Every publishing job in it declares
`needs: [verify-core-version, verify-release-credentials, release-gate, publish-rust-crate]`,
so no ecosystem package can reach npm, PyPI, Maven Central, NuGet or a Go tag
before the gated core crate is out — or before the credentials the run will
need have been checked.

#### Release secrets

| Secret | Used by | Notes |
|---|---|---|
| `CARGO_REGISTRY_TOKEN` *or* `CRATES_IO_TOKEN` | crates.io (core + Rust SDK) | either name works |
| `NPM_TOKEN` | npm (Node.js and WASM SDKs) | |
| `PYPI_TOKEN` | PyPI | |
| `OSSRH_USERNAME`, `OSSRH_TOKEN` | Maven Central (Java and Kotlin) | |
| `GPG_PRIVATE_KEY`, `GPG_PASSPHRASE` | Maven Central artifact signing | |
| `NUGET_API_KEY` | NuGet | |
| `GO_SDK_RELEASE_TOKEN` | Go SDK release tag | **cross-repository write** |

`GO_SDK_RELEASE_TOKEN` is the one that is not like the others. A Go module's
published version *is* its git tag, so releasing the Go SDK means pushing a tag
into `streamlinelabs/streamline-go-sdk` — a different repository. The workflow's
automatic `GITHUB_TOKEN` cannot do that: it is scoped to
`streamlinelabs/streamline` and `publish-sdks.yml` grants it only
`contents: read`. Configure a fine-grained PAT or a GitHub App installation
token with `contents: write` on `streamlinelabs/streamline-go-sdk`.

Because the Go tag is pushed *last* — after crates.io, npm, PyPI, Maven Central
and NuGet have all published irrevocably — a missing token discovered there used
to leave a five-sixths-shipped release that no re-run could repair. The
`verify-release-credentials` job now runs
`scripts/release/require-release-credentials.sh` before any publisher starts, so
a misconfigured release fails with nothing published. **Dry runs deliberately do
not require any publishing secret**, so a release can always be rehearsed.

Each job checks the SDK repository out into `sdk/` (this repository stays at the
workspace root so its release-control scripts are still on disk) and runs
`scripts/release/verify-sdk-version.sh` **before** it builds anything:

| SDK | Authoritative version source |
|---|---|
| Rust (`streamline-client`) | `Cargo.toml` `[package] version` |
| Node.js | `package.json` `version` |
| Python | `pyproject.toml` `[project] version` + `__version__` of each wheel package |
| Java | the reactor POM's `<version>` and every module POM |
| .NET | `Directory.Build.props` `<StreamlinePackageVersion>` |
| Go | `streamline/version.go` `const Version`, **plus** the `v<version>` tag state |
| Kotlin | `build.gradle.kts` `version` |
| WASM | `Cargo.toml` **and** `package.json` (wasm-pack derives the npm package from both) |

Dry runs perform the same checks — a dry run that skips them cannot tell you
whether the real run would be safe. A missing file, an unparsable file, an
unknown ecosystem or a Go checkout that is not a git repository is an error, not
a pass: the verifier never reports equality without reading a real artefact.

Go is the special case. A Go module has no manifest version — the published
version *is* the tag — so the SDK carries an in-repo `const Version` and the
verifier additionally refuses to proceed when `v<version>` already exists at a
different commit, which is how a module proxy ends up serving two trees for one
version. The tag is pushed with `GO_SDK_RELEASE_TOKEN` (see
[Release secrets](#release-secrets)); a dry run checks the public repository out
with the ordinary token and persists no write credential at all.

If you must publish by hand, run the same verifier first:

```bash
./scripts/release/verify-sdk-version.sh \
  --ecosystem python --dir ../streamline-python-sdk --expected 0.X.0
```

### 4. Publish Testcontainers

```bash
# Java
cd streamline-java-sdk/testcontainers && mvn deploy -P release

# Python
cd streamline-python-sdk/testcontainers && python -m build && twine upload dist/*

# Node
cd streamline-node-sdk/testcontainers && npm publish

# Go (tagged with SDK)
```

### 5. Update Ecosystem

- [ ] Merge Homebrew formula PR
- [ ] Update Helm chart version: `streamline-deploy/helm/streamline/Chart.yaml`
- [ ] Update Terraform provider version
- [ ] Update VS Code extension version and publish to marketplace
- [ ] Update Docker image tags
- [ ] Update documentation version in Docusaurus

### 6. Announce

- [ ] GitHub Release notes (auto-generated by release-drafter)
- [ ] Blog post on docs site
- [ ] Discord announcement
- [ ] Twitter/social media

## Hotfix Process

For patch releases (e.g., v0.4.1):

```bash
git checkout -b release/v0.4.1 v0.4.0
# Apply fixes
git tag -a v0.4.1 -m "Hotfix: v0.4.1"
git push origin v0.4.1
# Same automated pipeline runs — including the release gate
```

## Version Numbering

- **v0.X.0**: Minor release (new features, may include breaking changes pre-1.0)
- **v0.X.Y**: Patch release (bug fixes only)
- **v1.0.0**: First stable release (API freeze for Stable-tier modules)
- **vX.Y.Z-alpha/beta/rc**: Pre-release (skips crates.io publish, marks as prerelease on GitHub)
