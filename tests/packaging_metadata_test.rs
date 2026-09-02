//! Packaging metadata regression tests.
//!
//! `cargo publish` rejects a dependency that has a `path` but no `version`,
//! because the published crate must resolve that dependency from the registry.
//! The workspace member crates (`streamline-wasm`, `streamline-analytics`) are
//! consumed by the root crate via `path`, so they must always carry a matching
//! `version`, and they must be published *before* the root crate.
//!
//! These tests parse the manifests as text on purpose: they must keep working
//! without adding a TOML parser dependency, and they guard the exact strings a
//! human would edit.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read_manifest(rel: &str) -> String {
    let path = repo_root().join(rel);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()))
}

/// Extract `version = "x.y.z"` from the `[package]` section of a manifest.
fn package_version(manifest: &str) -> String {
    let mut in_package = false;
    for line in manifest.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            in_package = trimmed == "[package]";
            continue;
        }
        if in_package {
            if let Some(rest) = trimmed.strip_prefix("version") {
                let rest = rest.trim_start();
                if let Some(rest) = rest.strip_prefix('=') {
                    return rest.trim().trim_matches('"').to_string();
                }
            }
        }
    }
    panic!("no [package] version found in manifest");
}

/// Collect `name = { ... path = "crates/..." ... }` dependency lines from the
/// root manifest, keyed by dependency name.
fn workspace_path_dependency_lines(manifest: &str) -> BTreeMap<String, String> {
    let mut found = BTreeMap::new();
    for line in manifest.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('#') || !trimmed.contains("path = \"crates/") {
            continue;
        }
        let Some((name, _)) = trimmed.split_once('=') else {
            continue;
        };
        found.insert(name.trim().to_string(), trimmed.to_string());
    }
    found
}

/// Workspace members that are published, as `(package name, dependency alias in
/// the root manifest, manifest path)`.
///
/// The alias differs from the package name for the `serde-wincode` bridge: the
/// root imports `streamline-serde-wincode` under the upstream crate's name via
/// `package = "..."`, so `src/bincode_compat.rs` still writes
/// `use serde_wincode::...`.
fn member_manifest_paths() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        (
            "streamline-wasm",
            "streamline-wasm",
            "crates/streamline-wasm/Cargo.toml",
        ),
        (
            "streamline-analytics",
            "streamline-analytics",
            "crates/streamline-analytics/Cargo.toml",
        ),
        (
            "streamline-serde-wincode",
            "serde-wincode",
            "crates/streamline-serde-wincode/Cargo.toml",
        ),
    ]
}

#[test]
fn workspace_path_dependencies_declare_a_version() {
    let root = read_manifest("Cargo.toml");
    let deps = workspace_path_dependency_lines(&root);

    assert!(
        !deps.is_empty(),
        "expected the root manifest to depend on workspace member crates by path"
    );

    for (name, line) in &deps {
        assert!(
            line.contains("version = \""),
            "dependency `{name}` uses `path` without `version`; `cargo publish` \
             would reject it. Offending line:\n  {line}"
        );
    }
}

#[test]
fn workspace_path_dependency_versions_match_member_crates() {
    let root = read_manifest("Cargo.toml");
    let deps = workspace_path_dependency_lines(&root);

    for (name, alias, manifest_path) in member_manifest_paths() {
        let member_version = package_version(&read_manifest(manifest_path));
        // Keyed by the dependency alias, which is what appears on the left of
        // the `=` in the root manifest; it differs from the package name when
        // the dependency is renamed with `package = "..."`.
        let line = deps
            .get(alias)
            .unwrap_or_else(|| panic!("root manifest has no path dependency on `{alias}`"));

        let expected = format!("version = \"{member_version}\"");
        assert!(
            line.contains(&expected),
            "root manifest declares `{alias}` with a version that does not match \
             {manifest_path} ({member_version}). Offending line:\n  {line}"
        );

        if alias != name {
            assert!(
                line.contains(&format!("package = \"{name}\"")),
                "`{alias}` is a renamed dependency and must name its real package \
                 with `package = \"{name}\"`. Offending line:\n  {line}"
            );
        }
    }
}

#[test]
fn workspace_crates_share_the_root_version() {
    let root_version = package_version(&read_manifest("Cargo.toml"));
    for (name, _alias, manifest_path) in member_manifest_paths() {
        let member_version = package_version(&read_manifest(manifest_path));
        assert_eq!(
            member_version, root_version,
            "`{name}` is at {member_version} but the root crate is at {root_version}; \
             workspace crates are released together"
        );
    }
}

#[test]
fn publishable_crates_have_the_metadata_crates_io_requires() {
    let mut manifests: Vec<(&str, &str)> = vec![("streamline", "Cargo.toml")];
    manifests.extend(
        member_manifest_paths()
            .into_iter()
            .map(|(name, _alias, path)| (name, path)),
    );

    for (name, manifest_path) in manifests {
        let manifest = read_manifest(manifest_path);
        for field in ["description", "license", "repository"] {
            assert!(
                manifest.contains(&format!("{field} = ")),
                "`{name}` ({manifest_path}) is missing the required `{field}` field"
            );
        }
        assert!(
            manifest.contains("rust-version = "),
            "`{name}` ({manifest_path}) is missing `rust-version`; MSRV must be \
             declared consistently across the workspace"
        );
    }
}

#[test]
fn node_diagnostic_reports_are_ignored_and_excluded_from_the_root_package() {
    const PATTERN: &str = "report.*.json";

    let manifest = read_manifest("Cargo.toml");
    let package_section = manifest
        .split("[package]")
        .nth(1)
        .and_then(|section| section.split("\n[").next())
        .expect("Cargo.toml must have a [package] section");
    assert!(
        package_section
            .lines()
            .any(|line| line.trim().trim_matches(',').trim_matches('"') == PATTERN),
        "the root crate's [package].exclude must contain `{PATTERN}` so Node \
         diagnostic reports cannot enter a .crate archive even if one is \
         force-added to git"
    );

    let gitignore = read_manifest(".gitignore");
    assert!(
        gitignore.lines().any(|line| line.trim() == PATTERN),
        ".gitignore must contain `{PATTERN}` because Node diagnostic reports \
         expose host, network, and process metadata"
    );
}

/// The publish order encoded in `.github/workflows/publish-crate.yml` must put
/// the member crates before the root crate, otherwise the root crate's registry
/// dependencies will not resolve.
#[test]
fn publish_workflow_orders_member_crates_before_the_root_crate() {
    let workflow_path = repo_root().join(".github/workflows/publish-crate.yml");
    if !Path::new(&workflow_path).exists() {
        panic!(
            "expected publish workflow at {} — publishing must go through one \
             dependable chain",
            workflow_path.display()
        );
    }
    let workflow = std::fs::read_to_string(&workflow_path).unwrap();

    let order_line = workflow
        .lines()
        .find(|l| l.trim_start().starts_with("PUBLISH_ORDER:"))
        .expect("publish workflow must declare PUBLISH_ORDER");

    let order: Vec<&str> = order_line
        .split_once(':')
        .expect("malformed PUBLISH_ORDER")
        .1
        .trim()
        .trim_matches('\'')
        .trim_matches('"')
        .split_whitespace()
        .collect();

    let root_index = order
        .iter()
        .position(|c| *c == "streamline")
        .expect("PUBLISH_ORDER must include the root crate");

    for (name, _alias, _) in member_manifest_paths() {
        let index = order
            .iter()
            .position(|c| *c == name)
            .unwrap_or_else(|| panic!("PUBLISH_ORDER must include `{name}`"));
        assert!(
            index < root_index,
            "`{name}` must be published before the root crate; got order {order:?}"
        );
    }
}

/// The MSRV declared in the manifests must match the MSRV the CI workflows
/// actually build with. Previously the MSRV job installed a pinned toolchain
/// but `rust-toolchain.toml` (`channel = "stable"`) took precedence, so the job
/// re-tested stable and the MSRV claim was unverified.
#[test]
fn ci_msrv_matches_the_manifest_rust_version() {
    let manifest = read_manifest("Cargo.toml");
    let rust_version = manifest
        .lines()
        .find_map(|l| l.trim().strip_prefix("rust-version"))
        .and_then(|rest| rest.trim_start().strip_prefix('='))
        .map(|v| v.trim().trim_matches('"').to_string())
        .expect("root manifest must declare rust-version");

    let ci = std::fs::read_to_string(repo_root().join(".github/workflows/ci.yml"))
        .expect("ci workflow must exist");

    assert!(
        ci.contains(&format!("MSRV: '{rust_version}'")),
        "ci.yml MSRV does not match Cargo.toml rust-version ({rust_version})"
    );
    assert!(
        ci.contains(&format!("RUSTUP_TOOLCHAIN: '{rust_version}'")),
        "the MSRV job must set RUSTUP_TOOLCHAIN to {rust_version}; otherwise \
         rust-toolchain.toml silently overrides it and the job tests stable"
    );
}

/// Publishing the core workspace must go through exactly one workflow. A second
/// workflow that also runs `cargo publish` on this workspace can race the
/// release gate and cannot honour the member-crate publish order.
///
/// Jobs that check out a *different* repository (the standalone SDK repos) are
/// not publishing this workspace and are therefore exempt.
#[test]
fn only_one_workflow_publishes_this_workspace_to_crates_io() {
    let workflow_dir = repo_root().join(".github/workflows");
    let mut offenders = Vec::new();

    for entry in std::fs::read_dir(&workflow_dir).expect("workflows dir") {
        let path = entry.expect("dir entry").path();
        if path.extension().and_then(|e| e.to_str()) != Some("yml") {
            continue;
        }
        let file_name = path.file_name().unwrap().to_string_lossy().to_string();
        if file_name == "publish-crate.yml" {
            continue;
        }
        let body = std::fs::read_to_string(&path).unwrap_or_default();

        for job in split_jobs(&body) {
            let publishes = job
                .lines()
                .filter(|l| !l.trim_start().starts_with('#'))
                .any(|l| l.contains("cargo publish"));
            // A job that checks out another repository is publishing that
            // repository's crate, not this workspace.
            let external_checkout = job.contains("repository: streamlinelabs/");
            if publishes && !external_checkout {
                offenders.push(file_name.clone());
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "only publish-crate.yml may publish this workspace to crates.io; \
         found `cargo publish` in {offenders:?}"
    );
}

/// Shell entry points are part of the release attack surface too. The only
/// local registry publisher is the helper invoked by the gated reusable
/// workflow; every other script must dispatch a workflow rather than publishing
/// from a developer machine.
#[test]
fn only_the_gated_release_helper_can_publish_locally() {
    let mut offenders = Vec::new();

    for (name, body) in shell_script_files() {
        if name == "scripts/release/publish-crates.sh" || name.starts_with("scripts/release/tests/")
        {
            continue;
        }

        for (line_number, line) in uncommented(&body).enumerate() {
            let line = line.trim();
            if line.starts_with("echo ") || line.starts_with("printf ") {
                continue;
            }
            let publishes = [
                "cargo publish",
                "npm publish",
                "twine upload",
                "mvn deploy",
                "dotnet nuget push",
                "gradlew publish",
                "git push origin",
            ]
            .iter()
            .any(|command| line.contains(command));
            if publishes {
                offenders.push(format!("{name}:{}: {line}", line_number + 1));
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "release scripts other than scripts/release/publish-crates.sh must not \
         publish locally; dispatch a gated workflow instead:\n{}",
        offenders.join("\n")
    );

    let wrapper = std::fs::read_to_string(repo_root().join("scripts/publish-sdks.sh"))
        .expect("scripts/publish-sdks.sh must exist");
    assert!(
        wrapper.contains("workflow run publish-sdks.yml")
            && wrapper.contains(r#"--field "version=$version""#)
            && wrapper.contains(r#"--field "dry_run=$dry_run""#),
        "scripts/publish-sdks.sh must do nothing except dispatch the gated \
         publish-sdks.yml workflow with explicit version and dry-run inputs"
    );
    for forbidden in [
        "0.2.0",
        "npm version",
        "streamline-rust-sdk",
        "streamline-node-sdk",
        "streamline-python-sdk",
        "streamline-java-sdk",
        "streamline-dotnet-sdk",
        "streamline-kotlin-sdk",
        "streamline-wasm-sdk",
    ] {
        assert!(
            !wrapper.contains(forbidden),
            "scripts/publish-sdks.sh must not contain the legacy local-release \
             token `{forbidden}`"
        );
    }
}

/// Split a workflow body into per-job chunks. Jobs are the two-space-indented
/// keys under `jobs:`.
fn split_jobs(body: &str) -> Vec<String> {
    let Some(jobs_start) = body.find("\njobs:") else {
        return Vec::new();
    };
    let mut jobs: Vec<String> = Vec::new();
    let mut current = String::new();
    for line in body[jobs_start..].lines().skip(1) {
        let is_job_header = line.starts_with("  ")
            && !line.starts_with("   ")
            && line.trim_end().ends_with(':')
            && !line.trim_start().starts_with('#');
        if is_job_header && !current.is_empty() {
            jobs.push(std::mem::take(&mut current));
        }
        current.push_str(line);
        current.push('\n');
    }
    if !current.is_empty() {
        jobs.push(current);
    }
    jobs
}

/// The release workflow must depend on the release gate before it publishes
/// anything (crates.io or a GitHub Release).
#[test]
fn release_workflow_gates_publishing() {
    let release = std::fs::read_to_string(repo_root().join(".github/workflows/release.yml"))
        .expect("release workflow must exist");

    assert!(
        release.contains("uses: ./.github/workflows/release-gate.yml"),
        "release.yml must invoke the release gate as a reusable workflow"
    );

    for job in ["publish-crate:", "release:"] {
        let idx = release
            .find(&format!("\n  {job}"))
            .unwrap_or_else(|| panic!("release.yml must define a `{job}` job"));
        // Look at the job body up to the next top-level job definition.
        let rest = &release[idx + 1..];
        let end = rest[1..].find("\n  ").map(|i| i + 1).unwrap_or(rest.len());
        let body = &rest[..rest.len().min(end + 600)];
        assert!(
            body.contains("release-gate"),
            "job `{job}` must declare `needs: [release-gate, ...]` so publishing \
             cannot run before the gate"
        );
    }
}

/// The tag version check must be the first dependency of every release path.
/// A mismatched `vX.Y.Z` tag must fail before the reusable release gate builds
/// code, before binary artifacts are created, and before either publication
/// workflow can run.
#[test]
fn release_workflow_verifies_the_tag_version_before_all_release_jobs() {
    let body = read_workflow("release.yml");
    let verify_steps = steps_of("release.yml", "verify-tag-version");
    let compare = verify_steps
        .iter()
        .find(|step| step.contains("Compare tag and root package version"))
        .expect("release.yml must compare the tag with the root package version");

    assert!(
        compare.contains(r#"tag_version="${tag#v}""#),
        "the release check must compare `${{ github.ref_name }}` without exactly \
         one leading `v`; got:\n{compare}"
    );
    assert!(
        compare.contains("verify-sdk-version.sh")
            && compare.contains("--ecosystem rust --dir . --expected \"$tag_version\""),
        "the stripped tag must be compared with the authoritative root \
         workspace/package version by verify-sdk-version.sh:\n{compare}"
    );

    for job in [
        "release-gate",
        "build",
        "publish-crate",
        "release",
        "slsa-provenance",
    ] {
        let needs = needs_of(&body, job);
        assert!(
            needs.contains(&"verify-tag-version".to_string()),
            "release.yml job `{job}` can build, publish, create artifacts, or \
             update the GitHub release without waiting for the tag/version \
             check; got needs {needs:?}"
        );
    }
}

/// Binary archives and the signed checksum manifest are a cross-repository
/// contract consumed by the Homebrew tap.
#[test]
fn core_release_assets_match_the_homebrew_contract() {
    let workflow = read_workflow("release.yml");

    for suffix in ["tar.gz", "zip"] {
        let expected =
            format!("streamline-${{{{ github.ref_name }}}}-${{{{ matrix.target }}}}.{suffix}");
        assert!(
            workflow.contains(&expected),
            "release.yml must publish v-prefixed archives through github.ref_name: {expected}"
        );
    }
    assert!(
        workflow.contains("> checksums.txt")
            && workflow.contains("artifacts/checksums.txt.sig")
            && workflow.contains("artifacts/checksums.txt.pem"),
        "release.yml must publish checksums.txt and its keyless Cosign signature/certificate"
    );
    assert!(
        workflow.contains("cosign sign-blob --yes")
            && workflow.contains("--output-signature artifacts/checksums.txt.sig")
            && workflow.contains("--output-certificate artifacts/checksums.txt.pem"),
        "checksums.txt must be signed by the tagged release workflow before upload"
    );
}

/// Every workflow that publishes to crates.io must resolve the token the same
/// way — canonical `CARGO_REGISTRY_TOKEN`, falling back to `CRATES_IO_TOKEN` —
/// and must pass it through the environment.
///
/// The two publishing workflows previously disagreed (`publish-crate.yml` read
/// `CARGO_REGISTRY_TOKEN`, `publish-sdks.yml` read `CRATES_IO_TOKEN`), so with
/// `secrets: inherit` a repository configured with only one of the names had a
/// publish path that failed with "token is not configured" while another
/// appeared to work. `cargo publish --token <secret>` additionally put the
/// secret in the process's argv.
#[test]
fn crates_io_token_usage_is_standardized() {
    const FALLBACK: &str = "${{ secrets.CARGO_REGISTRY_TOKEN || secrets.CRATES_IO_TOKEN }}";

    let mut checked = 0usize;
    for (name, body) in workflow_files() {
        assert!(
            !body.contains("cargo publish --token"),
            "{name}: pass the crates.io token via the CARGO_REGISTRY_TOKEN \
             environment variable, not on the command line"
        );

        for job in split_jobs(&body) {
            // `cargo publish` now runs inside scripts/release/publish-crates.sh,
            // so a job that invokes that script in publish mode is a publishing
            // job even though the workflow text never says `cargo publish`.
            let publishes = uncommented(&job).any(|l| {
                (l.contains("cargo publish") && !l.contains("--dry-run"))
                    || (l.contains("publish-crates.sh") && l.contains("--mode publish"))
            });
            if !publishes {
                continue;
            }
            checked += 1;
            assert!(
                job.contains(FALLBACK),
                "{name}: job `{}` publishes to crates.io and must resolve the token \
                 as `{FALLBACK}` so both supported secret names work",
                job_name(&job)
            );
        }
    }

    assert!(
        checked >= 2,
        "expected publish-crate.yml and the standalone Rust SDK job to both \
         publish to crates.io; found {checked} publishing job(s), so this guard \
         would be vacuous"
    );
}

// ---------------------------------------------------------------------------
// Release-control invariants for the two publishing workflows.
// ---------------------------------------------------------------------------

/// The step chunks of a named job in a workflow.
fn steps_of(workflow: &str, job: &str) -> Vec<String> {
    let body = read_workflow(workflow);
    let chunk = split_jobs(&body)
        .into_iter()
        .find(|j| job_name(j) == job)
        .unwrap_or_else(|| panic!("{workflow} must define a `{job}` job"));
    split_steps(&chunk)
}

/// The `needs:` list of a named job, as individual job names.
fn needs_of(body: &str, job: &str) -> Vec<String> {
    let chunk = split_jobs(body)
        .into_iter()
        .find(|j| job_name(j) == job)
        .unwrap_or_else(|| panic!("no `{job}` job"));
    let line = uncommented(&chunk)
        .find(|l| l.trim_start().starts_with("needs:"))
        .unwrap_or("");
    line.split_once(':')
        .map(|(_, rest)| rest)
        .unwrap_or("")
        .trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .split(',')
        .map(|n| n.trim().to_string())
        .filter(|n| !n.is_empty())
        .collect()
}

/// A `workflow_dispatch` of publish-crate.yml must be dry-run-only, and it must
/// be impossible to express anything else.
///
/// The manual trigger previously took a `dry_run` boolean that fed straight
/// into the token check and `cargo publish`, so anyone who could dispatch the
/// workflow could make an irrevocable crates.io release with none of the
/// release-gate checks having run. The boolean is gone; there is nothing manual
/// left to supply.
#[test]
fn manual_dispatch_of_publish_crate_cannot_publish() {
    let body = read_workflow("publish-crate.yml");

    // The dispatch trigger declares no inputs at all.
    let dispatch = body
        .split("\n  workflow_dispatch:")
        .nth(1)
        .expect("publish-crate.yml must declare workflow_dispatch");
    let dispatch_block: String = dispatch
        .lines()
        .take_while(|l| l.trim().is_empty() || l.starts_with("    ") || l.starts_with("  #"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        !dispatch_block.contains("inputs:"),
        "publish-crate.yml `workflow_dispatch` must declare no inputs — a manual \
         boolean is exactly what let a dispatch publish. Found:\n{dispatch_block}"
    );

    // The `workflow_call` input is required, so "absent" can never be read as
    // "false" through GitHub's null/false coercion.
    let call = body
        .split("workflow_call:")
        .nth(1)
        .expect("publish-crate.yml must declare workflow_call");
    let dry_run_block = call
        .split("dry_run:")
        .nth(1)
        .expect("workflow_call must declare a dry_run input");
    assert!(
        dry_run_block
            .lines()
            // The keys of this input: everything indented under `dry_run:`,
            // comments included, up to the next sibling key.
            .take_while(|l| l.trim().is_empty() || l.starts_with("        "))
            .any(|l| l.trim() == "required: true"),
        "the `workflow_call` dry_run input must be `required: true`, so a caller \
         cannot omit it and have it coerced to `false`"
    );

    // The invocation event is derived from the presence of that input, because
    // `github.event_name` inside a reusable workflow is the *caller's* event and
    // is never the literal `workflow_call`.
    assert!(
        body.contains(
            "INVOCATION_EVENT: ${{ format('{0}', inputs.dry_run) == '' && github.event_name || 'workflow_call' }}"
        ),
        "publish-crate.yml must derive INVOCATION_EVENT from whether the \
         workflow_call-only `dry_run` input is present. Comparing \
         `inputs.dry_run == false` directly is unsafe (GitHub coerces a null \
         input to 0, which equals false), and `github.event_name` is the \
         caller's event inside a reusable workflow."
    );
}

/// Nothing that can publish may run unless the resolved mode is `publish`, and
/// that mode can only be produced for a `workflow_call` invocation.
#[test]
fn non_dry_publish_is_conditioned_on_a_workflow_call_invocation() {
    const GUARD: &str = "needs.resolve-mode.outputs.mode == 'publish'";

    let steps = steps_of("publish-crate.yml", "publish");
    let mut guarded = Vec::new();

    for step in &steps {
        let dangerous = uncommented(step).any(|l| {
            (l.contains("publish-crates.sh") && l.contains("--mode publish"))
                || (l.contains("cargo publish") && !l.contains("--dry-run"))
                || l.contains("refusing to publish")
        });
        if !dangerous {
            continue;
        }
        let name = step
            .lines()
            .next()
            .unwrap_or_default()
            .trim()
            .trim_start_matches("- name:")
            .trim()
            .to_string();
        assert!(
            uncommented(step).any(|l| l.contains(GUARD)),
            "publish-crate.yml step `{name}` can publish (or is the token check \
             that guards it) and must be conditioned on `{GUARD}`:\n{step}"
        );
        guarded.push(name);
    }

    assert_eq!(
        guarded,
        vec![
            "Fail if the registry token is missing for a real publish".to_string(),
            "Publish to crates.io".to_string(),
        ],
        "expected exactly the token check and the publish step to be gated"
    );

    // The gate itself: only a workflow_call invocation from a gated caller can
    // produce `publish`.
    let script =
        std::fs::read_to_string(repo_root().join("scripts/release/resolve-publish-mode.sh"))
            .expect("scripts/release/resolve-publish-mode.sh must exist");
    assert!(
        script.contains(r#"if [ "$INVOCATION_EVENT" != "workflow_call" ]; then"#),
        "resolve-publish-mode.sh must refuse anything that did not arrive through \
         workflow_call before it can emit `publish`"
    );
    assert!(
        script.contains("ALLOWED_PUBLISH_CALLERS"),
        "resolve-publish-mode.sh must additionally restrict publishing to callers \
         that run the release gate"
    );
    assert!(
        read_workflow("publish-crate.yml")
            .contains("ALLOWED_PUBLISH_CALLERS: 'release.yml publish-sdks.yml'"),
        "publish-crate.yml must name the gated callers explicitly"
    );
}

/// A manual run must say so in the summary, so nobody reads a green dispatch as
/// "the release went out".
#[test]
fn manual_publish_crate_summary_reports_a_dry_run() {
    let body = read_workflow("publish-crate.yml");
    let summary = split_steps(
        &split_jobs(&body)
            .into_iter()
            .find(|j| job_name(j) == "publish")
            .expect("publish job"),
    )
    .into_iter()
    .find(|s| s.contains("Publish summary"))
    .expect("publish-crate.yml must write a publish summary");

    assert!(
        summary.contains(r#"PUBLISH_MODE: ${{ needs.resolve-mode.outputs.mode }}"#),
        "the summary must report the *resolved* mode, not the requested one:\n{summary}"
    );
    assert!(
        summary.contains(r#"if [ "$PUBLISH_MODE" = "publish" ]; then"#)
            && summary.contains("dry run (packaging verified only, nothing published)"),
        "the summary must say `dry run` for anything that did not publish:\n{summary}"
    );
}

/// The fixed `sleep` between crates is gone: index visibility is now waited for
/// deterministically, and an already-published version is skipped rather than
/// re-published.
#[test]
fn crates_io_publishing_is_resumable_and_does_not_sleep() {
    let body = read_workflow("publish-crate.yml");
    assert!(
        !uncommented(&body).any(|l| l.trim().starts_with("sleep ")),
        "publish-crate.yml must not wait for the registry with a fixed sleep; \
         scripts/release/publish-crates.sh polls for real visibility instead"
    );
    assert!(
        body.contains("VISIBILITY_TIMEOUT_SECONDS")
            && body.contains("./scripts/release/publish-crates.sh --mode publish"),
        "publish-crate.yml must publish through the resumable helper with a \
         bounded visibility timeout"
    );

    let script = std::fs::read_to_string(repo_root().join("scripts/release/publish-crates.sh"))
        .expect("scripts/release/publish-crates.sh must exist");
    for required in [
        // exact-version existence probe
        "${CRATES_IO_API}/${crate}/${version}",
        // only 200/404 are actionable
        "only 200 (published) and 404 (not published) are actionable",
        // another version is never evidence
        "refusing to treat another version as sufficient",
        // deterministic visibility check with a bound
        r#"info "${crate}@${version}""#,
        "VISIBILITY_TIMEOUT_SECONDS",
    ] {
        assert!(
            script.contains(required),
            "publish-crates.sh must contain `{required}`"
        );
    }
    // The token is only ever tested for emptiness (`${CARGO_REGISTRY_TOKEN:-}`)
    // and handed to cargo through the environment; it is never expanded into a
    // message, a URL or a command line.
    assert!(
        !script.contains("${CARGO_REGISTRY_TOKEN}"),
        "publish-crates.sh must never interpolate the registry token into output"
    );
}

/// Every SDK ecosystem this repository knows how to release.
///
/// `(job name, verifier ecosystem)`. `publish-rust-crate` is the core workspace
/// and is verified by `verify-core-version` instead.
const SDK_PUBLISH_JOBS: &[(&str, &str)] = &[
    ("publish-rust-sdk", "rust"),
    ("publish-npm", "node"),
    ("publish-pypi", "python"),
    ("publish-maven", "java"),
    ("publish-nuget", "dotnet"),
    ("publish-go", "go"),
    ("publish-kotlin", "kotlin"),
    ("publish-wasm", "wasm"),
];

/// No ecosystem package may publish ahead of the gated core.
///
/// The SDK jobs previously had no `needs:` at all, so a `workflow_dispatch` ran
/// them in parallel with the release gate and with the crates.io publish. An
/// npm or PyPI release that lands before the server it targets cannot be
/// withdrawn.
#[test]
fn every_sdk_publish_job_waits_for_the_gated_core() {
    let body = read_workflow("publish-sdks.yml");

    // The enumeration must be complete: every `publish-*` job in the workflow
    // has to be accounted for here, or a new one could be added ungated.
    let discovered: Vec<String> = split_jobs(&body)
        .iter()
        .map(|j| job_name(j))
        .filter(|n| n.starts_with("publish-"))
        .collect();
    let mut expected: Vec<String> = SDK_PUBLISH_JOBS
        .iter()
        .map(|(j, _)| j.to_string())
        .collect();
    expected.push("publish-rust-crate".to_string());
    expected.sort();
    let mut discovered_sorted = discovered.clone();
    discovered_sorted.sort();
    assert_eq!(
        discovered_sorted, expected,
        "publish-sdks.yml gained or lost a publishing job. Add it to \
         SDK_PUBLISH_JOBS (with its verifier ecosystem) so it is covered by \
         these guards."
    );

    // The core publish itself only needs the gate and the version check.
    let core = needs_of(&body, "publish-rust-crate");
    for required in ["verify-core-version", "release-gate"] {
        assert!(
            core.contains(&required.to_string()),
            "`publish-rust-crate` must declare `needs: {required}`; got {core:?}"
        );
    }

    for (job, _) in SDK_PUBLISH_JOBS {
        let needs = needs_of(&body, job);
        for required in ["verify-core-version", "release-gate", "publish-rust-crate"] {
            assert!(
                needs.contains(&required.to_string()),
                "`{job}` must declare `needs: {required}` so no ecosystem package \
                 can publish ahead of the gated core; got {needs:?}"
            );
        }
    }
}

/// Every SDK job must verify the checked-out repository really is at the version
/// being released, before it builds or publishes anything — in dry runs too.
#[test]
fn every_sdk_publish_job_verifies_its_version_before_building() {
    let body = read_workflow("publish-sdks.yml");

    for (job, ecosystem) in SDK_PUBLISH_JOBS {
        let steps = steps_of("publish-sdks.yml", job);

        let verify_at = steps
            .iter()
            .position(|s| s.contains("verify-sdk-version.sh"))
            .unwrap_or_else(|| {
                panic!(
                    "`{job}` must run scripts/release/verify-sdk-version.sh; without it \
                     the `version` input is only a label and the job publishes whatever \
                     the SDK's default branch declares"
                )
            });

        let verify_step = &steps[verify_at];
        assert!(
            verify_step.contains(&format!("--ecosystem {ecosystem}")),
            "`{job}` must verify the `{ecosystem}` version source:\n{verify_step}"
        );
        assert!(
            verify_step.contains("--dir sdk"),
            "`{job}` must verify the SDK checkout in `sdk/`, not this repository:\n{verify_step}"
        );
        assert!(
            verify_step.contains(r#"EXPECTED: ${{ inputs.version }}"#),
            "`{job}` must compare against the release's `version` input:\n{verify_step}"
        );
        assert!(
            !uncommented(verify_step).any(|l| l.trim_start().starts_with("if:")),
            "`{job}`'s version check must be unconditional — a dry run that skips it \
             cannot tell you whether the real run would be safe:\n{verify_step}"
        );

        // Nothing that builds or ships may precede it.
        for (index, step) in steps.iter().enumerate().take(verify_at) {
            let acts = uncommented(step).any(|l| {
                let l = l.trim_start();
                l.starts_with("run:")
                    && [
                        "npm ",
                        "python -m build",
                        "mvn ",
                        "dotnet pack",
                        "go build",
                        "gradlew",
                        "wasm-pack build",
                        "cargo publish",
                        "twine ",
                        "git tag",
                    ]
                    .iter()
                    .any(|cmd| l.contains(cmd))
            });
            assert!(
                !acts,
                "`{job}` step {index} builds or publishes before the version check:\n{step}"
            );
        }

        // The SDK must be checked out beside this repository, not over it, or
        // the release-control scripts would be gone by the time they are needed.
        let chunk = split_jobs(&body)
            .into_iter()
            .find(|j| job_name(j) == *job)
            .expect("job");
        assert!(
            chunk.contains("path: sdk"),
            "`{job}` must check the SDK repository out into `sdk/` so this \
             repository's release scripts stay on disk"
        );
    }
}

/// The standalone WASM repository publishes its root npm package, not the
/// transient package.json that wasm-pack generates inside pkg/.
#[test]
fn wasm_sdk_publish_uses_its_root_package_identity_and_build_script() {
    let steps = steps_of("publish-sdks.yml", "publish-wasm");

    let identity = steps
        .iter()
        .find(|step| step.contains("Verify npm package identity"))
        .expect("publish-wasm must verify the root npm package identity");
    assert!(
        identity.contains("@streamlinelabs/streamline-wasm")
            && identity.contains("working-directory: sdk"),
        "publish-wasm must require the root package identity before building:\n{identity}"
    );

    let build = steps
        .iter()
        .find(|step| step.contains("name: Build"))
        .expect("publish-wasm must have a build step");
    assert!(
        build.contains("working-directory: sdk") && build.contains("npm run build"),
        "publish-wasm must use the SDK repository's own root build script:\n{build}"
    );
    assert!(
        !uncommented(build).any(|line| line.contains("wasm-pack build")),
        "publish-wasm must not bypass npm run build with a raw wasm-pack command:\n{build}"
    );

    let publish = steps
        .iter()
        .find(|step| step.contains("name: Publish"))
        .expect("publish-wasm must have a publish step");
    assert!(
        publish.contains("working-directory: sdk")
            && publish.contains("npm publish --access public"),
        "publish-wasm must publish the root @streamlinelabs/streamline-wasm package:\n{publish}"
    );
    assert!(
        !publish.contains("working-directory: sdk/pkg"),
        "publish-wasm must never publish wasm-pack's transient pkg identity:\n{publish}"
    );

    let dry_run = steps
        .iter()
        .find(|step| step.contains("name: Dry run"))
        .expect("publish-wasm must exercise npm packing in dry runs");
    assert!(
        dry_run.contains("working-directory: sdk")
            && dry_run.contains("npm pack --dry-run"),
        "publish-wasm dry runs must pack the root npm package:\n{dry_run}"
    );
}

/// The verifier must actually know every ecosystem the workflow asks it about,
/// and must fail closed on anything else.
#[test]
fn the_version_verifier_supports_every_ecosystem_the_workflow_uses() {
    let script = std::fs::read_to_string(repo_root().join("scripts/release/verify-sdk-version.sh"))
        .expect("scripts/release/verify-sdk-version.sh must exist");

    for (_, ecosystem) in SDK_PUBLISH_JOBS {
        assert!(
            script.contains(&format!("\n    {ecosystem})")),
            "verify-sdk-version.sh has no `{ecosystem}` case"
        );
    }
    assert!(
        script.contains("unknown ecosystem"),
        "verify-sdk-version.sh must reject an unknown ecosystem rather than \
         silently passing"
    );
    assert!(
        script.contains("verify_go_tag_state"),
        "the Go SDK has no manifest version, so the verifier must validate its \
         release-tag state as well as its in-repo constant"
    );
}

/// The registry-visibility wait must prove something the checkout on disk
/// cannot answer.
///
/// `cargo info <crate>@<version>` resolves against the *current workspace*
/// first. Every crate this script publishes is a member of the workspace it is
/// run from — the repository root for `publish-crate.yml`, and the SDK checkout
/// for `publish-sdks.yml`'s `working-directory: sdk` job — so the probe used to
/// succeed on its first attempt against the manifest on disk, for a version
/// that had not reached crates.io at all. The bounded wait, the poll interval
/// and the whole "don't publish a dependent before its dependency resolves"
/// guarantee were decoration.
///
/// The fix has two halves, and both are asserted here because each is
/// insufficient alone: the exact-version crates.io API (which no local checkout
/// can answer) and a `cargo info` run from a scratch directory outside any
/// workspace (which is what a dependent's `cargo publish` actually needs).
#[test]
fn the_registry_visibility_probe_cannot_be_answered_by_the_local_workspace() {
    let script = std::fs::read_to_string(repo_root().join("scripts/release/publish-crates.sh"))
        .expect("scripts/release/publish-crates.sh must exist");

    let wait = script
        .split_once("\nwait_for_visibility() {")
        .map(|(_, rest)| rest.split("\n}\n").next().unwrap_or(rest).to_string())
        .expect("publish-crates.sh must define wait_for_visibility");

    assert!(
        wait.contains("registry_has_exact_version"),
        "wait_for_visibility must poll the exact-version crates.io API — the only \
         visibility evidence a local workspace cannot produce"
    );
    assert!(
        wait.contains("new_registry_probe_dir"),
        "wait_for_visibility must run its cargo probe from a dedicated scratch \
         directory, not from the workspace the script was invoked in"
    );

    // Every `cargo info` in the script must run inside the scratch directory.
    // A bare invocation is the defect itself.
    for line in script.lines() {
        if !line.contains(r#""$CARGO" info"#) {
            continue;
        }
        assert!(
            line.contains(r#"cd "$probe""#),
            "publish-crates.sh runs `cargo info` without first entering the \
             registry probe directory, so cargo would answer from the local \
             manifest:\n{line}"
        );
    }

    // The scratch directory must itself be proved to be outside a workspace: a
    // TMPDIR inside a checkout would silently reintroduce the shortcut.
    let probe_dir = script
        .split_once("\nnew_registry_probe_dir() {")
        .map(|(_, rest)| rest.split("\n}\n").next().unwrap_or(rest).to_string())
        .expect("publish-crates.sh must define new_registry_probe_dir");
    assert!(
        probe_dir.contains("Cargo.toml") && probe_dir.contains("::error::"),
        "new_registry_probe_dir must fail closed when the scratch directory has \
         an ancestor Cargo.toml"
    );

    // Both branches — freshly published and already present — must wait.
    assert!(
        script.contains("wait_for_visibility \"$crate\" \"$version\""),
        "publish-crates.sh must call wait_for_visibility for every crate in the \
         order, not only for the ones it published in this run"
    );
    assert_eq!(
        script.matches("wait_for_visibility \"$crate\"").count(),
        1,
        "there must be exactly one wait_for_visibility call site, shared by the \
         published and already-present branches; a second one is how the \
         already-present case loses its wait"
    );
}

// ---------------------------------------------------------------------------
// Cross-repository release credentials.
// ---------------------------------------------------------------------------

/// The job that must run before anything is published.
const CREDENTIAL_PREFLIGHT_JOB: &str = "verify-release-credentials";

/// Every job in publish-sdks.yml that publishes something.
fn publishing_jobs() -> Vec<String> {
    let mut jobs: Vec<String> = SDK_PUBLISH_JOBS
        .iter()
        .map(|(j, _)| j.to_string())
        .collect();
    jobs.push("publish-rust-crate".to_string());
    jobs
}

/// No publisher may start before the credential preflight.
///
/// The Go SDK's release tag is pushed to *another* repository, after crates.io,
/// npm, PyPI, Maven Central and NuGet have already published irrevocably. When
/// its credential was missing the run failed there, and no re-run could repair
/// it: the fix is a repository-settings change, and the other five ecosystems
/// were already out. The credential is therefore checked in a job that every
/// publisher waits on, so a misconfigured release is a run in which nothing
/// happened.
#[test]
fn every_publishing_job_waits_for_the_credential_preflight() {
    let body = read_workflow("publish-sdks.yml");

    let names: Vec<String> = split_jobs(&body).iter().map(|j| job_name(j)).collect();
    assert!(
        names.iter().any(|n| n == CREDENTIAL_PREFLIGHT_JOB),
        "publish-sdks.yml must define a `{CREDENTIAL_PREFLIGHT_JOB}` job; got {names:?}"
    );

    // The preflight must not itself be downstream of anything: a dependency
    // would let a publisher run first and defeat the point.
    let preflight_needs = needs_of(&body, CREDENTIAL_PREFLIGHT_JOB);
    assert!(
        preflight_needs.is_empty(),
        "`{CREDENTIAL_PREFLIGHT_JOB}` must have no `needs:` so it runs before \
         everything else; got {preflight_needs:?}"
    );

    for job in publishing_jobs() {
        let needs = needs_of(&body, &job);
        assert!(
            needs.contains(&CREDENTIAL_PREFLIGHT_JOB.to_string()),
            "`{job}` publishes irrevocably and must declare \
             `needs: {CREDENTIAL_PREFLIGHT_JOB}`; got {needs:?}"
        );
    }
}

/// A real run must fail on a missing credential; a dry run must not need one.
///
/// The check lives in `scripts/release/require-release-credentials.sh` because
/// secrets cannot be referenced from a job- or step-level `if:` — the workflow
/// can only hand the value to a step and let it decide. The behaviour itself is
/// covered hermetically by `scripts/release/tests/release-scripts.test.sh`;
/// this test asserts the workflow is actually wired to it.
#[test]
fn the_credential_preflight_blocks_a_real_run_and_exempts_a_dry_run() {
    let steps = steps_of("publish-sdks.yml", CREDENTIAL_PREFLIGHT_JOB);

    let check = steps
        .iter()
        .find(|s| s.contains("require-release-credentials.sh"))
        .unwrap_or_else(|| {
            panic!(
                "`{CREDENTIAL_PREFLIGHT_JOB}` must run \
                 scripts/release/require-release-credentials.sh"
            )
        });

    assert!(
        check.contains("GO_SDK_RELEASE_TOKEN: ${{ secrets.GO_SDK_RELEASE_TOKEN }}"),
        "the preflight must receive the dedicated cross-repository secret \
         through the environment:\n{check}"
    );
    assert!(
        check.contains("MODE: ${{ inputs.dry_run && 'dry-run' || 'publish' }}"),
        "the preflight must run in `publish` mode exactly when the workflow is \
         not a dry run:\n{check}"
    );
    assert!(
        !uncommented(check).any(|l| l.trim_start().starts_with("if:")),
        "the preflight step must be unconditional — the dry-run exemption \
         belongs in the script, where it is tested, not in a step condition \
         that also skips the real check:\n{check}"
    );

    let script =
        std::fs::read_to_string(repo_root().join("scripts/release/require-release-credentials.sh"))
            .expect("scripts/release/require-release-credentials.sh must exist");
    assert!(
        script.contains(r#"if [ "$MODE" = "dry-run" ]"#) && script.contains("exit 0"),
        "the preflight script must exempt dry runs, which publish nothing and \
         must stay runnable without publishing secrets"
    );
    assert!(
        script.contains(r#""${GO_SDK_RELEASE_TOKEN:-}""#),
        "the preflight script must treat an unset and an empty secret alike; \
         GitHub renders an unconfigured secret as the empty string"
    );
    assert!(
        !script.contains("${GO_SDK_RELEASE_TOKEN}"),
        "the preflight script must never interpolate the token value into \
         output — only its emptiness may be reported"
    );
}

/// The Go SDK tag must be pushed with a credential that can actually write to
/// the Go SDK repository, and a dry run must not hold one.
///
/// This workflow's automatic `GITHUB_TOKEN` is scoped to this repository and is
/// granted `contents: read`, so it can neither create the tag nor be quietly
/// escalated into doing so. `actions/checkout` persists whatever token it used
/// as the credential for `origin`, which is what `git push` then authenticates
/// with — so the checkout is where the authority is decided.
#[test]
fn the_go_sdk_tag_uses_a_dedicated_cross_repository_token() {
    let body = read_workflow("publish-sdks.yml");

    // The escalation this replaces: the workflow must stay read-only.
    let permissions = body
        .split_once("\npermissions:")
        .map(|(_, rest)| rest.lines().take(3).collect::<Vec<_>>().join("\n"))
        .expect("publish-sdks.yml must declare workflow permissions");
    assert!(
        permissions.contains("contents: read") && !permissions.contains("contents: write"),
        "publish-sdks.yml must keep `permissions: contents: read`; the Go tag is \
         pushed with a dedicated cross-repository token, not by widening this \
         repository's own token:\n{permissions}"
    );

    let steps = steps_of("publish-sdks.yml", "publish-go");

    let checkout = steps
        .iter()
        .find(|s| s.contains("repository: streamlinelabs/streamline-go-sdk"))
        .expect("`publish-go` must check out the Go SDK repository");
    assert!(
        checkout.contains("secrets.GO_SDK_RELEASE_TOKEN"),
        "the Go SDK checkout must use the dedicated cross-repository token so \
         the push it persists credentials for can succeed:\n{checkout}"
    );
    assert!(
        checkout.contains(
            "token: ${{ inputs.dry_run && github.token || secrets.GO_SDK_RELEASE_TOKEN }}"
        ),
        "a dry run must read the public Go SDK repository with the ordinary \
         token, and only a real run with the write credential:\n{checkout}"
    );
    assert!(
        checkout.contains("persist-credentials: ${{ !inputs.dry_run }}"),
        "a dry run publishes nothing and must not leave a cross-repository write \
         credential in the checkout's git config:\n{checkout}"
    );

    let push = steps
        .iter()
        .find(|s| s.contains("git push origin"))
        .expect("`publish-go` must push the release tag");
    assert!(
        push.contains("GO_SDK_RELEASE_TOKEN: ${{ secrets.GO_SDK_RELEASE_TOKEN }}"),
        "the tag push must name the credential it relies on, so it fails closed \
         instead of reaching an unauthenticated `git push`:\n{push}"
    );
    assert!(
        !push.contains("secrets.GITHUB_TOKEN") && !push.contains("github.token"),
        "the tag push must not fall back to this repository's token, which \
         cannot write to streamlinelabs/streamline-go-sdk:\n{push}"
    );
    assert!(
        uncommented(push).any(|l| l.contains("exit 1")),
        "the tag push step must exit non-zero when the credential is absent:\n{push}"
    );
}

/// The release helper scripts must stay executable and syntactically valid.
#[test]
#[cfg_attr(not(unix), ignore = "POSIX permissions and bash are unix-only")]
fn release_helper_scripts_are_executable_and_parse() {
    use std::os::unix::fs::PermissionsExt;

    for script in [
        "scripts/release/resolve-publish-mode.sh",
        "scripts/release/publish-crates.sh",
        "scripts/release/verify-sdk-version.sh",
        "scripts/release/require-release-credentials.sh",
        "scripts/release/tests/release-scripts.test.sh",
    ] {
        let path = repo_root().join(script);
        let mode = std::fs::metadata(&path)
            .unwrap_or_else(|e| panic!("{script} must exist: {e}"))
            .permissions()
            .mode();
        assert!(
            mode & 0o111 != 0,
            "{script} must be executable (mode is {mode:o})"
        );

        let parsed = std::process::Command::new("bash")
            .arg("-n")
            .arg(&path)
            .output()
            .expect("failed to run bash -n");
        assert!(
            parsed.status.success(),
            "{script} is not valid bash:\n{}",
            String::from_utf8_lossy(&parsed.stderr)
        );
    }
}

/// Release artifact handling invariants.
///
/// * `actions/download-artifact` creates one directory per artifact, named
///   `streamline-<target>`. Checksums and attestation subjects must be computed
///   from files only, so those directories are removed and the globs are
///   explicit.
/// * The published SBOM must be the *root* CycloneDX document, asserted by its
///   `metadata.component.name`, rather than whichever JSON file happened to be
///   found first.
#[test]
fn release_workflow_handles_artifacts_and_sbom_deterministically() {
    let release = std::fs::read_to_string(repo_root().join(".github/workflows/release.yml"))
        .expect("release workflow must exist");

    assert!(
        !release.contains("sha256sum streamline-* "),
        "checksums must not be computed from a bare `streamline-*` glob: it \
         also matches the per-target artifact directories"
    );
    assert!(
        release.contains("-type d -name 'streamline-*' -exec rm -rf {} +"),
        "release.yml must delete the per-target artifact directories after \
         flattening the archives"
    );
    assert!(
        !release.contains("subject-path: artifacts/streamline-*"),
        "attestation subjects must list the archive globs explicitly so that \
         directories and cosign .sig/.pem files are not attested"
    );
    assert!(
        release.contains(".metadata.component.name == \"streamline\""),
        "the release must assert the SBOM describes the root `streamline` \
         component before publishing it"
    );
}

// ---------------------------------------------------------------------------
// Workflow helpers shared by the CI-invariant tests below.
// ---------------------------------------------------------------------------

/// Every `.github/workflows/*.yml` file as `(file name, body)`.
fn workflow_files() -> Vec<(String, String)> {
    let dir = repo_root().join(".github/workflows");
    let mut files = Vec::new();
    for entry in std::fs::read_dir(&dir).expect("workflows dir") {
        let path = entry.expect("dir entry").path();
        if path.extension().and_then(|e| e.to_str()) != Some("yml") {
            continue;
        }
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        files.push((name, std::fs::read_to_string(&path).unwrap_or_default()));
    }
    files.sort();
    files
}

/// Every `scripts/**/*.sh` file as `(repository-relative path, body)`.
fn shell_script_files() -> Vec<(String, String)> {
    fn visit(root: &Path, dir: &Path, files: &mut Vec<(String, String)>) {
        for entry in std::fs::read_dir(dir).expect("script directory") {
            let entry = entry.expect("script entry");
            let path = entry.path();
            let file_type = entry.file_type().expect("script file type");
            if file_type.is_dir() {
                visit(root, &path, files);
            } else if path.extension().and_then(|extension| extension.to_str()) == Some("sh") {
                let name = path
                    .strip_prefix(root)
                    .expect("script must be inside the repository")
                    .to_string_lossy()
                    .replace('\\', "/");
                files.push((name, std::fs::read_to_string(&path).unwrap_or_default()));
            }
        }
    }

    let root = repo_root();
    let mut files = Vec::new();
    visit(&root, &root.join("scripts"), &mut files);
    files.sort_by(|left, right| left.0.cmp(&right.0));
    files
}

fn read_workflow(name: &str) -> String {
    std::fs::read_to_string(repo_root().join(".github/workflows").join(name))
        .unwrap_or_else(|e| panic!("{name} must exist: {e}"))
}

/// The job name of a chunk produced by [`split_jobs`].
fn job_name(job: &str) -> String {
    job.lines()
        .next()
        .unwrap_or_default()
        .trim()
        .trim_end_matches(':')
        .to_string()
}

/// Split a job chunk into per-step chunks. The step indentation is taken from
/// the first list item under `steps:`, so `- ` lines inside a shell script are
/// not mistaken for step headers.
fn split_steps(job: &str) -> Vec<String> {
    let Some(steps_idx) = job.find("\n    steps:") else {
        return Vec::new();
    };
    let mut indent: Option<usize> = None;
    let mut steps: Vec<String> = Vec::new();
    let mut current = String::new();

    for line in job[steps_idx..].lines().skip(1) {
        let leading = line.len() - line.trim_start().len();
        let is_item = line.trim_start().starts_with("- ");
        if is_item && indent.is_none() {
            indent = Some(leading);
        }
        let is_step_header = is_item && Some(leading) == indent;
        if is_step_header && !current.is_empty() {
            steps.push(std::mem::take(&mut current));
        }
        if is_step_header || !current.is_empty() {
            current.push_str(line);
            current.push('\n');
        }
    }
    if !current.is_empty() {
        steps.push(current);
    }
    steps
}

/// Lines of a chunk with YAML comments removed, so that a rule described in a
/// comment is never mistaken for the rule itself being configured.
fn uncommented(chunk: &str) -> impl Iterator<Item = &str> {
    chunk.lines().filter(|l| !l.trim_start().starts_with('#'))
}

/// `rust-toolchain.toml` pins the toolchain this workspace builds with, and it
/// takes precedence over whatever toolchain a setup action makes default.
///
/// A step that installs a cross-compilation target must therefore install it
/// into *that* toolchain. `dtolnay/rust-toolchain@stable` with only `targets:`
/// installed the target into `stable`, while cargo went on to use the pinned
/// toolchain — where the target was missing — so `cargo build --target ...`
/// failed with "the <target> target may not be installed".
///
/// Jobs that check out one of the standalone SDK repositories build a
/// different workspace and are exempt: this repository's pin does not apply
/// there.
#[test]
fn cross_targets_are_installed_into_the_pinned_toolchain() {
    let toolchain_file = std::fs::read_to_string(repo_root().join("rust-toolchain.toml"))
        .expect("rust-toolchain.toml must exist");
    let channel = toolchain_file
        .lines()
        .find_map(|l| l.trim().strip_prefix("channel"))
        .and_then(|rest| rest.trim_start().strip_prefix('='))
        .map(|v| v.trim().trim_matches('"').to_string())
        .expect("rust-toolchain.toml must declare a channel");

    // `1.88.0` and `1.88` name the same toolchain to rustup; accept either.
    let mut parts = channel.split('.');
    let series = format!(
        "{}.{}",
        parts.next().unwrap_or_default(),
        parts.next().unwrap_or_default()
    );

    let mut checked = 0usize;
    for (name, body) in workflow_files() {
        for job in split_jobs(&body) {
            if job.contains("repository: streamlinelabs/") {
                continue;
            }
            for step in split_steps(&job) {
                if !step.contains("dtolnay/rust-toolchain") {
                    continue;
                }
                if !uncommented(&step).any(|l| l.trim_start().starts_with("targets:")) {
                    continue;
                }
                checked += 1;
                let pinned = uncommented(&step).any(|l| {
                    let t = l.trim_start();
                    t.starts_with("toolchain:")
                        && (t.contains(&format!("'{series}'"))
                            || t.contains(&format!("\"{series}\""))
                            || t.contains(&format!("'{channel}'"))
                            || t.contains(&format!("\"{channel}\"")))
                });
                assert!(
                    pinned,
                    "{name}: job `{}` installs a cross-compilation target without \
                     pinning `toolchain: '{series}'`. rust-toolchain.toml ({channel}) \
                     wins over the action's default, so the target would be installed \
                     into a toolchain cargo never uses.",
                    job_name(&job)
                );
            }
        }
    }

    assert!(
        checked > 0,
        "expected at least one workflow step installing a cross-compilation \
         target for this workspace; the guard would otherwise be vacuous"
    );
}

/// Anything that publishes this workspace to crates.io must run behind the
/// reusable release gate.
///
/// `publish-sdks.yml` called `publish-crate.yml` with `dry_run: false` while
/// depending only on a version-string check, so a `workflow_dispatch` could
/// publish an irrevocable crates.io release without the stability, security,
/// Kafka-compatibility, test and documentation checks ever running.
#[test]
fn every_crates_io_publish_depends_on_the_release_gate() {
    let mut callers = 0usize;
    for (name, body) in workflow_files() {
        for job in split_jobs(&body) {
            let calls_publish = uncommented(&job)
                .any(|l| l.contains("uses: ./.github/workflows/publish-crate.yml"));
            if !calls_publish {
                continue;
            }
            callers += 1;

            let needs = uncommented(&job)
                .find(|l| l.trim_start().starts_with("needs:"))
                .unwrap_or_else(|| {
                    panic!(
                        "{name}: job `{}` calls publish-crate.yml with no `needs:`",
                        job_name(&job)
                    )
                });
            assert!(
                needs.contains("release-gate"),
                "{name}: job `{}` calls publish-crate.yml without depending on \
                 `release-gate`; publishing to crates.io is irrevocable and must \
                 not be able to run ahead of the gate. Got: {}",
                job_name(&job),
                needs.trim()
            );
            assert!(
                body.contains("uses: ./.github/workflows/release-gate.yml"),
                "{name}: declares a `release-gate` dependency but never invokes \
                 ./.github/workflows/release-gate.yml"
            );
        }
    }

    assert!(
        callers >= 2,
        "expected release.yml and publish-sdks.yml to both call publish-crate.yml; \
         found {callers} caller(s)"
    );
}

/// The benchmark workflow builds and runs code from the checked-out ref, which
/// on a pull request is untrusted contributor code. No job that executes that
/// code may hold a token that can write to the repository; only the job that
/// moves already-produced artifacts onto the gh-pages dashboard may.
#[test]
fn benchmark_jobs_running_repository_code_are_read_only() {
    const WORKFLOW: &str = "benchmarks-unified.yml";
    let body = read_workflow(WORKFLOW);

    let mut writers = Vec::new();
    for job in split_jobs(&body) {
        let name = job_name(&job);
        let runs_repo_code =
            uncommented(&job).any(|l| l.contains("cargo ") || l.contains("./target/release/"));
        let grants_write = uncommented(&job).any(|l| {
            let trimmed = l.trim();
            trimmed.ends_with(": write")
        });

        assert!(
            !(runs_repo_code && grants_write),
            "{WORKFLOW}: job `{name}` both executes repository code and holds \
             a writable token. Split the publishing/comment step into its own job so \
             untrusted PR code never runs with a writable token."
        );
        if grants_write {
            writers.push(name);
        }
    }

    assert_eq!(
        writers,
        vec![
            "publish-dashboard".to_string(),
            "comment-comparison".to_string()
        ],
        "{WORKFLOW}: only the artifact-only publish/comment jobs may hold \
         writable permissions"
    );
}

/// The planned M3 WASM size check must never masquerade as an active gate.
///
/// It previously watched `streamline/src/**`, changed into a non-existent
/// `streamline/` directory, and converted a missing binary/artifact into a
/// warning plus exit 0. Making that job automatic before the binary exists
/// would merely turn it into permanent red CI, so it remains manual and fails
/// with an explicit implementation blocker.
#[test]
fn wasm_size_gate_targets_this_repo_and_never_degrades_to_a_noop() {
    let body = read_workflow("wasm-build.yml");

    for stale in [
        "\"streamline/src/**\"",
        "\"streamline/Cargo.toml\"",
        "working-directory: streamline",
        "build skipped",
        "exiting size gate as no-op",
    ] {
        assert!(
            !body.contains(stale),
            "wasm-build.yml still contains the fail-open/stale fragment `{stale}`"
        );
    }

    assert!(body.contains("\n  workflow_dispatch:"));
    assert!(
        !body.contains("\n  push:") && !body.contains("\n  pull_request:"),
        "the planned M3 check must not run automatically before its binary target exists"
    );
    assert!(body.contains("Cargo.toml does not declare the streamline-core-wasm binary target"));
    assert!(body.contains("--bin streamline-core-wasm"));
    assert!(body.contains("expected WASM artifact was not produced"));
}
