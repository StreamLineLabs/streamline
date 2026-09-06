//! Runs the hermetic test suite for the release helper scripts.
//!
//! `scripts/release/tests/release-scripts.test.sh` exercises
//! `resolve-publish-mode.sh`, `publish-crates.sh` and `verify-sdk-version.sh`
//! against fake `cargo`/`curl` binaries and generated SDK fixtures. It touches
//! no network, no registry and no sibling repository.
//!
//! It is wired into `cargo test` so the release-control logic is covered by the
//! same command everything else is, rather than needing a second CI harness
//! that is easy to forget.

use std::path::PathBuf;
use std::process::Command;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

#[test]
#[cfg_attr(not(unix), ignore = "the release helpers are bash scripts")]
fn release_helper_scripts_pass_their_hermetic_suite() {
    let script = repo_root().join("scripts/release/tests/release-scripts.test.sh");
    assert!(
        script.exists(),
        "{} must exist — it is the only coverage the publishing logic has that \
         does not require a real registry",
        script.display()
    );

    let output = Command::new("bash")
        .arg(&script)
        .current_dir(repo_root())
        // `git` is used to build the Go fixtures; keep the environment
        // deterministic so a developer's global config cannot change the result.
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .env("GIT_AUTHOR_NAME", "streamline-tests")
        .env("GIT_AUTHOR_EMAIL", "tests@streamlinelabs.invalid")
        .env("GIT_COMMITTER_NAME", "streamline-tests")
        .env("GIT_COMMITTER_EMAIL", "tests@streamlinelabs.invalid")
        .output()
        .expect("failed to run the release script test suite");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "the release helper script suite failed:\n{stdout}\n{stderr}"
    );

    // Guard against a suite that silently stops running anything.
    let summary = stdout
        .lines()
        .find(|line| line.contains(" passed, ") && line.contains(" failed"))
        .unwrap_or_else(|| panic!("the suite printed no summary:\n{stdout}"));
    let passed: usize = summary
        .split_whitespace()
        .next()
        .and_then(|n| n.parse().ok())
        .unwrap_or(0);
    assert!(
        passed >= 50,
        "only {passed} assertions ran ({summary}); the suite is supposed to cover \
         the publish-mode gate, resumable publishing and all eight SDK ecosystems"
    );
}
