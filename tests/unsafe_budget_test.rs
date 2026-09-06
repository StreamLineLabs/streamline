//! Regression coverage for the CI unsafe-code ratchet.

use std::path::PathBuf;
use std::process::Command;

const UNSAFE_BUDGET: usize = 120;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn run_checker(root: &std::path::Path, budget: usize) -> std::process::Output {
    Command::new("python3")
        .arg(repo_root().join("scripts/check_unsafe_budget.py"))
        .arg("--root")
        .arg(root)
        .arg("--budget")
        .arg(budget.to_string())
        .output()
        .expect("unsafe-budget checker must run with python3")
}

#[test]
fn repository_unsafe_usage_matches_the_ci_budget() {
    let root = repo_root();
    let output = run_checker(&root, UNSAFE_BUDGET);
    assert!(
        output.status.success(),
        "unsafe checker failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains(&format!(
            "unsafe source lines without an adjacent SAFETY justification: {UNSAFE_BUDGET}"
        )),
        "the checked-in budget must equal the scanner's measured baseline"
    );

    for workflow in ["security-scan.yml", "release-gate.yml"] {
        let path = root.join(".github/workflows").join(workflow);
        let body = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
        assert!(
            body.contains(&format!("UNSAFE_BUDGET: '{UNSAFE_BUDGET}'")),
            "{workflow} must preserve the non-increasing unsafe budget of {UNSAFE_BUDGET}"
        );
        assert!(
            body.contains("python3 scripts/check_unsafe_budget.py"),
            "{workflow} must use the shared unsafe-budget checker"
        );
    }
}

#[test]
fn rust_literals_cannot_hide_a_later_unsafe_block() {
    let fixture = repo_root().join("tests/fixtures/unsafe_budget/string_literals");
    let rejected = run_checker(&fixture, 0);
    assert!(
        !rejected.status.success(),
        "the fixture's undocumented unsafe block must exceed a zero budget"
    );
    assert!(
        String::from_utf8_lossy(&rejected.stderr).contains("src/lib.rs:"),
        "the checker must report the unsafe line after strings containing comment delimiters"
    );

    let accepted = run_checker(&fixture, 1);
    assert!(
        accepted.status.success(),
        "the fixture must contain exactly one undocumented unsafe source line:\n{}",
        String::from_utf8_lossy(&accepted.stderr)
    );
    assert!(String::from_utf8_lossy(&accepted.stdout)
        .contains("unsafe source lines without an adjacent SAFETY justification: 1"));
}

#[test]
fn workspace_member_sources_are_part_of_the_ratchet() {
    let fixture = repo_root().join("tests/fixtures/unsafe_budget/workspace_member");
    let rejected = run_checker(&fixture, 0);
    assert!(
        !rejected.status.success(),
        "unsafe in a workspace member must exceed a zero budget"
    );
    assert!(
        String::from_utf8_lossy(&rejected.stderr).contains("member/src/lib.rs:"),
        "the checker must report the member source path"
    );

    let accepted = run_checker(&fixture, 1);
    assert!(
        accepted.status.success(),
        "the workspace fixture must contain exactly one undocumented unsafe line"
    );
}
