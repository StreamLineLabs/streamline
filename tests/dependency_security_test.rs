//! Dependency-security regression tests.
//!
//! These tests fail the build if a dependency update reintroduces a crate that
//! was deliberately removed from the resolved graph. They parse `Cargo.lock`
//! directly (rather than shelling out to `cargo audit`/`cargo deny`) so they run
//! offline in every CI job and in every feature configuration.
//!
//! Background — the removals these tests lock in:
//!
//! * `quick-xml < 0.41` — RUSTSEC-2026-0194 (quadratic duplicate-attribute
//!   scan) and RUSTSEC-2026-0195 (unbounded namespace-declaration allocation),
//!   both CVSS 7.5 and reachable from attacker-influenced S3/Azure list XML.
//! * `rkyv` — RUSTSEC-2026-0001 and RUSTSEC-2026-0235 (unsound archive
//!   validation). Removed by moving to `rust_decimal` 1.43, which dropped
//!   rkyv 0.7 from its normal dependencies.
//! * `native-tls` / `hyper-tls` / `openssl` — Streamline ships rustls only; an
//!   OpenSSL path would add a second TLS stack and a C toolchain requirement.
//! * `bincode` — RUSTSEC-2025-0141. The advisory has `patched = []`: it applies
//!   to every published version, 1.x and 2.x alike, so there is no version to
//!   upgrade to. Replaced by `serde-wincode`/`wincode`, which reproduce the
//!   bincode 1 wire format byte-for-byte (see `src/bincode_compat.rs`).
//! * `lru < 0.18.2` — RUSTSEC-2026-0002 / RUSTSEC-2026-0253 (stacked-borrows
//!   violation and panic-triggered use-after-free). Removed by upgrading
//!   Ratatui from 0.29 to 0.30.2.
//! * `scc < 3.8.4` — RUSTSEC-2026-0205 (panic-triggered double free). The only
//!   edge was the `serial_test` dev dependency; the affected tests now use a
//!   Tokio mutex and no longer resolve `scc`.
//!
//! None of these are suppressed via `deny.toml` `[advisories] ignore`. If one
//! reappears, fix the dependency edge — do not relax this test.
//!
//! # MSRV: published requirements, not lockfile pins
//!
//! The second half of this file is about a different failure mode. Several
//! dependencies have newer releases that require a toolchain above this crate's
//! MSRV of 1.88, and they used to be held back by `Cargo.lock` alone. A
//! lockfile binds *this repository*: a consumer of the published `streamline`
//! resolves from scratch against the normalised manifests crates.io serves and
//! never sees it. So did the comments that said "Cargo.lock pins it" — they
//! documented a constraint that did not exist for anyone downstream.
//!
//! Every such edge now carries an exact `=x.y.z` requirement in a manifest that
//! is itself published (see [`MSRV_SENSITIVE_PINS`]), and the proof is
//! `fresh_consumer_resolves_an_msrv_compatible_graph`: it packages the crates,
//! deletes the embedded lockfiles, and resolves a brand-new consumer with the
//! real MSRV toolchain.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Crates that must not appear in the resolved dependency graph at all.
const BANNED_CRATES: &[(&str, &str)] = &[
    (
        "rkyv",
        "RUSTSEC-2026-0001 / RUSTSEC-2026-0235; removed via rust_decimal >= 1.43",
    ),
    (
        "native-tls",
        "Streamline is rustls-only; native-tls pulls the OpenSSL stack",
    ),
    (
        "hyper-tls",
        "Streamline is rustls-only; hyper-tls pulls native-tls -> OpenSSL",
    ),
    (
        "openssl",
        "Streamline is rustls-only; see deny.toml [bans] deny",
    ),
    (
        "openssl-sys",
        "Streamline is rustls-only; see deny.toml [bans] deny",
    ),
    (
        "rustls-pemfile",
        "RUSTSEC-2025-0134 (unmaintained, archived Aug 2025); PEM parsing now \
         uses rustls::pki_types::pem::PemObject, which ships with rustls itself",
    ),
    (
        "bincode",
        "RUSTSEC-2025-0141 (unmaintained, `patched = []` — every version is \
         affected, including 2.x). The bincode 1 on-disk wire format is now \
         produced by serde-wincode/wincode; see src/bincode_compat.rs",
    ),
    (
        "bincode_derive",
        "Pulled in only by bincode 2.x, which is banned above (RUSTSEC-2025-0141)",
    ),
];

/// Minimum acceptable `quick-xml` version: the first release fixing both
/// RUSTSEC-2026-0194 and RUSTSEC-2026-0195.
const MIN_QUICK_XML: (u64, u64) = (0, 41);

/// First releases fixing the RustSec soundness advisories listed above.
const MIN_SOUND_LRU: (u64, u64, u64) = (0, 18, 2);
const MIN_SOUND_SCC: (u64, u64, u64) = (3, 8, 4);

/// A `[[package]]` entry from `Cargo.lock`.
#[derive(Debug)]
struct LockedPackage {
    name: String,
    version: String,
}

fn workspace_lockfile() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.lock")
}

/// Minimal `Cargo.lock` reader.
///
/// `Cargo.lock` is a restricted TOML subset: a flat sequence of `[[package]]`
/// tables whose `name`/`version` values are always simple quoted strings. That
/// keeps this parser dependency-free and immune to a TOML crate bump.
fn parse_lockfile(contents: &str) -> Vec<LockedPackage> {
    let mut packages = Vec::new();
    let mut name: Option<String> = None;
    let mut version: Option<String> = None;

    let unquote = |line: &str| -> Option<String> {
        let value = line.split_once('=')?.1.trim();
        value
            .strip_prefix('"')
            .and_then(|v| v.strip_suffix('"'))
            .map(str::to_owned)
    };

    for line in contents.lines() {
        let line = line.trim();
        if line == "[[package]]" {
            if let (Some(n), Some(v)) = (name.take(), version.take()) {
                packages.push(LockedPackage {
                    name: n,
                    version: v,
                });
            }
            name = None;
            version = None;
        } else if line.starts_with("name = ") && name.is_none() {
            name = unquote(line);
        } else if line.starts_with("version = ") && version.is_none() {
            version = unquote(line);
        }
    }
    if let (Some(n), Some(v)) = (name, version) {
        packages.push(LockedPackage {
            name: n,
            version: v,
        });
    }
    packages
}

fn locked_packages() -> Vec<LockedPackage> {
    let path = workspace_lockfile();
    let contents = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
    let packages = parse_lockfile(&contents);
    assert!(
        packages.len() > 100,
        "parsed only {} packages from {} — the lockfile parser is probably broken, \
         which would make every assertion below vacuously pass",
        packages.len(),
        path.display()
    );
    packages
}

/// Parse a `major.minor` prefix from a semver string.
fn major_minor(version: &str) -> (u64, u64) {
    let mut parts = version.split('.');
    let major = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
    let minor = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
    (major, minor)
}

/// Parse a full `major.minor.patch` triple from a semver string.
///
/// `major_minor` is too coarse for the async-graphql family: the first release
/// that raised `rust-version` to 1.89 was 7.0.**18**, a patch bump.
fn version_triple(version: &str) -> (u64, u64, u64) {
    let core = version
        .split(['-', '+'])
        .next()
        .unwrap_or(version)
        .trim_start_matches('=');
    let mut parts = core.split('.');
    let major = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
    let minor = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
    let patch = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
    (major, minor, patch)
}

#[test]
fn lockfile_contains_no_banned_crates() {
    let packages = locked_packages();
    let mut violations = Vec::new();

    for package in &packages {
        if let Some((_, reason)) = BANNED_CRATES
            .iter()
            .find(|(banned, _)| *banned == package.name)
        {
            violations.push(format!(
                "  {} {} — {}",
                package.name, package.version, reason
            ));
        }
    }

    assert!(
        violations.is_empty(),
        "Cargo.lock reintroduced banned crate(s):\n{}\n\n\
         Fix the dependency edge that pulls them in. Do not add an advisory \
         suppression and do not relax this test.",
        violations.join("\n")
    );
}

#[test]
fn lockfile_has_no_vulnerable_quick_xml() {
    let packages = locked_packages();
    let offenders: Vec<_> = packages
        .iter()
        .filter(|p| p.name == "quick-xml")
        .filter(|p| major_minor(&p.version) < MIN_QUICK_XML)
        .map(|p| format!("  quick-xml {}", p.version))
        .collect();

    assert!(
        offenders.is_empty(),
        "Cargo.lock contains quick-xml older than {}.{} \
         (RUSTSEC-2026-0194, RUSTSEC-2026-0195 — CVSS 7.5):\n{}\n\n\
         These advisories are reachable from object-storage list XML. Upgrade \
         the dependency that pins the old version instead of suppressing them.",
        MIN_QUICK_XML.0,
        MIN_QUICK_XML.1,
        offenders.join("\n")
    );
}

#[test]
fn lockfile_has_no_known_unsound_lru_or_scc() {
    let packages = locked_packages();
    let offenders: Vec<_> = packages
        .iter()
        .filter_map(|package| {
            let version = version_triple(&package.version);
            match package.name.as_str() {
                "lru" if version < MIN_SOUND_LRU => Some(format!(
                    "  lru {} — RUSTSEC-2026-0002 / RUSTSEC-2026-0253",
                    package.version
                )),
                "scc" if version < MIN_SOUND_SCC => {
                    Some(format!("  scc {} — RUSTSEC-2026-0205", package.version))
                }
                _ => None,
            }
        })
        .collect();

    assert!(
        offenders.is_empty(),
        "Cargo.lock contains dependency versions with known soundness bugs:\n{}\n\n\
         Upgrade or remove the dependency edge; do not suppress these \
         advisories.",
        offenders.join("\n")
    );
}

/// The lakehouse connector crates were removed because no MSRV-compatible
/// release is free of the advisories above. Reintroducing them silently would
/// undo goals 2 and 3 of the dependency-security work.
#[test]
fn lockfile_contains_no_removed_lakehouse_crates() {
    let packages = locked_packages();
    let removed = ["deltalake", "delta_kernel", "iceberg", "opendal", "reqsign"];

    let offenders: Vec<_> = packages
        .iter()
        .filter(|p| removed.contains(&p.name.as_str()))
        .map(|p| format!("  {} {}", p.name, p.version))
        .collect();

    assert!(
        offenders.is_empty(),
        "Cargo.lock reintroduced a removed lakehouse crate:\n{}\n\n\
         These pull quick-xml < 0.41 (and, for Delta Lake, native-tls/OpenSSL). \
         Only re-add them once upstream ships an MSRV-compatible release on \
         quick-xml >= 0.41 — and then update src/sink/unavailable.rs too.",
        offenders.join("\n")
    );
}

/// `deny.toml` must keep denying OpenSSL and must not carry advisory
/// suppressions, otherwise the checks above could be worked around in CI.
#[test]
fn deny_toml_has_no_advisory_suppressions() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("deny.toml");
    let contents = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));

    let ignore_line = contents
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with("ignore"))
        .expect("deny.toml must declare an `ignore` list under [advisories]");

    assert_eq!(
        ignore_line, "ignore = []",
        "deny.toml [advisories] ignore must stay empty — advisories are fixed, not suppressed"
    );

    assert!(
        contents.contains("{ name = \"openssl\""),
        "deny.toml must keep denying the `openssl` crate"
    );
}

#[test]
fn every_cargo_audit_gate_denies_unsound_advisories() {
    fn visit(dir: &Path, files: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", dir.display()))
        {
            let entry = entry.expect("audit gate entry");
            let path = entry.path();
            let file_type = entry.file_type().expect("audit gate file type");
            if file_type.is_dir() {
                visit(&path, files);
            } else {
                files.push(path);
            }
        }
    }

    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut candidates = vec![root.join("Makefile"), root.join(".pre-commit-config.yaml")];
    visit(&root.join(".github/workflows"), &mut candidates);
    visit(&root.join("scripts"), &mut candidates);

    let mut checked = 0usize;
    let mut offenders = Vec::new();
    for path in candidates {
        let extension = path.extension().and_then(|value| value.to_str());
        if path.file_name().and_then(|value| value.to_str()) != Some("Makefile")
            && !matches!(extension, Some("sh" | "yml" | "yaml"))
        {
            continue;
        }

        let contents = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
        for (line_number, line) in contents.lines().enumerate() {
            let trimmed = line.trim_start();
            if trimmed.starts_with('#')
                || trimmed.starts_with("- name:")
                || trimmed.starts_with("name:")
                || trimmed.starts_with("echo ")
                || !line.contains("cargo audit")
                || line.contains("cargo audit:")
                || line.contains("cargo audit reported")
            {
                continue;
            }

            checked += 1;
            if !line.contains("cargo audit --deny unsound") {
                offenders.push(format!(
                    "{}:{}: {}",
                    path.strip_prefix(&root).unwrap_or(&path).display(),
                    line_number + 1,
                    line.trim()
                ));
            }
        }
    }

    assert!(
        checked >= 5,
        "found only {checked} cargo-audit gates; the scan is probably too narrow"
    );
    assert!(
        offenders.is_empty(),
        "every cargo-audit gate must deny RustSec soundness advisories:\n{}",
        offenders.join("\n")
    );
}

#[test]
fn jsonschema_default_external_resolvers_stay_disabled() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let manifest = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
    let declaration = manifest
        .lines()
        .find(|line| line.trim_start().starts_with("jsonschema ="))
        .expect("Cargo.toml must declare jsonschema");

    assert!(
        declaration.contains("default-features = false"),
        "jsonschema's defaults enable an HTTP retriever that constructs a \
         providerless blocking reqwest client. Keep defaults disabled and \
         reject external references explicitly. Found:\n  {declaration}"
    );
    assert!(
        !declaration.contains("resolve-http") && !declaration.contains("resolve-file"),
        "Streamline does not support external JSON Schema retrieval; do not \
         enable jsonschema's HTTP or file resolvers. Found:\n  {declaration}"
    );
}

#[test]
fn metrics_exporter_does_not_enable_a_second_rustls_provider() {
    let manifest_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let manifest = std::fs::read_to_string(&manifest_path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", manifest_path.display()));
    let dependency = manifest
        .lines()
        .find(|line| {
            line.trim_start()
                .starts_with("metrics-exporter-prometheus =")
        })
        .expect("Cargo.toml must declare metrics-exporter-prometheus");

    assert!(
        dependency.contains("default-features = false"),
        "metrics-exporter-prometheus defaults include push-gateway -> hyper-rustls/aws-lc-rs, \
         while Streamline's TLS stack uses ring. Enabling both providers makes rustls automatic \
         provider selection panic; keep the exporter defaults disabled."
    );
}

#[test]
fn direct_tls_dependencies_use_one_explicit_crypto_provider() {
    let manifest_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let manifest = std::fs::read_to_string(&manifest_path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", manifest_path.display()));

    assert!(
        manifest.contains(
            r#"rustls = { version = "0.23", default-features = false, features = ["std", "aws_lc_rs", "tls12", "logging"] }"#
        ),
        "the direct rustls dependency must select aws-lc-rs explicitly"
    );
    assert!(
        manifest.contains(
            r#"tokio-rustls = { version = "0.26", default-features = false, features = ["aws-lc-rs", "tls12", "logging"] }"#
        ),
        "tokio-rustls must use the same crypto provider as rustls"
    );
    assert!(
        manifest.contains("rustls-tls-webpki-roots-no-provider"),
        "reqwest must not force-enable a competing rustls provider"
    );
    assert!(
        manifest.contains(r#"features = ["runtime-tokio", "rustls-aws-lc-rs"]"#),
        "QUIC must use the same rustls crypto provider"
    );
}

/// The bincode replacement must actually be present. Without this, dropping the
/// dependency altogether would satisfy `lockfile_contains_no_banned_crates`
/// vacuously while breaking the on-disk format guarantees.
#[test]
fn lockfile_contains_the_bincode_replacement() {
    let packages = locked_packages();

    for expected in ["streamline-serde-wincode", "wincode"] {
        assert!(
            packages.iter().any(|p| p.name == expected),
            "Cargo.lock is missing `{expected}`, which produces the bincode 1 wire \
             format that every segment, raft file, time-travel archive and vector \
             store on disk is encoded in. See src/bincode_compat.rs."
        );
    }

    // The *upstream* crate must be gone: its `wincode = ">=0.4, <1"` edge is the
    // whole reason the fork exists, and leaving both in the graph would compile
    // `SerdeCompat` twice against two different `wincode` ABIs.
    assert!(
        !packages.iter().any(|p| p.name == "serde-wincode"),
        "the upstream `serde-wincode` is back in the resolved graph. It requires \
         `wincode = \">=0.4, <1\"`, which Cargo does not unify with `=0.4.9` \
         (0.4/0.5/0.6 are mutually semver-incompatible), so it resolves a second \
         wincode. Depend on `crates/streamline-serde-wincode` instead."
    );
}

/// wincode 0.5.0 raised its `rust-version` to 1.89.0, while this crate's MSRV is
/// 1.88.
///
/// The historical trap: upstream `serde-wincode` 0.1.2 requires
/// `wincode = ">=0.4, <1"`. That is **not** unified with a `=0.4.9` requirement
/// elsewhere — for `0.x` releases Cargo treats the minor as the major, so
/// 0.4/0.5/0.6 are mutually incompatible and Cargo resolves *both* edges
/// independently. A fresh consumer therefore got
/// `streamline -> wincode 0.4.9` **and** `serde-wincode -> wincode 0.6.1`.
///
/// This file previously asserted the opposite ("Cargo unifies the two
/// requirements"), and verified it with `cargo metadata --locked --offline` —
/// which only re-read this repository's `Cargo.lock` and so could never have
/// caught it. The constraint now lives in a *published* manifest, the
/// `streamline-serde-wincode` fork, whose `wincode` requirement is exactly
/// `=0.4.9`; and it is verified by resolving a real consumer with no lockfile in
/// `fresh_consumer_resolves_one_wincode_from_the_packaged_crates`.
const MAX_WINCODE_EXCLUSIVE: (u64, u64) = (0, 5);

/// The exact requirement both the root manifest and the fork must carry.
const WINCODE_REQUIREMENT: &str = "=0.4.9";

/// The workspace fork that owns the `serde` bridge, and the alias the root
/// manifest imports it under.
const FORK_PACKAGE: &str = "streamline-serde-wincode";
const FORK_ALIAS: &str = "serde-wincode";

#[test]
fn wincode_stays_msrv_compatible() {
    let packages = locked_packages();
    let resolved: Vec<_> = packages.iter().filter(|p| p.name == "wincode").collect();

    assert_eq!(
        resolved.len(),
        1,
        "expected exactly one resolved `wincode`, found {}: {:?}. Two versions means \
         something re-widened the requirement; collapse it with \
         `cargo update -p wincode@<new> --precise <old>`.",
        resolved.len(),
        resolved.iter().map(|p| &p.version).collect::<Vec<_>>()
    );

    let version = major_minor(&resolved[0].version);
    assert!(
        version < MAX_WINCODE_EXCLUSIVE,
        "Cargo.lock resolved wincode {} but versions >= {}.{} require Rust 1.89.0, above \
         this crate's MSRV of 1.88. Run \
         `cargo update -p wincode@{} --precise 0.4.9`, or raise the MSRV \
         (Cargo.toml `rust-version`, rust-toolchain.toml and CI) first.",
        resolved[0].version,
        MAX_WINCODE_EXCLUSIVE.0,
        MAX_WINCODE_EXCLUSIVE.1,
        resolved[0].version,
    );
}

/// The MSRV restriction has to survive publication, so it must be a *direct*
/// requirement in this crate's manifest — not just a lockfile entry.
///
/// `src/bincode_compat.rs` builds its `wincode::config::Configuration` from this
/// direct dependency and hands it to the fork's `SerdeCompat`, so if the two
/// ever resolved to different `wincode` crates the trait bounds would not line
/// up and the build would fail loudly instead of silently changing the on-disk
/// format.
#[test]
fn root_manifest_pins_wincode_exactly() {
    let manifest_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let manifest = std::fs::read_to_string(&manifest_path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", manifest_path.display()));

    let declaration = manifest
        .lines()
        .find(|line| line.trim_start().starts_with("wincode ="))
        .expect(
            "Cargo.toml must declare a direct `wincode` dependency, so that an ABI \
             mismatch with the serde bridge is a compile error rather than a silent \
             change to the on-disk format.",
        );

    assert!(
        declaration.contains(&format!("version = \"{WINCODE_REQUIREMENT}\"")),
        "the direct `wincode` dependency must use the exact requirement \
         `version = \"{WINCODE_REQUIREMENT}\"`; found:\n  {declaration}",
    );

    // The serde bridge must be the workspace fork, imported under the
    // `serde-wincode` alias so `src/bincode_compat.rs` keeps compiling unchanged.
    let bridge = manifest
        .lines()
        .find(|line| line.trim_start().starts_with(&format!("{FORK_ALIAS} =")))
        .unwrap_or_else(|| panic!("Cargo.toml must declare the `{FORK_ALIAS}` dependency"));

    assert!(
        bridge.contains(&format!("package = \"{FORK_PACKAGE}\"")),
        "the `{FORK_ALIAS}` dependency must resolve to the workspace fork \
         `{FORK_PACKAGE}`, whose published manifest pins wincode exactly. Upstream \
         `serde-wincode` requires `>=0.4, <1`, which Cargo resolves independently of \
         the exact pin above. Found:\n  {bridge}",
    );
    assert!(
        bridge.contains("path = \"crates/streamline-serde-wincode\"")
            && bridge.contains("version = \""),
        "the fork must be declared with both `path` and `version`, or `cargo publish` \
         rejects it. Found:\n  {bridge}",
    );

    // And the fork itself must carry the exact requirement — this is the edge
    // that actually reaches a downstream consumer.
    let fork_manifest_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("crates/streamline-serde-wincode/Cargo.toml");
    let fork = std::fs::read_to_string(&fork_manifest_path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", fork_manifest_path.display()));
    let fork_wincode = fork
        .lines()
        .find(|line| line.trim_start().starts_with("wincode ="))
        .expect("the fork must declare a `wincode` dependency");
    assert!(
        fork_wincode.contains(&format!("version = \"{WINCODE_REQUIREMENT}\"")),
        "the fork's `wincode` requirement must be exactly `{WINCODE_REQUIREMENT}` — it is \
         the only thing that constrains this edge for a consumer of the published \
         crate. Found:\n  {fork_wincode}",
    );

    // The bridge is what makes the exact pin load-bearing rather than decorative:
    // it must consume the direct crate, not the bridge's re-export.
    let bincode_compat_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/bincode_compat.rs");
    let bincode_compat = std::fs::read_to_string(&bincode_compat_path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", bincode_compat_path.display()));

    assert!(
        bincode_compat.contains("use wincode::config::"),
        "src/bincode_compat.rs must import the direct `wincode` crate, so that a graph \
         in which the serde bridge resolved a different wincode fails to compile"
    );
    assert!(
        !bincode_compat.contains("use serde_wincode::{wincode"),
        "src/bincode_compat.rs must not reach wincode through the bridge's re-export: \
         that would make any resolved wincode version compile silently"
    );
}

/// Run `cargo` with the given arguments in the workspace root.
fn run_cargo(args: &[&str]) -> std::process::Output {
    std::process::Command::new(env!("CARGO"))
        .args(args)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        // Keep the nested invocation out of the outer build's target directory,
        // whose lock this test process is holding.
        .env("CARGO_TARGET_DIR", packaging_target_dir())
        .env_remove("RUSTC_WRAPPER")
        .output()
        .unwrap_or_else(|e| panic!("failed to run `cargo {}`: {e}", args.join(" ")))
}

fn packaging_target_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/packaging")
}

/// Resolving *this* workspace proves nothing about a published crate: the
/// resolution is bound by `Cargo.lock`, which no consumer ever sees. This is the
/// sanity check that the repository's own graph is consistent; the
/// publication-surviving proof is
/// `fresh_consumer_resolves_one_wincode_from_the_packaged_crates`.
#[test]
fn workspace_resolves_a_single_wincode() {
    let output = run_cargo(&["metadata", "--format-version", "1", "--locked", "--offline"]);
    assert!(
        output.status.success(),
        "`cargo metadata --locked` failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let metadata: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("cargo metadata emitted invalid JSON");

    let nodes = metadata["resolve"]["nodes"]
        .as_array()
        .expect("cargo metadata resolve.nodes");

    let wincode_ids: Vec<&str> = nodes
        .iter()
        .filter_map(|node| node["id"].as_str())
        .filter(|id| package_name_of(id) == "wincode")
        .collect();

    assert_eq!(
        wincode_ids.len(),
        1,
        "expected exactly one `wincode` in the resolved graph, found {wincode_ids:?}"
    );
    let wincode_id = wincode_ids[0];
    assert!(
        wincode_id.ends_with("wincode@0.4.9"),
        "the resolved wincode must be 0.4.9; found {wincode_id}"
    );

    // The fork's edge and the root's direct edge must be the same node, or
    // `src/bincode_compat.rs` would hand a configuration from one `wincode` to a
    // `SerdeCompat` compiled against another.
    for owner in [FORK_PACKAGE, "streamline"] {
        let node = nodes
            .iter()
            .find(|node| {
                node["id"]
                    .as_str()
                    .is_some_and(|id| package_name_of(id) == owner)
            })
            .unwrap_or_else(|| panic!("the resolved graph must contain `{owner}`"));

        let targets: Vec<&str> = node["dependencies"]
            .as_array()
            .unwrap_or_else(|| panic!("`{owner}` dependencies"))
            .iter()
            .filter_map(|dep| dep.as_str())
            .collect();

        assert!(
            targets.contains(&wincode_id),
            "`{owner}` resolves to a different wincode than the pinned one.\n  \
             pinned: {wincode_id}\n  {owner} -> {targets:?}"
        );
    }
}

/// Extract the package name from a `cargo metadata` package id.
///
/// Registry ids look like `registry+https://...#wincode@0.4.9`; path ids drop
/// the name from the fragment when it matches the directory
/// (`path+file:///…/streamline#0.3.0`); older ids look like
/// `wincode 0.4.9 (registry+…)`.
fn package_name_of(id: &str) -> &str {
    // Legacy shape: `wincode 0.4.9 (registry+…)`. Package ids never contain a
    // space in any other form.
    if let Some((name, _)) = id.split_once(' ') {
        return name;
    }

    let (head, fragment) = match id.split_once('#') {
        Some((head, fragment)) => (head, Some(fragment)),
        None => (id, None),
    };

    if let Some(fragment) = fragment {
        let candidate = fragment.split('@').next().unwrap_or(fragment);
        if !candidate.is_empty() && !candidate.starts_with(|c: char| c.is_ascii_digit()) {
            return candidate;
        }
        // The fragment was only a version, so the name is the last path segment.
    }

    let head = head.trim_end_matches('/');
    let last = head.rsplit('/').next().unwrap_or(head);
    last.split([' ', '@']).next().unwrap_or(last)
}

#[test]
fn package_ids_are_parsed_in_every_shape_cargo_emits() {
    assert_eq!(
        package_name_of("registry+https://github.com/rust-lang/crates.io-index#wincode@0.4.9"),
        "wincode"
    );
    assert_eq!(
        package_name_of(
            "registry+https://github.com/rust-lang/crates.io-index#serde-wincode@0.1.2"
        ),
        "serde-wincode"
    );
    assert_eq!(
        package_name_of("path+file:///home/user/streamline#0.3.0"),
        "streamline"
    );
    assert_eq!(
        package_name_of("path+file:///home/user/streamline/crates/streamline-wasm#0.3.0"),
        "streamline-wasm"
    );
    assert_eq!(
        package_name_of("wincode 0.4.9 (registry+https://github.com/rust-lang/crates.io-index)"),
        "wincode"
    );
}

/// The unpublished workspace members, as `--config patch.crates-io...` overrides.
///
/// These stand in for the publish order that
/// `publish_workflow_orders_member_crates_before_the_root_crate` enforces: the
/// member crates are not on crates.io yet, so the root crate's packaged manifest
/// cannot be resolved without pointing those edges back at the local paths. They
/// affect resolution only — never the emitted manifest, which is what these
/// tests read back.
const MEMBER_PATCHES: &[&str] = &[
    r#"patch.crates-io.streamline-analytics.path="crates/streamline-analytics""#,
    r#"patch.crates-io.streamline-wasm.path="crates/streamline-wasm""#,
    r#"patch.crates-io.streamline-serde-wincode.path="crates/streamline-serde-wincode""#,
];

/// `cargo package` a workspace member and return the path to the `.crate` file.
///
/// `--no-verify` skips the compile of the packaged crate, which is what makes
/// this cheap enough to keep in the test suite; `--allow-dirty` is required
/// because release preparation runs with a dirty worktree.
///
/// `with_patches` applies [`MEMBER_PATCHES`]; only the root crate needs them,
/// and passing them elsewhere just produces "patch was not used" warnings.
fn package_crate(name: &str, version: &str, with_patches: bool) -> PathBuf {
    let mut args = vec![
        "package",
        "-p",
        name,
        "--allow-dirty",
        "--no-verify",
        "--locked",
        "--offline",
    ];
    if with_patches {
        for patch in MEMBER_PATCHES {
            args.push("--config");
            args.push(patch);
        }
    }

    let output = run_cargo(&args);
    assert!(
        output.status.success(),
        "`cargo package -p {name}` failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let crate_file = packaging_target_dir()
        .join("package")
        .join(format!("{name}-{version}.crate"));
    assert!(
        crate_file.exists(),
        "cargo package did not produce {}",
        crate_file.display()
    );
    crate_file
}

/// Every publishable crate in this workspace, in dependency order — the same
/// order `.github/workflows/publish-crate.yml` publishes them in.
const PUBLISHABLE_CRATES: &[&str] = &[
    FORK_PACKAGE,
    "streamline-wasm",
    "streamline-analytics",
    "streamline",
];

/// The packaged crates, built exactly once per test binary.
///
/// `cargo package` takes a lock on a shared scratch directory and writes each
/// crate to a single fixed path, so two tests packaging concurrently race:
/// one moves the tarball into place while the other is reading it ("truncated
/// gzip input"). Packaging once and sharing the result removes the race.
///
/// The map is keyed by package name; the second field is the shared workspace
/// version.
fn packaged_crates() -> &'static (BTreeMap<&'static str, PathBuf>, String) {
    static PACKAGED: std::sync::OnceLock<(BTreeMap<&'static str, PathBuf>, String)> =
        std::sync::OnceLock::new();
    PACKAGED.get_or_init(|| {
        let version = package_version_from_manifest();
        let mut packaged = BTreeMap::new();
        for name in PUBLISHABLE_CRATES {
            // Only the root crate has `{ path, version }` edges that need the
            // patch overrides; passing them elsewhere just warns.
            let with_patches = *name == "streamline";
            packaged.insert(*name, package_crate(name, &version, with_patches));
        }
        (packaged, version)
    })
}

/// The packaged `.crate` file for one publishable crate.
fn packaged_crate(name: &str) -> &'static Path {
    let (packaged, _) = packaged_crates();
    packaged
        .get(name)
        .unwrap_or_else(|| panic!("`{name}` is not in PUBLISHABLE_CRATES"))
}

/// The shared workspace version, as packaged.
fn packaged_version() -> &'static str {
    &packaged_crates().1
}

/// Read one file out of a packaged `.crate`.
fn read_from_crate(crate_file: &Path, inner_path: &str) -> String {
    let extracted = std::process::Command::new("tar")
        .arg("-xzOf")
        .arg(crate_file)
        .arg(inner_path)
        .output()
        .expect("failed to run tar on the packaged crate");
    assert!(
        extracted.status.success(),
        "could not read {inner_path} from {}:\n{}",
        crate_file.display(),
        String::from_utf8_lossy(&extracted.stderr)
    );
    String::from_utf8(extracted.stdout).expect("packaged file is not UTF-8")
}

/// The `version` requirement from a normalised `[dependencies.<name>]` table.
///
/// `cargo package` rewrites dependencies into that table form, so assert on it
/// rather than on the inline form the source manifest uses.
fn packaged_dependency_block<'a>(manifest: &'a str, dependency: &str) -> &'a str {
    manifest
        .split("[dependencies.")
        .find(|block| {
            block
                .split(']')
                .next()
                .is_some_and(|name| name == dependency)
        })
        .unwrap_or_else(|| {
            panic!("the packaged manifest has no `[dependencies.{dependency}]` table:\n{manifest}")
        })
}

fn requirement_of(block: &str) -> &str {
    block
        .lines()
        .find(|line| line.trim_start().starts_with("version ="))
        .expect("dependency block has no version requirement")
        .trim()
}

/// The exact requirement must survive `cargo package`'s manifest normalisation,
/// since that normalised manifest — not this repository's — is what a downstream
/// consumer resolves against.
///
/// Both crates are checked: the root's direct `wincode` edge, and — the one that
/// actually matters — the fork's, because that is the edge upstream
/// `serde-wincode` left permissive.
#[test]
#[cfg_attr(not(unix), ignore = "the packaged .crate is read with tar")]
fn packaged_manifests_preserve_the_exact_wincode_requirement() {
    let fork_crate = packaged_crate(FORK_PACKAGE);
    let root_crate = packaged_crate("streamline");
    let version = packaged_version();

    // ── the fork ────────────────────────────────────────────────────────────
    let fork_manifest =
        read_from_crate(fork_crate, &format!("{FORK_PACKAGE}-{version}/Cargo.toml"));

    let fork_wincode = requirement_of(packaged_dependency_block(&fork_manifest, "wincode"));
    assert!(
        fork_wincode.contains(WINCODE_REQUIREMENT),
        "the packaged fork relaxed its wincode requirement to `{fork_wincode}`. This is the \
         edge upstream `serde-wincode` declared as `>=0.4, <1`; if it is not exact, a \
         consumer resolves a second, semver-incompatible wincode alongside the root's \
         0.4.9 — a different ABI and rust-version 1.89.0."
    );

    // The fork must carry its attribution into the package, since it is a
    // redistribution of Apache-2.0 licensed work.
    let listing = std::process::Command::new("tar")
        .arg("-tzf")
        .arg(fork_crate)
        .output()
        .expect("failed to list the packaged fork");
    let listing = String::from_utf8_lossy(&listing.stdout);
    for required in ["LICENSE", "NOTICE", "README.md"] {
        assert!(
            listing
                .lines()
                .any(|entry| entry.ends_with(&format!("/{required}"))),
            "the packaged fork must include `{required}` — it redistributes \
             Apache-2.0 licensed work and needs the licence and attribution with it.\n{listing}"
        );
    }

    // ── the root ────────────────────────────────────────────────────────────
    let root_manifest = read_from_crate(root_crate, &format!("streamline-{version}/Cargo.toml"));

    let root_wincode = requirement_of(packaged_dependency_block(&root_manifest, "wincode"));
    assert!(
        root_wincode.contains(WINCODE_REQUIREMENT),
        "the packaged root manifest relaxed the wincode requirement to `{root_wincode}`"
    );

    // The bridge must be the fork, reached through the `serde-wincode` alias,
    // and the `path` must have been stripped in favour of the version.
    let bridge = packaged_dependency_block(&root_manifest, FORK_ALIAS);
    assert!(
        bridge
            .lines()
            .any(|line| line.trim() == format!("package = \"{FORK_PACKAGE}\"")),
        "the packaged root manifest must import `{FORK_PACKAGE}` under the \
         `{FORK_ALIAS}` alias; found:\n{bridge}"
    );
    assert!(
        requirement_of(bridge).contains(version),
        "the packaged `{FORK_ALIAS}` dependency must request the coordinated \
         workspace version {version}; found:\n{}",
        requirement_of(bridge)
    );
    assert!(
        !bridge
            .lines()
            .any(|line| line.trim_start().starts_with("path =")),
        "`cargo package` must strip the `path` from the published dependency, \
         otherwise consumers cannot resolve it:\n{bridge}"
    );

    // Upstream must be gone from the published manifest entirely.
    assert!(
        !root_manifest.contains("package = \"serde-wincode\"")
            && !root_manifest.contains("[dependencies.serde-wincode]\nversion = \"0.1"),
        "the packaged root manifest still references the upstream `serde-wincode`:\n{root_manifest}"
    );
}

/// Every MSRV-sensitive requirement must survive `cargo package`'s manifest
/// normalisation, because that normalised manifest is what crates.io serves and
/// what a fresh consumer resolves against.
///
/// This is the published-surface counterpart of
/// `root_manifest_pins_msrv_sensitive_dependencies_exactly`, which only reads
/// the source manifest.
#[test]
#[cfg_attr(not(unix), ignore = "the packaged .crate is read with tar")]
fn packaged_root_manifest_preserves_every_msrv_pin() {
    let root_crate = packaged_crate("streamline");
    let version = packaged_version();
    let root_manifest = read_from_crate(root_crate, &format!("streamline-{version}/Cargo.toml"));

    for pin in MSRV_SENSITIVE_PINS {
        if pin.manifest != "Cargo.toml" {
            continue;
        }
        let block = packaged_dependency_block(&root_manifest, pin.name);
        let requirement = requirement_of(block);
        assert!(
            requirement.contains(pin.requirement),
            "the packaged root manifest relaxed the `{}` requirement to `{requirement}` \
             (expected `{}`). {} — without the exact requirement in the *published* \
             manifest, a fresh consumer floats past this crate's MSRV of 1.88.",
            pin.name,
            pin.requirement,
            pin.why,
        );
    }

    // The optional MSRV-floor dependencies must remain optional, or they would
    // be pulled into every default build for no reason.
    for optional in ["crc-fast", "crc", "async-graphql-derive"] {
        let block = packaged_dependency_block(&root_manifest, optional);
        assert!(
            block.lines().any(|line| line.trim() == "optional = true"),
            "`{optional}` exists only to bound a feature-gated graph and must stay \
             optional in the published manifest:\n{block}"
        );
    }
}

/// The `version` from the root `[package]` table.
fn package_version_from_manifest() -> String {
    let manifest_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let manifest = std::fs::read_to_string(&manifest_path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", manifest_path.display()));

    manifest
        .lines()
        .skip_while(|line| line.trim() != "[package]")
        .find(|line| line.trim_start().starts_with("version ="))
        .and_then(|line| line.split('"').nth(1))
        .expect("root Cargo.toml must declare a package version")
        .to_string()
}

/// A dependency whose newer releases raise `rust-version` above this crate's
/// MSRV of 1.88, and which therefore carries an **exact** requirement in a
/// manifest that is itself published.
///
/// The distinction matters. `Cargo.lock` binds this repository and nothing
/// else: a consumer of the published `streamline` resolves from scratch against
/// the normalised manifests crates.io serves, so a lockfile pin is invisible to
/// them. Everything listed here is therefore expressed as `version = "=x.y.z"`
/// in [`Cargo.toml`] (or, for `wincode`'s serde bridge, in the published
/// `streamline-serde-wincode` fork), not merely recorded in the lockfile.
struct MsrvPin {
    /// Crate name, as it appears in `Cargo.lock` and in the manifest.
    name: &'static str,
    /// The first release that requires a toolchain newer than the MSRV.
    first_too_new: (u64, u64, u64),
    /// The exact requirement the manifest must carry.
    requirement: &'static str,
    /// The manifest that must carry it, relative to the workspace root.
    manifest: &'static str,
    /// Why the edge would otherwise float, for the failure message.
    why: &'static str,
}

/// Every MSRV-sensitive edge, and the published manifest that constrains it.
const MSRV_SENSITIVE_PINS: &[MsrvPin] = &[
    // async-graphql 7.0.18 raised `rust-version` to 1.89.0; 7.2.x also pulls
    // `asynk-strim` (likewise 1.89).
    MsrvPin {
        name: "async-graphql",
        first_too_new: (7, 0, 18),
        requirement: "=7.0.17",
        manifest: "Cargo.toml",
        why: "the `graphql` feature's direct dependency",
    },
    // async-graphql asks for its own companion crates with *caret* requirements
    // (`7.0.17` == `^7.0.17`), so pinning the facade alone lets the derive,
    // parser and value crates float to the 7.2.x line. That is exactly what
    // this repository's lockfile had recorded before these pins existed.
    MsrvPin {
        name: "async-graphql-derive",
        first_too_new: (7, 0, 18),
        requirement: "=7.0.17",
        manifest: "Cargo.toml",
        why: "async-graphql depends on it with a caret requirement",
    },
    MsrvPin {
        name: "async-graphql-parser",
        first_too_new: (7, 0, 18),
        requirement: "=7.0.17",
        manifest: "Cargo.toml",
        why: "async-graphql depends on it with a caret requirement",
    },
    MsrvPin {
        name: "async-graphql-value",
        first_too_new: (7, 0, 18),
        requirement: "=7.0.17",
        manifest: "Cargo.toml",
        why: "async-graphql depends on it with a caret requirement",
    },
    // crc-fast >= 1.10.0 requires rustc 1.89. object_store asks for `^1.6`.
    MsrvPin {
        name: "crc-fast",
        first_too_new: (1, 10, 0),
        requirement: "=1.9.0",
        manifest: "Cargo.toml",
        why: "object_store depends on it with `^1.6`",
    },
    // crc-fast 1.9.0 requires `crc = "~3.3"`; kafka-protocol asks for `^3.0.0`.
    // Pinning keeps the two consistent regardless of which edge dominates.
    MsrvPin {
        name: "crc",
        first_too_new: (3, 4, 0),
        requirement: "=3.3.0",
        manifest: "Cargo.toml",
        why: "crc-fast requires `~3.3` while kafka-protocol allows `^3.0.0`",
    },
    // wincode 0.5.0 raised `rust-version` to 1.89.0. Constrained twice: by the
    // root's direct dependency and by the published fork that owns the serde
    // bridge (see `root_manifest_pins_wincode_exactly`).
    MsrvPin {
        name: "wincode",
        first_too_new: (0, 5, 0),
        requirement: "=0.4.9",
        manifest: "Cargo.toml",
        why: "upstream serde-wincode declares `>=0.4, <1`, which Cargo does not unify",
    },
];

#[test]
fn msrv_sensitive_pins_are_respected() {
    let packages = locked_packages();

    for pin in MSRV_SENSITIVE_PINS {
        let name = pin.name;
        let resolved: Vec<_> = packages.iter().filter(|p| p.name == name).collect();

        assert_eq!(
            resolved.len(),
            1,
            "expected exactly one resolved `{name}`, found {}: {:?}. Multiple versions \
             mean something re-widened the requirement; the manifest requirement is \
             `{}`.",
            resolved.len(),
            resolved.iter().map(|p| &p.version).collect::<Vec<_>>(),
            pin.requirement,
        );

        let version = version_triple(&resolved[0].version);
        assert!(
            version < pin.first_too_new,
            "Cargo.lock resolved {name} {} but versions >= {}.{}.{} require a toolchain \
             newer than this crate's MSRV of 1.88, which breaks \
             `cargo clippy --all-targets --all-features --locked`. The manifest \
             requirement is `{}` ({}); do not widen it without raising the MSRV \
             (Cargo.toml `rust-version`, rust-toolchain.toml and CI) first.",
            resolved[0].version,
            pin.first_too_new.0,
            pin.first_too_new.1,
            pin.first_too_new.2,
            pin.requirement,
            pin.why,
        );
    }
}

/// The lockfile check above is necessary but not sufficient: it says nothing
/// about a consumer of the *published* crate. Every MSRV-sensitive edge must
/// therefore carry its exact requirement in a manifest that is published.
///
/// This is the regression that the previous "Cargo.lock pins it" comments left
/// open: a fresh consumer never sees this repository's lockfile.
#[test]
fn root_manifest_pins_msrv_sensitive_dependencies_exactly() {
    for pin in MSRV_SENSITIVE_PINS {
        let manifest_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(pin.manifest);
        let manifest = std::fs::read_to_string(&manifest_path)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", manifest_path.display()));

        let declaration = manifest
            .lines()
            .find(|line| line.trim_start().starts_with(&format!("{} =", pin.name)))
            .unwrap_or_else(|| {
                panic!(
                    "{} must declare a direct `{}` dependency so the exact requirement \
                     `{}` survives publication ({})",
                    pin.manifest, pin.name, pin.requirement, pin.why,
                )
            });

        assert!(
            declaration.contains(&format!("version = \"{}\"", pin.requirement)),
            "{}: `{}` must use the exact requirement `version = \"{}\"` — a lockfile pin \
             does not travel with a published crate ({}). Found:\n  {declaration}",
            pin.manifest,
            pin.name,
            pin.requirement,
            pin.why,
        );

        // A `=x.y.z` requirement must also be the version the lockfile settled
        // on, or the two sources of truth have drifted.
        assert_eq!(
            version_triple(pin.requirement),
            version_triple(
                &locked_packages()
                    .into_iter()
                    .find(|p| p.name == pin.name)
                    .unwrap_or_else(|| panic!("`{}` must be in Cargo.lock", pin.name))
                    .version
            ),
            "the manifest requirement for `{}` and the locked version disagree",
            pin.name,
        );
    }
}

/// The two families above must be *activated* by the feature that pulls the
/// crate whose permissive edge they exist to bound. A pin that is never
/// activated constrains nothing.
#[test]
fn msrv_pins_are_activated_by_the_feature_that_needs_them() {
    let manifest_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let manifest = std::fs::read_to_string(&manifest_path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", manifest_path.display()));

    let feature_line = |feature: &str| -> String {
        manifest
            .lines()
            .find(|line| line.trim_start().starts_with(&format!("{feature} = [")))
            .unwrap_or_else(|| panic!("Cargo.toml must define the `{feature}` feature"))
            .to_string()
    };

    // `cloud-storage` pulls object_store, whose `crc-fast = "^1.6"` edge floats.
    let cloud_storage = feature_line("cloud-storage");
    for dep in ["dep:object_store", "dep:crc-fast", "dep:crc"] {
        assert!(
            cloud_storage.contains(dep),
            "the `cloud-storage` feature must activate `{dep}` so the exact MSRV \
             requirement applies wherever object_store is used. Found:\n  {cloud_storage}"
        );
    }

    // `graphql` pulls async-graphql, whose companion-crate edges float.
    let graphql = feature_line("graphql");
    for dep in [
        "dep:async-graphql",
        "dep:async-graphql-derive",
        "dep:async-graphql-parser",
        "dep:async-graphql-value",
    ] {
        assert!(
            graphql.contains(dep),
            "the `graphql` feature must activate `{dep}` so the exact MSRV requirement \
             applies wherever async-graphql is used. Found:\n  {graphql}"
        );
    }
}

#[test]
fn lockfile_parser_extracts_name_and_version() {
    // Guards the assertions above against a silently broken parser.
    let sample = r#"
version = 4

[[package]]
name = "alpha"
version = "1.2.3"
dependencies = [
 "beta",
]

[[package]]
name = "beta"
version = "0.41.0"
source = "registry+https://github.com/rust-lang/crates.io-index"
"#;
    let packages = parse_lockfile(sample);
    assert_eq!(packages.len(), 2);
    assert_eq!(packages[0].name, "alpha");
    assert_eq!(packages[0].version, "1.2.3");
    assert_eq!(packages[1].name, "beta");
    assert_eq!(packages[1].version, "0.41.0");
    assert_eq!(major_minor("0.36.2"), (0, 36));
    assert!(major_minor("0.41.0") >= MIN_QUICK_XML);
    assert!(major_minor("0.37.5") < MIN_QUICK_XML);
}

// ── Publication-surviving resolution proof ──────────────────────────────────

/// Where the fresh-consumer fixtures are built. Inside `target/`, so it is
/// ignored by git and removed by `cargo clean`.
fn fresh_consumer_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/fresh-consumer")
}

/// Extract a packaged `.crate` into `dest` and return the unpacked directory.
///
/// The `Cargo.lock` that `cargo package` embeds is **deleted**. Cargo ignores a
/// dependency's lockfile anyway, but removing it makes it impossible for this
/// test to be accidentally re-reading a resolution decided elsewhere — which is
/// the exact failure mode of the test this one replaces.
fn unpack_crate(crate_file: &Path, dest: &Path, name: &str, version: &str) -> PathBuf {
    std::fs::create_dir_all(dest).expect("create vendor dir");
    let status = std::process::Command::new("tar")
        .arg("-xzf")
        .arg(crate_file)
        .arg("-C")
        .arg(dest)
        .status()
        .expect("failed to unpack the packaged crate");
    assert!(status.success(), "tar failed on {}", crate_file.display());

    let unpacked = dest.join(format!("{name}-{version}"));
    assert!(unpacked.is_dir(), "{} was not unpacked", unpacked.display());
    let _ = std::fs::remove_file(unpacked.join("Cargo.lock"));
    assert!(
        !unpacked.join("Cargo.lock").exists(),
        "the unpacked crate must carry no lockfile into the fresh resolution"
    );
    unpacked
}

/// The unpacked, lockfile-free package sources for every publishable crate.
///
/// Built once per test binary. Each entry is the directory `cargo package`'s
/// tarball expanded to — i.e. exactly the source tree crates.io would serve —
/// with its embedded `Cargo.lock` removed.
fn unpacked_packages() -> &'static BTreeMap<&'static str, PathBuf> {
    static UNPACKED: std::sync::OnceLock<BTreeMap<&'static str, PathBuf>> =
        std::sync::OnceLock::new();
    UNPACKED.get_or_init(|| {
        let version = packaged_version();
        let vendor = fresh_consumer_dir().join("vendor");
        let _ = std::fs::remove_dir_all(&vendor);
        let mut unpacked = BTreeMap::new();
        for name in PUBLISHABLE_CRATES {
            unpacked.insert(
                *name,
                unpack_crate(packaged_crate(name), &vendor, name, version),
            );
        }
        unpacked
    })
}

/// `[patch.crates-io]` lines pointing every unpublished Streamline crate at its
/// **unpacked package**, not at the working tree.
///
/// These exist for one reason only: the workspace crates are not on crates.io
/// yet, so a consumer of the packaged root cannot resolve them at all. They are
/// *not* the fix for any of the MSRV edges — those are exact requirements in
/// published manifests, which is the whole point. Pointing at the unpacked
/// packages (rather than at `crates/…`) keeps the requirements under test the
/// published ones.
fn unpublished_crate_patches() -> String {
    let mut patches = String::from("[patch.crates-io]\n");
    for (name, dir) in unpacked_packages() {
        patches.push_str(&format!("{name} = {{ path = \"{}\" }}\n", dir.display()));
    }
    patches
}

/// A resolved fresh consumer: the `cargo metadata` graph plus the `Cargo.lock`
/// that cargo generated for it.
struct FreshResolution {
    metadata: serde_json::Value,
    lockfile: String,
}

impl FreshResolution {
    /// Every resolved `(name, version)` pair, from the generated lockfile.
    fn locked(&self) -> Vec<LockedPackage> {
        parse_lockfile(&self.lockfile)
    }

    /// Versions of `name` in the resolved graph, from `cargo metadata`.
    fn versions_of(&self, name: &str) -> Vec<String> {
        self.metadata["packages"]
            .as_array()
            .expect("cargo metadata packages")
            .iter()
            .filter(|package| package["name"].as_str() == Some(name))
            .filter_map(|package| package["version"].as_str().map(str::to_owned))
            .collect()
    }
}

/// The cargo this test drives the fresh resolution with.
///
/// `rust-toolchain.toml` pins the workspace to the MSRV, so under a normal
/// `cargo test` this *is* the 1.88 toolchain. The assertion makes that explicit:
/// "resolves on 1.88" is only a claim if the resolver really was 1.88, and a
/// newer cargo would additionally apply MSRV-aware resolution, which would mask
/// exactly the floating-edge defect these tests exist to catch.
fn assert_cargo_is_the_msrv_toolchain() {
    let output = std::process::Command::new(env!("CARGO"))
        .arg("--version")
        .output()
        .expect("failed to run `cargo --version`");
    let version = String::from_utf8_lossy(&output.stdout);
    let reported = version
        .split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .to_string();
    assert!(
        reported.starts_with(&format!("{MSRV}.")) || reported == MSRV,
        "this test must run on the MSRV toolchain ({MSRV}), because a newer cargo \
         applies MSRV-aware resolution and would hide a floating edge. Got: {}. \
         Run it via `rustup run {MSRV}.0 cargo test` or let rust-toolchain.toml \
         select the toolchain.",
        version.trim(),
    );
}

/// This crate's MSRV, as `major.minor`.
const MSRV: &str = "1.88";

/// Write a throwaway consumer crate and resolve it with **no lockfile**.
///
/// Generates a brand-new `Cargo.lock` with `cargo generate-lockfile` and then
/// reads the graph back with `cargo metadata --locked`, so the assertions are
/// made against a resolution this test produced from published manifests — never
/// against this repository's lockfile.
fn resolve_fresh_consumer(name: &str, manifest: &str) -> FreshResolution {
    let dir = fresh_consumer_dir().join(name);
    // Start from nothing: any leftover `Cargo.lock` would defeat the point.
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("src")).expect("create consumer dir");
    std::fs::write(dir.join("Cargo.toml"), manifest).expect("write consumer manifest");
    std::fs::write(dir.join("src/main.rs"), "fn main() {}\n").expect("write consumer main");
    assert!(
        !dir.join("Cargo.lock").exists(),
        "the fresh consumer must start without a lockfile"
    );

    let cargo = |args: &[&str]| {
        std::process::Command::new(env!("CARGO"))
            .args(args)
            .current_dir(&dir)
            .env("CARGO_TARGET_DIR", fresh_consumer_dir().join("target"))
            .env("CARGO_INCREMENTAL", "0")
            .env_remove("RUSTC_WRAPPER")
            .output()
            .unwrap_or_else(|e| panic!("failed to run `cargo {}`: {e}", args.join(" ")))
    };

    let generated = cargo(&["generate-lockfile"]);
    assert!(
        generated.status.success(),
        "the fresh consumer `{name}` failed to resolve. This test needs the crates.io \
         index (directly or from the local registry cache):\n{}",
        String::from_utf8_lossy(&generated.stderr)
    );

    let lockfile = std::fs::read_to_string(dir.join("Cargo.lock"))
        .expect("cargo generate-lockfile must produce a Cargo.lock");

    // `--locked` proves the graph below is the one just generated, and that it
    // is internally consistent with the published manifests.
    let output = cargo(&["metadata", "--format-version", "1", "--locked"]);
    assert!(
        output.status.success(),
        "`cargo metadata --locked` failed for the fresh consumer `{name}`:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    FreshResolution {
        metadata: serde_json::from_slice(&output.stdout)
            .expect("cargo metadata emitted invalid JSON"),
        lockfile,
    }
}

/// Every `wincode` package in a resolved graph, as `(name, version)`.
fn wincode_packages(metadata: &serde_json::Value) -> Vec<(String, String)> {
    metadata["packages"]
        .as_array()
        .expect("cargo metadata packages")
        .iter()
        .filter_map(|package| {
            let name = package["name"].as_str()?;
            let version = package["version"].as_str()?;
            (name == "wincode").then(|| (name.to_string(), version.to_string()))
        })
        .collect()
}

/// A consumer manifest for the packaged root crate with `features` enabled.
fn root_consumer_manifest(name: &str, features: &[&str]) -> String {
    let feature_list = features
        .iter()
        .map(|f| format!("\"{f}\""))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "[workspace]\n\
         resolver = \"2\"\n\n\
         [package]\n\
         name = \"{name}\"\n\
         version = \"0.0.0\"\n\
         edition = \"2021\"\n\n\
         [dependencies]\n\
         streamline = {{ version = \"{version}\", features = [{feature_list}] }}\n\n\
         {patches}",
        version = packaged_version(),
        patches = unpublished_crate_patches(),
    )
}

/// **The proof that survives publication.**
///
/// A `Cargo.lock` binds this repository and nothing else; a consumer of the
/// published crates resolves from scratch against the *normalised* manifests
/// crates.io serves. So this packages every publishable crate for real, unpacks
/// them, deletes the embedded lockfiles, and resolves a brand-new consumer that
/// has never seen this repository's `Cargo.lock`.
///
/// What it asserts:
///
/// * the fork's own `wincode` edge lands on exactly 0.4.9;
/// * no `wincode >= 0.5` is selected anywhere in the graph;
/// * the root and the fork share one `wincode` node, which is what keeps
///   `src/bincode_compat.rs`'s configuration type-compatible with `SerdeCompat`.
///
/// The unpublished workspace crates are supplied as `[patch.crates-io]` path
/// entries pointing at the **unpacked packages** — the minimum needed to resolve
/// crates that are not on crates.io yet, and exactly what
/// `publish_workflow_orders_member_crates_before_the_root_crate` removes the
/// need for once they are published. The patches are not the fix for any
/// requirement under test: patches are never propagated to consumers of a
/// published crate.
#[test]
#[cfg_attr(not(unix), ignore = "the packaged .crate is read with tar")]
fn fresh_consumer_resolves_one_wincode_from_the_packaged_crates() {
    assert_cargo_is_the_msrv_toolchain();
    let version = packaged_version();
    let fork_dir = unpacked_packages()[FORK_PACKAGE].clone();

    // ── 1. a consumer of the fork alone ─────────────────────────────────────
    // The tightest statement of the bug: upstream's `>=0.4, <1` on this edge is
    // what pulled in a second wincode.
    let resolution = resolve_fresh_consumer(
        "fork-consumer",
        &format!(
            "[workspace]\n\
             resolver = \"2\"\n\n\
             [package]\n\
             name = \"fork-consumer\"\n\
             version = \"0.0.0\"\n\
             edition = \"2021\"\n\n\
             [dependencies]\n\
             {FORK_PACKAGE} = \"{version}\"\n\n\
             [patch.crates-io]\n\
             {FORK_PACKAGE} = {{ path = \"{}\" }}\n",
            fork_dir.display()
        ),
    );

    let resolved = wincode_packages(&resolution.metadata);
    assert_eq!(
        resolved,
        vec![("wincode".to_string(), "0.4.9".to_string())],
        "a fresh consumer of the packaged fork resolved {resolved:?}. Exactly one \
         wincode 0.4.9 is required: upstream `serde-wincode` 0.1.2 resolves \
         wincode 0.6.1 here, which needs Rust 1.89.0 and has a different ABI."
    );

    // ── 2. a consumer of the packaged root crate ────────────────────────────
    let resolution = resolve_fresh_consumer(
        "root-consumer",
        &root_consumer_manifest("root-consumer", &[]),
    );

    let resolved = wincode_packages(&resolution.metadata);
    assert_eq!(
        resolved,
        vec![("wincode".to_string(), "0.4.9".to_string())],
        "a fresh consumer of the packaged root crate resolved {resolved:?}; \
         expected exactly one wincode 0.4.9"
    );

    // No wincode at or above 0.5 anywhere, stated independently of the exact
    // set above so the intent survives a future 0.4.x bump.
    let too_new: Vec<_> = resolved
        .iter()
        .filter(|(_, version)| major_minor(version) >= MAX_WINCODE_EXCLUSIVE)
        .collect();
    assert!(
        too_new.is_empty(),
        "a fresh consumer selected wincode {too_new:?}; 0.5.0+ requires Rust 1.89.0, \
         above this crate's MSRV of 1.88"
    );

    // The generated lockfile must agree with the metadata graph — this is the
    // resolution a consumer would actually build from.
    let locked_wincode: Vec<_> = resolution
        .locked()
        .into_iter()
        .filter(|p| p.name == "wincode")
        .map(|p| p.version)
        .collect();
    assert_eq!(
        locked_wincode,
        vec!["0.4.9".to_string()],
        "the lockfile cargo generated for the fresh consumer resolved {locked_wincode:?}"
    );

    // And both edges must be the *same* node.
    let nodes = resolution.metadata["resolve"]["nodes"]
        .as_array()
        .expect("cargo metadata resolve.nodes");
    let wincode_id = nodes
        .iter()
        .filter_map(|node| node["id"].as_str())
        .find(|id| package_name_of(id) == "wincode")
        .expect("the fresh graph must contain wincode");

    for owner in [FORK_PACKAGE, "streamline"] {
        let node = nodes
            .iter()
            .find(|node| {
                node["id"]
                    .as_str()
                    .is_some_and(|id| package_name_of(id) == owner)
            })
            .unwrap_or_else(|| panic!("the fresh graph must contain `{owner}`"));
        let targets: Vec<&str> = node["dependencies"]
            .as_array()
            .unwrap_or_else(|| panic!("`{owner}` dependencies"))
            .iter()
            .filter_map(|dep| dep.as_str())
            .collect();
        assert!(
            targets.contains(&wincode_id),
            "`{owner}` does not point at the single resolved wincode.\n  \
             expected: {wincode_id}\n  got: {targets:?}"
        );
    }
}

/// **The published-MSRV proof.**
///
/// The same fresh-consumer harness, but with the optional feature graph that
/// actually carries the MSRV-sensitive edges switched on:
///
/// * `graphql`  → async-graphql and its derive/parser/value companions;
/// * `cloud-storage` → object_store, and through it crc-fast → crc;
/// * the default (`lite`) features, which carry the serialization stack
///   (`streamline-serde-wincode` → wincode).
///
/// Nothing here reads this repository's `Cargo.lock`, and nothing here asserts
/// on normalised-manifest *text*: it generates a real lockfile from the packaged
/// manifests with the real MSRV toolchain and inspects the resolution.
///
/// The final assertion is the general one: **no crate anywhere in the resolved
/// graph may declare a `rust-version` newer than this crate's MSRV.** That is
/// the property the individual pins exist to preserve, and it catches a new
/// floating edge that nobody has written a pin for yet.
#[test]
#[cfg_attr(not(unix), ignore = "the packaged .crate is read with tar")]
fn fresh_consumer_resolves_an_msrv_compatible_graph() {
    assert_cargo_is_the_msrv_toolchain();

    let resolution = resolve_fresh_consumer(
        "msrv-consumer",
        &root_consumer_manifest("msrv-consumer", &["graphql", "cloud-storage"]),
    );

    // The affected optional graph really is active — otherwise every assertion
    // below would pass vacuously.
    for expected in [
        "async-graphql",
        "async-graphql-derive",
        "async-graphql-parser",
        "async-graphql-value",
        "object_store",
        "crc-fast",
        "crc",
        "wincode",
        FORK_PACKAGE,
    ] {
        assert!(
            !resolution.versions_of(expected).is_empty(),
            "the fresh consumer did not activate `{expected}`; the `graphql` and \
             `cloud-storage` features must both reach the graph or this test proves \
             nothing.\nlockfile:\n{}",
            resolution.lockfile,
        );
    }

    // ── the specific edges this change exists to bound ──────────────────────
    for pin in MSRV_SENSITIVE_PINS {
        let resolved = resolution.versions_of(pin.name);
        assert_eq!(
            resolved.len(),
            1,
            "a fresh consumer resolved {} copies of `{}`: {resolved:?}. The published \
             requirement is `{}` ({}).",
            resolved.len(),
            pin.name,
            pin.requirement,
            pin.why,
        );
        assert!(
            version_triple(&resolved[0]) < pin.first_too_new,
            "a fresh consumer of the *published* crates resolved {} {}, at or above \
             {}.{}.{} — which needs a toolchain newer than this crate's MSRV of {MSRV}. \
             {}. A Cargo.lock pin cannot fix this; the requirement must be exact in a \
             published manifest.",
            pin.name,
            resolved[0],
            pin.first_too_new.0,
            pin.first_too_new.1,
            pin.first_too_new.2,
            pin.why,
        );
    }

    // The same statement made against the generated lockfile, so the proof does
    // not rest on `cargo metadata`'s view alone.
    let locked = resolution.locked();
    assert!(
        locked.len() > 100,
        "the generated lockfile has only {} packages — the fresh consumer did not \
         resolve the real graph",
        locked.len()
    );
    for package in &locked {
        if let Some(pin) = MSRV_SENSITIVE_PINS.iter().find(|p| p.name == package.name) {
            assert!(
                version_triple(&package.version) < pin.first_too_new,
                "the lockfile generated for the fresh consumer contains {} {}",
                package.name,
                package.version
            );
        }
    }

    // ── the general property ────────────────────────────────────────────────
    let msrv = version_triple(&format!("{MSRV}.0"));
    let too_new: Vec<String> = resolution.metadata["packages"]
        .as_array()
        .expect("cargo metadata packages")
        .iter()
        .filter_map(|package| {
            let declared = package["rust_version"].as_str()?;
            (version_triple(declared) > msrv).then(|| {
                format!(
                    "  {} {} declares rust-version {declared}",
                    package["name"].as_str().unwrap_or("?"),
                    package["version"].as_str().unwrap_or("?"),
                )
            })
        })
        .collect();
    assert!(
        too_new.is_empty(),
        "a fresh consumer of the published crates resolved package(s) that cannot be \
         built on this crate's MSRV of {MSRV}:\n{}\n\n\
         Add an exact optional direct dependency in Cargo.toml (activated by the \
         feature that pulls the permissive edge) and register it in \
         MSRV_SENSITIVE_PINS. Do not fix this with a Cargo.lock pin or a \
         `[patch]`: neither reaches a consumer of the published crate.",
        too_new.join("\n")
    );
}

/// Non-vacuity control for `fresh_consumer_resolves_an_msrv_compatible_graph`.
///
/// The exact requirements in `Cargo.toml` are only worth having if the
/// permissive ones they replaced really did float past the MSRV. This resolves
/// a consumer that asks for the *same* versions with ordinary caret
/// requirements — `async-graphql = "7.0.17"` and `object_store = "0.14.1"`,
/// which is what the manifest said before — and asserts that it picks up
/// releases needing Rust 1.89:
///
/// * `async-graphql` and its derive/parser/value crates jump to the 7.2.x line;
/// * `object_store`'s `crc-fast = "^1.6"` selects crc-fast 1.10.
///
/// Skipped, loudly, when the crates.io index is unavailable: a control that
/// silently passes is worse than none.
#[test]
fn permissive_requirements_would_still_float_past_the_msrv() {
    let dir = fresh_consumer_dir().join("msrv-control");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("src")).expect("create control dir");
    std::fs::write(
        dir.join("Cargo.toml"),
        "[workspace]\n\
         resolver = \"2\"\n\n\
         [package]\n\
         name = \"msrv-control\"\n\
         version = \"0.0.0\"\n\
         edition = \"2021\"\n\n\
         [dependencies]\n\
         async-graphql = { version = \"7.0.17\", features = [\"chrono\", \"uuid\"] }\n\
         object_store = { version = \"0.14.1\", features = [\"aws\", \"azure\", \"gcp\"] }\n",
    )
    .expect("write control manifest");
    std::fs::write(dir.join("src/main.rs"), "fn main() {}\n").expect("write control main");

    let output = std::process::Command::new(env!("CARGO"))
        .args(["generate-lockfile"])
        .current_dir(&dir)
        .env("CARGO_TARGET_DIR", fresh_consumer_dir().join("target"))
        .env("CARGO_INCREMENTAL", "0")
        .env_remove("RUSTC_WRAPPER")
        .output()
        .expect("failed to run cargo generate-lockfile for the control");

    if !output.status.success() {
        eprintln!(
            "SKIPPED: the permissive-requirement control could not be resolved, so the \
             non-vacuity check did not run. This needs the crates.io index.\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    let lockfile = std::fs::read_to_string(dir.join("Cargo.lock")).expect("control lockfile");
    let packages = parse_lockfile(&lockfile);

    let floated: Vec<String> = MSRV_SENSITIVE_PINS
        .iter()
        .filter_map(|pin| {
            let resolved = packages.iter().find(|p| p.name == pin.name)?;
            (version_triple(&resolved.version) >= pin.first_too_new)
                .then(|| format!("{} {}", resolved.name, resolved.version))
        })
        .collect();

    assert!(
        !floated.is_empty(),
        "the control resolved nothing above the MSRV. It is supposed to reproduce \
         the defect the exact requirements fix — permissive caret requirements \
         selecting releases that need Rust 1.89. If upstream has since stopped \
         publishing those, the pins and this control both need revisiting.\n\
         control lockfile packages: {:?}",
        packages
            .iter()
            .filter(|p| MSRV_SENSITIVE_PINS.iter().any(|pin| pin.name == p.name))
            .map(|p| format!("{} {}", p.name, p.version))
            .collect::<Vec<_>>()
    );

    // Specifically: the async-graphql family and crc-fast, which is exactly what
    // this repository's lockfile had recorded.
    for expected in ["async-graphql-derive", "crc-fast"] {
        assert!(
            floated.iter().any(|entry| entry.starts_with(expected)),
            "the control was expected to float `{expected}` past the MSRV; it \
             resolved {floated:?}"
        );
    }
}

/// Non-vacuity control for the test above.
///
/// If the fresh-consumer harness could not detect the original bug, it would be
/// worth nothing. This resolves a consumer of the *upstream* `serde-wincode`
/// 0.1.2 — with the same exact `wincode = "=0.4.9"` the root manifest carries —
/// and asserts that it **does** drag in a second, newer wincode. That is the
/// defect the fork removes, reproduced on demand.
///
/// Skipped, loudly, when upstream cannot be resolved (offline with a cold
/// registry cache): a control that silently passes is worse than none.
#[test]
fn upstream_serde_wincode_would_still_resolve_a_second_wincode() {
    let dir = fresh_consumer_dir().join("upstream-control");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("src")).expect("create control dir");
    std::fs::write(
        dir.join("Cargo.toml"),
        "[workspace]\n\n\
         [package]\n\
         name = \"upstream-control\"\n\
         version = \"0.0.0\"\n\
         edition = \"2021\"\n\n\
         [dependencies]\n\
         serde-wincode = { version = \"0.1.2\", default-features = false, features = [\"std\"] }\n\
         wincode = { version = \"=0.4.9\", default-features = false, features = [\"std\"] }\n",
    )
    .expect("write control manifest");
    std::fs::write(dir.join("src/main.rs"), "fn main() {}\n").expect("write control main");

    let output = std::process::Command::new(env!("CARGO"))
        .args(["metadata", "--format-version", "1"])
        .current_dir(&dir)
        .env("CARGO_TARGET_DIR", fresh_consumer_dir().join("target"))
        .env_remove("RUSTC_WRAPPER")
        .output()
        .expect("failed to run cargo metadata for the control");

    if !output.status.success() {
        eprintln!(
            "SKIPPED: upstream `serde-wincode` 0.1.2 could not be resolved, so the \
             non-vacuity control did not run. This needs the crates.io index.\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    let metadata: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("cargo metadata emitted invalid JSON");
    let resolved = wincode_packages(&metadata);

    assert!(
        resolved.len() > 1
            && resolved
                .iter()
                .any(|(_, version)| major_minor(version) >= MAX_WINCODE_EXCLUSIVE),
        "the control resolved {resolved:?}. It is supposed to reproduce the defect — \
         upstream `serde-wincode`'s `>=0.4, <1` edge resolving a wincode >= 0.5 \
         *alongside* the exact 0.4.9 pin. If upstream has since tightened its \
         requirement, this control and the fork's rationale both need revisiting."
    );
}

/// The ALPN list in `src/http_client.rs` mirrors what reqwest would have set,
/// and reqwest only offers `h2` when its `http2` feature is on.
///
/// Streamline depends on reqwest with `default-features = false` and does not
/// enable `http2`, so the correct list is `["http/1.1"]`. Feature unification
/// could change that from another crate in the graph, and because Streamline
/// hands reqwest a *preconfigured* rustls config, reqwest would not fix it up:
/// HTTP/2 would simply never be negotiated.
#[test]
fn reqwest_is_resolved_without_the_http2_feature() {
    let lockfile =
        std::fs::read_to_string(workspace_lockfile()).expect("failed to read Cargo.lock");

    let block = lockfile
        .split("[[package]]")
        .find(|block| {
            block
                .lines()
                .any(|line| line.trim() == r#"name = "reqwest""#)
                && block
                    .lines()
                    .any(|line| line.trim() == r#"version = "0.12.25""#)
        })
        .expect("Cargo.lock must contain the reqwest 0.12 package Streamline depends on");

    assert!(
        !block.lines().any(|line| line.trim() == "\"h2\","),
        "reqwest now resolves with `h2`, i.e. its `http2` feature is enabled. \
         `ALPN_PROTOCOLS` in src/http_client.rs must then advertise `h2` before \
         `http/1.1`, or HTTP/2 can never be negotiated on any Streamline client:\n{block}"
    );
}
