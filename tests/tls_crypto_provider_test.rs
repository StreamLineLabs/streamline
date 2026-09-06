//! Regression tests for explicit rustls crypto-provider selection.
//!
//! rustls 0.23 can be used in two ways:
//!
//! * **Implicitly** — `ServerConfig::builder()`, `ClientConfig::builder()` and
//!   `WebPkiClientVerifier::builder()` call `CryptoProvider::get_default()`.
//!   That returns whatever was installed process-wide, which means the TLS
//!   implementation a Streamline listener uses is decided by whichever crate
//!   called `install_default()` first — and, if nothing did, the builder
//!   *panics*. When more than one provider feature is enabled anywhere in the
//!   graph, rustls cannot pick for you at all.
//! * **Explicitly** — `*_with_provider(...)` takes the `Arc<CryptoProvider>` to
//!   use. Provider choice becomes visible at every construction site, cannot be
//!   changed by a transitive dependency, and needs no global state.
//!
//! Streamline uses the explicit form everywhere it builds a rustls object, and
//! installs no process-wide default at all — including in tests, where an
//! `install_default()` fixture would mask an implicit call by making it work.
//!
//! These are source-level assertions on purpose. The runtime half lives next to
//! the code it protects, in the unit tests of `src/server/tls.rs`,
//! `src/transport/quic.rs` and `src/transport/webtransport.rs`, because those
//! modules are crate-internal and unreachable from an integration test. Both
//! halves are needed: the runtime tests prove the explicit path works with no
//! global default installed, and these prove no implicit path was left behind
//! (including in code paths that only compile under a feature this test binary
//! does not enable).

use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// One scanned source file.
struct Source {
    /// Path relative to the repository root.
    ///
    /// Kept as a [`PathBuf`] and compared through [`Source::is`], never as a
    /// rendered string. `strip_prefix` preserves the platform separator, so on
    /// Windows this is `tests\tls_crypto_provider_test.rs`; comparing that
    /// against a hard-coded `"tests/tls_crypto_provider_test.rs"` is always
    /// false, which used to make the scanner scan *itself* and fail every
    /// assertion below on its own documentation.
    path: PathBuf,
    contents: String,
}

impl Source {
    /// Whether this file is the one named by `relative`, given as path
    /// components.
    ///
    /// `PathBuf`'s `PartialEq` compares components rather than raw bytes, so
    /// collecting the components here yields the same answer on every platform.
    fn is(&self, relative: &[&str]) -> bool {
        self.path == relative.iter().collect::<PathBuf>()
    }

    /// `/`-separated rendering, so failure messages read identically on every
    /// platform.
    fn display(&self) -> String {
        self.path
            .components()
            .map(|component| component.as_os_str().to_string_lossy().into_owned())
            .collect::<Vec<_>>()
            .join("/")
    }
}

/// Every `.rs` file under `dir`, with paths relative to the repository root.
fn rust_sources(dir: &Path) -> Vec<Source> {
    let mut out = Vec::new();
    walk(dir, &repo_root(), &mut out);
    out.sort_by(|a, b| a.path.cmp(&b.path));
    assert!(
        out.len() > 50,
        "expected to scan the whole crate but found only {} files under {} — \
         the walker is broken, which would make every assertion below vacuous",
        out.len(),
        dir.display()
    );
    out
}

/// Recursive `.rs` collector, skipping build output.
fn walk(dir: &Path, root: &Path, out: &mut Vec<Source>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().is_some_and(|name| name == "target") {
                continue;
            }
            walk(&path, root, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            if let Ok(contents) = std::fs::read_to_string(&path) {
                out.push(Source {
                    path: path.strip_prefix(root).unwrap_or(&path).to_path_buf(),
                    contents,
                });
            }
        }
    }
}

/// All Streamline-owned Rust sources: the crate itself, the workspace members,
/// and the test/bench/example/fuzz trees that could otherwise smuggle an
/// implicit provider selection back in through a fixture.
fn streamline_sources() -> Vec<Source> {
    let root = repo_root();
    let mut sources = rust_sources(&root.join("src"));
    for extra in ["tests", "benches", "examples", "crates", "fuzz"] {
        let dir = root.join(extra);
        if dir.exists() {
            walk(&dir, &root, &mut sources);
        }
    }
    sources
}

/// This file necessarily mentions the very patterns it bans, so it is excluded
/// from the scan.
///
/// Given as path *components*: see [`Source::path`] for why a
/// `"tests/tls_crypto_provider_test.rs"` string would silently stop excluding
/// anything on Windows.
const SELF: &[&str] = &["tests", "tls_crypto_provider_test.rs"];

/// The single module allowed to call `reqwest`'s own constructors, because it
/// is the one that supplies the preconfigured rustls configuration.
const HTTP_CLIENT_HELPER: &[&str] = &["src", "http_client.rs"];

/// Ignore full-line comments while preserving `//` inside code string literals.
///
/// Truncating at the first `//` would make a URL hide everything after it, so
/// `client_for("https://example.com", Client::new())` would evade the guard.
fn code_only(line: &str) -> &str {
    if line.trim_start().starts_with("//") {
        ""
    } else {
        line
    }
}

/// Report `file:line` for every *code* line containing `needle`, skipping files
/// listed in `exempt`.
fn find_except(sources: &[Source], needle: &str, exempt: &[&[&str]]) -> Vec<String> {
    sources
        .iter()
        .filter(|source| !exempt.iter().any(|relative| source.is(relative)))
        .flat_map(|source| {
            source
                .contents
                .lines()
                .enumerate()
                .filter(move |(_, line)| code_only(line).contains(needle))
                .map(move |(index, line)| {
                    format!("  {}:{} — {}", source.display(), index + 1, line.trim())
                })
        })
        .collect()
}

/// Report `file:line` for every *code* line containing `needle`.
fn find(sources: &[Source], needle: &str) -> Vec<String> {
    find_except(sources, needle, &[SELF])
}

/// Installing a process-wide provider is exactly the ambient state this design
/// removes. A single `install_default()` — even in a test fixture — makes every
/// implicit `::builder()` call start working, so the explicit wiring could rot
/// without any test noticing.
#[test]
fn no_process_wide_crypto_provider_is_ever_installed() {
    let sources = streamline_sources();
    // Assembled at runtime so this file does not match its own scan.
    let needle = format!("install_{}", "default()");
    let offenders = find(&sources, &needle);

    assert!(
        offenders.is_empty(),
        "`install_default()` re-entered the codebase:\n{}\n\n\
         Streamline passes `Arc<CryptoProvider>` explicitly to every rustls \
         builder. Installing a process-wide default would hide an implicit \
         `::builder()` call instead of failing it. Pass the provider from \
         `crate::server::tls::crypto_provider()` instead.",
        offenders.join("\n")
    );
}

/// rustls configuration and verifier builders that resolve the provider from
/// global state.
fn implicit_builders() -> Vec<String> {
    // Assembled at runtime for the same reason as above.
    let open_close = format!("{}{}", "(", ")");
    vec![
        format!("ServerConfig::builder{open_close}"),
        format!("ClientConfig::builder{open_close}"),
        format!("WebPkiClientVerifier::builder{}", "("),
        format!("ClientCertVerifierBuilder::builder{}", "("),
    ]
}

#[test]
fn rustls_configs_and_verifiers_never_use_the_implicit_builder() {
    let sources = streamline_sources();
    let mut offenders = Vec::new();

    for needle in implicit_builders() {
        // `builder_with_provider(` contains neither `builder()` nor
        // `builder(` followed by an argument list that starts a root store, so
        // a plain `contains` is precise enough here.
        offenders.extend(find(&sources, &needle));
    }

    assert!(
        offenders.is_empty(),
        "a rustls object is being built without an explicit crypto provider:\n{}\n\n\
         Use `builder_with_provider(...)` (and, for verifiers, \
         `WebPkiClientVerifier::builder_with_provider(roots, provider)`), cloning \
         the same `Arc<CryptoProvider>` into every part of one configuration.",
        offenders.join("\n")
    );
}

/// `builder_with_provider` deliberately has no default protocol-version set: it
/// returns a `ConfigBuilder<_, WantsVersions>`. Selecting versions is therefore
/// not optional, but it *is* possible to satisfy the type system with the wrong
/// call, so assert that each config builder states its versions explicitly and
/// close to the builder.
///
/// `WebPkiClientVerifier::builder_with_provider` is exempt: a verifier has no
/// protocol-version stage — the versions belong to the `ServerConfig` it is
/// installed into, which is checked in its own right.
#[test]
fn every_explicit_builder_selects_protocol_versions() {
    let sources = streamline_sources();
    let mut offenders = Vec::new();

    for source in sources.iter().filter(|source| !source.is(SELF)) {
        let lines: Vec<&str> = source.contents.lines().collect();
        for (index, line) in lines.iter().enumerate() {
            let code = code_only(line);
            if !code.contains("builder_with_provider(") {
                continue;
            }
            if code.contains("WebPkiClientVerifier::builder_with_provider") {
                continue;
            }
            let window_end = (index + 12).min(lines.len());
            let window = lines[index..window_end].join("\n");
            if !window.contains("with_protocol_versions")
                && !window.contains("with_safe_default_protocol_versions")
            {
                offenders.push(format!(
                    "  {}:{} — {}",
                    source.display(),
                    index + 1,
                    line.trim()
                ));
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "these `builder_with_provider` call sites do not select protocol versions \
         explicitly:\n{}\n\n\
         Call `.with_protocol_versions(&versions)` (or \
         `.with_safe_default_protocol_versions()`) so the accepted TLS versions \
         are visible at the construction site.",
        offenders.join("\n")
    );
}

/// The explicit provider must actually be AWS-LC everywhere, otherwise "explicit"
/// would just mean "explicitly inconsistent".
#[test]
fn streamline_sources_only_name_the_aws_lc_provider() {
    let sources = streamline_sources();

    let offenders = find(&sources, "rustls::crypto::ring");
    assert!(
        offenders.is_empty(),
        "the `ring` rustls provider is referenced in Streamline sources:\n{}\n\n\
         Streamline is AWS-LC only; mixing providers reintroduces exactly the \
         ambiguity that explicit selection removes.",
        offenders.join("\n")
    );

    let aws_lc_sites = find(&sources, "aws_lc_rs::default_provider()");
    assert_eq!(
        aws_lc_sites.len(),
        1,
        "the AWS-LC provider should be constructed in exactly one place \
         (`crate::server::tls::crypto_provider()`), so every builder can clone the \
         same `Arc`. Found:\n{}",
        aws_lc_sites.join("\n")
    );
}

/// `rustls` itself must resolve with the AWS-LC backend and without `ring`.
///
/// Cargo unifies features across the graph: a single dependency enabling
/// `rustls/ring` would add a second provider to the same `rustls` build, which
/// is what makes `CryptoProvider::get_default()` ambiguous in the first place.
#[test]
fn resolved_rustls_enables_only_the_aws_lc_backend() {
    let lockfile =
        std::fs::read_to_string(repo_root().join("Cargo.lock")).expect("failed to read Cargo.lock");

    let block = lockfile
        .split("[[package]]")
        .find(|block| {
            block
                .lines()
                .any(|line| line.trim() == r#"name = "rustls""#)
        })
        .expect("Cargo.lock must contain the `rustls` package");

    assert!(
        block.contains("\"aws-lc-rs\""),
        "resolved `rustls` does not depend on aws-lc-rs:\n{block}"
    );
    assert!(
        !block.contains("\"ring\""),
        "resolved `rustls` pulls in `ring` as well as aws-lc-rs. Two providers in \
         one rustls build make implicit provider selection ambiguous and would \
         make `CryptoProvider::get_default()` unusable:\n{block}"
    );
}

/// The direct manifest requirements must keep selecting AWS-LC explicitly. This
/// complements the lockfile check: the lockfile records what was resolved once,
/// the manifest records what will be resolved again.
#[test]
fn direct_manifest_features_select_aws_lc_only() {
    let manifest =
        std::fs::read_to_string(repo_root().join("Cargo.toml")).expect("failed to read Cargo.toml");

    let expectations: &[(&str, &str)] = &[
        ("rustls =", "\"aws_lc_rs\""),
        ("tokio-rustls =", "\"aws-lc-rs\""),
        ("quinn =", "\"rustls-aws-lc-rs\""),
    ];

    for (prefix, feature) in expectations {
        let line = manifest
            .lines()
            .find(|line| line.trim_start().starts_with(prefix))
            .unwrap_or_else(|| panic!("Cargo.toml must declare `{prefix}`"));
        assert!(
            line.contains(feature),
            "`{prefix}` must enable {feature} explicitly; found:\n  {line}"
        );
        assert!(
            !line.contains("\"ring\"") && !line.contains("rustls-ring"),
            "`{prefix}` must not enable a second crypto provider; found:\n  {line}"
        );
    }
}

// ── Self-exclusion ──────────────────────────────────────────────────────────

/// The scan must never read this file, which mentions every pattern it bans.
///
/// The exclusion used to compare `path.to_string_lossy()` against the literal
/// `"tests/tls_crypto_provider_test.rs"`. Paths come from `strip_prefix`, which
/// preserves the platform separator, so on Windows the rendered form is
/// `tests\tls_crypto_provider_test.rs` and the comparison was *always false*:
/// the scanner scanned itself and every ban above failed on its own
/// documentation. Windows CI would have been red for a reason that has nothing
/// to do with crypto providers.
///
/// This proves the exclusion works from the outside — the file is in the scan
/// set, it does contain a banned pattern, and no offender is reported from it —
/// which holds on every platform without needing to run on Windows.
#[test]
fn the_scanner_never_scans_itself() {
    let sources = streamline_sources();

    let me = sources.iter().find(|source| source.is(SELF)).expect(
        "the walker must reach tests/tls_crypto_provider_test.rs — if it does not, the \
                 self-exclusion is untested and the path handling is wrong",
    );

    let needle = format!("install_{}", "default()");
    assert!(
        me.contents.contains(&needle),
        "this file must still contain a banned pattern, or excluding it proves nothing"
    );

    let offenders = find(&sources, &needle);
    assert!(
        !offenders
            .iter()
            .any(|offender| offender.contains("tls_crypto_provider_test")),
        "the scanner reported itself:\n{}",
        offenders.join("\n")
    );
}

/// Path matching must be by components, not by a `/`-joined string.
///
/// This is the platform-independent half: on a `/` platform both forms agree,
/// on Windows only the component comparison does. Asserting the *equivalence*
/// rather than either side makes the test meaningful in both places.
#[test]
fn self_exclusion_matches_path_components_not_a_slash_joined_string() {
    // Built exactly as `walk` builds it — by joining components, which uses the
    // platform separator.
    let built: PathBuf = SELF.iter().collect();
    let source = Source {
        path: built.clone(),
        contents: String::new(),
    };

    assert!(
        source.is(SELF),
        "component comparison must match regardless of platform separator"
    );
    assert!(
        !source.is(HTTP_CLIENT_HELPER),
        "component comparison must not match a different file"
    );

    // And this is the bug that was fixed: the rendered form only equals the
    // slash-joined constant where the separator happens to be `/`.
    let rendered = built.to_string_lossy().into_owned();
    assert_eq!(
        rendered == SELF.join("/"),
        std::path::MAIN_SEPARATOR == '/',
        "a rendered-string comparison is separator-dependent, which is why the \
         exclusion is done on components"
    );
}

// ── reqwest constructors ────────────────────────────────────────────────────

/// `reqwest` is built with `rustls-tls-webpki-roots-no-provider`, so when it has
/// to construct its own rustls `ClientConfig` it reads the process-wide provider
/// through `CryptoProvider::get_default()` and, finding none, executes a bare
/// `panic!("No provider set")` (`reqwest-0.12/src/async_impl/client.rs`).
///
/// Every ambient constructor therefore aborts at runtime rather than returning
/// an error: `Client::new()`, `Client::builder().build()`,
/// `ClientBuilder::new().build()` and their blocking/default twins alike. The
/// only path that avoids it is `use_preconfigured_tls`, which
/// `crate::http_client` is the single place to call.
///
/// This is the static half of that guarantee. The runtime half lives in the unit
/// tests of `src/http_client.rs`, `src/auth/oauth.rs`, `src/security/vault.rs`
/// and `src/sink/serverless.rs`, which build clients with no default installed.
/// Both halves are needed: the runtime tests prove the explicit path works, and
/// this one proves no ambient path was left behind in a module whose feature
/// this test binary does not enable.
#[test]
fn reqwest_clients_are_only_constructed_through_the_shared_helper() {
    let sources = streamline_sources();
    let exempt = [SELF, HTTP_CLIENT_HELPER];

    // Assembled at runtime so this file does not match its own scan.
    let banned = [
        format!("Client::new{}{}", "(", ")"),
        format!("Client::builder{}{}", "(", ")"),
        format!("Client::default{}{}", "(", ")"),
        format!("ClientBuilder::new{}{}", "(", ")"),
        format!("ClientBuilder::default{}{}", "(", ")"),
    ];

    let mut offenders = Vec::new();
    for needle in &banned {
        offenders.extend(find_except(&sources, needle, &exempt));
    }

    assert!(
        offenders.is_empty(),
        "a reqwest client is being constructed outside `crate::http_client`:\n{}\n\n\
         These constructors make reqwest resolve a crypto provider from process-wide \
         state, which Streamline never installs — so they panic with \"No provider set\" \
         instead of returning an error. Use `crate::http_client::builder()`, \
         `blocking_builder()`, `client()` or `blocking_client()`, which pass the \
         provider explicitly through `use_preconfigured_tls`.",
        offenders.join("\n")
    );
}

#[test]
fn source_scanner_does_not_treat_url_slashes_as_a_comment() {
    let line = r#"client_for("https://example.com", reqwest::Client::new())"#;
    assert_eq!(code_only(line), line);
    assert_eq!(code_only("  // reqwest::Client::new() is forbidden"), "");
}

/// The helper must actually be the thing that supplies a preconfigured
/// configuration — otherwise the exemption above would just be a hole.
#[test]
fn the_http_client_helper_preconfigures_tls_explicitly() {
    let sources = streamline_sources();

    let helper = sources
        .iter()
        .find(|source| source.is(HTTP_CLIENT_HELPER))
        .expect("src/http_client.rs must exist — it is the only module exempt from the ban");

    for required in [
        "use_preconfigured_tls",
        "builder_with_provider",
        "crate::server::tls::crypto_provider()",
        "with_protocol_versions",
        "with_no_client_auth",
        "alpn_protocols",
    ] {
        assert!(
            helper
                .contents
                .lines()
                .any(|line| code_only(line).contains(required)),
            "src/http_client.rs must call `{required}`: reqwest applies none of its own \
             TLS defaults to a preconfigured configuration, so everything it would have \
             set has to be set there"
        );
    }
}

/// `src/http_client.rs` gates itself on the features that enable `reqwest`. That
/// list is duplicated knowledge, so check it against the manifest instead of
/// trusting it: a new feature that adds `dep:reqwest` without extending the gate
/// would make the module vanish and its call sites fail to compile.
#[test]
fn the_http_client_gate_covers_every_feature_that_enables_reqwest() {
    let manifest =
        std::fs::read_to_string(repo_root().join("Cargo.toml")).expect("failed to read Cargo.toml");

    let mut expected: Vec<String> = manifest
        .lines()
        .skip_while(|line| line.trim() != "[features]")
        .take_while(|line| !line.trim_start().starts_with("[dev-dependencies]"))
        .filter(|line| line.contains("dep:reqwest"))
        .filter_map(|line| {
            line.split_once('=')
                .map(|(name, _)| name.trim().to_string())
        })
        .collect();
    expected.sort();
    expected.dedup();

    assert!(
        expected.len() >= 5,
        "expected to find the features enabling reqwest in Cargo.toml, found {expected:?} — \
         the manifest parser is broken, which would make this assertion vacuous"
    );

    let helper = std::fs::read_to_string(repo_root().join("src/http_client.rs"))
        .expect("failed to read src/http_client.rs");
    // Anchor on the gate itself. The module docs quote reqwest's own
    // `#[cfg(not(feature = "__rustls-ring"))]`, so searching for the first
    // `))]` in the file would stop inside the documentation instead.
    let gate_start = helper
        .find("#![cfg(any(")
        .expect("src/http_client.rs must carry an inner #![cfg(any(...))] gate");
    let gate_end = gate_start
        + helper[gate_start..]
            .find("))]")
            .expect("the #![cfg(any(...))] gate must be terminated");
    let gate = &helper[gate_start..gate_end];

    let missing: Vec<&String> = expected
        .iter()
        .filter(|feature| !gate.contains(&format!("feature = \"{feature}\"")))
        .collect();

    assert!(
        missing.is_empty(),
        "these features enable `dep:reqwest` but are absent from the \
         `#![cfg(any(...))]` gate in src/http_client.rs: {missing:?}\n\n\
         Add them, or the helper disappears in exactly the configuration that \
         needs it.",
    );

    // And the gate must not claim features that do not enable reqwest, which
    // would fail to compile for the opposite reason.
    let mut gated: Vec<String> = gate
        .match_indices("feature = \"")
        .filter_map(|(index, marker)| {
            let rest = &gate[index + marker.len()..];
            rest.find('"').map(|end| rest[..end].to_string())
        })
        .collect();
    gated.sort();
    gated.dedup();

    assert_eq!(
        gated, expected,
        "the `#![cfg(any(...))]` gate in src/http_client.rs and the set of features \
         enabling `dep:reqwest` in Cargo.toml have diverged"
    );
}
