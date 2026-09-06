//! The single place Streamline constructs `reqwest` clients.
//!
//! # Why this module exists
//!
//! `reqwest` is depended on with `rustls-tls-webpki-roots-no-provider`, which
//! deliberately gives reqwest *no* crypto provider of its own. When reqwest has
//! to build the rustls `ClientConfig` itself it resolves one from process-global
//! state (`reqwest-0.12/src/async_impl/client.rs`, the `TlsBackend::Rustls`
//! arm):
//!
//! ```text
//! // Allow user to have installed a runtime default.
//! // If not, we use ring.
//! let provider = rustls::crypto::CryptoProvider::get_default()
//!     .map(|arc| arc.clone())
//!     .unwrap_or_else(|| {
//!         #[cfg(not(feature = "__rustls-ring"))]
//!         panic!("No provider set");
//!         ...
//!     });
//! ```
//!
//! `get_default()` returns whatever `CryptoProvider::install_default()`
//! installed process-wide. Streamline installs nothing — that ambient state is
//! exactly what `crate::server::tls::crypto_provider` and
//! `tests/tls_crypto_provider_test.rs` exist to remove — and the `ring` fallback
//! is compiled out by the `-no-provider` feature. The branch is therefore a bare
//! `panic!("No provider set")`, so **every** `reqwest::Client::new()` and every
//! plain `reqwest::Client::builder().build()` aborts at runtime.
//!
//! The one path that never reaches that branch is
//! [`ClientBuilder::use_preconfigured_tls`][reqwest::ClientBuilder::use_preconfigured_tls]:
//! a caller-supplied `rustls::ClientConfig` is stored as `TlsBackend::BuiltRustls`
//! and handed to the connector untouched. [`builder`] and [`blocking_builder`]
//! do exactly that, with the provider taken from
//! [`crate::server::tls::crypto_provider`] — the same `aws-lc-rs` provider the
//! server listeners, QUIC and WebTransport use.
//!
//! # What the preconfigured configuration reproduces
//!
//! Because `TlsBackend::BuiltRustls` is passed through verbatim, reqwest applies
//! *none* of its own TLS defaults to it. Everything reqwest would have set has
//! to be set here instead, and is:
//!
//! * **Protocol versions** — TLS 1.2 and TLS 1.3, matching reqwest's default
//!   version set and `crate::server::tls`'s server-side default.
//!   `builder_with_provider` returns a `ConfigBuilder<_, WantsVersions>`, so this
//!   choice is not optional; it is stated explicitly rather than deferred to
//!   `with_safe_default_protocol_versions()`.
//! * **Roots** — the WebPKI root set, which is what the
//!   `rustls-tls-webpki-roots-no-provider` feature name promises and what
//!   reqwest would otherwise load. `webpki-roots` is a direct dependency so the
//!   root set is chosen here rather than inherited from a feature flag.
//! * **No client authentication** — Streamline presents no client certificate on
//!   outbound HTTP, which is what reqwest does without a configured `Identity`.
//! * **ALPN** — see [`ALPN_PROTOCOLS`]. The `BuiltRustls` arm does *not* set
//!   `alpn_protocols`, so omitting it here would silently disable protocol
//!   negotiation.
//!
//! Server-name indication is left at rustls' default (`enable_sni = true`),
//! which is also reqwest's default.
//!
//! # Using this module
//!
//! Call [`builder`] / [`blocking_builder`] instead of `reqwest::Client::builder()`
//! and [`client`] / [`blocking_client`] instead of `reqwest::Client::new()`, then
//! apply per-call-site options (timeouts, headers, user agent) as before.
//! `reqwest_clients_are_only_constructed_through_the_shared_helper` in
//! `tests/tls_crypto_provider_test.rs` fails the build if a raw constructor
//! reappears anywhere else.

// reqwest is optional and is enabled by exactly these features. The list is not
// maintained by hand: `the_http_client_gate_covers_every_feature_that_enables_reqwest`
// in tests/tls_crypto_provider_test.rs parses Cargo.toml for the features that
// enable `dep:reqwest` and fails if this gate and that set ever disagree.
#![cfg(any(
    feature = "ai",
    feature = "attestation",
    feature = "auth",
    feature = "branches",
    feature = "semantic-topics",
    feature = "serverless",
    feature = "web-ui"
))]

use rustls::version::{TLS12, TLS13};
use rustls::{ClientConfig, RootCertStore, SupportedProtocolVersion};

/// The TLS versions offered on outbound connections.
///
/// Same set as `reqwest`'s default and as the server-side default in
/// [`crate::server::tls`], so replacing reqwest's internal configuration with
/// this one does not change which handshakes succeed.
const PROTOCOL_VERSIONS: &[&SupportedProtocolVersion] = &[&TLS12, &TLS13];

/// The ALPN protocol list advertised on outbound connections.
///
/// reqwest derives its own list from `Config::http_version_pref`, whose default
/// (`HttpVersionPref::All`) expands to `["h2", "http/1.1"]` *only when reqwest's
/// `http2` feature is on*. Streamline depends on reqwest with
/// `default-features = false` and does not enable `http2`, so reqwest's list
/// would be exactly `["http/1.1"]` — which is what this reproduces.
///
/// `reqwest_is_resolved_without_the_http2_feature` in
/// `tests/dependency_security_test.rs` ties that assumption to the resolved
/// graph: if `h2` ever appears under reqwest, this list has to gain `"h2"` or
/// HTTP/2 would be silently un-negotiable.
const ALPN_PROTOCOLS: &[&[u8]] = &[b"http/1.1"];

/// Failure to build the rustls configuration shared by every Streamline client.
///
/// Only reachable if the AWS-LC provider stops supporting TLS 1.2 and TLS 1.3,
/// which `tls_config_builds_without_a_process_wide_provider` proves is not the
/// case today. It is still surfaced as an error rather than an `expect()` so a
/// provider change becomes a handled failure instead of a panic in a broker.
#[derive(Debug)]
pub struct TlsConfigError(rustls::Error);

impl std::fmt::Display for TlsConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "could not build the outbound TLS configuration: {}",
            self.0
        )
    }
}

impl std::error::Error for TlsConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

/// Build the rustls client configuration every Streamline HTTP client uses.
///
/// The provider is passed explicitly, so this never reads — and never needs —
/// the process-wide default that `reqwest` would otherwise resolve.
pub fn tls_config() -> Result<ClientConfig, TlsConfigError> {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let mut config = ClientConfig::builder_with_provider(crate::server::tls::crypto_provider())
        .with_protocol_versions(PROTOCOL_VERSIONS)
        .map_err(TlsConfigError)?
        .with_root_certificates(roots)
        .with_no_client_auth();

    config.alpn_protocols = ALPN_PROTOCOLS.iter().map(|p| p.to_vec()).collect();

    Ok(config)
}

/// An async `reqwest` client builder with Streamline's TLS configuration
/// already installed.
///
/// Apply per-call-site options (timeout, headers, user agent) to the returned
/// builder exactly as with `reqwest::Client::builder()`.
pub fn builder() -> Result<reqwest::ClientBuilder, TlsConfigError> {
    Ok(reqwest::Client::builder().use_preconfigured_tls(tls_config()?))
}

/// A blocking `reqwest` client builder with Streamline's TLS configuration
/// already installed.
pub fn blocking_builder() -> Result<reqwest::blocking::ClientBuilder, TlsConfigError> {
    Ok(reqwest::blocking::Client::builder().use_preconfigured_tls(tls_config()?))
}

/// An async client with default options — the safe replacement for
/// `reqwest::Client::new()`.
///
/// Unlike `Client::new()`, this reports a failure instead of panicking.
pub fn client() -> Result<reqwest::Client, Box<dyn std::error::Error + Send + Sync>> {
    Ok(builder()?.build()?)
}

/// A blocking client with default options — the safe replacement for
/// `reqwest::blocking::Client::new()`.
pub fn blocking_client(
) -> Result<reqwest::blocking::Client, Box<dyn std::error::Error + Send + Sync>> {
    Ok(blocking_builder()?.build()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The whole point of this module: none of it may depend on a process-wide
    /// provider having been installed.
    ///
    /// If some other test in this binary installed one, this assertion would
    /// pass vacuously — so it checks that no default is installed first, which
    /// `tests/tls_crypto_provider_test.rs` also enforces at the source level.
    #[test]
    fn tls_config_builds_without_a_process_wide_provider() {
        assert!(
            rustls::crypto::CryptoProvider::get_default().is_none(),
            "a process-wide crypto provider was installed, which would make this \
             test vacuous — Streamline must never install one"
        );

        let config = tls_config().expect("the AWS-LC provider must support TLS 1.2 and TLS 1.3");

        assert!(
            !config.alpn_protocols.is_empty(),
            "ALPN must be set explicitly: reqwest's `BuiltRustls` path does not set it"
        );
        assert_eq!(config.alpn_protocols, vec![b"http/1.1".to_vec()]);
    }

    /// The builders are the part that would panic if they ever fell through to
    /// reqwest's own rustls configuration.
    #[test]
    fn builders_construct_clients_with_no_provider_installed() {
        assert!(
            rustls::crypto::CryptoProvider::get_default().is_none(),
            "a process-wide crypto provider was installed, which would make this test vacuous"
        );

        builder()
            .expect("async builder")
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("async client must build without a process-wide provider");

        blocking_builder()
            .expect("blocking builder")
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("blocking client must build without a process-wide provider");

        client().expect("default async client");
        blocking_client().expect("default blocking client");
    }

    /// The root store must actually be populated; an empty one would fail every
    /// handshake at runtime rather than at construction.
    #[test]
    fn webpki_roots_are_loaded() {
        assert!(
            webpki_roots::TLS_SERVER_ROOTS.len() > 50,
            "expected the full WebPKI root set, found {}",
            webpki_roots::TLS_SERVER_ROOTS.len()
        );
    }
}
