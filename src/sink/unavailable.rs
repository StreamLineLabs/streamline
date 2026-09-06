//! Explicit-unavailable adapters for sink connectors that this build cannot run.
//!
//! # Why this module exists
//!
//! The Apache Iceberg and Delta Lake sink connectors are **not available** in
//! this release. They are not "disabled by default" and they are not "coming
//! soon behind a feature flag" — there is no supported build of Streamline in
//! which they can be constructed.
//!
//! Every released upstream version that is compatible with this crate's MSRV
//! (Rust 1.88) pulls in dependencies with unfixed, remotely reachable security
//! advisories:
//!
//! | Connector | Upstream chain | Advisory |
//! |---|---|---|
//! | Iceberg | `iceberg` 0.4 → `opendal` 0.50 → `quick-xml` 0.36 | RUSTSEC-2026-0194, RUSTSEC-2026-0195 |
//! | Iceberg | `iceberg` 0.4 → `opendal` 0.50 → `reqsign` 0.16 → `quick-xml` 0.37 | RUSTSEC-2026-0194, RUSTSEC-2026-0195 |
//! | Delta Lake | `deltalake` 0.22 → `delta_kernel` 0.4 → `object_store` 0.11 → `quick-xml` 0.37 | RUSTSEC-2026-0194, RUSTSEC-2026-0195 |
//! | Delta Lake | `deltalake` 0.22 → `delta_kernel` 0.4 → `reqwest` (default-tls) → `native-tls` → `openssl` | OpenSSL surface this project does not ship |
//!
//! Both quick-xml advisories are rated CVSS 7.5 and are reachable from
//! attacker-influenced object-storage XML (S3/Azure list responses), which is
//! exactly the data these connectors parse. Streamline does not suppress
//! advisories, so the dependencies were removed rather than shipped vulnerable.
//!
//! The connector implementations are preserved verbatim in
//! `crate::sink::iceberg` and `crate::sink::delta` behind the never-enabled
//! `iceberg_backend` / `delta_backend` cfgs, so they can be restored once
//! upstream publishes MSRV-compatible releases built on `quick-xml >= 0.41`
//! without `native-tls`.
//!
//! # The Serverless and Cloud Function connectors
//!
//! These two are a different case: they are perfectly safe, they are simply
//! *optional*. Both deliver over HTTP through [`crate::http_client`], which is
//! only compiled when the `serverless` Cargo feature is enabled (that feature is
//! what pulls in `reqwest`, `webpki-roots`, `sha2` and `hmac`).
//!
//! Before this module gated them, a default build compiled the connector types
//! but not their delivery paths: `SinkManager::create_sink` happily constructed
//! and *registered* a serverless sink, and every subsequent batch failed with
//! "requires --features serverless" — a sink that looked healthy in
//! `sink list` while dropping records. They are therefore reported unavailable
//! whenever the feature is off, and creation is rejected up front.
//!
//! Unlike the lakehouse connectors, this is recoverable by the operator:
//! rebuild with `--features serverless`.
//!
//! # Behaviour
//!
//! The `iceberg` and `delta-lake` Cargo features still exist so that existing
//! build scripts keep resolving, but they enable no dependencies. Attempting to
//! create any unavailable sink fails with [`StreamlineError::Sink`] carrying the
//! corresponding message below.

use crate::error::StreamlineError;
use crate::sink::config::SinkType;

/// Why the Apache Iceberg sink connector cannot be constructed.
pub const ICEBERG_UNAVAILABLE_REASON: &str = concat!(
    "Iceberg sink connector is unavailable in this release and cannot be enabled by any feature flag. ",
    "The `iceberg` 0.4 crate depends on `opendal` 0.50, which pulls quick-xml < 0.41 ",
    "(RUSTSEC-2026-0194, RUSTSEC-2026-0195 — CVSS 7.5, reachable from object-storage XML). ",
    "No MSRV-compatible upstream release fixes this, and Streamline does not suppress advisories, ",
    "so the dependency was removed. Track https://github.com/apache/iceberg-rust for a release built on quick-xml >= 0.41."
);

/// Why the Delta Lake sink connector cannot be constructed.
pub const DELTA_LAKE_UNAVAILABLE_REASON: &str = concat!(
    "Delta Lake sink connector is unavailable in this release and cannot be enabled by any feature flag. ",
    "The `deltalake` 0.22 crate depends on `delta_kernel`, which pulls `object_store` 0.11 / quick-xml < 0.41 ",
    "(RUSTSEC-2026-0194, RUSTSEC-2026-0195 — CVSS 7.5) and `reqwest` with default-tls, forcing ",
    "native-tls/OpenSSL into the build. Released `deltalake` <= 0.31.1 still pins object_store 0.12 / quick-xml < 0.41. ",
    "Streamline does not suppress advisories, so the dependency was removed. ",
    "Track https://github.com/delta-io/delta-rs for a rustls-only release built on quick-xml >= 0.41."
);

/// Why the Serverless sink connector cannot be constructed without the
/// `serverless` feature.
pub const SERVERLESS_UNAVAILABLE_REASON: &str = concat!(
    "Serverless sink connector is unavailable in this build: it was compiled without the ",
    "`serverless` Cargo feature, which supplies the HTTP client it delivers through. ",
    "Rebuild with `--features serverless` (for example `cargo build --features serverless`, ",
    "or `cargo install streamline --features serverless`) and create the sink again. ",
    "Creation is refused rather than deferred so the sink is never registered in a state ",
    "where it would accept records and drop them."
);

/// Why the Cloud Function sink connector cannot be constructed without the
/// `serverless` feature.
pub const CLOUD_FUNCTION_UNAVAILABLE_REASON: &str = concat!(
    "Cloud Function sink connector (AWS Lambda, Google Cloud Functions, Azure Functions, ",
    "Cloudflare Workers) is unavailable in this build: it was compiled without the ",
    "`serverless` Cargo feature, which supplies the HTTP client it invokes functions through. ",
    "Rebuild with `--features serverless` (for example `cargo build --features serverless`, ",
    "or `cargo install streamline --features serverless`) and create the sink again. ",
    "Creation is refused rather than deferred so the sink is never registered in a state ",
    "where it would accept records and drop them."
);

/// Returns `true` if a connector of this type can actually be constructed and
/// run in this build.
///
/// This is the single source of truth used by the CLI, the sink manager and the
/// regression tests.
///
/// * The lakehouse connectors report `false` in **every** supported build,
///   including `--features iceberg`, `--features delta-lake`, `--features full`
///   and `--all-features`: their gate is the never-enabled `iceberg_backend` /
///   `delta_backend` cfg, not a feature.
/// * The Serverless and Cloud Function connectors report `true` exactly when the
///   `serverless` feature is enabled — the same condition under which
///   `crate::sink::serverless` and `crate::sink::cloud_functions` are compiled
///   at all.
pub fn is_available(sink_type: SinkType) -> bool {
    match sink_type {
        SinkType::Iceberg => cfg!(iceberg_backend),
        SinkType::DeltaLake => cfg!(delta_backend),
        SinkType::Serverless | SinkType::CloudFunction => cfg!(feature = "serverless"),
    }
}

/// Returns the reason a connector is unavailable, or `None` when it is usable.
pub fn unavailable_reason(sink_type: SinkType) -> Option<&'static str> {
    if is_available(sink_type) {
        return None;
    }
    Some(match sink_type {
        SinkType::Iceberg => ICEBERG_UNAVAILABLE_REASON,
        SinkType::DeltaLake => DELTA_LAKE_UNAVAILABLE_REASON,
        SinkType::Serverless => SERVERLESS_UNAVAILABLE_REASON,
        SinkType::CloudFunction => CLOUD_FUNCTION_UNAVAILABLE_REASON,
    })
}

/// Builds the error returned when an unavailable connector is requested.
///
/// Returns `None` when the connector is available, so callers can fall through
/// to the real constructor.
pub fn unavailable_error(sink_type: SinkType) -> Option<StreamlineError> {
    unavailable_reason(sink_type).map(|reason| StreamlineError::Sink(reason.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lakehouse_connectors_are_reported_unavailable() {
        assert!(!is_available(SinkType::Iceberg));
        assert!(!is_available(SinkType::DeltaLake));
    }

    /// The serverless connectors track their feature exactly — in both
    /// directions, so this test is meaningful in either build.
    #[test]
    fn serverless_connectors_track_the_serverless_feature() {
        let expected = cfg!(feature = "serverless");
        assert_eq!(is_available(SinkType::Serverless), expected);
        assert_eq!(is_available(SinkType::CloudFunction), expected);
    }

    #[test]
    fn unavailable_reasons_name_the_advisories() {
        for sink_type in [SinkType::Iceberg, SinkType::DeltaLake] {
            let reason = unavailable_reason(sink_type).expect("must be unavailable");
            assert!(reason.contains("RUSTSEC-2026-0194"), "{reason}");
            assert!(reason.contains("RUSTSEC-2026-0195"), "{reason}");
            assert!(reason.contains("unavailable"), "{reason}");
        }
    }

    #[cfg(not(feature = "serverless"))]
    #[test]
    fn serverless_reasons_are_actionable_without_the_feature() {
        for sink_type in [SinkType::Serverless, SinkType::CloudFunction] {
            let reason = unavailable_reason(sink_type).expect("must be unavailable");
            assert!(reason.contains("--features serverless"), "{reason}");
            assert!(reason.contains("unavailable"), "{reason}");
            // The lakehouse rationale must not leak into a message that is
            // really about a build flag.
            assert!(!reason.contains("RUSTSEC"), "{reason}");
        }
    }

    #[cfg(feature = "serverless")]
    #[test]
    fn serverless_reasons_are_absent_when_the_feature_is_on() {
        assert_eq!(unavailable_reason(SinkType::Serverless), None);
        assert_eq!(unavailable_reason(SinkType::CloudFunction), None);
    }

    #[test]
    fn unavailable_error_matches_unavailable_reason() {
        for sink_type in [
            SinkType::Iceberg,
            SinkType::DeltaLake,
            SinkType::Serverless,
            SinkType::CloudFunction,
        ] {
            assert_eq!(
                unavailable_error(sink_type).is_some(),
                unavailable_reason(sink_type).is_some(),
                "{sink_type} disagrees between unavailable_error and unavailable_reason"
            );
            assert_eq!(
                unavailable_error(sink_type).is_some(),
                !is_available(sink_type)
            );
        }
    }
}
