//! Regression tests: the lakehouse sink connectors must report themselves
//! unavailable in every supported build configuration.
//!
//! These replace the former `iceberg_sink_test.rs` / `delta_sink_test.rs`
//! integration suites, which exercised connectors that no longer exist. The
//! `iceberg` and `delta-lake` upstream crates were removed because every
//! MSRV-compatible release pulls quick-xml < 0.41 (RUSTSEC-2026-0194 and
//! RUSTSEC-2026-0195, both CVSS 7.5) and, for Delta Lake, native-tls/OpenSSL.
//!
//! The point of these tests is to prevent a regression in the *opposite*
//! direction from the usual: nothing must start claiming the connectors work.
//! A "helpful" change that makes `create_sink` silently succeed, buffer records
//! and drop them would be worse than the honest failure asserted here.

use std::sync::Arc;
use streamline::sink::config::{SinkConfig, SinkType};
use streamline::sink::unavailable::{
    is_available, unavailable_error, unavailable_reason, DELTA_LAKE_UNAVAILABLE_REASON,
    ICEBERG_UNAVAILABLE_REASON,
};
use streamline::sink::SinkManager;
use streamline::TopicManager;
use tempfile::TempDir;

/// Build a `SinkManager` over a temporary data directory with `topic` created,
/// so `create_sink` reaches the availability check rather than failing earlier
/// on topic validation.
fn manager_with_topic(topic: &str) -> (TempDir, SinkManager) {
    let temp_dir = TempDir::new().expect("temp dir");
    let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).expect("topic manager"));
    topic_manager.create_topic(topic, 1).expect("create topic");
    let manager = SinkManager::new(topic_manager);
    (temp_dir, manager)
}

fn sink_config(name: &str, sink_type: SinkType, topic: &str) -> SinkConfig {
    SinkConfig {
        name: name.to_string(),
        sink_type,
        topics: vec![topic.to_string()],
        config: serde_json::json!({}),
    }
}

#[test]
fn lakehouse_connectors_are_not_available() {
    assert!(
        !is_available(SinkType::Iceberg),
        "Iceberg connector must not be reported as available"
    );
    assert!(
        !is_available(SinkType::DeltaLake),
        "Delta Lake connector must not be reported as available"
    );
}

/// The serverless connectors are *optional*, not removed: availability must
/// track the `serverless` feature exactly, in both directions.
#[test]
fn serverless_connector_availability_tracks_the_feature() {
    let expected = cfg!(feature = "serverless");
    assert_eq!(is_available(SinkType::Serverless), expected);
    assert_eq!(is_available(SinkType::CloudFunction), expected);
    assert_eq!(unavailable_error(SinkType::Serverless).is_none(), expected);
    assert_eq!(
        unavailable_error(SinkType::CloudFunction).is_none(),
        expected
    );
}

#[tokio::test]
async fn creating_an_iceberg_sink_fails_with_a_security_message() {
    let (_dir, manager) = manager_with_topic("events");
    let err = manager
        .create_sink(sink_config("events-iceberg", SinkType::Iceberg, "events"))
        .await
        .expect_err("Iceberg sink creation must fail");

    let message = err.to_string();
    assert!(
        message.contains("unavailable"),
        "error must say the connector is unavailable, got: {message}"
    );
    assert!(
        message.contains("RUSTSEC-2026-0194") && message.contains("RUSTSEC-2026-0195"),
        "error must cite the advisories that forced removal, got: {message}"
    );
    assert!(
        message.contains("quick-xml"),
        "error must name the vulnerable dependency, got: {message}"
    );
}

#[tokio::test]
async fn creating_a_delta_lake_sink_fails_with_a_security_message() {
    let (_dir, manager) = manager_with_topic("events");
    let err = manager
        .create_sink(sink_config("events-delta", SinkType::DeltaLake, "events"))
        .await
        .expect_err("Delta Lake sink creation must fail");

    let message = err.to_string();
    assert!(
        message.contains("unavailable"),
        "error must say the connector is unavailable, got: {message}"
    );
    assert!(
        message.contains("RUSTSEC-2026-0194") && message.contains("RUSTSEC-2026-0195"),
        "error must cite the advisories that forced removal, got: {message}"
    );
    assert!(
        message.contains("native-tls") || message.contains("OpenSSL"),
        "Delta Lake error must also mention the TLS stack problem, got: {message}"
    );
}

#[tokio::test]
async fn failed_lakehouse_sinks_are_not_registered() {
    let (_dir, manager) = manager_with_topic("events");

    for (name, sink_type) in [
        ("events-iceberg", SinkType::Iceberg),
        ("events-delta", SinkType::DeltaLake),
    ] {
        let _ = manager
            .create_sink(sink_config(name, sink_type, "events"))
            .await;
    }

    assert!(
        manager.list_sinks().is_empty(),
        "no lakehouse sink may end up registered: {:?}",
        manager.list_sinks()
    );
    assert!(
        manager.list_sinks_detailed().await.is_empty(),
        "no lakehouse sink may appear in detailed listings"
    );
}

#[tokio::test]
async fn lakehouse_sinks_cannot_be_started_after_a_failed_create() {
    let (_dir, manager) = manager_with_topic("events");
    let _ = manager
        .create_sink(sink_config("events-iceberg", SinkType::Iceberg, "events"))
        .await;

    // Since creation failed, there is nothing to start, stop or report on.
    assert!(manager.start_sink("events-iceberg").await.is_err());
    assert!(manager.stop_sink("events-iceberg").await.is_err());
    assert!(manager.get_status("events-iceberg").await.is_err());
    assert!(manager.get_metrics("events-iceberg").await.is_err());
}

#[test]
fn unavailable_reasons_point_at_upstream_trackers() {
    let iceberg = unavailable_reason(SinkType::Iceberg).expect("iceberg unavailable");
    assert_eq!(iceberg, ICEBERG_UNAVAILABLE_REASON);
    assert!(
        iceberg.contains("iceberg-rust"),
        "reason should tell users what to watch: {iceberg}"
    );

    let delta = unavailable_reason(SinkType::DeltaLake).expect("delta unavailable");
    assert_eq!(delta, DELTA_LAKE_UNAVAILABLE_REASON);
    assert!(
        delta.contains("delta-rs"),
        "reason should tell users what to watch: {delta}"
    );
}

/// The compatibility feature flags must not resurrect the connectors.
///
/// This test is compiled with whatever feature set CI uses; the assertion holds
/// for `--features iceberg`, `--features delta-lake`, `--features full` and
/// `--all-features` alike, because availability is keyed off the never-enabled
/// `iceberg_backend` / `delta_backend` cfgs rather than off the features.
#[test]
fn compatibility_features_do_not_enable_the_connectors() {
    assert!(!is_available(SinkType::Iceberg));
    assert!(!is_available(SinkType::DeltaLake));
}

// ── Serverless / Cloud Function connectors ──────────────────────────────────
//
// These two are gated on the `serverless` Cargo feature, which supplies the
// HTTP client they deliver through. Before the gate existed, a default build
// constructed *and registered* them and then failed on every batch with
// "requires --features serverless": a sink that reported itself healthy while
// dropping records. The tests below are split by feature so both halves of the
// contract are exercised by CI's default and `--features serverless` runs.

/// A minimal, valid outbound serverless webhook configuration.
///
/// `ServerlessConnectorConfig` flattens its untagged `ServerlessConfig`, so the
/// connector-specific keys sit at the top level next to `connector_type`.
fn serverless_webhook_config() -> serde_json::Value {
    serde_json::json!({
        "connector_type": "http_webhook_outbound",
        "endpoint": "https://example.invalid/hook",
    })
}

/// A minimal, valid AWS Lambda cloud-function configuration.
///
/// `CloudFunctionConfig` likewise flattens its untagged `ProviderConfig`.
fn cloud_function_config() -> serde_json::Value {
    serde_json::json!({
        "provider": "aws_lambda",
        "function_name": "streamline-events",
    })
}

fn typed_sink_config(
    name: &str,
    sink_type: SinkType,
    topic: &str,
    cfg: serde_json::Value,
) -> SinkConfig {
    SinkConfig {
        name: name.to_string(),
        sink_type,
        topics: vec![topic.to_string()],
        config: cfg,
    }
}

#[cfg(not(feature = "serverless"))]
mod without_the_serverless_feature {
    use super::*;
    use streamline::sink::unavailable::{
        CLOUD_FUNCTION_UNAVAILABLE_REASON, SERVERLESS_UNAVAILABLE_REASON,
    };

    #[test]
    fn both_connectors_report_unavailable_with_an_actionable_reason() {
        for (sink_type, expected) in [
            (SinkType::Serverless, SERVERLESS_UNAVAILABLE_REASON),
            (SinkType::CloudFunction, CLOUD_FUNCTION_UNAVAILABLE_REASON),
        ] {
            assert!(
                !is_available(sink_type),
                "{sink_type} must be unavailable without the `serverless` feature"
            );
            let reason = unavailable_reason(sink_type).expect("must be unavailable");
            assert_eq!(reason, expected);
            assert!(
                reason.contains("--features serverless"),
                "the reason must tell the operator how to fix it, got: {reason}"
            );
            assert!(
                reason.contains("unavailable"),
                "the reason must say the connector is unavailable, got: {reason}"
            );
            // This is a build-configuration problem, not a security removal.
            assert!(
                !reason.contains("RUSTSEC"),
                "the serverless reason must not borrow the lakehouse advisories: {reason}"
            );
        }
    }

    /// The rejection must happen *before* duplicate, topic and config
    /// validation, so the caller always learns the real problem.
    #[tokio::test]
    async fn creation_is_rejected_before_topic_validation() {
        let (_dir, manager) = manager_with_topic("events");

        for (name, sink_type, expected) in [
            (
                "missing-topic-serverless",
                SinkType::Serverless,
                SERVERLESS_UNAVAILABLE_REASON,
            ),
            (
                "missing-topic-cloud-function",
                SinkType::CloudFunction,
                CLOUD_FUNCTION_UNAVAILABLE_REASON,
            ),
        ] {
            // A topic that does not exist would normally fail validation first.
            let err = manager
                .create_sink(sink_config(name, sink_type, "no-such-topic"))
                .await
                .expect_err("creation must fail");
            // `StreamlineError::Sink` prefixes its Display with "Sink error: ".
            let message = err.to_string();
            assert!(
                message.ends_with(expected),
                "expected the availability reason, not a topic error: {message}"
            );
            assert!(
                !message.contains("does not exist"),
                "topic validation must not run first: {message}"
            );
        }
    }

    /// Malformed connector configuration must also lose to the availability
    /// check — otherwise the error tells the operator to fix their JSON when
    /// the real problem is the build.
    #[tokio::test]
    async fn creation_is_rejected_before_config_validation() {
        let (_dir, manager) = manager_with_topic("events");

        for (name, sink_type, expected) in [
            (
                "bad-config-serverless",
                SinkType::Serverless,
                SERVERLESS_UNAVAILABLE_REASON,
            ),
            (
                "bad-config-cloud-function",
                SinkType::CloudFunction,
                CLOUD_FUNCTION_UNAVAILABLE_REASON,
            ),
        ] {
            let err = manager
                .create_sink(typed_sink_config(
                    name,
                    sink_type,
                    "events",
                    serde_json::json!({ "connector_type": 17, "nonsense": true }),
                ))
                .await
                .expect_err("creation must fail");
            let message = err.to_string();
            assert!(
                message.ends_with(expected),
                "expected the availability reason: {message}"
            );
            assert!(
                !message.contains("Invalid"),
                "config validation must not run first: {message}"
            );
        }
    }

    /// Nothing may reach the registry, including on a repeat attempt — a
    /// duplicate-name error would prove the first attempt had registered.
    #[tokio::test]
    async fn nothing_is_registered_and_repeat_attempts_report_the_same_reason() {
        let (_dir, manager) = manager_with_topic("events");

        for (sink_type, cfg) in [
            (SinkType::Serverless, serverless_webhook_config()),
            (SinkType::CloudFunction, cloud_function_config()),
        ] {
            let first = manager
                .create_sink(typed_sink_config("dup", sink_type, "events", cfg.clone()))
                .await
                .expect_err("creation must fail");
            let second = manager
                .create_sink(typed_sink_config("dup", sink_type, "events", cfg))
                .await
                .expect_err("creation must fail again");
            assert_eq!(
                first.to_string(),
                second.to_string(),
                "the second attempt must repeat the availability reason, not report a \
                 duplicate — a duplicate error would mean the first attempt registered"
            );
        }

        assert!(
            manager.list_sinks().is_empty(),
            "no serverless sink may be registered without the feature: {:?}",
            manager.list_sinks()
        );
        assert!(
            manager.list_sinks_detailed().await.is_empty(),
            "no serverless sink may appear in detailed listings"
        );
    }

    /// And with nothing registered there is nothing to start, so no batch can
    /// ever be delivered (or silently dropped).
    #[tokio::test]
    async fn a_rejected_sink_cannot_be_started_or_inspected() {
        let (_dir, manager) = manager_with_topic("events");
        let _ = manager
            .create_sink(typed_sink_config(
                "events-webhook",
                SinkType::Serverless,
                "events",
                serverless_webhook_config(),
            ))
            .await;

        assert!(manager.start_sink("events-webhook").await.is_err());
        assert!(manager.stop_sink("events-webhook").await.is_err());
        assert!(manager.get_status("events-webhook").await.is_err());
        assert!(manager.get_metrics("events-webhook").await.is_err());
    }
}

#[cfg(feature = "serverless")]
mod with_the_serverless_feature {
    use super::*;

    #[test]
    fn both_connectors_report_available() {
        assert!(is_available(SinkType::Serverless));
        assert!(is_available(SinkType::CloudFunction));
        assert_eq!(unavailable_reason(SinkType::Serverless), None);
        assert_eq!(unavailable_reason(SinkType::CloudFunction), None);
        assert!(unavailable_error(SinkType::Serverless).is_none());
        assert!(unavailable_error(SinkType::CloudFunction).is_none());
    }

    /// The gate must not have cost the feature build anything: valid minimal
    /// configurations still construct and register.
    #[tokio::test]
    async fn valid_configurations_construct_and_register() {
        let (_dir, manager) = manager_with_topic("events");

        manager
            .create_sink(typed_sink_config(
                "events-webhook",
                SinkType::Serverless,
                "events",
                serverless_webhook_config(),
            ))
            .await
            .expect("a valid serverless webhook sink must be created");

        manager
            .create_sink(typed_sink_config(
                "events-lambda",
                SinkType::CloudFunction,
                "events",
                cloud_function_config(),
            ))
            .await
            .expect("a valid cloud function sink must be created");

        let mut registered = manager.list_sinks();
        registered.sort();
        assert_eq!(registered, vec!["events-lambda", "events-webhook"]);

        let detailed = manager.list_sinks_detailed().await;
        assert_eq!(detailed.len(), 2);
        assert!(manager.get_status("events-webhook").await.is_ok());
        assert!(manager.get_metrics("events-lambda").await.is_ok());
    }

    /// With the feature on, the *other* validations are reachable again — this
    /// is what proves the availability gate is feature-scoped rather than a
    /// blanket rejection.
    #[tokio::test]
    async fn ordinary_validation_errors_are_reachable_again() {
        let (_dir, manager) = manager_with_topic("events");

        let missing_topic = manager
            .create_sink(typed_sink_config(
                "missing",
                SinkType::Serverless,
                "no-such-topic",
                serverless_webhook_config(),
            ))
            .await
            .expect_err("a missing topic must still fail");
        assert!(
            missing_topic.to_string().contains("does not exist"),
            "expected a topic error, got: {missing_topic}"
        );

        let bad_config = manager
            .create_sink(typed_sink_config(
                "malformed",
                SinkType::Serverless,
                "events",
                serde_json::json!({ "connector_type": 17 }),
            ))
            .await
            .expect_err("a malformed config must still fail");
        assert!(
            bad_config.to_string().contains("Invalid serverless config"),
            "expected a config error, got: {bad_config}"
        );
    }
}
