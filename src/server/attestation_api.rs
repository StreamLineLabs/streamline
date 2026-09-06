//! HTTP API for event attestation (M4 wiring).
//!
//! Routes:
//!   * `POST /api/v1/attest` — sign an attestation envelope for a record.
//!     Returns base64-encoded signature plus the canonical header value
//!     that producers should attach as `streamline-attest`.
//!   * `POST /api/v1/attest/verify` — verify a sig against an envelope.
//!
//! Both routes use a process-global [`LocalAgeProvider`] keyed by `key_id`.
//! Production wiring would inject a real KMS-backed provider via
//! application state; the route shape is identical.

use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use base64::Engine;
use serde::{Deserialize, Serialize};

use crate::security::attestation::{
    sign_attestation, verify_attestation, Attestation, ATTEST_HEADER,
};
use crate::security::kms::Algorithm;
use crate::security::local_age::LocalAgeProvider;

#[derive(Clone)]
pub struct AttestationApiState {
    pub provider: Arc<LocalAgeProvider>,
}

impl AttestationApiState {
    pub fn shared() -> Self {
        static PROVIDER: OnceLock<Arc<LocalAgeProvider>> = OnceLock::new();
        Self {
            provider: PROVIDER
                .get_or_init(|| Arc::new(LocalAgeProvider::new()))
                .clone(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct AttestRequest {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    /// UTF-8 record value. Use `value_b64` for binary payloads.
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub value_b64: Option<String>,
    #[serde(default)]
    pub schema_id: u32,
    #[serde(default)]
    pub timestamp_ms: Option<i64>,
    pub key_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AttestResponse {
    pub key_id: String,
    pub algorithm: String,
    pub timestamp_ms: i64,
    pub payload_sha256: String,
    pub signature_b64: String,
    pub header_name: String,
    /// Wire-format value for the `streamline-attest` header. The current
    /// encoding is the base64 signature; verifiers must reconstruct the
    /// envelope from record metadata.
    pub header_value: String,
}

#[derive(Debug, Deserialize)]
pub struct VerifyRequest {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub value_b64: Option<String>,
    #[serde(default)]
    pub schema_id: u32,
    pub timestamp_ms: i64,
    pub key_id: String,
    pub signature_b64: String,
    #[serde(default = "default_algorithm")]
    pub algorithm: String,
}

fn default_algorithm() -> String {
    "ed25519".to_string()
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VerifyResponse {
    pub valid: bool,
    pub key_id: String,
    pub algorithm: String,
}

#[derive(Debug, Serialize)]
struct ApiError {
    error: &'static str,
    message: String,
}

fn bad_request(message: impl Into<String>) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(ApiError {
            error: "invalid_argument",
            message: message.into(),
        }),
    )
        .into_response()
}

fn parse_algorithm(s: &str) -> Result<Algorithm, String> {
    match s.to_ascii_lowercase().as_str() {
        "ed25519" => Ok(Algorithm::Ed25519),
        "ecdsa-p256" | "ecdsa_p256" | "p256" => Ok(Algorithm::EcdsaP256),
        other => Err(format!("unsupported algorithm: {other}")),
    }
}

fn resolve_value(text: Option<&String>, b64: Option<&String>) -> Result<Vec<u8>, String> {
    match (text, b64) {
        (Some(t), None) => Ok(t.as_bytes().to_vec()),
        (None, Some(s)) => base64::engine::general_purpose::STANDARD
            .decode(s.as_bytes())
            .map_err(|e| format!("invalid base64: {e}")),
        (Some(_), Some(_)) => Err("provide exactly one of `value` or `value_b64`".into()),
        (None, None) => Err("missing record value: provide `value` or `value_b64`".into()),
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

async fn attest(
    axum::extract::State(state): axum::extract::State<AttestationApiState>,
    Json(req): Json<AttestRequest>,
) -> Response {
    let bytes = match resolve_value(req.value.as_ref(), req.value_b64.as_ref()) {
        Ok(b) => b,
        Err(m) => return bad_request(m),
    };
    let timestamp_ms = req.timestamp_ms.unwrap_or_else(now_ms);

    // Auto-register the key if missing so the API is self-contained for the
    // common dev/CI case. Production callers pre-provision keys via KMS.
    if state
        .provider
        .register_key(&req.key_id, Algorithm::Ed25519)
        .is_err()
    {
        // Already exists — ignore; sign() will surface real errors.
    }

    let att = Attestation::for_record(
        &req.topic,
        req.partition,
        req.offset,
        &bytes,
        req.schema_id,
        timestamp_ms,
        &req.key_id,
    );
    let sig = match sign_attestation(state.provider.as_ref(), &req.key_id, &att) {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError {
                    error: "sign_failed",
                    message: e.to_string(),
                }),
            )
                .into_response()
        }
    };
    let sig_b64 = base64::engine::general_purpose::STANDARD.encode(&sig);

    (
        StatusCode::OK,
        Json(AttestResponse {
            key_id: req.key_id,
            algorithm: "ed25519".into(),
            timestamp_ms,
            payload_sha256: att.payload_sha256.clone(),
            header_value: sig_b64.clone(),
            header_name: ATTEST_HEADER.to_string(),
            signature_b64: sig_b64,
        }),
    )
        .into_response()
}

async fn verify(
    axum::extract::State(state): axum::extract::State<AttestationApiState>,
    Json(req): Json<VerifyRequest>,
) -> Response {
    let bytes = match resolve_value(req.value.as_ref(), req.value_b64.as_ref()) {
        Ok(b) => b,
        Err(m) => return bad_request(m),
    };
    let alg = match parse_algorithm(&req.algorithm) {
        Ok(a) => a,
        Err(m) => return bad_request(m),
    };
    let sig = match base64::engine::general_purpose::STANDARD.decode(req.signature_b64.as_bytes()) {
        Ok(s) => s,
        Err(e) => return bad_request(format!("invalid signature_b64: {e}")),
    };
    let att = Attestation::for_record(
        &req.topic,
        req.partition,
        req.offset,
        &bytes,
        req.schema_id,
        req.timestamp_ms,
        &req.key_id,
    );
    let valid = verify_attestation(state.provider.as_ref(), &att, &sig, alg).is_ok();
    (
        StatusCode::OK,
        Json(VerifyResponse {
            valid,
            key_id: req.key_id,
            algorithm: req.algorithm,
        }),
    )
        .into_response()
}

pub fn create_attestation_api_router(state: AttestationApiState) -> Router {
    Router::new()
        .route("/api/v1/attest", post(attest))
        .route("/api/v1/attest/verify", post(verify))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{to_bytes, Body};
    use axum::http::Request;
    use tower::ServiceExt;

    fn fresh_state() -> AttestationApiState {
        AttestationApiState {
            provider: Arc::new(LocalAgeProvider::new()),
        }
    }

    fn post_json(uri: &str, body: serde_json::Value) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    }

    #[tokio::test]
    async fn attest_then_verify_roundtrip() {
        let state = fresh_state();
        let app = create_attestation_api_router(state);

        let resp = app
            .clone()
            .oneshot(post_json(
                "/api/v1/attest",
                serde_json::json!({
                    "topic": "orders",
                    "partition": 0,
                    "offset": 42,
                    "value": "{\"id\":1}",
                    "schema_id": 7,
                    "timestamp_ms": 1700000000000_i64,
                    "key_id": "broker-0"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = to_bytes(resp.into_body(), 65_536).await.unwrap();
        let signed: AttestResponse = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(signed.header_name, ATTEST_HEADER);
        assert!(!signed.signature_b64.is_empty());

        let resp = app
            .oneshot(post_json(
                "/api/v1/attest/verify",
                serde_json::json!({
                    "topic": "orders",
                    "partition": 0,
                    "offset": 42,
                    "value": "{\"id\":1}",
                    "schema_id": 7,
                    "timestamp_ms": 1700000000000_i64,
                    "key_id": "broker-0",
                    "signature_b64": signed.signature_b64,
                    "algorithm": "ed25519"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = to_bytes(resp.into_body(), 65_536).await.unwrap();
        let v: VerifyResponse = serde_json::from_slice(&bytes).unwrap();
        assert!(v.valid);
    }

    #[tokio::test]
    async fn verify_rejects_tampered_offset() {
        let state = fresh_state();
        let app = create_attestation_api_router(state);

        let resp = app
            .clone()
            .oneshot(post_json(
                "/api/v1/attest",
                serde_json::json!({
                    "topic": "t",
                    "partition": 0,
                    "offset": 1,
                    "value": "x",
                    "timestamp_ms": 1,
                    "key_id": "k"
                }),
            ))
            .await
            .unwrap();
        let bytes = to_bytes(resp.into_body(), 65_536).await.unwrap();
        let signed: AttestResponse = serde_json::from_slice(&bytes).unwrap();

        let resp = app
            .oneshot(post_json(
                "/api/v1/attest/verify",
                serde_json::json!({
                    "topic": "t",
                    "partition": 0,
                    "offset": 99,
                    "value": "x",
                    "timestamp_ms": 1,
                    "key_id": "k",
                    "signature_b64": signed.signature_b64
                }),
            ))
            .await
            .unwrap();
        let bytes = to_bytes(resp.into_body(), 65_536).await.unwrap();
        let v: VerifyResponse = serde_json::from_slice(&bytes).unwrap();
        assert!(!v.valid);
    }

    #[tokio::test]
    async fn missing_value_returns_400() {
        let state = fresh_state();
        let app = create_attestation_api_router(state);
        let resp = app
            .oneshot(post_json(
                "/api/v1/attest",
                serde_json::json!({
                    "topic": "t",
                    "partition": 0,
                    "offset": 0,
                    "key_id": "k"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn binary_value_via_b64() {
        let state = fresh_state();
        let app = create_attestation_api_router(state);
        let payload = base64::engine::general_purpose::STANDARD.encode([0u8, 1, 2, 3]);
        let resp = app
            .oneshot(post_json(
                "/api/v1/attest",
                serde_json::json!({
                    "topic": "t",
                    "partition": 0,
                    "offset": 0,
                    "value_b64": payload,
                    "timestamp_ms": 1,
                    "key_id": "k"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    /// Regression: when the broker is constructed with an injected
    /// `LocalAgeProvider`, signatures produced by tenant A's provider
    /// must NOT verify under tenant B's provider. Locks in multi-tenant
    /// key isolation.
    #[tokio::test]
    async fn injected_provider_isolates_keys_per_tenant() {
        let provider_a = Arc::new(LocalAgeProvider::new());
        let provider_b = Arc::new(LocalAgeProvider::new());
        let app_a = create_attestation_api_router(AttestationApiState {
            provider: provider_a,
        });
        let app_b = create_attestation_api_router(AttestationApiState {
            provider: provider_b,
        });

        // Sign with tenant A.
        let resp = app_a
            .oneshot(post_json(
                "/api/v1/attest",
                serde_json::json!({
                    "topic": "t",
                    "partition": 0,
                    "offset": 1,
                    "value": "v",
                    "timestamp_ms": 1,
                    "key_id": "tenant-a"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = to_bytes(resp.into_body(), 65_536).await.unwrap();
        let signed: AttestResponse = serde_json::from_slice(&bytes).unwrap();

        // Verify in tenant B's app — must NOT be valid (different keypair).
        let resp = app_b
            .oneshot(post_json(
                "/api/v1/attest/verify",
                serde_json::json!({
                    "topic": "t",
                    "partition": 0,
                    "offset": 1,
                    "value": "v",
                    "timestamp_ms": 1,
                    "key_id": "tenant-a",
                    "signature_b64": signed.signature_b64,
                    "algorithm": "ed25519"
                }),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = to_bytes(resp.into_body(), 65_536).await.unwrap();
        let v: VerifyResponse = serde_json::from_slice(&bytes).unwrap();
        assert!(
            !v.valid,
            "tenant-b accepted a signature from tenant-a's keypair: leak!"
        );
    }
}
