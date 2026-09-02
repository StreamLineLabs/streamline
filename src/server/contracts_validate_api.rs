//! HTTP API for contract validation (M4 wiring — pre-flight / dry-run).
//!
//! Producers and CI pipelines can validate a payload against a contract
//! without going through the actual produce path. This is the cheapest way
//! to surface contract violations during development and in golden-test
//! suites.
//!
//! Routes:
//!   * `POST /api/v1/contracts/validate` — body `{ contract, value }`,
//!     returns 200 on success or 400 with a [`ContractRejection`] payload.
//!   * `POST /api/v1/contracts/apply` — register a contract for a topic.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use crate::contracts::produce_guard::{
    validate_record, Contract, ContractRejection, ExpectedType, FieldAssertion,
};

#[derive(Debug, Deserialize)]
pub struct AssertionDto {
    pub path: String,
    pub expected: String,
}

#[derive(Debug, Deserialize)]
pub struct ContractDto {
    pub topic: String,
    #[serde(default)]
    pub version: u32,
    #[serde(default)]
    pub schema_id: Option<u32>,
    #[serde(default)]
    pub assertions: Vec<AssertionDto>,
}

#[derive(Debug, Deserialize)]
pub struct ValidateRequest {
    pub contract: ContractDto,
    /// Partition is informational; pure validation is partition-independent.
    #[serde(default)]
    pub partition: i32,
    /// Either a JSON value or a string containing JSON. Both are accepted.
    pub value: serde_json::Value,
}

#[derive(Debug, Serialize)]
pub struct ValidateOk {
    pub status: &'static str,
    pub topic: String,
}

#[derive(Debug, Serialize)]
pub struct ValidateErr {
    pub status: &'static str,
    pub topic: String,
    pub partition: i32,
    pub field_path: String,
    pub expected: String,
    pub actual: String,
    pub schema_id: Option<u32>,
    pub message: String,
}

fn parse_expected(s: &str) -> Result<ExpectedType, String> {
    match s.to_ascii_lowercase().as_str() {
        "number" => Ok(ExpectedType::Number),
        "string" => Ok(ExpectedType::String),
        "bool" | "boolean" => Ok(ExpectedType::Bool),
        "object" => Ok(ExpectedType::Object),
        "array" => Ok(ExpectedType::Array),
        other => Err(format!("unknown expected type: {other}")),
    }
}

async fn validate(Json(req): Json<ValidateRequest>) -> Response {
    let mut contract = Contract {
        topic: req.contract.topic.clone(),
        version: req.contract.version,
        schema_id: req.contract.schema_id,
        assertions: Vec::with_capacity(req.contract.assertions.len()),
    };
    for a in req.contract.assertions {
        match parse_expected(&a.expected) {
            Ok(ty) => contract.assertions.push(FieldAssertion {
                path: a.path,
                expected: ty,
            }),
            Err(msg) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "status": "invalid_contract",
                        "message": msg
                    })),
                )
                    .into_response();
            }
        }
    }

    let bytes = match &req.value {
        serde_json::Value::String(s) => s.as_bytes().to_vec(),
        v => v.to_string().into_bytes(),
    };

    match validate_record(&contract.topic, req.partition, &bytes, &contract) {
        Ok(()) => (
            StatusCode::OK,
            Json(ValidateOk {
                status: "valid",
                topic: contract.topic,
            }),
        )
            .into_response(),
        Err(r) => rejection_response(&contract.topic, r),
    }
}

fn rejection_response(topic: &str, r: ContractRejection) -> Response {
    let message = r.to_string();
    let body = ValidateErr {
        status: "rejected",
        topic: topic.to_string(),
        partition: r.partition,
        field_path: r.field_path,
        expected: r.expected,
        actual: r.actual,
        schema_id: r.schema_id,
        message,
    };
    (StatusCode::BAD_REQUEST, Json(body)).into_response()
}

// ---------------------------------------------------------------------------
// Contract apply (register/store a contract for a topic)
// ---------------------------------------------------------------------------

/// Global in-memory contract registry.
fn contract_registry() -> &'static Mutex<HashMap<String, StoredContract>> {
    static REGISTRY: OnceLock<Mutex<HashMap<String, StoredContract>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

#[derive(Debug, Clone)]
struct StoredContract {
    topic: String,
    version: u32,
    contract: Contract,
}

#[derive(Debug, Deserialize)]
pub struct ApplyContractRequest {
    pub topic: String,
    #[serde(default)]
    pub assertions: Vec<AssertionDto>,
}

#[derive(Debug, Serialize)]
pub struct ApplyContractResponse {
    pub applied: bool,
    pub topic: String,
    pub version: u32,
}

async fn apply_handler(Json(req): Json<ApplyContractRequest>) -> Response {
    if req.topic.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "status": "invalid_request",
                "message": "topic must not be empty"
            })),
        )
            .into_response();
    }

    let mut assertions = Vec::with_capacity(req.assertions.len());
    for a in &req.assertions {
        match parse_expected(&a.expected) {
            Ok(ty) => assertions.push(FieldAssertion {
                path: a.path.clone(),
                expected: ty,
            }),
            Err(msg) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "status": "invalid_contract",
                        "message": msg
                    })),
                )
                    .into_response();
            }
        }
    }

    let mut registry = contract_registry().lock().unwrap_or_else(|e| e.into_inner());
    let version = registry
        .get(&req.topic)
        .map(|c| c.version + 1)
        .unwrap_or(1);

    let contract = Contract {
        topic: req.topic.clone(),
        version,
        schema_id: None,
        assertions,
    };

    registry.insert(
        req.topic.clone(),
        StoredContract {
            topic: req.topic.clone(),
            version,
            contract,
        },
    );

    (
        StatusCode::CREATED,
        Json(ApplyContractResponse {
            applied: true,
            topic: req.topic,
            version,
        }),
    )
        .into_response()
}

pub fn create_contracts_validate_router() -> Router {
    Router::new()
        .route("/api/v1/contracts/validate", post(validate))
        .route("/api/v1/contracts/apply", post(apply_handler))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{to_bytes, Body};
    use axum::http::Request;
    use tower::ServiceExt;

    fn post_json(body: serde_json::Value) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri("/api/v1/contracts/validate")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    }

    #[tokio::test]
    async fn empty_contract_passes() {
        let app = create_contracts_validate_router();
        let resp = app
            .oneshot(post_json(serde_json::json!({
                "contract": {"topic": "t"},
                "value": {"any": "thing"}
            })))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn missing_field_returns_400_with_actionable_body() {
        let app = create_contracts_validate_router();
        let resp = app
            .oneshot(post_json(serde_json::json!({
                "contract": {
                    "topic": "orders",
                    "schema_id": 7,
                    "assertions": [{"path": "$.amount", "expected": "number"}]
                },
                "value": {"id": 1}
            })))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let bytes = to_bytes(resp.into_body(), 65_536).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["status"], "rejected");
        assert_eq!(body["field_path"], "$.amount");
        assert_eq!(body["actual"], "missing");
        assert_eq!(body["schema_id"], 7);
    }

    #[tokio::test]
    async fn wrong_type_returns_400() {
        let app = create_contracts_validate_router();
        let resp = app
            .oneshot(post_json(serde_json::json!({
                "contract": {
                    "topic": "orders",
                    "assertions": [{"path": "$.amount", "expected": "number"}]
                },
                "value": {"amount": "twenty"}
            })))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn unknown_expected_type_returns_400_invalid_contract() {
        let app = create_contracts_validate_router();
        let resp = app
            .oneshot(post_json(serde_json::json!({
                "contract": {
                    "topic": "t",
                    "assertions": [{"path": "$.x", "expected": "uint128"}]
                },
                "value": {}
            })))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let bytes = to_bytes(resp.into_body(), 65_536).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["status"], "invalid_contract");
    }

    #[tokio::test]
    async fn value_passed_as_string_is_parsed() {
        let app = create_contracts_validate_router();
        let resp = app
            .oneshot(post_json(serde_json::json!({
                "contract": {
                    "topic": "t",
                    "assertions": [{"path": "$.x", "expected": "number"}]
                },
                "value": "{\"x\": 1}"
            })))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn apply_stores_contract() {
        let app = create_contracts_validate_router();
        let body = serde_json::json!({
            "topic": "orders",
            "assertions": [{"path": "$.amount", "expected": "number"}]
        });
        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/contracts/apply")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        let bytes = to_bytes(resp.into_body(), 65_536).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["applied"], true);
        assert_eq!(body["topic"], "orders");
        assert!(body["version"].as_u64().unwrap() >= 1);
    }

    #[tokio::test]
    async fn apply_rejects_empty_topic() {
        let app = create_contracts_validate_router();
        let body = serde_json::json!({
            "topic": "",
            "assertions": []
        });
        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/contracts/apply")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn apply_rejects_invalid_assertion_type() {
        let app = create_contracts_validate_router();
        let body = serde_json::json!({
            "topic": "t",
            "assertions": [{"path": "$.x", "expected": "uint128"}]
        });
        let req = Request::builder()
            .method("POST")
            .uri("/api/v1/contracts/apply")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // -----------------------------------------------------------------------
    // Serde unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn validate_request_deserializes_correctly() {
        let json = r#"{
            "contract": {
                "topic": "orders",
                "version": 2,
                "schema_id": 7,
                "assertions": [{"path": "$.amount", "expected": "number"}]
            },
            "partition": 3,
            "value": {"amount": 42}
        }"#;
        let req: ValidateRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.contract.topic, "orders");
        assert_eq!(req.contract.version, 2);
        assert_eq!(req.contract.schema_id, Some(7));
        assert_eq!(req.contract.assertions.len(), 1);
        assert_eq!(req.contract.assertions[0].path, "$.amount");
        assert_eq!(req.contract.assertions[0].expected, "number");
        assert_eq!(req.partition, 3);
    }

    #[test]
    fn validate_request_defaults() {
        let json = r#"{"contract":{"topic":"t"},"value":{}}"#;
        let req: ValidateRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.contract.version, 0);
        assert!(req.contract.schema_id.is_none());
        assert!(req.contract.assertions.is_empty());
        assert_eq!(req.partition, 0);
    }

    #[test]
    fn validate_ok_serializes() {
        let ok = ValidateOk {
            status: "valid",
            topic: "orders".into(),
        };
        let json = serde_json::to_value(&ok).unwrap();
        assert_eq!(json["status"], "valid");
        assert_eq!(json["topic"], "orders");
    }

    #[test]
    fn validate_err_serializes_with_violations() {
        let err = ValidateErr {
            status: "rejected",
            topic: "orders".into(),
            partition: 0,
            field_path: "$.amount".into(),
            expected: "number".into(),
            actual: "string".into(),
            schema_id: Some(7),
            message: "field type mismatch".into(),
        };
        let json = serde_json::to_value(&err).unwrap();
        assert_eq!(json["status"], "rejected");
        assert_eq!(json["topic"], "orders");
        assert_eq!(json["field_path"], "$.amount");
        assert_eq!(json["expected"], "number");
        assert_eq!(json["actual"], "string");
        assert_eq!(json["schema_id"], 7);
    }

    #[test]
    fn validate_err_null_schema_id() {
        let err = ValidateErr {
            status: "rejected",
            topic: "t".into(),
            partition: 0,
            field_path: "$.x".into(),
            expected: "number".into(),
            actual: "missing".into(),
            schema_id: None,
            message: "field missing".into(),
        };
        let json = serde_json::to_value(&err).unwrap();
        assert!(json["schema_id"].is_null());
    }

    #[test]
    fn apply_contract_request_deserializes() {
        let json = r#"{
            "topic": "events",
            "assertions": [
                {"path": "$.user_id", "expected": "string"},
                {"path": "$.count", "expected": "number"}
            ]
        }"#;
        let req: ApplyContractRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.topic, "events");
        assert_eq!(req.assertions.len(), 2);
        assert_eq!(req.assertions[0].path, "$.user_id");
        assert_eq!(req.assertions[1].expected, "number");
    }

    #[test]
    fn apply_contract_request_defaults_assertions() {
        let json = r#"{"topic": "t"}"#;
        let req: ApplyContractRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.topic, "t");
        assert!(req.assertions.is_empty());
    }

    #[test]
    fn apply_contract_response_serializes() {
        let resp = ApplyContractResponse {
            applied: true,
            topic: "orders".into(),
            version: 3,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["applied"], true);
        assert_eq!(json["topic"], "orders");
        assert_eq!(json["version"], 3);
    }

    #[test]
    fn contract_dto_deserializes_with_defaults() {
        let json = r#"{"topic": "t"}"#;
        let dto: ContractDto = serde_json::from_str(json).unwrap();
        assert_eq!(dto.topic, "t");
        assert_eq!(dto.version, 0);
        assert!(dto.schema_id.is_none());
        assert!(dto.assertions.is_empty());
    }

    #[test]
    fn assertion_dto_deserializes() {
        let json = r#"{"path": "$.name", "expected": "string"}"#;
        let dto: AssertionDto = serde_json::from_str(json).unwrap();
        assert_eq!(dto.path, "$.name");
        assert_eq!(dto.expected, "string");
    }

    #[test]
    fn parse_expected_valid_types() {
        assert!(matches!(parse_expected("number"), Ok(ExpectedType::Number)));
        assert!(matches!(parse_expected("string"), Ok(ExpectedType::String)));
        assert!(matches!(parse_expected("bool"), Ok(ExpectedType::Bool)));
        assert!(matches!(parse_expected("boolean"), Ok(ExpectedType::Bool)));
        assert!(matches!(parse_expected("object"), Ok(ExpectedType::Object)));
        assert!(matches!(parse_expected("array"), Ok(ExpectedType::Array)));
        assert!(matches!(parse_expected("NUMBER"), Ok(ExpectedType::Number)));
    }

    #[test]
    fn parse_expected_invalid_type() {
        assert!(parse_expected("uint128").is_err());
        assert!(parse_expected("").is_err());
    }
}
