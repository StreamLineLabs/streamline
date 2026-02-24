//! Schema Registry REST API
//!
//! This module provides Confluent-compatible Schema Registry REST API endpoints.
//!
//! ## Endpoints
//!
//! - `GET /subjects` - List all subjects
//! - `GET /subjects/{subject}/versions` - List versions for a subject
//! - `GET /subjects/{subject}/versions/{version}` - Get schema by version
//! - `GET /subjects/{subject}/versions/latest` - Get latest schema
//! - `GET /subjects/{subject}/versions/{version}/schema` - Get raw schema string
//! - `GET /subjects/{subject}/versions/{version}/referencedby` - Get referencing schema IDs
//! - `POST /subjects/{subject}/versions` - Register a new schema
//! - `POST /subjects/{subject}` - Check if schema exists
//! - `DELETE /subjects/{subject}` - Delete a subject
//! - `DELETE /subjects/{subject}/versions/{version}` - Delete a version
//! - `POST /compatibility/subjects/{subject}/versions/{version}` - Check compatibility
//! - `POST /compatibility/subjects/{subject}/versions/latest` - Check compatibility with latest
//! - `GET /config` - Get global compatibility level
//! - `PUT /config` - Set global compatibility level
//! - `GET /config/{subject}` - Get subject compatibility level
//! - `PUT /config/{subject}` - Set subject compatibility level
//! - `DELETE /config/{subject}` - Delete subject config (reset to global)
//! - `GET /schemas/ids/{id}` - Get schema by ID
//! - `GET /schemas/ids/{id}/schema` - Get raw schema string by ID
//! - `GET /schemas/ids/{id}/subjects` - Get subjects for schema ID
//! - `GET /schemas/ids/{id}/versions` - Get subject-version pairs for schema ID
//! - `GET /schemas/types` - List supported schema types

use crate::schema::{
    CompatibilityCheckRequest, CompatibilityCheckResponse, ConfigResponse, ConfigUpdateRequest,
    RegisteredSchema, SchemaError, SchemaLookupResponse, SchemaRegistrationRequest,
    SchemaRegistrationResponse, SchemaStore, SchemaType,
};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
    Json, Router,
};
use serde::Serialize;
use std::sync::Arc;
use tracing::{debug, error, info};

/// Shared state for the Schema Registry API
#[derive(Clone)]
pub(crate) struct SchemaApiState {
    /// Schema store
    pub store: Arc<SchemaStore>,
}

/// Error response for Schema Registry API
#[derive(Debug, Serialize)]
pub struct SchemaApiError {
    /// Error code (compatible with Confluent Schema Registry)
    pub error_code: i32,
    /// Error message
    pub message: String,
}

impl SchemaApiError {
    fn subject_not_found(subject: &str) -> Self {
        Self {
            error_code: 40401,
            message: format!("Subject '{}' not found.", subject),
        }
    }

    fn version_not_found(subject: &str, version: i32) -> Self {
        Self {
            error_code: 40402,
            message: format!("Version {} not found for subject '{}'.", version, subject),
        }
    }

    fn schema_not_found(id: i32) -> Self {
        Self {
            error_code: 40403,
            message: format!("Schema {} not found.", id),
        }
    }

    fn invalid_schema(message: &str) -> Self {
        Self {
            error_code: 42201,
            message: format!("Invalid schema: {}", message),
        }
    }

    fn incompatible_schema(message: &str) -> Self {
        Self {
            error_code: 409,
            message: format!("Schema is incompatible: {}", message),
        }
    }

    fn internal_error(message: &str) -> Self {
        Self {
            error_code: 50001,
            message: format!("Internal server error: {}", message),
        }
    }
}

impl From<SchemaError> for SchemaApiError {
    fn from(err: SchemaError) -> Self {
        match err {
            SchemaError::SubjectNotFound(subject) => Self::subject_not_found(&subject),
            SchemaError::VersionNotFound { subject, version } => {
                Self::version_not_found(&subject, version)
            }
            SchemaError::SchemaNotFound(msg) => Self {
                error_code: 40403,
                message: format!("Schema not found: {}", msg),
            },
            SchemaError::InvalidSchema(msg) => Self::invalid_schema(&msg),
            SchemaError::IncompatibleSchema(msg) => Self::incompatible_schema(&msg),
            _ => Self::internal_error(&err.to_string()),
        }
    }
}

/// Create the Schema Registry API router
pub(crate) fn create_schema_api_router(state: SchemaApiState) -> Router {
    Router::new()
        // Subjects endpoints
        .route("/subjects", get(list_subjects))
        .route("/subjects/:subject/versions", get(list_versions))
        .route("/subjects/:subject/versions", post(register_schema))
        .route(
            "/subjects/:subject/versions/:version",
            get(get_schema_by_version),
        )
        .route(
            "/subjects/:subject/versions/:version",
            delete(delete_version),
        )
        .route(
            "/subjects/:subject/versions/:version/schema",
            get(get_schema_string_by_version),
        )
        .route(
            "/subjects/:subject/versions/:version/referencedby",
            get(get_referenced_by),
        )
        .route("/subjects/:subject", post(lookup_schema))
        .route("/subjects/:subject", delete(delete_subject))
        // Schemas endpoints
        .route("/schemas/ids/:id", get(get_schema_by_id))
        .route("/schemas/ids/:id/schema", get(get_raw_schema_by_id))
        .route("/schemas/ids/:id/subjects", get(get_schema_subjects))
        .route("/schemas/ids/:id/versions", get(get_schema_versions))
        .route("/schemas/types", get(get_schema_types))
        // Compatibility endpoints
        .route(
            "/compatibility/subjects/:subject/versions/:version",
            post(check_compatibility),
        )
        // Config endpoints
        .route("/config", get(get_global_config))
        .route("/config", put(set_global_config))
        .route("/config/:subject", get(get_subject_config))
        .route("/config/:subject", put(set_subject_config))
        .route("/config/:subject", delete(delete_subject_config))
        // Schema UI/Dashboard endpoints
        .route("/api/v1/schema/dashboard", get(schema_dashboard_handler))
        .route("/api/v1/schema/subjects", get(schema_subjects_browser))
        .route(
            "/api/v1/schema/subjects/:subject/history",
            get(schema_version_history),
        )
        .route("/api/v1/schema/infer", post(schema_auto_infer))
        .route("/api/v1/schema/search", get(schema_search))
        .with_state(state)
}

/// List all subjects
async fn list_subjects(State(state): State<SchemaApiState>) -> Response {
    let subjects = state.store.get_subjects();
    (StatusCode::OK, Json(subjects)).into_response()
}

/// List versions for a subject
async fn list_versions(
    State(state): State<SchemaApiState>,
    Path(subject): Path<String>,
) -> Response {
    match state.store.get_versions(&subject) {
        Ok(versions) => (StatusCode::OK, Json(versions)).into_response(),
        Err(e) => {
            let error: SchemaApiError = e.into();
            (StatusCode::NOT_FOUND, Json(error)).into_response()
        }
    }
}

/// Get schema by version
async fn get_schema_by_version(
    State(state): State<SchemaApiState>,
    Path((subject, version)): Path<(String, String)>,
) -> Response {
    let result = if version == "latest" {
        state.store.get_latest_schema(&subject)
    } else {
        match version.parse::<i32>() {
            Ok(v) => state.store.get_schema(&subject, v),
            Err(_) => {
                let error = SchemaApiError::invalid_schema("Invalid version number");
                return (StatusCode::BAD_REQUEST, Json(error)).into_response();
            }
        }
    };

    match result {
        Ok(schema) => {
            let response = SchemaResponse::from(schema);
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            let error: SchemaApiError = e.into();
            (StatusCode::NOT_FOUND, Json(error)).into_response()
        }
    }
}

/// Schema response (Confluent compatible format)
#[derive(Debug, Serialize)]
struct SchemaResponse {
    subject: String,
    version: i32,
    id: i32,
    #[serde(rename = "schemaType", skip_serializing_if = "is_avro")]
    schema_type: SchemaType,
    schema: String,
}

fn is_avro(schema_type: &SchemaType) -> bool {
    *schema_type == SchemaType::Avro
}

impl From<RegisteredSchema> for SchemaResponse {
    fn from(schema: RegisteredSchema) -> Self {
        Self {
            subject: schema.subject,
            version: schema.version,
            id: schema.id,
            schema_type: schema.schema_type,
            schema: schema.schema,
        }
    }
}

/// Register a new schema
async fn register_schema(
    State(state): State<SchemaApiState>,
    Path(subject): Path<String>,
    Json(request): Json<SchemaRegistrationRequest>,
) -> Response {
    debug!(subject = %subject, schema_type = %request.schema_type, "Registering schema");

    match state
        .store
        .register_schema(
            &subject,
            &request.schema,
            request.schema_type,
            request.references,
        )
        .await
    {
        Ok(id) => {
            info!(subject = %subject, id = id, "Schema registered");
            let response = SchemaRegistrationResponse { id };
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            error!(error = %e, "Failed to register schema");
            let error: SchemaApiError = e.into();
            let status = if error.error_code == 409 {
                StatusCode::CONFLICT
            } else if error.error_code >= 42000 && error.error_code < 43000 {
                StatusCode::UNPROCESSABLE_ENTITY
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            (status, Json(error)).into_response()
        }
    }
}

/// Lookup schema by content
async fn lookup_schema(
    State(state): State<SchemaApiState>,
    Path(subject): Path<String>,
    Json(request): Json<SchemaRegistrationRequest>,
) -> Response {
    match state
        .store
        .lookup_schema(&subject, &request.schema, request.schema_type)
    {
        Some(schema) => {
            let response = SchemaLookupResponse {
                subject: schema.subject,
                id: schema.id,
                version: schema.version,
                schema: schema.schema,
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        None => {
            let error = SchemaApiError::schema_not_found(0);
            (StatusCode::NOT_FOUND, Json(error)).into_response()
        }
    }
}

/// Delete a subject
async fn delete_subject(
    State(state): State<SchemaApiState>,
    Path(subject): Path<String>,
) -> Response {
    match state.store.delete_subject(&subject).await {
        Ok(versions) => {
            info!(subject = %subject, versions = ?versions, "Subject deleted");
            (StatusCode::OK, Json(versions)).into_response()
        }
        Err(e) => {
            let error: SchemaApiError = e.into();
            (StatusCode::NOT_FOUND, Json(error)).into_response()
        }
    }
}

/// Delete a version
async fn delete_version(
    State(state): State<SchemaApiState>,
    Path((subject, version)): Path<(String, i32)>,
) -> Response {
    match state.store.delete_version(&subject, version).await {
        Ok(id) => {
            info!(subject = %subject, version = version, "Version deleted");
            (StatusCode::OK, Json(id)).into_response()
        }
        Err(e) => {
            let error: SchemaApiError = e.into();
            (StatusCode::NOT_FOUND, Json(error)).into_response()
        }
    }
}

/// Get schema by ID
async fn get_schema_by_id(State(state): State<SchemaApiState>, Path(id): Path<i32>) -> Response {
    match state.store.get_schema_by_id(id) {
        Ok(schema) => {
            #[derive(Serialize)]
            struct SchemaByIdResponse {
                schema: String,
                #[serde(rename = "schemaType", skip_serializing_if = "is_avro")]
                schema_type: SchemaType,
            }

            let response = SchemaByIdResponse {
                schema: schema.schema,
                schema_type: schema.schema_type,
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            let error: SchemaApiError = e.into();
            (StatusCode::NOT_FOUND, Json(error)).into_response()
        }
    }
}

/// Get supported schema types
async fn get_schema_types() -> Response {
    let types = vec!["AVRO", "PROTOBUF", "JSON"];
    (StatusCode::OK, Json(types)).into_response()
}

/// Get raw schema string by global ID (GET /schemas/ids/{id}/schema)
async fn get_raw_schema_by_id(State(state): State<SchemaApiState>, Path(id): Path<i32>) -> Response {
    match state.store.get_schema_by_id(id) {
        Ok(schema) => (StatusCode::OK, schema.schema).into_response(),
        Err(e) => {
            let error: SchemaApiError = e.into();
            (StatusCode::NOT_FOUND, Json(error)).into_response()
        }
    }
}

/// Get subjects associated with a schema ID (GET /schemas/ids/{id}/subjects)
async fn get_schema_subjects(
    State(state): State<SchemaApiState>,
    Path(id): Path<i32>,
) -> Response {
    // Verify the schema ID exists first
    if let Err(e) = state.store.get_schema_by_id(id) {
        let error: SchemaApiError = e.into();
        return (StatusCode::NOT_FOUND, Json(error)).into_response();
    }
    let subjects = state.store.get_subjects_for_schema_id(id);
    (StatusCode::OK, Json(subjects)).into_response()
}

/// Get subject-version pairs for a schema ID (GET /schemas/ids/{id}/versions)
async fn get_schema_versions(
    State(state): State<SchemaApiState>,
    Path(id): Path<i32>,
) -> Response {
    // Verify the schema ID exists first
    if let Err(e) = state.store.get_schema_by_id(id) {
        let error: SchemaApiError = e.into();
        return (StatusCode::NOT_FOUND, Json(error)).into_response();
    }
    let versions = state.store.get_subject_versions_for_schema_id(id);
    (StatusCode::OK, Json(versions)).into_response()
}

/// Get raw schema string by subject/version (GET /subjects/{subject}/versions/{version}/schema)
async fn get_schema_string_by_version(
    State(state): State<SchemaApiState>,
    Path((subject, version)): Path<(String, String)>,
) -> Response {
    let result = if version == "latest" {
        state.store.get_latest_schema(&subject)
    } else {
        match version.parse::<i32>() {
            Ok(v) => state.store.get_schema(&subject, v),
            Err(_) => {
                let error = SchemaApiError::invalid_schema("Invalid version number");
                return (StatusCode::BAD_REQUEST, Json(error)).into_response();
            }
        }
    };

    match result {
        Ok(schema) => (StatusCode::OK, schema.schema).into_response(),
        Err(e) => {
            let error: SchemaApiError = e.into();
            (StatusCode::NOT_FOUND, Json(error)).into_response()
        }
    }
}

/// Get schema IDs that reference a given subject/version (GET /subjects/{subject}/versions/{version}/referencedby)
async fn get_referenced_by(
    State(state): State<SchemaApiState>,
    Path((subject, version)): Path<(String, String)>,
) -> Response {
    let v = if version == "latest" {
        match state.store.get_latest_schema(&subject) {
            Ok(schema) => schema.version,
            Err(e) => {
                let error: SchemaApiError = e.into();
                return (StatusCode::NOT_FOUND, Json(error)).into_response();
            }
        }
    } else {
        match version.parse::<i32>() {
            Ok(v) => v,
            Err(_) => {
                let error = SchemaApiError::invalid_schema("Invalid version number");
                return (StatusCode::BAD_REQUEST, Json(error)).into_response();
            }
        }
    };

    // Verify the subject/version exists
    if let Err(e) = state.store.get_schema(&subject, v) {
        let error: SchemaApiError = e.into();
        return (StatusCode::NOT_FOUND, Json(error)).into_response();
    }

    let ids = state.store.get_referencing_schemas(&subject, v);
    (StatusCode::OK, Json(ids)).into_response()
}

/// Check compatibility
async fn check_compatibility(
    State(state): State<SchemaApiState>,
    Path((subject, version)): Path<(String, String)>,
    Json(request): Json<CompatibilityCheckRequest>,
) -> Response {
    let version_opt = if version == "latest" {
        None
    } else {
        match version.parse::<i32>() {
            Ok(v) => Some(v),
            Err(_) => {
                let error = SchemaApiError::invalid_schema("Invalid version number");
                return (StatusCode::BAD_REQUEST, Json(error)).into_response();
            }
        }
    };

    match state
        .store
        .check_compatibility(&subject, &request.schema, request.schema_type, version_opt)
        .await
    {
        Ok((is_compatible, messages)) => {
            let response = CompatibilityCheckResponse {
                is_compatible,
                messages,
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            let error: SchemaApiError = e.into();
            let status = if error.error_code >= 40400 && error.error_code < 40500 {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::UNPROCESSABLE_ENTITY
            };
            (status, Json(error)).into_response()
        }
    }
}

/// Get global config
async fn get_global_config(State(state): State<SchemaApiState>) -> Response {
    let level = state.store.get_global_compatibility().await;
    let response = ConfigResponse {
        compatibility_level: level,
    };
    (StatusCode::OK, Json(response)).into_response()
}

/// Set global config
async fn set_global_config(
    State(state): State<SchemaApiState>,
    Json(request): Json<ConfigUpdateRequest>,
) -> Response {
    match state
        .store
        .set_global_compatibility(request.compatibility)
        .await
    {
        Ok(()) => {
            let response = ConfigResponse {
                compatibility_level: request.compatibility,
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            let error: SchemaApiError = e.into();
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error)).into_response()
        }
    }
}

/// Get subject config
async fn get_subject_config(
    State(state): State<SchemaApiState>,
    Path(subject): Path<String>,
) -> Response {
    let level = state.store.get_subject_compatibility(&subject).await;
    let response = ConfigResponse {
        compatibility_level: level,
    };
    (StatusCode::OK, Json(response)).into_response()
}

/// Set subject config
async fn set_subject_config(
    State(state): State<SchemaApiState>,
    Path(subject): Path<String>,
    Json(request): Json<ConfigUpdateRequest>,
) -> Response {
    match state
        .store
        .set_subject_compatibility(&subject, request.compatibility)
        .await
    {
        Ok(()) => {
            let response = ConfigResponse {
                compatibility_level: request.compatibility,
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            let error: SchemaApiError = e.into();
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error)).into_response()
        }
    }
}

/// Delete subject config (reset to global default)
async fn delete_subject_config(
    State(state): State<SchemaApiState>,
    Path(subject): Path<String>,
) -> Response {
    match state
        .store
        .delete_subject_compatibility(&subject)
        .await
    {
        Ok(previous) => {
            let response = ConfigResponse {
                compatibility_level: previous,
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            let error: SchemaApiError = e.into();
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error)).into_response()
        }
    }
}

// ============================================================================
// Schema UI / Dashboard Endpoints
// ============================================================================

/// Schema dashboard overview response
#[derive(Debug, Serialize)]
struct SchemaDashboardResponse {
    /// Total number of subjects
    total_subjects: usize,
    /// Total number of schema versions
    total_versions: usize,
    /// Total number of unique schema IDs
    total_schemas: usize,
    /// Schema type distribution
    type_distribution: std::collections::HashMap<String, usize>,
    /// Global compatibility level
    global_compatibility: String,
    /// List of subjects with summary
    subjects: Vec<SubjectSummary>,
}

/// Subject summary for dashboard
#[derive(Debug, Serialize)]
struct SubjectSummary {
    /// Subject name
    name: String,
    /// Number of versions
    version_count: usize,
    /// Latest version number
    latest_version: i32,
    /// Schema type
    schema_type: String,
    /// Compatibility level
    compatibility: String,
}

/// Schema version history response
#[derive(Debug, Serialize)]
struct SchemaVersionHistory {
    /// Subject name
    subject: String,
    /// Versions with details
    versions: Vec<VersionDetail>,
}

/// Schema version detail
#[derive(Debug, Serialize)]
struct VersionDetail {
    /// Version number
    version: i32,
    /// Schema ID
    id: i32,
    /// Schema type
    schema_type: String,
    /// Schema content
    schema: String,
}

/// Schema auto-inference request
#[derive(Debug, serde::Deserialize)]
struct SchemaInferRequest {
    /// JSON samples to infer schema from
    samples: Vec<serde_json::Value>,
    /// Output format: "avro" or "json_schema"
    #[serde(default = "default_output_format")]
    output_format: String,
    /// Name for the inferred schema
    #[serde(default = "default_schema_name")]
    name: String,
}

fn default_output_format() -> String {
    "avro".to_string()
}

fn default_schema_name() -> String {
    "InferredSchema".to_string()
}

/// Schema auto-inference response
#[derive(Debug, Serialize)]
struct SchemaInferResponse {
    /// The inferred schema
    schema: serde_json::Value,
    /// Output format used
    format: String,
    /// Number of samples analyzed
    samples_analyzed: usize,
    /// Detected fields
    fields: Vec<InferredFieldInfo>,
}

/// Inferred field info for UI display
#[derive(Debug, Serialize)]
struct InferredFieldInfo {
    /// Field name
    name: String,
    /// Inferred type
    field_type: String,
    /// Whether the field is nullable
    nullable: bool,
}

/// Schema search query
#[derive(Debug, serde::Deserialize)]
struct SchemaSearchQuery {
    /// Search query string
    q: String,
    /// Optional schema type filter
    #[serde(rename = "type")]
    schema_type: Option<String>,
}

/// Schema search result
#[derive(Debug, Serialize)]
struct SchemaSearchResult {
    /// Subject name
    subject: String,
    /// Version
    version: i32,
    /// Schema ID
    id: i32,
    /// Schema type
    schema_type: String,
    /// Matching context snippet
    snippet: String,
}

/// GET /api/v1/schema/dashboard - Schema registry overview
async fn schema_dashboard_handler(State(state): State<SchemaApiState>) -> Response {
    let subjects = state.store.get_subjects();

    let mut type_distribution: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    let mut subject_summaries = Vec::new();
    let mut total_versions = 0usize;
    let mut total_schemas = std::collections::HashSet::new();

    for subject in &subjects {
        let version_count = state
            .store
            .get_versions(subject)
            .map(|v| v.len())
            .unwrap_or(0);
        total_versions += version_count;

        let latest = state.store.get_latest_schema(subject);
        let (latest_version, schema_type_str, compat) = match latest {
            Ok(schema) => {
                total_schemas.insert(schema.id);
                let type_str = format!("{:?}", schema.schema_type);
                *type_distribution.entry(type_str.clone()).or_insert(0) += 1;
                let compat = state.store.get_subject_compatibility(subject).await;
                (schema.version, type_str, format!("{:?}", compat))
            }
            Err(_) => (0, "UNKNOWN".to_string(), "BACKWARD".to_string()),
        };

        subject_summaries.push(SubjectSummary {
            name: subject.clone(),
            version_count,
            latest_version,
            schema_type: schema_type_str,
            compatibility: compat,
        });
    }

    let global_compat = state.store.get_global_compatibility().await;

    let response = SchemaDashboardResponse {
        total_subjects: subjects.len(),
        total_versions,
        total_schemas: total_schemas.len(),
        type_distribution,
        global_compatibility: format!("{:?}", global_compat),
        subjects: subject_summaries,
    };

    (StatusCode::OK, Json(response)).into_response()
}

/// GET /api/v1/schema/subjects - Browse all subjects with details
async fn schema_subjects_browser(State(state): State<SchemaApiState>) -> Response {
    let subjects = state.store.get_subjects();
    let mut summaries = Vec::new();

    for subject in &subjects {
        let version_count = state
            .store
            .get_versions(subject)
            .map(|v| v.len())
            .unwrap_or(0);

        let latest = state.store.get_latest_schema(subject);
        let (latest_version, schema_type_str, compat) = match latest {
            Ok(schema) => {
                let type_str = format!("{:?}", schema.schema_type);
                let compat = state.store.get_subject_compatibility(subject).await;
                (schema.version, type_str, format!("{:?}", compat))
            }
            Err(_) => (0, "UNKNOWN".to_string(), "BACKWARD".to_string()),
        };

        summaries.push(SubjectSummary {
            name: subject.clone(),
            version_count,
            latest_version,
            schema_type: schema_type_str,
            compatibility: compat,
        });
    }

    (StatusCode::OK, Json(summaries)).into_response()
}

/// GET /api/v1/schema/subjects/:subject/history - Get full version history
async fn schema_version_history(
    State(state): State<SchemaApiState>,
    Path(subject): Path<String>,
) -> Response {
    let versions = match state.store.get_versions(&subject) {
        Ok(v) => v,
        Err(e) => {
            let error: SchemaApiError = e.into();
            return (StatusCode::NOT_FOUND, Json(error)).into_response();
        }
    };

    let mut version_details = Vec::new();
    for v in &versions {
        if let Ok(schema) = state.store.get_schema(&subject, *v) {
            version_details.push(VersionDetail {
                version: schema.version,
                id: schema.id,
                schema_type: format!("{:?}", schema.schema_type),
                schema: schema.schema,
            });
        }
    }

    let response = SchemaVersionHistory {
        subject,
        versions: version_details,
    };

    (StatusCode::OK, Json(response)).into_response()
}

/// POST /api/v1/schema/infer - Auto-infer schema from JSON samples
async fn schema_auto_infer(Json(request): Json<SchemaInferRequest>) -> Response {
    use crate::schema::inference::SchemaInferrer;

    let inferrer = SchemaInferrer::new();

    // Convert samples to string representations
    let sample_strings: Vec<String> = request.samples.iter().map(|v| v.to_string()).collect();

    let sample_refs: Vec<&str> = sample_strings.iter().map(|s| s.as_str()).collect();

    let inferred = match inferrer.infer_from_samples(&sample_refs) {
        Ok(schema) => schema,
        Err(e) => {
            let error = SchemaApiError::invalid_schema(&e.to_string());
            return (StatusCode::BAD_REQUEST, Json(error)).into_response();
        }
    };

    // Convert fields to display format (only if the inferred type is an Object)
    let fields: Vec<InferredFieldInfo> =
        if let crate::schema::inference::InferredType::Object { ref fields } = inferred {
            fields
                .iter()
                .map(|(name, field)| InferredFieldInfo {
                    name: name.clone(),
                    field_type: format!("{:?}", field.field_type),
                    nullable: field.field_type.is_nullable(),
                })
                .collect()
        } else {
            vec![]
        };

    // Generate schema in requested format
    let schema_value = if request.output_format == "json_schema" {
        inferred.to_json_schema()
    } else {
        // Default to Avro
        inferred.to_avro_schema(&request.name)
    };

    let response = SchemaInferResponse {
        schema: schema_value,
        format: request.output_format,
        samples_analyzed: request.samples.len(),
        fields,
    };

    (StatusCode::OK, Json(response)).into_response()
}

/// GET /api/v1/schema/search - Search schemas by content
async fn schema_search(
    State(state): State<SchemaApiState>,
    axum::extract::Query(query): axum::extract::Query<SchemaSearchQuery>,
) -> Response {
    let subjects = state.store.get_subjects();
    let mut results = Vec::new();

    let search_lower = query.q.to_lowercase();

    for subject in &subjects {
        // Check if subject name matches
        let subject_matches = subject.to_lowercase().contains(&search_lower);

        if let Ok(versions) = state.store.get_versions(subject) {
            for v in &versions {
                if let Ok(schema) = state.store.get_schema(subject, *v) {
                    // Apply type filter if provided
                    if let Some(ref type_filter) = query.schema_type {
                        let schema_type_str = format!("{:?}", schema.schema_type).to_uppercase();
                        if schema_type_str != type_filter.to_uppercase() {
                            continue;
                        }
                    }

                    // Check if schema content matches
                    let content_matches = schema.schema.to_lowercase().contains(&search_lower);

                    if subject_matches || content_matches {
                        // Extract snippet around match
                        let snippet = if content_matches {
                            let lower_schema = schema.schema.to_lowercase();
                            let pos = lower_schema.find(&search_lower).unwrap_or(0);
                            let start = pos.saturating_sub(40);
                            let end = (pos + search_lower.len() + 40).min(schema.schema.len());
                            let s = &schema.schema[start..end];
                            if start > 0 {
                                format!("...{}...", s)
                            } else {
                                format!("{}...", s)
                            }
                        } else {
                            schema.schema.chars().take(80).collect::<String>()
                        };

                        results.push(SchemaSearchResult {
                            subject: subject.clone(),
                            version: *v,
                            id: schema.id,
                            schema_type: format!("{:?}", schema.schema_type),
                            snippet,
                        });
                    }
                }
            }
        }
    }

    (StatusCode::OK, Json(results)).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{CompatibilityLevel, SchemaRegistryConfig, SubjectVersionPair};
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn create_test_state() -> SchemaApiState {
        let config = SchemaRegistryConfig {
            enabled: true,
            default_compatibility: CompatibilityLevel::Backward,
            cache_ttl_seconds: 300,
            max_cache_size: 1000,
            validate_on_produce: false,
            schema_topic: "_schemas".to_string(),
        };
        SchemaApiState {
            store: Arc::new(SchemaStore::new(config)),
        }
    }

    #[tokio::test]
    async fn test_list_subjects_empty() {
        let state = create_test_state();
        let app = create_schema_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/subjects")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_register_and_get_schema() {
        let state = create_test_state();
        let app = create_schema_api_router(state.clone());

        // Register schema
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/subjects/test-value/versions")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"schema": "{\"type\": \"string\"}", "schemaType": "AVRO"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Get schema
        let app = create_schema_api_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/subjects/test-value/versions/1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_schema_types() {
        let state = create_test_state();
        let app = create_schema_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/schemas/types")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_config() {
        let state = create_test_state();
        let app = create_schema_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/config")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_set_config() {
        let state = create_test_state();
        let app = create_schema_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/config")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"compatibility": "FULL"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_subject_not_found() {
        let state = create_test_state();
        let app = create_schema_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/subjects/nonexistent/versions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_check_compatibility() {
        let state = create_test_state();

        // Register initial schema
        state
            .store
            .register_schema(
                "test-value",
                r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}]}"#,
                SchemaType::Avro,
                vec![],
            )
            .await
            .unwrap();

        let app = create_schema_api_router(state);

        // Check compatibility with new schema
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/compatibility/subjects/test-value/versions/latest")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"schema": "{\"type\": \"record\", \"name\": \"Test\", \"fields\": [{\"name\": \"name\", \"type\": \"string\"}, {\"name\": \"age\", \"type\": \"int\", \"default\": 0}]}", "schemaType": "AVRO"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    // ========================================================================
    // Tests for Schema UI / Dashboard endpoints
    // ========================================================================

    #[tokio::test]
    async fn test_schema_dashboard_empty() {
        let state = create_test_state();
        let app = create_schema_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/schema/dashboard")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_schema_dashboard_with_schemas() {
        let state = create_test_state();

        // Register schemas
        state
            .store
            .register_schema(
                "users-value",
                r#"{"type": "string"}"#,
                SchemaType::Avro,
                vec![],
            )
            .await
            .unwrap();

        state
            .store
            .register_schema(
                "orders-value",
                r#"{"type": "string"}"#,
                SchemaType::Avro,
                vec![],
            )
            .await
            .unwrap();

        let app = create_schema_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/schema/dashboard")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_schema_subjects_browser() {
        let state = create_test_state();

        state
            .store
            .register_schema(
                "test-subject",
                r#"{"type": "string"}"#,
                SchemaType::Avro,
                vec![],
            )
            .await
            .unwrap();

        let app = create_schema_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/schema/subjects")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_schema_version_history() {
        let state = create_test_state();

        // Register multiple versions
        state
            .store
            .register_schema(
                "history-test",
                r#"{"type": "string"}"#,
                SchemaType::Avro,
                vec![],
            )
            .await
            .unwrap();

        let app = create_schema_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/schema/subjects/history-test/history")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_schema_version_history_not_found() {
        let state = create_test_state();
        let app = create_schema_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/schema/subjects/nonexistent/history")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_schema_auto_infer() {
        let state = create_test_state();
        let app = create_schema_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/schema/infer")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"samples": [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}], "output_format": "json_schema", "name": "Person"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_schema_auto_infer_empty_samples() {
        let state = create_test_state();
        let app = create_schema_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/schema/infer")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"samples": [], "output_format": "avro", "name": "Empty"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        // May succeed with empty schema or return an error - both are valid
        assert!(
            response.status() == StatusCode::OK || response.status() == StatusCode::BAD_REQUEST
        );
    }

    #[tokio::test]
    async fn test_schema_search() {
        let state = create_test_state();

        // Register schema with searchable content
        state
            .store
            .register_schema(
                "search-test-value",
                r#"{"type": "record", "name": "SearchTest", "fields": [{"name": "email", "type": "string"}]}"#,
                SchemaType::Avro,
                vec![],
            )
            .await
            .unwrap();

        let app = create_schema_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/schema/search?q=email")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_schema_search_no_results() {
        let state = create_test_state();
        let app = create_schema_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/schema/search?q=nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    // ========================================================================
    // Tests for new Confluent-compatible endpoints
    // ========================================================================

    #[tokio::test]
    async fn test_get_raw_schema_by_id() {
        let state = create_test_state();

        let id = state
            .store
            .register_schema(
                "raw-test",
                r#"{"type": "string"}"#,
                SchemaType::Avro,
                vec![],
            )
            .await
            .unwrap();

        let app = create_schema_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/schemas/ids/{}/schema", id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_raw_schema_by_id_not_found() {
        let state = create_test_state();
        let app = create_schema_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/schemas/ids/9999/schema")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_get_schema_subjects() {
        let state = create_test_state();

        let id = state
            .store
            .register_schema(
                "subjects-test-value",
                r#"{"type": "string"}"#,
                SchemaType::Avro,
                vec![],
            )
            .await
            .unwrap();

        let app = create_schema_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/schemas/ids/{}/subjects", id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let subjects: Vec<String> = serde_json::from_slice(&body).unwrap();
        assert!(subjects.contains(&"subjects-test-value".to_string()));
    }

    #[tokio::test]
    async fn test_get_schema_versions_by_id() {
        let state = create_test_state();

        let id = state
            .store
            .register_schema(
                "versions-test-value",
                r#"{"type": "string"}"#,
                SchemaType::Avro,
                vec![],
            )
            .await
            .unwrap();

        let app = create_schema_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/schemas/ids/{}/versions", id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let versions: Vec<SubjectVersionPair> = serde_json::from_slice(&body).unwrap();
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].subject, "versions-test-value");
        assert_eq!(versions[0].version, 1);
    }

    #[tokio::test]
    async fn test_get_schema_string_by_version() {
        let state = create_test_state();

        state
            .store
            .register_schema(
                "raw-version-test",
                r#"{"type": "string"}"#,
                SchemaType::Avro,
                vec![],
            )
            .await
            .unwrap();

        let app = create_schema_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/subjects/raw-version-test/versions/1/schema")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_schema_string_by_version_latest() {
        let state = create_test_state();

        state
            .store
            .register_schema(
                "raw-latest-test",
                r#"{"type": "string"}"#,
                SchemaType::Avro,
                vec![],
            )
            .await
            .unwrap();

        let app = create_schema_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/subjects/raw-latest-test/versions/latest/schema")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_referenced_by() {
        let state = create_test_state();

        state
            .store
            .register_schema(
                "ref-test",
                r#"{"type": "string"}"#,
                SchemaType::Avro,
                vec![],
            )
            .await
            .unwrap();

        let app = create_schema_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/subjects/ref-test/versions/1/referencedby")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let ids: Vec<i32> = serde_json::from_slice(&body).unwrap();
        assert!(ids.is_empty());
    }

    #[tokio::test]
    async fn test_delete_subject_config() {
        let state = create_test_state();

        // Set subject-level compatibility
        state
            .store
            .set_subject_compatibility("config-test", crate::schema::CompatibilityLevel::Full)
            .await
            .unwrap();

        let app = create_schema_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/config/config-test")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    // ── Confluent Schema Registry Compatibility Tests ────────────────────────

    #[tokio::test]
    async fn test_confluent_compat_schema_types_endpoint() {
        let state = create_test_state();
        let app = create_schema_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/schemas/types")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body: Vec<String> = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), 10_000)
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(body.contains(&"AVRO".to_string()));
        assert!(body.contains(&"JSON".to_string()));
        assert!(body.contains(&"PROTOBUF".to_string()));
    }

    #[tokio::test]
    async fn test_confluent_compat_register_and_lookup_by_id() {
        let state = create_test_state();
        let app = create_schema_api_router(state);

        // Register a schema
        let register_body = serde_json::json!({
            "schema": r#"{"type":"record","name":"Test","fields":[{"name":"f1","type":"string"}]}"#,
            "schemaType": "AVRO"
        });

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/subjects/test-value/versions")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&register_body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body: serde_json::Value = serde_json::from_slice(
            &axum::body::to_bytes(resp.into_body(), 10_000)
                .await
                .unwrap(),
        )
        .unwrap();
        let schema_id = body["id"].as_i64().unwrap();

        // Lookup by ID
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/schemas/ids/{}", schema_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // Lookup subjects for ID
        let resp = app
            .oneshot(
                Request::builder()
                    .uri(format!("/schemas/ids/{}/subjects", schema_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_confluent_compat_check_subject_exists() {
        let state = create_test_state();
        let app = create_schema_api_router(state);

        // Register
        let schema = serde_json::json!({
            "schema": r#"{"type":"record","name":"User","fields":[{"name":"name","type":"string"}]}"#,
            "schemaType": "AVRO"
        });
        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/subjects/user-value/versions")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&schema).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // POST to /subjects/{subject} should check if schema already exists
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/subjects/user-value")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&schema).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_confluent_compat_compatibility_check() {
        let state = create_test_state();
        let app = create_schema_api_router(state);

        // Register v1
        let schema_v1 = serde_json::json!({
            "schema": r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"string"}]}"#,
            "schemaType": "AVRO"
        });
        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/subjects/event-value/versions")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&schema_v1).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Check compatibility of v2 against latest
        let schema_v2 = serde_json::json!({
            "schema": r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"string"},{"name":"ts","type":["null","long"],"default":null}]}"#,
            "schemaType": "AVRO"
        });
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/compatibility/subjects/event-value/versions/latest")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&schema_v2).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_confluent_compat_global_config() {
        let state = create_test_state();
        let app = create_schema_api_router(state);

        // GET /config
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/config")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // PUT /config
        let config = serde_json::json!({"compatibility": "FULL"});
        let resp = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/config")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_string(&config).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
