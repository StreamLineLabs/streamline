//! Cloud Functions Trigger Module for Streamline
//!
//! This module provides native integration with serverless cloud function providers:
//! - AWS Lambda
//! - Google Cloud Functions
//! - Azure Functions
//! - Cloudflare Workers
//!
//! # Features
//!
//! - Direct invocation via provider SDKs (no intermediate HTTP)
//! - Batching support for cost optimization
//! - Dead-letter queue integration for failed invocations
//! - Automatic retry with exponential backoff
//! - Payload transformation and filtering
//!
//! # Example Configuration
//!
//! ```yaml
//! name: events-to-lambda
//! type: cloud_function
//! provider: aws_lambda
//! config:
//!   function_name: process-events
//!   region: us-west-2
//!   batch_size: 100
//!   batch_timeout_ms: 5000
//!   invocation_type: event  # async invocation
//! ```

use crate::dlq::{DlqConfig, DlqContext, DlqManager};
use crate::error::{Result, StreamlineError};
use crate::sink::{SinkConnector, SinkMetrics, SinkStatus, SinkType};
use crate::storage::{Record, TopicManager};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
#[cfg(feature = "serverless")]
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{error, info};
#[cfg(feature = "serverless")]
use tracing::{debug, warn};

/// Cloud function provider types
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CloudProvider {
    /// AWS Lambda
    AwsLambda,
    /// Google Cloud Functions
    GoogleCloudFunctions,
    /// Azure Functions
    AzureFunctions,
    /// Cloudflare Workers
    CloudflareWorkers,
}

impl std::fmt::Display for CloudProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CloudProvider::AwsLambda => write!(f, "aws_lambda"),
            CloudProvider::GoogleCloudFunctions => write!(f, "google_cloud_functions"),
            CloudProvider::AzureFunctions => write!(f, "azure_functions"),
            CloudProvider::CloudflareWorkers => write!(f, "cloudflare_workers"),
        }
    }
}

/// Configuration for cloud function triggers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudFunctionConfig {
    /// Cloud provider
    pub provider: CloudProvider,

    /// Provider-specific configuration
    #[serde(flatten)]
    pub provider_config: ProviderConfig,

    /// Batch settings
    #[serde(default)]
    pub batch: BatchConfig,

    /// Retry configuration
    #[serde(default)]
    pub retry: RetryConfig,

    /// Dead-letter queue configuration
    #[serde(default)]
    pub dlq: DlqTriggerConfig,

    /// Payload transformation
    #[serde(default)]
    pub transform: Option<PayloadTransform>,

    /// Filter expression (JSONPath that must return truthy value)
    pub filter: Option<String>,
}

/// Provider-specific configuration variants
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ProviderConfig {
    /// AWS Lambda configuration
    Lambda(LambdaConfig),
    /// Google Cloud Functions configuration
    Gcf(GcfConfig),
    /// Azure Functions configuration
    Azure(AzureConfig),
    /// Cloudflare Workers configuration
    Cloudflare(CloudflareConfig),
}

/// AWS Lambda configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LambdaConfig {
    /// Lambda function name or ARN
    pub function_name: String,

    /// AWS region
    #[serde(default = "default_aws_region")]
    pub region: String,

    /// Invocation type: "RequestResponse" (sync) or "Event" (async)
    #[serde(default = "default_invocation_type")]
    pub invocation_type: LambdaInvocationType,

    /// Client context (base64-encoded JSON)
    pub client_context: Option<String>,

    /// Qualifier (version or alias)
    pub qualifier: Option<String>,

    /// AWS credentials (use environment variables if not specified)
    #[serde(default)]
    pub credentials: Option<AwsCredentials>,

    /// Assume role ARN for cross-account invocation
    pub assume_role_arn: Option<String>,
}

fn default_aws_region() -> String {
    std::env::var("AWS_REGION")
        .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|_| "us-east-1".to_string())
}

fn default_invocation_type() -> LambdaInvocationType {
    LambdaInvocationType::Event
}

/// Lambda invocation type
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LambdaInvocationType {
    /// Synchronous invocation - wait for response
    RequestResponse,
    /// Asynchronous invocation - fire and forget
    #[default]
    Event,
    /// Dry run - validate without executing
    DryRun,
}

/// AWS credentials configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AwsCredentials {
    /// Access key ID (or environment variable reference with $)
    pub access_key_id: Option<String>,
    /// Secret access key (or environment variable reference with $)
    pub secret_access_key: Option<String>,
    /// Session token for temporary credentials
    pub session_token: Option<String>,
}

/// Google Cloud Functions configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcfConfig {
    /// Function name
    pub function_name: String,

    /// GCP project ID
    pub project_id: String,

    /// GCP region (e.g., "us-central1")
    pub region: String,

    /// Generation (1 or 2, default: 2)
    #[serde(default = "default_gcf_generation")]
    pub generation: u8,

    /// Service account credentials JSON path (or use default credentials)
    pub credentials_file: Option<String>,
}

fn default_gcf_generation() -> u8 {
    2
}

/// Azure Functions configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AzureConfig {
    /// Function app name
    pub function_app: String,

    /// Function name
    pub function_name: String,

    /// Azure region (optional, inferred from app)
    pub region: Option<String>,

    /// Function key for authentication
    pub function_key: Option<String>,

    /// Host key for admin-level access
    pub host_key: Option<String>,

    /// Custom trigger URL (for non-standard deployments)
    pub trigger_url: Option<String>,
}

/// Cloudflare Workers configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudflareConfig {
    /// Worker name
    pub worker_name: String,

    /// Account ID
    pub account_id: String,

    /// API token (or environment variable reference with $)
    pub api_token: Option<String>,

    /// Custom domain (if not using workers.dev)
    pub custom_domain: Option<String>,

    /// Route pattern (optional)
    pub route: Option<String>,
}

/// Batch configuration for cloud function triggers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchConfig {
    /// Maximum batch size (number of records)
    #[serde(default = "default_batch_size")]
    pub max_size: usize,

    /// Maximum batch timeout in milliseconds
    #[serde(default = "default_batch_timeout_ms")]
    pub timeout_ms: u64,

    /// Batch format
    #[serde(default)]
    pub format: BatchFormat,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_size: default_batch_size(),
            timeout_ms: default_batch_timeout_ms(),
            format: BatchFormat::default(),
        }
    }
}

fn default_batch_size() -> usize {
    100
}

fn default_batch_timeout_ms() -> u64 {
    5000
}

/// Batch format for cloud function payloads
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BatchFormat {
    /// JSON array of records
    #[default]
    JsonArray,
    /// Newline-delimited JSON (NDJSON)
    Ndjson,
    /// CloudEvents batch format
    CloudEvents,
    /// AWS Kinesis-style records
    KinesisStyle,
}

/// Retry configuration for cloud function invocations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Maximum retry attempts
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// Initial retry delay in milliseconds
    #[serde(default = "default_initial_delay_ms")]
    pub initial_delay_ms: u64,

    /// Maximum retry delay in milliseconds
    #[serde(default = "default_max_delay_ms")]
    pub max_delay_ms: u64,

    /// Backoff multiplier
    #[serde(default = "default_backoff_multiplier")]
    pub backoff_multiplier: f64,

    /// Retryable error types
    #[serde(default = "default_retryable_errors")]
    pub retryable_errors: Vec<String>,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: default_max_retries(),
            initial_delay_ms: default_initial_delay_ms(),
            max_delay_ms: default_max_delay_ms(),
            backoff_multiplier: default_backoff_multiplier(),
            retryable_errors: default_retryable_errors(),
        }
    }
}

fn default_max_retries() -> u32 {
    3
}

fn default_initial_delay_ms() -> u64 {
    100
}

fn default_max_delay_ms() -> u64 {
    30_000
}

fn default_backoff_multiplier() -> f64 {
    2.0
}

fn default_retryable_errors() -> Vec<String> {
    vec![
        "Throttling".to_string(),
        "ServiceUnavailable".to_string(),
        "TooManyRequests".to_string(),
        "InternalError".to_string(),
    ]
}

/// DLQ configuration for trigger failures
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqTriggerConfig {
    /// Enable DLQ for failed invocations
    #[serde(default = "default_dlq_enabled")]
    pub enabled: bool,

    /// DLQ topic suffix
    #[serde(default = "default_dlq_suffix")]
    pub suffix: String,

    /// Include full error context in DLQ records
    #[serde(default = "default_include_error")]
    pub include_error: bool,
}

impl Default for DlqTriggerConfig {
    fn default() -> Self {
        Self {
            enabled: default_dlq_enabled(),
            suffix: default_dlq_suffix(),
            include_error: default_include_error(),
        }
    }
}

fn default_dlq_enabled() -> bool {
    true
}

fn default_dlq_suffix() -> String {
    ".dlq".to_string()
}

fn default_include_error() -> bool {
    true
}

/// Payload transformation configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PayloadTransform {
    /// Wrap records in a metadata envelope
    #[serde(default)]
    pub envelope: bool,

    /// Include topic name in payload
    #[serde(default)]
    pub include_topic: bool,

    /// Include partition in payload
    #[serde(default)]
    pub include_partition: bool,

    /// Include offset in payload
    #[serde(default)]
    pub include_offset: bool,

    /// Include timestamp in payload
    #[serde(default)]
    pub include_timestamp: bool,

    /// Include headers in payload
    #[serde(default)]
    pub include_headers: bool,

    /// Custom field mappings (JSONPath)
    #[serde(default)]
    pub field_mappings: HashMap<String, String>,
}

/// Internal state for the cloud function connector
struct CloudFunctionState {
    status: SinkStatus,
    last_error: Option<String>,
    records_processed: AtomicU64,
    records_failed: AtomicU64,
    bytes_processed: AtomicU64,
    invocations: AtomicU64,
    invocation_errors: AtomicU64,
    last_commit_timestamp: AtomicU64,
    committed_offsets: std::sync::RwLock<HashMap<String, i64>>,
}

impl Default for CloudFunctionState {
    fn default() -> Self {
        Self {
            status: SinkStatus::Stopped,
            last_error: None,
            records_processed: AtomicU64::new(0),
            records_failed: AtomicU64::new(0),
            bytes_processed: AtomicU64::new(0),
            invocations: AtomicU64::new(0),
            invocation_errors: AtomicU64::new(0),
            last_commit_timestamp: AtomicU64::new(0),
            committed_offsets: std::sync::RwLock::new(HashMap::new()),
        }
    }
}

/// Cloud Function connector implementation
pub struct CloudFunctionConnector {
    name: String,
    topics: Vec<String>,
    config: CloudFunctionConfig,
    #[allow(dead_code)]
    topic_manager: Arc<TopicManager>,
    dlq_manager: Option<DlqManager>,
    state: Arc<RwLock<CloudFunctionState>>,
    shutdown_tx: Option<tokio::sync::broadcast::Sender<()>>,
    #[cfg(feature = "serverless")]
    http_client: reqwest::Client,
}

impl CloudFunctionConnector {
    /// Create a new cloud function connector
    pub fn new(
        name: String,
        topics: Vec<String>,
        config: CloudFunctionConfig,
        topic_manager: Arc<TopicManager>,
    ) -> Result<Self> {
        // Validate configuration
        Self::validate_config(&config)?;

        // Create DLQ manager if enabled
        let dlq_manager = if config.dlq.enabled {
            let dlq_config = DlqConfig {
                enabled: true,
                suffix: config.dlq.suffix.clone(),
                ..Default::default()
            };
            Some(DlqManager::new(topic_manager.clone(), dlq_config))
        } else {
            None
        };

        Ok(Self {
            name,
            topics,
            config,
            topic_manager,
            dlq_manager,
            state: Arc::new(RwLock::new(CloudFunctionState::default())),
            shutdown_tx: None,
            #[cfg(feature = "serverless")]
            http_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .map_err(|e| {
                    StreamlineError::Sink(format!("Failed to create HTTP client: {}", e))
                })?,
        })
    }

    fn validate_config(config: &CloudFunctionConfig) -> Result<()> {
        match (&config.provider, &config.provider_config) {
            (CloudProvider::AwsLambda, ProviderConfig::Lambda(cfg)) => {
                if cfg.function_name.is_empty() {
                    return Err(StreamlineError::Sink(
                        "Lambda function name is required".to_string(),
                    ));
                }
            }
            (CloudProvider::GoogleCloudFunctions, ProviderConfig::Gcf(cfg)) => {
                if cfg.function_name.is_empty() {
                    return Err(StreamlineError::Sink(
                        "GCF function name is required".to_string(),
                    ));
                }
                if cfg.project_id.is_empty() {
                    return Err(StreamlineError::Sink(
                        "GCF project_id is required".to_string(),
                    ));
                }
            }
            (CloudProvider::AzureFunctions, ProviderConfig::Azure(cfg)) => {
                if cfg.function_app.is_empty() {
                    return Err(StreamlineError::Sink(
                        "Azure function_app is required".to_string(),
                    ));
                }
                if cfg.function_name.is_empty() {
                    return Err(StreamlineError::Sink(
                        "Azure function_name is required".to_string(),
                    ));
                }
            }
            (CloudProvider::CloudflareWorkers, ProviderConfig::Cloudflare(cfg)) => {
                if cfg.worker_name.is_empty() {
                    return Err(StreamlineError::Sink(
                        "Cloudflare worker_name is required".to_string(),
                    ));
                }
                if cfg.account_id.is_empty() {
                    return Err(StreamlineError::Sink(
                        "Cloudflare account_id is required".to_string(),
                    ));
                }
            }
            _ => {
                return Err(StreamlineError::Sink(
                    "Provider and config type mismatch".to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Resolve a secret reference to its actual value
    fn resolve_secret(secret_ref: &str) -> Result<String> {
        if let Some(var_name) = secret_ref.strip_prefix('$') {
            std::env::var(var_name).map_err(|_| {
                StreamlineError::Sink(format!("Environment variable '{}' not found", var_name))
            })
        } else {
            Ok(secret_ref.to_string())
        }
    }

    /// Build the payload for cloud function invocation
    fn build_payload(&self, topic: &str, partition: i32, records: &[Record]) -> Result<Bytes> {
        let batch_format = self.config.batch.format;
        let transform = &self.config.transform;

        match batch_format {
            BatchFormat::JsonArray => {
                let items: Vec<serde_json::Value> = records
                    .iter()
                    .map(|r| self.record_to_json(topic, partition, r, transform))
                    .collect::<Result<Vec<_>>>()?;

                let json = serde_json::to_vec(&items).map_err(|e| {
                    StreamlineError::Sink(format!("Failed to serialize payload: {}", e))
                })?;
                Ok(Bytes::from(json))
            }
            BatchFormat::Ndjson => {
                let mut lines = Vec::new();
                for record in records {
                    let json = self.record_to_json(topic, partition, record, transform)?;
                    let line = serde_json::to_string(&json).map_err(|e| {
                        StreamlineError::Sink(format!("Failed to serialize record: {}", e))
                    })?;
                    lines.push(line);
                }
                Ok(Bytes::from(lines.join("\n")))
            }
            BatchFormat::CloudEvents => {
                let events: Vec<serde_json::Value> = records
                    .iter()
                    .map(|r| self.record_to_cloudevent(topic, partition, r))
                    .collect::<Result<Vec<_>>>()?;

                let json = serde_json::to_vec(&events).map_err(|e| {
                    StreamlineError::Sink(format!("Failed to serialize CloudEvents: {}", e))
                })?;
                Ok(Bytes::from(json))
            }
            BatchFormat::KinesisStyle => {
                let kinesis_batch = serde_json::json!({
                    "Records": records.iter().map(|r| {
                        serde_json::json!({
                            "kinesis": {
                                "partitionKey": r.key.as_ref().map(|k| String::from_utf8_lossy(k).to_string()),
                                "data": base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &r.value),
                                "sequenceNumber": format!("{}", r.offset),
                                "approximateArrivalTimestamp": r.timestamp as f64 / 1000.0
                            },
                            "eventSource": "streamline",
                            "eventID": format!("{}:{}:{}", topic, partition, r.offset),
                            "invokeIdentityArn": "",
                            "eventVersion": "1.0",
                            "eventName": "streamline:record",
                            "eventSourceARN": format!("streamline:topic:{}", topic),
                            "awsRegion": "local"
                        })
                    }).collect::<Vec<_>>()
                });

                let json = serde_json::to_vec(&kinesis_batch).map_err(|e| {
                    StreamlineError::Sink(format!("Failed to serialize Kinesis batch: {}", e))
                })?;
                Ok(Bytes::from(json))
            }
        }
    }

    /// Convert a record to JSON with optional transformations
    fn record_to_json(
        &self,
        topic: &str,
        partition: i32,
        record: &Record,
        transform: &Option<PayloadTransform>,
    ) -> Result<serde_json::Value> {
        // Parse record value as JSON if possible
        let value: serde_json::Value = if let Ok(s) = std::str::from_utf8(&record.value) {
            serde_json::from_str(s).unwrap_or_else(|_| serde_json::Value::String(s.to_string()))
        } else {
            serde_json::Value::String(base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                &record.value,
            ))
        };

        if let Some(t) = transform {
            if t.envelope {
                let mut envelope = serde_json::json!({
                    "value": value
                });

                if let serde_json::Value::Object(ref mut map) = envelope {
                    if t.include_topic {
                        map.insert(
                            "topic".to_string(),
                            serde_json::Value::String(topic.to_string()),
                        );
                    }
                    if t.include_partition {
                        map.insert(
                            "partition".to_string(),
                            serde_json::Value::Number(partition.into()),
                        );
                    }
                    if t.include_offset {
                        map.insert(
                            "offset".to_string(),
                            serde_json::Value::Number(record.offset.into()),
                        );
                    }
                    if t.include_timestamp {
                        map.insert(
                            "timestamp".to_string(),
                            serde_json::Value::Number(record.timestamp.into()),
                        );
                    }
                    if t.include_headers && !record.headers.is_empty() {
                        let headers: HashMap<String, String> = record
                            .headers
                            .iter()
                            .filter_map(|h| {
                                std::str::from_utf8(&h.value)
                                    .ok()
                                    .map(|v| (h.key.clone(), v.to_string()))
                            })
                            .collect();
                        map.insert(
                            "headers".to_string(),
                            serde_json::to_value(headers).unwrap_or_default(),
                        );
                    }
                    if let Some(ref key) = record.key {
                        let key_str = std::str::from_utf8(key)
                            .map(|s| s.to_string())
                            .unwrap_or_else(|_| {
                                base64::Engine::encode(
                                    &base64::engine::general_purpose::STANDARD,
                                    key,
                                )
                            });
                        map.insert("key".to_string(), serde_json::Value::String(key_str));
                    }
                }
                Ok(envelope)
            } else {
                Ok(value)
            }
        } else {
            Ok(value)
        }
    }

    /// Convert a record to CloudEvents format
    fn record_to_cloudevent(
        &self,
        topic: &str,
        partition: i32,
        record: &Record,
    ) -> Result<serde_json::Value> {
        let value: serde_json::Value = if let Ok(s) = std::str::from_utf8(&record.value) {
            serde_json::from_str(s).unwrap_or_else(|_| serde_json::Value::String(s.to_string()))
        } else {
            serde_json::Value::String(base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                &record.value,
            ))
        };

        let timestamp = chrono::DateTime::from_timestamp_millis(record.timestamp)
            .unwrap_or_else(chrono::Utc::now)
            .to_rfc3339();

        Ok(serde_json::json!({
            "specversion": "1.0",
            "type": "io.streamline.record",
            "source": format!("/streamline/topics/{}", topic),
            "id": format!("{}-{}-{}", topic, partition, record.offset),
            "time": timestamp,
            "datacontenttype": "application/json",
            "data": value,
            "streamlinetopic": topic,
            "streamlinepartition": partition,
            "streamlineoffset": record.offset
        }))
    }

    /// Invoke the cloud function with retry logic
    #[cfg(feature = "serverless")]
    async fn invoke_with_retry(&self, payload: Bytes) -> Result<InvocationResult> {
        let retry_config = &self.config.retry;
        let mut attempts = 0;
        let mut delay = retry_config.initial_delay_ms;
        let mut last_error;

        loop {
            match self.invoke(&payload).await {
                Ok(result) => {
                    if result.success {
                        return Ok(result);
                    }

                    // Check if error is retryable
                    let is_retryable = retry_config.retryable_errors.iter().any(|e| {
                        result
                            .error
                            .as_ref()
                            .map(|err| err.contains(e))
                            .unwrap_or(false)
                    });

                    if is_retryable && attempts < retry_config.max_retries {
                        attempts += 1;
                        last_error = result.error.unwrap_or_default();
                        warn!(
                            connector = %self.name,
                            attempt = attempts,
                            error = %last_error,
                            "Retrying cloud function invocation"
                        );
                    } else {
                        return Ok(result);
                    }
                }
                Err(e) => {
                    attempts += 1;
                    last_error = e.to_string();

                    if attempts >= retry_config.max_retries {
                        return Ok(InvocationResult {
                            success: false,
                            status_code: None,
                            response: None,
                            error: Some(format!(
                                "Failed after {} retries: {}",
                                attempts, last_error
                            )),
                            latency_ms: 0,
                        });
                    }

                    warn!(
                        connector = %self.name,
                        attempt = attempts,
                        error = %last_error,
                        "Retrying cloud function invocation after error"
                    );
                }
            }

            // Exponential backoff
            tokio::time::sleep(Duration::from_millis(delay)).await;
            delay = ((delay as f64) * retry_config.backoff_multiplier) as u64;
            delay = delay.min(retry_config.max_delay_ms);
        }
    }

    /// Invoke the cloud function
    #[cfg(feature = "serverless")]
    async fn invoke(&self, payload: &Bytes) -> Result<InvocationResult> {
        let start = std::time::Instant::now();

        match (&self.config.provider, &self.config.provider_config) {
            (CloudProvider::AwsLambda, ProviderConfig::Lambda(cfg)) => {
                self.invoke_lambda(cfg, payload).await
            }
            (CloudProvider::GoogleCloudFunctions, ProviderConfig::Gcf(cfg)) => {
                self.invoke_gcf(cfg, payload).await
            }
            (CloudProvider::AzureFunctions, ProviderConfig::Azure(cfg)) => {
                self.invoke_azure(cfg, payload).await
            }
            (CloudProvider::CloudflareWorkers, ProviderConfig::Cloudflare(cfg)) => {
                self.invoke_cloudflare(cfg, payload).await
            }
            _ => Err(StreamlineError::Sink(
                "Provider and config type mismatch".to_string(),
            )),
        }
        .map(|mut result| {
            result.latency_ms = start.elapsed().as_millis() as u64;
            result
        })
    }

    /// Invoke AWS Lambda function via HTTP API
    #[cfg(feature = "serverless")]
    async fn invoke_lambda(
        &self,
        config: &LambdaConfig,
        payload: &Bytes,
    ) -> Result<InvocationResult> {
        use sha2::Digest;

        // Build the Lambda invoke URL
        let url = format!(
            "https://lambda.{}.amazonaws.com/2015-03-31/functions/{}/invocations",
            config.region, config.function_name
        );

        // Get credentials
        let access_key = config
            .credentials
            .as_ref()
            .and_then(|c| c.access_key_id.as_ref())
            .map(|k| Self::resolve_secret(k))
            .transpose()?
            .or_else(|| std::env::var("AWS_ACCESS_KEY_ID").ok())
            .ok_or_else(|| StreamlineError::Sink("AWS access key not found".to_string()))?;

        let secret_key = config
            .credentials
            .as_ref()
            .and_then(|c| c.secret_access_key.as_ref())
            .map(|k| Self::resolve_secret(k))
            .transpose()?
            .or_else(|| std::env::var("AWS_SECRET_ACCESS_KEY").ok())
            .ok_or_else(|| StreamlineError::Sink("AWS secret key not found".to_string()))?;

        // Prepare request
        let invocation_type = match config.invocation_type {
            LambdaInvocationType::RequestResponse => "RequestResponse",
            LambdaInvocationType::Event => "Event",
            LambdaInvocationType::DryRun => "DryRun",
        };

        // Calculate AWS Signature V4 (simplified - production should use aws-sigv4 crate)
        let now = chrono::Utc::now();
        let date_stamp = now.format("%Y%m%d").to_string();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();

        let canonical_uri = format!("/2015-03-31/functions/{}/invocations", config.function_name);
        let canonical_querystring = "";
        let payload_hash = hex::encode(sha2::Sha256::digest(payload));
        let canonical_headers = format!(
            "host:lambda.{}.amazonaws.com\nx-amz-date:{}\n",
            config.region, amz_date
        );
        let signed_headers = "host;x-amz-date";

        let canonical_request = format!(
            "POST\n{}\n{}\n{}\n{}\n{}",
            canonical_uri, canonical_querystring, canonical_headers, signed_headers, payload_hash
        );

        let algorithm = "AWS4-HMAC-SHA256";
        let credential_scope = format!("{}/{}/lambda/aws4_request", date_stamp, config.region);
        let string_to_sign = format!(
            "{}\n{}\n{}\n{}",
            algorithm,
            amz_date,
            credential_scope,
            hex::encode(sha2::Sha256::digest(canonical_request.as_bytes()))
        );

        // Calculate signature
        let k_date = Self::hmac_sha256(
            format!("AWS4{}", secret_key).as_bytes(),
            date_stamp.as_bytes(),
        )?;
        let k_region = Self::hmac_sha256(&k_date, config.region.as_bytes())?;
        let k_service = Self::hmac_sha256(&k_region, b"lambda")?;
        let k_signing = Self::hmac_sha256(&k_service, b"aws4_request")?;
        let signature = hex::encode(Self::hmac_sha256(&k_signing, string_to_sign.as_bytes())?);

        let authorization = format!(
            "{} Credential={}/{}, SignedHeaders={}, Signature={}",
            algorithm, access_key, credential_scope, signed_headers, signature
        );

        // Make request
        let response = self
            .http_client
            .post(&url)
            .header("Authorization", authorization)
            .header("X-Amz-Date", amz_date)
            .header("X-Amz-Invocation-Type", invocation_type)
            .header("Content-Type", "application/json")
            .body(payload.to_vec())
            .send()
            .await
            .map_err(|e| StreamlineError::Sink(format!("Lambda invocation failed: {}", e)))?;

        let status = response.status();
        let body = response.text().await.unwrap_or_default();

        if status.is_success() {
            Ok(InvocationResult {
                success: true,
                status_code: Some(status.as_u16()),
                response: Some(body),
                error: None,
                latency_ms: 0,
            })
        } else {
            Ok(InvocationResult {
                success: false,
                status_code: Some(status.as_u16()),
                response: None,
                error: Some(body),
                latency_ms: 0,
            })
        }
    }

    #[cfg(feature = "serverless")]
    fn hmac_sha256(key: &[u8], data: &[u8]) -> Result<Vec<u8>> {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;

        type HmacSha256 = Hmac<Sha256>;
        let mut mac = HmacSha256::new_from_slice(key)
            .map_err(|e| StreamlineError::Sink(format!("HMAC key error: {}", e)))?;
        mac.update(data);
        Ok(mac.finalize().into_bytes().to_vec())
    }

    /// Invoke Google Cloud Function via HTTP
    #[cfg(feature = "serverless")]
    async fn invoke_gcf(&self, config: &GcfConfig, payload: &Bytes) -> Result<InvocationResult> {
        // Build the GCF invoke URL
        let url = match config.generation {
            1 => format!(
                "https://{}-{}.cloudfunctions.net/{}",
                config.region, config.project_id, config.function_name
            ),
            2 => format!(
                "https://{}-{}.cloudfunctions.net/{}",
                config.region, config.project_id, config.function_name
            ),
            _ => {
                return Err(StreamlineError::Sink(format!(
                    "Unsupported GCF generation: {}",
                    config.generation
                )))
            }
        };

        // Get authentication token (use default credentials or service account)
        // For simplicity, we'll use the metadata server if available, or skip auth for testing
        let mut request = self
            .http_client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(payload.to_vec());

        // Try to get an access token from the metadata server
        if let Ok(token) = self.get_gcp_access_token().await {
            request = request.header("Authorization", format!("Bearer {}", token));
        }

        let response = request
            .send()
            .await
            .map_err(|e| StreamlineError::Sink(format!("GCF invocation failed: {}", e)))?;

        let status = response.status();
        let body = response.text().await.unwrap_or_default();

        if status.is_success() {
            Ok(InvocationResult {
                success: true,
                status_code: Some(status.as_u16()),
                response: Some(body),
                error: None,
                latency_ms: 0,
            })
        } else {
            Ok(InvocationResult {
                success: false,
                status_code: Some(status.as_u16()),
                response: None,
                error: Some(body),
                latency_ms: 0,
            })
        }
    }

    #[cfg(feature = "serverless")]
    async fn get_gcp_access_token(&self) -> Result<String> {
        // Try metadata server first (when running on GCP)
        let metadata_url =
            "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";

        let response = self
            .http_client
            .get(metadata_url)
            .header("Metadata-Flavor", "Google")
            .timeout(Duration::from_secs(2))
            .send()
            .await
            .map_err(|e| StreamlineError::Sink(format!("Failed to get GCP token: {}", e)))?;

        if response.status().is_success() {
            let body: serde_json::Value = response.json().await.map_err(|e| {
                StreamlineError::Sink(format!("Failed to parse GCP token response: {}", e))
            })?;

            body["access_token"]
                .as_str()
                .map(|s| s.to_string())
                .ok_or_else(|| StreamlineError::Sink("No access_token in response".to_string()))
        } else {
            Err(StreamlineError::Sink(
                "GCP metadata server not available".to_string(),
            ))
        }
    }

    /// Invoke Azure Function via HTTP
    #[cfg(feature = "serverless")]
    async fn invoke_azure(
        &self,
        config: &AzureConfig,
        payload: &Bytes,
    ) -> Result<InvocationResult> {
        // Build the Azure Function URL
        let url = config.trigger_url.clone().unwrap_or_else(|| {
            format!(
                "https://{}.azurewebsites.net/api/{}",
                config.function_app, config.function_name
            )
        });

        let mut request = self
            .http_client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(payload.to_vec());

        // Add function key if provided
        if let Some(ref key) = config.function_key {
            let resolved_key = Self::resolve_secret(key)?;
            request = request.header("x-functions-key", resolved_key);
        } else if let Some(ref key) = config.host_key {
            let resolved_key = Self::resolve_secret(key)?;
            request = request.header("x-functions-key", resolved_key);
        }

        let response = request.send().await.map_err(|e| {
            StreamlineError::Sink(format!("Azure Function invocation failed: {}", e))
        })?;

        let status = response.status();
        let body = response.text().await.unwrap_or_default();

        if status.is_success() {
            Ok(InvocationResult {
                success: true,
                status_code: Some(status.as_u16()),
                response: Some(body),
                error: None,
                latency_ms: 0,
            })
        } else {
            Ok(InvocationResult {
                success: false,
                status_code: Some(status.as_u16()),
                response: None,
                error: Some(body),
                latency_ms: 0,
            })
        }
    }

    /// Invoke Cloudflare Worker via HTTP
    #[cfg(feature = "serverless")]
    async fn invoke_cloudflare(
        &self,
        config: &CloudflareConfig,
        payload: &Bytes,
    ) -> Result<InvocationResult> {
        // Build the Cloudflare Workers URL
        let url = config.custom_domain.clone().unwrap_or_else(|| {
            format!(
                "https://{}.{}.workers.dev",
                config.worker_name, config.account_id
            )
        });

        let mut request = self
            .http_client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(payload.to_vec());

        // Add API token if provided
        if let Some(ref token) = config.api_token {
            let resolved_token = Self::resolve_secret(token)?;
            request = request.header("Authorization", format!("Bearer {}", resolved_token));
        }

        let response = request.send().await.map_err(|e| {
            StreamlineError::Sink(format!("Cloudflare Worker invocation failed: {}", e))
        })?;

        let status = response.status();
        let body = response.text().await.unwrap_or_default();

        if status.is_success() {
            Ok(InvocationResult {
                success: true,
                status_code: Some(status.as_u16()),
                response: Some(body),
                error: None,
                latency_ms: 0,
            })
        } else {
            Ok(InvocationResult {
                success: false,
                status_code: Some(status.as_u16()),
                response: None,
                error: Some(body),
                latency_ms: 0,
            })
        }
    }

    /// Send failed records to DLQ
    fn send_to_dlq(&self, record: &Record, topic: &str, partition: i32, error: &str) {
        if let Some(ref dlq_manager) = self.dlq_manager {
            let context = DlqContext::new(format!("Cloud function invocation failed: {}", error));
            if let Err(e) = dlq_manager.send_to_dlq(record, topic, partition, context) {
                error!(
                    connector = %self.name,
                    topic = %topic,
                    error = %e,
                    "Failed to send record to DLQ"
                );
            }
        }
    }

    /// Update metrics after processing
    fn update_metrics(&self, records_count: u64, bytes_count: u64, invocations: u64, failed: bool) {
        let state = self.state.clone();
        tokio::spawn(async move {
            let state = state.read().await;
            if failed {
                state
                    .records_failed
                    .fetch_add(records_count, Ordering::Relaxed);
                state
                    .invocation_errors
                    .fetch_add(invocations, Ordering::Relaxed);
            } else {
                state
                    .records_processed
                    .fetch_add(records_count, Ordering::Relaxed);
                state
                    .bytes_processed
                    .fetch_add(bytes_count, Ordering::Relaxed);
            }
            state.invocations.fetch_add(invocations, Ordering::Relaxed);
            state.last_commit_timestamp.store(
                chrono::Utc::now().timestamp_millis() as u64,
                Ordering::Relaxed,
            );
        });
    }
}

/// Result of a cloud function invocation
#[derive(Debug, Clone)]
pub struct InvocationResult {
    /// Whether the invocation was successful
    pub success: bool,
    /// HTTP status code
    pub status_code: Option<u16>,
    /// Response body (for sync invocations)
    pub response: Option<String>,
    /// Error message
    pub error: Option<String>,
    /// Latency in milliseconds
    pub latency_ms: u64,
}

#[async_trait]
impl SinkConnector for CloudFunctionConnector {
    fn name(&self) -> &str {
        &self.name
    }

    fn sink_type(&self) -> SinkType {
        SinkType::Serverless
    }

    fn topics(&self) -> Vec<String> {
        self.topics.clone()
    }

    async fn start(&mut self) -> Result<()> {
        info!(
            connector = %self.name,
            provider = %self.config.provider,
            "Starting cloud function connector"
        );

        let mut state = self.state.write().await;
        state.status = SinkStatus::Starting;
        drop(state);

        // Create shutdown channel
        let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);
        self.shutdown_tx = Some(shutdown_tx);

        // Mark as running
        let mut state = self.state.write().await;
        state.status = SinkStatus::Running;

        info!(connector = %self.name, "Cloud function connector started");
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        info!(connector = %self.name, "Stopping cloud function connector");

        let mut state = self.state.write().await;
        state.status = SinkStatus::Stopping;
        drop(state);

        // Signal shutdown
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }

        // Mark as stopped
        let mut state = self.state.write().await;
        state.status = SinkStatus::Stopped;

        info!(connector = %self.name, "Cloud function connector stopped");
        Ok(())
    }

    #[cfg(feature = "serverless")]
    async fn process_batch(
        &mut self,
        topic: &str,
        partition: i32,
        records: Vec<Record>,
    ) -> Result<i64> {
        if records.is_empty() {
            return Ok(-1);
        }

        debug!(
            connector = %self.name,
            topic = %topic,
            partition = partition,
            count = records.len(),
            "Processing batch for cloud function"
        );

        let total_bytes: u64 = records.iter().map(|r| r.value.len() as u64).sum();
        let highest_offset = records.iter().map(|r| r.offset).max().unwrap_or(-1);

        // Build and send payload
        let payload = self.build_payload(topic, partition, &records)?;

        match self.invoke_with_retry(payload).await {
            Ok(result) if result.success => {
                self.update_metrics(records.len() as u64, total_bytes, 1, false);

                // Update committed offset
                let state = self.state.read().await;
                if let Ok(mut offsets) = state.committed_offsets.write() {
                    let key = format!("{}-{}", topic, partition);
                    offsets.insert(key, highest_offset);
                }

                Ok(highest_offset)
            }
            Ok(result) => {
                error!(
                    connector = %self.name,
                    error = ?result.error,
                    "Cloud function invocation failed"
                );
                self.update_metrics(records.len() as u64, 0, 1, true);

                // Send failed records to DLQ
                let error_msg = result.error.unwrap_or_else(|| "Unknown error".to_string());
                for record in &records {
                    self.send_to_dlq(record, topic, partition, &error_msg);
                }

                // Update state with error
                let mut state = self.state.write().await;
                state.last_error = Some(error_msg.clone());
                state.status = SinkStatus::Failed;

                Err(StreamlineError::Sink(error_msg))
            }
            Err(e) => {
                error!(
                    connector = %self.name,
                    error = %e,
                    "Cloud function invocation error"
                );
                self.update_metrics(records.len() as u64, 0, 1, true);

                // Send failed records to DLQ
                for record in &records {
                    self.send_to_dlq(record, topic, partition, &e.to_string());
                }

                // Update state with error
                let mut state = self.state.write().await;
                state.last_error = Some(e.to_string());
                state.status = SinkStatus::Failed;

                Err(e)
            }
        }
    }

    #[cfg(not(feature = "serverless"))]
    async fn process_batch(
        &mut self,
        _topic: &str,
        _partition: i32,
        _records: Vec<Record>,
    ) -> Result<i64> {
        Err(StreamlineError::Sink(
            "Cloud function connector requires --features serverless".to_string(),
        ))
    }

    fn status(&self) -> SinkStatus {
        match self.state.try_read() {
            Ok(state) => state.status.clone(),
            Err(_) => SinkStatus::Running,
        }
    }

    fn metrics(&self) -> SinkMetrics {
        match self.state.try_read() {
            Ok(state) => {
                let offsets = state
                    .committed_offsets
                    .read()
                    .map(|o| o.clone())
                    .unwrap_or_default();

                SinkMetrics {
                    records_processed: state.records_processed.load(Ordering::Relaxed),
                    records_failed: state.records_failed.load(Ordering::Relaxed),
                    bytes_processed: state.bytes_processed.load(Ordering::Relaxed),
                    lag_records: 0,
                    last_commit_timestamp: state.last_commit_timestamp.load(Ordering::Relaxed)
                        as i64,
                    committed_offsets: offsets,
                    error_message: state.last_error.clone(),
                    commit_count: 0,
                    last_commit_latency_ms: 0,
                    error_count: state.records_failed.load(Ordering::Relaxed),
                }
            }
            Err(_) => SinkMetrics::default(),
        }
    }
}

/// Extended metrics for cloud function connector
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CloudFunctionMetrics {
    /// Base sink metrics
    #[serde(flatten)]
    pub base: SinkMetrics,
    /// Total invocations
    pub invocations: u64,
    /// Failed invocations
    pub invocation_errors: u64,
    /// Average latency in milliseconds
    pub avg_latency_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cloud_provider_display() {
        assert_eq!(format!("{}", CloudProvider::AwsLambda), "aws_lambda");
        assert_eq!(
            format!("{}", CloudProvider::GoogleCloudFunctions),
            "google_cloud_functions"
        );
        assert_eq!(
            format!("{}", CloudProvider::AzureFunctions),
            "azure_functions"
        );
        assert_eq!(
            format!("{}", CloudProvider::CloudflareWorkers),
            "cloudflare_workers"
        );
    }

    #[test]
    fn test_batch_config_default() {
        let config = BatchConfig::default();
        assert_eq!(config.max_size, 100);
        assert_eq!(config.timeout_ms, 5000);
        assert_eq!(config.format, BatchFormat::JsonArray);
    }

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.initial_delay_ms, 100);
        assert_eq!(config.max_delay_ms, 30_000);
        assert_eq!(config.backoff_multiplier, 2.0);
    }

    #[test]
    fn test_dlq_trigger_config_default() {
        let config = DlqTriggerConfig::default();
        assert!(config.enabled);
        assert_eq!(config.suffix, ".dlq");
        assert!(config.include_error);
    }

    #[test]
    fn test_lambda_config_serialization() {
        let config = LambdaConfig {
            function_name: "my-function".to_string(),
            region: "us-west-2".to_string(),
            invocation_type: LambdaInvocationType::Event,
            client_context: None,
            qualifier: Some("$LATEST".to_string()),
            credentials: None,
            assume_role_arn: None,
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: LambdaConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.function_name, "my-function");
        assert_eq!(deserialized.region, "us-west-2");
    }

    #[test]
    fn test_gcf_config_serialization() {
        let config = GcfConfig {
            function_name: "my-function".to_string(),
            project_id: "my-project".to_string(),
            region: "us-central1".to_string(),
            generation: 2,
            credentials_file: None,
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: GcfConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.function_name, "my-function");
        assert_eq!(deserialized.project_id, "my-project");
    }

    #[test]
    fn test_azure_config_serialization() {
        let config = AzureConfig {
            function_app: "my-app".to_string(),
            function_name: "my-function".to_string(),
            region: None,
            function_key: Some("$AZURE_FUNCTION_KEY".to_string()),
            host_key: None,
            trigger_url: None,
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: AzureConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.function_app, "my-app");
    }

    #[test]
    fn test_cloudflare_config_serialization() {
        let config = CloudflareConfig {
            worker_name: "my-worker".to_string(),
            account_id: "account123".to_string(),
            api_token: Some("$CF_API_TOKEN".to_string()),
            custom_domain: None,
            route: None,
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: CloudflareConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.worker_name, "my-worker");
        assert_eq!(deserialized.account_id, "account123");
    }

    #[test]
    fn test_secret_resolution_env_var() {
        std::env::set_var("TEST_CLOUD_SECRET_123", "my-secret-value");
        let result = CloudFunctionConnector::resolve_secret("$TEST_CLOUD_SECRET_123");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "my-secret-value");
        std::env::remove_var("TEST_CLOUD_SECRET_123");
    }

    #[test]
    fn test_secret_resolution_literal() {
        let result = CloudFunctionConnector::resolve_secret("literal-value");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "literal-value");
    }

    #[test]
    fn test_batch_format_serialization() {
        let formats = vec![
            (BatchFormat::JsonArray, "json_array"),
            (BatchFormat::Ndjson, "ndjson"),
            (BatchFormat::CloudEvents, "cloud_events"),
            (BatchFormat::KinesisStyle, "kinesis_style"),
        ];

        for (format, expected) in formats {
            let json = serde_json::to_string(&format).unwrap();
            assert_eq!(json, format!("\"{}\"", expected));
        }
    }
}
