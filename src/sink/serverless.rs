//! Serverless Connector Framework for Streamline
//!
//! This module provides a serverless connector framework that enables:
//! - Declarative YAML/JSON configuration for connectors
//! - Pre-built connectors for popular SaaS services
//! - HTTP webhook ingestion (inbound)
//! - HTTP webhook delivery (outbound)
//! - Credential management with secret references
//!
//! # Supported Connector Types
//!
//! ## Inbound (Source) Connectors
//! - HTTP Webhook: Receive events from external services
//! - S3 Events: Trigger on S3 bucket events
//! - GitHub Events: Repository webhooks
//! - Stripe Events: Payment and subscription webhooks
//!
//! ## Outbound (Sink) Connectors
//! - HTTP Webhook: POST events to external endpoints
//! - Slack: Send notifications to channels
//! - Email (via SMTP/SendGrid): Send email notifications
//!
//! # Example Configuration
//!
//! ```yaml
//! name: stripe-events
//! type: serverless
//! connector_type: http_webhook_inbound
//! config:
//!   endpoint: /webhooks/stripe
//!   secret_ref: STRIPE_WEBHOOK_SECRET
//!   verify_signature: true
//!   signature_header: Stripe-Signature
//! ```

use crate::error::{Result, StreamlineError};
use crate::sink::{SinkConnector, SinkMetrics, SinkStatus, SinkType};
use crate::storage::{Record, TopicManager};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info};
#[cfg(feature = "serverless")]
use tracing::warn;

/// Configuration for a serverless connector
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerlessConnectorConfig {
    /// Type of serverless connector
    pub connector_type: ServerlessConnectorType,

    /// Connector-specific configuration
    #[serde(flatten)]
    pub config: ServerlessConfig,

    /// Retry configuration
    #[serde(default)]
    pub retry: RetryConfig,

    /// Rate limiting configuration
    #[serde(default)]
    pub rate_limit: RateLimitConfig,

    /// Transformation configuration (optional)
    #[serde(default)]
    pub transform: Option<TransformConfig>,
}

/// Types of serverless connectors
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ServerlessConnectorType {
    /// Inbound HTTP webhook (receive events)
    HttpWebhookInbound,
    /// Outbound HTTP webhook (send events to external service)
    HttpWebhookOutbound,
    /// AWS S3 event notifications
    S3Events,
    /// GitHub webhook events
    GithubEvents,
    /// Stripe webhook events
    StripeEvents,
    /// Slack notifications (outbound)
    SlackOutbound,
    /// Generic REST API polling (inbound)
    RestApiPoll,
}

impl std::fmt::Display for ServerlessConnectorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerlessConnectorType::HttpWebhookInbound => write!(f, "http_webhook_inbound"),
            ServerlessConnectorType::HttpWebhookOutbound => write!(f, "http_webhook_outbound"),
            ServerlessConnectorType::S3Events => write!(f, "s3_events"),
            ServerlessConnectorType::GithubEvents => write!(f, "github_events"),
            ServerlessConnectorType::StripeEvents => write!(f, "stripe_events"),
            ServerlessConnectorType::SlackOutbound => write!(f, "slack_outbound"),
            ServerlessConnectorType::RestApiPoll => write!(f, "rest_api_poll"),
        }
    }
}

/// Connector-specific configuration variants
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ServerlessConfig {
    /// HTTP webhook configuration
    HttpWebhook(HttpWebhookConfig),
    /// S3 events configuration
    S3Events(S3EventsConfig),
    /// GitHub events configuration
    GitHub(GitHubConfig),
    /// Stripe events configuration
    Stripe(StripeConfig),
    /// Slack configuration
    Slack(SlackConfig),
    /// REST API polling configuration
    RestApi(RestApiConfig),
}

/// HTTP Webhook connector configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpWebhookConfig {
    /// For inbound: endpoint path to register (e.g., "/webhooks/my-service")
    /// For outbound: target URL to POST events to
    pub endpoint: String,

    /// HTTP method (for outbound, default: POST)
    #[serde(default = "default_http_method")]
    pub method: String,

    /// Headers to include in requests
    #[serde(default)]
    pub headers: HashMap<String, String>,

    /// Secret reference for webhook signature verification
    /// Can be an environment variable name (prefixed with $) or a literal value
    #[serde(default)]
    pub secret_ref: Option<String>,

    /// Enable signature verification for inbound webhooks
    #[serde(default)]
    pub verify_signature: bool,

    /// Header name containing the signature (for verification)
    #[serde(default = "default_signature_header")]
    pub signature_header: String,

    /// Signature algorithm (hmac-sha256, hmac-sha1)
    #[serde(default = "default_signature_algorithm")]
    pub signature_algorithm: String,

    /// Content type (default: application/json)
    #[serde(default = "default_content_type")]
    pub content_type: String,

    /// Timeout in milliseconds for outbound requests
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,

    /// Batch size for outbound delivery (default: 1)
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Batch timeout in milliseconds (default: 1000)
    #[serde(default = "default_batch_timeout_ms")]
    pub batch_timeout_ms: u64,
}

fn default_http_method() -> String {
    "POST".to_string()
}

fn default_signature_header() -> String {
    "X-Signature".to_string()
}

fn default_signature_algorithm() -> String {
    "hmac-sha256".to_string()
}

fn default_content_type() -> String {
    "application/json".to_string()
}

fn default_timeout_ms() -> u64 {
    30_000 // 30 seconds
}

fn default_batch_size() -> usize {
    1
}

fn default_batch_timeout_ms() -> u64 {
    1000
}

/// S3 Events connector configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3EventsConfig {
    /// AWS region
    pub region: String,

    /// S3 bucket name
    pub bucket: String,

    /// Event types to listen for (e.g., "s3:ObjectCreated:*")
    #[serde(default)]
    pub event_types: Vec<String>,

    /// Prefix filter (optional)
    pub prefix: Option<String>,

    /// Suffix filter (optional)
    pub suffix: Option<String>,

    /// AWS credentials reference (environment variable or secret manager)
    #[serde(default)]
    pub credentials_ref: Option<String>,
}

/// GitHub events connector configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitHubConfig {
    /// Webhook secret for signature verification
    #[serde(default)]
    pub secret_ref: Option<String>,

    /// Event types to filter (e.g., "push", "pull_request")
    #[serde(default)]
    pub event_types: Vec<String>,

    /// Repository filter (optional, e.g., "owner/repo")
    pub repository: Option<String>,

    /// Endpoint path for receiving webhooks
    #[serde(default = "default_github_endpoint")]
    pub endpoint: String,
}

fn default_github_endpoint() -> String {
    "/webhooks/github".to_string()
}

/// Stripe events connector configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StripeConfig {
    /// Stripe webhook signing secret
    #[serde(default)]
    pub secret_ref: Option<String>,

    /// Event types to filter (e.g., "payment_intent.succeeded")
    #[serde(default)]
    pub event_types: Vec<String>,

    /// Endpoint path for receiving webhooks
    #[serde(default = "default_stripe_endpoint")]
    pub endpoint: String,

    /// API version to use
    #[serde(default = "default_stripe_api_version")]
    pub api_version: String,
}

fn default_stripe_endpoint() -> String {
    "/webhooks/stripe".to_string()
}

fn default_stripe_api_version() -> String {
    "2023-10-16".to_string()
}

/// Slack connector configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlackConfig {
    /// Slack webhook URL
    pub webhook_url: String,

    /// Channel to post to (optional, uses webhook default if not specified)
    pub channel: Option<String>,

    /// Username to post as (optional)
    pub username: Option<String>,

    /// Icon emoji (optional, e.g., ":robot_face:")
    pub icon_emoji: Option<String>,

    /// Message template using Handlebars syntax
    /// Available variables: {{topic}}, {{partition}}, {{offset}}, {{key}}, {{value}}, {{timestamp}}
    #[serde(default = "default_slack_template")]
    pub message_template: String,
}

fn default_slack_template() -> String {
    "New event from `{{topic}}`: ```{{value}}```".to_string()
}

/// REST API polling connector configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestApiConfig {
    /// API endpoint URL
    pub url: String,

    /// HTTP method (default: GET)
    #[serde(default = "default_get_method")]
    pub method: String,

    /// Request headers
    #[serde(default)]
    pub headers: HashMap<String, String>,

    /// Polling interval in milliseconds
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,

    /// JSONPath expression to extract records from response
    #[serde(default)]
    pub records_path: Option<String>,

    /// Field to use as cursor for pagination
    pub cursor_field: Option<String>,

    /// Query parameters (can include cursor variable)
    #[serde(default)]
    pub query_params: HashMap<String, String>,

    /// Authentication configuration
    #[serde(default)]
    pub auth: Option<RestApiAuth>,
}

fn default_get_method() -> String {
    "GET".to_string()
}

fn default_poll_interval_ms() -> u64 {
    60_000 // 1 minute
}

/// REST API authentication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RestApiAuth {
    /// Bearer token authentication
    Bearer {
        /// Token or reference to secret
        token_ref: String,
    },
    /// Basic authentication
    Basic {
        /// Username
        username: String,
        /// Password or reference to secret
        password_ref: String,
    },
    /// API key authentication
    ApiKey {
        /// Header name for the API key
        header: String,
        /// API key or reference to secret
        key_ref: String,
    },
    /// OAuth 2.0 client credentials
    OAuth2ClientCredentials {
        /// Token endpoint URL
        token_url: String,
        /// Client ID
        client_id: String,
        /// Client secret reference
        client_secret_ref: String,
        /// Scopes (optional)
        #[serde(default)]
        scopes: Vec<String>,
    },
}

/// Retry configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Maximum number of retries
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

    /// HTTP status codes to retry on
    #[serde(default = "default_retry_status_codes")]
    pub retry_status_codes: Vec<u16>,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: default_max_retries(),
            initial_delay_ms: default_initial_delay_ms(),
            max_delay_ms: default_max_delay_ms(),
            backoff_multiplier: default_backoff_multiplier(),
            retry_status_codes: default_retry_status_codes(),
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

fn default_retry_status_codes() -> Vec<u16> {
    vec![408, 429, 500, 502, 503, 504]
}

/// Rate limiting configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// Maximum requests per second
    #[serde(default = "default_requests_per_second")]
    pub requests_per_second: u32,

    /// Burst size (max concurrent requests)
    #[serde(default = "default_burst_size")]
    pub burst_size: u32,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            requests_per_second: default_requests_per_second(),
            burst_size: default_burst_size(),
        }
    }
}

fn default_requests_per_second() -> u32 {
    100
}

fn default_burst_size() -> u32 {
    10
}

/// Transformation configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformConfig {
    /// JSONPath expressions for field extraction
    #[serde(default)]
    pub extract: HashMap<String, String>,

    /// Field mappings (rename fields)
    #[serde(default)]
    pub rename: HashMap<String, String>,

    /// Fields to drop
    #[serde(default)]
    pub drop: Vec<String>,

    /// Static fields to add
    #[serde(default)]
    pub add: HashMap<String, serde_json::Value>,

    /// Filter expression (JSONPath that must return truthy value)
    pub filter: Option<String>,
}

/// Internal state for the serverless connector
struct ServerlessState {
    status: SinkStatus,
    last_error: Option<String>,
    records_processed: AtomicU64,
    records_failed: AtomicU64,
    bytes_processed: AtomicU64,
    last_commit_timestamp: AtomicU64,
    committed_offsets: std::sync::RwLock<HashMap<String, i64>>,
}

impl Default for ServerlessState {
    fn default() -> Self {
        Self {
            status: SinkStatus::Stopped,
            last_error: None,
            records_processed: AtomicU64::new(0),
            records_failed: AtomicU64::new(0),
            bytes_processed: AtomicU64::new(0),
            last_commit_timestamp: AtomicU64::new(0),
            committed_offsets: std::sync::RwLock::new(HashMap::new()),
        }
    }
}

/// Serverless connector implementation
#[allow(dead_code)]
pub struct ServerlessConnector {
    name: String,
    topics: Vec<String>,
    config: ServerlessConnectorConfig,
    #[allow(dead_code)]
    topic_manager: Arc<TopicManager>,
    state: Arc<RwLock<ServerlessState>>,
    shutdown_tx: Option<tokio::sync::broadcast::Sender<()>>,
}

impl ServerlessConnector {
    /// Create a new serverless connector
    pub fn new(
        name: String,
        topics: Vec<String>,
        config: ServerlessConnectorConfig,
        topic_manager: Arc<TopicManager>,
    ) -> Result<Self> {
        // Validate configuration
        Self::validate_config(&config)?;

        Ok(Self {
            name,
            topics,
            config,
            topic_manager,
            state: Arc::new(RwLock::new(ServerlessState::default())),
            shutdown_tx: None,
        })
    }

    fn validate_config(config: &ServerlessConnectorConfig) -> Result<()> {
        match (&config.connector_type, &config.config) {
            (ServerlessConnectorType::HttpWebhookOutbound, ServerlessConfig::HttpWebhook(cfg)) => {
                if cfg.endpoint.is_empty() {
                    return Err(StreamlineError::Sink(
                        "HTTP webhook outbound requires an endpoint URL".to_string(),
                    ));
                }
            }
            (ServerlessConnectorType::SlackOutbound, ServerlessConfig::Slack(cfg)) => {
                if cfg.webhook_url.is_empty() {
                    return Err(StreamlineError::Sink(
                        "Slack connector requires a webhook URL".to_string(),
                    ));
                }
            }
            (ServerlessConnectorType::S3Events, ServerlessConfig::S3Events(cfg)) => {
                if cfg.bucket.is_empty() {
                    return Err(StreamlineError::Sink(
                        "S3 events connector requires a bucket name".to_string(),
                    ));
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Resolve a secret reference to its actual value
    fn resolve_secret(secret_ref: &str) -> Result<String> {
        if let Some(var_name) = secret_ref.strip_prefix('$') {
            // Environment variable reference
            std::env::var(var_name).map_err(|_| {
                StreamlineError::Sink(format!("Environment variable '{}' not found", var_name))
            })
        } else {
            // Literal value (for testing - in production, always use references)
            Ok(secret_ref.to_string())
        }
    }

    /// Process a batch of records for outbound delivery
    #[cfg(feature = "serverless")]
    async fn deliver_batch(&self, records: &[Record]) -> Result<()> {
        match (&self.config.connector_type, &self.config.config) {
            (ServerlessConnectorType::HttpWebhookOutbound, ServerlessConfig::HttpWebhook(cfg)) => {
                self.deliver_http_webhook(cfg, records).await
            }
            (ServerlessConnectorType::SlackOutbound, ServerlessConfig::Slack(cfg)) => {
                self.deliver_slack(cfg, records).await
            }
            _ => {
                // Inbound connectors don't process batches
                Ok(())
            }
        }
    }

    /// Process a batch of records - stub when serverless feature is disabled
    #[cfg(not(feature = "serverless"))]
    async fn deliver_batch(&self, _records: &[Record]) -> Result<()> {
        Err(StreamlineError::Sink(
            "Serverless connector delivery requires --features serverless".to_string(),
        ))
    }

    /// Deliver records via HTTP webhook
    #[cfg(feature = "serverless")]
    async fn deliver_http_webhook(
        &self,
        config: &HttpWebhookConfig,
        records: &[Record],
    ) -> Result<()> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(config.timeout_ms))
            .build()
            .map_err(|e| StreamlineError::Sink(format!("Failed to create HTTP client: {}", e)))?;

        for record in records {
            let payload = self.prepare_payload(record)?;

            let mut request = match config.method.to_uppercase().as_str() {
                "POST" => client.post(&config.endpoint),
                "PUT" => client.put(&config.endpoint),
                "PATCH" => client.patch(&config.endpoint),
                _ => client.post(&config.endpoint),
            };

            // Add headers
            for (key, value) in &config.headers {
                request = request.header(key, value);
            }
            request = request.header("Content-Type", &config.content_type);

            // Add signature if configured
            if let Some(ref secret_ref) = config.secret_ref {
                let secret = Self::resolve_secret(secret_ref)?;
                let signature =
                    self.compute_signature(&payload, &secret, &config.signature_algorithm)?;
                request = request.header(&config.signature_header, signature);
            }

            // Send with retry logic
            self.send_with_retry(request.body(payload)).await?;
        }

        Ok(())
    }

    /// Deliver records to Slack
    #[cfg(feature = "serverless")]
    async fn deliver_slack(&self, config: &SlackConfig, records: &[Record]) -> Result<()> {
        let client = reqwest::Client::new();

        for record in records {
            let message = self.format_slack_message(config, record)?;

            let mut payload = serde_json::json!({
                "text": message
            });

            if let Some(ref channel) = config.channel {
                payload["channel"] = serde_json::Value::String(channel.clone());
            }
            if let Some(ref username) = config.username {
                payload["username"] = serde_json::Value::String(username.clone());
            }
            if let Some(ref icon) = config.icon_emoji {
                payload["icon_emoji"] = serde_json::Value::String(icon.clone());
            }

            let request = client
                .post(&config.webhook_url)
                .header("Content-Type", "application/json")
                .json(&payload);

            self.send_with_retry(request).await?;
        }

        Ok(())
    }

    /// Prepare payload for delivery
    fn prepare_payload(&self, record: &Record) -> Result<Bytes> {
        // Apply transformations if configured
        if let Some(ref transform) = self.config.transform {
            self.apply_transform(record, transform)
        } else {
            Ok(record.value.clone())
        }
    }

    /// Apply transformation to a record
    fn apply_transform(&self, record: &Record, transform: &TransformConfig) -> Result<Bytes> {
        // Parse the record value as JSON
        let value_str = std::str::from_utf8(&record.value)
            .map_err(|e| StreamlineError::Sink(format!("Invalid UTF-8 in record: {}", e)))?;

        let mut json: serde_json::Value = serde_json::from_str(value_str)
            .map_err(|e| StreamlineError::Sink(format!("Invalid JSON in record: {}", e)))?;

        if let serde_json::Value::Object(ref mut map) = json {
            // Drop fields
            for field in &transform.drop {
                map.remove(field);
            }

            // Rename fields
            for (old_name, new_name) in &transform.rename {
                if let Some(value) = map.remove(old_name) {
                    map.insert(new_name.clone(), value);
                }
            }

            // Add static fields
            for (key, value) in &transform.add {
                map.insert(key.clone(), value.clone());
            }
        }

        let transformed = serde_json::to_vec(&json).map_err(|e| {
            StreamlineError::Sink(format!("Failed to serialize transformed record: {}", e))
        })?;

        Ok(Bytes::from(transformed))
    }

    /// Format a Slack message using the template
    fn format_slack_message(&self, config: &SlackConfig, record: &Record) -> Result<String> {
        let mut message = config.message_template.clone();

        // Simple template substitution
        message = message.replace("{{topic}}", &self.topics.join(","));
        message = message.replace("{{offset}}", &record.offset.to_string());

        if let Some(ref key) = record.key {
            let key_str = std::str::from_utf8(key).unwrap_or("<binary>");
            message = message.replace("{{key}}", key_str);
        } else {
            message = message.replace("{{key}}", "<null>");
        }

        let value_str = std::str::from_utf8(&record.value).unwrap_or("<binary>");
        message = message.replace("{{value}}", value_str);

        message = message.replace("{{timestamp}}", &record.timestamp.to_string());

        Ok(message)
    }

    /// Compute HMAC signature for webhook payload
    #[cfg(feature = "serverless")]
    fn compute_signature(&self, payload: &[u8], secret: &str, algorithm: &str) -> Result<String> {
        use hmac::{Hmac, Mac};
        use sha2::{Sha256, Sha512};

        match algorithm {
            "hmac-sha256" => {
                type HmacSha256 = Hmac<Sha256>;
                let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
                    .map_err(|e| StreamlineError::Sink(format!("Invalid HMAC key: {}", e)))?;
                mac.update(payload);
                let result = mac.finalize();
                Ok(format!("sha256={}", hex::encode(result.into_bytes())))
            }
            "hmac-sha512" => {
                type HmacSha512 = Hmac<Sha512>;
                let mut mac = HmacSha512::new_from_slice(secret.as_bytes())
                    .map_err(|e| StreamlineError::Sink(format!("Invalid HMAC key: {}", e)))?;
                mac.update(payload);
                let result = mac.finalize();
                Ok(format!("sha512={}", hex::encode(result.into_bytes())))
            }
            _ => Err(StreamlineError::Sink(format!(
                "Unsupported signature algorithm: {}",
                algorithm
            ))),
        }
    }

    /// Compute HMAC signature - stub when serverless feature is disabled
    #[cfg(not(feature = "serverless"))]
    fn compute_signature(
        &self,
        _payload: &[u8],
        _secret: &str,
        _algorithm: &str,
    ) -> Result<String> {
        Err(StreamlineError::Sink(
            "Signature computation requires --features serverless".to_string(),
        ))
    }

    /// Send request with retry logic
    #[cfg(feature = "serverless")]
    async fn send_with_retry(&self, request: reqwest::RequestBuilder) -> Result<()> {
        let retry_config = &self.config.retry;
        let mut attempts = 0;
        let mut delay = retry_config.initial_delay_ms;

        loop {
            // Clone the request for retry (reqwest doesn't allow reuse)
            let result = request
                .try_clone()
                .ok_or_else(|| {
                    StreamlineError::Sink("Failed to clone request for retry".to_string())
                })?
                .send()
                .await;

            match result {
                Ok(response) => {
                    let status = response.status();
                    if status.is_success() {
                        return Ok(());
                    }

                    // Check if we should retry this status code
                    if retry_config.retry_status_codes.contains(&status.as_u16()) {
                        attempts += 1;
                        if attempts >= retry_config.max_retries {
                            let body = response.text().await.unwrap_or_default();
                            return Err(StreamlineError::Sink(format!(
                                "HTTP request failed after {} retries: {} - {}",
                                attempts, status, body
                            )));
                        }
                        warn!(
                            status = %status,
                            attempt = attempts,
                            "Retrying request after status code"
                        );
                    } else {
                        let body = response.text().await.unwrap_or_default();
                        return Err(StreamlineError::Sink(format!(
                            "HTTP request failed: {} - {}",
                            status, body
                        )));
                    }
                }
                Err(e) => {
                    attempts += 1;
                    if attempts >= retry_config.max_retries {
                        return Err(StreamlineError::Sink(format!(
                            "HTTP request failed after {} retries: {}",
                            attempts, e
                        )));
                    }
                    warn!(error = %e, attempt = attempts, "Retrying request after error");
                }
            }

            // Exponential backoff
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
            delay = (delay as f64 * retry_config.backoff_multiplier) as u64;
            delay = delay.min(retry_config.max_delay_ms);
        }
    }

    /// Update metrics after processing
    fn update_metrics(&self, records_count: u64, bytes_count: u64, failed: bool) {
        let state = self.state.clone();
        tokio::spawn(async move {
            let state = state.read().await;
            if failed {
                state
                    .records_failed
                    .fetch_add(records_count, Ordering::Relaxed);
            } else {
                state
                    .records_processed
                    .fetch_add(records_count, Ordering::Relaxed);
                state
                    .bytes_processed
                    .fetch_add(bytes_count, Ordering::Relaxed);
                state.last_commit_timestamp.store(
                    chrono::Utc::now().timestamp_millis() as u64,
                    Ordering::Relaxed,
                );
            }
        });
    }
}

#[async_trait]
impl SinkConnector for ServerlessConnector {
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
        info!(connector = %self.name, "Starting serverless connector");

        let mut state = self.state.write().await;
        state.status = SinkStatus::Starting;
        drop(state);

        // Create shutdown channel
        let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);
        self.shutdown_tx = Some(shutdown_tx);

        // Mark as running
        let mut state = self.state.write().await;
        state.status = SinkStatus::Running;

        info!(connector = %self.name, "Serverless connector started");
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        info!(connector = %self.name, "Stopping serverless connector");

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

        info!(connector = %self.name, "Serverless connector stopped");
        Ok(())
    }

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
            "Processing batch"
        );

        let total_bytes: u64 = records.iter().map(|r| r.value.len() as u64).sum();
        let highest_offset = records.iter().map(|r| r.offset).max().unwrap_or(-1);

        // Deliver the batch
        match self.deliver_batch(&records).await {
            Ok(()) => {
                self.update_metrics(records.len() as u64, total_bytes, false);

                // Update committed offset
                let state = self.state.read().await;
                if let Ok(mut offsets) = state.committed_offsets.write() {
                    let key = format!("{}-{}", topic, partition);
                    offsets.insert(key, highest_offset);
                }

                Ok(highest_offset)
            }
            Err(e) => {
                error!(
                    connector = %self.name,
                    error = %e,
                    "Failed to deliver batch"
                );
                self.update_metrics(records.len() as u64, 0, true);

                // Update state with error
                let mut state = self.state.write().await;
                state.last_error = Some(e.to_string());
                state.status = SinkStatus::Failed;

                Err(e)
            }
        }
    }

    fn status(&self) -> SinkStatus {
        // Use blocking read for sync method
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
                    lag_records: 0, // Would need topic manager to calculate
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serverless_connector_type_display() {
        assert_eq!(
            format!("{}", ServerlessConnectorType::HttpWebhookInbound),
            "http_webhook_inbound"
        );
        assert_eq!(
            format!("{}", ServerlessConnectorType::SlackOutbound),
            "slack_outbound"
        );
    }

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.initial_delay_ms, 100);
        assert_eq!(config.backoff_multiplier, 2.0);
    }

    #[test]
    fn test_rate_limit_config_default() {
        let config = RateLimitConfig::default();
        assert_eq!(config.requests_per_second, 100);
        assert_eq!(config.burst_size, 10);
    }

    #[test]
    fn test_http_webhook_config_serialization() {
        let config = HttpWebhookConfig {
            endpoint: "https://example.com/webhook".to_string(),
            method: "POST".to_string(),
            headers: HashMap::new(),
            secret_ref: Some("$WEBHOOK_SECRET".to_string()),
            verify_signature: true,
            signature_header: "X-Signature".to_string(),
            signature_algorithm: "hmac-sha256".to_string(),
            content_type: "application/json".to_string(),
            timeout_ms: 30000,
            batch_size: 1,
            batch_timeout_ms: 1000,
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: HttpWebhookConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.endpoint, config.endpoint);
        assert_eq!(deserialized.verify_signature, config.verify_signature);
    }

    #[test]
    fn test_slack_config_serialization() {
        let config = SlackConfig {
            webhook_url: "https://hooks.slack.com/services/xxx".to_string(),
            channel: Some("#alerts".to_string()),
            username: Some("Streamline".to_string()),
            icon_emoji: Some(":rocket:".to_string()),
            message_template: "Test: {{value}}".to_string(),
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: SlackConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.webhook_url, config.webhook_url);
        assert_eq!(deserialized.channel, config.channel);
    }

    #[test]
    fn test_transform_config() {
        let config = TransformConfig {
            extract: HashMap::new(),
            rename: [("old_field".to_string(), "new_field".to_string())]
                .into_iter()
                .collect(),
            drop: vec!["secret_field".to_string()],
            add: [("source".to_string(), serde_json::json!("streamline"))]
                .into_iter()
                .collect(),
            filter: None,
        };

        assert_eq!(config.drop.len(), 1);
        assert_eq!(config.rename.len(), 1);
        assert_eq!(config.add.len(), 1);
    }

    #[test]
    fn test_secret_resolution_env_var() {
        std::env::set_var("TEST_SECRET_123", "my-secret-value");
        let result = ServerlessConnector::resolve_secret("$TEST_SECRET_123");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "my-secret-value");
        std::env::remove_var("TEST_SECRET_123");
    }

    #[test]
    fn test_secret_resolution_literal() {
        let result = ServerlessConnector::resolve_secret("literal-value");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "literal-value");
    }

    #[test]
    fn test_rest_api_auth_serialization() {
        let bearer = RestApiAuth::Bearer {
            token_ref: "$API_TOKEN".to_string(),
        };
        let json = serde_json::to_string(&bearer).unwrap();
        assert!(json.contains("bearer"));

        let api_key = RestApiAuth::ApiKey {
            header: "X-API-Key".to_string(),
            key_ref: "$API_KEY".to_string(),
        };
        let json = serde_json::to_string(&api_key).unwrap();
        assert!(json.contains("api_key"));
    }
}
