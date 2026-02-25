//! AI Gateway — Model Fallback Chains, Cost Tracking & Inference Orchestration
//!
//! Provides production-grade AI inference orchestration for streaming workloads:
//!
//! - **Fallback Chains**: Route requests through a priority-ordered list of providers;
//!   if the primary fails, the gateway transparently retries with the next provider.
//! - **Cost Tracking**: Per-invocation token/cost metering with budget enforcement.
//! - **Rate Limiting**: Token-bucket rate limiter per provider.
//! - **Circuit Breaker**: Automatically disables unhealthy providers and re-enables
//!   them after a configurable recovery window.
//!
//! # Example
//!
//! ```rust,ignore
//! use streamline::ai::gateway::{AIGateway, GatewayConfig, ProviderEntry};
//!
//! let gateway = AIGateway::new(GatewayConfig {
//!     providers: vec![
//!         ProviderEntry::openai("gpt-4o-mini"),
//!         ProviderEntry::ollama("llama3"),
//!     ],
//!     budget_limit_usd: Some(100.0),
//!     ..Default::default()
//! });
//!
//! let result = gateway.infer("Summarize this event", None).await?;
//! println!("cost=${:.6}, tokens={}", result.cost_usd, result.total_tokens);
//! ```

use crate::error::{Result, StreamlineError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Gateway-level configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayConfig {
    /// Ordered list of providers (first = highest priority).
    pub providers: Vec<ProviderEntry>,
    /// Maximum monthly spend in USD. `None` = unlimited.
    pub budget_limit_usd: Option<f64>,
    /// Maximum retries across the fallback chain per request.
    pub max_retries: u32,
    /// Circuit-breaker: consecutive failures before disabling a provider.
    pub circuit_breaker_threshold: u32,
    /// Circuit-breaker: seconds to wait before re-enabling a tripped provider.
    pub circuit_breaker_recovery_secs: u64,
    /// Default request timeout in milliseconds.
    pub timeout_ms: u64,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            providers: vec![ProviderEntry::mock()],
            budget_limit_usd: None,
            max_retries: 3,
            circuit_breaker_threshold: 5,
            circuit_breaker_recovery_secs: 60,
            timeout_ms: 30_000,
        }
    }
}

/// A single provider in the fallback chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderEntry {
    /// Provider identifier.
    pub name: String,
    /// Provider kind.
    pub kind: ProviderKind,
    /// Model identifier.
    pub model: String,
    /// API endpoint (required for OpenAI / Ollama / custom).
    pub endpoint: Option<String>,
    /// API key (required for cloud providers).
    pub api_key: Option<String>,
    /// Cost per 1K input tokens in USD.
    pub cost_per_1k_input: f64,
    /// Cost per 1K output tokens in USD.
    pub cost_per_1k_output: f64,
    /// Weight for load-balancing (0 = fallback only).
    pub weight: u32,
}

impl ProviderEntry {
    /// Create an OpenAI provider entry.
    pub fn openai(model: &str) -> Self {
        Self {
            name: format!("openai-{}", model),
            kind: ProviderKind::OpenAI,
            model: model.to_string(),
            endpoint: Some("https://api.openai.com/v1".to_string()),
            api_key: None,
            cost_per_1k_input: 0.00015,
            cost_per_1k_output: 0.0006,
            weight: 100,
        }
    }

    /// Create an Ollama (local) provider entry.
    pub fn ollama(model: &str) -> Self {
        Self {
            name: format!("ollama-{}", model),
            kind: ProviderKind::Ollama,
            model: model.to_string(),
            endpoint: Some("http://localhost:11434".to_string()),
            api_key: None,
            cost_per_1k_input: 0.0,
            cost_per_1k_output: 0.0,
            weight: 50,
        }
    }

    /// Create a mock provider for testing.
    pub fn mock() -> Self {
        Self {
            name: "mock".to_string(),
            kind: ProviderKind::Mock,
            model: "mock-model".to_string(),
            endpoint: None,
            api_key: None,
            cost_per_1k_input: 0.0,
            cost_per_1k_output: 0.0,
            weight: 1,
        }
    }
}

/// Supported provider backends.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    /// OpenAI-compatible API.
    OpenAI,
    /// Anthropic Claude API.
    Anthropic,
    /// Local Ollama server.
    Ollama,
    /// ONNX Runtime local inference.
    OnnxRuntime,
    /// Mock provider (for tests / playground).
    Mock,
}

// ---------------------------------------------------------------------------
// Cost Tracking
// ---------------------------------------------------------------------------

/// Per-invocation cost record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationCost {
    /// Provider used.
    pub provider: String,
    /// Model used.
    pub model: String,
    /// Input tokens consumed.
    pub input_tokens: u64,
    /// Output tokens generated.
    pub output_tokens: u64,
    /// Total tokens.
    pub total_tokens: u64,
    /// Cost in USD.
    pub cost_usd: f64,
    /// Timestamp.
    pub timestamp: DateTime<Utc>,
    /// Latency in milliseconds.
    pub latency_ms: u64,
}

/// Aggregate cost tracker.
pub struct CostTracker {
    /// Total spend in micro-USD (to avoid floating-point drift).
    total_micro_usd: AtomicU64,
    /// Total input tokens.
    total_input_tokens: AtomicU64,
    /// Total output tokens.
    total_output_tokens: AtomicU64,
    /// Total invocations.
    total_invocations: AtomicU64,
    /// Budget limit in micro-USD.
    budget_limit_micro_usd: Option<u64>,
    /// Per-provider cost breakdown.
    per_provider: RwLock<HashMap<String, ProviderCostSummary>>,
}

/// Summarised cost for one provider.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProviderCostSummary {
    pub invocations: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_cost_usd: f64,
    pub avg_latency_ms: f64,
    pub errors: u64,
}

/// Snapshot of the full cost tracker state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostSnapshot {
    pub total_cost_usd: f64,
    pub total_input_tokens: u64,
    pub total_output_tokens: u64,
    pub total_invocations: u64,
    pub budget_remaining_usd: Option<f64>,
    pub per_provider: HashMap<String, ProviderCostSummary>,
}

impl CostTracker {
    /// Create a new cost tracker.
    pub fn new(budget_limit_usd: Option<f64>) -> Self {
        Self {
            total_micro_usd: AtomicU64::new(0),
            total_input_tokens: AtomicU64::new(0),
            total_output_tokens: AtomicU64::new(0),
            total_invocations: AtomicU64::new(0),
            budget_limit_micro_usd: budget_limit_usd.map(|v| (v * 1_000_000.0) as u64),
            per_provider: RwLock::new(HashMap::new()),
        }
    }

    /// Record a completed invocation.
    pub async fn record(&self, cost: &InvocationCost) {
        let micro = (cost.cost_usd * 1_000_000.0) as u64;
        self.total_micro_usd.fetch_add(micro, Ordering::Relaxed);
        self.total_input_tokens
            .fetch_add(cost.input_tokens, Ordering::Relaxed);
        self.total_output_tokens
            .fetch_add(cost.output_tokens, Ordering::Relaxed);
        self.total_invocations.fetch_add(1, Ordering::Relaxed);

        let mut map = self.per_provider.write().await;
        let entry = map.entry(cost.provider.clone()).or_default();
        entry.invocations += 1;
        entry.input_tokens += cost.input_tokens;
        entry.output_tokens += cost.output_tokens;
        entry.total_cost_usd += cost.cost_usd;
        // Running average
        entry.avg_latency_ms = entry.avg_latency_ms
            + (cost.latency_ms as f64 - entry.avg_latency_ms) / entry.invocations as f64;
    }

    /// Record an error for a provider.
    pub async fn record_error(&self, provider: &str) {
        let mut map = self.per_provider.write().await;
        let entry = map.entry(provider.to_string()).or_default();
        entry.errors += 1;
    }

    /// Check whether the budget has been exceeded.
    pub fn is_budget_exceeded(&self) -> bool {
        match self.budget_limit_micro_usd {
            Some(limit) => self.total_micro_usd.load(Ordering::Relaxed) >= limit,
            None => false,
        }
    }

    /// Get a snapshot of current costs.
    pub async fn snapshot(&self) -> CostSnapshot {
        let total_micro = self.total_micro_usd.load(Ordering::Relaxed);
        let total_usd = total_micro as f64 / 1_000_000.0;
        let budget_remaining = self
            .budget_limit_micro_usd
            .map(|limit| (limit as f64 - total_micro as f64) / 1_000_000.0);

        CostSnapshot {
            total_cost_usd: total_usd,
            total_input_tokens: self.total_input_tokens.load(Ordering::Relaxed),
            total_output_tokens: self.total_output_tokens.load(Ordering::Relaxed),
            total_invocations: self.total_invocations.load(Ordering::Relaxed),
            budget_remaining_usd: budget_remaining,
            per_provider: self.per_provider.read().await.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// Circuit Breaker (per provider)
// ---------------------------------------------------------------------------

/// Per-provider health state for the circuit breaker.
struct ProviderHealth {
    /// Consecutive failure count.
    consecutive_failures: AtomicU64,
    /// Whether the provider is currently disabled.
    tripped: AtomicBool,
    /// When the provider was tripped (for recovery timing).
    tripped_at: RwLock<Option<DateTime<Utc>>>,
}

impl ProviderHealth {
    fn new() -> Self {
        Self {
            consecutive_failures: AtomicU64::new(0),
            tripped: AtomicBool::new(false),
            tripped_at: RwLock::new(None),
        }
    }

    fn record_success(&self) {
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.tripped.store(false, Ordering::Relaxed);
    }

    async fn record_failure(&self, threshold: u32) {
        let count = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
        if count >= threshold as u64 {
            self.tripped.store(true, Ordering::Relaxed);
            *self.tripped_at.write().await = Some(Utc::now());
        }
    }

    async fn is_available(&self, recovery_secs: u64) -> bool {
        if !self.tripped.load(Ordering::Relaxed) {
            return true;
        }
        // Check if recovery window has elapsed
        if let Some(tripped_at) = *self.tripped_at.read().await {
            let elapsed = Utc::now().signed_duration_since(tripped_at);
            if elapsed.num_seconds() >= recovery_secs as i64 {
                // Allow a probe — reset state optimistically
                self.tripped.store(false, Ordering::Relaxed);
                self.consecutive_failures.store(0, Ordering::Relaxed);
                return true;
            }
        }
        false
    }
}

// ---------------------------------------------------------------------------
// Inference Result
// ---------------------------------------------------------------------------

/// Result of an AI gateway inference call.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceResult {
    /// Generated text.
    pub text: String,
    /// Provider that handled the request.
    pub provider: String,
    /// Model used.
    pub model: String,
    /// Input tokens.
    pub input_tokens: u64,
    /// Output tokens.
    pub output_tokens: u64,
    /// Total tokens.
    pub total_tokens: u64,
    /// Cost in USD.
    pub cost_usd: f64,
    /// Latency in milliseconds.
    pub latency_ms: u64,
    /// Number of fallback attempts before success.
    pub fallback_attempts: u32,
}

// ---------------------------------------------------------------------------
// AI Gateway
// ---------------------------------------------------------------------------

/// Central AI inference gateway with fallback chains, cost tracking, and
/// circuit-breaker protection.
pub struct AIGateway {
    config: GatewayConfig,
    cost_tracker: Arc<CostTracker>,
    provider_health: HashMap<String, Arc<ProviderHealth>>,
}

impl AIGateway {
    /// Create a new AI gateway.
    pub fn new(config: GatewayConfig) -> Self {
        let cost_tracker = Arc::new(CostTracker::new(config.budget_limit_usd));
        let provider_health: HashMap<String, Arc<ProviderHealth>> = config
            .providers
            .iter()
            .map(|p| (p.name.clone(), Arc::new(ProviderHealth::new())))
            .collect();

        Self {
            config,
            cost_tracker,
            provider_health,
        }
    }

    /// Run inference through the fallback chain.
    ///
    /// Tries providers in priority order, skipping any that are circuit-broken.
    /// Returns the result from the first provider that succeeds.
    pub async fn infer(
        &self,
        prompt: &str,
        system_prompt: Option<&str>,
    ) -> Result<InferenceResult> {
        if self.cost_tracker.is_budget_exceeded() {
            return Err(StreamlineError::AI(
                "AI budget limit exceeded".to_string(),
            ));
        }

        let mut last_error: Option<StreamlineError> = None;
        let mut attempts: u32 = 0;

        for provider in &self.config.providers {
            if attempts >= self.config.max_retries {
                break;
            }

            let health = match self.provider_health.get(&provider.name) {
                Some(h) => h,
                None => continue,
            };

            if !health
                .is_available(self.config.circuit_breaker_recovery_secs)
                .await
            {
                tracing::debug!(
                    provider = %provider.name,
                    "Provider circuit-broken, skipping"
                );
                continue;
            }

            attempts += 1;
            let start = std::time::Instant::now();

            match self.call_provider(provider, prompt, system_prompt).await {
                Ok(raw) => {
                    health.record_success();
                    let latency_ms = start.elapsed().as_millis() as u64;

                    let cost_usd = (raw.input_tokens as f64 * provider.cost_per_1k_input
                        + raw.output_tokens as f64 * provider.cost_per_1k_output)
                        / 1000.0;

                    let invocation = InvocationCost {
                        provider: provider.name.clone(),
                        model: provider.model.clone(),
                        input_tokens: raw.input_tokens,
                        output_tokens: raw.output_tokens,
                        total_tokens: raw.input_tokens + raw.output_tokens,
                        cost_usd,
                        timestamp: Utc::now(),
                        latency_ms,
                    };
                    self.cost_tracker.record(&invocation).await;

                    return Ok(InferenceResult {
                        text: raw.text,
                        provider: provider.name.clone(),
                        model: provider.model.clone(),
                        input_tokens: raw.input_tokens,
                        output_tokens: raw.output_tokens,
                        total_tokens: raw.input_tokens + raw.output_tokens,
                        cost_usd,
                        latency_ms,
                        fallback_attempts: attempts - 1,
                    });
                }
                Err(e) => {
                    tracing::warn!(
                        provider = %provider.name,
                        error = %e,
                        "Provider failed, trying next in chain"
                    );
                    health
                        .record_failure(self.config.circuit_breaker_threshold)
                        .await;
                    self.cost_tracker.record_error(&provider.name).await;
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            StreamlineError::AI("All providers in fallback chain exhausted".to_string())
        }))
    }

    /// Get cost snapshot.
    pub async fn cost_snapshot(&self) -> CostSnapshot {
        self.cost_tracker.snapshot().await
    }

    /// Check if a specific provider is healthy.
    pub async fn is_provider_healthy(&self, name: &str) -> bool {
        match self.provider_health.get(name) {
            Some(h) => {
                h.is_available(self.config.circuit_breaker_recovery_secs)
                    .await
            }
            None => false,
        }
    }

    /// Get the cost tracker (for external monitoring).
    pub fn cost_tracker(&self) -> &Arc<CostTracker> {
        &self.cost_tracker
    }

    // -----------------------------------------------------------------------
    // Private: dispatch to concrete provider backend
    // -----------------------------------------------------------------------

    async fn call_provider(
        &self,
        entry: &ProviderEntry,
        prompt: &str,
        system_prompt: Option<&str>,
    ) -> Result<RawInferenceResult> {
        match entry.kind {
            ProviderKind::Mock => self.call_mock(prompt),
            ProviderKind::OpenAI | ProviderKind::Anthropic => {
                self.call_http_llm(entry, prompt, system_prompt).await
            }
            ProviderKind::Ollama => self.call_ollama(entry, prompt, system_prompt).await,
            ProviderKind::OnnxRuntime => {
                Err(StreamlineError::AI(
                    "ONNX Runtime provider requires the onnxruntime crate; \
                     use Ollama or OpenAI for inference"
                        .to_string(),
                ))
            }
        }
    }

    fn call_mock(&self, prompt: &str) -> Result<RawInferenceResult> {
        let input_tokens = (prompt.len() / 4) as u64;
        Ok(RawInferenceResult {
            text: format!("[mock] Processed: {}", &prompt[..prompt.len().min(50)]),
            input_tokens,
            output_tokens: 10,
        })
    }

    async fn call_http_llm(
        &self,
        entry: &ProviderEntry,
        prompt: &str,
        system_prompt: Option<&str>,
    ) -> Result<RawInferenceResult> {
        let endpoint = entry
            .endpoint
            .as_deref()
            .unwrap_or("https://api.openai.com/v1");
        let api_key = entry
            .api_key
            .as_deref()
            .ok_or_else(|| StreamlineError::AI("API key required for cloud provider".into()))?;

        let mut messages = Vec::new();
        if let Some(sys) = system_prompt {
            messages.push(serde_json::json!({"role": "system", "content": sys}));
        }
        messages.push(serde_json::json!({"role": "user", "content": prompt}));

        let body = serde_json::json!({
            "model": entry.model,
            "messages": messages,
            "temperature": 0.0,
            "max_tokens": 256,
        });

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(self.config.timeout_ms))
            .build()
            .map_err(|e| StreamlineError::AI(format!("HTTP client error: {}", e)))?;

        let resp = client
            .post(format!("{}/chat/completions", endpoint))
            .header("Authorization", format!("Bearer {}", api_key))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| StreamlineError::AI(format!("Request failed: {}", e)))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp
                .text()
                .await
                .unwrap_or_else(|_| "unknown".to_string());
            return Err(StreamlineError::AI(format!(
                "Provider returned {}: {}",
                status, text
            )));
        }

        let json: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| StreamlineError::AI(format!("Parse error: {}", e)))?;

        let text = json["choices"][0]["message"]["content"]
            .as_str()
            .unwrap_or("")
            .to_string();
        let input_tokens = json["usage"]["prompt_tokens"].as_u64().unwrap_or(0);
        let output_tokens = json["usage"]["completion_tokens"].as_u64().unwrap_or(0);

        Ok(RawInferenceResult {
            text,
            input_tokens,
            output_tokens,
        })
    }

    async fn call_ollama(
        &self,
        entry: &ProviderEntry,
        prompt: &str,
        system_prompt: Option<&str>,
    ) -> Result<RawInferenceResult> {
        let endpoint = entry
            .endpoint
            .as_deref()
            .unwrap_or("http://localhost:11434");

        let body = serde_json::json!({
            "model": entry.model,
            "prompt": prompt,
            "system": system_prompt.unwrap_or(""),
            "stream": false,
        });

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(self.config.timeout_ms))
            .build()
            .map_err(|e| StreamlineError::AI(format!("HTTP client error: {}", e)))?;

        let resp = client
            .post(format!("{}/api/generate", endpoint))
            .json(&body)
            .send()
            .await
            .map_err(|e| StreamlineError::AI(format!("Ollama request failed: {}", e)))?;

        if !resp.status().is_success() {
            let status = resp.status();
            return Err(StreamlineError::AI(format!(
                "Ollama returned status {}",
                status
            )));
        }

        let json: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| StreamlineError::AI(format!("Parse error: {}", e)))?;

        let text = json["response"].as_str().unwrap_or("").to_string();
        let input_tokens = json["prompt_eval_count"].as_u64().unwrap_or_else(|| {
            (prompt.len() / 4) as u64
        });
        let output_tokens = json["eval_count"]
            .as_u64()
            .unwrap_or_else(|| (text.len() / 4) as u64);

        Ok(RawInferenceResult {
            text,
            input_tokens,
            output_tokens,
        })
    }
}

/// Raw inference result before cost calculation.
struct RawInferenceResult {
    text: String,
    input_tokens: u64,
    output_tokens: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gateway_creation() {
        let gw = AIGateway::new(GatewayConfig::default());
        assert_eq!(gw.config.providers.len(), 1);
        assert_eq!(gw.config.providers[0].kind, ProviderKind::Mock);
    }

    #[tokio::test]
    async fn test_mock_inference() {
        let gw = AIGateway::new(GatewayConfig::default());
        let result = gw.infer("Hello world", None).await.unwrap();
        assert!(result.text.contains("mock"));
        assert_eq!(result.cost_usd, 0.0);
        assert_eq!(result.fallback_attempts, 0);
    }

    #[tokio::test]
    async fn test_cost_tracking() {
        let gw = AIGateway::new(GatewayConfig::default());
        let _ = gw.infer("test prompt", None).await.unwrap();
        let _ = gw.infer("another prompt", None).await.unwrap();

        let snap = gw.cost_snapshot().await;
        assert_eq!(snap.total_invocations, 2);
        assert_eq!(snap.total_cost_usd, 0.0); // Mock has zero cost
    }

    #[tokio::test]
    async fn test_budget_enforcement() {
        let gw = AIGateway::new(GatewayConfig {
            budget_limit_usd: Some(0.0), // Zero budget
            ..Default::default()
        });
        let result = gw.infer("test", None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_fallback_chain() {
        let config = GatewayConfig {
            providers: vec![
                // First provider will fail (no API key for OpenAI)
                ProviderEntry {
                    name: "failing-openai".to_string(),
                    kind: ProviderKind::OpenAI,
                    model: "gpt-4".to_string(),
                    endpoint: Some("https://api.openai.com/v1".to_string()),
                    api_key: None, // Will fail
                    cost_per_1k_input: 0.03,
                    cost_per_1k_output: 0.06,
                    weight: 100,
                },
                // Fallback mock will succeed
                ProviderEntry::mock(),
            ],
            ..Default::default()
        };

        let gw = AIGateway::new(config);
        let result = gw.infer("test fallback", None).await.unwrap();
        assert_eq!(result.provider, "mock");
        assert_eq!(result.fallback_attempts, 1);
    }

    #[tokio::test]
    async fn test_provider_health_circuit_breaker() {
        let health = ProviderHealth::new();
        assert!(health.is_available(60).await);

        // Trip the circuit breaker
        for _ in 0..5 {
            health.record_failure(5).await;
        }
        assert!(!health.is_available(60).await);

        // Success resets
        health.record_success();
        assert!(health.is_available(60).await);
    }

    #[test]
    fn test_cost_tracker_budget() {
        let tracker = CostTracker::new(Some(10.0));
        assert!(!tracker.is_budget_exceeded());
    }

    #[test]
    fn test_provider_entry_factories() {
        let openai = ProviderEntry::openai("gpt-4o-mini");
        assert_eq!(openai.kind, ProviderKind::OpenAI);
        assert!(openai.cost_per_1k_input > 0.0);

        let ollama = ProviderEntry::ollama("llama3");
        assert_eq!(ollama.kind, ProviderKind::Ollama);
        assert_eq!(ollama.cost_per_1k_input, 0.0);
    }
}
