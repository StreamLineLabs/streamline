//! AI Module Configuration

use serde::{Deserialize, Serialize};

/// Main AI configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AIConfig {
    /// Enable AI features
    pub enabled: bool,
    /// Embedding configuration
    pub embedding: EmbeddingConfig,
    /// Search configuration
    pub search: SearchConfig,
    /// LLM configuration
    pub llm: LLMConfig,
    /// Routing configuration
    pub routing: RoutingConfig,
    /// Anomaly detection configuration
    pub anomaly: AnomalyConfig,
}

impl Default for AIConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            embedding: EmbeddingConfig::default(),
            search: SearchConfig::default(),
            llm: LLMConfig::default(),
            routing: RoutingConfig::default(),
            anomaly: AnomalyConfig::default(),
        }
    }
}

/// Embedding configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingConfig {
    /// Enable embeddings
    pub enabled: bool,
    /// Embedding model to use
    pub model: EmbeddingModelType,
    /// Embedding dimension
    pub dimension: usize,
    /// Normalize embeddings
    pub normalize: bool,
    /// Batch size for embedding generation
    pub batch_size: usize,
    /// Cache embeddings
    pub cache_enabled: bool,
    /// Cache size
    pub cache_size: usize,
    /// API endpoint for external embedding service
    pub api_endpoint: Option<String>,
    /// API key
    pub api_key: Option<String>,
}

impl Default for EmbeddingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            model: EmbeddingModelType::default(),
            dimension: 384,
            normalize: true,
            batch_size: 32,
            cache_enabled: true,
            cache_size: 10000,
            api_endpoint: None,
            api_key: None,
        }
    }
}

/// Embedding model types
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingModelType {
    /// Simple hash-based embeddings (no external dependencies)
    #[default]
    SimpleHash,
    /// TF-IDF based embeddings
    TfIdf,
    /// External API (OpenAI, Cohere, etc.)
    ExternalApi,
    /// Local sentence transformer model
    SentenceTransformer,
}

/// Search configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchConfig {
    /// Enable semantic search
    pub enabled: bool,
    /// Index type
    pub index_type: IndexType,
    /// Number of neighbors for approximate search
    pub num_neighbors: usize,
    /// Similarity threshold
    pub similarity_threshold: f32,
    /// Maximum results per query
    pub max_results: usize,
    /// Enable hybrid search (semantic + keyword)
    pub hybrid_enabled: bool,
    /// Weight for semantic vs keyword (0.0 = keyword only, 1.0 = semantic only)
    pub hybrid_weight: f32,
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            index_type: IndexType::default(),
            num_neighbors: 100,
            similarity_threshold: 0.7,
            max_results: 100,
            hybrid_enabled: true,
            hybrid_weight: 0.7,
        }
    }
}

/// Vector index types
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexType {
    /// Flat (exact) search
    #[default]
    Flat,
    /// HNSW (Hierarchical Navigable Small World)
    Hnsw,
    /// IVF (Inverted File Index)
    Ivf,
    /// Product Quantization
    Pq,
}

/// LLM configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LLMConfig {
    /// Enable LLM integration
    pub enabled: bool,
    /// LLM provider
    pub provider: LLMProviderType,
    /// Model name
    pub model: String,
    /// API endpoint
    pub api_endpoint: Option<String>,
    /// API key
    pub api_key: Option<String>,
    /// Enable classification
    pub classification_enabled: bool,
    /// Classification categories
    pub classification_categories: Vec<String>,
    /// Enable enrichment
    pub enrichment_enabled: bool,
    /// Enrichment fields to extract
    pub enrichment_fields: Vec<String>,
    /// Temperature for generation
    pub temperature: f32,
    /// Max tokens for response
    pub max_tokens: usize,
    /// Request timeout in seconds
    pub timeout_secs: u64,
    /// Rate limit (requests per minute)
    pub rate_limit: usize,
}

impl Default for LLMConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: LLMProviderType::default(),
            model: "gpt-3.5-turbo".to_string(),
            api_endpoint: None,
            api_key: None,
            classification_enabled: true,
            classification_categories: vec![
                "general".to_string(),
                "alert".to_string(),
                "error".to_string(),
                "transaction".to_string(),
            ],
            enrichment_enabled: true,
            enrichment_fields: vec![
                "sentiment".to_string(),
                "entities".to_string(),
                "summary".to_string(),
            ],
            temperature: 0.0,
            max_tokens: 100,
            timeout_secs: 30,
            rate_limit: 60,
        }
    }
}

/// LLM provider types
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LLMProviderType {
    /// OpenAI GPT models
    #[default]
    OpenAI,
    /// Anthropic Claude models
    Anthropic,
    /// Local LLM (llama.cpp, ollama)
    Local,
    /// Azure OpenAI
    AzureOpenAI,
    /// AWS Bedrock
    Bedrock,
    /// Custom API endpoint
    Custom,
}

/// Routing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingConfig {
    /// Enable semantic routing
    pub enabled: bool,
    /// Routing rules
    pub rules: Vec<RoutingRuleConfig>,
    /// Default destination if no rules match
    pub default_destination: String,
    /// Use embeddings for routing
    pub use_embeddings: bool,
    /// Similarity threshold for routing
    pub similarity_threshold: f32,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            rules: Vec::new(),
            default_destination: "default".to_string(),
            use_embeddings: true,
            similarity_threshold: 0.8,
        }
    }
}

/// Routing rule configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingRuleConfig {
    /// Rule name
    pub name: String,
    /// Description or example text
    pub description: String,
    /// Target destination
    pub destination: String,
    /// Priority (higher = more important)
    pub priority: i32,
    /// Pre-computed embedding (optional)
    pub embedding: Option<Vec<f32>>,
}

/// Anomaly detection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnomalyConfig {
    /// Enable anomaly detection
    pub enabled: bool,
    /// Detection method
    pub method: AnomalyMethod,
    /// Sensitivity (0.0 - 1.0, higher = more sensitive)
    pub sensitivity: f32,
    /// Window size for time-based detection
    pub window_size: usize,
    /// Minimum samples before detection
    pub min_samples: usize,
    /// Use embeddings for content anomaly detection
    pub use_embeddings: bool,
    /// Threshold for embedding-based anomalies
    pub embedding_threshold: f32,
    /// Alert on anomaly
    pub alert_enabled: bool,
    /// Alert destination topic
    pub alert_topic: Option<String>,
}

impl Default for AnomalyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            method: AnomalyMethod::default(),
            sensitivity: 0.5,
            window_size: 1000,
            min_samples: 100,
            use_embeddings: true,
            embedding_threshold: 0.5,
            alert_enabled: false,
            alert_topic: None,
        }
    }
}

/// Anomaly detection methods
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnomalyMethod {
    /// Statistical (z-score based)
    #[default]
    Statistical,
    /// Isolation Forest
    IsolationForest,
    /// Local Outlier Factor
    Lof,
    /// Autoencoder reconstruction error
    Autoencoder,
    /// Embedding distance
    EmbeddingDistance,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ai_config_default() {
        let config = AIConfig::default();
        assert!(config.enabled);
        assert!(config.embedding.enabled);
        assert!(config.search.enabled);
        assert!(!config.llm.enabled); // LLM disabled by default (needs API key)
    }

    #[test]
    fn test_embedding_config() {
        let config = EmbeddingConfig::default();
        assert_eq!(config.dimension, 384);
        assert!(config.normalize);
    }
}
