# AI-Native Streaming

## Summary

Add first-class AI/ML capabilities to Streamline, making it the world's first streaming platform with native vector embeddings, semantic search, LLM integration, and AI-powered stream processing. This positions Streamline as "The AI-Native Streaming Platform" - a unique market position with no direct competition.

## Current State

- No vector storage or embedding capabilities
- No semantic search functionality
- No LLM integration
- Basic anomaly detection stub in `src/autotuning/` (pattern-based, not ML)
- WASM transforms exist (`src/wasm/`) but not AI-focused
- DuckDB analytics (`src/analytics/`) provides SQL but no vector operations

## Requirements

### Core AI Features

| Requirement | Priority | Description |
|-------------|----------|-------------|
| Vector Storage | P0 | Store and index vector embeddings alongside messages |
| Embedding Generation | P0 | Generate embeddings via configurable providers (OpenAI, local) |
| Semantic Search | P0 | Query streams by meaning, not just keys |
| AI Enrichment | P1 | LLM-powered message classification/enrichment |
| Semantic Routing | P1 | Route messages based on semantic similarity |
| Anomaly Detection | P2 | ML-based pattern anomaly detection on streams |
| RAG Pipeline | P2 | Real-time Retrieval-Augmented Generation support |

### Data Structures

Create `src/ai/mod.rs`:

```rust
//! AI-Native Streaming Module
//!
//! Provides vector embeddings, semantic search, and LLM integration
//! for intelligent stream processing.

pub mod embeddings;
pub mod vector_store;
pub mod semantic_search;
pub mod llm;
pub mod routing;
pub mod anomaly;
pub mod rag;

/// Configuration for AI features
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiConfig {
    /// Embedding provider configuration
    pub embedding: EmbeddingConfig,
    /// LLM provider configuration
    pub llm: Option<LlmConfig>,
    /// Vector index configuration
    pub vector_index: VectorIndexConfig,
    /// Enable semantic routing
    pub semantic_routing: bool,
    /// Anomaly detection settings
    pub anomaly_detection: Option<AnomalyConfig>,
}

/// Embedding provider configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingConfig {
    /// Provider type: "openai", "local", "huggingface"
    pub provider: EmbeddingProvider,
    /// Model name (e.g., "text-embedding-3-small")
    pub model: String,
    /// Vector dimension (e.g., 1536 for OpenAI)
    pub dimension: usize,
    /// API key (for cloud providers)
    pub api_key: Option<String>,
    /// Batch size for embedding generation
    pub batch_size: usize,
    /// Cache embeddings to avoid recomputation
    pub cache_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EmbeddingProvider {
    OpenAI,
    Local,           // ONNX runtime
    HuggingFace,
    Cohere,
    Custom(String),  // Custom endpoint
}

/// LLM provider configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmConfig {
    pub provider: LlmProvider,
    pub model: String,
    pub api_key: Option<String>,
    pub max_tokens: usize,
    pub temperature: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LlmProvider {
    OpenAI,
    Anthropic,
    Local,          // Ollama/llama.cpp
    Custom(String),
}
```

Create `src/ai/vector_store.rs`:

```rust
/// Vector index for similarity search
pub struct VectorIndex {
    /// Index name (usually topic name)
    name: String,
    /// Vector dimension
    dimension: usize,
    /// HNSW index for approximate nearest neighbor
    index: HnswIndex,
    /// Metadata storage (offset -> metadata)
    metadata: HashMap<u64, VectorMetadata>,
    /// Statistics
    stats: VectorIndexStats,
}

/// Metadata associated with each vector
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorMetadata {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub key: Option<Vec<u8>>,
    /// Original text that was embedded
    pub text: String,
    /// Custom metadata fields
    pub attributes: HashMap<String, Value>,
}

/// Vector search result
#[derive(Debug, Clone)]
pub struct VectorSearchResult {
    pub metadata: VectorMetadata,
    pub score: f32,       // Similarity score (0-1)
    pub distance: f32,    // L2 distance
    pub vector: Vec<f32>, // The actual embedding
}

/// Configuration for vector index
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorIndexConfig {
    /// Index type: "hnsw", "flat", "ivf"
    pub index_type: VectorIndexType,
    /// HNSW M parameter (connections per node)
    pub hnsw_m: usize,
    /// HNSW ef_construction parameter
    pub hnsw_ef_construction: usize,
    /// HNSW ef_search parameter
    pub hnsw_ef_search: usize,
    /// Distance metric
    pub metric: DistanceMetric,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DistanceMetric {
    Cosine,
    Euclidean,
    DotProduct,
}
```

Create `src/ai/semantic_search.rs`:

```rust
/// Semantic search engine for stream data
pub struct SemanticSearchEngine {
    /// Embedding generator
    embeddings: Arc<EmbeddingGenerator>,
    /// Vector indices by topic
    indices: DashMap<String, VectorIndex>,
    /// Search configuration
    config: SemanticSearchConfig,
}

impl SemanticSearchEngine {
    /// Search across topics by semantic meaning
    pub async fn search(
        &self,
        query: &str,
        options: SearchOptions,
    ) -> Result<Vec<VectorSearchResult>>;

    /// Search within a specific topic
    pub async fn search_topic(
        &self,
        topic: &str,
        query: &str,
        options: SearchOptions,
    ) -> Result<Vec<VectorSearchResult>>;

    /// Find similar messages to a given offset
    pub async fn find_similar(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        top_k: usize,
    ) -> Result<Vec<VectorSearchResult>>;
}

#[derive(Debug, Clone)]
pub struct SearchOptions {
    pub top_k: usize,
    pub min_score: f32,
    pub topics: Option<Vec<String>>,
    pub time_range: Option<(i64, i64)>,
    pub filters: HashMap<String, Value>,
}
```

Create `src/ai/llm.rs`:

```rust
/// LLM integration for stream enrichment
pub struct LlmProcessor {
    provider: Box<dyn LlmProvider>,
    config: LlmConfig,
    /// Rate limiter for API calls
    rate_limiter: RateLimiter,
    /// Response cache
    cache: LruCache<String, LlmResponse>,
}

/// Trait for LLM providers
#[async_trait]
pub trait LlmProvider: Send + Sync {
    async fn complete(&self, prompt: &str, options: &LlmOptions) -> Result<LlmResponse>;
    async fn classify(&self, text: &str, categories: &[String]) -> Result<Classification>;
    async fn extract(&self, text: &str, schema: &JsonSchema) -> Result<Value>;
    async fn summarize(&self, texts: &[String]) -> Result<String>;
}

/// Stream enrichment pipeline
pub struct EnrichmentPipeline {
    /// Classification rules
    classifiers: Vec<ClassificationRule>,
    /// Extraction schemas
    extractors: Vec<ExtractionRule>,
    /// Enrichment functions
    enrichers: Vec<EnrichmentRule>,
}

#[derive(Debug, Clone)]
pub struct ClassificationRule {
    pub name: String,
    pub categories: Vec<String>,
    pub output_field: String,
    pub confidence_threshold: f32,
}
```

### CLI Commands

Add to `src/cli.rs`:

```bash
# Vector index management
streamline-cli ai index create <topic> --dimension 1536 --provider openai
streamline-cli ai index list
streamline-cli ai index describe <topic>
streamline-cli ai index rebuild <topic>

# Semantic search
streamline-cli ai search "error in payment processing" --topics events,logs --top-k 10
streamline-cli ai similar <topic> --offset 12345 --top-k 5

# Enrichment pipelines
streamline-cli ai enrich create <pipeline-name> --config pipeline.yaml
streamline-cli ai enrich start <pipeline-name>
streamline-cli ai enrich status <pipeline-name>

# Anomaly detection
streamline-cli ai anomaly enable <topic> --sensitivity medium
streamline-cli ai anomaly status
```

### HTTP API Endpoints

Add to `src/server/ai_api.rs`:

```
POST   /api/v1/ai/search                    # Semantic search
POST   /api/v1/ai/embed                     # Generate embeddings
GET    /api/v1/ai/topics/{topic}/index      # Get vector index info
POST   /api/v1/ai/topics/{topic}/index      # Create vector index
DELETE /api/v1/ai/topics/{topic}/index      # Delete vector index
POST   /api/v1/ai/topics/{topic}/similar    # Find similar messages
POST   /api/v1/ai/enrich                    # Enrich message with LLM
POST   /api/v1/ai/classify                  # Classify message
GET    /api/v1/ai/anomalies                 # Get detected anomalies
```

### Implementation Tasks

1. **Task 1: Core AI Module Structure**
   - Create `src/ai/mod.rs` with module exports
   - Define `AiConfig`, `EmbeddingConfig`, `LlmConfig`
   - Add `ai` feature flag to Cargo.toml
   - Add dependencies: `hnsw_rs`, `ort` (ONNX Runtime), `tiktoken-rs`
   - Files: `src/ai/mod.rs`, `src/ai/config.rs`, `Cargo.toml`
   - Acceptance: Module compiles, feature flag works

2. **Task 2: Embedding Generation**
   - Implement `EmbeddingGenerator` trait
   - Add OpenAI embeddings provider (API client)
   - Add local ONNX embeddings provider (for offline use)
   - Add embedding cache with LRU eviction
   - Add batch embedding for efficiency
   - Files: `src/ai/embeddings.rs`, `src/ai/providers/openai.rs`, `src/ai/providers/onnx.rs`
   - Acceptance: Can generate embeddings from text, cache works

3. **Task 3: Vector Storage & Indexing**
   - Implement `VectorIndex` with HNSW algorithm
   - Add persistent storage (vector segments)
   - Implement vector metadata storage
   - Add index rebuild functionality
   - Support multiple distance metrics
   - Files: `src/ai/vector_store.rs`, `src/ai/hnsw.rs`
   - Acceptance: Can insert/search vectors, persistence works

4. **Task 4: Semantic Search Engine**
   - Implement `SemanticSearchEngine`
   - Add cross-topic search capability
   - Implement filtering (time range, attributes)
   - Add search result ranking/scoring
   - Files: `src/ai/semantic_search.rs`
   - Acceptance: Semantic search returns relevant results

5. **Task 5: Stream Integration**
   - Hook embeddings into produce path (optional)
   - Add `VectorIndexManager` to `TopicManager`
   - Implement real-time indexing on message arrival
   - Add backfill capability for existing messages
   - Files: `src/storage/topic.rs`, `src/ai/indexer.rs`
   - Acceptance: New messages automatically indexed

6. **Task 6: LLM Integration**
   - Implement `LlmProvider` trait
   - Add OpenAI provider (GPT-4, GPT-3.5)
   - Add Anthropic provider (Claude)
   - Add local provider (Ollama integration)
   - Implement rate limiting and caching
   - Files: `src/ai/llm.rs`, `src/ai/providers/`
   - Acceptance: Can call LLM for text processing

7. **Task 7: Enrichment Pipeline**
   - Implement `EnrichmentPipeline` for stream processing
   - Add classification rules (categorize messages)
   - Add extraction rules (extract structured data)
   - Add routing rules (semantic routing)
   - Files: `src/ai/enrichment.rs`, `src/ai/routing.rs`
   - Acceptance: Pipeline enriches messages with AI

8. **Task 8: Anomaly Detection**
   - Integrate ML anomaly detection
   - Use embeddings for semantic anomaly detection
   - Add statistical anomaly detection (baseline)
   - Implement alerting on anomalies
   - Files: `src/ai/anomaly.rs`
   - Acceptance: Detects unusual patterns in streams

9. **Task 9: CLI Commands**
   - Add `ai` subcommand to CLI
   - Implement index management commands
   - Implement search commands
   - Implement enrichment pipeline commands
   - Files: `src/cli.rs`, `src/cli_utils/ai_commands.rs`
   - Acceptance: All CLI commands work

10. **Task 10: HTTP API**
    - Implement AI REST endpoints
    - Add WebSocket streaming for search results
    - Add OpenAPI documentation
    - Files: `src/server/ai_api.rs`, `src/server/http.rs`
    - Acceptance: API endpoints functional

11. **Task 11: RAG Pipeline Support**
    - Implement RAG query interface
    - Add context window management
    - Add retrieval + generation pipeline
    - Files: `src/ai/rag.rs`
    - Acceptance: Can perform RAG queries on stream data

12. **Task 12: Documentation & Examples**
    - Add module documentation
    - Create example applications
    - Add configuration guide
    - Files: `docs/ai-native-streaming.md`, `examples/ai/`
    - Acceptance: Documentation complete

## Dependencies

Add to `Cargo.toml`:

```toml
# AI/ML dependencies (optional - ai feature)
hnsw_rs = { version = "0.3", optional = true }
ort = { version = "2.0", optional = true }  # ONNX Runtime
tiktoken-rs = { version = "0.5", optional = true }
async-openai = { version = "0.23", optional = true }

[features]
ai = ["dep:hnsw_rs", "dep:ort", "dep:tiktoken-rs", "dep:async-openai"]
full = ["...", "ai"]  # Add to full edition
```

## Acceptance Criteria

- [ ] Vector embeddings generated and stored with messages
- [ ] Semantic search returns relevant results across topics
- [ ] LLM enrichment works with OpenAI and local providers
- [ ] Semantic routing directs messages based on meaning
- [ ] Anomaly detection identifies unusual patterns
- [ ] CLI commands fully functional
- [ ] HTTP API documented and working
- [ ] Performance: <10ms for vector search (1M vectors)
- [ ] All tests pass
- [ ] Documentation complete

## Example Usage

```rust
use streamline::{EmbeddedStreamline, ai::{AiConfig, SemanticSearch}};

// Create instance with AI enabled
let config = AiConfig {
    embedding: EmbeddingConfig {
        provider: EmbeddingProvider::OpenAI,
        model: "text-embedding-3-small".into(),
        dimension: 1536,
        api_key: Some(env::var("OPENAI_API_KEY")?),
        ..Default::default()
    },
    ..Default::default()
};

let streamline = EmbeddedStreamline::with_ai(config)?;

// Create vector index on topic
streamline.ai().create_index("events")?;

// Produce message (auto-indexed)
streamline.produce("events", 0, None, b"Payment failed for user 123")?;

// Semantic search
let results = streamline.ai().search("payment failures", SearchOptions {
    top_k: 10,
    min_score: 0.7,
    ..Default::default()
}).await?;

// LLM enrichment
let enriched = streamline.ai().classify(
    "Payment failed for user 123",
    &["urgent", "normal", "spam"]
).await?;
```

## Related Files

- `src/ai/` - New AI module (all files)
- `src/storage/topic.rs` - Integration with storage layer
- `src/server/ai_api.rs` - HTTP API endpoints
- `src/cli.rs` - CLI commands
- `Cargo.toml` - Dependencies and features
- `src/lib.rs` - Module exports

## Labels

`enhancement`, `feature`, `ai`, `game-changer`, `xlarge`
