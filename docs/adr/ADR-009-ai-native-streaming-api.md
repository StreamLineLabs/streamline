# ADR-009: AI-Native Streaming SDK API

## Status
Proposed

## Date
2026-03-04

## Context
Streamline's AI module (src/ai/) provides vector embeddings, semantic search, anomaly detection, RAG pipelines, and LLM integration. These capabilities need to be exposed through SDKs for Python and Node.js developers.

## Decision

### SDK API Surface

#### Embedding
```python
# Python
vectors = await client.ai.embed(["hello world", "streaming data"])
# Returns: [[0.1, 0.2, ...], [0.3, 0.4, ...]]
```

```typescript
// Node.js
const vectors = await client.ai.embed(["hello world", "streaming data"]);
```

#### Semantic Search
```python
results = await client.ai.search("user login events", topic="events", top_k=10)
# Returns: [{"score": 0.95, "offset": 42, "value": {...}}, ...]
```

#### Anomaly Detection
```python
async for alert in client.ai.detect_anomalies("metrics", config={"threshold": 2.0}):
    print(f"Anomaly: {alert.field} = {alert.value} (z-score: {alert.z_score})")
```

#### RAG (Retrieval-Augmented Generation)
```python
answer = await client.ai.rag(
    query="What caused the outage on March 1st?",
    context_topic="incidents",
    model="gpt-4"
)
```

### HTTP API Endpoints

```
POST /api/v1/ai/embed
  Request: { "texts": ["..."], "model": "default" }
  Response: { "vectors": [[...]], "model": "text-embedding-3-small", "usage": { "tokens": 42 } }

POST /api/v1/ai/search
  Request: { "query": "...", "topic": "events", "top_k": 10 }
  Response: { "results": [{ "score": 0.95, "offset": 42, "value": {...} }] }

POST /api/v1/ai/anomalies/detect
  Request: { "topic": "metrics", "config": { "threshold": 2.0, "window_size": 100 } }
  Response (SSE): event: anomaly\ndata: { "field": "cpu", "value": 99.5, "z_score": 3.2 }

POST /api/v1/ai/rag
  Request: { "query": "...", "context_topic": "docs", "model": "gpt-4", "top_k": 5 }
  Response: { "answer": "...", "sources": [{ "offset": 42, "score": 0.9 }] }
```

### Provider Configuration
```toml
[ai]
default_provider = "openai"

[ai.providers.openai]
api_key = "${OPENAI_API_KEY}"
embedding_model = "text-embedding-3-small"
chat_model = "gpt-4"

[ai.providers.cohere]
api_key = "${COHERE_API_KEY}"

[ai.providers.local]
model_path = "/models/all-MiniLM-L6-v2"
```

## Consequences
- **Positive:** AI becomes a first-class feature accessible from any SDK
- **Positive:** Pluggable provider model (OpenAI, Cohere, local)
- **Positive:** SSE streaming for anomaly detection
- **Negative:** Requires API key management for cloud providers
- **Negative:** Vector storage adds memory overhead
