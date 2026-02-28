# RAG Pipeline Example

Demonstrates building a Retrieval-Augmented Generation pipeline with Streamline.

## Overview
1. Ingest documents into a Streamline topic
2. Auto-embed documents using Streamline's AI module
3. Query with natural language — Streamline retrieves relevant context and generates answers

## Setup
```bash
export OPENAI_API_KEY="your-key"
streamline --features ai --playground
```

## Run
```python
from streamline_sdk import StreamlineClient
from streamline_sdk.ai import AIClient

client = StreamlineClient("localhost:9092")
ai = AIClient("http://localhost:9094")

# Ingest documents
for doc in documents:
    await client.produce("knowledge-base", doc)

# Query
result = await ai.rag(
    query="What are the best practices for streaming architecture?",
    context_topic="knowledge-base",
    model="gpt-4",
)
print(result.answer)
```
