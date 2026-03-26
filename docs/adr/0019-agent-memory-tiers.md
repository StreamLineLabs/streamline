# ADR-0019: Agent Memory Tiering Model

## Status

Proposed (M1 P0 deliverable)

## Context

M1 (Agent Memory Fabric) exposes durable memory to LLM agents through MCP
tools. Cognitive-architecture literature distinguishes:

- **Episodic** — specific past events (turn-level facts).
- **Semantic** — generalized knowledge derived from episodes.
- **Procedural** — skills / patterns the agent has learned to apply.

Without explicit tiers we get one undifferentiated bag-of-facts that
under-recalls in two directions: noise from old turns drowns important
facts, and rare-but-important skills are evicted before they are useful.

## Decision

Three system topics per agent namespace:

| Tier        | Topic suffix         | Retention default  | Index   |
|-------------|----------------------|--------------------|---------|
| Episodic    | `__mem.<agent>.epi`  | 90 days, decayed   | HNSW    |
| Semantic    | `__mem.<agent>.sem`  | infinite           | HNSW    |
| Procedural  | `__mem.<agent>.proc` | infinite, versioned| BM25    |

**Importance score (v1)** for any incoming memory:

```
score = 0.4 * recency_weight
      + 0.3 * access_count_log1p
      + 0.2 * explicit_marker(if provided by caller)
      + 0.1 * embedding_distinctness
```

Ranges 0..1. The decay engine (M1 P2) demotes episodic entries with
`score < 0.2` and `age > 30d`; promotes any entry hit ≥ 5 times to the
semantic tier (with a generated summary).

**MCP tool surface (v1):**
- `recall(query, k=10, tier=any)` — vector + lexical hybrid search.
- `remember(text, tier=epi, importance=auto, ttl=null)` — write to a tier.
- `share(memory_id, with_agent)` — grant access across agent namespaces.

## Consequences

### Positive
- Maps cleanly to existing literature; enables future research integrations.
- Per-tier retention means cheap entry path with bounded long-term cost.
- Procedural tier is BM25-only — fast, deterministic, debuggable; matches
  how skills are usually phrased.

### Negative
- Three topics × N agents could explode if naively created. M1 P2 will add
  agent-namespace quotas and lazy creation.
- Importance heuristic is hand-tuned; will need an A/B harness in P2.

### Neutral
- Tier boundaries are advisory not enforced — `recall(tier=any)` always
  works; tiers exist mostly for retention + cost shaping.

## References

- Tulving (1985), "How Many Memory Systems Are There?":
  <https://psycnet.apa.org/doi/10.1037/0003-066X.40.4.385>
- LangGraph memory docs: <https://langchain-ai.github.io/langgraph/concepts/memory/>
- Letta (formerly MemGPT): <https://github.com/letta-ai/letta>
- mem0: <https://github.com/mem0ai/mem0>
- Internal: `MOONSHOT_PLAN.md` Feature M1
