# ADR-0014: Default Embedding Model for Semantic Topics

## Status

Proposed (M2 P0 deliverable — see `MOONSHOT_PLAN.md` §M2 and session `plan.md`)

## Context

M2 (Semantic Topics) requires a default embedding model that ships in the
single binary and runs on commodity 8-core CPU at line-rate alongside append
traffic. The model must:

- Run in-process (no external service) to preserve the single-binary promise
  established by ADR-0002.
- Stay under ~150 MB on disk to keep the default install small.
- Sustain ≥ 0.5× the topic's append throughput on 8 cores so embedding can
  run inline with backpressure (see `streamline/src/ai/` worker design in
  M2 P1).
- Hit recall@10 ≥ 0.85 on a representative dataset so the search experience
  is credible at GA.

Three open candidates were considered:

| Model                       | Dim | Size  | License    | Notes                          |
|-----------------------------|-----|-------|------------|--------------------------------|
| `bge-small-en-v1.5`         | 384 | 133MB | MIT        | Strong English MTEB scores     |
| `gte-small`                 | 384 | 67MB  | MIT        | Smallest; lower recall ceiling |
| `multilingual-e5-small`     | 384 | 471MB | MIT        | Multilingual; larger           |

Inference runtimes considered: `candle` (pure Rust, no system deps) vs `ort`
(ONNX Runtime, larger but mature). `candle` aligns with the
zero-system-deps promise.

## Decision

1. **Default model:** `bge-small-en-v1.5` shipped as a quantized (int8) GGUF
   under the `ai-embed` feature flag in `streamline/src/ai/embed/`.
2. **Runtime:** `candle` for default; `ort` available behind `ai-embed-onnx`
   for users who want hardware acceleration (CUDA/CoreML).
3. **Pluggable:** Topic config `semantic.model = <name>` resolves through a
   registry so users can swap in `gte-small`, `multilingual-e5-small`, or a
   custom path. Resolver lives at `streamline/src/ai/embed/registry.rs`.
4. **Multilingual:** Users who need it set `semantic.model = e5-multilingual`
   explicitly. We do not pay the size tax by default.

The decision is revisited if Phase-0 benchmark numbers fail the throughput
or recall gates above.

## Consequences

### Positive
- Out-of-the-box English semantic search with one config knob.
- No network calls during embedding; works air-gapped.
- Same registry pattern can later host re-rankers and SPLADE-style sparse
  encoders.

### Negative
- English-first default risks alienating non-English users until they discover
  the multilingual swap.
- Pinning to a quantized model means recall is slightly below the fp16 paper
  numbers.

### Neutral
- Model files are downloaded on first use (via `streamline embed pull`) rather
  than embedded in the binary, keeping the binary itself small.

## References

- BGE paper: <https://arxiv.org/abs/2309.07597>
- MTEB leaderboard: <https://huggingface.co/spaces/mteb/leaderboard>
- candle: <https://github.com/huggingface/candle>
- Internal: `MOONSHOT_PLAN.md` Feature M2; session `plan.md` § M2 P0.
