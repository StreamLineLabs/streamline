# M1 P0 — Agent Memory Eval & Decay Simulation Artifacts

> Phase-0 spike outputs for `m1-p0-evalset` and `m1-p0-decay-sim`.

## `eval_v1.jsonl` — 200-question recall benchmark

JSON-Lines; one record per query. Fields:

| Field             | Type     | Notes                                                |
|-------------------|----------|------------------------------------------------------|
| `id`              | string   | Stable id (`q000`..`q199`) for tracking regressions  |
| `history`         | array    | Synthetic agent turn history (4 strings: 1 truth + 3 distractors) |
| `query`           | string   | Question to answer using memory recall               |
| `expected_substr` | string   | Substring that MUST appear in the recall result      |
| `tier_hint`       | string   | Which tier (`episodic`/`semantic`/`procedural`) the truth belongs in |

Generation seed: `42`. Re-generate with the inline Python snippet in
session checkpoint (or the planned `streamline/scripts/gen_eval.py` —
not yet committed).

### How M1 should consume it

```rust
// streamline/benches/agent_memory_recall.rs (M1 P1 deliverable)
let lines = std::fs::read_to_string("benches/agent_memory/eval_v1.jsonl")?;
for line in lines.lines() {
    let q: EvalQ = serde_json::from_str(line)?;
    // ingest q.history into memory under tier q.tier_hint
    // recall(q.query, k=10)
    // assert any hit contains q.expected_substr
}
```

Pass criterion for M1 P1 demo: **recall@10 ≥ 0.7**.
Pass criterion for M1 GA: **recall@10 ≥ 0.85**.

## `decay_sim.rs` — half-life sweep

Simulates 90 days of synthetic events (~ 1000 events) and sweeps decay
half-life ∈ {3, 7, 14, 30, 60} days. Reports % retained, % important
retained, % heavy-hit retained.

Run as a one-shot binary (see file header for instructions). Outputs
CSV on stdout — easy to pipe to a notebook or `csvlook`.

The recommended half-life printed at the end seeds ADR-0019's
"importance score v1" defaults. Re-run with adjusted constants when:

- The synthetic generator changes shape.
- Real eval traces from M1 P2 design partners suggest a different
  retention target.
