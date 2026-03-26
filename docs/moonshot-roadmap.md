# Streamline Moonshots — Phase 2/3/4 Roadmap

> Companion to `MOONSHOT_PLAN.md` and `plan.md`. This doc expands every
> "later phase" placeholder for the 5 moonshots into concrete file targets,
> exit criteria, and design-partner notes.
>
> **Sequencing reminder:** P2 (Alpha) starts only after P1 demo passes;
> P3 (Beta) starts only after P2 design partners are live; P4 (GA) starts
> only after P3 SLOs hold for 30 days.
>
> File targets cited below assume the P1 scaffolds in this repo are wired
> into `lib.rs` first (see `MOONSHOT_PLAN.md` §"Build-safety strategy").

---

## M2 — Semantic Topics

### M2 P2 Alpha (~6 weeks)
**Theme:** Real workloads, hybrid retrieval, more SDKs.

Deliverables:
- **Hybrid retrieval (BM25 + vector via RRF)**
  - `streamline/src/ai/semantic_topics/hybrid.rs` — Reciprocal Rank Fusion combiner.
  - `streamline/src/ai/bm25.rs` (extend existing) — partition-local index.
  - HTTP `?mode=hybrid|vector|bm25` flag on `/search`.
- **Cold-tier search** — search over Iceberg snapshots without rehydrating.
  - `streamline/src/lakehouse/iceberg_search.rs`.
- **SDK fan-out** — Java, Go, Node, Rust SDKs each expose `consumer.search(query, k)`.
  - `streamline-java-sdk/streamline-client/src/main/java/io/streamline/search/`
  - `streamline-go-sdk/search.go`
  - `streamline-node-sdk/src/search.ts`
  - `streamline-rust-sdk/src/search.rs`
- **Re-embed migration tool** — `streamline ai reembed --topic <t> --model <m>`.
  - `streamline/src/cli_ai.rs` (extend).

Exit criteria: 3 design partners (1 retrieval-heavy SaaS, 1 fintech compliance, 1 dev-tools) running ≥10M records each; recall@10 ≥ 0.85; p95 < 200ms at 100 QPS.

### M2 P3 Beta (~6 weeks)
- **Cost ledger** — per-topic `embedding_cost_usd_total` metric.
- **Observability** — `streamline_semantic_index_size_bytes`, `…_recall_proxy`.
- **Schema evolution** — re-embed without downtime via shadow index.
- **Public design-partner reference logos** in `streamline-docs/`.

Exit criteria: 30-day SLO holds (p95 < 200 ms, error rate < 0.1%).

### M2 P4 GA
- Promote `semantic-topics` feature flag to default in `lite` profile.
- Public **Streamline-Bench** repo with reproducible numbers vs Pinecone/pgvector/Qdrant.
- Stability tier promoted to `Beta` in `docs/API_STABILITY.md` (one release later → `Stable`).
- Launch venue: HN front-page + a16z/redpoint AI infra newsletter.

---

## M4 — Enforced Contracts + Provenance

### M4 P2 Alpha (~8 weeks)
- **Lineage graph** — every accepted record carries `parent-attest` header chain.
  - `streamline/src/lineage/graph.rs` (extend) + `/v1/lineage/{topic}/{offset}` HTTP.
- **SDK verifiers** — Go, Python, Node, .NET, Rust mirror the Java verifier.
  - One file each: `streamline-<lang>-sdk/.../attestation/Verifier.<ext>`.
- **KMS adapters** — Vault + AWS KMS implementations of `KeyProvider`.
  - `streamline/src/security/kms_vault.rs`, `…/kms_aws.rs`.
- **CLI** — `streamline contract diff <old> <new>`, `… verify --since <ts>`.

Exit criteria: 2 enterprise pilots verifying every consumed record; signature p95 add ≤ 5% throughput cost.

### M4 P3 Beta (~8 weeks)
- **Drift dashboards** — Grafana JSON in `streamline-deploy/grafana/`.
- **DataHub bridge** — emit lineage to a DataHub instance.
- **Compliance pack** — AI Act Article 10 mapping doc, SOC2 audit log appendix.
- **Bypass audit UI** — surface in `streamline console`.

### M4 P4 GA
- Promote `attestation` feature to `Beta` tier; default-on for new topics with active contracts.
- Launch alongside SOC2 Type II report.

---

## M1 — Agent Memory Fabric

### M1 P2 Alpha (~10 weeks)
- **Decay engine** — half-life-based importance decay; configurable per agent.
  - `streamline/src/memory/decay.rs` (uses `benches/agent_memory/decay_sim.rs` defaults).
- **Multi-agent share + ACL** — `share()` MCP tool fully wired with TTL grants.
  - `streamline/src/memory/acl.rs` + `__acl.<agent>` system topic.
- **Export / import** — `streamline memory export --agent <id> > backup.jsonl`.
  - `streamline/src/cli_memory.rs`.
- **Design partners** — Letta, mem0, CrewAI integrations (each: 1 example app + docs page).

Exit criteria: 3 design partners shipping memory-backed apps; recall@10 ≥ 0.75 on shared eval.

### M1 P3 Beta (~10 weeks)
- **Multi-tenant** — namespace `__mem.{tenant}.{agent}.{tier}`; RBAC via existing `auth`.
- **Per-agent envelope keys** via KMS adapter from M4.
- **GDPR delete** — `streamline memory forget --agent <id> --before <ts>` writes tombstones across all tiers.
- **SOC2 audit log** — every `recall`/`remember`/`share` emits to `__audit.memory`.

### M1 P4 GA
- Promote `agent-memory` feature to `Stable`.
- Public **Memory Eval Leaderboard** at `streamline-docs/memory-leaderboard/`.
- Cursor + Claude Desktop + ChatGPT GPT integration cookbook.

---

## M5 — Branched Streams / Time-Travel Replay

### M5 P2 Alpha (~10 weeks)
- **`branch run` with WASM transforms** — replay history through a WASM transform on a branch.
  - `streamline/src/branches/replay.rs` + reuse `streamline/src/faas/wasm_runtime.rs`.
- **`branch diff`** — diff records between branch and base offset.
  - HTTP `/v1/branches/{id}/diff?since=…`.
- **Branch quotas** — cap branch-write storage per tenant; add metric.

Exit criteria: 2 ML/dev-tools partners running prod replay-test workflows on branches.

### M5 P3 Beta (~12 weeks)
- **Iceberg snapshot integration** — Streamline branches map 1:1 to Iceberg `branches/refs`.
  - `streamline/src/lakehouse/iceberg_branches.rs` (uses spike `docs/spikes/m5-iceberg-snapshots.md`).
- **Lineage UI** — graph visualization in `streamline console`.
- **Storage GC** — auto-discard branches older than retention; alert on stale.

### M5 P4 GA
- Stability tier promoted to `Beta` then `Stable` after 60-day soak.
- Case studies: ML eval workflow (Hugging Face?), backfill / debugging flows.
- Launch alongside Iceberg integration blog post.

---

## M3 — Streamline Anywhere

### M3 P2 Alpha (~12 weeks)
- **Multi-CRDT** — GSet, ORSet, MVRegister, PNCounter shipped + WASM merge plugin point.
  - `streamline/src/edge/sync/{gset.rs,orset.rs,mvregister.rs,pncounter.rs}`.
- **Merge observability** — per-topic `crdt_conflicts_total`, `crdt_merge_duration_ms`.
- **Mobile uniffi bindings** — generate Swift + Kotlin SDKs from Rust core.
  - `streamline-swift-sdk/`, `streamline-kotlin-sdk/` (already-empty repos in this org).

Exit criteria: 1 collaborative-editor design partner; 2-tab convergence latency p95 < 1s on 1000 ops/sec.

### M3 P3 Beta (~14 weeks)
- **Jepsen-class tests** — chaos suite with network partitions, clock skew.
  - `streamline/tests/jepsen/`.
- **Bandwidth-aware sync** — adaptive batching on cellular vs broadband.
- **WebTransport hardening** — fall-back ladder, telemetry on transport choice.

### M3 P4 GA
- Public examples: collaborative whiteboard, multi-user form, offline TODO.
- Design partners (each ships an app): 1 collab editor, 1 mobile-first SaaS.
- Launch venue: Chrome Dev Summit + JS Nation.

---

## Cross-Phase: Stability Tier Promotion Gate

For **every** phase exit, the following must be true (enforced by
`scripts/check_stability_tiers.sh` + manual review):

1. Documentation page exists in `streamline-docs/`.
2. Test coverage on the moonshot's modules ≥ 80%.
3. Performance gate met (numbers in the moonshot's exit criteria above).
4. ≥ 1 design-partner reference quote (P2+) or public reference (P3+).
5. ADR exists for any architectural change since the previous phase.
6. CHANGELOG entry under `streamline/CHANGELOG.md` for the release.
