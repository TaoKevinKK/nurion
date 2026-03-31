# Nurion Strategic Roadmap

Prioritized by **business value**, not technical elegance. Each item answers:
"What user scenario does this unlock that we can't serve today?"

> **Last Updated**: 2026-03-24
> **Positioning**: Distributed data processing engine with first-class LLM inference.
> **Competitive benchmark**: Ray Data, Spark.
> **Future direction**: RL training loops, Agent evaluation pipelines.

---

## Guiding Principles

1. **Differentiate, don't duplicate** — Nurion's moat is unified data processing + LLM serving. Don't compete with Spark on SQL; win on AI-native workflows.
2. **Business value first** — Every feature must unlock a concrete user scenario. No speculative hardening.
3. **Table-stakes before moonshots** — Missing compute primitives (shuffle, fan-in) block users today. Fix these before building RL loops.
4. **Design docs are cheap, code is expensive** — If a design doc exists, validate the design against current architecture before implementing.

---

## Phase 1: Compute Engine Table Stakes

> Unlock: general data processing, dedup routing, groupby, distributed join.
> Without these, users hit walls that Ray Data / Spark handle trivially.

### 1.1 Shuffle Partition Routing + QueueGroup ✅

- **Status**: **Completed** (PR #59 + QueueGroup, 2026-03-06)
- **Implemented**:
  - Shuffle routing: `StageWorker._shuffle_output_and_ack()` routes by `__target_partition`
  - QueueGroup: first-class partition group abstraction in anvil-rs
  - `AckAndScatter`: atomic ack upstream + push to N partition queues (exactly-once)
  - `ClaimFromGroup`: O(1) broker-directed claim with round-robin + work-stealing
  - `IsGroupFinished` / `MarkGroupFinished`: single-RPC completion checking
  - Phase 5 cleanup: removed `partition_queue_names` from engine data structures, deleted legacy `_run_partition_claim_loop`, eliminated at-least-once fallback
- **Unlocks**: Dedup routing, GroupBy aggregation, distributed Join, any repartition
- **Design doc**: `../design/queue-group-and-skew-handling.md`

### 1.2 Multi-Upstream Fan-in

- **Status**: DAG model supports it (`Job.add_stage(upstream_stages=["a","b"])`), runtime hardcoded to first upstream
- **Gap**: `ray_runner.py:246` — `upstream_id = upstream_ids[0]` with a TODO comment
- **Unlocks**: Complex DAGs (data enrichment, multi-source merge mid-pipeline), prerequisite for RL (merge experience + reward streams)
- **Scope**: `stage.py` (StageRuntime: single → multi queue), `stage_worker.py` (multi-queue claim), `ray_runner.py` (multi-upstream wiring)
- **Design doc**: `../design/multi-upstream-join.md` (full design exists, validate before implementing)

---

## Phase 2: Differentiation — Seamless LLM Integration

> Unlock: declarative model deployment, zero-boilerplate inference pipelines.
> This is what Ray Data + vLLM can't do. Make it Nurion's killer feature.

### 2.1 Declarative Serve ↔ Pipeline Integration

- **Status**: Users must manually `create_manager()` → `deploy_model()` → pass `registry` handle → `job.run()` → cleanup
- **Gap**: No auto-deployment hooks in `RayJobRunner`; `ExternalLLMOperatorConfig.registry` requires manual injection
- **Target UX**:
  ```python
  # User declares model need in Stage config — engine handles the rest
  Stage(
      operator_config=ExternalLLMOperatorConfig(model="Qwen/Qwen3-VL-32B", ...),
      model_config=ModelConfig(tensor_parallel_size=4, min_workers=2),
  )
  job.run()  # auto: deploy → execute → cleanup
  ```
- **Unlocks**: Frictionless batch inference, synthetic data generation, RLHF reward scoring — all without infrastructure glue code
- **Scope**: `ray_runner.py` (job-level model lifecycle), `ExternalLLMOperatorConfig` (auto-registry discovery), `ModelServiceManager` (job-scoped deployment)
- **Design doc**: Extend `../design/llm-inference.md`

### 2.2 Public API Export for LLM Operators ✅

- **Status**: **Completed** (`d5902c9`, 2026-03)
- **Implemented**: `nurion/__init__.py` exports `EmbeddedLLMOperator`, `EmbeddedLLMOperatorConfig`, `ExternalLLMOperator`, `ExternalLLMOperatorConfig`

### 2.3 Token-Length-Based Model Routing ✅

- **Status**: **Completed** (`d5902c9`, 2026-03)
- **Implemented**:
  - `RoutedChatCompletionsClient` with `estimate_tokens()`, `pick_model()`, `next_model()`, context-length fallback
  - `ModelRoutingConfig` dataclass for routing configuration

---

## Phase 3: RL and Agent Enablement

> Unlock: training loops, experience collection, iterative convergence, agent evaluation.
> These are the architectural foundations for RL/Agent workloads.

### 3.1 Iterative Execution (Loop Stages)

- **Status**: Design exists in `../design/work-queue-redesign.md` (CCIterateMaster); `@master_callable` infrastructure implemented
- **Gap**: `master_class` field not wired in `OperatorConfig`; queue loopback logic not in `ray_runner.py`
- **Unlocks**:
  - RL training loops: collect experience → compute reward → update policy → repeat
  - Agent eval loops: infer → act → observe → continue
  - Graph algorithms: Connected Components, PageRank (multi-round convergence)
  - Active learning: label → train → sample uncertain → re-label
- **Scope**: `operator.py` (master_class field), `ray_runner.py` (iteration loop + queue loopback), `stage_master.py` (convergence check)
- **Design doc**: `../design/work-queue-redesign.md` §CC Redesign (validate before implementing)

### 3.2 Dynamic / Conditional DAG

- **Status**: No design, no code. DAG is fully static (defined before `job.run()`)
- **Gap**: Agent workflows need runtime branching (if model says X, route to stage A; otherwise stage B)
- **Unlocks**: Agent tool-use pipelines, conditional data processing, adaptive workflows
- **Scope**: Large — requires rethinking `Job`/`Stage` model. Defer until iterative execution is proven.
- **Design doc**: Needed before implementation

### 3.3 Streaming / Online Sources

- **Status**: Internal execution is streaming-style (pull-based + backpressure), but all sources are bounded
- **Gap**: No Kafka/Kinesis/unbounded source; framework self-identifies as "offline/batch" in design docs
- **Unlocks**: Online RL experience collection, real-time agent feedback, continuous data ingestion
- **Scope**: Large — source contract changes, completion semantics rethink. Defer until after Phase 2.
- **Design doc**: Needed before implementation

---

## Deprioritized (Low Business Value)

Items that look technically appealing but don't unlock meaningful user scenarios today:

| Item | Why deprioritized |
|------|-------------------|
| **DLQ (Dead Letter Queue)** | In data processing, the right response to poison messages is fix-and-rerun, not route-to-DLQ. DLQ adds complexity without solving root cause. Revisit only if we move to streaming/online mode. |
| **Lease renewal / heartbeat** | Set a larger `claim_timeout_secs`. The complexity of heartbeat protocol isn't justified until we have concrete evidence of long-tail processing issues at scale. |
| **StageMaster failover** | Job-level re-run is sufficient today. Master failover is complex (state snapshot + reattach) and only matters for multi-hour jobs, which are rare. |
| **Job-level checkpoint** | Same reasoning as master failover. Re-run is cheaper than checkpoint infrastructure for current job durations. |
| **DiagnosticAgent** | Cool but not core. Depends on WebUI v2 write path. Build after the engine is feature-complete. |
| **Payload durability contract** | Documentation task, not a feature. Write it when we have production deployment patterns to document. |

---

## Relationship to Other TODO Files

| File | Scope |
|------|-------|
| `04-runtime-prod-hardening.md` | Runtime correctness, scale, and operability backlog |
| `02-serve.md` | Serve module (GPU scheduling, model routing, inference workers) |
| `03-dedup.md` | Dedup operators, Union-Find service, MinHash pipeline |
| `README.md` | Directory structure and conventions |

This roadmap is the **strategic layer**; per-module TODOs track **tactical items**.
