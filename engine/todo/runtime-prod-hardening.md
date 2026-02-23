# Runtime Production Hardening TODO

Track runtime hardening gaps for extreme production scenarios:
- 1B+ records
- 1000+ workers
- complex DAGs (fan-in/fan-out)
- fault recovery and interruption resume
- full-chain backpressure

> **Last Updated**: 2026-02-23
> **Scope**: `engine/_internal/core`, `engine/_internal/runtime`, `engine/_internal/queue`
> **References**: `../AGENTS.md`, `../../.claude/rules/architecture.md`, `../../.claude/rules/workqueue.md`

---

## ✅ Completed

### Recent fixes already landed

- [x] **Backpressure split-drop bug fixed in source production loop**
  - `SourceManager` now waits under backpressure without advancing iterator.
- [x] **Background source production failures now surface in StageMaster run-loop**
  - `raise_if_production_failed()` prevents silent hangs.
- [x] **Anti-Join payload key collision fixed**
  - Unique payload key per `AntiJoinSourceConfig` instance.
- [x] **Anti-Join switched to fail-fast for missing payload/key**
  - No silent filter bypass on runtime wiring errors.
- [x] **Consumed input payloads are eagerly deleted after successful ack**
  - Reduces unbounded payload-store growth versus end-of-job cleanup only.

---

## 🚧 In Progress

- [ ] *None currently (next iteration should start from P0 items below).*

---

## 📋 TODO

### High Priority (P0) — correctness and recoverability blockers

- [ ] **Hard-fail or implement multi-upstream fan-in semantics**
  - Current runtime still uses only the first upstream queue for non-source stages.
  - Complex DAGs can silently produce wrong results.
  - **Acceptance**:
    - Runtime rejects multi-upstream jobs explicitly, or
    - Runtime supports deterministic fan-in with full test coverage.

- [ ] **Payload durability contract for interruption recovery**
  - Avoid state where queue metadata survives but payload object does not.
  - Define and enforce production-safe config combinations (`workqueue_db_path`, `payload_store_uri`).
  - **Acceptance**:
    - Documented durability matrix (memory, local disk, object storage).
    - Recovery tests pass under node kill and process restart.

- [ ] **Poison-message handling with bounded retries + DLQ**
  - Current behavior can repeatedly nack/reclaim the same bad message.
  - **Acceptance**:
    - Per-message retry counter.
    - `max_retries` routing to DLQ queue.
    - DLQ payload includes split/message metadata + error snapshot.

- [ ] **Lease renewal for long-running split processing**
  - Static `claim_timeout_secs` is insufficient for long-tail batches.
  - **Acceptance**:
    - Worker heartbeat extends lease while actively processing.
    - No duplicate processing for long-running but healthy tasks.

- [ ] **Worker-level output backpressure**
  - Current backpressure is mostly source/planner-oriented.
  - Workers should avoid unlimited `ack_and_forward` pressure to slow downstream.
  - **Acceptance**:
    - StageWorker checks downstream pressure before forwarding.
    - Throughput stabilizes under slow sink scenarios without memory blowup.

### Medium Priority (P1) — scale and operability

- [ ] **Remove central payload-store metadata hotspot**
  - `RaySplitPayloadStore` uses a single actor for key->ref mapping.
  - 1B-scale traffic risks RPC bottleneck.
  - **Acceptance**:
    - Sharded metadata actor or lock-free mapping strategy.
    - Throughput benchmark at 1000+ workers with no single-actor saturation.

- [ ] **High-cardinality state/event control**
  - Current per-message event writes can explode state volume at very large scale.
  - **Acceptance**:
    - Sampling/aggregation modes for ack events.
    - Configurable retention + compaction policy.
    - WebUI still supports actionable debugging.

- [ ] **Delayed payload GC window (TTL-based cleanup)**
  - Eager delete reduces footprint but removes replay window for recent failures.
  - **Acceptance**:
    - Optional TTL cleanup mode with async GC.
    - Replay/recovery behavior documented and tested.

- [ ] **StageMaster failover model (remove control-plane SPOF)**
  - Job-level recovery is not implemented yet.
  - **Acceptance**:
    - Master state snapshot + restart/reattach flow.
    - Stage-level failover test passes without full job loss.

- [ ] **Fan-out atomicity across multiple downstream queues**
  - `ack_and_forward` currently guarantees atomicity for one downstream queue.
  - **Acceptance**:
    - Multi-destination exactly-once contract (or explicit at-least-once + reconciliation design).

- [ ] **Skew handling for join/shuffle-heavy workflows**
  - Heavy keys can hotspot single workers and cause repeated OOM/retry loops.
  - **Acceptance**:
    - Hot-key detection + adaptive repartitioning.
    - No single-worker runaway memory on skew benchmarks.

- [ ] **Streaming merge execution path**
  - Avoid large in-memory materialization when `merge_upstream > 1` on wide tables.
  - **Acceptance**:
    - Chunked merge mode with bounded memory.
    - Performance regression tests for wide payloads.

### Low Priority (P2) — optimization and ecosystem quality

- [ ] **Autoscaler profile for 1000+ worker ramps**
  - Current step/cooldown defaults are conservative for burst traffic.

- [ ] **Per-worker time-breakdown observability**
  - Separate wait/claim/decode/process/forward/ack timing in metrics.

- [ ] **Chaos and long-soak reliability suite**
  - Continuous validation for lease recovery, payload loss, broker restarts, and skew spikes.

---

## 🔄 Design Changes

Differences between current runtime behavior and desired production architecture:

- Multi-upstream workflows are not fully supported end-to-end yet.
- Data durability assumptions are not enforced by config validation.
- Retry/DLQ policy is not first-class in runtime contracts.
- Backpressure needs to be enforced at both source and worker forwarding layers.

Design-doc follow-up required once P0 decisions are finalized:
- add explicit delivery semantics matrix (exactly-once / at-least-once boundaries)
- add durability contract matrix
- add recovery playbook per failure mode

---

## 📝 Next Iteration Suggestions

1. **Iteration A (P0 safety rails)**  
   Multi-upstream hard-fail, DLQ, lease renewal, payload durability guardrails.

2. **Iteration B (P1 scale hardening)**  
   Payload-store hotspot removal, event-cardinality control, worker-level backpressure.

3. **Iteration C (P1/P2 reliability + performance)**  
   Master failover, skew mitigation, streaming merge, soak and chaos automation.

---

## Proposed Tracking Labels

- runtime/p0-correctness
- runtime/p0-recovery
- runtime/p1-scale
- runtime/p1-observability
- runtime/p2-optimization
