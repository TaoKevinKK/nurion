# Runtime Production Hardening TODO

Track runtime hardening gaps for production workloads at scale.

> **Last Updated**: 2026-03-24
> **Scope**: `engine/_internal/core`, `engine/_internal/runtime`, `engine/_internal/queue`
> **Strategic context**: See `01-roadmap.md` for business-value-driven prioritization.

---

## Completed

- [x] **Backpressure split-drop bug** — `SourceManager` now waits under backpressure without advancing iterator
- [x] **Background source production failures** — `raise_if_production_failed()` prevents silent hangs
- [x] **Anti-Join payload key collision** — Unique payload key per `AntiJoinSourceConfig` instance
- [x] **Anti-Join fail-fast for missing payload/key** — No silent filter bypass on runtime wiring errors
- [x] **Consumed input payloads eagerly deleted** — Reduces unbounded payload-store growth

---

## TODO

### High Priority — Unlocks User Scenarios

- [x] **Shuffle partition routing + QueueGroup** ✅ (PR #59 + QueueGroup, 2026-03-06)
  - Shuffle routing + QueueGroup abstraction with exactly-once `ack_and_scatter`
  - O(1) `claim_from_group` with work-stealing, single-RPC completion checking
  - Phase 5 cleanup: removed legacy partition paths from engine

- [ ] **Multi-upstream fan-in** ← _blocks complex DAGs, future RL pipelines_
  - `ray_runner.py:246` hardcodes `upstream_ids[0]`; multi-upstream silently uses first only
  - Validate design in `../design/multi-upstream-join.md` before implementing
  - **Tracked in**: `01-roadmap.md` §1.2

- [ ] **Worker-level output backpressure**
  - Workers do unlimited `ack_and_forward` to slow downstream, risking memory blowup
  - StageWorker should check downstream queue depth before forwarding
  - **Acceptance**: Throughput stabilizes under slow-sink scenarios without OOM

### Medium Priority — Scale and Operability

- [ ] **Autoscaler manual override API**
  - Design: `../design/dynamic-worker-scaling.md` — claimed "Complete" but not implemented
  - `set_stage_workers()`, `freeze_stage()`, `unfreeze_stage()`, `pause_autoscaling()`, `resume_autoscaling()`
  - Also missing: `AutoscaleConfig.fixed_workers` and `frozen_stages` fields
  - **Acceptance**: Operators can pin a stage to N workers or freeze scaling during debugging

- [ ] **Resource-aware scaling (proactive)**
  - Design: `../design/dynamic-worker-scaling.md` — `can_spawn_worker()` via `ray.available_resources()`
  - Current: reactive only (try to create worker, cancel if it fails to become ready)
  - **Acceptance**: Autoscaler skips scale-up when cluster resources are insufficient

- [ ] **Remove central payload-store metadata hotspot**
  - `RaySplitPayloadStore` uses a single actor for key→ref mapping
  - At 1000+ workers, this becomes an RPC bottleneck
  - **Acceptance**: Sharded metadata or lock-free strategy; no single-actor saturation

- [ ] **High-cardinality state/event control**
  - Per-message event writes can explode state volume at very large scale
  - **Acceptance**: Sampling/aggregation modes for ack events; configurable retention

- [x] **Skew handling: work-stealing** ✅ (2026-03-06)
  - `ClaimFromGroup` with `allow_steal` + round-robin probe across unassigned partitions
  - `GetGroupStats` with skew_ratio, hot_partition detection
  - **Remaining**: Salted two-phase aggregation for GroupBy/Join (DAG layer, future)

- [ ] **Fan-out atomicity across multiple downstream queues**
  - Now solved for partition scatter via `ack_and_scatter`
  - Remaining: non-partition multi-destination (e.g., broadcast to multiple stages)

- [ ] **Streaming merge execution path**
  - Large in-memory materialization when `merge_upstream > 1` on wide tables
  - **Acceptance**: Chunked merge mode with bounded memory

### Low Priority — Optimization

- [ ] **Autoscaler profile for 1000+ worker ramps**
  - Current step/cooldown defaults are conservative for burst traffic

- [ ] **Per-worker time-breakdown observability**
  - Separate wait/claim/decode/process/forward/ack timing in metrics

- [ ] **Chaos and long-soak reliability suite**
  - Continuous validation for lease recovery, payload loss, broker restarts, skew spikes

- [ ] **NvmeSpaceManager with watermark-based eviction**
  - Design: `../design/nvme-payload-store.md` — three-watermark (HIGH/CRITICAL/FATAL)
  - Current: ENOSPC fallback only, no proactive eviction
  - Low priority: current fallback to S3 on ENOSPC works for production use cases

- [ ] **NVMe Phase 2: Locality-aware claim**
  - Design: `../design/nvme-payload-store.md` Phase 2
  - `origin_node` metadata in queue messages, broker-side node-aware partition assignment
  - Reduces cross-node Arrow Flight reads

- [x] **Clean up dead code: `FAULT_BEFORE_MARK_PROCESSED`** ✅ (PR #69, 2026-03-25)
  - Removed dead fault constants, checkpoint methods, `queue/backend.py`, `state/` module

### Deprioritized (Revisit When Needed)

These items were previously P0 but have been deprioritized based on business value analysis.
See `01-roadmap.md` §Deprioritized for reasoning.

- [ ] ~~**Poison-message handling with DLQ**~~ — Fix-and-rerun is the right pattern for batch processing. DLQ adds complexity without solving root cause. Revisit if we add streaming/online sources.
- [ ] ~~**Lease renewal for long-running splits**~~ — Use larger `claim_timeout_secs`. Heartbeat protocol complexity not justified without concrete evidence of long-tail issues at scale.
- [ ] ~~**Payload durability contract**~~ — Documentation task, not a code feature. Write when production deployment patterns are established.
- [ ] ~~**StageMaster failover**~~ — Job re-run is sufficient today. Revisit for multi-hour jobs or when iterative execution (roadmap §3.1) makes job restart expensive.

### WorkQueue — Designed but Not Implemented

From `../design/work-queue-redesign.md` and `../design/workqueue-semantics.md`:

- [ ] **`push_with_dedup`** (exactly-once source dedup)
  - Design: `work-queue-redesign.md` §4.7, `workqueue-semantics.md` Phase 2
  - Business-key-based deduplication on push to prevent duplicate source messages
  - **Acceptance**: Duplicate push with same business key is a no-op

- [ ] **Queue-depth backpressure enforcement**
  - Design: `work-queue-redesign.md` §5 Issue #5
  - `max_queue_depth` field exists in config but is `#[allow(dead_code)]` and never enforced
  - Related to "Worker-level output backpressure" above

- [ ] **State TTL / cleanup**
  - Design: `workqueue-semantics.md` Open Question 8.1
  - No state TTL or job-scoped state cleanup implemented
  - State keys accumulate across jobs sharing a broker

- [ ] **`claim_with_state`** (combined RPC)
  - Design: `work-queue-redesign.md` §4.6
  - Fetch state keys inline during claim (one RPC instead of two)
  - Optimization only; current separate RPCs work

- [ ] **WorkQueue observability metrics**
  - Design: `workqueue-semantics.md` Phase 4
  - Dedup hit/miss counters, recovery event counters, state size metrics
  - No metrics export from broker currently

- [ ] **NackReason-aware handling**
  - `NackReason` enum defined in proto (PAYLOAD_MISSING, SKIP) but ignored in Rust service
  - Nack always re-enqueues regardless of reason

---

## Design Follow-ups

Required once fan-in and shuffle are implemented:

- [ ] Delivery semantics matrix (exactly-once vs at-least-once boundaries per operation)
- [ ] Recovery playbook per failure mode (worker crash, broker restart, node loss)
