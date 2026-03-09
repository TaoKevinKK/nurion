# Runtime Production Hardening TODO

Track runtime hardening gaps for production workloads at scale.

> **Last Updated**: 2026-03-06
> **Scope**: `engine/_internal/core`, `engine/_internal/runtime`, `engine/_internal/queue`
> **Strategic context**: See `roadmap.md` for business-value-driven prioritization.

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
  - **Tracked in**: `roadmap.md` §1.2

- [ ] **Worker-level output backpressure**
  - Workers do unlimited `ack_and_forward` to slow downstream, risking memory blowup
  - StageWorker should check downstream queue depth before forwarding
  - **Acceptance**: Throughput stabilizes under slow-sink scenarios without OOM

### Medium Priority — Scale and Operability

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

### Deprioritized (Revisit When Needed)

These items were previously P0 but have been deprioritized based on business value analysis.
See `roadmap.md` §Deprioritized for reasoning.

- [ ] ~~**Poison-message handling with DLQ**~~ — Fix-and-rerun is the right pattern for batch processing. DLQ adds complexity without solving root cause. Revisit if we add streaming/online sources.
- [ ] ~~**Lease renewal for long-running splits**~~ — Use larger `claim_timeout_secs`. Heartbeat protocol complexity not justified without concrete evidence of long-tail issues at scale.
- [ ] ~~**Payload durability contract**~~ — Documentation task, not a code feature. Write when production deployment patterns are established.
- [ ] ~~**StageMaster failover**~~ — Job re-run is sufficient today. Revisit for multi-hour jobs or when iterative execution (roadmap §3.1) makes job restart expensive.

---

## Design Follow-ups

Required once fan-in and shuffle are implemented:

- [ ] Delivery semantics matrix (exactly-once vs at-least-once boundaries per operation)
- [ ] Recovery playbook per failure mode (worker crash, broker restart, node loss)
