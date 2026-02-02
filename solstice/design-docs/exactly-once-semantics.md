# Exactly-Once Semantics Design

> ⚠️ **DEPRECATED** - This document describes the offset-based exactly-once design for the old Tansu/Kafka partition model.
> With the new WorkQueue (single-queue multi-consumer) model introduced in PR #35, this design is no longer applicable.
> See `workqueue-semantics.md` for the new design.
>
> _Deprecated: 2026-02-02_

_Design document - January 2026_

---

## Implementation Status

| Component | Status | Notes |
|-----------|--------|-------|
| **SemanticGuarantee Enum** | ✅ Complete | `AT_LEAST_ONCE` (default), `EXACTLY_ONCE` |
| **Offset-based Deduplication** | ✅ Complete | `last_offset` tracking in `Operator` |
| **State Store Integration** | ✅ Complete | SlateDB for persistent offset storage |
| **Config Propagation** | ✅ Complete | `JobConfig` → `StageConfig` → `StageWorker` → `Operator` |
| **Fault Injection Framework** | ✅ Complete | `FaultInjector` for testing |
| **Integration Tests** | ✅ Complete | Config propagation + fault injection tests |

---

## Problem Statement

### Core Challenge

In distributed stream processing, failures can occur at any point:
1. **After processing, before commit**: Message processed but offset not saved → duplicate on retry
2. **After commit, before downstream**: Offset saved but downstream didn't receive → data loss
3. **Partial batch**: Some messages in batch committed, others not → inconsistent state

### Requirements

1. **No data loss**: Every message must be processed at least once
2. **No duplicates**: In exactly-once mode, each message processed exactly once
3. **Recovery**: After crash, resume from last committed position
4. **Performance**: Minimal overhead for at-least-once workloads

---

## Design Decisions

### Decision 1: At-Least-Once + Idempotent Sinks

**Chosen approach**: At-least-once delivery with downstream deduplication.

**Rationale**:
- True exactly-once across distributed systems requires 2PC or similar, which is complex and slow
- Most real sinks can be made idempotent (database upserts, object storage with deterministic keys)
- Simpler implementation, better performance, easier to reason about

**Trade-off**:
- Requires idempotent sink implementations
- Duplicates may be sent to downstream (but deduplicated there)

### Decision 2: Offset-based Deduplication

**Chosen approach**: Track `last_offset` per partition, skip if `offset <= last_offset`.

**Rationale**:
- Kafka-style sequential consumption within partitions
- Single integer comparison vs. set membership (O(1) vs. O(n) space)
- No need for split_id tracking since split_id is derived from offset

**Alternative considered**: `seen_splits: Set[str]` - rejected due to:
- Unbounded memory growth
- Complex serialization for state store
- Redundant with offset tracking for sequential consumption

### Decision 3: Atomic Offset + State Updates

**Chosen approach**: Save offset and business state in single `put_batch` call.

**Rationale**:
- SlateDB `put_batch` is atomic
- Ensures offset and state are always consistent
- On recovery, either both are restored or neither

```python
def save_state(self, offset: int, state_updates: List[Tuple[bytes, bytes]]):
    updates = [(OFFSET_KEY, str(offset).encode())]
    if state_updates:
        updates.extend(state_updates)
    self._state_store.put_batch(updates)  # Atomic
```

### Decision 4: Job-Level Semantic Guarantee

**Chosen approach**: Configure at job level, not per-stage.

**Rationale**:
- Simpler mental model
- Consistent behavior across pipeline
- Avoid mixed-mode complexity

```python
job = Job(
    job_id="my_job",
    config=JobConfig(
        semantic_guarantee=SemanticGuarantee.EXACTLY_ONCE,
    ),
)
```

---

## Architecture

### Config Propagation Chain

```
JobConfig.semantic_guarantee
    │
    ▼
RayJobRunner._build_stage_config()
    │
    ▼
StageConfig.semantic_guarantee
    │
    ▼
WorkerManager._spawn_worker()
    │
    ▼
StageWorker.__init__(semantic_guarantee=...)
    │
    ▼
Operator.config.semantic_guarantee
```

### Processing Flow

```
┌─────────────────────────────────────────────────────────────┐
│                      StageWorker                            │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              _process_partition loop                 │   │
│  │                                                      │   │
│  │  1. Fetch message from upstream queue               │   │
│  │              ▼                                       │   │
│  │  2. Check: op.is_duplicate(offset)?                 │   │
│  │     - If EXACTLY_ONCE: offset <= last_offset → skip │   │
│  │     - If AT_LEAST_ONCE: always False                │   │
│  │              ▼                                       │   │
│  │  3. Process: op.process_split(split, payload)       │   │
│  │              ▼                                       │   │
│  │  4. Produce to downstream queue                     │   │
│  │              ▼                                       │   │
│  │  5. [FAULT_BEFORE_MARK_PROCESSED] ← fault hook      │   │
│  │              ▼                                       │   │
│  │  6. Mark processed: op.mark_processed(offset)       │   │
│  │     - Updates last_offset                           │   │
│  │     - Saves to state store (atomic with state)      │   │
│  │              ▼                                       │   │
│  │  7. Commit offset to upstream queue                 │   │
│  │                                                      │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### State Store Layout

```
Partition 0 State Store (SlateDB):
┌────────────────────────────────────────┐
│ Key                    │ Value         │
├────────────────────────┼───────────────┤
│ _solstice_offset       │ "42"          │  ← Last processed offset
│ user_key_1             │ <user_data>   │  ← Business state
│ user_key_2             │ <user_data>   │
└────────────────────────┴───────────────┘
```

---

## Recovery Behavior

### Scenario: Crash After Process, Before Mark

```
Timeline:
  t1: Process message offset=5 ✓
  t2: Produce to downstream ✓
  t3: [CRASH] ← Before mark_processed
  
State Store: last_offset = 4

Recovery:
  t4: Operator.init_from_state_store() → last_offset = 4
  t5: Fetch message offset=5
  t6: is_duplicate(5)? → 5 <= 4? → False → Process again
  t7: Downstream receives duplicate of offset=5
  t8: Idempotent sink deduplicates
  t9: mark_processed(5) ✓
  
Result: Message 5 processed twice, but idempotent sink ensures exactly-once output
```

### Scenario: Crash After Mark

```
Timeline:
  t1: Process message offset=5 ✓
  t2: Produce to downstream ✓
  t3: mark_processed(5) ✓ → last_offset = 5
  t4: [CRASH] ← Before queue commit
  
State Store: last_offset = 5

Recovery:
  t5: Operator.init_from_state_store() → last_offset = 5
  t6: Fetch message offset=5 (queue didn't commit, re-delivers)
  t7: is_duplicate(5)? → 5 <= 5? → True → Skip
  t8: Continue with offset=6
  
Result: No duplicate processing
```

---

## API Reference

### SemanticGuarantee Enum

```python
class SemanticGuarantee(Enum):
    AT_LEAST_ONCE = "at_least_once"  # Default, no dedup overhead
    EXACTLY_ONCE = "exactly_once"     # Offset-based deduplication
```

### JobConfig

```python
@dataclass
class JobConfig:
    semantic_guarantee: SemanticGuarantee = SemanticGuarantee.AT_LEAST_ONCE
    # ... other fields
```

### Operator Methods

```python
class Operator:
    # Check if offset was already processed
    def is_duplicate(self, offset: int) -> bool:
        if self.semantic_guarantee == SemanticGuarantee.AT_LEAST_ONCE:
            return False  # No dedup in at-least-once mode
        return offset <= self.last_offset
    
    # Mark offset as processed (atomic with state updates)
    def mark_processed(self, offset: int, state_updates: List[Tuple[bytes, bytes]] = None):
        self.save_state(offset, state_updates)
        self.processed_count += 1
    
    # Recover state from store
    def init_from_state_store(self):
        if self._state_store:
            offset_bytes = self._state_store.get(OFFSET_KEY)
            if offset_bytes:
                self.last_offset = int(offset_bytes.decode())
```

---

## Testing Strategy

### Unit Tests (Operator Level)

```python
# Test offset-based deduplication
def test_offset_dedup():
    op = create_operator(semantic_guarantee=EXACTLY_ONCE)
    op.last_offset = 5
    assert op.is_duplicate(5) == True   # Already processed
    assert op.is_duplicate(4) == True   # Earlier offset
    assert op.is_duplicate(6) == False  # New offset

# Test state recovery
def test_recovery():
    op1 = create_operator(state_store_path=tmpdir)
    op1.mark_processed(5)
    op1.close()
    
    op2 = create_operator(state_store_path=tmpdir)
    op2.init_from_state_store()
    assert op2.last_offset == 5
```

### Fault Injection Tests

```python
# Test crash before mark_processed
def test_fault_before_mark():
    injector = FaultInjector(enabled=True)
    injector.fail_after(FAULT_BEFORE_MARK_PROCESSED, count=5)
    set_fault_injector(injector)
    
    # Process messages, crash on 6th mark_processed
    # Verify: message 5 written to sink but last_offset = 4
    # Recovery: message 5 reprocessed, idempotent sink deduplicates
    # Final: exactly 10 unique values
```

### Config Propagation Tests

```python
# Verify semantic_guarantee flows through config chain
def test_config_propagation():
    job = Job(config=JobConfig(semantic_guarantee=EXACTLY_ONCE))
    job.add_stage(...)
    
    runner = job.create_ray_runner()
    stage_config = runner._build_stage_config(stage)
    
    assert stage_config.semantic_guarantee == EXACTLY_ONCE
```

---

## Performance Considerations

### AT_LEAST_ONCE Mode

- No dedup overhead
- `is_duplicate()` always returns `False`
- State store writes only for business state (if any)
- Best for idempotent workloads or when duplicates are acceptable

### EXACTLY_ONCE Mode

- Single integer comparison per message
- State store write per message (offset + state atomic)
- ~10-20% overhead vs. at-least-once (depends on state store latency)

### Optimization Opportunities

1. **Batch offset commits**: Commit every N messages instead of every message
2. **Async state store writes**: Fire-and-forget with periodic sync
3. **In-memory dedup cache**: LRU cache for recent offsets (reduces state store reads)

---

## Files Reference

| Purpose | File |
|---------|------|
| SemanticGuarantee enum | `solstice/core/operator.py` |
| Operator dedup logic | `solstice/core/operator.py` |
| StageConfig | `solstice/core/stage_config.py` |
| Config propagation | `solstice/runtime/ray_runner.py` |
| Worker creation | `solstice/core/managers/worker_manager.py` |
| Processing loop | `solstice/core/stage_worker.py` |
| Fault injection | `solstice/testing/fault_injection.py` |
| Integration tests | `tests/test_exactly_once_integration.py` |

---

## Future Work

1. **Transactional sinks**: Integrate with sinks that support transactions (e.g., Kafka transactions)
2. **Checkpoint barriers**: Aligned checkpoints across stages (Flink-style)
3. **Exactly-once counters**: Track duplicates sent vs. deduplicated for observability
4. **State compaction**: Periodic compaction of state store to reduce storage

---

_Last updated: January 2026_
