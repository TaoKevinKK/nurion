# Lesson: Simplify `_process_and_ack`

**Date**: 2026-02-23
**File**: `engine/_internal/core/stage_worker.py`
**Type**: Refactor + Bug fix

---

## Problem

`_process_and_ack()` was ~170 lines with 5 phases all inlined. Two issues:

### 1. Structural: deep nesting obscures flow

The nack fallback (payload missing) was 28 lines nested inside the `for record in records` loop. Reading the method required mentally tracking two interleaved paths (happy path vs error recovery), making it hard to understand the overall flow at a glance.

### 2. Correctness: WebUI event count wrong for merge_upstream > 1

```python
# BUG: only records[0] was passed
event_puts = self._build_event_puts(
    record=records[0],   # <-- N-1 records silently dropped
    split_id=split_id,
    ...
)
```

When `merge_upstream=4` (e.g., Lance sink batching 4 upstream messages), only the first record got an ack event in the WebUI. The other 3 messages were acked at the queue level but invisible to the WebUI event stream, causing the dashboard's "processed messages" count to be systematically low.

---

## Root cause

The `_build_event_puts` method was written for the original single-record path. When merge support was added, the caller was updated to pass `records[0]` as a quick fix, but nobody updated the method signature to handle the full list.

**Pattern to watch for**: When a method takes a single item but the caller has a list, check if there's a semantic reason for picking just one, or if it's an incomplete migration.

---

## Solution

### Extract methods to flatten the 5-phase pipeline

| Method | Responsibility | Returns |
|---|---|---|
| `_parse_records(records)` | Loop-parse records, fetch payloads, nack on error | `Optional[_ParsedBatch]` (None = already nacked) |
| `_nack_all(msg_ids, claim_tokens, reason)` | Build nack events + call queue nack | `None` |
| `_merge_and_build_split(batch)` | Arrow concat + build Split/SplitPayload | `(split_id, Split, Optional[SplitPayload])` |
| `_serialize_outputs(result, split_id)` | Collect process_split results, store + serialize | `list[bytes]` |

### Introduce `_ParsedBatch` NamedTuple

```python
class _ParsedBatch(NamedTuple):
    msg_ids: list[str]
    claim_tokens: list[str]
    records: list[AnvilRecord]       # full list, needed by _build_event_puts
    tables: list[pa.Table]
    parent_split_ids: list[str]
    consumed_payload_keys: list[str]
    source_message: Optional[SourceQueueMessage]
    source_stage: Optional[str]
```

Why NamedTuple instead of dataclass: immutable, lightweight, unpacks naturally in `split_id, split, payload = self._merge_and_build_split(batch)`.

### Fix `_build_event_puts` to emit one event per record

```python
def _build_event_puts(self, records: list[AnvilRecord], ...):
    for i, record in enumerate(records):
        queue_wait_ms = max(0.0, (now - record.created_at) * 1000.0)
        event = {"event_type": "ack", "queue_wait_ms": queue_wait_ms, ...}
        puts[event_key(self.stage_id, ts_ns + i, record.msg_id)] = encode_json(event)

    # Plus one split event for the merged processing unit
    puts[split_key(split_id)] = encode_json(split_event)
```

Each record gets its own ack event with its own `queue_wait_ms` (they were enqueued at different times). One split event represents the merged processing unit.

---

## Result

- `_process_and_ack`: ~170 lines -> ~65 lines, reads as a 5-step sequence
- Nack recovery: extracted to `_nack_all()`, no deep nesting
- WebUI event count: now correct for all `merge_upstream` values
- All 331 unit tests pass, no interface changes

---

## Lessons for future changes

1. **When adding a "batch" mode to a "single" code path**: audit every downstream method that receives a single item. The compiler won't warn you if you pass `list[0]` instead of `list` -- this class of bugs is silent and only shows up as wrong counts.

2. **NamedTuple as inter-method data carrier**: when a method produces 5+ related values that another method consumes, a NamedTuple is a good fit. It's lighter than a dataclass, immutable by default, and the field names document the data flow.

3. **Nack/error recovery should be its own method**: error recovery logic mixed into the happy path creates cognitive load. Extract it so the main flow reads as a straight sequence of steps.

4. **WebUI events are the observability contract**: if a queue-level operation (ack) doesn't have a corresponding WebUI event, the dashboard lies. Treat event emission as part of the operation's correctness, not a nice-to-have.
