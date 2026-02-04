# Deprecated Design Docs

This directory contains legacy design documents that no longer reflect the
current WorkQueue-based runtime. They are preserved for historical reference.

## Lessons Learned

1. Avoid partition-coupled execution
   - Worker ↔ partition binding created skew and rebalance complexity.
   - Work-stealing on a single queue yields simpler, more elastic scaling.

2. Prefer claim-based progress over offset tracking
   - Offset/commit logic was fragile and hard to reason about.
   - Claim/ack with server-managed state is simpler and more reliable.

3. Backpressure and autoscaling should be job-level
   - Per-stage controllers diverged and produced conflicting signals.
   - A single job-level controller using queue stats is clearer and cheaper.

4. Single source of truth for metrics
   - Worker/master counters drifted and confused debugging.
   - Queue stats are authoritative for backlog and in-flight visibility.

5. Deprecate aggressively, keep docs honest
   - Leave explicit pointers to current designs.
   - Remove compatibility paths to keep the codebase clean.

## Moved Documents

- `architecture.md`
- `partition-backpressure-improvements.md`
- `queue-issues-to-resolve.md`
- `tansu-pyo3-binding.md`
