---
globs:
  - lib/workqueue-rs/**
  - engine/_internal/queue/**
---

# WorkQueue

- All hot-path ops (claim, ack, push, stats) MUST be O(1)
- Scans only for background tasks (GC every 60s, recovery every 10s)
- Key schema: `meta:{queue}`, `pending:{queue}:{seq}`, `msg:{queue}:{msg_id}`, `claimed:{queue}:{msg_id}`
- Use counters in QueueMeta instead of scanning to count
- Batch with WriteBatch for atomicity
- See `lib/workqueue-rs/AGENTS.md` for full constraints
