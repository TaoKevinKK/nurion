# Lessons

Refactoring history and decision records. Each file documents a concrete code change: what the problem was, what we tried, and what we learned.

Read these before proposing similar changes to learn from past decisions.

| Lesson | Scope | Date |
|---|---|---|
| [stage-worker-process-and-ack](./stage-worker-process-and-ack.md) | `stage_worker.py` refactor + bug fix | 2026-02-23 |
| [stage-completion-data-loss-postmortem](./stage-completion-data-loss-postmortem.md) | Indexed lessons L1–L13 from completion-race investigation | 2026-04 |
| [shuffle-abstraction-leak](./shuffle-abstraction-leak.md) | Why shuffle leaked into core | 2026-04 |
| [hash-vs-range-partition-skew](./hash-vs-range-partition-skew.md) | Partition skew analysis | 2026-04 |
| [anvil-publish-commit-race](./anvil-publish-commit-race.md) | Publish-commit watermark fix (PR #84) | 2026-04-24 |
| [anvil-counter-mutex-evolution](./anvil-counter-mutex-evolution.md) | Seven atomics → one mutex (PRs #82–#88) | 2026-04-25 |
