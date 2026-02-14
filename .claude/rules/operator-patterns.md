---
globs:
  - engine/_internal/operators/**
  - engine/_internal/core/operator.py
  - engine/_internal/core/stage_worker.py
---

# Operator Development

- Operators are config-driven, stateless: `__init__` takes `OperatorConfig` + `OperatorRuntime` only
- Runtime context (job_id, stage_id, worker_id) lives in `OperatorRuntime`
- No `set_*()` methods — everything comes from config
- New operator: inherit `Operator`, implement `process_split()`, set `Config.operator_class`
- New source: inherit `SourceOperator`, implement `plan_splits()`
- No `_total_xxx_count` stats counters; minimize `self._` state
- Export new operators in `nurion/__init__.py`
