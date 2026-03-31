---
globs:
  - engine/_internal/operators/**
  - engine/_internal/core/operator.py
  - engine/_internal/core/stage_worker.py
---

# Operator Development

> Canonical patterns for writing operators. All rules enforced in code review.
> **Update this file when operator contract changes.**

---

## Core Rules

- Operators are config-driven, stateless: `__init__` takes `OperatorConfig` + `OperatorRuntime` only
- Runtime context (job_id, stage_id, worker_id, broker_endpoint) lives in `OperatorRuntime`
- No `set_*()` methods — everything comes from config or runtime
- New operator: use `@operator(Config)` decorator (sets `Config.operator_class` bidirectionally)
- New source: inherit `SourceOperator`, implement `plan_splits()`; config must implement `create_source()`
- No `_total_xxx_count` stats counters; minimize `self._` state
- Export new operators in `nurion/__init__.py`

---

## Minimal Operator Template

```python
from dataclasses import dataclass
from typing import Optional
from _internal.core.operator import Operator, OperatorConfig, OperatorRuntime, operator
from _internal.core.models import Split, SplitPayload

@dataclass
class MyOperatorConfig(OperatorConfig):
    # All user settings — immutable dataclass fields
    threshold: float = 0.5
    batch_size: int = 32

    # Optional: merge N upstream messages before process_split() (default=1)
    def get_merge_upstream(self) -> int:
        return self.batch_size

@operator(MyOperatorConfig)   # binds Config.operator_class ↔ Operator.config_class
class MyOperator(Operator):
    def __init__(self, config: MyOperatorConfig, runtime: OperatorRuntime):
        super().__init__(config, runtime)
        # self._config, self._runtime, self.logger set by super()
        # Only add immutable derived values here, not mutable state

    def process_split(self, split: Split, payload: Optional[SplitPayload] = None) -> ...:
        # Return None         → drop (filter)
        # Return SplitPayload → 1:1 map
        # yield SplitPayload  → 1:N explode
        ...
```

---

## process_split Return Types

```python
# All valid return forms:
def process_split(self, split, payload): return payload             # sync single
def process_split(self, split, payload): yield p1; yield p2        # sync generator
def process_split(self, split, payload): return None               # drop (filter)
async def process_split(self, split, payload): return payload       # async single
async def process_split(self, split, payload): yield p1; yield p2  # async generator
# Also: return RawOutputBytes → raw bytes to output queue (sink commit use case)
```

---

## Persistent State (Anvil model)

State is stored in Anvil (not local files). Access via `runtime.broker_endpoint`:
- `state_get(namespace, key)` / `state_put(namespace, key, value)`
- Atomic with ack: `ack_and_forward` commits ack + state update in one WriteBatch
- Do NOT use local files, instance variables, or external DBs for cross-split state

---

## Master-Callable Methods

```python
from _internal.core.operator import master_callable

class MyOperator(Operator):
    @master_callable   # StageMaster can call via worker.invoke_operator("get_stats")
    def get_stats(self) -> dict: ...

    @master_callable
    def reset_for_iteration(self, iteration: int) -> None: ...
```

Use `@master_callable` to expose methods to StageMaster without modifying `StageWorker`.

---

## Anti-Patterns (rejected in review)

```python
# ❌ mutable state counter
def __init__(self, config, runtime):
    self._total_records = 0

# ❌ set_*() injection method
def set_model(self, model): ...

# ❌ lambda/callable in config (not Ray-serializable)
@dataclass
class BadConfig(OperatorConfig):
    transform_fn: Callable = lambda x: x

# ✓ All config in OperatorConfig, read in process_split
def process_split(self, split, payload):
    threshold = self._config.threshold
    return payload if meets_threshold(payload, threshold) else None
```

---

## Export Checklist

After adding a new operator:
1. `engine/nurion/__init__.py` — add import and `__all__` entry
2. `engine/_internal/INDEX.md` — add row to appropriate section
