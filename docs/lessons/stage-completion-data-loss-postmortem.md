# Refactoring Lessons

> Indexed patterns, pitfalls, and root-cause analyses from major refactors.
> **Check before any non-trivial change to core/, runtime/, or anvil-rs.**

## Index

| # | Title | Category | Files involved |
|---|---|---|---|
| L1 | Discriminated union > Optional fields | Data modeling | core/models, stage_master, worker_manager |
| L2 | Circular import: core ↔ runtime | Python imports | core/, runtime/ |
| L3 | Flag dance is a distributed anti-pattern | Distributed state | stage_master, worker_manager, stage_worker |
| L4 | Declarative > inferential completion | Queue semantics | anvil-rs/storage.rs |
| L5 | Broker runs in driver — don't over-engineer crash recovery | Architecture | anvil-rs/storage.rs |
| L6 | catch-all exception handler hides bugs | Error handling | stage_worker.py |
| L7 | CI flaky test diagnosis playbook | Testing | tests/ |
| L8 | State enum > multiple bools | State machine | stage_master, stage_worker |
| L9 | Worker self fields: property > copy | Code hygiene | stage_worker.py |
| L10 | Test claim_timeout must match scenario | Test config | test_pipeline_factory.py |
| L11 | "Has work" vs "can exit" are different questions | Queue semantics | stage_master, stage_worker |
| L12 | Broker-driven exit: the final architecture | Architecture | proto, service.rs, stage_worker, stage_master |
| L13 | Wrong turns and why they failed | Process | — |

---

## L1: Discriminated union > Optional fields
*PR #61, 2026-03*

**Problem**: Multiple mutually-exclusive Optional fields represent variants of the same concept.
**Fix**: Single discriminated union type (`QueueRef(name, is_group)`).
**Dev check**: 2+ mutually-exclusive Optional fields → merge into a union.

---

## L2: Circular import: core ↔ runtime
*PR #61, 2026-03*

`core/` cannot top-level import `runtime/` (reverse import chain via `runtime/__init__.py`).
**Fix**: `TYPE_CHECKING` block for annotations, local import for runtime construction.

---

## L3: Flag dance is a distributed anti-pattern
*PR #79, 2026-04*

**Problem**: Cross-process bool flag (`_safe_to_exit`) to coordinate worker exit. Flag propagated via fire-and-forget RPC, with N race windows:
1. One-shot latch — once set, never resets → recovery workers exit immediately
2. `_poll_queue_completion` background task notify timing uncontrollable
3. `spawn_worker()` checks flag and skips spawn entirely

**Fix**: **Delete the entire flag mechanism.** Add `upstream_drained` field to broker claim response (`finished && pending==0 && claimed==0`). Worker exit driven entirely by broker ground truth.

**Removed**: 3 state variables, 2 background tasks, 5 RPC methods, ~100 lines of code.

**Dev check**: Cross-process bool flag + fire-and-forget notification → red flag. Replace with ground truth polling.

---

## L4: Declarative > inferential completion
*PR #80, 2026-04*

**Problem**: `drained` definition used an inferential heuristic:
```rust
// BAD: temporarily empty queue looks drained
drained = pending==0 && claimed==0 && (total_pushed > 0 || finished)
```
`total_pushed > 0` let downstream stages believe "done" before upstream called `mark_finished` → premature exit → cross-stage completion race → data loss.

**Fix**: One-line Rust change:
```rust
// GOOD: only explicitly declared "no more pushes" counts as drained
drained = finished && pending==0 && claimed==0
```

**Dev check**: Queue/stream "completion" must be based on an explicit finished signal, never on "looks empty."

---

## L5: Broker runs in driver — don't over-engineer crash recovery
*PR #80, 2026-04*

**Wrong attempt**: Defer atomic counter updates in `ack_and_scatter` to after WriteBatch commit, to prevent counter desync on broker crash.

**Why it was wrong**:
1. Broker runs in the driver process. Broker crash = entire workflow crash. "Broker crashes independently" does not happen.
2. Worker SIGKILL doesn't affect broker — broker completes the gRPC handler normally, WriteBatch always commits.
3. Deferring counter updates introduced a new bug: two concurrent operations `load()` the same value → WriteBatch writes identical persisted values → counter vs DB inconsistency.

**Lesson**: Before fixing a bug, verify the failure mode actually exists. Don't add complexity for scenarios that cannot happen.

**Dev check**: Ask "can this component crash independently of its host process?" If not, no need for cross-process consistency protection.

---

## L6: catch-all exception handler hides bugs
*PR #79, 2026-04*

**Problem**: Worker claim loop `except Exception: log + sleep` swallowed `claim_token_mismatch` (ack failed because claim expired). This should nack + retry, not silently continue.

**Dev check**: `except Exception` must at least distinguish recoverable vs fatal errors. Never log + continue blindly.

---

## L7: CI flaky test diagnosis playbook
*PR #79-80, 2026-04*

1. **Check develop first** — rule out whether it's your change
2. **Add driver-side diagnostics** (`print`, not logger — Ray worker stderr is deduped; only captured stderr is shown on test failure)
3. **Collect diagnostics before `runner.stop()`** — broker must be alive for queue stat queries
4. **Key diagnostic info**:
   - Missing ID ranges → first batch (init issue), middle (recovery race), last (completion race)
   - Collector dedup count → non-zero means recovery caused duplicate processing
   - Broker per-partition pending/claimed → message state at failure time
5. **Broker-side tracing** (Rust `tracing::info!`): `upstream_drained=true` timing + output queue state at `mark_finished` time

---

## L8: State enum > multiple bools
*PR #79-80, 2026-04*

**Problem**: Multiple mutable bools with invalid combinations (e.g. `running=True, finished=True`).
**Fix**: StageMaster uses `_StageState` enum; StageWorker uses `_stopped` bool + broker `drained`.
**Trap**: When merging bools → enum, loop condition must cover all "keep running" states. `while state == RUNNING` exits on DRAIN (bug) — should be `while state != STOP`.

---

## L9: Worker self fields: property > copy
*PR #79, 2026-04*

Remove 6 redundant field copies from WorkerRuntime, replace with `@property` accessors. 15 → 7 fields. `__init__` only assigns; operator init deferred to `run()`.

---

## L10: Test claim_timeout must match scenario
*PR #79, 2026-04*

| Scenario | claim_timeout | recovery_interval | Reason |
|---|---|---|---|
| Non-kill tests | 30s | 5s | CI slow CPU may exceed 2s for first split init |
| Kill tests | 10s | 2s | Long enough to avoid false expiry, short enough for fast recovery |
| Production | 60s | 10s | Safe default |

---

## L11: "Has work" vs "can exit" are different questions
*PR #80, 2026-04*

**Problem**: `_has_unprocessed_messages` (master) and `upstream_drained` (worker) both used `all_drained` (requires `finished`). This made master think "has unprocessed messages" when queue was empty but upstream hadn't called `mark_finished` yet → spawns idle workers → pipeline hangs.

**Two distinct semantics**:
| Question | Who asks | Correct semantics |
|---|---|---|
| "Can I exit?" | Worker (via broker) | `finished && pending==0 && claimed==0` |
| "Is there work to do?" | Master | `pending > 0 \|\| claimed > 0` (raw counts, ignores finished) |

**Fix**: `_has_unprocessed_messages` for QueueGroup checks partition raw pending+claimed counts, not `all_drained`.

---

## L12: Broker-driven exit: the final architecture
*PR #79-80, 2026-04*

**Three-layer change**:

1. **Proto** (anvil.proto): Add `bool upstream_drained` to `ClaimResponse`
2. **Rust** (service.rs): When claim returns empty, call `check_group_completion`/`check_queue_completion`, set `upstream_drained = finished && pending==0 && claimed==0`
3. **Python**:
   - `AnvilQueueClient.claim()` returns `(records, drained)` tuple
   - Worker claim loop: `elif drained: flush; break`
   - Deleted: `_safe_to_exit` flag, `_poll_queue_completion` task, `notify_safe_to_exit` RPC, `clear_safe_to_exit` hack

**Key invariants**:
- Worker exit **sole signal** is broker's `upstream_drained=True` (in claim response)
- Broker is single source of truth — no master relay needed
- `drained` requires `finished` (L4) — only exits after upstream explicitly says "no more data"
- Master only spawns/recovers workers + marks output finished; does not participate in worker exit decisions

**Pipeline completion chain**:
```
Source: mark planner queue finished → source workers see drained → exit
        → source master marks output group finished
Transform: workers see upstream drained → exit
           → transform master marks output group finished
Sink: workers see upstream drained → exit → sink master exits
```

Each level waits for explicit upstream `mark_finished`. No inference, no flags, no background tasks.

---

## L13: Wrong turns and why they failed
*PR #79-80, 2026-04*

Dead ends encountered during data loss investigation:

| Attempt | Why it didn't work |
|---|---|
| Increase claim_timeout (2s→30s) | Only fixed non-kill scenario. Kill race is not about timeout |
| `clear_safe_to_exit` in recovery path | Fixed one race, but `_poll_queue_completion` re-notifies safe_to_exit at arbitrary times |
| Worker `_should_exit()` double-checks broker | Added exit latency, caused timeout issues; worker shouldn't do stage-level coordination |
| `_ExitSignal` enum (RUNNING/DRAIN/STOP) | Loop condition `== RUNNING` exits on DRAIN (missed flush), should be `!= STOP` |
| Defer counter updates (post-commit) | Broker doesn't crash independently; deferral introduced concurrent `load()` race |

**Lesson**: Patch-style bug fixes compound. Each patch covers one race window but opens another. Find the simplest root cause (L4: `drained` definition) and fix it with one line.
