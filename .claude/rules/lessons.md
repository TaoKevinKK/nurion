# Lessons Learned

> Before making changes to core/, runtime/, queue/, or anvil-rs, read the relevant lesson.

## Required Reading

| Change area | Read first |
|---|---|
| Worker exit / completion detection | `docs/lessons/stage-completion-data-loss-postmortem.md` (L3, L4, L11, L12) |
| Queue drained / finished semantics | Same doc (L4, L11) |
| Anvil broker atomic counters / coupled state | `docs/lessons/anvil-counter-mutex-evolution.md` |
| Anvil push/claim/ack hot paths | `docs/lessons/anvil-publish-commit-race.md` + `anvil-counter-mutex-evolution.md` |
| Cross-process state coordination | `stage-completion-...md` (L3, L13) |
| Stage state management (bools/enums) | Same doc (L8) |
| Test claim_timeout configuration | Same doc (L10) |
| CI flaky test debugging | Same doc (L7) |

## Key Rules (summary)

1. **Never use cross-process bool flags for coordination** — use broker ground truth (L3)
2. **Queue "drained" = `finished && empty`** — never infer completion from "looks empty" (L4)
3. **"Has work?" (master) ≠ "Can exit?" (worker)** — master checks raw counts, worker checks drained (L11)
4. **Worker exits only when broker says `upstream_drained=True` in claim response** (L12)
5. **Broker runs in driver process** — don't add crash recovery for non-independent components (L5)
6. **`except Exception` must distinguish recoverable vs fatal** — never log + continue blindly (L6)
7. **Coupled counters → one mutex, not N atomics** — lock-free across N counters is N²-pair correctness (`anvil-counter-mutex-evolution.md`)
8. **Hold the lock for in-memory mutation only, never across `db.write` / `await`** — sub-µs critical sections are not the bottleneck
9. **Reservation patterns need RAII `Drop`** — every exit path (panic, early return, error) must finalize, or the watermark wedges
