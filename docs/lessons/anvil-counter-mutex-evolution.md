# Anvil counter design: from seven atomics to one mutex

*Investigation: 2026-04-23 → 2026-04-25, PRs #82 → #88*

A chain of four "fix the next race" PRs ended with us deleting all of them
and replacing seven independent atomics with a single `Mutex<QueueState>`.
The lesson is structural, not memory-ordering trivia: **lock-free across N
counters is N²-pair correctness**, and we kept finding new pairs in
production. The fix that worked was design-level, not patch-level.

This document is the long-form retrospective. The original publish-commit
race write-up is still authoritative for that specific bug; see
[`anvil-publish-commit-race.md`](./anvil-publish-commit-race.md). Here we
walk the whole arc.

---

## Symptom

For weeks, three CI suites — `chaos`, `stability`, `distributed` — flaked
with the same shape:

```
Expected: 10000, Got: 9900, Delta: 100
Missing composite keys: [(550, 0), …, (599, 1)]
Affected batches (split indices): [11]   ← exactly one contiguous batch
Collector: 9900 records, 0 duplicates    ← no second worker reprocessed
source/transform/sink queues: pending=0, claimed=0   ← all clean at end
```

Constant signatures every time:

- One contiguous range missing (never scattered).
- Zero duplicates (no double-claim).
- All queue counters report empty at end-of-run.
- Always under chaos: a worker is killed, recovery re-enqueues its
  in-flight messages, and one batch evaporates.

This is the canonical fingerprint of an **orphaned message**: it is
durably written to the DB but no claimer will ever read it because the
broker's `claim_seq` has advanced past its slot.

---

## The journey: four PRs, four bugs, one design lesson

### PR #82: retry workarounds (rejected by user)

The first instinct under CI pressure was to mark these tests with
`pytest-rerunfailures`. The user pushed back hard:

> retry is hide the problem is not solve the problem

This was the right call. Every subsequent PR found a real bug.

### PR #84: publish-commit race — committed-watermark fix

**Bug**. `push_messages` and `nack_messages_internal` did:

```rust
let base_seq = c.push_seq.fetch_add(count, Relaxed);  // 1. counter visible
// build WriteBatch with pending_key(base_seq..)
self.db.write(batch).await?;                          // 2. DB commit
```

A concurrent `claim_messages` could observe `push_seq = base_seq + count`,
CAS its `claim_seq` past the reservation, read `pending_key(seq)` → not yet
in DB → silent skip. When the writer finally committed, no future claim
would ever see it.

**Fix**. Introduce a *committed watermark*:

- `push_seq_alloc` — reservation cursor (writer-only).
- `push_seq_committed` — the watermark claimers read; advanced *after*
  `db.write` returns.
- `commit_log: Mutex<BTreeMap<base_seq, Reservation>>` — in-flight
  reservations. On commit, walk the contiguous-done prefix forward.
- `PushReservationGuard` with `Drop` — RAII guarantees the watermark
  doesn't wedge if a writer panics or early-returns.

This is documented end-to-end in `anvil-publish-commit-race.md`.

### PR #86: counter-snapshot torn read in `check_queue_completion`

After #84 merged, `test_all_workers_crash_and_recovery` *still* lost
exactly 50 records. Different bug, same flavor.

**Bug**. The writer (`nack_messages_internal`) bumped two independent
atomics in program order:

```rust
push_seq_alloc.fetch_add(count,  Relaxed);   // step 1
total_unclaimed.fetch_add(count, Relaxed);   // step 2
```

The reader (`check_queue_completion`) loaded *in the same order*:

```rust
let alloc_seq      = c.push_seq_alloc.load(Acquire);
let total_unclaimed = c.total_unclaimed.load(Relaxed);  // could be from step 2
```

Under Relaxed ordering, the reader could see `(alloc_seq=old,
total_unclaimed=new)` — a torn snapshot. With those values:

- `pending_count = alloc_seq - claim_seq = 0` (looked drained)
- `claimed_count = total_claimed - total_unclaimed = 0`
- `drained = finished && 0==0 && 0==0 → true`

Master saw `drained=true` 7ms after recovery started reclaiming a dead
worker's claim. Sink exited; the in-flight reclaim never reached it.

**Fix**. Read in the *opposite* order from the writer's program order,
and bump the writer's `total_unclaimed.fetch_add` to `Release`:

```rust
// writer
push_seq_alloc.fetch_add(count, Release);
total_unclaimed.fetch_add(count, Release);  // synchronizes-with reader

// reader (reverse order)
let total_unclaimed = c.total_unclaimed.load(Acquire);  // synchronizes-with writer
let total_claimed   = c.total_claimed.load(Acquire);
let claim_seq       = c.claim_seq.load(Acquire);
let alloc_seq       = c.push_seq_alloc.load(Acquire);
```

If the reader observes the new `total_unclaimed`, it must observe every
prior write, including the `push_seq_alloc` bump. The over-counts now go
in the safe direction (transient `pending > 0` during in-flight nack).

### PR #87: claim/check write order race

Stability tests still flaked. Different reader, same flavor.

**Bug**. In `claim_messages`:

```rust
// CAS claim_seq from cur → target
c.claim_seq.compare_exchange(cur, target, ...)?;
// then bump total_claimed
c.total_claimed.fetch_add(actual_claimed, Release);
```

A concurrent `check_queue_completion` could observe
`(claim_seq=new, total_claimed=old)` — the *opposite* tear from #86.
With the new `claim_seq` and old `total_claimed`:

- `pending_count = alloc_seq - claim_seq` = small/zero
- `claimed_count = total_claimed_old - total_unclaimed` = stale (low)
- `drained → true` while messages were just claimed but not yet counted.

**Fix attempt**. Bump `total_claimed` *before* the CAS, with a phantom-claim
undo if the actual reads from DB found fewer messages than reserved:

```rust
c.total_claimed.fetch_add(reserved, Release);    // pre-bump
let actual = read_pending_keys(...);
c.claim_seq.compare_exchange(cur, target, ...)?; // CAS visible after total_claimed
if actual < reserved {
    c.total_claimed.fetch_sub(reserved - actual, Release); // undo
}
```

This worked in unit tests but the structural problem was now obvious:
**every counter pair has a writer-order vs reader-order obligation**, and
adding/touching any counter creates new pairs to audit. The design was
asking us to enumerate an O(N²) lattice every time we changed code.

### PR #88: collapse to a single mutex (the actual fix)

The user pushed back on a heavier proposal involving two locks:

> which means you need 2 big lock to prevent conflicts, but it is too heavy

…then approved the right answer when posed as a single lock:

> ok

**Final design**. One `std::sync::Mutex` over a struct holding everything
that has to move together:

```rust
pub struct QueueState {
    push_seq_committed: u64,
    push_seq_alloc:     u64,
    claim_seq:          u64,
    total_pushed:       u64,
    total_claimed:      u64,
    total_unclaimed:    u64,
    total_acked:        u64,
    commit_log:         BTreeMap<u64, PushReservation>,
}

pub struct QueueCounters {
    pub state: std::sync::Mutex<QueueState>,
}
```

Discipline:

- Every writer takes the lock, mutates `QueueState`, drops the lock,
  *then* runs `db.write` unlocked. Critical section is sub-µs (struct
  field bumps + maybe one BTreeMap insert).
- `PushReservationGuard::Drop` reacquires briefly to flip its entry to
  `done` and walk the contiguous-committed prefix forward.
- Readers (`check_queue_completion`, `get_meta`, `get_queue_stats`) take
  one locked snapshot. No reordering, no synchronizes-with chains, no
  pairwise audit.
- `ack_and_scatter` (which had been bypassing the reservation pattern
  and bumping `push_seq` directly — a latent bug) now goes through the
  same reserve/commit path as every other writer.

The mutex is two orders of magnitude faster than the storage layer
(~20M ops/s vs ~10K ops/s for `db.write`), so it doesn't bottleneck.
Locks are not held across I/O.

| Bug class | Why it's gone |
|---|---|
| Publish-commit race (#84) | Watermark + commit-log invariant unchanged |
| Reservation leak (#84)   | RAII guard kept |
| Drained ignores in-flight (#86) | `pending_count` reads from same locked snapshot |
| Counter snapshot torn read (#86) | Single lock — impossible |
| Claim/check write order (#87)    | Single lock — impossible |
| Phantom claim over-bump (#87 followup) | Undone under same critical section |

Result: full engine suite green, including chaos / stability / distributed.

---

## What we should have noticed sooner

Three signals that we were patching, not designing:

1. **Each fix exposed the next bug.** PR #84 fixed publish-commit; PR #86
   fixed a torn read in the same module; PR #87 fixed the *symmetric*
   torn read in the same function. When fix N exposes bug N+1 with the
   same shape, the design is wrong.
2. **The fixes were memory-ordering tricks.** "Read in reverse writer
   order so synchronizes-with works" is a code smell. If correctness
   depends on humans correctly enumerating writer/reader pairs, humans
   will eventually miss one.
3. **The state was already coupled.** All seven counters represented
   one logical fact ("queue progress"). Splitting them across atomics
   was an optimization, not a model. The lock-free design was paying for
   contention we didn't have (broker is one process, ~10K ops/s ceiling
   from the DB).

---

## Decision rules from this experience

> Add to `.claude/rules/lessons.md`. Read before any change to
> `lib/anvil-rs/src/storage.rs` or any module with multiple coupled
> counters.

**R1. Multiple atomics that must move together → one mutex.**
If two atomics are read together by *any* code path and the reader
needs a consistent snapshot, they're not really independent. Put them
behind one lock unless you have a measured contention reason not to.

**R2. Lock-free is N² correctness; locked is N.**
With K independent atomics, every reader/writer pair has its own
ordering obligation. Adding a counter is K new audits. With one lock,
adding a field is one audit (the new field's invariants).

**R3. "Synchronizes-with" pairings in production code are a smell.**
Memory-ordering doc-comments age out. Future readers won't recognize
the invariant. Locks encode the invariant in the type system.

**R4. Hold the lock for in-memory mutation only, never across I/O.**
The single-mutex design works because `db.write` runs unlocked. The
lock is held for nanoseconds; throughput is limited by I/O, not
contention. If you find yourself holding a lock across `await`, the
design is wrong.

**R5. RAII for reservations.**
Any "reserve now, commit later" pattern needs a `Drop` impl that
finalizes on every exit path, including panics and early-return errors.
Without it, the watermark wedges forever after one error.

**R6. Before fixing race N, draw the writer/reader pair table.**
For every bug pair you fix, list the *other* pairs in the same module.
If there are more than two or three, stop patching and refactor the
state model.

---

## Code pointers

- Final design: `lib/anvil-rs/src/storage.rs::QueueState`,
  `QueueCounters`, `PushReservationGuard`, `reserve_push_range`,
  `commit_push_reservation`.
- `claim_messages` (under single lock): `storage.rs` (~line 480 area).
- `check_queue_completion` (single locked snapshot): `storage.rs`.
- DST coverage: `lib/anvil-rs/src/dst.rs` — 9/9 trials pass under chaos
  with this design.

## PR chain

| PR | State | What it tried |
|---|---|---|
| #82 | merged | RayDP cross-build + retry workarounds (per user, retries were the wrong answer) |
| #83 | closed | More retry config (obsoleted by #84) |
| #84 | merged | Committed-watermark + commit-log + RAII guard (fixed publish-commit race) |
| #85 | closed | Regression test for in-flight reservation drained-check (invariant becomes trivial under #88) |
| #86 | merged | Reverse-order Acquire reads in `check_queue_completion` (fixed torn snapshot #1) |
| #87 | closed | Pre-bump `total_claimed` in `claim_messages` (fixed torn snapshot #2; superseded by #88) |
| #88 | merged | Single `Mutex<QueueState>` over all counters + commit log (eliminates the bug class) |

## Related lessons

- [`anvil-publish-commit-race.md`](./anvil-publish-commit-race.md) —
  deep dive on the original race (PR #84). Still authoritative for
  the watermark design.
- L4 (declarative > inferential completion), L11 ("has work" vs "can
  exit"), L12 (broker-driven exit) in
  [`stage-completion-data-loss-postmortem.md`](./stage-completion-data-loss-postmortem.md)
  — the *upstream* invariants. The bugs in this doc were violations
  *under* those invariants: even with correct `drained` semantics,
  torn snapshots made the invariants lie.
