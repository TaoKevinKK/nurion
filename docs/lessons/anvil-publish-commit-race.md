# Anvil push/nack–claim race — root cause of CI data-loss flakes

*Investigation: 2026-04-24*

## Problem

`test_chaos_stress::test_many_small_batches_stress`,
`test_long_running_stability`, `test_sustained_chaos`,
`test_stability_worker_recovery::test_worker_restart_continues_from_offset`,
and `test_distributed_elasticity::test_scale_down_worker_failures` all
flake with the same signature under CI: kill a worker mid-flight and
~1–5 % of records go missing at the sink. One example (PR #82, run
24907821368, job 72941673472):

```
DIAGNOSTIC: test_scale_down_worker_failures
Expected: 10000, Got: 9900, Delta: 100
Missing composite keys: [(550, 0), …, (599, 1)]
Affected batches (split indices): [11]
Batch ranges: [(550, 599)]
Collector: 9900 records stored, 0 duplicates filtered
--- Broker Queue Stats ---
  source    output: pending=0, claimed=0
  transform output: pending=0, claimed=0
  sink      output: pending=0, claimed=0
```

Exactly one contiguous batch missing, zero duplicates, every queue
empty at end of run. A whole batch simply vanished — it wasn't
reprocessed by a second worker (no dup), and it wasn't stuck in flight
(all queues empty).

## Root cause

The broker's push/nack code advances the in-memory `push_seq` atomic
counter **before** committing the `pending_key` to the underlying DB:

`lib/anvil-rs/src/storage.rs`

```rust
// push_messages
let base_seq = c.push_seq.fetch_add(count, Ordering::Relaxed);   // 1. counter up
// … build WriteBatch with pending_key(base_seq..) …
self.db.write(batch).await?;                                      // 2. DB commit
```

```rust
// nack_messages_internal (used by recover_expired_claims)
let base_seq = c.push_seq.fetch_add(nack_count, Ordering::Relaxed);
for (i, msg_id) in msg_ids.iter().enumerate() {
    batch.delete(claimed_key(queue, msg_id));
    batch.put(pending_key(queue, base_seq + i as u64), msg_id.as_bytes());
}
self.db.write(batch).await?;
```

And `claim_messages` uses the same atomic as its upper bound:

```rust
let cur = c.claim_seq.load(Ordering::Acquire);
let lim = c.push_seq.load(Ordering::Acquire);   // ← may be ahead of what's committed
if cur >= lim { return Ok(Vec::new()); }
let target = std::cmp::min(cur + batch_size as u64, lim);
// CAS claim_seq cur → target
// Read pending_key(seq) for seq in [cur, target)
// If pending_key missing: skip silently  ← data loss
```

There is a window — between `push_seq.fetch_add` and `db.write` — where
a claimer can:

1. Observe the new `push_seq`.
2. CAS its `claim_seq` past the reserved range.
3. Read `pending_key(seq)`, find **nothing** (not yet committed).
4. Skip silently (comment: *"gap from crashed push, skip silently"*).

`claim_seq` has now advanced past seq N. When the nacking writer finally
commits `pending_key(N) = msg_id`, no future claim will ever read it —
it is orphaned.

Under chaos tests the window is hit repeatedly because recovery reclaims
N messages in a tight loop while many workers are actively claiming.

## Why it matches the symptoms

| Observation | Explained |
| --- | --- |
| Exactly one contiguous batch missing | One reclaim op lost its pending_key to one concurrent claim |
| 0 duplicates filtered | No second worker ever processed the msg — it was never claimable |
| All queues empty at end | The msg is orphaned in `pending_key(N)` but `claim_seq ≥ N`, so `pending_count = push_seq − claim_seq` reports 0 |
| `acked_count = 0` on transform output | Stat reporter returns counter state after retention window; orthogonal to the bug |
| Only 1–5 % loss, not 100 % | The race window is narrow (in-memory counter update → batch.write); most reclaims complete before a concurrent claim reads the gap |

The same shape applies to `push_messages` during source production, but
the test suite doesn't usually run concurrent claimers on a source's
first batch, so the source-side race rarely bites in practice. Reclaim
is the dominant trigger.

## The "silent skip" comment is wrong

`storage.rs:532`:

```rust
// If pending key is missing (gap from crashed push), skip silently
```

There is no crashed-push scenario under the current architecture — the
broker runs in the driver (see lesson L5), and `push_messages` either
returns success with both counter and batch committed, or returns an
error and neither change lands persistently (the atomic counter *can*
drift on error, but that's a separate counter-corruption bug — see the
comment at `storage.rs:601-604`). The "gap" the skip exists for is
actually the publish–commit race described above, not a crash.

## Fix that landed: lock-free committed watermark

Two atomics + a tiny mutex, no serialization between writers, claimers
stay lock-free.

- `push_seq_alloc` (new): the **reservation** cursor. Writers fetch_add
  this to allocate a unique seq range. NOT visible to claimers.
- `push_seq` (existing field, repurposed): the **committed watermark**.
  Only advanced *after* `db.write` returns. Claimers use this as their
  upper bound — anything `< push_seq` is guaranteed to have its
  `pending_key` durable.
- `push_commit_log: Mutex<BTreeMap<base_seq, PushReservation>>` —
  in-flight reservations with a `done` flag. Held only briefly to
  flip the flag and walk the contiguous-committed prefix; never held
  across `db.write`.

Writer flow (`push_messages` / `nack_messages_internal` / the
downstream-push branch of `ack_internal` all share this pattern via
two helpers):

```rust
let base_seq = reserve_push_range(&c, count).await;  // fetch_add + insert into log
// build batch with pending_keys at [base_seq, base_seq + count)
let result = self.db.write(batch).await;             // no per-queue lock held
commit_push_reservation(&c, base_seq).await;          // flip done, advance watermark
result?
```

`commit_push_reservation` walks the front of the BTreeMap and advances
`push_seq` through every contiguous done entry. Out-of-order commits
just wait at the gap until earlier reservations land. Failure runs the
same path so the watermark never stalls behind a failed batch (the
`pending_key` range is empty; claimers warn-and-skip that range, but
every later push is unaffected).

Claimers stay lock-free:

```rust
let cur = c.claim_seq.load(Acquire);
let lim = c.push_seq.load(Acquire);   // post-commit watermark — durable invariant
// CAS claim_seq cur → target
// Read pending_key(seq) — guaranteed in DB now
```

Persisted `seq_push_key` switched from `base_seq + count` to
`push_seq_alloc.load()`. The alloc cursor is monotonic in memory, so
out-of-order DB commits no longer roll the persisted watermark
backward (a latent secondary bug under the original design).

Also evaluated and rejected:

- **Per-queue RwLock around `fetch_add` + `db.write`.** Correct, but
  serializes all writers on a queue and stalls claimers during
  commits. Tried it (commit `1b18c7f`), throughput dropped enough that
  the concurrency test had to be given a 30-second deadline. Rolled
  back in favor of the watermark.
- **Retry-on-gap in claim.** Rolling back a CAS'd `claim_seq` is
  brittle and just papers over the invariant violation.
- **Move counter into the WriteBatch only.** Biggest rewrite, smallest
  payoff over the watermark.

The "publish-commit race, orphaned msg" `tracing::warn!` in
`claim_messages` stays as a regression detector: under the watermark
design it should never fire on the happy path. If it does, the new
code has a bug.

## Related code / commits

- Broker push: `lib/anvil-rs/src/storage.rs` `push_messages` (~line 454)
- Reclaim: `lib/anvil-rs/src/storage.rs` `nack_messages_internal`
  (~line 857), called from `recover_expired_claims` (~line 1087)
- Claim: `lib/anvil-rs/src/storage.rs` `claim_messages` (~line 493),
  silent-skip at `storage.rs:532`
- Diagnostics helper: `engine/tests/utils/diagnostics.py`
- Recent area-touching commits: #79, #80 added diagnostics but didn't
  fix the race; #74 (b10d50e) "WorkQueue atomic counters" introduced
  the current counter scheme.
