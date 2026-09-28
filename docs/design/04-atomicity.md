# 4. Atomicity

Nearly every bug a queue like this can have is a crash or race between two Redis
commands that were meant to happen together. The rule in this codebase:

> **Anything that moves an id between lists *and* changes the task hash happens in a
> single Lua script.**

Redis runs a script start to finish with nothing else interleaved, so there's never a
moment where the id is in one place and the hash says another. Each script also
**re-checks state (and owner) inside Redis before doing anything**, because the
caller's view of the task might already be stale.

## Why not MULTI/EXEC or WATCH?

- `MULTI/EXEC` is atomic but can't branch. "Only ack if I'm still the owner" needs a
  read, then a decision, then writes.
- `WATCH` + `MULTI` can do check-then-act, but it's optimistic: on contention the
  transaction aborts and you retry in a loop. That's more code and more round trips.
- Lua does read, branch and write in one round trip, with no retries.

`enqueue` is the one place that uses a plain `MULTI` pipeline: it's two unconditional
writes (hash, then push) with nothing to check.

## The scripts

### `enqueue` (MULTI, not Lua)
`HSET task` then `LPUSH pending`. **Race it closes:** without the transaction a worker
could `BLMOVE` the id before the hash exists and find nothing there.

### `BLMOVE` + `_CLAIM` (`lease`)
`BLMOVE pending → processing` is atomic by itself, so the id is never out of both
lists. That's the core of at-least-once: a two-step `RPOP` + `LPUSH` loses the task
if the worker dies in between.

`_CLAIM` then stamps `state=processing, owner, lease_expires_at`. These are two
separate commands because `BLMOVE` is blocking and Lua scripts can't block. The gap is
handled by the reaper's two-scan rule (see [reaper](06-reaper.md)). `_CLAIM` also
drops the id if its hash has disappeared.

### `_RENEW`
Checks `state == processing` and `owner == me`, then bumps `lease_expires_at`.
**Race it closes:** a reaped worker extending the lease of a task someone else now owns.

### `_ACK`
Checks state and owner, sets `succeeded`, `LREM processing`.
**Races it closes:** a reaped worker acking a task that's running elsewhere (which would
mark it succeeded while it might still fail), and a crash between "mark succeeded" and
"remove from processing" (which would leave a succeeded task for the reaper to requeue).

### `_NACK`
Checks state and owner, `HINCRBY attempts`, records the error, `LREM processing`, then
either `ZADD delayed` (retry) or `LPUSH dlq` (dead).
**Race it closes:** a crash between leaving `processing` and arriving at the destination,
which would drop or duplicate the task.

The retry delay is computed in Python from an `HGET attempts` read beforehand, so the
read isn't inside the script. That's fine: only the owner can nack, and the owner is
the only one changing `attempts` at that point. The worst case is a slightly wrong
backoff delay, not a lost task.

### `_PROMOTE`
`ZRANGEBYSCORE delayed -inf now`, then `ZREM` + `LPUSH pending` for each.
**Race it closes:** two workers promoting at once would each push the same id to
`pending`, and it would run twice. Inside the script, the second caller finds the zset
already empty.

### `_RECLAIM` (reaper)
Re-checks that the lease really is expired **inside the script**, then requeues or
dead-letters. **Race it closes:** the reaper reads `lease_expires_at`, the worker renews,
and the reaper requeues a task that's alive. Also makes multiple reapers safe: the
second one sees `state != processing` and does nothing.

### `_REPLAY`
`LREM dlq` and only continues if it actually removed something, then resets and pushes
to pending. **Race it closes:** double-clicking replay pushing the id twice.

## Script caching

`register_script` sends `EVALSHA` and falls back to `EVAL` if Redis answers `NOSCRIPT`
(e.g. after a restart flushed the script cache), so the scripts survive Redis restarts
without extra code.
