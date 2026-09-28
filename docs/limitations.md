# Known limitations

Things that are wrong, weak, or unfinished, roughly in order of how much they'd matter
in production.

## Clock skew between workers and the reaper

`lease_expires_at` is written using the worker's `time.time()` and compared against the
reaper's `time.time()`. If a worker's clock runs 30s behind, every lease it takes looks
expired the moment it's written.

**Fix:** use Redis's clock for everything lease-related. Call `redis.call('TIME')`
inside `_CLAIM`, `_RENEW` and `_RECLAIM` rather than passing `now` in from Python, so
there's only one clock involved. Since Redis 5, scripts replicate by effect, so calling
`TIME` inside a script is allowed.

## Redis is a single point of failure

One Redis instance. If it's down, nothing can be enqueued or leased.

- **Durability:** compose runs Redis with `--appendonly yes`, which by default fsyncs
  every second. A Redis crash can lose up to ~1s of writes, meaning recently enqueued
  tasks or acks. A lost ack means a re-run, which idempotent handlers already have to
  handle. A lost enqueue means the task is gone. `appendfsync always` fixes that at a
  large throughput cost.
- **Availability:** Sentinel or a managed Redis with failover. Replication is async,
  so a failover can lose recent writes as well.
- **Cluster:** every script touches several keys, and Cluster requires them all to be
  in one hash slot. That's doable with hash tags (`{dtq}:pending`), but everything would
  land on one shard anyway.

## Stale handlers keep running

When a worker loses its lease, its handler thread keeps going because Python can't
kill a thread. Its ack is rejected, but its *side effects* still happen, at the same
time as the new owner's. Idempotency covers correctness, but the work is wasted.

**Fix:** run handlers in a subprocess that can be killed when renewal fails, or pass
handlers a cancellation token they're expected to check.

## `LREM` is O(n)

`ack`, `nack` and reclaim remove ids from `processing` with `LREM`, which scans the
list. That's fine while in-flight count is small (it's bounded by total worker threads).
If it weren't, `processing` could become a sorted set scored by lease expiry, which
would also make the reaper's scan `ZRANGEBYSCORE -inf now` rather than a scan of every
id.

## Succeeded tasks are never deleted

Every task hash stays in Redis forever. Memory grows with total tasks ever run.

**Fix:** `EXPIRE` the hash in `_ACK` (e.g. 7 days), and do the same for DLQ tasks once
they're replayed or discarded.

## No priorities, scheduling, or rate limits

One FIFO. No "run at 9am", no per-handler concurrency limits. Priorities could be
multiple pending lists that `lease` checks in order. `BLMPOP` takes several keys,
but it pops without moving, so it'd need a script-based non-blocking move with a
short sleep loop instead.

## No handler timeout

A handler that hangs forever keeps heartbeating forever and holds its slot. There's no
max runtime.

## Reaper suspect set is per-process

The "seen unclaimed on the last scan" set lives in memory. Restarting the reaper
resets it, which only delays reclaiming those ids by one scan. With multiple reapers
each keeps its own set, which is also fine.

## Observability

There's `/stats` and print statements, nothing else. Prometheus counters (enqueued,
succeeded, retried, dead-lettered, reclaimed) and a histogram of task durations would
be the next step.
