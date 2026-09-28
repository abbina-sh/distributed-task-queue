# 6. The reaper

`taskqueue/reaper.py` runs as its own process. Every `REAPER_INTERVAL` seconds:

1. For each id in `processing`, run `_RECLAIM`, which requeues it if the lease has expired
2. Promote due retries from `delayed`

## Why a separate process?

It could run inside each worker, but then recovery would depend on at least one
worker being alive, and the workers are the thing that crashes. As a separate process
it's small, has no handler code to crash it, and can be scaled or restarted on its own.

## Multiple reapers

Running two for redundancy is safe. The expiry check happens **inside** `_RECLAIM`,
not in Python beforehand, so:

```
reaper A: _RECLAIM(t1) -> expired, requeue, state = pending
reaper B: _RECLAIM(t1) -> state != processing, return 0
```

`test_two_reapers_reclaim_once` covers this.

## The unclaimed-id gap

`lease()` takes two steps: `BLMOVE` (the id enters `processing`), then `_CLAIM` (the hash
gets `state=processing` and a lease). A worker that dies between them leaves an id in
`processing` whose hash still says `pending`, with no lease timestamp. Nothing would ever
expire it.

The reaper can't reclaim these straight away, because the same thing is also
visible for a few milliseconds during every *normal* lease. Grabbing it would race a
live worker's claim.

**Rule: an unclaimed id is only reclaimed if it was also unclaimed on the previous
scan.** A normal claim finishes in milliseconds and the scans are 5s apart, so seeing
it twice means the worker is gone. The set of suspects is kept in reaper memory
(`_unclaimed_last_scan`). If the reaper restarts it forgets them, and they just get
caught one scan later.

Why not close the gap with a single script? `BLMOVE` blocks, and Lua scripts can't
block. The non-blocking `LMOVE` could go inside a script, but then workers would have to
poll instead of block.

## Poison tasks

Lease expiry increments `attempts`, and `_RECLAIM` dead-letters at `MAX_ATTEMPTS`.
Without that, a task that crashes its worker every time would take down workers
indefinitely. The reaper also writes `last_error = "lease expired (owner: <id>)"` so you
can tell from the DLQ which worker was running it.

## Cost

Each scan runs `LRANGE processing 0 -1` plus one script per id, so it's O(tasks in
flight). That's fine since in-flight is bounded by `workers × WORKER_CONCURRENCY`.
If it ever grew large, a sorted set of leases scored by expiry would make it
`ZRANGEBYSCORE -inf now`: only the expired ones get touched. See
[limitations](../limitations.md).
