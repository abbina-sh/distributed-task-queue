# 7. The worker pool

## Shape

One worker process runs `WORKER_CONCURRENCY` threads. Each thread loops:

```
lease (block ≤1s) ──> start heartbeat ──> run handler ──> ack | nack ──> stop heartbeat
```

Each thread has its own owner id, `hostname:pid:index`, so each lease belongs to one
specific thread and not just to the process.

## Why threads?

Task handlers in a queue like this are usually I/O-bound: HTTP calls, database writes,
sending email. Threads release the GIL while waiting on I/O, so they give real
concurrency for that. They're also simple: blocking `redis-py` calls, no event loop,
and handlers are plain functions.

- **CPU-bound handlers** won't speed up with more threads (GIL). Run more worker
  *processes* instead: `docker compose up --scale worker=N`.
- **asyncio** would need an async Redis client, and every handler would have to be
  `async def` too, which pushes complexity onto users for little gain here.
- **multiprocessing inside the worker** is a possible next step if CPU-bound tasks
  matter, but separate containers already give process isolation.

## Ack after the handler, never before

```python
try:
    fn(task.payload)
except Exception as exc:
    queue.nack(...)
else:
    queue.ack(...)
```

Acking first and then running the handler turns any handler crash into a lost task.
That would be at-most-once again.

## Owner checks on ack/nack

The worker passes `owner=` to `ack` and `nack`. If the worker stalled past its lease and
got reaped, its ack is rejected inside the script and the new owner's run is what
counts. Covered by `test_stalled_worker_cannot_ack_after_being_reaped`.

## The handler registry and `__main__`

The registry (`HANDLERS` and `@handler`) lives in `handlers.py`, not `worker.py`.
The scaffold had it in `worker.py`, which breaks:

`python -m taskqueue.worker` runs `worker.py` as the module `__main__`. A handler module
doing `from taskqueue.worker import handler` imports `worker.py` **again**, under the
name `taskqueue.worker`. That creates a second copy with its own empty `HANDLERS` dict.
Handlers register into the copy, the running worker reads from `__main__`'s dict, and
every task fails with "no handler registered".

Putting the registry in a module that's never run as `__main__` means there's only one
copy. `worker.py` re-exports `handler` so both imports work.

## Graceful shutdown

On SIGTERM or SIGINT a shared `threading.Event` is set. Each thread finishes its
current task (ack or nack included) and then exits instead of leasing another. The
main thread joins with short timeouts so it can still receive signals, since Python
only delivers signals to the main thread and a plain `join()` would block that.

Why drain at all, if killing mid-task is safe? It's *safe* but *wasteful*: the task
waits out a whole lease TTL and then runs again from the start. Draining makes deploys
and scale-downs free. Docker gives containers 10s after SIGTERM before SIGKILL, so
tasks longer than that still get cut off and recovered by the reaper.

## Error handling in the loop

If Redis goes away, `run_once` raises. The loop logs the traceback, waits 1s and tries
again rather than crashing the process. Any lease held at that moment expires and the
reaper recovers it once Redis is back.
