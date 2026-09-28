# 3. Leases and heartbeats

## The problem

A worker pulls a task and dies. How does anyone find out?

The obvious answer is liveness detection: workers register themselves, something
pings them, and dead workers' tasks get reassigned. That needs a registry, a failure
detector, and a policy for "is it dead or just slow?" Each of those is a place to get
it wrong.

## Leases instead

A worker doesn't *take* a task, it *leases* it for `LEASE_TTL_SECONDS`. While the
handler runs, a heartbeat thread pushes `lease_expires_at` forward every
`LEASE_RENEW_INTERVAL`. If the worker dies, the heartbeat dies with it, the lease runs
out, and the reaper takes the task back.

```
t=0     lease          expires_at = 30
t=10    heartbeat      expires_at = 40
t=20    heartbeat      expires_at = 50
t=23    💥 worker dies
t=50    lease expired
t=5x    reaper scan    task -> pending, attempts += 1
```

The reaper never asks "is worker X alive?" It only compares a timestamp to the clock.
**Heartbeats are the mechanism, lease expiry is the policy.**

## Choosing the numbers

Defaults: TTL 30s, renew every 10s, reaper every 5s.

- **Renew interval must be well under the TTL.** `config.validate()` rejects
  `renew >= ttl`. With a 3× margin, a worker can miss two heartbeats in a row (a slow
  Redis call, a GC pause) and still keep its lease.
- **The TTL sets recovery time.** A crashed worker's task comes back after at most
  `TTL + reaper_interval` ≈ 35s. Shorter TTL means faster recovery but more renew
  traffic and a higher chance of reaping a worker that's just slow.
- **Recovery time doesn't depend on how long the task takes.** A 2-hour job is fine as
  long as the heartbeat keeps running. That's the advantage over a fixed visibility
  timeout (like SQS's), where you have to guess the task's max runtime up front.

## Renewing only if you still own it

`renew_lease(task_id, owner)` is a Lua script that checks `owner` first. That matters
for a worker that stalls instead of dying:

1. worker A stalls for 45s (TTL is 30)
2. reaper requeues the task, worker B leases it
3. worker A wakes up, heartbeat tries to renew

If renew didn't check ownership, A would push B's lease forward and both would think
they own it. With the check, A's renew returns `False` and A logs "lost lease". Its
handler keeps running (Python can't kill a thread) but its eventual ack is rejected
for the same reason.

## Heartbeat implementation

```python
while not done.wait(settings.lease_renew_interval):
    if not queue.renew_lease(task.id, owner):
        return
```

`Event.wait(timeout)` is both the sleep and the exit signal. When the handler finishes,
`done.set()` wakes the heartbeat immediately instead of waiting out the interval, so
there's no delay tacked onto every task.

## Clocks

`lease_expires_at` is computed with the **worker's** clock and compared against the
**reaper's** clock. If they disagree by more than the TTL margin, a live task can be
reaped (or a dead one kept too long). See [limitations](../limitations.md).
