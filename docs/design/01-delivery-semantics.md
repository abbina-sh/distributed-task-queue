# 1. Delivery semantics

## The guarantee

**Every enqueued task runs to completion at least once, or ends up in the DLQ.**
Nothing is silently dropped. A task may run more than once.

## Why not at-most-once?

At-most-once is the naive queue: `RPOP` the task, run it. If the worker dies after the
pop, the task is gone and nothing records that it existed. It's simple and it's
exactly the failure mode this project exists to fix.

## Why not exactly-once?

Consider a worker that finishes a handler and crashes before `ack()` reaches Redis:

```
worker:  lease ──> run handler ──> (side effects happen) ──> 💥   ack never sent
reaper:                                                      lease expires ──> requeue
worker2:                                                                        run handler again
```

The queue has no way to tell "crashed before doing the work" from "crashed after doing
the work but before acking". Both look like an expired lease. To get exactly-once you'd
need the handler's side effects and the ack to commit in one transaction, and the side
effects live in some other system (a database, an email API, S3). That's a distributed
transaction problem, and the queue can't solve it on the caller's behalf.

So the queue promises at-least-once and pushes one requirement to the caller:

## Handlers must be idempotent

Running a handler twice with the same payload has to be safe. Common ways to do that:

- **Idempotency key.** Use the task id (or a key in the payload) as a unique key in the
  destination: `INSERT ... ON CONFLICT DO NOTHING`, or Stripe-style idempotency headers.
- **Check-then-act on state you own.** "Mark invoice 42 as sent" is idempotent; "send
  an email" isn't unless you record that you sent it.
- **Naturally idempotent operations.** `SET x = 5` is fine to repeat, `x += 5` isn't.

## Where duplicates can come from

| Cause | Why it re-runs |
|---|---|
| Worker crashes after handler, before ack | lease expires, reaper requeues |
| Worker stalls longer than the lease TTL (GC pause, swap, debugger) | reaper requeues while the original is still running |
| Redis connection drops right as `ack` is sent | ack may or may not have applied; if not, lease expires |

The second case is the sneaky one: two workers can be running the *same task at the
same time*. The owner checks (see [atomicity](04-atomicity.md)) make sure only the
current owner's ack counts, but they can't stop the stale handler from running.
