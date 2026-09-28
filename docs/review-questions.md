# Review questions

Questions to check you understand the design. Try answering before opening the link.

## Delivery

1. Why is this at-least-once and not exactly-once? Describe the exact sequence that
   causes a duplicate run. → [01](design/01-delivery-semantics.md)
2. Give two ways to make a "charge the customer" handler idempotent.
   → [01](design/01-delivery-semantics.md)
3. Can two workers run the same task *at the same time*? How?
   → [01](design/01-delivery-semantics.md), [03](design/03-leases-and-heartbeats.md)

## Atomicity

4. What goes wrong if `lease()` did `RPOP pending` then `LPUSH processing`?
   → [04](design/04-atomicity.md)
5. Why is `enqueue` a `MULTI` but `ack` is a Lua script?
   → [04](design/04-atomicity.md)
6. Why does `_RECLAIM` re-check the lease expiry when the reaper could have checked it
   in Python first? → [04](design/04-atomicity.md), [06](design/06-reaper.md)
7. Why can't `BLMOVE` and the claim be one Lua script?
   → [06](design/06-reaper.md)

## Leases

8. Walk through what happens, step by step, when a worker is SIGKILLed mid-task.
   How long until the task runs again with default settings?
   → [03](design/03-leases-and-heartbeats.md)
9. Why must `LEASE_RENEW_INTERVAL` be less than `LEASE_TTL_SECONDS`, and why is 3× a
   reasonable margin? → [03](design/03-leases-and-heartbeats.md)
10. How is a lease different from an SQS visibility timeout? What's the advantage?
    → [03](design/03-leases-and-heartbeats.md)
11. A worker stalls for 45s with a 30s TTL. What happens to its renew, its ack, and its
    handler's side effects? → [03](design/03-leases-and-heartbeats.md), [limitations](limitations.md)

## Retries

12. Why full jitter? What happens to a recovering database without it?
    → [05](design/05-retries-and-backoff.md)
13. Why does lease expiry count as an attempt?
    → [05](design/05-retries-and-backoff.md)
14. Why a sorted set for delayed retries and not a worker sleeping before it nacks?
    → [05](design/05-retries-and-backoff.md)
15. If every worker is down, what moves due retries back to pending?
    → [05](design/05-retries-and-backoff.md)

## Reaper

16. Why does the reaper wait two scans before reclaiming an unclaimed id?
    → [06](design/06-reaper.md)
17. Is it safe to run three reapers? Why? → [06](design/06-reaper.md)

## Worker

18. Why does `python -m taskqueue.worker` break if the handler registry lives in
    `worker.py`? → [07](design/07-worker-pool.md)
19. Why do we ack *after* the handler and not before? → [07](design/07-worker-pool.md)
20. Why threads rather than asyncio or processes? When would you change that?
    → [07](design/07-worker-pool.md)
21. Why does the main thread `join(0.5)` in a loop rather than just `join()`?
    → [07](design/07-worker-pool.md)

## Production

22. Where does clock skew bite, and how would you remove it? → [limitations](limitations.md)
23. What's lost if Redis crashes with `appendfsync everysec`? → [limitations](limitations.md)
24. How would you add task priorities? → [limitations](limitations.md)
25. The `processing` list grows to 100k ids. What gets slow, and what would you change?
    → [limitations](limitations.md), [06](design/06-reaper.md)

## Hands-on

- Run the stack, enqueue a 20s `sleep`, `docker compose kill worker`, and watch the
  task come back via `/tasks/<id>`.
- Comment out the owner check in `_ACK` and see which test fails.
- Set `LEASE_RENEW_INTERVAL` higher than the TTL in `conftest.py`. What stops it?
- Change `lease()` to pop and push in two steps, then kill a worker between them.
