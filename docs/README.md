# Docs

Design notes for the task queue: what each piece does, why it's built that way, and
what was traded off. The top-level README covers how to run it; this covers *why*.

## Design

1. [Delivery semantics](design/01-delivery-semantics.md) — at-least-once, and why not exactly-once
2. [Data model](design/02-data-model.md) — the Redis keys and the task state machine
3. [Leases and heartbeats](design/03-leases-and-heartbeats.md) — how crash recovery works without liveness detection
4. [Atomicity](design/04-atomicity.md) — every Lua script and the race it closes
5. [Retries and backoff](design/05-retries-and-backoff.md) — the delayed zset and full jitter
6. [The reaper](design/06-reaper.md) — reclaiming orphans, multiple reapers, poison tasks
7. [The worker pool](design/07-worker-pool.md) — threads, handler registry, graceful drain
8. [Testing](design/08-testing.md) — why the tests use a real Redis and a real process kill

## Other

- [Known limitations](limitations.md) — things that are wrong or weak, and what fixing them would take
- [Review questions](review-questions.md) — questions to check you actually understand the design
