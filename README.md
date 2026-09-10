# Distributed Task Queue

A Redis-backed asynchronous task queue with a worker pool, at-least-once delivery,
exponential-backoff retries, a dead-letter queue, and lease-based recovery of tasks
orphaned by crashed workers.

## Why this exists

Most toy task queues lose work when a worker dies mid-task: the job was popped off the
queue, so it's gone, and nothing ever notices. This project is built around that failure
case. A worker doesn't *take* a task, it *leases* one — and a lease that stops being
renewed is reclaimed and redelivered.

## Architecture

```
   POST /tasks
        |
        v
  +-------------+        BRPOPLPUSH        +--------------+
  |  pending    | -----------------------> |  processing  |
  |  (list)     |                          |  (list)      |
  +-------------+                          +--------------+
        ^                                        |
        |                                        | worker renews lease
        | requeue on                             | every LEASE_RENEW_INTERVAL
        | retry or                               v
        | lease expiry                    +--------------+
        |                                 | task:<id>    |  state, attempts,
        +---------------------------------| (hash)       |  lease_expires_at
        |                                 +--------------+
        |                                        |
        |                            attempts exhausted
        |                                        v
        |                                 +--------------+
        +---------------------------------|  dlq (list)  |
                                          +--------------+

  reaper loop: scans `processing` for tasks whose lease_expires_at < now,
               moves them back to `pending`, increments attempts
```

### Key design decisions

**`BRPOPLPUSH` for the pending → processing move.** This is atomic. A worker cannot
pop a task and then die before recording that it holds it — the task is in `processing`
the instant it leaves `pending`. This is what makes at-least-once delivery possible.

**Leases instead of locks.** Each task hash carries `lease_expires_at`. A live worker
renews it on a timer; a dead worker stops. The reaper only needs to compare timestamps,
so it never has to detect worker liveness directly. Heartbeats are the mechanism, lease
expiry is the policy.

**At-least-once, not exactly-once.** A worker can finish a task and crash before
acknowledging it, so that task will run twice. This is a deliberate trade: exactly-once
requires distributed transactions across Redis and whatever the task touches. **Handlers
must be idempotent** — that requirement is pushed to the caller rather than faked here.

**Retries carry attempt count in the task hash, not the payload.** Requeuing is then a
list operation plus a field increment, and the backoff schedule can change without
rewriting in-flight tasks.

## Layout

```
taskqueue/
  config.py     Environment-driven settings
  models.py     Task, TaskState, serialization
  keys.py       Redis key naming — one place, so nothing drifts
  queue.py      Enqueue, lease, ack, retry, DLQ routing
  worker.py     Worker pool, heartbeat/lease renewal, handler dispatch
  reaper.py     Lease-expiry scan and orphan requeue
  backoff.py    Exponential backoff with jitter
  api.py        FastAPI control plane
tests/
  ...           Including a real crash-recovery test
```

## Running it

```bash
docker compose up -d redis
pip install -r requirements.txt

uvicorn taskqueue.api:app --reload    # control API on :8000
python -m taskqueue.worker            # worker pool
python -m taskqueue.reaper            # lease reaper
```

Or the whole stack:

```bash
docker compose up --build
```

## API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/tasks` | Enqueue a task |
| `GET` | `/tasks/{id}` | Task state, attempts, last error |
| `GET` | `/stats` | Queue depths by state |
| `GET` | `/dlq` | List dead-lettered tasks |
| `POST` | `/dlq/{id}/replay` | Move a task from the DLQ back to pending |

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection |
| `MAX_ATTEMPTS` | `5` | Attempts before dead-lettering |
| `LEASE_TTL_SECONDS` | `30` | How long a lease is valid |
| `LEASE_RENEW_INTERVAL` | `10` | Worker heartbeat period |
| `REAPER_INTERVAL` | `5` | Seconds between orphan scans |
| `BACKOFF_BASE_SECONDS` | `1.0` | First retry delay |
| `BACKOFF_MAX_SECONDS` | `300.0` | Retry delay ceiling |
| `WORKER_CONCURRENCY` | `4` | Tasks in flight per worker process |

## Roadmap

- [ ] Implement `queue.py` lease/ack/retry paths
- [ ] Implement `worker.py` heartbeat renewal
- [ ] Implement `reaper.py` orphan scan
- [ ] Crash-recovery test: kill a worker mid-task, assert redelivery
- [ ] Delayed retries via a Redis sorted set instead of immediate requeue
- [ ] Prometheus metrics endpoint
- [ ] Deploy to Cloud Run / ECS

## License

MIT
