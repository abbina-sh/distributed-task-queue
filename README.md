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
  +-------------+          BLMOVE          +--------------+
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

  failed attempt: task waits in `delayed` (zset, scored by due time) until
                  its backoff is up, then gets promoted back to `pending`

  reaper loop: scans `processing` for tasks whose lease_expires_at < now,
               moves them back to `pending`, increments attempts
```

### Key design decisions

**`BLMOVE` for the pending → processing move.** This is atomic. A worker cannot
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

**State changes are Lua scripts.** Anything that moves an id between lists *and*
updates the task hash (claim, ack, nack, reclaim, replay) runs as one script, so
Redis applies it atomically. Each script also re-checks the task's state and owner
first — that's what stops a worker that stalled past its lease from acking a task
the reaper already handed to someone else.

**Retries wait in a sorted set.** A failed attempt goes into `delayed` scored by
`now + backoff`, and gets promoted to `pending` once it's due. Workers promote due
retries while they block on `lease()`, and the reaper does it too in case every
worker is down.

**Retries carry attempt count in the task hash, not the payload.** Requeuing is then a
list operation plus a field increment, and the backoff schedule can change without
rewriting in-flight tasks.

Design notes and the reasoning behind each choice are in [`docs/`](docs/README.md).

## Layout

```
taskqueue/
  config.py     Environment-driven settings
  models.py     Task, TaskState, serialization
  keys.py       Redis key naming — one place, so nothing drifts
  queue.py      Enqueue, lease, ack, retry, DLQ routing
  handlers.py   Handler registry + built-in handlers (noop, sleep, fail, flaky)
  worker.py     Worker pool, heartbeat/lease renewal, handler dispatch
  reaper.py     Lease-expiry scan and orphan requeue
  backoff.py    Exponential backoff with jitter
  api.py        FastAPI control plane
tests/
  test_queue.py     Lease/ack/nack/retry behaviour
  test_reaper.py    Lease expiry, double reaping, poison tasks
  test_worker.py    Heartbeats, stale owners, and killing a real worker process mid-task
  test_api.py       REST endpoints incl. DLQ replay
  test_recovery.py  The original spec tests
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

Try it:

```bash
curl -X POST localhost:8000/tasks -H 'content-type: application/json'      -d '{"handler": "sleep", "payload": {"seconds": 20}}'

docker compose kill worker        # kill workers mid-task
docker compose up -d worker       # ...the reaper puts the task back, a new worker finishes it

curl localhost:8000/tasks/<id>    # attempts: 1, last_error: "lease expired (owner: ...)"
```

Custom handlers:

```python
from taskqueue.handlers import handler

@handler("send_email")
def send_email(payload):
    ...  # must be idempotent, it can run more than once
```

## Tests

Tests run against a real Redis (no mocks — the atomicity is the thing being tested).
They default to db 15 and flush it, so don't point them at anything you care about.

```bash
docker compose up -d redis
pytest -v
```

## API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/tasks` | Enqueue a task |
| `GET` | `/tasks/{id}` | Task state, attempts, last error |
| `GET` | `/stats` | Queue depths (pending, processing, delayed, dlq) |
| `GET` | `/dlq?limit=&offset=` | List dead-lettered tasks, newest first |
| `POST` | `/dlq/{id}/replay` | Move a task from the DLQ back to pending |
| `GET` | `/health` | Redis connectivity check |

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

- [x] Implement `queue.py` lease/ack/retry paths
- [x] Implement `worker.py` heartbeat renewal
- [x] Implement `reaper.py` orphan scan
- [x] Crash-recovery test: kill a worker mid-task, assert redelivery
- [x] Delayed retries via a Redis sorted set instead of immediate requeue
- [x] CI with a Redis service container
- [ ] Priority queues
- [ ] Prometheus metrics endpoint
- [ ] Deploy to Cloud Run / ECS

## License

MIT
