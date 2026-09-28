# 8. Testing

## Real Redis, no mocks

What's being tested here is mostly *atomicity*: Lua scripts, `BLMOVE`, blocking with
timeouts, sorted set ranges. A mock Redis (or `fakeredis`) would be checking my
understanding of Redis against itself. Running against a real Redis 7 means the
scripts actually execute.

Safety: `tests/conftest.py` defaults `REDIS_URL` to **db 15**, and every test calls
`flushdb()`. Don't point the tests at a Redis whose db 15 you care about.

## Fast timings

Settings are read from the environment when the modules are imported, so `conftest.py`
sets them before anything imports `taskqueue`:

| Setting | Test value | Why |
|---|---|---|
| `LEASE_TTL_SECONDS` | 2 | expiry tests wait seconds, not 30s |
| `LEASE_RENEW_INTERVAL` | 1 | still under the TTL, so `validate()` passes |
| `BACKOFF_BASE_SECONDS` | 0.05 | retry tests don't wait on backoff |
| `BACKOFF_MAX_SECONDS` | 0.2 | same |

## What's covered

| File | Focus |
|---|---|
| `test_queue.py` | FIFO, lease timeout, owner checks, delayed retries, dangling ids |
| `test_reaper.py` | live leases untouched, double reaping, poison tasks, the unclaimed gap |
| `test_worker.py` | ack/nack paths, heartbeat keeping a long task alive, stale owner, drain, **real crash** |
| `test_api.py` | endpoints, DLQ listing and replay, 404s, validation |
| `test_recovery.py` | the original spec tests from the scaffold |

## The crash test

`test_orphaned_task_is_redelivered` in `test_recovery.py` fakes a crash by editing
`lease_expires_at`. That checks the reaper logic but not that a dead process actually
stops heartbeating.

`test_killed_worker_process_task_is_redelivered` does it for real:

1. enqueue a 60s `sleep` task
2. start `python -m taskqueue.worker` as a subprocess
3. wait until the task is `processing`
4. `proc.kill()`: SIGKILL on Linux, TerminateProcess on Windows, no cleanup handlers run
5. reaper immediately → reclaims 0 (lease still valid)
6. wait out the TTL → reaper reclaims 1
7. lease again: same id, `attempts == 1`, `last_error` mentions lease expiry

Step 5 matters as much as step 6: it proves the reaper doesn't take a task back early.

## The heartbeat test

`test_heartbeat_keeps_long_task_alive` runs a task for TTL + 1.5s and calls the reaper
past the original expiry. It has to reclaim 0 and the task has to succeed with
`attempts == 0`. Without renewal this test fails, which is how you know it's testing
the heartbeat and not just passing.

## CI

`.github/workflows/tests.yml` runs the suite on Python 3.12 with a `redis:7-alpine`
service container on every push and PR.
