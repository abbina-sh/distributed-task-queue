"""Queue operations: enqueue, lease, ack, nack, DLQ routing.

This is the core of the project and is deliberately left unimplemented — the
contracts below are the spec. Each docstring states the invariant that matters;
if you satisfy those, the tests in tests/ should pass.
"""

from __future__ import annotations

import redis

from .config import settings
from .models import Task

_client: redis.Redis | None = None


def client() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.from_url(settings.redis_url, decode_responses=True)
    return _client


def enqueue(handler: str, payload: dict) -> Task:
    """Create a task and place it on the pending list.

    Invariant: the task hash must exist before the id is pushed to `pending`,
    or a worker can lease an id whose hash hasn't been written yet. Use a
    pipeline/MULTI so both happen atomically.
    """
    raise NotImplementedError


def lease(owner: str, timeout: int = 5) -> Task | None:
    """Atomically move one task id from `pending` to `processing` and claim it.

    Use BRPOPLPUSH (or BLMOVE) — the atomicity is the whole point. A two-step
    "pop then push" loses the task if the process dies between the steps.

    On success, set on the task hash:
        state            = PROCESSING
        owner            = owner
        lease_expires_at = now + settings.lease_ttl_seconds

    Returns None if `timeout` elapses with an empty queue.
    """
    raise NotImplementedError


def renew_lease(task_id: str, owner: str) -> bool:
    """Push `lease_expires_at` forward by lease_ttl_seconds.

    Must be a no-op returning False if `owner` no longer holds the task — a
    worker that stalled long enough to be reaped must not be able to reclaim a
    task another worker is now running.
    """
    raise NotImplementedError


def ack(task_id: str) -> None:
    """Mark succeeded and remove the id from `processing`.

    Note LREM is O(n) on list length; that's acceptable while `processing` stays
    small, and is a reason to keep lease TTLs short.
    """
    raise NotImplementedError


def nack(task_id: str, error: str) -> None:
    """Handle a failed attempt.

    Increment attempts, record last_error, then:
      - attempts < settings.max_attempts -> back to `pending` after
        backoff.delay_for_attempt(attempts)
      - otherwise                        -> state DEAD, push to `dlq`

    Removing from `processing` and adding to the destination must be atomic, or
    a crash between them duplicates or drops the task.
    """
    raise NotImplementedError


def get(task_id: str) -> Task:
    """Load a task by id. Raises KeyError if unknown."""
    raise NotImplementedError


def stats() -> dict[str, int]:
    """Return queue depths: {"pending": n, "processing": n, "dlq": n}."""
    raise NotImplementedError


def replay(task_id: str) -> None:
    """Move a task from the DLQ back to `pending` with attempts reset to 0."""
    raise NotImplementedError
