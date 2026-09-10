"""The tests that matter.

The first three are ordinary. `test_orphaned_task_is_redelivered` is the one that
justifies the whole design — write the implementation until it passes, and you
have something real to talk about in an interview.

Run: pytest -v   (needs Redis on REDIS_URL)
"""

from __future__ import annotations

import time

import pytest

from taskqueue import queue, reaper
from taskqueue.config import settings
from taskqueue.models import TaskState


@pytest.fixture(autouse=True)
def clean_redis():
    queue.client().flushdb()
    yield
    queue.client().flushdb()


def test_enqueue_then_lease_returns_same_task():
    created = queue.enqueue("noop", {"n": 1})
    leased = queue.lease(owner="w1", timeout=1)
    assert leased is not None
    assert leased.id == created.id
    assert leased.state == TaskState.PROCESSING


def test_ack_marks_succeeded_and_clears_processing():
    queue.enqueue("noop", {})
    t = queue.lease(owner="w1", timeout=1)
    queue.ack(t.id)

    assert queue.get(t.id).state == TaskState.SUCCEEDED
    assert queue.stats()["processing"] == 0


def test_task_dead_letters_after_max_attempts():
    queue.enqueue("always_fails", {})

    for _ in range(settings.max_attempts):
        t = queue.lease(owner="w1", timeout=1)
        assert t is not None
        queue.nack(t.id, error="boom")
        time.sleep(settings.backoff_base_seconds * 2)

    assert queue.get(t.id).state == TaskState.DEAD
    assert queue.stats()["dlq"] == 1


def test_orphaned_task_is_redelivered():
    """Simulate a worker crash: lease a task, then never renew or ack it.

    Once the lease expires the reaper must return it to pending so another
    worker picks it up. No work is lost.
    """
    original = queue.enqueue("slow", {})
    leased = queue.lease(owner="worker-that-dies", timeout=1)
    assert leased.id == original.id

    # The worker "crashes" — no renew_lease, no ack. Force expiry.
    queue.client().hset(
        f"dtq:task:{leased.id}", "lease_expires_at", str(time.time() - 1)
    )

    assert reaper.reap_once() == 1

    redelivered = queue.lease(owner="worker-2", timeout=1)
    assert redelivered is not None
    assert redelivered.id == original.id
    assert redelivered.attempts == 1
