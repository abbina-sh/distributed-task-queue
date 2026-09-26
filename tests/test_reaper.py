"""Reaper edge cases. The main orphan test lives in test_recovery.py."""

from __future__ import annotations

import time

import pytest

from taskqueue import keys, queue, reaper
from taskqueue.config import settings
from taskqueue.models import TaskState


@pytest.fixture(autouse=True)
def clean_redis():
    queue.client().flushdb()
    reaper._unclaimed_last_scan.clear()
    yield
    queue.client().flushdb()


def expire(task_id: str) -> None:
    queue.client().hset(keys.task(task_id), "lease_expires_at", str(time.time() - 1))


def test_live_lease_is_left_alone():
    queue.enqueue("noop", {})
    t = queue.lease("w1", timeout=1)

    assert reaper.reap_once() == 0
    assert queue.get(t.id).state == TaskState.PROCESSING


def test_reclaim_records_previous_owner():
    queue.enqueue("noop", {})
    t = queue.lease("dead-worker", timeout=1)
    expire(t.id)
    reaper.reap_once()

    after = queue.get(t.id)
    assert after.owner is None
    assert "dead-worker" in after.last_error


def test_reaped_worker_cannot_renew_or_ack():
    queue.enqueue("noop", {})
    t = queue.lease("slow-worker", timeout=1)
    expire(t.id)
    reaper.reap_once()

    assert not queue.renew_lease(t.id, "slow-worker")
    assert not queue.ack(t.id, owner="slow-worker")
    assert queue.get(t.id).state == TaskState.PENDING


def test_task_that_keeps_crashing_workers_is_dead_lettered():
    queue.enqueue("poison", {})

    for _ in range(settings.max_attempts):
        t = queue.lease("w", timeout=1)
        assert t is not None
        expire(t.id)
        reaper.reap_once()

    assert queue.get(t.id).state == TaskState.DEAD
    assert queue.stats()["dlq"] == 1
    assert queue.stats()["pending"] == 0


def test_two_reapers_reclaim_once():
    queue.enqueue("noop", {})
    t = queue.lease("w1", timeout=1)
    expire(t.id)

    assert reaper.reap_once() == 1
    assert reaper.reap_once() == 0
    assert queue.get(t.id).attempts == 1
    assert queue.stats()["pending"] == 1


def test_unclaimed_id_needs_two_scans():
    # worker died right after BLMOVE, before the claim script ran
    t = queue.enqueue("noop", {})
    queue.client().lmove(keys.PENDING, keys.PROCESSING, "RIGHT", "LEFT")

    assert reaper.reap_once() == 0   # might be a claim in progress
    assert reaper.reap_once() == 1   # still unclaimed, so it's orphaned
    assert queue.lease("w2", timeout=1).id == t.id


def test_dangling_id_is_dropped():
    queue.client().lpush(keys.PROCESSING, "ghost")
    assert reaper.reap_once() == 0
    assert queue.stats()["processing"] == 0
