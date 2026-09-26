"""Unit-ish tests for queue.py against a real Redis."""

from __future__ import annotations

import time

import pytest

from taskqueue import keys, queue
from taskqueue.config import settings
from taskqueue.models import TaskState


@pytest.fixture(autouse=True)
def clean_redis():
    queue.client().flushdb()
    yield
    queue.client().flushdb()


def test_lease_is_fifo():
    first = queue.enqueue("noop", {"n": 1})
    second = queue.enqueue("noop", {"n": 2})

    assert queue.lease("w1", timeout=1).id == first.id
    assert queue.lease("w1", timeout=1).id == second.id


def test_lease_times_out_on_empty_queue():
    start = time.time()
    assert queue.lease("w1", timeout=1) is None
    assert time.time() - start >= 0.9


def test_lease_sets_owner_and_expiry():
    queue.enqueue("noop", {})
    before = time.time()
    t = queue.lease("w1", timeout=1)

    assert t.owner == "w1"
    assert t.lease_expires_at >= before + settings.lease_ttl_seconds
    assert queue.stats()["processing"] == 1
    assert queue.stats()["pending"] == 0


def test_payload_round_trips():
    payload = {"to": "a@b.com", "nested": {"xs": [1, 2, 3]}}
    queue.enqueue("send", payload)
    assert queue.lease("w1", timeout=1).payload == payload


def test_get_unknown_task_raises():
    with pytest.raises(KeyError):
        queue.get("does-not-exist")


def test_renew_extends_lease():
    queue.enqueue("noop", {})
    t = queue.lease("w1", timeout=1)
    time.sleep(0.1)

    assert queue.renew_lease(t.id, "w1")
    assert queue.get(t.id).lease_expires_at > t.lease_expires_at


def test_renew_rejected_for_wrong_owner():
    queue.enqueue("noop", {})
    t = queue.lease("w1", timeout=1)

    assert not queue.renew_lease(t.id, "someone-else")
    assert queue.get(t.id).lease_expires_at == t.lease_expires_at


def test_ack_with_wrong_owner_is_ignored():
    queue.enqueue("noop", {})
    t = queue.lease("w1", timeout=1)

    assert not queue.ack(t.id, owner="stale-worker")
    assert queue.get(t.id).state == TaskState.PROCESSING


def test_nack_schedules_delayed_retry():
    queue.enqueue("flaky", {})
    t = queue.lease("w1", timeout=1)

    assert queue.nack(t.id, "boom", owner="w1") == "retry"

    after = queue.get(t.id)
    assert after.state == TaskState.PENDING
    assert after.attempts == 1
    assert after.last_error == "boom"
    assert after.owner is None
    assert queue.stats()["delayed"] == 1
    assert queue.stats()["processing"] == 0


def test_retry_is_redelivered_after_backoff():
    queue.enqueue("flaky", {})
    t = queue.lease("w1", timeout=1)
    queue.nack(t.id, "boom")

    again = queue.lease("w2", timeout=2)
    assert again is not None
    assert again.id == t.id
    assert again.attempts == 1


def test_nack_after_ack_is_noop():
    queue.enqueue("noop", {})
    t = queue.lease("w1", timeout=1)
    queue.ack(t.id)

    assert queue.nack(t.id, "late failure") is None
    assert queue.get(t.id).state == TaskState.SUCCEEDED


def test_lease_skips_ids_with_no_hash():
    # simulates a hash that got deleted out from under the queue
    queue.client().lpush(keys.PENDING, "ghost")
    real = queue.enqueue("noop", {})

    assert queue.lease("w1", timeout=1).id == real.id
    assert queue.stats()["processing"] == 1
