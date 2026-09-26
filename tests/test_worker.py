"""Worker tests, including actually killing a worker process mid-task."""

from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from taskqueue import keys, queue, reaper, worker
from taskqueue.config import settings
from taskqueue.handlers import handler
from taskqueue.models import TaskState

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(autouse=True)
def clean_redis():
    queue.client().flushdb()
    reaper._unclaimed_last_scan.clear()
    yield
    queue.client().flushdb()


def wait_for_state(task_id, state, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if queue.get(task_id).state == state:
            return
        time.sleep(0.05)
    raise AssertionError(f"task {task_id} never reached {state}")


def test_run_once_acks_on_success():
    t = queue.enqueue("noop", {})
    assert worker.run_once("w1")
    assert queue.get(t.id).state == TaskState.SUCCEEDED


def test_run_once_returns_false_when_empty():
    assert not worker.run_once("w1", timeout=1)


def test_handler_exception_is_nacked_with_error():
    t = queue.enqueue("fail", {"message": "nope"})
    worker.run_once("w1")

    after = queue.get(t.id)
    assert after.state == TaskState.PENDING
    assert after.attempts == 1
    assert after.last_error == "RuntimeError: nope"


def test_unknown_handler_is_nacked():
    t = queue.enqueue("does_not_exist", {})
    worker.run_once("w1")
    assert "no handler registered" in queue.get(t.id).last_error


def test_heartbeat_keeps_long_task_alive():
    # task runs longer than the lease TTL, reaper must not steal it
    t = queue.enqueue("sleep", {"seconds": settings.lease_ttl_seconds + 1.5})
    th = threading.Thread(target=worker.run_once, args=("w1",))
    th.start()

    wait_for_state(t.id, TaskState.PROCESSING)
    time.sleep(settings.lease_ttl_seconds + 0.5)
    assert reaper.reap_once() == 0

    th.join()
    assert queue.get(t.id).state == TaskState.SUCCEEDED
    assert queue.get(t.id).attempts == 0


def test_stalled_worker_cannot_ack_after_being_reaped():
    release = threading.Event()

    @handler("stall")
    def stall(payload):
        release.wait(5)

    t = queue.enqueue("stall", {})
    th = threading.Thread(target=worker.run_once, args=("slow",))
    th.start()
    wait_for_state(t.id, TaskState.PROCESSING)

    # pretend the lease expired and the reaper handed it to someone else
    queue.client().hset(keys.task(t.id), "lease_expires_at", str(time.time() - 1))
    reaper.reap_once()
    other = queue.lease("fast", timeout=1)
    assert other.id == t.id

    release.set()
    th.join()

    # the slow worker's ack got rejected, fast still owns it
    after = queue.get(t.id)
    assert after.state == TaskState.PROCESSING
    assert after.owner == "fast"


def test_run_forever_drains_on_stop():
    t = queue.enqueue("sleep", {"seconds": 1})
    stop = threading.Event()
    th = threading.Thread(target=worker.run_forever, args=(stop,))
    th.start()

    wait_for_state(t.id, TaskState.PROCESSING)
    stop.set()
    th.join(timeout=10)

    assert not th.is_alive()
    assert queue.get(t.id).state == TaskState.SUCCEEDED


def test_killed_worker_process_task_is_redelivered():
    """The real version of test_orphaned_task_is_redelivered: start an actual
    worker process, hard-kill it mid-task, and check nothing is lost."""
    t = queue.enqueue("sleep", {"seconds": 60})

    env = {**os.environ, "WORKER_CONCURRENCY": "1"}
    proc = subprocess.Popen(
        [sys.executable, "-m", "taskqueue.worker"],
        cwd=ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        wait_for_state(t.id, TaskState.PROCESSING)
    finally:
        proc.kill()  # SIGKILL / TerminateProcess, no chance to clean up
        proc.wait()

    assert reaper.reap_once() == 0  # lease is still valid right after the crash
    time.sleep(settings.lease_ttl_seconds + 0.5)
    assert reaper.reap_once() == 1

    redelivered = queue.lease("w2", timeout=1)
    assert redelivered.id == t.id
    assert redelivered.attempts == 1
    assert "lease expired" in redelivered.last_error
