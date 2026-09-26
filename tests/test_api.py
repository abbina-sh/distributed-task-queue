"""API tests. Hits the real queue underneath, just through FastAPI's TestClient."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from taskqueue import queue, worker
from taskqueue.api import app
from taskqueue.config import settings

api = TestClient(app)


@pytest.fixture(autouse=True)
def clean_redis():
    queue.client().flushdb()
    yield
    queue.client().flushdb()


def kill_task(handler="fail"):
    """Enqueue a task and fail it until it lands in the DLQ."""
    task_id = api.post("/tasks", json={"handler": handler}).json()["id"]
    for _ in range(settings.max_attempts):
        worker.run_once("w1", timeout=2)
    return task_id


def test_create_and_get_task():
    r = api.post("/tasks", json={"handler": "noop", "payload": {"x": 1}})
    assert r.status_code == 201
    task_id = r.json()["id"]

    body = api.get(f"/tasks/{task_id}").json()
    assert body["handler"] == "noop"
    assert body["payload"] == {"x": 1}
    assert body["state"] == "pending"
    assert body["attempts"] == 0


def test_get_unknown_task_404():
    assert api.get("/tasks/nope").status_code == 404


def test_empty_handler_rejected():
    assert api.post("/tasks", json={"handler": ""}).status_code == 422


def test_stats():
    api.post("/tasks", json={"handler": "noop"})
    api.post("/tasks", json={"handler": "noop"})
    assert api.get("/stats").json() == {
        "pending": 2, "processing": 0, "delayed": 0, "dlq": 0,
    }


def test_dlq_lists_dead_tasks():
    task_id = kill_task()

    dead = api.get("/dlq").json()
    assert [t["id"] for t in dead] == [task_id]
    assert dead[0]["state"] == "dead"
    assert dead[0]["attempts"] == settings.max_attempts


def test_replay_moves_task_back_to_pending():
    task_id = kill_task()

    r = api.post(f"/dlq/{task_id}/replay")
    assert r.status_code == 200

    body = api.get(f"/tasks/{task_id}").json()
    assert body["state"] == "pending"
    assert body["attempts"] == 0
    assert body["last_error"]  # kept for debugging
    assert api.get("/stats").json()["dlq"] == 0

    # and it actually runs again
    queue.client().hset(f"dtq:task:{task_id}", "handler", "noop")
    worker.run_once("w1")
    assert api.get(f"/tasks/{task_id}").json()["state"] == "succeeded"


def test_replay_task_not_in_dlq_404():
    task_id = api.post("/tasks", json={"handler": "noop"}).json()["id"]
    assert api.post(f"/dlq/{task_id}/replay").status_code == 404


def test_health():
    assert api.get("/health").json() == {"status": "ok"}
