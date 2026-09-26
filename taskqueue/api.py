"""REST control plane.

Deliberately thin: it exposes queue.py and owns no logic of its own. Keeping the
control plane dumb means the queue's invariants live in exactly one place.
"""

from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from . import queue
from .models import Task

app = FastAPI(title="Distributed Task Queue", version="0.1.0")


class EnqueueRequest(BaseModel):
    handler: str = Field(min_length=1)
    payload: dict = {}


def task_json(t: Task) -> dict:
    return {
        "id": t.id,
        "handler": t.handler,
        "payload": t.payload,
        "state": t.state,
        "attempts": t.attempts,
        "last_error": t.last_error,
        "owner": t.owner,
        "created_at": t.created_at,
        "updated_at": t.updated_at,
    }


@app.post("/tasks", status_code=201)
def create_task(req: EnqueueRequest):
    # no check that the handler exists - handlers live in the worker processes,
    # the API has no way of knowing what they've registered
    task = queue.enqueue(req.handler, req.payload)
    return {"id": task.id, "state": task.state}


@app.get("/tasks/{task_id}")
def get_task(task_id: str):
    try:
        t = queue.get(task_id)
    except KeyError:
        raise HTTPException(404, "task not found")
    return task_json(t)


@app.get("/stats")
def get_stats():
    return queue.stats()


@app.get("/dlq")
def list_dlq(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    return [task_json(t) for t in queue.dead_letters(limit, offset)]


@app.post("/dlq/{task_id}/replay")
def replay_task(task_id: str):
    if not queue.replay(task_id):
        raise HTTPException(404, "task not in dead-letter queue")
    return {"id": task_id, "state": "pending"}


@app.get("/health")
def health():
    try:
        queue.client().ping()
    except Exception as exc:
        raise HTTPException(503, f"redis unreachable: {exc}")
    return {"status": "ok"}
