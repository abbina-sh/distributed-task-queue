"""REST control plane.

Deliberately thin: it exposes queue.py and owns no logic of its own. Keeping the
control plane dumb means the queue's invariants live in exactly one place.
"""

from __future__ import annotations

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from . import queue

app = FastAPI(title="Distributed Task Queue", version="0.1.0")


class EnqueueRequest(BaseModel):
    handler: str
    payload: dict = {}


@app.post("/tasks", status_code=201)
def create_task(req: EnqueueRequest):
    task = queue.enqueue(req.handler, req.payload)
    return {"id": task.id, "state": task.state}


@app.get("/tasks/{task_id}")
def get_task(task_id: str):
    try:
        t = queue.get(task_id)
    except KeyError:
        raise HTTPException(404, "task not found")
    return {
        "id": t.id,
        "handler": t.handler,
        "state": t.state,
        "attempts": t.attempts,
        "last_error": t.last_error,
        "owner": t.owner,
    }


@app.get("/stats")
def get_stats():
    return queue.stats()


@app.get("/dlq")
def list_dlq():
    raise NotImplementedError


@app.post("/dlq/{task_id}/replay")
def replay_task(task_id: str):
    queue.replay(task_id)
    return {"id": task_id, "state": "pending"}


@app.get("/health")
def health():
    try:
        queue.client().ping()
    except Exception as exc:
        raise HTTPException(503, f"redis unreachable: {exc}")
    return {"status": "ok"}
