"""Task model and state machine.

State transitions:

    PENDING ──lease──> PROCESSING ──ack────> SUCCEEDED
                            │
                            ├──nack, attempts < max──> PENDING
                            ├──nack, attempts >= max─> DEAD
                            └──lease expiry──────────> PENDING (attempts += 1)
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any


class TaskState(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    SUCCEEDED = "succeeded"
    DEAD = "dead"


@dataclass
class Task:
    handler: str
    payload: dict[str, Any] = field(default_factory=dict)

    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    state: TaskState = TaskState.PENDING
    attempts: int = 0
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)

    # Set while leased; None otherwise. The reaper compares this to wall clock.
    lease_expires_at: float | None = None
    owner: str | None = None

    last_error: str | None = None

    def to_redis(self) -> dict[str, str]:
        """Flatten to a Redis hash. Redis stores strings, so nested values are JSON."""
        d = asdict(self)
        d["state"] = self.state.value
        d["payload"] = json.dumps(self.payload)
        return {k: ("" if v is None else str(v)) for k, v in d.items()}

    @classmethod
    def from_redis(cls, raw: dict[str, str]) -> "Task":
        if not raw:
            raise KeyError("task not found")

        def opt_float(v: str) -> float | None:
            return float(v) if v else None

        return cls(
            id=raw["id"],
            handler=raw["handler"],
            payload=json.loads(raw["payload"]) if raw.get("payload") else {},
            state=TaskState(raw["state"]),
            attempts=int(raw["attempts"]),
            created_at=float(raw["created_at"]),
            updated_at=float(raw["updated_at"]),
            lease_expires_at=opt_float(raw.get("lease_expires_at", "")),
            owner=raw.get("owner") or None,
            last_error=raw.get("last_error") or None,
        )
