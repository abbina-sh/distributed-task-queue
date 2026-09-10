"""Worker pool.

A worker runs two things concurrently per leased task:
  1. the handler
  2. a heartbeat that renews the lease every settings.lease_renew_interval

If the process dies, the heartbeat stops, the lease expires, and the reaper
returns the task to `pending`. That is the entire crash-recovery mechanism —
there is no liveness detection anywhere.
"""

from __future__ import annotations

import signal
import socket
import os
from typing import Callable

HANDLERS: dict[str, Callable[[dict], None]] = {}


def handler(name: str):
    """Register a task handler.

    Handlers MUST be idempotent. Delivery is at-least-once: a worker can complete
    a task and die before acking, and that task will run again.

        @handler("send_email")
        def send_email(payload): ...
    """
    def decorator(fn: Callable[[dict], None]) -> Callable[[dict], None]:
        HANDLERS[name] = fn
        return fn
    return decorator


def worker_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


def run_once(owner: str) -> bool:
    """Lease one task, run its handler with heartbeat renewal, ack or nack.

    Returns True if a task was processed, False if the lease timed out empty.

    Ordering matters: ack only after the handler returns. Acking first turns a
    handler crash into silent data loss.
    """
    raise NotImplementedError


def run_forever() -> None:
    """Main loop. Must handle SIGTERM by finishing the in-flight task, then exiting.

    Killing mid-task is safe (the reaper recovers it) but redoing work you'd
    already finished is wasteful, so drain cleanly when asked politely.
    """
    raise NotImplementedError


if __name__ == "__main__":
    run_forever()
