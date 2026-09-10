"""Lease reaper — recovers tasks orphaned by crashed workers.

Runs as its own process. Scans `processing` for tasks whose lease has expired
and returns them to `pending`.

Running several reapers is safe as long as the reclaim is atomic per task:
two reapers must not both requeue the same task. Guard it with a Lua script or
a per-task WATCH.
"""

from __future__ import annotations

import time

from .config import settings


def reap_once() -> int:
    """Requeue every task in `processing` whose lease_expires_at < now.

    For each expired task: increment attempts, clear owner and lease_expires_at,
    set state back to PENDING, move the id from `processing` to `pending`.

    A task that has now exhausted settings.max_attempts goes to the DLQ instead —
    otherwise a task that reliably kills its worker requeues forever.

    Returns the number of tasks reclaimed.
    """
    raise NotImplementedError


def run_forever() -> None:
    while True:
        reclaimed = reap_once()
        if reclaimed:
            print(f"reaper: reclaimed {reclaimed} orphaned task(s)")
        time.sleep(settings.reaper_interval)


if __name__ == "__main__":
    run_forever()
