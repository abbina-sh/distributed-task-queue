"""Redis key naming.

Kept in one module so key formats can't drift between the queue, the worker and
the reaper — a class of bug that is miserable to debug because nothing errors,
work just silently goes missing.
"""

PREFIX = "dtq"

PENDING = f"{PREFIX}:pending"        # list  — tasks waiting for a worker
PROCESSING = f"{PREFIX}:processing"  # list  — tasks currently leased
DLQ = f"{PREFIX}:dlq"                # list  — tasks that exhausted their attempts


def task(task_id: str) -> str:
    """Hash holding a single task's state."""
    return f"{PREFIX}:task:{task_id}"
