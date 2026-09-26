"""Handler registry and a few built-in handlers.

The registry lives here rather than in worker.py because `python -m
taskqueue.worker` runs worker.py as __main__. Anything that imported
`taskqueue.worker` to register a handler would get a second copy of the module
with its own empty dict, and the worker would never see the handler.

Handlers MUST be idempotent. Delivery is at-least-once: a worker can finish a
task and die before acking, and that task will run again.
"""

from __future__ import annotations

import random
import time
from typing import Callable

HANDLERS: dict[str, Callable[[dict], None]] = {}


def handler(name: str):
    """Register a task handler.

        @handler("send_email")
        def send_email(payload): ...
    """
    def decorator(fn: Callable[[dict], None]) -> Callable[[dict], None]:
        HANDLERS[name] = fn
        return fn
    return decorator


# --- Built-ins, mostly for testing/demoing -----------------------------------


@handler("noop")
def noop(payload: dict) -> None:
    pass


@handler("sleep")
def sleep(payload: dict) -> None:
    time.sleep(float(payload.get("seconds", 1)))


@handler("fail")
def fail(payload: dict) -> None:
    raise RuntimeError(payload.get("message", "task failed on purpose"))


@handler("flaky")
def flaky(payload: dict) -> None:
    if random.random() < float(payload.get("fail_rate", 0.5)):
        raise RuntimeError("flaky task failed")
