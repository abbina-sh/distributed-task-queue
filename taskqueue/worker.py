"""Worker pool.

A worker runs two things concurrently per leased task:
  1. the handler
  2. a heartbeat that renews the lease every settings.lease_renew_interval

If the process dies, the heartbeat stops, the lease expires, and the reaper
returns the task to `pending`. That is the entire crash-recovery mechanism —
there is no liveness detection anywhere.

Concurrency is threads, one lease loop per thread. Handlers are expected to be
mostly I/O (HTTP calls, DB writes), so the GIL isn't a problem. For CPU-heavy
work run more worker processes instead of raising WORKER_CONCURRENCY.
"""

from __future__ import annotations

import os
import signal
import socket
import threading
import traceback

from . import queue
from .config import settings
from .handlers import HANDLERS, handler  # noqa: F401  (re-exported)
from .models import Task


def worker_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


def _heartbeat(task: Task, owner: str, done: threading.Event) -> None:
    # Event.wait doubles as the sleep, so we stop as soon as the handler returns
    while not done.wait(settings.lease_renew_interval):
        if not queue.renew_lease(task.id, owner):
            # we got reaped (probably stalled past the TTL). the handler keeps
            # running since we can't kill a thread, but ack/nack will be
            # rejected because we're no longer the owner
            print(f"{owner}: lost lease on {task.id}")
            return


def run_once(owner: str, timeout: int = 1) -> bool:
    """Lease one task, run its handler with heartbeat renewal, ack or nack.

    Returns True if a task was processed, False if the lease timed out empty.

    Ordering matters: ack only after the handler returns. Acking first turns a
    handler crash into silent data loss.
    """
    task = queue.lease(owner, timeout=timeout)
    if task is None:
        return False

    fn = HANDLERS.get(task.handler)
    if fn is None:
        queue.nack(task.id, f"no handler registered for {task.handler!r}", owner=owner)
        return True

    done = threading.Event()
    hb = threading.Thread(target=_heartbeat, args=(task, owner, done), daemon=True)
    hb.start()

    try:
        fn(task.payload)
    except Exception as exc:
        queue.nack(task.id, f"{type(exc).__name__}: {exc}", owner=owner)
    else:
        queue.ack(task.id, owner=owner)
    finally:
        done.set()
        hb.join()

    return True


def _loop(owner: str, stop: threading.Event) -> None:
    while not stop.is_set():
        try:
            run_once(owner)
        except Exception:
            # usually redis going away. back off a bit instead of spinning
            traceback.print_exc()
            stop.wait(1)


def run_forever(stop: threading.Event | None = None) -> None:
    """Main loop. Handles SIGTERM/SIGINT by finishing in-flight tasks, then exiting.

    Killing mid-task is safe (the reaper recovers it) but redoing work you'd
    already finished is wasteful, so drain cleanly when asked politely.

    `stop` is only there so tests can shut it down without sending signals.
    """
    settings.validate()
    stop = stop or threading.Event()

    if threading.current_thread() is threading.main_thread():
        def on_signal(signum, _frame):
            print(f"worker: got signal {signum}, draining")
            stop.set()

        signal.signal(signal.SIGTERM, on_signal)
        signal.signal(signal.SIGINT, on_signal)

    base = worker_id()
    threads = [
        threading.Thread(target=_loop, args=(f"{base}:{i}", stop), name=f"worker-{i}")
        for i in range(settings.worker_concurrency)
    ]
    for t in threads:
        t.start()
    print(f"worker {base}: {len(threads)} thread(s), handlers: {sorted(HANDLERS)}")

    # join with a timeout so the main thread can still receive signals
    while any(t.is_alive() for t in threads):
        for t in threads:
            t.join(0.5)

    print(f"worker {base}: stopped")


if __name__ == "__main__":
    run_forever()
