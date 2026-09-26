"""Lease reaper — recovers tasks orphaned by crashed workers.

Runs as its own process. Scans `processing` for tasks whose lease has expired
and returns them to `pending`.

Running several reapers is safe: the reclaim is a Lua script that re-checks the
lease inside Redis, so two reapers can't both requeue the same task. The second
one sees the task is no longer processing and does nothing.
"""

from __future__ import annotations

import time

from . import keys, queue
from .config import settings

# KEYS: task hash, processing, pending, dlq
# ARGV: task id, now, max_attempts, allow_unclaimed ('1' or '0')
#
# Returns 1 if the task was reclaimed, 0 otherwise.
_RECLAIM = """
local state = redis.call('HGET', KEYS[1], 'state')

if not state then
    -- hash is gone, just drop the dangling id
    redis.call('LREM', KEYS[2], 1, ARGV[1])
    return 0
end

if state == 'processing' then
    local expires = tonumber(redis.call('HGET', KEYS[1], 'lease_expires_at'))
    if expires and expires >= tonumber(ARGV[2]) then return 0 end
elseif state == 'pending' then
    -- in processing but never claimed: the worker died between BLMOVE and
    -- the claim script. Only touch it if the caller has seen it before,
    -- otherwise we might be racing a claim that's happening right now.
    if ARGV[4] ~= '1' then return 0 end
else
    return 0
end

local owner = redis.call('HGET', KEYS[1], 'owner') or ''
local attempts = redis.call('HINCRBY', KEYS[1], 'attempts', 1)
redis.call('HSET', KEYS[1],
    'owner', '',
    'lease_expires_at', '',
    'last_error', 'lease expired (owner: ' .. owner .. ')',
    'updated_at', ARGV[2])
redis.call('LREM', KEYS[2], 1, ARGV[1])

-- a task that keeps killing its worker would otherwise loop forever
if attempts >= tonumber(ARGV[3]) then
    redis.call('HSET', KEYS[1], 'state', 'dead')
    redis.call('LPUSH', KEYS[4], ARGV[1])
else
    redis.call('HSET', KEYS[1], 'state', 'pending')
    redis.call('LPUSH', KEYS[3], ARGV[1])
end
return 1
"""

# ids seen in `processing` without a claim on the previous scan
_unclaimed_last_scan: set[str] = set()


def reap_once() -> int:
    """Requeue every task in `processing` whose lease_expires_at < now.

    Expired tasks get attempts += 1 and go back to `pending`, or to the DLQ if
    that was their last attempt.

    Returns the number of tasks reclaimed.
    """
    global _unclaimed_last_scan

    c = queue.client()
    reclaim = queue._script("reclaim", _RECLAIM)
    now = time.time()
    reclaimed = 0
    unclaimed_now = set()

    for task_id in c.lrange(keys.PROCESSING, 0, -1):
        if c.hget(keys.task(task_id), "state") == "pending":
            unclaimed_now.add(task_id)

        allow_unclaimed = "1" if task_id in _unclaimed_last_scan else "0"
        if reclaim(
            keys=[keys.task(task_id), keys.PROCESSING, keys.PENDING, keys.DLQ],
            args=[task_id, now, settings.max_attempts, allow_unclaimed],
        ):
            reclaimed += 1
            unclaimed_now.discard(task_id)

    _unclaimed_last_scan = unclaimed_now

    # workers promote due retries too, but if every worker is down they'd
    # sit in `delayed` and never show up in the pending count
    queue.promote_due()

    return reclaimed


def run_forever() -> None:
    settings.validate()
    print(f"reaper: scanning every {settings.reaper_interval}s")
    while True:
        try:
            reclaimed = reap_once()
            if reclaimed:
                print(f"reaper: reclaimed {reclaimed} orphaned task(s)")
        except Exception as exc:
            # redis blip - don't die, just try again next tick
            print(f"reaper: scan failed: {exc}")
        time.sleep(settings.reaper_interval)


if __name__ == "__main__":
    run_forever()
