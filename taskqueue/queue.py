"""Queue operations: enqueue, lease, ack, nack, DLQ routing.

Anything that moves a task id between lists *and* changes the task hash is done
in a Lua script. Redis runs a script atomically, so there's no window where a
crash leaves the id in one list while the hash says it's somewhere else.
"""

from __future__ import annotations

import time

import redis

from . import backoff, keys
from .config import settings
from .models import Task, TaskState

_client: redis.Redis | None = None
_scripts: dict[str, redis.commands.core.Script] = {}


def client() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.from_url(settings.redis_url, decode_responses=True)
    return _client


# --- Lua scripts -------------------------------------------------------------
#
# Every script re-checks state/owner before touching anything. The caller's view
# of the task may be stale (e.g. the reaper already took it back), and the
# script is the only place that check can be done without a race.

# KEYS: task hash, processing
# ARGV: task id, owner, now, lease_expires_at
# Returns the task hash as a flat list, or nil if the hash is gone.
_CLAIM = """
if redis.call('EXISTS', KEYS[1]) == 0 then
    redis.call('LREM', KEYS[2], 1, ARGV[1])
    return nil
end
redis.call('HSET', KEYS[1],
    'state', 'processing',
    'owner', ARGV[2],
    'updated_at', ARGV[3],
    'lease_expires_at', ARGV[4])
return redis.call('HGETALL', KEYS[1])
"""

# KEYS: task hash
# ARGV: owner, now, lease_expires_at
_RENEW = """
if redis.call('HGET', KEYS[1], 'state') ~= 'processing' then return 0 end
if redis.call('HGET', KEYS[1], 'owner') ~= ARGV[1] then return 0 end
redis.call('HSET', KEYS[1], 'lease_expires_at', ARGV[3], 'updated_at', ARGV[2])
return 1
"""

# KEYS: task hash, processing
# ARGV: task id, owner ('' = don't check), now
_ACK = """
if redis.call('HGET', KEYS[1], 'state') ~= 'processing' then return 0 end
if ARGV[2] ~= '' and redis.call('HGET', KEYS[1], 'owner') ~= ARGV[2] then return 0 end
redis.call('HSET', KEYS[1],
    'state', 'succeeded',
    'owner', '',
    'lease_expires_at', '',
    'updated_at', ARGV[3])
redis.call('LREM', KEYS[2], 1, ARGV[1])
return 1
"""

# KEYS: task hash, processing, delayed, dlq
# ARGV: task id, owner ('' = don't check), now, error, max_attempts, retry_at
_NACK = """
if redis.call('HGET', KEYS[1], 'state') ~= 'processing' then return nil end
if ARGV[2] ~= '' and redis.call('HGET', KEYS[1], 'owner') ~= ARGV[2] then return nil end

local attempts = redis.call('HINCRBY', KEYS[1], 'attempts', 1)
redis.call('HSET', KEYS[1],
    'owner', '',
    'lease_expires_at', '',
    'last_error', ARGV[4],
    'updated_at', ARGV[3])
redis.call('LREM', KEYS[2], 1, ARGV[1])

if attempts >= tonumber(ARGV[5]) then
    redis.call('HSET', KEYS[1], 'state', 'dead')
    redis.call('LPUSH', KEYS[4], ARGV[1])
    return 'dead'
end
redis.call('HSET', KEYS[1], 'state', 'pending')
redis.call('ZADD', KEYS[3], ARGV[6], ARGV[1])
return 'retry'
"""

# KEYS: delayed, pending
# ARGV: now, batch size
_PROMOTE = """
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for _, id in ipairs(due) do
    redis.call('ZREM', KEYS[1], id)
    redis.call('LPUSH', KEYS[2], id)
end
return #due
"""


# KEYS: task hash, dlq, pending
# ARGV: task id, now
_REPLAY = """
if redis.call('LREM', KEYS[2], 1, ARGV[1]) == 0 then return 0 end
redis.call('HSET', KEYS[1], 'state', 'pending', 'attempts', 0, 'updated_at', ARGV[2])
redis.call('LPUSH', KEYS[3], ARGV[1])
return 1
"""


def _script(name: str, source: str):
    # register_script caches the SHA and falls back to EVAL on NOSCRIPT, so a
    # Redis restart doesn't break us
    if name not in _scripts:
        _scripts[name] = client().register_script(source)
    return _scripts[name]


def _pairs_to_dict(flat: list[str]) -> dict[str, str]:
    return dict(zip(flat[::2], flat[1::2]))


# --- Public API --------------------------------------------------------------


def enqueue(handler: str, payload: dict) -> Task:
    """Create a task and place it on the pending list.

    The hash is written before the id is pushed (same MULTI), so a worker can
    never lease an id whose hash doesn't exist yet.
    """
    task = Task(handler=handler, payload=payload)
    pipe = client().pipeline(transaction=True)
    pipe.hset(keys.task(task.id), mapping=task.to_redis())
    pipe.lpush(keys.PENDING, task.id)
    pipe.execute()
    return task


def lease(owner: str, timeout: int = 5) -> Task | None:
    """Atomically move one task id from `pending` to `processing` and claim it.

    BLMOVE does the list move atomically, then _CLAIM stamps owner + lease on
    the hash. If we die between those two steps the id sits in `processing`
    with no lease, and the reaper picks it up once it's older than a TTL.

    Blocks in short slices so due retries in `delayed` get promoted while we
    wait, instead of sitting there until the next reaper tick.

    Returns None if `timeout` elapses with an empty queue.
    """
    deadline = time.time() + timeout

    while True:
        promote_due()

        remaining = deadline - time.time()
        if remaining <= 0:
            return None

        # don't block past the next retry's due time or it waits a full slice
        wait = min(remaining, 1.0)
        next_due = client().zrange(keys.DELAYED, 0, 0, withscores=True)
        if next_due:
            wait = max(0.01, min(wait, next_due[0][1] - time.time()))

        task_id = client().blmove(keys.PENDING, keys.PROCESSING, wait, "RIGHT", "LEFT")
        if task_id is None:
            continue

        now = time.time()
        raw = _script("claim", _CLAIM)(
            keys=[keys.task(task_id), keys.PROCESSING],
            args=[task_id, owner, now, now + settings.lease_ttl_seconds],
        )
        if raw is None:
            # orphaned id with no hash (someone deleted it) - already cleaned up
            continue
        return Task.from_redis(_pairs_to_dict(raw))


def renew_lease(task_id: str, owner: str) -> bool:
    """Push `lease_expires_at` forward by lease_ttl_seconds.

    No-op returning False if `owner` no longer holds the task — a worker that
    stalled long enough to be reaped must not be able to reclaim a task another
    worker is now running.
    """
    now = time.time()
    ok = _script("renew", _RENEW)(
        keys=[keys.task(task_id)],
        args=[owner, now, now + settings.lease_ttl_seconds],
    )
    return bool(ok)


def ack(task_id: str, owner: str | None = None) -> bool:
    """Mark succeeded and remove the id from `processing`.

    If `owner` is given, only acks when that owner still holds the lease. The
    worker always passes it, so a worker that got reaped can't ack a task that
    someone else is now running.

    LREM is O(n) on list length; that's fine while `processing` stays small,
    and is a reason to keep lease TTLs short.
    """
    ok = _script("ack", _ACK)(
        keys=[keys.task(task_id), keys.PROCESSING],
        args=[task_id, owner or "", time.time()],
    )
    return bool(ok)


def nack(task_id: str, error: str, owner: str | None = None) -> str | None:
    """Handle a failed attempt.

    Increments attempts and records last_error, then either schedules a retry
    (into `delayed`, due after the backoff delay) or dead-letters the task once
    max_attempts is hit.

    Returns "retry", "dead", or None if the task wasn't ours to nack.
    """
    attempts = int(client().hget(keys.task(task_id), "attempts") or 0)
    now = time.time()
    retry_at = now + backoff.delay_for_attempt(attempts + 1)

    return _script("nack", _NACK)(
        keys=[keys.task(task_id), keys.PROCESSING, keys.DELAYED, keys.DLQ],
        args=[task_id, owner or "", now, error, settings.max_attempts, retry_at],
    )


def promote_due(batch: int = 100) -> int:
    """Move retries whose backoff has elapsed from `delayed` onto `pending`."""
    return _script("promote", _PROMOTE)(
        keys=[keys.DELAYED, keys.PENDING], args=[time.time(), batch]
    )


def get(task_id: str) -> Task:
    """Load a task by id. Raises KeyError if unknown."""
    return Task.from_redis(client().hgetall(keys.task(task_id)))


def stats() -> dict[str, int]:
    """Queue depths by list."""
    pipe = client().pipeline(transaction=False)
    pipe.llen(keys.PENDING)
    pipe.llen(keys.PROCESSING)
    pipe.zcard(keys.DELAYED)
    pipe.llen(keys.DLQ)
    pending, processing, delayed, dlq = pipe.execute()
    return {
        "pending": pending,
        "processing": processing,
        "delayed": delayed,
        "dlq": dlq,
    }


def dead_letters(limit: int = 50, offset: int = 0) -> list[Task]:
    """Tasks in the DLQ, most recently dead-lettered first."""
    ids = client().lrange(keys.DLQ, offset, offset + limit - 1)
    pipe = client().pipeline(transaction=False)
    for task_id in ids:
        pipe.hgetall(keys.task(task_id))
    return [Task.from_redis(raw) for raw in pipe.execute() if raw]


def replay(task_id: str) -> bool:
    """Move a task from the DLQ back to `pending` with attempts reset to 0.

    last_error is kept so you can still see why it died the first time.
    Returns False if the task isn't in the DLQ.
    """
    ok = _script("replay", _REPLAY)(
        keys=[keys.task(task_id), keys.DLQ, keys.PENDING],
        args=[task_id, time.time()],
    )
    return bool(ok)
