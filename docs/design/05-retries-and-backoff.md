# 5. Retries and backoff

## What counts as a failed attempt

- the handler raised → worker calls `nack` → **delayed retry**
- the lease expired (worker died or stalled) → reaper → **immediate requeue**
- no handler registered for the task's name → `nack` → delayed retry

All three increment `attempts`. At `MAX_ATTEMPTS` (default 5) the task goes to the DLQ.

Lease expiry counts as an attempt on purpose: a task that reliably crashes its worker
(segfault in a C extension, OOM on a huge payload) would otherwise cycle forever,
taking down a worker every time. That's a "poison message", and the attempt cap is the
defence against it.

Reaper requeues skip the backoff because the task has already waited out a full
lease TTL (30s by default), which is longer than the early backoff delays anyway.

## Full-jitter exponential backoff

```
delay(n) = uniform(0, min(base * 2^(n-1), max))
```

With base 1s and max 300s: attempt 1 waits 0–1s, attempt 2 waits 0–2s, attempt 3
waits 0–4s, and so on up to 0–300s.

**Why exponential:** if a dependency is down, hammering it doesn't help. Each
failure makes the next wait longer.

**Why jitter:** suppose 200 tasks all fail at once because the database blipped.
Without jitter they all retry at exactly t+1s, then t+3s, then t+7s: synchronised
spikes against a system that's trying to recover. Randomising over the whole window
spreads them out. "Full" jitter (random over `[0, cap]`) spreads them more than
"equal" jitter (`cap/2 + random(0, cap/2)`), and the AWS Architecture Blog post that
compares them found it did the least total work.

## Where retries wait: the `delayed` sorted set

The scaffold's spec had failed tasks go straight back onto `pending`, which means the
backoff delay would never actually be applied. Instead retries wait in a sorted set,
`dtq:delayed`, scored by the retry's due time:

```
nack   ──ZADD delayed (now + delay) id──>  delayed
promote ──ZRANGEBYSCORE -inf now──> ZREM + LPUSH pending
```

Options considered:

| Option | Problem |
|---|---|
| Worker sleeps before nacking | holds a worker slot and the lease while doing nothing |
| `time.sleep` then requeue in a thread | lost if the process dies |
| Redis key with TTL + keyspace notifications | notifications are fire-and-forget; missed if nobody's subscribed |
| **Sorted set scored by due time** | durable, and "what's due?" is one range query |

## Who promotes due retries

Nothing in Redis moves them automatically, so two things call `promote_due()`:

1. **`lease()`**, before every blocking slice. An idle worker would otherwise block on
   an empty `pending` list while a retry sits due in `delayed`. `lease()` also caps
   how long it blocks at the time until the next retry is due, so retries aren't
   delayed by up to a full second.
2. **The reaper**, every scan. That way retries still show up in `pending` (and in
   `/stats`) when every worker is down.

`_PROMOTE` is a Lua script, so concurrent promoters can't double-push an id.
