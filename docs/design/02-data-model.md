# 2. Data model

All keys are defined in `taskqueue/keys.py` and nowhere else. If the worker and reaper
spelled a key differently nothing would error, tasks would just vanish, so there's one
source of truth.

## Keys

| Key | Type | Holds |
|---|---|---|
| `dtq:task:<id>` | hash | the task itself (see fields below) |
| `dtq:pending` | list | ids waiting for a worker |
| `dtq:processing` | list | ids currently leased |
| `dtq:delayed` | sorted set | ids waiting out a retry backoff, scored by due time |
| `dtq:dlq` | list | ids that ran out of attempts |

The lists only hold **ids**. All task data lives in the hash. That keeps list
operations cheap and means there's exactly one copy of each task's state.

**Why lists for pending?** `LPUSH` + `BLMOVE ... RIGHT LEFT` gives FIFO with a
blocking pop, so idle workers don't poll. Redis Streams with consumer groups would
also work (they have a built-in pending-entries list, similar to `processing`), but
building it on lists makes every piece of the at-least-once machinery explicit, which
was the point.

## Task hash fields

| Field | Meaning |
|---|---|
| `id`, `handler`, `payload` | what to run; payload is JSON since hash values are strings |
| `state` | `pending` / `processing` / `succeeded` / `dead` |
| `attempts` | failed attempts so far (nack or lease expiry) |
| `owner` | `hostname:pid:thread` of the worker holding the lease, empty otherwise |
| `lease_expires_at` | unix time; empty when not leased |
| `last_error` | most recent failure message |
| `created_at`, `updated_at` | unix timestamps |

`attempts` lives on the hash rather than in the payload so a requeue is just a list
move plus `HINCRBY`, and changing the retry policy doesn't require rewriting tasks
that are already queued.

## State machine

```
                  enqueue
                     │
                     v
   ┌──────────> PENDING ──lease──> PROCESSING ──ack──> SUCCEEDED
   │               ^                  │
   │               │                  ├── nack, attempts < max ──> PENDING (via delayed)
   │   replay      │                  ├── lease expiry, attempts < max ──> PENDING
   │               │                  ├── nack, attempts = max ──> DEAD
   │               │                  └── lease expiry, attempts = max ──> DEAD
   │               │                                                        │
   └───────────── DEAD <────────────────────────────────────────────────────┘
```

Where the id sits for each state:

| State | Id is in |
|---|---|
| pending | `pending` list, or `delayed` zset if waiting on backoff |
| processing | `processing` list |
| succeeded | nowhere (hash only) |
| dead | `dlq` list |

There's one brief exception: between `BLMOVE` and the claim script, an id is in
`processing` while its hash still says `pending`. See [the reaper](06-reaper.md) for how
that's handled.

## What isn't cleaned up

Succeeded task hashes stay in Redis forever. See [limitations](../limitations.md).
