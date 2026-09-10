"""Exponential backoff with full jitter.

Full jitter (rather than a fixed exponential) matters once you have more than one
worker: without it, a batch of tasks that fail together retries in lockstep and
hammers whatever just failed at exactly the same moment, repeatedly.

Reference: AWS Architecture Blog, "Exponential Backoff And Jitter".
"""

from __future__ import annotations

import random

from .config import settings


def delay_for_attempt(attempt: int) -> float:
    """Seconds to wait before retry number `attempt` (1-indexed).

    attempt=1 -> uniform(0, base)
    attempt=2 -> uniform(0, base*2)
    attempt=n -> uniform(0, min(base * 2**(n-1), max))
    """
    if attempt < 1:
        raise ValueError("attempt is 1-indexed")

    ceiling = min(
        settings.backoff_base_seconds * (2 ** (attempt - 1)),
        settings.backoff_max_seconds,
    )
    return random.uniform(0, ceiling)
