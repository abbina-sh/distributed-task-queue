"""Environment-driven configuration.

Everything tunable lives here so tests can override a single object rather than
patching module-level constants scattered across the codebase.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    # Retry policy
    max_attempts: int = int(os.getenv("MAX_ATTEMPTS", "5"))
    backoff_base_seconds: float = float(os.getenv("BACKOFF_BASE_SECONDS", "1.0"))
    backoff_max_seconds: float = float(os.getenv("BACKOFF_MAX_SECONDS", "300.0"))

    # Lease / heartbeat policy.
    # lease_ttl must be comfortably larger than lease_renew_interval, or a live
    # worker will have its task reaped out from under it during a slow tick.
    lease_ttl_seconds: int = int(os.getenv("LEASE_TTL_SECONDS", "30"))
    lease_renew_interval: int = int(os.getenv("LEASE_RENEW_INTERVAL", "10"))
    reaper_interval: int = int(os.getenv("REAPER_INTERVAL", "5"))

    worker_concurrency: int = int(os.getenv("WORKER_CONCURRENCY", "4"))

    def validate(self) -> None:
        if self.lease_renew_interval >= self.lease_ttl_seconds:
            raise ValueError(
                "LEASE_RENEW_INTERVAL must be smaller than LEASE_TTL_SECONDS, "
                "otherwise a healthy worker's lease can expire between renewals"
            )
        if self.max_attempts < 1:
            raise ValueError("MAX_ATTEMPTS must be at least 1")


settings = Settings()
