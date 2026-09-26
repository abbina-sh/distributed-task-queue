"""Shared test setup.

Settings are read from the environment at import time, so these have to be set
before anything imports taskqueue. Short timings keep the suite fast.

Defaults to db 15 so running the tests against a local Redis doesn't flush
whatever else is living in db 0.
"""

import os

os.environ.setdefault("REDIS_URL", "redis://localhost:6379/15")
os.environ.setdefault("BACKOFF_BASE_SECONDS", "0.05")
os.environ.setdefault("BACKOFF_MAX_SECONDS", "0.2")
os.environ.setdefault("LEASE_TTL_SECONDS", "2")
os.environ.setdefault("LEASE_RENEW_INTERVAL", "1")
