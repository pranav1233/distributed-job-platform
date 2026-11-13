"""Redis cache-aside for job reads.

Design points:
  * Short TTL for active states, long TTL for terminal states.
  * Every state transition invalidates the key. Invalidation records the new row version
    in a tombstone key, and a fill is rejected when it carries an older version. This
    closes the race where a reader loads the old row, a transition commits and
    invalidates, and the reader then writes the stale row back.
  * Stampede protection: on a miss one caller takes a short SET NX lock and loads from
    Postgres, the others briefly poll the cache and fall back to Postgres if it stays
    empty.
  * Fail-open: any Redis error degrades to a direct database read.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import structlog
from redis.asyncio import Redis

from jobplatform import metrics
from jobplatform.domain.states import TERMINAL_STATES, JobStatus
from jobplatform.repo.records import JobRecord

log = structlog.get_logger(__name__)

# KEYS[1]=data key, KEYS[2]=version tombstone, ARGV[1]=payload, ARGV[2]=version, ARGV[3]=ttl
_FILL_LUA = """
local floor = tonumber(redis.call('GET', KEYS[2]) or '0')
if tonumber(ARGV[2]) < floor then
  return 0
end
redis.call('SET', KEYS[1], ARGV[1], 'EX', tonumber(ARGV[3]))
return 1
"""

# KEYS[1]=data key, KEYS[2]=version tombstone, ARGV[1]=new version, ARGV[2]=tombstone ttl
_INVALIDATE_LUA = """
local floor = tonumber(redis.call('GET', KEYS[2]) or '0')
if tonumber(ARGV[1]) > floor then
  redis.call('SET', KEYS[2], ARGV[1], 'EX', tonumber(ARGV[2]))
end
redis.call('DEL', KEYS[1])
return 1
"""

_TOMBSTONE_TTL = 120


def _data_key(job_id: uuid.UUID | str) -> str:
    return f"job:{job_id}"


def _ver_key(job_id: uuid.UUID | str) -> str:
    return f"jobver:{job_id}"


def _lock_key(job_id: uuid.UUID | str) -> str:
    return f"joblock:{job_id}"


class JobCache:
    def __init__(
        self,
        redis: Redis,
        *,
        ttl_active: int,
        ttl_terminal: int,
        lock_seconds: int = 5,
        wait_step: float = 0.02,
        wait_steps: int = 25,
    ) -> None:
        self._redis = redis
        self._ttl_active = ttl_active
        self._ttl_terminal = ttl_terminal
        self._lock_seconds = lock_seconds
        self._wait_step = wait_step
        self._wait_steps = wait_steps
        self._fill = redis.register_script(_FILL_LUA)
        self._invalidate = redis.register_script(_INVALIDATE_LUA)

    def ttl_for(self, status: JobStatus) -> int:
        return self._ttl_terminal if status in TERMINAL_STATES else self._ttl_active

    async def get(
        self, job_id: uuid.UUID, loader: Callable[[], Awaitable[JobRecord | None]]
    ) -> dict[str, Any] | None:
        """Return the public job dict, or None if the job does not exist."""
        cached = await self._read(job_id)
        if cached is not None:
            metrics.CACHE_HITS.inc()
            return cached
        metrics.CACHE_MISSES.inc()

        holds_lock = await self._acquire(job_id)
        if not holds_lock:
            # Another caller is loading. Poll briefly instead of piling onto Postgres.
            for _ in range(self._wait_steps):
                await asyncio.sleep(self._wait_step)
                cached = await self._read(job_id)
                if cached is not None:
                    return cached
        try:
            record = await loader()
            if record is None:
                return None
            data = record.public_dict()
            data["owner"] = record.owner
            data["version"] = record.version
            await self._store(record, data)
            return data
        finally:
            if holds_lock:
                await self._release(job_id)

    async def invalidate(self, record: JobRecord) -> None:
        """Call after the transition that produced `record` has committed."""
        try:
            await self._invalidate(
                keys=[_data_key(record.id), _ver_key(record.id)],
                args=[record.version, _TOMBSTONE_TTL],
            )
        except Exception as exc:
            metrics.CACHE_ERRORS.inc()
            log.warning("cache_invalidate_failed", job_id=str(record.id), error=repr(exc))

    async def _read(self, job_id: uuid.UUID) -> dict[str, Any] | None:
        try:
            raw = await self._redis.get(_data_key(job_id))
        except Exception as exc:
            metrics.CACHE_ERRORS.inc()
            log.warning("cache_read_failed", error=repr(exc))
            return None
        if raw is None:
            return None
        loaded: dict[str, Any] = json.loads(raw)
        return loaded

    async def _store(self, record: JobRecord, data: dict[str, Any]) -> None:
        try:
            await self._fill(
                keys=[_data_key(record.id), _ver_key(record.id)],
                args=[json.dumps(data), record.version, self.ttl_for(record.status)],
            )
        except Exception as exc:
            metrics.CACHE_ERRORS.inc()
            log.warning("cache_write_failed", error=repr(exc))

    async def _acquire(self, job_id: uuid.UUID) -> bool:
        try:
            return bool(
                await self._redis.set(_lock_key(job_id), "1", nx=True, ex=self._lock_seconds)
            )
        except Exception:
            return True  # Redis trouble: do not block on the lock, just load

    async def _release(self, job_id: uuid.UUID) -> None:
        try:
            await self._redis.delete(_lock_key(job_id))
        except Exception as exc:
            log.warning("cache_unlock_failed", error=repr(exc))


def dumps_compact(data: Any) -> str:
    return json.dumps(data, separators=(",", ":"))
