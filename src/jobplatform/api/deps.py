"""Application context, authentication and rate limiting dependencies."""

from __future__ import annotations

import time
from dataclasses import dataclass, field

import structlog
from fastapi import Request
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from jobplatform import metrics
from jobplatform.api.errors import ApiError
from jobplatform.cache.job_cache import JobCache
from jobplatform.config import Settings
from jobplatform.ratelimit.token_bucket import TokenBucketLimiter
from jobplatform.repo.api_keys import Principal, find_principal
from jobplatform.security import hash_api_key
from jobplatform.service import JobService
from jobplatform.worker.registry import JobTypeSpec

_NEGATIVE_TTL = 5.0
_MAX_CACHED_KEYS = 10_000


@dataclass
class PrincipalCache:
    """In-process cache of key hash -> principal so authentication skips the database."""

    ttl: float
    _entries: dict[str, tuple[float, Principal | None]] = field(default_factory=dict)

    def get(self, key_hash: str) -> tuple[bool, Principal | None]:
        entry = self._entries.get(key_hash)
        if entry is None or entry[0] < time.monotonic():
            return False, None
        return True, entry[1]

    def put(self, key_hash: str, principal: Principal | None) -> None:
        if len(self._entries) >= _MAX_CACHED_KEYS:
            self._entries.clear()
        ttl = self.ttl if principal is not None else _NEGATIVE_TTL
        self._entries[key_hash] = (time.monotonic() + ttl, principal)


@dataclass
class AppContext:
    settings: Settings
    engine: AsyncEngine
    session_factory: async_sessionmaker[AsyncSession]
    redis: Redis
    jobs: JobService
    cache: JobCache
    limiter: TokenBucketLimiter
    registry: dict[str, JobTypeSpec]
    principals: PrincipalCache


def get_ctx(request: Request) -> AppContext:
    ctx: AppContext = request.app.state.ctx
    return ctx


def _extract_key(request: Request) -> str:
    header = request.headers.get("authorization", "")
    if header.lower().startswith("bearer "):
        return header[7:].strip()
    return request.headers.get("x-api-key", "").strip()


async def authenticate(request: Request) -> Principal:
    """Resolve the API key and enforce the per-key token bucket."""
    ctx = get_ctx(request)
    raw = _extract_key(request)
    if not raw:
        raise ApiError(
            401, "unauthorized", "missing API key", headers={"WWW-Authenticate": "Bearer"}
        )
    key_hash = hash_api_key(raw)
    found, principal = ctx.principals.get(key_hash)
    if not found:
        async with ctx.session_factory() as session:
            principal = await find_principal(session, key_hash)
        ctx.principals.put(key_hash, principal)
    if principal is None:
        raise ApiError(
            401, "unauthorized", "invalid API key", headers={"WWW-Authenticate": "Bearer"}
        )

    s = ctx.settings
    result = await ctx.limiter.check(
        str(principal.key_id),
        principal.rate_limit_capacity or s.default_rate_limit_capacity,
        principal.rate_limit_refill_per_sec or s.default_rate_limit_refill_per_sec,
    )
    request.state.rate_limit = result
    structlog.contextvars.bind_contextvars(owner=principal.owner)
    if not result.allowed:
        metrics.RATE_LIMIT_REJECTIONS.inc()
        raise ApiError(
            429,
            "rate_limited",
            "rate limit exceeded",
            headers={"Retry-After": str(result.retry_after_seconds)},
        )
    return principal
