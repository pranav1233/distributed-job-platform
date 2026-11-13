"""Distributed token bucket in Redis.

The whole check-and-take runs as one Lua script, so it is atomic across any number of
API instances. Time comes from Redis (TIME), not from the callers, so clock skew between
API hosts cannot distort refill. State is a hash with the token count and the last
refill timestamp in microseconds. Keys expire once the bucket would be full again.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import structlog
from redis.asyncio import Redis

from jobplatform import metrics

log = structlog.get_logger(__name__)

TOKEN_BUCKET_LUA = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_per_sec = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])

local t = redis.call('TIME')
local now_us = tonumber(t[1]) * 1000000 + tonumber(t[2])

local state = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts = tonumber(state[2])
if tokens == nil then
  tokens = capacity
  ts = now_us
end

local elapsed = math.max(0, now_us - ts)
tokens = math.min(capacity, tokens + (elapsed / 1000000) * refill_per_sec)

local allowed = 0
local retry_after_us = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  retry_after_us = math.ceil(((cost - tokens) / refill_per_sec) * 1000000)
end

redis.call('HSET', key, 'tokens', tostring(tokens), 'ts', tostring(now_us))
local full_us = math.ceil(((capacity - tokens) / refill_per_sec) * 1000000)
local ttl_ms = math.max(1000, math.ceil(full_us / 1000) + 1000)
redis.call('PEXPIRE', key, ttl_ms)

return {allowed, tostring(tokens), retry_after_us, full_us}
"""  # noqa: S105


@dataclass(frozen=True, slots=True)
class RateLimitResult:
    allowed: bool
    limit: int
    remaining: int
    retry_after_seconds: int
    reset_seconds: int
    degraded: bool = False


class TokenBucketLimiter:
    def __init__(self, redis: Redis, prefix: str = "rl") -> None:
        self._redis = redis
        self._prefix = prefix
        self._script = redis.register_script(TOKEN_BUCKET_LUA)

    async def check(
        self, subject: str, capacity: int, refill_per_sec: float, cost: int = 1
    ) -> RateLimitResult:
        try:
            allowed, tokens, retry_us, full_us = await self._script(
                keys=[f"{self._prefix}:{subject}"],
                args=[capacity, refill_per_sec, cost],
            )
        except Exception as exc:
            # Fail open: an outage of the limiter must not become an outage of the API.
            metrics.RATE_LIMIT_FAIL_OPEN.inc()
            log.warning("rate_limiter_unavailable_fail_open", error=repr(exc))
            return RateLimitResult(True, capacity, capacity, 0, 0, degraded=True)
        return RateLimitResult(
            allowed=bool(allowed),
            limit=capacity,
            remaining=max(0, math.floor(float(tokens))),
            retry_after_seconds=max(1, math.ceil(int(retry_us) / 1_000_000)) if not allowed else 0,
            reset_seconds=max(0, math.ceil(int(full_us) / 1_000_000)),
        )
