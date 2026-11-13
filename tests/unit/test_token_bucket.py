"""The Lua token bucket, executed by an in-process Redis (fakeredis with Lua support)."""

import asyncio

import fakeredis
import pytest

from jobplatform.ratelimit.token_bucket import TokenBucketLimiter


@pytest.fixture
async def limiter() -> TokenBucketLimiter:
    redis = fakeredis.FakeAsyncRedis()
    return TokenBucketLimiter(redis)


async def test_admits_up_to_capacity_then_rejects(limiter: TokenBucketLimiter) -> None:
    results = [await limiter.check("k", capacity=3, refill_per_sec=0.1) for _ in range(5)]
    assert [r.allowed for r in results] == [True, True, True, False, False]
    assert [r.remaining for r in results[:3]] == [2, 1, 0]
    assert all(r.limit == 3 for r in results)


async def test_rejection_reports_retry_after(limiter: TokenBucketLimiter) -> None:
    for _ in range(2):
        await limiter.check("k", capacity=2, refill_per_sec=0.5)
    denied = await limiter.check("k", capacity=2, refill_per_sec=0.5)
    assert not denied.allowed
    assert denied.retry_after_seconds == 2  # one token at 0.5/s is 2 seconds away
    assert denied.reset_seconds >= 2


async def test_tokens_refill_over_time(limiter: TokenBucketLimiter) -> None:
    assert (await limiter.check("k", capacity=1, refill_per_sec=20)).allowed
    assert not (await limiter.check("k", capacity=1, refill_per_sec=20)).allowed
    await asyncio.sleep(0.12)  # 20 tokens/s refills the single token in 50 ms
    assert (await limiter.check("k", capacity=1, refill_per_sec=20)).allowed


async def test_refill_never_exceeds_capacity(limiter: TokenBucketLimiter) -> None:
    await limiter.check("k", capacity=2, refill_per_sec=20)
    await asyncio.sleep(0.5)  # 10 tokens' worth of refill, but the bucket holds only 2
    results = [await limiter.check("k", capacity=2, refill_per_sec=20) for _ in range(3)]
    assert [r.allowed for r in results] == [True, True, False]


async def test_subjects_are_isolated(limiter: TokenBucketLimiter) -> None:
    assert (await limiter.check("a", 1, 0.001)).allowed
    assert not (await limiter.check("a", 1, 0.001)).allowed
    assert (await limiter.check("b", 1, 0.001)).allowed


async def test_cost_greater_than_one(limiter: TokenBucketLimiter) -> None:
    assert (await limiter.check("k", 10, 0.001, cost=7)).allowed
    assert not (await limiter.check("k", 10, 0.001, cost=7)).allowed
    assert (await limiter.check("k", 10, 0.001, cost=3)).allowed


async def test_concurrent_checks_are_atomic(limiter: TokenBucketLimiter) -> None:
    results = await asyncio.gather(*(limiter.check("k", 25, 0.001) for _ in range(100)))
    assert sum(r.allowed for r in results) == 25


async def test_idle_buckets_expire() -> None:
    redis = fakeredis.FakeAsyncRedis()
    limiter = TokenBucketLimiter(redis)
    await limiter.check("k", capacity=5, refill_per_sec=100)
    ttl = await redis.pttl("rl:k")
    assert 0 < ttl <= 2000  # full again almost immediately, so the key is short-lived


async def test_fails_open_when_redis_raises() -> None:
    class Broken(fakeredis.FakeAsyncRedis):
        def register_script(self, script: str):  # type: ignore[no-untyped-def]
            async def boom(*args: object, **kwargs: object) -> None:
                raise ConnectionError("down")

            return boom

    limiter = TokenBucketLimiter(Broken())
    result = await limiter.check("k", capacity=5, refill_per_sec=1)
    assert result.allowed and result.degraded
