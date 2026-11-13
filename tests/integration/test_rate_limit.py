from __future__ import annotations

import asyncio

import httpx
from asgi_lifespan import LifespanManager
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from jobplatform import metrics
from jobplatform.api.app import create_app
from jobplatform.config import Settings
from tests.integration.conftest import make_key


async def test_token_bucket_rejects_with_429_and_headers(
    api: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    key = await make_key(session_factory, "carol", capacity=3, refill=0.5)
    statuses = []
    remaining = []
    for _ in range(5):
        r = await api.get("/v1/jobs", headers=key)
        statuses.append(r.status_code)
        remaining.append(r.headers["X-RateLimit-Remaining"])
        assert r.headers["X-RateLimit-Limit"] == "3"
    assert statuses == [200, 200, 200, 429, 429]
    assert remaining[:3] == ["2", "1", "0"]

    limited = await api.get("/v1/jobs", headers=key)
    assert limited.status_code == 429
    assert limited.json()["error"]["code"] == "rate_limited"
    assert 1 <= int(limited.headers["Retry-After"]) <= 3
    assert int(limited.headers["X-RateLimit-Reset"]) >= 1


async def test_buckets_are_per_key_and_refill_over_time(
    api: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    a = await make_key(session_factory, "a", capacity=1, refill=5)
    b = await make_key(session_factory, "b", capacity=1, refill=5)
    assert (await api.get("/v1/jobs", headers=a)).status_code == 200
    assert (await api.get("/v1/jobs", headers=a)).status_code == 429
    assert (await api.get("/v1/jobs", headers=b)).status_code == 200  # independent bucket
    await asyncio.sleep(0.5)  # 5 tokens/sec refills the single token
    assert (await api.get("/v1/jobs", headers=a)).status_code == 200


async def test_concurrent_requests_never_exceed_capacity(
    api: httpx.AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    """The Lua script is atomic, so a burst cannot over-admit."""
    key = await make_key(session_factory, "burst", capacity=10, refill=0.01)
    responses = await asyncio.gather(*(api.get("/v1/jobs", headers=key) for _ in range(60)))
    assert sum(r.status_code == 200 for r in responses) == 10
    assert sum(r.status_code == 429 for r in responses) == 50


async def test_fails_open_when_redis_is_down(
    settings: Settings, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    broken = settings.model_copy(update={"redis_url": "redis://127.0.0.1:1/0"})
    app = create_app(broken)
    async with LifespanManager(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            key = await make_key(session_factory, "dave", capacity=1, refill=0.001)
            before = metrics.RATE_LIMIT_FAIL_OPEN._value.get()
            for _ in range(5):
                assert (await client.get("/v1/jobs", headers=key)).status_code == 200
            assert metrics.RATE_LIMIT_FAIL_OPEN._value.get() - before == 5
            ready = await client.get("/readyz")
            assert ready.status_code == 200  # Redis is optional
            assert ready.json()["checks"]["redis"].startswith("degraded")
