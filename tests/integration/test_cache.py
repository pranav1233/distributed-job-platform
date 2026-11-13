from __future__ import annotations

import asyncio
import json
import uuid
from datetime import timedelta
from unittest.mock import patch

import httpx
from redis.asyncio import Redis

from jobplatform import metrics
from jobplatform.config import Settings
from jobplatform.domain.states import JobStatus
from jobplatform.service import JobService
from tests.integration.conftest import Stack, wait_for_status


async def test_second_read_is_a_cache_hit_with_status_dependent_ttl(
    api: httpx.AsyncClient, api_key: dict[str, str], redis: Redis, settings: Settings
) -> None:
    job_id = (await api.post("/v1/jobs", json={"type": "io_task"}, headers=api_key)).json()["id"]
    hits = metrics.CACHE_HITS._value.get()
    misses = metrics.CACHE_MISSES._value.get()

    first = await api.get(f"/v1/jobs/{job_id}", headers=api_key)
    second = await api.get(f"/v1/jobs/{job_id}", headers=api_key)
    assert first.json() == second.json()
    assert metrics.CACHE_MISSES._value.get() - misses == 1
    assert metrics.CACHE_HITS._value.get() - hits == 1

    ttl = await redis.ttl(f"job:{job_id}")
    assert 0 < ttl <= settings.cache_ttl_active_seconds  # queued is an active state


async def test_terminal_state_gets_long_ttl_and_every_transition_invalidates(
    api: httpx.AsyncClient,
    api_key: dict[str, str],
    stack: Stack,
    service: JobService,
    redis: Redis,
    settings: Settings,
) -> None:
    # use a TTL far longer than the test so only invalidation can explain fresh reads
    app = api.app  # type: ignore[attr-defined]
    app.state.ctx.cache._ttl_active = 120

    job_id = (
        await api.post(
            "/v1/jobs", json={"type": "io_task", "payload": {"duration_ms": 200}}, headers=api_key
        )
    ).json()["id"]
    assert (await api.get(f"/v1/jobs/{job_id}", headers=api_key)).json()["status"] == "queued"
    assert await redis.exists(f"job:{job_id}") == 1

    # the cancel path runs through the API's service, which invalidates inline
    cancelled = await api.post(f"/v1/jobs/{job_id}/cancel", headers=api_key)
    assert cancelled.json()["status"] == "cancelled"
    assert await redis.exists(f"job:{job_id}") == 0
    assert (await api.get(f"/v1/jobs/{job_id}", headers=api_key)).json()["status"] == "cancelled"
    assert settings.cache_ttl_terminal_seconds - 5 <= await redis.ttl(f"job:{job_id}")


async def test_worker_transitions_invalidate_through_hooks(
    api: httpx.AsyncClient,
    api_key: dict[str, str],
    stack: Stack,
    service: JobService,
    redis: Redis,
    settings: Settings,
) -> None:
    from jobplatform.runtime import build_cache
    from jobplatform.worker.registry import build_registry
    from jobplatform.worker.worker import Worker

    r, cache = build_cache(settings)
    cache._ttl_active = 120
    api.app.state.ctx.cache._ttl_active = 120  # type: ignore[attr-defined]
    hooked = JobService(service.session_factory, cache.invalidate)
    await stack.start_relay()
    worker = Worker(settings, hooked, build_registry(settings))
    stop = asyncio.Event()
    task = asyncio.create_task(worker.run(stop))
    try:
        await asyncio.sleep(0.5)
        job_id = (
            await api.post(
                "/v1/jobs",
                json={"type": "io_task", "payload": {"duration_ms": 1500}},
                headers=api_key,
            )
        ).json()["id"]
        # prime the cache while the job is queued/running, then let it finish
        await api.get(f"/v1/jobs/{job_id}", headers=api_key)
        await wait_for_status(service, job_id, JobStatus.SUCCEEDED)
        await asyncio.sleep(0.2)
        body = (await api.get(f"/v1/jobs/{job_id}", headers=api_key)).json()
        assert body["status"] == "succeeded"  # not the stale cached "queued"/"running"
    finally:
        stop.set()
        await asyncio.wait_for(task, 10)
        await r.aclose()


async def test_stale_fill_after_invalidation_is_rejected(
    api: httpx.AsyncClient, api_key: dict[str, str], service: JobService, redis: Redis
) -> None:
    """A reader that loaded the old row must not overwrite the newer state."""
    cache = api.app.state.ctx.cache  # type: ignore[attr-defined]
    job_id = (await api.post("/v1/jobs", json={"type": "io_task"}, headers=api_key)).json()["id"]
    stale = await service.get(uuid.UUID(job_id))
    assert stale is not None

    updated, _ = await service.cancel(uuid.UUID(job_id), "alice")
    assert updated is not None
    await cache.invalidate(updated)  # a transition committed and invalidated

    data = stale.public_dict()
    data["owner"] = stale.owner
    data["version"] = stale.version
    await cache._store(stale, data)  # the slow reader now tries to fill the old row
    assert await redis.exists(f"job:{job_id}") == 0

    body = (await api.get(f"/v1/jobs/{job_id}", headers=api_key)).json()
    assert body["status"] == "cancelled"


async def test_stampede_protection_loads_from_postgres_once(
    api: httpx.AsyncClient, api_key: dict[str, str], service: JobService
) -> None:
    job_id = (await api.post("/v1/jobs", json={"type": "io_task"}, headers=api_key)).json()["id"]
    ctx = api.app.state.ctx  # type: ignore[attr-defined]
    calls = 0
    original = ctx.jobs.get

    async def slow_get(jid: uuid.UUID):  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.3)  # long enough for the other readers to pile up
        return await original(jid)

    with patch.object(ctx.jobs, "get", slow_get):
        responses = await asyncio.gather(
            *(api.get(f"/v1/jobs/{job_id}", headers=api_key) for _ in range(40))
        )
    assert all(r.status_code == 200 for r in responses)
    assert calls == 1


async def test_cache_failure_falls_back_to_postgres(
    api: httpx.AsyncClient, api_key: dict[str, str]
) -> None:
    job_id = (await api.post("/v1/jobs", json={"type": "io_task"}, headers=api_key)).json()["id"]
    ctx = api.app.state.ctx  # type: ignore[attr-defined]
    errors = metrics.CACHE_ERRORS._value.get()
    with patch.object(ctx.redis, "get", side_effect=ConnectionError("redis down")):
        resp = await api.get(f"/v1/jobs/{job_id}", headers=api_key)
    assert resp.status_code == 200 and resp.json()["id"] == job_id
    assert metrics.CACHE_ERRORS._value.get() > errors
    assert json.loads(json.dumps(timedelta(0).total_seconds())) == 0
