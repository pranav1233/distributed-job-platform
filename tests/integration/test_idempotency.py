from __future__ import annotations

import asyncio

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tests.integration.conftest import make_key


async def _count(sf: async_sessionmaker[AsyncSession], table: str) -> int:
    async with sf() as session:
        return int((await session.execute(text(f"SELECT count(*) FROM {table}"))).scalar() or 0)


async def test_same_key_returns_original_job(
    api: httpx.AsyncClient,
    api_key: dict[str, str],
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    headers = {**api_key, "Idempotency-Key": "order-1001"}
    body = {"type": "io_task", "payload": {"duration_ms": 10}}
    first = await api.post("/v1/jobs", json=body, headers=headers)
    second = await api.post("/v1/jobs", json=body, headers=headers)

    assert first.status_code == 201
    assert second.status_code == 200
    assert second.headers["Idempotent-Replayed"] == "true"
    assert second.json()["id"] == first.json()["id"]
    assert await _count(session_factory, "jobs") == 1
    assert await _count(session_factory, "outbox") == 1
    assert await _count(session_factory, "job_events") == 1


async def test_key_reuse_with_different_body_conflicts(
    api: httpx.AsyncClient, api_key: dict[str, str]
) -> None:
    headers = {**api_key, "Idempotency-Key": "k"}
    ok = await api.post(
        "/v1/jobs", json={"type": "io_task", "payload": {"duration_ms": 1}}, headers=headers
    )
    clash = await api.post(
        "/v1/jobs", json={"type": "io_task", "payload": {"duration_ms": 2}}, headers=headers
    )
    assert ok.status_code == 201
    assert clash.status_code == 409
    assert clash.json()["error"]["code"] == "idempotency_key_reused"


async def test_same_key_different_owner_creates_separate_jobs(
    api: httpx.AsyncClient,
    api_key: dict[str, str],
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    bob = await make_key(session_factory, "bob")
    body = {"type": "io_task"}
    a = await api.post("/v1/jobs", json=body, headers={**api_key, "Idempotency-Key": "shared"})
    b = await api.post("/v1/jobs", json=body, headers={**bob, "Idempotency-Key": "shared"})
    assert a.status_code == b.status_code == 201
    assert a.json()["id"] != b.json()["id"]


async def test_concurrent_requests_with_one_key_create_one_job(
    api: httpx.AsyncClient,
    api_key: dict[str, str],
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    headers = {**api_key, "Idempotency-Key": "race"}
    responses = await asyncio.gather(
        *(api.post("/v1/jobs", json={"type": "io_task"}, headers=headers) for _ in range(25))
    )
    assert len({r.json()["id"] for r in responses}) == 1
    assert sorted(r.status_code for r in responses).count(201) == 1
    assert await _count(session_factory, "jobs") == 1
    assert await _count(session_factory, "outbox") == 1


async def test_requests_without_key_are_never_deduplicated(
    api: httpx.AsyncClient,
    api_key: dict[str, str],
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    for _ in range(3):
        resp = await api.post("/v1/jobs", json={"type": "io_task"}, headers=api_key)
        assert resp.status_code == 201
    assert await _count(session_factory, "jobs") == 3
