from __future__ import annotations

import asyncio
import time

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from jobplatform.config import Settings
from jobplatform.domain.states import JobStatus
from jobplatform.service import JobService
from tests.integration.conftest import Stack, fetch_events, wait_for, wait_for_status


async def test_cancel_queued_job_is_never_published(
    api: httpx.AsyncClient,
    api_key: dict[str, str],
    stack: Stack,
    service: JobService,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    job_id = (await api.post("/v1/jobs", json={"type": "io_task"}, headers=api_key)).json()["id"]

    resp = await api.post(f"/v1/jobs/{job_id}/cancel", headers=api_key)
    assert resp.status_code == 200
    assert resp.json()["status"] == "cancelled"
    # cancelling again is idempotent
    assert (await api.post(f"/v1/jobs/{job_id}/cancel", headers=api_key)).status_code == 200

    await stack.start_relay()
    await stack.start_worker()

    async def outbox_settled() -> bool:
        async with session_factory() as session:
            row = (
                await session.execute(
                    text("SELECT published_at, skipped_reason FROM outbox WHERE job_id = :j"),
                    {"j": job_id},
                )
            ).one()
        return row.published_at is not None and row.skipped_reason == "job_cancelled"

    await wait_for(outbox_settled, what="relay to drop the stale outbox row")
    record = await service.get(job_id)  # type: ignore[arg-type]
    assert record is not None and record.status is JobStatus.CANCELLED
    assert record.attempts == 0


async def test_cancel_running_job_stops_it(
    api: httpx.AsyncClient,
    api_key: dict[str, str],
    stack: Stack,
    service: JobService,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await stack.start_relay()
    await stack.start_worker()
    job_id = (
        await api.post(
            "/v1/jobs", json={"type": "io_task", "payload": {"duration_ms": 60000}}, headers=api_key
        )
    ).json()["id"]
    await wait_for_status(service, job_id, JobStatus.RUNNING)

    started = time.monotonic()
    resp = await api.post(f"/v1/jobs/{job_id}/cancel", headers=api_key)
    assert resp.json()["status"] == "cancelled"

    # the worker notices through its next heartbeat and abandons the handler
    async def worker_idle() -> bool:
        return bool(stack.workers) and not stack.workers[0]._tasks

    await wait_for(worker_idle, timeout=10, what="worker to abandon the cancelled job")
    assert time.monotonic() - started < 5
    final = await service.get(job_id)  # type: ignore[arg-type]
    assert final is not None
    assert final.status is JobStatus.CANCELLED and final.result is None
    events = await fetch_events(session_factory, job_id)
    assert events[-1]["to_status"] == "cancelled"
    assert events[-1]["from_status"] == "running"


async def test_cancel_finished_job_conflicts(
    api: httpx.AsyncClient, api_key: dict[str, str], stack: Stack, service: JobService
) -> None:
    await stack.start_relay()
    await stack.start_worker()
    job_id = (
        await api.post(
            "/v1/jobs", json={"type": "io_task", "payload": {"duration_ms": 1}}, headers=api_key
        )
    ).json()["id"]
    await wait_for_status(service, job_id, JobStatus.SUCCEEDED)
    resp = await api.post(f"/v1/jobs/{job_id}/cancel", headers=api_key)
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "invalid_state"


async def test_cancel_retry_scheduled_job(
    api: httpx.AsyncClient,
    api_key: dict[str, str],
    stack: Stack,
    service: JobService,
    settings: Settings,
) -> None:
    from jobplatform.worker.registry import build_registry

    slow = settings.model_copy(
        update={"retry_base_delay_seconds": 30, "retry_max_delay_seconds": 60}
    )
    registry = build_registry(slow)
    await stack.start_relay()
    await stack.start_worker(registry=registry)
    job_id = (
        await api.post(
            "/v1/jobs", json={"type": "flaky", "payload": {"fail_times": 9}}, headers=api_key
        )
    ).json()["id"]
    await wait_for_status(service, job_id, JobStatus.RETRY_SCHEDULED)
    resp = await api.post(f"/v1/jobs/{job_id}/cancel", headers=api_key)
    assert resp.json()["status"] == "cancelled"
    await asyncio.sleep(0.3)
    final = await service.get(job_id)  # type: ignore[arg-type]
    assert final is not None and final.status is JobStatus.CANCELLED
