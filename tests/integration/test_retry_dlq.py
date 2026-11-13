from __future__ import annotations

import asyncio
from dataclasses import replace

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from jobplatform.config import Settings
from jobplatform.domain.states import JobStatus
from jobplatform.service import JobService
from jobplatform.worker.registry import build_registry
from tests.integration.conftest import (
    Stack,
    drain_queue,
    fetch_events,
    new_job,
    wait_for_status,
)


async def test_retry_with_backoff_then_success(
    stack: Stack,
    service: JobService,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await stack.start_relay()
    await stack.start_worker()
    job = await new_job(service, "flaky", {"fail_times": 2}, max_attempts=5)

    record = await wait_for_status(service, job.id, JobStatus.SUCCEEDED)
    assert record.attempts == 3
    assert record.result == {"succeeded_on_attempt": 3}

    events = await fetch_events(session_factory, job.id)
    path = [(e["from_status"], e["to_status"]) for e in events]
    assert path == [
        (None, "queued"),
        ("queued", "running"),
        ("running", "retry_scheduled"),
        ("retry_scheduled", "queued"),
        ("queued", "running"),
        ("running", "retry_scheduled"),
        ("retry_scheduled", "queued"),
        ("queued", "running"),
        ("running", "succeeded"),
    ]
    # backoff is decided in Postgres: each retry_scheduled event records its delay
    async with session_factory() as session:
        delays = (
            (
                await session.execute(
                    text(
                        "SELECT (detail->>'delay_seconds')::float FROM job_events "
                        "WHERE job_id = :j AND to_status = 'retry_scheduled' ORDER BY id"
                    ),
                    {"j": str(job.id)},
                )
            )
            .scalars()
            .all()
        )
    assert len(delays) == 2
    assert all(0 <= d <= 1.0 for d in delays)  # bounded by RETRY_MAX_DELAY_SECONDS


async def test_future_available_at_is_not_published_early(
    stack: Stack, service: JobService, settings: Settings
) -> None:
    """The relay only publishes rows with available_at <= now()."""
    await stack.start_relay()
    job = await new_job(service, "io_task", {"duration_ms": 1}, delay_seconds=2)
    await asyncio.sleep(0.8)
    current = await service.get(job.id)
    assert current is not None and current.status is JobStatus.QUEUED
    early = await drain_queue(settings, settings.queue_name, expected=0, timeout=1)
    assert early == []
    await asyncio.sleep(1.8)
    msgs = await drain_queue(settings, settings.queue_name, expected=1, timeout=5)
    assert [m["job_id"] for m in msgs] == [str(job.id)]


async def test_exhausted_retries_go_to_dlq_then_requeue(
    api: httpx.AsyncClient,
    api_key: dict[str, str],
    stack: Stack,
    service: JobService,
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await stack.start_relay()
    await stack.start_worker()
    resp = await api.post(
        "/v1/jobs",
        json={"type": "flaky", "payload": {"fail_times": 50}, "max_attempts": 3},
        headers=api_key,
    )
    job_id = resp.json()["id"]

    dead = await wait_for_status(service, job_id, JobStatus.DEAD)
    assert dead.attempts == 3
    assert dead.last_error and "simulated failure" in dead.last_error

    listed = (await api.get("/v1/dlq", headers=api_key)).json()
    assert [j["id"] for j in listed["items"]] == [job_id]

    # the DLQ copy reaches RabbitMQ through the outbox
    dlq_msgs = await drain_queue(settings, settings.dlq_name, expected=1, timeout=10)
    assert [m["job_id"] for m in dlq_msgs] == [job_id]
    assert dlq_msgs[0]["reason"] == "retries_exhausted"

    # fix the cause, then requeue through the API
    async with session_factory() as session, session.begin():
        await session.execute(
            text(
                "UPDATE jobs SET payload = "
                '\'{"fail_times": 0, "error": "retryable", "duration_ms": 0}\' WHERE id = :j'
            ),
            {"j": job_id},
        )
    requeued = await api.post(f"/v1/dlq/{job_id}/requeue", headers=api_key)
    assert requeued.status_code == 200
    assert requeued.json()["status"] == "queued"
    assert requeued.json()["attempts"] == 0

    done = await wait_for_status(service, job_id, JobStatus.SUCCEEDED)
    assert done.attempts == 1
    assert (await api.get("/v1/dlq", headers=api_key)).json()["items"] == []

    again = await api.post(f"/v1/dlq/{job_id}/requeue", headers=api_key)
    assert again.status_code == 409
    missing = await api.post(
        "/v1/dlq/00000000-0000-0000-0000-000000000000/requeue", headers=api_key
    )
    assert missing.status_code == 404


async def test_non_retryable_error_skips_retries(
    stack: Stack, service: JobService, settings: Settings
) -> None:
    await stack.start_relay()
    await stack.start_worker()
    job = await new_job(service, "flaky", {"fail_times": 5, "error": "fatal"}, max_attempts=5)
    dead = await wait_for_status(service, job.id, JobStatus.DEAD)
    assert dead.attempts == 1
    msgs = await drain_queue(settings, settings.dlq_name, expected=1, timeout=10)
    assert msgs[0]["reason"] == "non_retryable"


async def test_unknown_job_type_goes_dead(stack: Stack, service: JobService) -> None:
    await stack.start_relay()
    await stack.start_worker()
    job = await new_job(service, "type_that_has_no_handler")
    dead = await wait_for_status(service, job.id, JobStatus.DEAD)
    assert "no handler" in (dead.last_error or "")


async def test_handler_timeout_is_retried_then_dead(
    stack: Stack, service: JobService, settings: Settings
) -> None:
    registry = build_registry(settings)
    registry["io_task"] = replace(registry["io_task"], timeout_seconds=0.3)
    await stack.start_relay()
    await stack.start_worker(registry=registry)
    job = await new_job(service, "io_task", {"duration_ms": 5000}, max_attempts=2)
    dead = await wait_for_status(service, job.id, JobStatus.DEAD)
    assert dead.attempts == 2
    assert "timeout" in (dead.last_error or "")
