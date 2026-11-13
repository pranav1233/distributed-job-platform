"""Crash recovery, graceful shutdown, duplicate deliveries and poison messages."""

from __future__ import annotations

import asyncio
import json
import uuid

import aio_pika
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from jobplatform import metrics
from jobplatform.config import Settings
from jobplatform.domain.states import JobStatus
from jobplatform.service import JobService
from tests.integration.conftest import (
    Stack,
    drain_queue,
    fetch_events,
    new_job,
    wait_for,
    wait_for_status,
)


async def test_reaper_recovers_job_from_crashed_worker(
    stack: Stack,
    service: JobService,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    job = await new_job(service, "side_effect", {"effect": "x"})
    # A worker claims the job and then "dies": no heartbeat, no completion.
    claimed = await service.claim(job.id, "dead-worker", lease_seconds=1.0)
    assert claimed is not None and claimed.status is JobStatus.RUNNING

    await stack.start_relay()
    await stack.start_reaper()
    await stack.start_worker()

    record = await wait_for_status(service, job.id, JobStatus.SUCCEEDED, timeout=20)
    assert record.attempts == 2  # the crashed attempt counted, then a live worker finished it
    events = await fetch_events(session_factory, job.id)
    reasons = [e["reason"] for e in events]
    assert "lease_expired" in reasons
    reclaim = next(e for e in events if e["reason"] == "lease_expired")
    assert (reclaim["from_status"], reclaim["to_status"], reclaim["actor"]) == (
        "running",
        "queued",
        "reaper",
    )
    async with session_factory() as session:
        effects = (await session.execute(text("SELECT count(*) FROM job_effects"))).scalar()
    assert effects == 1


async def test_reaper_marks_job_dead_when_attempts_are_exhausted(
    stack: Stack, service: JobService, settings: Settings
) -> None:
    job = await new_job(service, "io_task", max_attempts=1)
    await service.claim(job.id, "dead-worker", lease_seconds=0.5)
    await stack.start_relay()
    await stack.start_reaper()
    dead = await wait_for_status(service, job.id, JobStatus.DEAD, timeout=10)
    assert "attempts exhausted" in (dead.last_error or "")
    msgs = await drain_queue(settings, settings.dlq_name, expected=1, timeout=10)
    assert msgs[0]["reason"] == "lease_expired"


async def test_heartbeat_keeps_lease_alive_for_long_jobs(stack: Stack, service: JobService) -> None:
    """A job running longer than the lease must not be reclaimed while it heartbeats."""
    await stack.start_relay()
    await stack.start_reaper()
    await stack.start_worker()
    job = await new_job(service, "io_task", {"duration_ms": 7000})  # lease is 3s
    record = await wait_for_status(service, job.id, JobStatus.SUCCEEDED, timeout=30)
    assert record.attempts == 1


async def test_stale_worker_cannot_overwrite_new_owner(service: JobService) -> None:
    job = await new_job(service)
    first = await service.claim(job.id, "w1", lease_seconds=0.3)
    assert first is not None
    await asyncio.sleep(0.5)
    reclaimed = await service.reclaim_expired(10)
    assert [r.id for r in reclaimed] == [job.id]
    second = await service.claim(job.id, "w2", lease_seconds=30)
    assert second is not None and second.attempts == 2

    # w1 wakes up late and tries to report its result: fenced out
    assert await service.complete(job.id, "w1", 1, {"late": True}) is None
    assert await service.heartbeat(job.id, "w1", 1, 30) is False
    assert await service.complete(job.id, "w2", 2, {"ok": True}) is not None
    final = await service.get(job.id)
    assert final is not None and final.result == {"ok": True}


async def test_duplicate_delivery_executes_once(
    stack: Stack,
    service: JobService,
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    job = await new_job(service, "side_effect", {"effect": "once", "duration_ms": 300})
    await stack.start_worker()

    body = json.dumps({"job_id": str(job.id), "type": "side_effect"}).encode()
    before = metrics.DUPLICATE_DELIVERIES._value.get()
    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    async with connection:
        channel = await connection.channel(publisher_confirms=True)
        exchange = await channel.get_exchange(settings.exchange_name)
        for _ in range(3):  # the same delivery three times, as after a relay crash
            await exchange.publish(aio_pika.Message(body, delivery_mode=2), routing_key="work")

    await wait_for_status(service, job.id, JobStatus.SUCCEEDED)
    await wait_for(
        lambda: metrics.DUPLICATE_DELIVERIES._value.get() - before >= 2, what="duplicates dropped"
    )
    async with session_factory() as session:
        row = (await session.execute(text("SELECT executions FROM job_effects"))).one()
    assert row.executions == 1
    record = await service.get(job.id)
    assert record is not None and record.attempts == 1


async def test_poison_message_is_dead_lettered_and_worker_survives(
    stack: Stack, service: JobService, settings: Settings
) -> None:
    await stack.start_worker()
    before = metrics.POISON_MESSAGES._value.get()
    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    async with connection:
        channel = await connection.channel(publisher_confirms=True)
        exchange = await channel.get_exchange(settings.exchange_name)
        await exchange.publish(aio_pika.Message(b"{not json", delivery_mode=2), routing_key="work")
        await exchange.publish(
            aio_pika.Message(json.dumps({"job_id": "nope"}).encode(), delivery_mode=2),
            routing_key="work",
        )
    msgs = await drain_queue(settings, settings.dlq_name, expected=2, timeout=10, raw=True)
    assert sorted(msgs) == sorted([b"{not json", json.dumps({"job_id": "nope"}).encode()])
    assert metrics.POISON_MESSAGES._value.get() - before == 2

    # the worker is still healthy: a normal job runs afterwards
    await stack.start_relay()
    job = await new_job(service, "io_task", {"duration_ms": 1})
    await wait_for_status(service, job.id, JobStatus.SUCCEEDED)


async def test_graceful_shutdown_releases_unfinished_jobs(
    stack: Stack,
    service: JobService,
    settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await stack.start_relay()
    worker, stop = await stack.start_worker(worker_id="shutdown-me")
    job = await new_job(service, "io_task", {"duration_ms": 60_000})
    await wait_for_status(service, job.id, JobStatus.RUNNING)

    stop.set()  # SIGTERM: stop consuming, drain for 2s, then release the lease
    released = await wait_for_status(service, job.id, JobStatus.QUEUED, timeout=15)
    assert released.attempts == 0  # the unfinished attempt is not charged to the job
    assert released.lease_owner is None
    events = await fetch_events(session_factory, job.id)
    assert events[-1]["reason"] == "lease_released"

    # another worker picks it up from the fresh outbox row
    stack.settings = settings
    await stack.start_worker(worker_id="replacement")
    fresh = await wait_for_status(service, job.id, JobStatus.RUNNING, timeout=15)
    assert fresh.lease_owner == "replacement"
    assert worker is not None


async def test_graceful_shutdown_lets_short_jobs_finish(stack: Stack, service: JobService) -> None:
    await stack.start_relay()
    _, stop = await stack.start_worker()
    job = await new_job(service, "io_task", {"duration_ms": 1000})
    await wait_for_status(service, job.id, JobStatus.RUNNING)
    stop.set()
    record = await wait_for_status(service, job.id, JobStatus.SUCCEEDED, timeout=10)
    assert record.attempts == 1


async def test_cpu_bound_job_runs_in_process_pool(stack: Stack, service: JobService) -> None:
    await stack.start_relay()
    await stack.start_worker(cpu=True)
    job = await new_job(service, "cpu_report", {"rows": 150_000, "seed": 7})
    done = await wait_for_status(service, job.id, JobStatus.SUCCEEDED, timeout=60)
    assert done.result is not None and done.result["rows"] == 150_000
    # deterministic: a second run with the same seed gives the same checksum
    again = await new_job(service, "cpu_report", {"rows": 150_000, "seed": 7})
    done2 = await wait_for_status(service, again.id, JobStatus.SUCCEEDED, timeout=60)
    assert done2.result is not None and done2.result["checksum"] == done.result["checksum"]


async def test_cpu_bound_job_is_cancelled_cooperatively(stack: Stack, service: JobService) -> None:
    await stack.start_relay()
    await stack.start_worker(cpu=True)
    job = await new_job(service, "cpu_report", {"rows": 20_000_000, "seed": 1})
    await wait_for_status(service, job.id, JobStatus.RUNNING, timeout=30)
    updated, _ = await service.cancel(job.id, "alice")
    assert updated is not None

    pool = stack.cpu_pools[0]

    async def slot_recycled() -> bool:
        return pool._slots.qsize() == pool.size and not stack.workers[0]._tasks

    # the child sees the flag at its next chunk, the slot comes back, the worker goes idle
    await wait_for(slot_recycled, timeout=20, what="cpu slot to be recycled")
    assert uuid.UUID(str(job.id))
