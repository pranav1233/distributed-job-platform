"""Illegal transitions are impossible: proven against every (state, operation) pair."""

from __future__ import annotations

import asyncio
import itertools
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from jobplatform.domain.states import TRANSITIONS, JobStatus
from jobplatform.repo.records import JobRecord
from jobplatform.service import JobService
from tests.integration.conftest import new_job

Op = Callable[[JobService, JobRecord], Awaitable[Any]]

# operation -> (states it may apply from, state it moves the job to)
OPERATIONS: dict[str, tuple[set[JobStatus], JobStatus]] = {
    "claim": ({JobStatus.QUEUED}, JobStatus.RUNNING),
    "complete": ({JobStatus.RUNNING}, JobStatus.SUCCEEDED),
    "schedule_retry": ({JobStatus.RUNNING}, JobStatus.RETRY_SCHEDULED),
    "mark_dead": ({JobStatus.RUNNING}, JobStatus.DEAD),
    "release": ({JobStatus.RUNNING}, JobStatus.QUEUED),
    "cancel": (
        {JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.RETRY_SCHEDULED},
        JobStatus.CANCELLED,
    ),
    "requeue_dead": ({JobStatus.DEAD}, JobStatus.QUEUED),
    "promote_due_retries": ({JobStatus.RETRY_SCHEDULED}, JobStatus.QUEUED),
    "reclaim_expired": ({JobStatus.RUNNING}, JobStatus.QUEUED),
}


async def make_job_in(service: JobService, status: JobStatus) -> JobRecord:
    job = await new_job(service, max_attempts=5)
    if status is JobStatus.QUEUED:
        return job
    if status is JobStatus.CANCELLED:
        updated, _ = await service.cancel(job.id, "alice")
        assert updated is not None
        return updated
    running = await service.claim(job.id, "w", lease_seconds=0.05)
    assert running is not None
    if status is JobStatus.RUNNING:
        return running
    if status is JobStatus.SUCCEEDED:
        done = await service.complete(job.id, "w", running.attempts, {})
        assert done is not None
        return done
    if status is JobStatus.RETRY_SCHEDULED:
        retry = await service.schedule_retry(
            job.id, "w", running.attempts, error="e", delay_seconds=0
        )
        assert retry is not None
        return retry
    dead = await service.mark_dead(job.id, "w", running.attempts, error="e", reason="r")
    assert dead is not None
    return dead


async def apply(service: JobService, op: str, job: JobRecord) -> bool:
    """Run one operation against the job. True means a transition was applied."""
    await asyncio.sleep(0.08)  # let the 0.05s lease expire and retry delays elapse
    if op == "claim":
        return await service.claim(job.id, "w2", 30) is not None
    if op == "complete":
        return await service.complete(job.id, "w", job.attempts, {}) is not None
    if op == "schedule_retry":
        return (
            await service.schedule_retry(job.id, "w", job.attempts, error="e", delay_seconds=0)
            is not None
        )
    if op == "mark_dead":
        return await service.mark_dead(job.id, "w", job.attempts, error="e", reason="r") is not None
    if op == "release":
        return await service.release(job.id, "w", job.attempts) is not None
    if op == "cancel":
        return (await service.cancel(job.id, "alice"))[0] is not None
    if op == "requeue_dead":
        return (await service.requeue_dead(job.id, "alice"))[0] is not None
    if op == "promote_due_retries":
        return any(r.id == job.id for r in await service.promote_due_retries(100))
    if op == "reclaim_expired":
        return any(r.id == job.id for r in await service.reclaim_expired(100))
    raise AssertionError(op)


@pytest.mark.parametrize(
    ("status", "op"), list(itertools.product(list(JobStatus), list(OPERATIONS)))
)
async def test_operation_applies_only_from_its_legal_source_state(
    service: JobService, status: JobStatus, op: str
) -> None:
    sources, target = OPERATIONS[op]
    job = await make_job_in(service, status)
    assert job.status is status

    applied = await apply(service, op, job)
    after = await service.get(job.id)
    assert after is not None

    if status in sources:
        assert applied, f"{op} should apply from {status.value}"
        if op == "reclaim_expired":
            assert after.status is JobStatus.QUEUED
        else:
            assert after.status is target
    else:
        assert not applied, f"{op} must not apply from {status.value}"
        assert after.status is status
        assert after.version == job.version  # a refused transition leaves no trace


def test_operation_table_matches_the_declared_state_machine() -> None:
    """Every edge the operations can produce is an edge in the declared table."""
    for sources, target in OPERATIONS.values():
        for source in sources:
            assert target in TRANSITIONS[source]


async def test_database_rejects_running_without_a_lease(
    session_factory: async_sessionmaker[AsyncSession], service: JobService
) -> None:
    job = await new_job(service)
    with pytest.raises(IntegrityError):
        async with session_factory() as session, session.begin():
            await session.execute(
                text("UPDATE jobs SET status = 'running' WHERE id = :j"), {"j": str(job.id)}
            )
    with pytest.raises(IntegrityError):
        async with session_factory() as session, session.begin():
            await session.execute(
                text("UPDATE jobs SET status = 'exploded' WHERE id = :j"), {"j": str(job.id)}
            )


async def test_job_events_are_append_only(
    session_factory: async_sessionmaker[AsyncSession], service: JobService
) -> None:
    await new_job(service)
    with pytest.raises(DBAPIError, match="append-only"):
        async with session_factory() as session, session.begin():
            await session.execute(text("UPDATE job_events SET reason = 'tampered'"))
    with pytest.raises(DBAPIError, match="append-only"):
        async with session_factory() as session, session.begin():
            await session.execute(text("DELETE FROM job_events"))


async def test_concurrent_claims_have_exactly_one_winner(service: JobService) -> None:
    job = await new_job(service)
    results = await asyncio.gather(*(service.claim(job.id, f"w{i}", 30) for i in range(20)))
    winners = [r for r in results if r is not None]
    assert len(winners) == 1
    final = await service.get(job.id)
    assert final is not None and final.attempts == 1
    assert final.lease_owner == winners[0].lease_owner
