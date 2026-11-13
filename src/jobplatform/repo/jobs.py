"""SQL for job state transitions.

Every function here runs inside a transaction owned by the caller and never commits.
A transition is a single conditional UPDATE guarded by the expected current status.
The matching job_events row, and any outbox row, are written in the same transaction,
so the audit trail and the dispatch intent can never diverge from the job state.

All time arithmetic uses the database clock (now()), never worker clocks.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import datetime
from typing import Any

from sqlalchemy import ColumnElement, func, insert, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from jobplatform import metrics
from jobplatform.db.models import Job, JobEvent, Outbox
from jobplatform.domain.states import (
    CANCELLABLE,
    JobStatus,
    assert_transition,
)
from jobplatform.repo.records import JobRecord, OutboxRecord

_JOB_COLS = tuple(Job.__table__.c)


class IdempotencyConflictError(Exception):
    """The idempotency key was reused with a different request body."""


def _interval(seconds: float | ColumnElement[Any]) -> ColumnElement[Any]:
    return func.make_interval(0, 0, 0, 0, 0, 0, seconds)


async def _record_event(
    session: AsyncSession,
    record: JobRecord,
    from_status: JobStatus | None,
    actor: str,
    reason: str | None,
    detail: dict[str, Any] | None,
) -> None:
    await session.execute(
        insert(JobEvent).values(
            job_id=record.id,
            from_status=from_status.value if from_status else None,
            to_status=record.status.value,
            attempt=record.attempts,
            actor=actor,
            reason=reason,
            detail=detail,
        )
    )
    metrics.JOB_TRANSITIONS.labels(
        from_status.value if from_status else "none", record.status.value
    ).inc()


async def enqueue_outbox(
    session: AsyncSession,
    record: JobRecord,
    kind: str,
    available_at: datetime | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    body: dict[str, Any] = {
        "job_id": str(record.id),
        "type": record.type,
        "priority": record.priority,
        "attempt": record.attempts,
    }
    if extra:
        body.update(extra)
    await session.execute(
        insert(Outbox).values(
            job_id=record.id,
            kind=kind,
            payload=body,
            priority=record.priority,
            available_at=available_at if available_at is not None else func.now(),
        )
    )


async def _transition(
    session: AsyncSession,
    job_id: uuid.UUID,
    expected: JobStatus,
    target: JobStatus,
    *,
    values: dict[str, Any],
    guards: Sequence[ColumnElement[bool]] = (),
    actor: str,
    reason: str | None = None,
    detail: dict[str, Any] | None = None,
) -> JobRecord | None:
    """Apply expected -> target with a conditional UPDATE. None means the guard failed."""
    assert_transition(expected, target)
    stmt = (
        update(Job)
        .where(Job.id == job_id, Job.status == expected.value, *guards)
        .values(
            status=target.value,
            version=Job.version + 1,
            updated_at=func.now(),
            **values,
        )
        .returning(*_JOB_COLS)
        .execution_options(synchronize_session=False)
    )
    row = (await session.execute(stmt)).mappings().first()
    if row is None:
        return None
    record = JobRecord.from_mapping(row)
    await _record_event(session, record, expected, actor, reason, detail)
    return record


_LEAVE_RUNNING: dict[str, Any] = {"lease_owner": None, "lease_expires_at": None}


async def create_job(
    session: AsyncSession,
    *,
    owner: str,
    type_: str,
    payload: dict[str, Any],
    priority: int,
    max_attempts: int,
    delay_seconds: float,
    idempotency_key: str | None,
    fingerprint: str | None,
) -> tuple[JobRecord, bool]:
    """Insert the job, its first event and its outbox row. Returns (record, created)."""
    stmt = (
        pg_insert(Job)
        .values(
            id=uuid.uuid4(),
            owner=owner,
            type=type_,
            payload=payload,
            priority=priority,
            max_attempts=max_attempts,
            status=JobStatus.QUEUED.value,
            available_at=func.now() + _interval(delay_seconds),
            idempotency_key=idempotency_key,
            request_fingerprint=fingerprint,
        )
        .on_conflict_do_nothing(constraint="uq_jobs_owner_idempotency_key")
        .returning(*_JOB_COLS)
    )
    row = (await session.execute(stmt)).mappings().first()
    if row is None:
        # A concurrent or earlier request with the same (owner, key) won.
        existing = (
            (
                await session.execute(
                    select(*_JOB_COLS).where(
                        Job.owner == owner, Job.idempotency_key == idempotency_key
                    )
                )
            )
            .mappings()
            .one()
        )
        record = JobRecord.from_mapping(existing)
        if record.request_fingerprint != fingerprint:
            raise IdempotencyConflictError(idempotency_key)
        return record, False
    record = JobRecord.from_mapping(row)
    await _record_event(session, record, None, f"api:{owner}", "created", None)
    await enqueue_outbox(session, record, "dispatch", available_at=record.available_at)
    return record, True


async def get_job(session: AsyncSession, job_id: uuid.UUID) -> JobRecord | None:
    row = (await session.execute(select(*_JOB_COLS).where(Job.id == job_id))).mappings().first()
    return JobRecord.from_mapping(row) if row else None


async def claim(
    session: AsyncSession, job_id: uuid.UUID, worker_id: str, lease_seconds: float
) -> JobRecord | None:
    """queued -> running. Fails (None) if the job is already running, finished or cancelled."""
    return await _transition(
        session,
        job_id,
        JobStatus.QUEUED,
        JobStatus.RUNNING,
        values={
            "lease_owner": worker_id,
            "lease_expires_at": func.now() + _interval(lease_seconds),
            "attempts": Job.attempts + 1,
            "started_at": func.coalesce(Job.started_at, func.now()),
        },
        actor=f"worker:{worker_id}",
        reason="claimed",
    )


def _fence(worker_id: str, attempt: int) -> list[ColumnElement[bool]]:
    return [Job.lease_owner == worker_id, Job.attempts == attempt]


async def heartbeat(
    session: AsyncSession,
    job_id: uuid.UUID,
    worker_id: str,
    attempt: int,
    lease_seconds: float,
) -> bool:
    """Extend the lease. False means the lease is gone (reclaimed, cancelled, finished)."""
    result = await session.execute(
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.RUNNING.value, *_fence(worker_id, attempt))
        .values(lease_expires_at=func.now() + _interval(lease_seconds))
        .execution_options(synchronize_session=False)
    )
    return bool(result.rowcount)  # type: ignore[attr-defined]


async def complete(
    session: AsyncSession,
    job_id: uuid.UUID,
    worker_id: str,
    attempt: int,
    result: dict[str, Any] | None,
) -> JobRecord | None:
    return await _transition(
        session,
        job_id,
        JobStatus.RUNNING,
        JobStatus.SUCCEEDED,
        values={**_LEAVE_RUNNING, "result": result, "finished_at": func.now(), "last_error": None},
        guards=_fence(worker_id, attempt),
        actor=f"worker:{worker_id}",
        reason="succeeded",
    )


async def schedule_retry(
    session: AsyncSession,
    job_id: uuid.UUID,
    worker_id: str,
    attempt: int,
    *,
    error: str,
    delay_seconds: float,
) -> JobRecord | None:
    """running -> retry_scheduled with a Postgres-driven backoff and a future outbox row."""
    record = await _transition(
        session,
        job_id,
        JobStatus.RUNNING,
        JobStatus.RETRY_SCHEDULED,
        values={
            **_LEAVE_RUNNING,
            "available_at": func.now() + _interval(delay_seconds),
            "last_error": error,
        },
        guards=_fence(worker_id, attempt),
        actor=f"worker:{worker_id}",
        reason="retry_scheduled",
        detail={"error": error, "delay_seconds": round(delay_seconds, 3)},
    )
    if record is not None:
        await enqueue_outbox(session, record, "dispatch", available_at=record.available_at)
    return record


async def mark_dead(
    session: AsyncSession,
    job_id: uuid.UUID,
    worker_id: str,
    attempt: int,
    *,
    error: str,
    reason: str,
) -> JobRecord | None:
    record = await _transition(
        session,
        job_id,
        JobStatus.RUNNING,
        JobStatus.DEAD,
        values={**_LEAVE_RUNNING, "last_error": error, "finished_at": func.now()},
        guards=_fence(worker_id, attempt),
        actor=f"worker:{worker_id}",
        reason=reason,
        detail={"error": error},
    )
    if record is not None:
        await enqueue_outbox(session, record, "dlq", extra={"error": error, "reason": reason})
    return record


async def release(
    session: AsyncSession, job_id: uuid.UUID, worker_id: str, attempt: int
) -> JobRecord | None:
    """Graceful shutdown: running -> queued, giving back the attempt that was never finished."""
    record = await _transition(
        session,
        job_id,
        JobStatus.RUNNING,
        JobStatus.QUEUED,
        values={**_LEAVE_RUNNING, "attempts": Job.attempts - 1, "available_at": func.now()},
        guards=_fence(worker_id, attempt),
        actor=f"worker:{worker_id}",
        reason="lease_released",
    )
    if record is not None:
        await enqueue_outbox(session, record, "dispatch")
    return record


async def reclaim_expired(session: AsyncSession, limit: int) -> list[JobRecord]:
    """Reaper: expired leases go back to queued, or to dead when attempts are exhausted."""
    ids = (
        (
            await session.execute(
                select(Job.id)
                .where(Job.status == JobStatus.RUNNING.value, Job.lease_expires_at < func.now())
                .order_by(Job.lease_expires_at)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
        )
        .scalars()
        .all()
    )
    reclaimed: list[JobRecord] = []
    for job_id in ids:
        expired = Job.lease_expires_at < func.now()
        record = await _transition(
            session,
            job_id,
            JobStatus.RUNNING,
            JobStatus.DEAD,
            values={
                **_LEAVE_RUNNING,
                "last_error": "lease expired and attempts exhausted",
                "finished_at": func.now(),
            },
            guards=[expired, Job.attempts >= Job.max_attempts],
            actor="reaper",
            reason="lease_expired_attempts_exhausted",
        )
        if record is not None:
            await enqueue_outbox(
                session,
                record,
                "dlq",
                extra={"error": record.last_error, "reason": "lease_expired"},
            )
            metrics.LEASE_RECLAIMS.labels("dead").inc()
            reclaimed.append(record)
            continue
        record = await _transition(
            session,
            job_id,
            JobStatus.RUNNING,
            JobStatus.QUEUED,
            values={**_LEAVE_RUNNING, "available_at": func.now()},
            guards=[expired, Job.attempts < Job.max_attempts],
            actor="reaper",
            reason="lease_expired",
        )
        if record is not None:
            await enqueue_outbox(session, record, "dispatch")
            metrics.LEASE_RECLAIMS.labels("requeued").inc()
            reclaimed.append(record)
    return reclaimed


async def cancel(
    session: AsyncSession, job_id: uuid.UUID, owner: str
) -> tuple[JobRecord | None, JobRecord | None]:
    """Cancel from queued, running or retry_scheduled.

    Returns (updated, current). updated is None when no transition happened, and current
    is the row as it stands (None if the job does not exist or is not owned by owner).
    """
    row = (
        (
            await session.execute(
                select(*_JOB_COLS).where(Job.id == job_id, Job.owner == owner).with_for_update()
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        return None, None
    current = JobRecord.from_mapping(row)
    if current.status not in CANCELLABLE:
        return None, current
    updated = await _transition(
        session,
        job_id,
        current.status,
        JobStatus.CANCELLED,
        values={**_LEAVE_RUNNING, "finished_at": func.now()},
        actor=f"api:{owner}",
        reason="cancel_requested",
    )
    return updated, updated or current


async def requeue_dead(
    session: AsyncSession, job_id: uuid.UUID, owner: str
) -> tuple[JobRecord | None, JobRecord | None]:
    """DLQ requeue: dead -> queued with a fresh attempt budget."""
    row = (
        (
            await session.execute(
                select(*_JOB_COLS).where(Job.id == job_id, Job.owner == owner).with_for_update()
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        return None, None
    current = JobRecord.from_mapping(row)
    if current.status is not JobStatus.DEAD:
        return None, current
    updated = await _transition(
        session,
        job_id,
        JobStatus.DEAD,
        JobStatus.QUEUED,
        values={"attempts": 0, "available_at": func.now(), "finished_at": None},
        actor=f"api:{owner}",
        reason="dlq_requeue",
    )
    if updated is not None:
        await enqueue_outbox(session, updated, "dispatch")
    return updated, updated or current


async def promote_due_retries(session: AsyncSession, limit: int) -> list[JobRecord]:
    """Scheduler step: retry_scheduled -> queued once the backoff has elapsed (bulk).

    This must be committed before the matching outbox row is published. If the promotion
    shared the publishing transaction, a fast worker could receive the message while the
    job was still retry_scheduled in the committed state, fail its claim and drop it.
    """
    due = (
        select(Job.id)
        .where(Job.status == JobStatus.RETRY_SCHEDULED.value, Job.available_at <= func.now())
        .order_by(Job.available_at)
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    rows = (
        (
            await session.execute(
                update(Job)
                .where(Job.id.in_(due), Job.status == JobStatus.RETRY_SCHEDULED.value)
                .values(
                    status=JobStatus.QUEUED.value,
                    version=Job.version + 1,
                    updated_at=func.now(),
                )
                .returning(*_JOB_COLS)
                .execution_options(synchronize_session=False)
            )
        )
        .mappings()
        .all()
    )
    records = [JobRecord.from_mapping(r) for r in rows]
    if records:
        await session.execute(
            insert(JobEvent),
            [
                {
                    "job_id": r.id,
                    "from_status": JobStatus.RETRY_SCHEDULED.value,
                    "to_status": JobStatus.QUEUED.value,
                    "attempt": r.attempts,
                    "actor": "relay",
                    "reason": "backoff_elapsed",
                }
                for r in records
            ],
        )
        metrics.JOB_TRANSITIONS.labels("retry_scheduled", "queued").inc(len(records))
    return records


async def job_statuses(
    session: AsyncSession, job_ids: Sequence[uuid.UUID]
) -> dict[uuid.UUID, JobStatus]:
    if not job_ids:
        return {}
    rows = await session.execute(select(Job.id, Job.status).where(Job.id.in_(job_ids)))
    return {r.id: JobStatus(r.status) for r in rows}


async def list_jobs(
    session: AsyncSession,
    *,
    owner: str,
    status: JobStatus | None,
    type_: str | None,
    limit: int,
    cursor: tuple[datetime, uuid.UUID] | None,
) -> list[JobRecord]:
    stmt = select(*_JOB_COLS).where(Job.owner == owner)
    if status is not None:
        stmt = stmt.where(Job.status == status.value)
    if type_ is not None:
        stmt = stmt.where(Job.type == type_)
    if cursor is not None:
        stmt = stmt.where(
            (Job.created_at < cursor[0]) | ((Job.created_at == cursor[0]) & (Job.id < cursor[1]))
        )
    stmt = stmt.order_by(Job.created_at.desc(), Job.id.desc()).limit(limit)
    rows = (await session.execute(stmt)).mappings().all()
    return [JobRecord.from_mapping(r) for r in rows]


async def list_events(
    session: AsyncSession, job_id: uuid.UUID, owner: str
) -> list[dict[str, Any]] | None:
    owned = (
        await session.execute(select(Job.id).where(Job.id == job_id, Job.owner == owner))
    ).first()
    if owned is None:
        return None
    rows = (
        (
            await session.execute(
                select(
                    JobEvent.id,
                    JobEvent.from_status,
                    JobEvent.to_status,
                    JobEvent.attempt,
                    JobEvent.actor,
                    JobEvent.reason,
                    JobEvent.detail,
                    JobEvent.created_at,
                )
                .where(JobEvent.job_id == job_id)
                .order_by(JobEvent.id)
            )
        )
        .mappings()
        .all()
    )
    return [{**dict(r), "created_at": r["created_at"].isoformat()} for r in rows]


# ---------------------------------------------------------------- outbox queries


async def fetch_due_outbox(session: AsyncSession, limit: int) -> list[OutboxRecord]:
    """Lock due, unpublished rows. SKIP LOCKED lets concurrent relays take disjoint batches."""
    rows = (
        (
            await session.execute(
                select(Outbox)
                .where(Outbox.published_at.is_(None), Outbox.available_at <= func.now())
                .order_by(Outbox.available_at, Outbox.id)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
        )
        .scalars()
        .all()
    )
    return [
        OutboxRecord(
            id=r.id,
            job_id=r.job_id,
            kind=r.kind,
            payload=r.payload,
            priority=r.priority,
            available_at=r.available_at,
            publish_attempts=r.publish_attempts,
        )
        for r in rows
    ]


async def mark_outbox_published(session: AsyncSession, ids: Sequence[int]) -> None:
    if ids:
        await session.execute(
            update(Outbox)
            .where(Outbox.id.in_(ids))
            .values(published_at=func.now(), publish_attempts=Outbox.publish_attempts + 1)
        )


async def mark_outbox_skipped(session: AsyncSession, ids: Sequence[int], reason: str) -> None:
    if ids:
        await session.execute(
            update(Outbox)
            .where(Outbox.id.in_(ids))
            .values(published_at=func.now(), skipped_reason=reason)
        )


async def mark_outbox_failed(session: AsyncSession, failures: dict[int, str]) -> None:
    for outbox_id, error in failures.items():
        await session.execute(
            update(Outbox)
            .where(Outbox.id == outbox_id)
            .values(publish_attempts=Outbox.publish_attempts + 1, last_error=error[:500])
        )


async def platform_stats(session: AsyncSession) -> dict[str, Any]:
    by_status = {
        r.status: r.n
        for r in await session.execute(
            text("SELECT status, count(*) AS n FROM jobs GROUP BY status")
        )
    }
    outbox = (
        await session.execute(
            text(
                "SELECT count(*) AS pending, "
                "coalesce(extract(epoch FROM now() - min(available_at)), 0) AS lag "
                "FROM outbox WHERE published_at IS NULL AND available_at <= now()"
            )
        )
    ).one()
    return {
        "by_status": by_status,
        "outbox_pending": outbox.pending,
        "outbox_lag": float(outbox.lag),
    }
