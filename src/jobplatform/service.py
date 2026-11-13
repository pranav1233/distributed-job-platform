"""Transaction boundaries and post-commit hooks around the job repository.

Each method opens one transaction, runs the repository call, commits, and only then
invokes the transition hook (cache invalidation). A hook failure never fails the
operation: the cache has a TTL and a version guard, so staleness is bounded.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from jobplatform.repo import jobs as repo
from jobplatform.repo.records import JobRecord

log = structlog.get_logger(__name__)

TransitionHook = Callable[[JobRecord], Awaitable[None]]


class JobService:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        on_transition: TransitionHook | None = None,
        *,
        background_hooks: bool = False,
    ) -> None:
        self._sf = session_factory
        self._hook = on_transition
        # Background hooks keep a slow or unavailable Redis off the worker's critical path.
        self._background = background_hooks
        self._pending: set[asyncio.Task[None]] = set()

    @property
    def session_factory(self) -> async_sessionmaker[AsyncSession]:
        return self._sf

    async def _run_hook(self, record: JobRecord) -> None:
        assert self._hook is not None
        try:
            await self._hook(record)
        except Exception as exc:
            log.warning("transition_hook_failed", job_id=str(record.id), error=repr(exc))

    async def _notify(self, records: Sequence[JobRecord | None]) -> None:
        if self._hook is None:
            return
        for record in records:
            if record is None:
                continue
            if self._background:
                if len(self._pending) < 1000:
                    task = asyncio.create_task(self._run_hook(record))
                    self._pending.add(task)
                    task.add_done_callback(self._pending.discard)
            else:
                await self._run_hook(record)

    async def flush_hooks(self) -> None:
        if self._pending:
            await asyncio.gather(*self._pending, return_exceptions=True)

    async def create(self, **kwargs: Any) -> tuple[JobRecord, bool]:
        async with self._sf() as session, session.begin():
            record, created = await repo.create_job(session, **kwargs)
        return record, created

    async def get(self, job_id: uuid.UUID) -> JobRecord | None:
        async with self._sf() as session:
            return await repo.get_job(session, job_id)

    async def claim(
        self, job_id: uuid.UUID, worker_id: str, lease_seconds: float
    ) -> JobRecord | None:
        async with self._sf() as session, session.begin():
            record = await repo.claim(session, job_id, worker_id, lease_seconds)
        await self._notify([record])
        return record

    async def heartbeat(
        self, job_id: uuid.UUID, worker_id: str, attempt: int, lease_seconds: float
    ) -> bool:
        async with self._sf() as session, session.begin():
            return await repo.heartbeat(session, job_id, worker_id, attempt, lease_seconds)

    async def complete(
        self, job_id: uuid.UUID, worker_id: str, attempt: int, result: dict[str, Any] | None
    ) -> JobRecord | None:
        async with self._sf() as session, session.begin():
            record = await repo.complete(session, job_id, worker_id, attempt, result)
        await self._notify([record])
        return record

    async def schedule_retry(
        self, job_id: uuid.UUID, worker_id: str, attempt: int, *, error: str, delay_seconds: float
    ) -> JobRecord | None:
        async with self._sf() as session, session.begin():
            record = await repo.schedule_retry(
                session, job_id, worker_id, attempt, error=error, delay_seconds=delay_seconds
            )
        await self._notify([record])
        return record

    async def mark_dead(
        self, job_id: uuid.UUID, worker_id: str, attempt: int, *, error: str, reason: str
    ) -> JobRecord | None:
        async with self._sf() as session, session.begin():
            record = await repo.mark_dead(
                session, job_id, worker_id, attempt, error=error, reason=reason
            )
        await self._notify([record])
        return record

    async def release(self, job_id: uuid.UUID, worker_id: str, attempt: int) -> JobRecord | None:
        async with self._sf() as session, session.begin():
            record = await repo.release(session, job_id, worker_id, attempt)
        await self._notify([record])
        return record

    async def reclaim_expired(self, limit: int) -> list[JobRecord]:
        async with self._sf() as session, session.begin():
            records = await repo.reclaim_expired(session, limit)
        await self._notify(records)
        return records

    async def cancel(
        self, job_id: uuid.UUID, owner: str
    ) -> tuple[JobRecord | None, JobRecord | None]:
        async with self._sf() as session, session.begin():
            updated, current = await repo.cancel(session, job_id, owner)
        await self._notify([updated])
        return updated, current

    async def requeue_dead(
        self, job_id: uuid.UUID, owner: str
    ) -> tuple[JobRecord | None, JobRecord | None]:
        async with self._sf() as session, session.begin():
            updated, current = await repo.requeue_dead(session, job_id, owner)
        await self._notify([updated])
        return updated, current

    async def notify(self, records: Sequence[JobRecord]) -> None:
        await self._notify(records)
