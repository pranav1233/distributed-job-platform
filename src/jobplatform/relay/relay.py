"""Transactional outbox relay.

One iteration, in a single transaction:
  1. SELECT due, unpublished outbox rows FOR UPDATE SKIP LOCKED, so concurrent relays
     take disjoint batches.
  2. For dispatch rows, promote retry_scheduled jobs whose backoff has elapsed to queued.
     A dispatch row whose job is no longer queued (cancelled, already running through a
     duplicate delivery, finished) is dropped as stale instead of published.
  3. Publish the rest with publisher confirms, concurrently.
  4. Mark confirmed rows published, record failures, commit.

A crash after a publish but before the commit republishes the batch. That is the
at-least-once edge the worker's conditional claim absorbs.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from jobplatform import metrics
from jobplatform.config import Settings
from jobplatform.domain.states import JobStatus
from jobplatform.messaging.publisher import ConfirmedPublisher
from jobplatform.repo import jobs as repo
from jobplatform.repo.records import JobRecord, OutboxRecord
from jobplatform.runtime import sleep_or_stop
from jobplatform.service import JobService

log = structlog.get_logger(__name__)


@dataclass(slots=True)
class BatchResult:
    published: int = 0
    skipped: int = 0
    failed: int = 0

    @property
    def total(self) -> int:
        return self.published + self.skipped + self.failed


class OutboxRelay:
    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession],
        publisher: ConfirmedPublisher,
        jobs: JobService | None = None,
    ) -> None:
        self._settings = settings
        self._sf = session_factory
        self._publisher = publisher
        self._jobs = jobs

    async def run_once(self) -> BatchResult:
        result = BatchResult()
        async with self._sf() as session, session.begin():
            rows = await repo.fetch_due_outbox(session, self._settings.relay_batch_size)
            if not rows:
                return result
            promoted = await self._promote(session, rows)
            statuses = await repo.job_statuses(
                session, [r.job_id for r in rows if r.kind == "dispatch"]
            )

            to_publish: list[OutboxRecord] = []
            skipped: dict[str, list[int]] = {}
            for row in rows:
                if row.kind == "dispatch" and statuses.get(row.job_id) is not JobStatus.QUEUED:
                    status = statuses.get(row.job_id)
                    reason = f"job_{status.value if status else 'missing'}"
                    skipped.setdefault(reason, []).append(row.id)
                else:
                    to_publish.append(row)

            outcomes = await asyncio.gather(
                *(self._publish_one(r) for r in to_publish), return_exceptions=False
            )
            ok_ids = [r.id for r, err in zip(to_publish, outcomes, strict=True) if err is None]
            failures = {
                r.id: err for r, err in zip(to_publish, outcomes, strict=True) if err is not None
            }
            await repo.mark_outbox_published(session, ok_ids)
            for reason, ids in skipped.items():
                await repo.mark_outbox_skipped(session, ids, reason)
                metrics.OUTBOX_SKIPPED.labels(reason).inc(len(ids))
            await repo.mark_outbox_failed(session, failures)
            result.published = len(ok_ids)
            result.skipped = sum(len(v) for v in skipped.values())
            result.failed = len(failures)
            for r in to_publish:
                if r.id in ok_ids:
                    metrics.OUTBOX_PUBLISHED.labels(r.kind).inc()
            if failures:
                metrics.OUTBOX_PUBLISH_ERRORS.inc(len(failures))
        # Commit succeeded: now invalidate caches for jobs the relay promoted.
        if self._jobs is not None and promoted:
            await self._jobs.notify(promoted)
        return result

    async def _promote(self, session: AsyncSession, rows: list[OutboxRecord]) -> list[JobRecord]:
        return await repo.promote_retries(
            session, list({r.job_id for r in rows if r.kind == "dispatch"})
        )

    async def _publish_one(self, row: OutboxRecord) -> str | None:
        body = dict(row.payload)
        body["outbox_id"] = row.id
        try:
            await self._publisher.publish(row.kind, body, row.priority, str(row.id))
        except Exception as exc:
            return f"{type(exc).__name__}: {exc}"
        return None

    async def run(self, stop: asyncio.Event) -> None:
        interval = self._settings.relay_poll_interval_seconds
        failure_backoff = 0.5
        while not stop.is_set():
            try:
                result = await self.run_once()
            except Exception as exc:
                log.error("relay_iteration_failed", error=repr(exc))
                await sleep_or_stop(stop, min(failure_backoff, 5.0))
                failure_backoff = min(failure_backoff * 2, 5.0)
                continue
            if result.failed and not result.published:
                # Broker trouble: rows stay pending in Postgres, so just back off.
                await sleep_or_stop(stop, min(failure_backoff, 5.0))
                failure_backoff = min(failure_backoff * 2, 5.0)
                continue
            failure_backoff = 0.5
            if result.total >= self._settings.relay_batch_size:
                continue  # more work is waiting, do not sleep
            await sleep_or_stop(stop, interval)
