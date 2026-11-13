"""Reaper: reclaims expired leases and publishes platform gauges.

A running job whose lease expired belongs to a worker that crashed, hung, or lost its
database connection. The reaper moves it back to queued (or to dead once attempts are
exhausted), writes the audit event, and writes a new outbox row, all in one transaction.
Multiple reapers are safe: the scan uses FOR UPDATE SKIP LOCKED.
"""

from __future__ import annotations

import asyncio
import time

import aio_pika
import structlog
from aio_pika.abc import AbstractRobustConnection
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from jobplatform import metrics
from jobplatform.config import Settings
from jobplatform.repo import jobs as repo
from jobplatform.runtime import sleep_or_stop
from jobplatform.service import JobService

log = structlog.get_logger("reaper")


class Reaper:
    def __init__(
        self,
        settings: Settings,
        jobs: JobService,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._settings = settings
        self._jobs = jobs
        self._sf = session_factory
        self._connection: AbstractRobustConnection | None = None

    async def reclaim_once(self) -> int:
        records = await self._jobs.reclaim_expired(self._settings.reaper_batch_size)
        for r in records:
            log.warning(
                "lease_reclaimed",
                job_id=str(r.id),
                owner=r.owner,
                outcome=r.status.value,
                attempts=r.attempts,
            )
        return len(records)

    async def prune_once(self) -> int:
        """Housekeeping: published outbox rows are only needed for a short debugging window."""
        total = 0
        while True:
            async with self._sf() as session, session.begin():
                n = await repo.prune_outbox(
                    session,
                    self._settings.outbox_retention_seconds,
                    self._settings.outbox_prune_batch,
                )
            total += n
            if n < self._settings.outbox_prune_batch:
                return total

    async def collect_stats(self) -> None:
        async with self._sf() as session:
            stats = await repo.platform_stats(session)
        known = ("queued", "running", "succeeded", "retry_scheduled", "dead", "cancelled")
        for status in known:
            metrics.JOBS_BY_STATUS.labels(status).set(stats["by_status"].get(status, 0))
        metrics.DLQ_SIZE.labels("postgres").set(stats["by_status"].get("dead", 0))
        metrics.OUTBOX_PENDING.set(stats["outbox_pending"])
        metrics.OUTBOX_LAG.set(stats["outbox_lag"])
        await self._collect_broker_stats()

    async def _collect_broker_stats(self) -> None:
        try:
            if self._connection is None or self._connection.is_closed:
                self._connection = await aio_pika.connect_robust(self._settings.rabbitmq_url)
            async with self._connection.channel() as channel:
                work = await channel.declare_queue(self._settings.queue_name, passive=True)
                dlq = await channel.declare_queue(self._settings.dlq_name, passive=True)
                metrics.QUEUE_DEPTH.set(work.declaration_result.message_count or 0)
                metrics.DLQ_SIZE.labels("broker").set(dlq.declaration_result.message_count or 0)
        except Exception as exc:
            log.warning("broker_stats_unavailable", error=repr(exc))
            self._connection = None

    async def run(self, stop: asyncio.Event) -> None:
        next_stats = 0.0
        try:
            while not stop.is_set():
                try:
                    reclaimed = await self.reclaim_once()
                    if time.monotonic() >= next_stats:
                        await self.collect_stats()
                        await self.prune_once()
                        next_stats = time.monotonic() + self._settings.stats_interval_seconds
                    if reclaimed >= self._settings.reaper_batch_size:
                        continue
                except Exception as exc:
                    log.error("reaper_iteration_failed", error=repr(exc))
                await sleep_or_stop(stop, self._settings.reaper_interval_seconds)
        finally:
            if self._connection is not None:
                await self._connection.close()
