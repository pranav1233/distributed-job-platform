"""Job worker.

Delivery from RabbitMQ is at-least-once. Correctness comes from three things:
  * the claim is a conditional UPDATE (queued -> running), so a duplicate delivery of a job
    that is running, finished or cancelled fails the claim and is acked and dropped;
  * every later transition is fenced on (lease_owner, attempts), so a worker that lost its
    lease cannot overwrite the new owner's result;
  * the message is acked only after the database transition has committed.

A worker that dies mid-job leaves the row in `running` with an expiring lease. The reaper
reclaims it and writes a fresh outbox row, so recovery never depends on the broker
redelivering the original message.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import structlog
from aio_pika.abc import AbstractIncomingMessage

from jobplatform import metrics
from jobplatform.config import Settings
from jobplatform.domain.backoff import full_jitter_delay
from jobplatform.messaging.topology import declare_topology
from jobplatform.repo.records import JobRecord
from jobplatform.runtime import connect_broker
from jobplatform.service import JobService
from jobplatform.worker.cpu_pool import CpuPool
from jobplatform.worker.handlers.base import JobContext
from jobplatform.worker.registry import JobTypeSpec

log = structlog.get_logger("worker")


@dataclass(slots=True)
class Success:
    result: dict[str, Any]


@dataclass(slots=True)
class Failure:
    error: BaseException


@dataclass(slots=True)
class LeaseLost:
    pass


Outcome = Success | Failure | LeaseLost


class Worker:
    def __init__(
        self,
        settings: Settings,
        jobs: JobService,
        registry: dict[str, JobTypeSpec],
        *,
        worker_id: str | None = None,
        cpu_pool: CpuPool | None = None,
    ) -> None:
        self.settings = settings
        self.jobs = jobs
        self.registry = registry
        self.worker_id = worker_id or f"w-{uuid.uuid4().hex[:10]}"
        self._cpu_pool = cpu_pool
        self._sem = asyncio.Semaphore(settings.worker_concurrency)
        self._cpu_sem = asyncio.Semaphore(cpu_pool.size if cpu_pool else 1)
        self._tasks: set[asyncio.Task[None]] = set()
        self._stop = asyncio.Event()

    # ------------------------------------------------------------------ lifecycle

    async def run(self, stop: asyncio.Event) -> None:
        self._stop = stop
        connection = await connect_broker(self.settings.rabbitmq_url, stop)
        if connection is None:
            return
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=self.settings.worker_prefetch)
        topology = await declare_topology(channel, self.settings)
        consumer_tag = await topology.work_queue.consume(self._on_message)
        log.info(
            "worker_started",
            worker_id=self.worker_id,
            concurrency=self.settings.worker_concurrency,
            prefetch=self.settings.worker_prefetch,
        )
        try:
            await stop.wait()
        finally:
            log.info("worker_stopping", worker_id=self.worker_id, inflight=len(self._tasks))
            await topology.work_queue.cancel(consumer_tag)
            await self._drain()
            await connection.close()
            log.info("worker_stopped", worker_id=self.worker_id)

    async def _drain(self) -> None:
        """Let in-flight jobs finish within the grace period, then release what is left."""
        pending = set(self._tasks)
        if pending:
            _, pending = await asyncio.wait(pending, timeout=self.settings.shutdown_grace_seconds)
        for task in pending:
            task.cancel()  # each task releases its own lease on cancellation
        if pending:
            await asyncio.wait(pending, timeout=15)
        await self.jobs.flush_hooks()

    async def _on_message(self, message: AbstractIncomingMessage) -> None:
        task = asyncio.create_task(self._handle(message))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    # ------------------------------------------------------------------ message path

    async def _handle(self, message: AbstractIncomingMessage) -> None:
        try:
            body = json.loads(message.body)
            job_id = uuid.UUID(body["job_id"])
        except (ValueError, KeyError, TypeError, UnicodeDecodeError):
            metrics.POISON_MESSAGES.inc()
            log.warning("poison_message_dead_lettered", message_id=message.message_id)
            await self._settle(message.reject(requeue=False))
            return

        spec = self.registry.get(str(body.get("type", "")))
        need_cpu = bool(spec and spec.cpu_bound and self._cpu_pool)
        got_cpu = False
        got_slot = False
        try:
            if need_cpu:
                got_cpu = await self._acquire(self._cpu_sem)
                if not got_cpu:
                    await self._settle(message.nack(requeue=True))
                    return
            got_slot = await self._acquire(self._sem)
            if not got_slot:
                await self._settle(message.nack(requeue=True))
                return

            try:
                record = await self.jobs.claim(job_id, self.worker_id, self.settings.lease_seconds)
            except Exception as exc:
                log.error("claim_failed", job_id=str(job_id), error=repr(exc))
                await asyncio.sleep(1.0)  # throttle redelivery while the database is unhealthy
                await self._settle(message.nack(requeue=True))
                return
            if record is None:
                metrics.DUPLICATE_DELIVERIES.inc()
                log.info("duplicate_delivery_dropped", job_id=str(job_id))
                await self._settle(message.ack())
                return

            committed = await self._execute(record)
            if committed:
                await self._settle(message.ack())  # ack only after the transition committed
            else:
                await self._settle(message.nack(requeue=True))
        finally:
            if got_slot:
                self._sem.release()
            if got_cpu:
                self._cpu_sem.release()

    async def _acquire(self, sem: asyncio.Semaphore) -> bool:
        """Acquire a slot, or give up if shutdown starts first."""
        if self._stop.is_set():
            return False
        acquire = asyncio.ensure_future(sem.acquire())
        stopped = asyncio.ensure_future(self._stop.wait())
        try:
            await asyncio.wait({acquire, stopped}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            stopped.cancel()
        if acquire.done() and not acquire.cancelled():
            if self._stop.is_set():
                sem.release()
                return False
            return True
        acquire.cancel()
        return False

    @staticmethod
    async def _settle(op: Awaitable[Any]) -> None:
        try:
            await op
        except Exception as exc:
            # Channel closed or connection lost: the broker redelivers, and the claim
            # check drops the duplicate. State in Postgres is already correct.
            log.warning("ack_failed", error=repr(exc))

    # ------------------------------------------------------------------ execution

    async def _execute(self, record: JobRecord) -> bool:
        """Run the job and commit its outcome. Returns False if the commit could not be made."""
        structlog.contextvars.bind_contextvars(
            job_id=str(record.id),
            owner=record.owner,
            job_type=record.type,
            attempt=record.attempts,
            worker_id=self.worker_id,
        )
        spec = self.registry.get(record.type)
        started = time.perf_counter()
        metrics.WORKER_INFLIGHT.inc()
        lease_lost = asyncio.Event()
        heartbeat = asyncio.create_task(self._heartbeat(record, lease_lost))
        try:
            if spec is None:
                outcome: Outcome = Failure(
                    _UnknownTypeError(f"no handler for type {record.type!r}")
                )
            else:
                outcome = await self._run_handler(spec, record, lease_lost)
            elapsed = time.perf_counter() - started
            try:
                return await self._commit_outcome(record, spec, outcome, elapsed)
            except _CommitFailedError as exc:
                # Postgres stayed unreachable. Do not ack: the delivery is requeued, the claim
                # check drops it while the lease is live, and the reaper recovers the job.
                log.error("commit_failed", error=str(exc))
                return False
        except asyncio.CancelledError:
            # Shutdown deadline reached: hand the job back instead of letting the lease lapse.
            await self._release(record)
            return True
        finally:
            heartbeat.cancel()
            metrics.WORKER_INFLIGHT.dec()

    async def _run_handler(
        self, spec: JobTypeSpec, record: JobRecord, lease_lost: asyncio.Event
    ) -> Outcome:
        if spec.cpu_bound:
            if self._cpu_pool is None or spec.cpu_handler is None:
                return Failure(_UnknownTypeError("CPU pool is not configured on this worker"))
            coro: Awaitable[dict[str, Any]] = self._cpu_pool.run(
                spec.cpu_handler, record.payload, record.attempts
            )
        elif spec.handler is not None:
            coro = spec.handler(
                JobContext(
                    job_id=record.id,
                    owner=record.owner,
                    type=record.type,
                    payload=record.payload,
                    attempt=record.attempts,
                    max_attempts=record.max_attempts,
                    worker_id=self.worker_id,
                    session_factory=self.jobs.session_factory,
                )
            )
        else:
            return Failure(_UnknownTypeError(f"type {spec.name!r} has no handler"))

        handler_task = asyncio.ensure_future(coro)
        lost_task = asyncio.ensure_future(lease_lost.wait())
        try:
            done, _ = await asyncio.wait(
                {handler_task, lost_task},
                timeout=spec.timeout_seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )
        except asyncio.CancelledError:
            await _cancel(handler_task)
            raise
        finally:
            lost_task.cancel()

        if handler_task in done:
            if handler_task.cancelled():
                return Failure(RuntimeError("handler was cancelled"))
            exc = handler_task.exception()
            if exc is not None:
                return Failure(exc)
            result = handler_task.result()
            if not isinstance(result, dict):
                return Failure(_BadResultError("handler must return a dict"))
            return Success(result)

        await _cancel(handler_task)  # cooperative cancellation
        if lease_lost.is_set():
            return LeaseLost()
        return Failure(TimeoutError(f"job exceeded its {spec.timeout_seconds:g}s timeout"))

    async def _heartbeat(self, record: JobRecord, lease_lost: asyncio.Event) -> None:
        interval = self.settings.heartbeat_interval_seconds
        while True:
            await asyncio.sleep(interval)
            try:
                alive = await self.jobs.heartbeat(
                    record.id, self.worker_id, record.attempts, self.settings.lease_seconds
                )
            except Exception as exc:
                metrics.HEARTBEAT_FAILURES.inc()
                log.warning("heartbeat_failed", error=repr(exc))
                continue
            if not alive:
                metrics.LEASE_LOST.inc()
                log.warning("lease_lost")
                lease_lost.set()
                return

    # ------------------------------------------------------------------ outcomes

    async def _commit_outcome(
        self, record: JobRecord, spec: JobTypeSpec | None, outcome: Outcome, elapsed: float
    ) -> bool:
        if isinstance(outcome, LeaseLost):
            metrics.JOB_DURATION.labels(record.type, "lease_lost").observe(elapsed)
            log.info("job_abandoned_lease_lost")
            return True

        if isinstance(outcome, Success):
            done = await self._with_retries(
                lambda: self.jobs.complete(
                    record.id, self.worker_id, record.attempts, outcome.result
                )
            )
            if done is None:
                log.warning("complete_fenced_out")
            else:
                metrics.JOB_DURATION.labels(record.type, "succeeded").observe(elapsed)
                metrics.JOB_END_TO_END.labels(record.type).observe(
                    (done.finished_at - done.created_at).total_seconds() if done.finished_at else 0
                )
                log.info("job_succeeded", duration_ms=round(elapsed * 1000, 1))
            return True

        exc = outcome.error
        error = f"{type(exc).__name__}: {exc}"[:2000]
        retryable = spec.is_retryable(exc) if spec else False
        if retryable and spec is not None and record.attempts < record.max_attempts:
            delay = full_jitter_delay(
                record.attempts, spec.base_delay_seconds, spec.max_delay_seconds
            )
            scheduled = await self._with_retries(
                lambda: self.jobs.schedule_retry(
                    record.id, self.worker_id, record.attempts, error=error, delay_seconds=delay
                )
            )
            if scheduled is not None:
                metrics.JOB_RETRIES.labels(record.type).inc()
                metrics.JOB_DURATION.labels(record.type, "retry").observe(elapsed)
                log.info("job_retry_scheduled", error=error, delay_s=round(delay, 3))
            return True

        reason = "retries_exhausted" if retryable else "non_retryable"
        dead = await self._with_retries(
            lambda: self.jobs.mark_dead(
                record.id, self.worker_id, record.attempts, error=error, reason=reason
            )
        )
        if dead is not None:
            metrics.JOB_DURATION.labels(record.type, "dead").observe(elapsed)
            log.warning("job_dead", reason=reason, error=error)
        return True

    async def _release(self, record: JobRecord) -> None:
        try:
            await asyncio.shield(self.jobs.release(record.id, self.worker_id, record.attempts))
            log.info("lease_released_on_shutdown", job_id=str(record.id))
        except Exception as exc:
            # The lease will expire and the reaper will reclaim the job.
            log.warning("lease_release_failed", error=repr(exc))

    @staticmethod
    async def _with_retries[T](op: Callable[[], Awaitable[T]], attempts: int = 4) -> T | None:
        """Retry a commit through a short database blip. Raises after the last attempt."""
        delay = 0.25
        for i in range(attempts):
            try:
                return await op()
            except Exception as exc:
                if i == attempts - 1:
                    raise _CommitFailedError(repr(exc)) from exc
                log.warning("commit_retry", error=repr(exc), retry_in=delay)
                await asyncio.sleep(delay)
                delay *= 2
        return None


async def _cancel(task: asyncio.Future[Any]) -> None:
    task.cancel()
    await asyncio.wait({task}, timeout=10)


class _UnknownTypeError(Exception):
    pass


class _BadResultError(Exception):
    pass


class _CommitFailedError(Exception):
    pass
