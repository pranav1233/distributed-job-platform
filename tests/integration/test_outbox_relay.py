from __future__ import annotations

import asyncio
import json
from collections import Counter

import aio_pika
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from jobplatform.config import Settings
from jobplatform.messaging.publisher import ConfirmedPublisher
from jobplatform.relay.relay import OutboxRelay
from jobplatform.repo import jobs as repo
from jobplatform.service import JobService
from tests.integration.conftest import drain_queue, new_job, wait_for


async def test_job_and_outbox_row_are_one_atomic_write(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """If the transaction fails after the job insert, neither the job nor its outbox row exists."""
    with pytest.raises(RuntimeError):
        async with session_factory() as session, session.begin():
            await repo.create_job(
                session,
                owner="alice",
                type_="io_task",
                payload={},
                priority=5,
                max_attempts=3,
                delay_seconds=0,
                idempotency_key=None,
                fingerprint=None,
            )
            raise RuntimeError("crash before commit")
    async with session_factory() as session:
        jobs = (await session.execute(text("SELECT count(*) FROM jobs"))).scalar()
        outbox = (await session.execute(text("SELECT count(*) FROM outbox"))).scalar()
        events = (await session.execute(text("SELECT count(*) FROM job_events"))).scalar()
    assert (jobs, outbox, events) == (0, 0, 0)


async def test_two_concurrent_relays_publish_every_row_exactly_once(
    settings: Settings,
    service: JobService,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    total = 600
    settings = settings.model_copy(update={"relay_batch_size": 50})
    ids = set()
    for chunk in range(0, total, 100):
        records = await asyncio.gather(*(new_job(service) for _ in range(100)))
        ids.update(str(r.id) for r in records)
        assert chunk >= 0
    assert len(ids) == total

    publishers = [ConfirmedPublisher(settings), ConfirmedPublisher(settings)]
    for p in publishers:
        await p.start()
    relays = [OutboxRelay(settings, session_factory, p) for p in publishers]

    async def drain(relay: OutboxRelay) -> int:
        published = 0
        while True:
            result = await relay.run_once()
            if result.total == 0:
                return published
            published += result.published

    counts = await asyncio.gather(*(drain(r) for r in relays))
    for p in publishers:
        await p.close()

    assert sum(counts) == total
    assert all(c > 0 for c in counts), f"both relays should have taken batches, got {counts}"

    messages = await drain_queue(settings, settings.queue_name, expected=total, timeout=20)
    seen = Counter(m["job_id"] for m in messages)
    assert set(seen) == ids  # nothing lost
    assert max(seen.values()) == 1  # nothing duplicated

    async with session_factory() as session:
        pending = (
            await session.execute(text("SELECT count(*) FROM outbox WHERE published_at IS NULL"))
        ).scalar()
    assert pending == 0


async def test_unconfirmed_rows_stay_pending_and_are_retried(
    settings: Settings,
    service: JobService,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """A broker failure must not lose or drop outbox rows."""
    job = await new_job(service)

    class FlakyPublisher(ConfirmedPublisher):
        failures_left = 2

        async def publish(self, kind, body, priority, message_id):  # type: ignore[no-untyped-def]
            if self.failures_left > 0:
                self.failures_left -= 1
                raise ConnectionError("broker unavailable")
            await super().publish(kind, body, priority, message_id)

    publisher = FlakyPublisher(settings)
    await publisher.start()
    relay = OutboxRelay(settings, session_factory, publisher)
    first = await relay.run_once()
    assert (first.published, first.failed) == (0, 1)
    second = await relay.run_once()
    assert second.failed == 1
    third = await relay.run_once()
    assert third.published == 1
    await publisher.close()

    async with session_factory() as session:
        row = (
            await session.execute(
                text("SELECT publish_attempts, published_at, last_error FROM outbox")
            )
        ).one()
    assert row.published_at is not None
    assert row.publish_attempts == 3  # two failed confirms, then the successful one
    assert "broker unavailable" in row.last_error
    msgs = await drain_queue(settings, settings.queue_name, expected=1, timeout=5)
    assert [m["job_id"] for m in msgs] == [str(job.id)]


async def test_messages_are_persistent_and_prioritised(
    settings: Settings,
    service: JobService,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    low = await new_job(service, priority=1)
    high = await new_job(service, priority=9)
    publisher = ConfirmedPublisher(settings)
    await publisher.start()
    await OutboxRelay(settings, session_factory, publisher).run_once()
    await publisher.close()

    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(settings.queue_name, passive=True)
        first = await queue.get(timeout=5)
        second = await queue.get(timeout=5)
        assert json.loads(first.body)["job_id"] == str(high.id)  # priority 9 jumps the queue
        assert json.loads(second.body)["job_id"] == str(low.id)
        assert first.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
        await first.ack()
        await second.ack()
    await wait_for(lambda: True)
