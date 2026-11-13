"""Integration fixtures: real PostgreSQL, Redis and RabbitMQ.

URLs come from TEST_DATABASE_URL / TEST_REDIS_URL / TEST_RABBITMQ_URL when set (CI service
containers), otherwise throwaway testcontainers are started once per session.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import uuid
from collections.abc import AsyncIterator, Callable, Iterator
from dataclasses import dataclass, field
from typing import Any

import aio_pika
import httpx
import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from asgi_lifespan import LifespanManager
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from jobplatform.api.app import create_app
from jobplatform.config import Settings
from jobplatform.db.session import create_engine, create_session_factory
from jobplatform.domain.states import JobStatus
from jobplatform.messaging.publisher import ConfirmedPublisher
from jobplatform.messaging.topology import declare_topology
from jobplatform.reaper.reaper import Reaper
from jobplatform.relay.relay import OutboxRelay
from jobplatform.repo.api_keys import create_api_key
from jobplatform.repo.records import JobRecord
from jobplatform.service import JobService
from jobplatform.worker.cpu_pool import CpuPool
from jobplatform.worker.registry import JobTypeSpec, build_registry
from jobplatform.worker.worker import Worker


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)


@dataclass(frozen=True)
class Infra:
    database_url: str
    redis_url: str
    rabbitmq_url: str


@pytest.fixture(scope="session")
def infra() -> Iterator[Infra]:
    db = os.environ.get("TEST_DATABASE_URL")
    redis = os.environ.get("TEST_REDIS_URL")
    rabbit = os.environ.get("TEST_RABBITMQ_URL")
    if db and redis and rabbit:
        yield Infra(db, redis, rabbit)
        return

    from testcontainers.community.postgres import PostgresContainer
    from testcontainers.community.rabbitmq import RabbitMqContainer
    from testcontainers.community.redis import RedisContainer

    with (
        PostgresContainer(
            "postgres:16-alpine", username="jobs", password="jobs", dbname="jobs"
        ) as pg,
        RedisContainer("redis:7-alpine") as rd,
        RabbitMqContainer("rabbitmq:3.13-management-alpine") as mq,
    ):
        pg_url = (
            f"postgresql+asyncpg://jobs:jobs@{pg.get_container_host_ip()}:"
            f"{pg.get_exposed_port(5432)}/jobs"
        )
        rd_url = f"redis://{rd.get_container_host_ip()}:{rd.get_exposed_port(6379)}/0"
        mq_url = f"amqp://guest:guest@{mq.get_container_host_ip()}:{mq.get_exposed_port(5672)}/"
        yield Infra(pg_url, rd_url, mq_url)


@pytest.fixture(scope="session")
def migrated(infra: Infra) -> Infra:
    os.environ["DATABASE_URL"] = infra.database_url
    cfg = Config("alembic.ini")
    command.upgrade(cfg, "head")
    return infra


@pytest.fixture
def settings(migrated: Infra) -> Settings:
    return Settings(
        database_url=migrated.database_url,
        redis_url=migrated.redis_url,
        rabbitmq_url=migrated.rabbitmq_url,
        log_json=False,
        log_level="WARNING",
        lease_seconds=3.0,
        heartbeat_interval_seconds=0.5,
        relay_poll_interval_seconds=0.05,
        reaper_interval_seconds=0.2,
        retry_base_delay_seconds=0.2,
        retry_max_delay_seconds=1.0,
        worker_concurrency=8,
        worker_prefetch=8,
        worker_cpu_processes=2,
        shutdown_grace_seconds=2.0,
        default_rate_limit_capacity=10_000,
        default_rate_limit_refill_per_sec=10_000,
        cache_ttl_active_seconds=3,
        db_pool_size=10,
    )


@pytest_asyncio.fixture
async def engine(settings: Settings) -> AsyncIterator[AsyncEngine]:
    eng = create_engine(settings)
    yield eng
    await eng.dispose()


@pytest.fixture
def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return create_session_factory(engine)


@pytest_asyncio.fixture(autouse=True)
async def clean_state(
    engine: AsyncEngine, settings: Settings, migrated: Infra
) -> AsyncIterator[None]:
    """Reset Postgres, Redis and RabbitMQ before every test."""
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "TRUNCATE outbox, job_events, job_effects, jobs, api_keys RESTART IDENTITY CASCADE"
            )
        )
    redis = Redis.from_url(settings.redis_url)
    await redis.flushall()
    await redis.aclose()
    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    async with connection:
        channel = await connection.channel()
        topo = await declare_topology(channel, settings)
        await topo.work_queue.purge()
        await topo.dlq.purge()
    yield


@pytest_asyncio.fixture
async def redis(settings: Settings) -> AsyncIterator[Redis]:
    client = Redis.from_url(settings.redis_url)
    yield client
    await client.aclose()


@pytest_asyncio.fixture
async def service(session_factory: async_sessionmaker[AsyncSession]) -> JobService:
    return JobService(session_factory)


@pytest_asyncio.fixture
async def api(settings: Settings) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(settings)
    async with LifespanManager(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            client.app = app  # type: ignore[attr-defined]
            yield client


@pytest_asyncio.fixture
async def api_key(session_factory: async_sessionmaker[AsyncSession]) -> dict[str, str]:
    async with session_factory() as session, session.begin():
        raw, _ = await create_api_key(session, owner="alice")
    return {"Authorization": f"Bearer {raw}"}


async def make_key(
    session_factory: async_sessionmaker[AsyncSession],
    owner: str,
    capacity: int | None = None,
    refill: float | None = None,
) -> dict[str, str]:
    async with session_factory() as session, session.begin():
        raw, _ = await create_api_key(
            session, owner=owner, rate_limit_capacity=capacity, rate_limit_refill_per_sec=refill
        )
    return {"Authorization": f"Bearer {raw}"}


@dataclass
class Stack:
    """Runs relay, worker and reaper instances in-process as asyncio tasks."""

    settings: Settings
    session_factory: async_sessionmaker[AsyncSession]
    tasks: list[asyncio.Task[None]] = field(default_factory=list)
    stops: list[asyncio.Event] = field(default_factory=list)
    publishers: list[ConfirmedPublisher] = field(default_factory=list)
    workers: list[Worker] = field(default_factory=list)
    cpu_pools: list[CpuPool] = field(default_factory=list)

    async def start_relay(self, jobs: JobService | None = None) -> OutboxRelay:
        publisher = ConfirmedPublisher(self.settings)
        await publisher.start()
        self.publishers.append(publisher)
        relay = OutboxRelay(self.settings, self.session_factory, publisher, jobs)
        self._spawn(relay.run)
        return relay

    async def start_worker(
        self,
        *,
        registry: dict[str, JobTypeSpec] | None = None,
        cpu: bool = False,
        worker_id: str | None = None,
        settings: Settings | None = None,
    ) -> tuple[Worker, asyncio.Event]:
        s = settings or self.settings
        pool = CpuPool(s.worker_cpu_processes) if cpu else None
        if pool:
            self.cpu_pools.append(pool)
        worker = Worker(
            s,
            JobService(self.session_factory),
            registry or build_registry(s),
            worker_id=worker_id,
            cpu_pool=pool,
        )
        stop = asyncio.Event()
        self.stops.append(stop)
        self.workers.append(worker)
        self.tasks.append(asyncio.create_task(worker.run(stop)))
        # wait until the consumer is attached
        await asyncio.sleep(0.5)
        return worker, stop

    async def start_reaper(self) -> Reaper:
        reaper = Reaper(self.settings, JobService(self.session_factory), self.session_factory)
        self._spawn(reaper.run)
        return reaper

    def _spawn(self, run: Callable[[asyncio.Event], Any]) -> None:
        stop = asyncio.Event()
        self.stops.append(stop)
        self.tasks.append(asyncio.create_task(run(stop)))

    async def shutdown(self) -> None:
        for stop in self.stops:
            stop.set()
        if self.tasks:
            await asyncio.wait(self.tasks, timeout=20)
        for task in self.tasks:
            if not task.done():
                task.cancel()
        for publisher in self.publishers:
            with contextlib.suppress(Exception):
                await publisher.close()
        for pool in self.cpu_pools:
            pool.shutdown()


@pytest_asyncio.fixture
async def stack(
    settings: Settings, session_factory: async_sessionmaker[AsyncSession]
) -> AsyncIterator[Stack]:
    s = Stack(settings, session_factory)
    yield s
    await s.shutdown()


async def wait_for(
    predicate: Callable[[], Any], timeout: float = 20.0, interval: float = 0.05, what: str = ""
) -> Any:
    """Poll an async or sync predicate until it returns something truthy."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        value = predicate()
        if asyncio.iscoroutine(value):
            value = await value
        if value:
            return value
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"timed out waiting for {what or predicate}")
        await asyncio.sleep(interval)


async def wait_for_status(
    service: JobService,
    job_id: uuid.UUID | str,
    *statuses: JobStatus,
    timeout: float = 20.0,
) -> JobRecord:
    jid = uuid.UUID(str(job_id))

    async def check() -> JobRecord | None:
        record = await service.get(jid)
        return record if record and record.status in statuses else None

    result: JobRecord = await wait_for(
        check, timeout=timeout, what=f"job {jid} to reach {[s.value for s in statuses]}"
    )
    return result


async def fetch_events(
    session_factory: async_sessionmaker[AsyncSession], job_id: uuid.UUID | str
) -> list[dict[str, Any]]:
    async with session_factory() as session:
        rows = await session.execute(
            text(
                "SELECT from_status, to_status, attempt, actor, reason FROM job_events "
                "WHERE job_id = :j ORDER BY id"
            ),
            {"j": str(job_id)},
        )
        return [dict(r._mapping) for r in rows]


async def new_job(
    service: JobService,
    type_: str = "io_task",
    payload: dict[str, Any] | None = None,
    owner: str = "alice",
    **kw: Any,
) -> JobRecord:
    record, _ = await service.create(
        owner=owner,
        type_=type_,
        payload=payload or {},
        priority=kw.get("priority", 5),
        max_attempts=kw.get("max_attempts", 5),
        delay_seconds=kw.get("delay_seconds", 0),
        idempotency_key=kw.get("idempotency_key"),
        fingerprint=kw.get("fingerprint"),
    )
    return record


async def drain_queue(
    settings: Settings, queue_name: str, expected: int, timeout: float = 10, raw: bool = False
) -> list[Any]:
    """Read up to `expected` messages from a queue (acking them)."""
    import json

    out: list[Any] = []
    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(queue_name, passive=True)
        deadline = asyncio.get_running_loop().time() + timeout
        while len(out) < expected and asyncio.get_running_loop().time() < deadline:
            msg = await queue.get(fail=False, timeout=1)
            if msg is None:
                await asyncio.sleep(0.05)
                continue
            out.append(msg.body if raw else json.loads(msg.body))
            await msg.ack()
        # take anything extra that is already there to expose duplicates
        while True:
            msg = await queue.get(fail=False, timeout=1)
            if msg is None:
                break
            out.append(msg.body if raw else json.loads(msg.body))
            await msg.ack()
    return out
