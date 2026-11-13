from __future__ import annotations

import asyncio
import socket
import uuid

import structlog

from jobplatform.config import get_settings
from jobplatform.db.session import create_engine, create_session_factory
from jobplatform.logging import configure_logging
from jobplatform.runtime import build_cache, install_signal_handlers, start_metrics_server
from jobplatform.service import JobService
from jobplatform.worker.cpu_pool import CpuPool
from jobplatform.worker.registry import build_registry
from jobplatform.worker.worker import Worker

log = structlog.get_logger("worker")


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_json, service="worker")
    stop = asyncio.Event()
    install_signal_handlers(stop)
    start_metrics_server(settings.worker_metrics_port)

    engine = create_engine(settings)
    factory = create_session_factory(engine)
    redis, cache = build_cache(settings)
    jobs = JobService(factory, cache.invalidate, background_hooks=True)
    cpu_pool = CpuPool(settings.worker_cpu_processes)
    worker_id = f"{socket.gethostname()}-{uuid.uuid4().hex[:6]}"
    worker = Worker(
        settings, jobs, build_registry(settings), worker_id=worker_id, cpu_pool=cpu_pool
    )
    try:
        await worker.run(stop)
    finally:
        cpu_pool.shutdown()
        await redis.aclose()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
