from __future__ import annotations

import asyncio

import structlog

from jobplatform.config import get_settings
from jobplatform.db.session import create_engine, create_session_factory
from jobplatform.logging import configure_logging
from jobplatform.messaging.publisher import ConfirmedPublisher
from jobplatform.relay.relay import OutboxRelay
from jobplatform.runtime import (
    build_cache,
    install_signal_handlers,
    retry_forever,
    start_metrics_server,
)
from jobplatform.service import JobService

log = structlog.get_logger("relay")


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_json, service="relay")
    stop = asyncio.Event()
    install_signal_handlers(stop)
    start_metrics_server(settings.relay_metrics_port)

    engine = create_engine(settings, pool_size=5)
    factory = create_session_factory(engine)
    redis, cache = build_cache(settings)
    jobs = JobService(factory, cache.invalidate, background_hooks=True)
    publisher = ConfirmedPublisher(settings)
    await retry_forever("publisher_start", publisher.start, stop)
    log.info("relay_started")
    try:
        await OutboxRelay(settings, factory, publisher, jobs).run(stop)
    finally:
        await publisher.close()
        await redis.aclose()
        await engine.dispose()
        log.info("relay_stopped")


if __name__ == "__main__":
    asyncio.run(main())
