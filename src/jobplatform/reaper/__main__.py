from __future__ import annotations

import asyncio

import structlog

from jobplatform.config import get_settings
from jobplatform.db.session import create_engine, create_session_factory
from jobplatform.logging import configure_logging
from jobplatform.reaper.reaper import Reaper
from jobplatform.runtime import build_cache, install_signal_handlers, start_metrics_server
from jobplatform.service import JobService

log = structlog.get_logger("reaper")


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_json, service="reaper")
    stop = asyncio.Event()
    install_signal_handlers(stop)
    start_metrics_server(settings.reaper_metrics_port)

    engine = create_engine(settings, pool_size=3)
    factory = create_session_factory(engine)
    redis, cache = build_cache(settings)
    jobs = JobService(factory, cache.invalidate, background_hooks=True)
    log.info("reaper_started")
    try:
        await Reaper(settings, jobs, factory).run(stop)
    finally:
        await redis.aclose()
        await engine.dispose()
        log.info("reaper_stopped")


if __name__ == "__main__":
    asyncio.run(main())
