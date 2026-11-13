"""Shared bootstrap for the long-running services (worker, relay, reaper)."""

from __future__ import annotations

import asyncio
import contextlib
import signal
from collections.abc import Awaitable, Callable

import aio_pika
import structlog
from aio_pika.abc import AbstractRobustConnection
from prometheus_client import start_http_server
from redis.asyncio import Redis

from jobplatform.cache.job_cache import JobCache
from jobplatform.config import Settings

log = structlog.get_logger(__name__)


def install_signal_handlers(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)


def start_metrics_server(port: int) -> None:
    start_http_server(port)
    log.info("metrics_server_started", port=port)


def build_cache(settings: Settings) -> tuple[Redis, JobCache]:
    redis = Redis.from_url(
        settings.redis_url, socket_connect_timeout=0.5, socket_timeout=0.5, health_check_interval=15
    )
    cache = JobCache(
        redis,
        ttl_active=settings.cache_ttl_active_seconds,
        ttl_terminal=settings.cache_ttl_terminal_seconds,
    )
    return redis, cache


async def sleep_or_stop(stop: asyncio.Event, seconds: float) -> None:
    """Sleep, but wake immediately if the stop event is set."""
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=seconds)


async def retry_forever[T](
    what: str, fn: Callable[[], Awaitable[T]], stop: asyncio.Event, max_delay: float = 5.0
) -> T | None:
    """Retry a startup step (for example connecting to the broker) until it works."""
    delay = 0.5
    while not stop.is_set():
        try:
            return await fn()
        except Exception as exc:
            log.warning("startup_retry", step=what, error=repr(exc), retry_in=delay)
            await sleep_or_stop(stop, delay)
            delay = min(delay * 2, max_delay)
    return None


async def connect_broker(url: str, stop: asyncio.Event) -> AbstractRobustConnection | None:
    async def _connect() -> AbstractRobustConnection:
        return await aio_pika.connect_robust(url)

    return await retry_forever("rabbitmq_connect", _connect, stop)
