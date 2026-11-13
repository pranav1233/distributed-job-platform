"""FastAPI application factory."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from redis.asyncio import Redis

from jobplatform import __version__
from jobplatform.api.deps import AppContext, PrincipalCache
from jobplatform.api.errors import install_error_handlers
from jobplatform.api.middleware import RequestContextMiddleware
from jobplatform.api.routes import dlq, health, jobs
from jobplatform.cache.job_cache import JobCache
from jobplatform.config import Settings, get_settings
from jobplatform.db.session import create_engine, create_session_factory
from jobplatform.logging import configure_logging
from jobplatform.ratelimit.token_bucket import TokenBucketLimiter
from jobplatform.service import JobService
from jobplatform.worker.registry import build_registry

log = structlog.get_logger(__name__)


def build_context(settings: Settings) -> AppContext:
    engine = create_engine(settings)
    session_factory = create_session_factory(engine)
    redis = Redis.from_url(
        settings.redis_url,
        socket_connect_timeout=0.5,
        socket_timeout=0.5,
        health_check_interval=15,
    )
    cache = JobCache(
        redis,
        ttl_active=settings.cache_ttl_active_seconds,
        ttl_terminal=settings.cache_ttl_terminal_seconds,
        lock_seconds=settings.cache_lock_seconds,
    )
    return AppContext(
        settings=settings,
        engine=engine,
        session_factory=session_factory,
        redis=redis,
        jobs=JobService(session_factory, cache.invalidate),
        cache=cache,
        limiter=TokenBucketLimiter(redis),
        registry=build_registry(settings),
        principals=PrincipalCache(ttl=settings.api_key_cache_seconds),
    )


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configure_logging(settings.log_level, settings.log_json, service="api")
        ctx = build_context(settings)
        app.state.ctx = ctx
        log.info("api_started", version=__version__)
        try:
            yield
        finally:
            await ctx.redis.aclose()
            await ctx.engine.dispose()
            log.info("api_stopped")

    app = FastAPI(
        title="Distributed Job Processing Platform",
        version=__version__,
        description="Submit long-running jobs, track their status, inspect the dead-letter queue.",
        lifespan=lifespan,
    )
    app.add_middleware(RequestContextMiddleware)
    install_error_handlers(app)
    app.include_router(jobs.router)
    app.include_router(dlq.router)
    app.include_router(health.router)
    return app
