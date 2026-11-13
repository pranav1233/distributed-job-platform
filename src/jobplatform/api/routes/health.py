from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import text

from jobplatform.api.deps import get_ctx

router = APIRouter(tags=["ops"])


@router.get("/healthz")
async def healthz() -> dict[str, str]:
    """Liveness: the process is up. Touches no dependency."""
    return {"status": "ok"}


@router.get("/readyz")
async def readyz(request: Request) -> JSONResponse:
    """Readiness: Postgres is required. Redis is optional (the API fails open without it).

    RabbitMQ is deliberately not checked: the API never talks to the broker, so a broker
    outage must not take the API out of rotation.
    """
    ctx = get_ctx(request)
    checks: dict[str, Any] = {}
    ready = True
    try:
        async with asyncio.timeout(2):
            async with ctx.session_factory() as session:
                await session.execute(text("SELECT 1"))
        checks["postgres"] = "ok"
    except Exception as exc:
        checks["postgres"] = f"error: {type(exc).__name__}"
        ready = False
    try:
        async with asyncio.timeout(1):
            await ctx.redis.ping()
        checks["redis"] = "ok"
    except Exception as exc:
        checks["redis"] = f"degraded: {type(exc).__name__}"
    return JSONResponse(
        {"status": "ready" if ready else "not_ready", "checks": checks},
        status_code=200 if ready else 503,
    )


@router.get("/metrics", include_in_schema=False)
async def metrics_endpoint() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
