"""Pure ASGI middleware: request id, log context, rate-limit headers, HTTP metrics."""

from __future__ import annotations

import re
import time
import uuid
from typing import Any

import structlog
from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from jobplatform import metrics

_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._\-]{1,128}$")
log = structlog.get_logger("http")


class RequestContextMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        raw = dict(scope["headers"]).get(b"x-request-id", b"").decode("latin-1")
        request_id = raw if _REQUEST_ID_RE.match(raw) else uuid.uuid4().hex
        scope.setdefault("state", {})["request_id"] = request_id
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(request_id=request_id)

        started = time.perf_counter()
        status_holder = {"status": 500}

        async def send_wrapper(message: Message) -> None:
            if message["type"] == "http.response.start":
                status_holder["status"] = message["status"]
                headers = MutableHeaders(scope=message)
                headers["X-Request-ID"] = request_id
                rate: Any = scope.get("state", {}).get("rate_limit")
                if rate is not None:
                    headers["X-RateLimit-Limit"] = str(rate.limit)
                    headers["X-RateLimit-Remaining"] = str(rate.remaining)
                    headers["X-RateLimit-Reset"] = str(rate.reset_seconds)
                    if not rate.allowed:
                        headers["Retry-After"] = str(rate.retry_after_seconds)
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            elapsed = time.perf_counter() - started
            route = scope.get("route")
            route_path = getattr(route, "path", "unmatched")
            method = scope["method"]
            metrics.HTTP_REQUESTS.labels(method, route_path, str(status_holder["status"])).inc()
            metrics.HTTP_LATENCY.labels(method, route_path).observe(elapsed)
            if route_path not in ("/healthz", "/readyz", "/metrics"):
                log.info(
                    "request",
                    method=method,
                    path=scope["path"],
                    status=status_holder["status"],
                    duration_ms=round(elapsed * 1000, 2),
                )
