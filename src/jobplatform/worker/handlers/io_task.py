from __future__ import annotations

import asyncio
from typing import Any

from jobplatform.worker.handlers.base import JobContext
from jobplatform.worker.payloads import IoTaskPayload


async def handle_io_task(ctx: JobContext) -> dict[str, Any]:
    """Simulated long-running I/O job. Cancellation arrives as asyncio cancellation."""
    payload = IoTaskPayload.model_validate(ctx.payload)
    await asyncio.sleep(payload.duration_ms / 1000)
    return {"slept_ms": payload.duration_ms, "worker": ctx.worker_id}
