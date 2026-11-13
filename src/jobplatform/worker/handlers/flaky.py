from __future__ import annotations

import asyncio
from typing import Any

from jobplatform.domain.errors import NonRetryableError, RetryableError
from jobplatform.worker.handlers.base import JobContext
from jobplatform.worker.payloads import FlakyPayload


async def handle_flaky(ctx: JobContext) -> dict[str, Any]:
    """Deterministically fails the first N attempts. Used to exercise retry and DLQ paths."""
    payload = FlakyPayload.model_validate(ctx.payload)
    if payload.duration_ms:
        await asyncio.sleep(payload.duration_ms / 1000)
    if ctx.attempt <= payload.fail_times:
        message = f"simulated failure on attempt {ctx.attempt} of {payload.fail_times} planned"
        if payload.error == "fatal":
            raise NonRetryableError(message)
        raise RetryableError(message)
    return {"succeeded_on_attempt": ctx.attempt}
