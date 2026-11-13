from __future__ import annotations

import asyncio
from typing import Any

from sqlalchemy.dialects.postgresql import insert as pg_insert

from jobplatform.db.models import JobEffect
from jobplatform.worker.handlers.base import JobContext
from jobplatform.worker.payloads import SideEffectPayload


async def handle_side_effect(ctx: JobContext) -> dict[str, Any]:
    """Records exactly one effect row per job, however many times the handler runs.

    The effect is keyed by job id and written with ON CONFLICT, so a re-execution after a
    crash or a lease reclaim only increments the `executions` counter. The effect is written
    before the optional sleep so that a worker killed mid-job leaves the effect behind and
    the retry has to prove it is idempotent.
    """
    payload = SideEffectPayload.model_validate(ctx.payload)
    stmt = pg_insert(JobEffect).values(
        job_id=ctx.job_id, effect=payload.effect, executions=1, first_worker=ctx.worker_id
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[JobEffect.job_id], set_={"executions": JobEffect.executions + 1}
    )
    async with ctx.session_factory() as session, session.begin():
        await session.execute(stmt)
    if payload.duration_ms:
        await asyncio.sleep(payload.duration_ms / 1000)
    return {"effect": payload.effect}
