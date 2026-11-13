from __future__ import annotations

import uuid
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query

from jobplatform.api.deps import AppContext, authenticate, get_ctx
from jobplatform.api.errors import ApiError
from jobplatform.api.routes.jobs import decode_cursor, encode_cursor
from jobplatform.api.schemas import JobOut, JobPage
from jobplatform.domain.states import JobStatus
from jobplatform.repo import jobs as repo
from jobplatform.repo.api_keys import Principal

router = APIRouter(prefix="/v1/dlq", tags=["dlq"])

CtxDep = Annotated[AppContext, Depends(get_ctx)]
PrincipalDep = Annotated[Principal, Depends(authenticate)]


@router.get("", response_model=JobPage)
async def list_dead(
    ctx: CtxDep,
    principal: PrincipalDep,
    type: Annotated[str | None, Query(max_length=100)] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    cursor: str | None = None,
) -> dict[str, Any]:
    """Dead jobs for the caller: retries exhausted, non-retryable failure, or lease exhaustion."""
    decoded = decode_cursor(cursor) if cursor else None
    async with ctx.session_factory() as session:
        rows = await repo.list_jobs(
            session,
            owner=principal.owner,
            status=JobStatus.DEAD,
            type_=type,
            limit=limit + 1,
            cursor=decoded,
        )
    page = rows[:limit]
    return {
        "items": [r.public_dict() for r in page],
        "next_cursor": encode_cursor(page[-1]) if len(rows) > limit else None,
    }


@router.post(
    "/{job_id}/requeue",
    response_model=JobOut,
    responses={404: {"description": "Not found"}, 409: {"description": "Job is not dead"}},
)
async def requeue(job_id: uuid.UUID, ctx: CtxDep, principal: PrincipalDep) -> dict[str, Any]:
    """Move a dead job back to queued with a fresh attempt budget."""
    updated, current = await ctx.jobs.requeue_dead(job_id, principal.owner)
    if current is None:
        raise ApiError(404, "not_found", "job not found")
    if updated is None:
        raise ApiError(409, "invalid_state", f"job is {current.status.value}, not dead")
    return updated.public_dict()
