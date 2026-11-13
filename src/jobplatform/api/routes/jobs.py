from __future__ import annotations

import base64
import hashlib
import json
import uuid
from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, Query, Request, Response
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from jobplatform.api.deps import AppContext, authenticate, get_ctx
from jobplatform.api.errors import ApiError
from jobplatform.api.schemas import EventList, JobCreate, JobOut, JobPage
from jobplatform.domain.states import JobStatus
from jobplatform.repo import jobs as repo
from jobplatform.repo.api_keys import Principal
from jobplatform.repo.records import JobRecord

router = APIRouter(prefix="/v1", tags=["jobs"])

CtxDep = Annotated[AppContext, Depends(get_ctx)]
PrincipalDep = Annotated[Principal, Depends(authenticate)]

_IDEMPOTENCY_MAX = 255


def encode_cursor(record: JobRecord) -> str:
    raw = f"{record.created_at.isoformat()}|{record.id}"
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


def decode_cursor(cursor: str) -> tuple[datetime, uuid.UUID]:
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        stamp, ident = base64.urlsafe_b64decode(padded.encode()).decode().split("|")
        return datetime.fromisoformat(stamp), uuid.UUID(ident)
    except (ValueError, UnicodeDecodeError) as exc:
        raise ApiError(422, "invalid_cursor", "cursor is not valid") from exc


def fingerprint(
    type_: str, payload: dict[str, Any], priority: int, max_attempts: int, delay: int
) -> str:
    canonical = json.dumps(
        {"t": type_, "p": payload, "pr": priority, "m": max_attempts, "d": delay},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def _public(data: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in data.items() if k not in ("owner", "version")}


@router.post(
    "/jobs",
    response_model=JobOut,
    status_code=201,
    responses={200: {"description": "Idempotent replay of an existing job"}},
)
async def submit_job(
    body: JobCreate,
    request: Request,
    response: Response,
    ctx: CtxDep,
    principal: PrincipalDep,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> dict[str, Any]:
    spec = ctx.registry.get(body.type)
    if spec is None:
        raise ApiError(
            422,
            "unknown_job_type",
            f"unknown job type {body.type!r}",
            details={"supported": sorted(ctx.registry)},
        )
    try:
        payload = spec.validate_payload(body.payload)
    except ValidationError as exc:
        raise ApiError(
            422,
            "invalid_payload",
            f"payload does not match the schema for {body.type!r}",
            details=json.loads(exc.json(include_url=False, include_context=False)),
        ) from exc
    if len(json.dumps(payload)) > ctx.settings.max_payload_bytes:
        raise ApiError(413, "payload_too_large", "payload exceeds the size limit")
    if idempotency_key is not None and not (0 < len(idempotency_key) <= _IDEMPOTENCY_MAX):
        raise ApiError(422, "invalid_idempotency_key", "Idempotency-Key must be 1-255 characters")

    max_attempts = body.max_attempts or spec.max_attempts
    try:
        record, created = await ctx.jobs.create(
            owner=principal.owner,
            type_=body.type,
            payload=payload,
            priority=body.priority,
            max_attempts=max_attempts,
            delay_seconds=body.delay_seconds,
            idempotency_key=idempotency_key,
            fingerprint=fingerprint(
                body.type, payload, body.priority, max_attempts, body.delay_seconds
            )
            if idempotency_key
            else None,
        )
    except repo.IdempotencyConflictError as exc:
        raise ApiError(
            409,
            "idempotency_key_reused",
            "this Idempotency-Key was already used with a different request body",
        ) from exc
    if not created:
        response.status_code = 200
        response.headers["Idempotent-Replayed"] = "true"
    return record.public_dict()


@router.get(
    "/jobs/{job_id}",
    response_model=JobOut,
    responses={404: {"description": "Not found"}},
)
async def get_job(job_id: uuid.UUID, ctx: CtxDep, principal: PrincipalDep) -> JSONResponse:
    async def load() -> JobRecord | None:
        return await ctx.jobs.get(job_id)

    data = await ctx.cache.get(job_id, load)
    if data is None or data["owner"] != principal.owner:
        raise ApiError(404, "not_found", "job not found")
    return JSONResponse(_public(data))


@router.get("/jobs", response_model=JobPage)
async def list_jobs(
    ctx: CtxDep,
    principal: PrincipalDep,
    status: JobStatus | None = None,
    type: Annotated[str | None, Query(max_length=100)] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    cursor: str | None = None,
) -> dict[str, Any]:
    decoded = decode_cursor(cursor) if cursor else None
    async with ctx.session_factory() as session:
        rows = await repo.list_jobs(
            session,
            owner=principal.owner,
            status=status,
            type_=type,
            limit=limit + 1,
            cursor=decoded,
        )
    page = rows[:limit]
    return {
        "items": [r.public_dict() for r in page],
        "next_cursor": encode_cursor(page[-1]) if len(rows) > limit else None,
    }


@router.get("/jobs/{job_id}/events", response_model=EventList)
async def job_events(job_id: uuid.UUID, ctx: CtxDep, principal: PrincipalDep) -> dict[str, Any]:
    async with ctx.session_factory() as session:
        events = await repo.list_events(session, job_id, principal.owner)
    if events is None:
        raise ApiError(404, "not_found", "job not found")
    return {"items": events}


@router.post(
    "/jobs/{job_id}/cancel",
    response_model=JobOut,
    responses={404: {"description": "Not found"}, 409: {"description": "Job already finished"}},
)
async def cancel_job(job_id: uuid.UUID, ctx: CtxDep, principal: PrincipalDep) -> dict[str, Any]:
    updated, current = await ctx.jobs.cancel(job_id, principal.owner)
    if current is None:
        raise ApiError(404, "not_found", "job not found")
    if updated is None and current.status is not JobStatus.CANCELLED:
        raise ApiError(
            409,
            "invalid_state",
            f"job is {current.status.value} and can no longer be cancelled",
        )
    return current.public_dict()
