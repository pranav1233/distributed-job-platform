from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class JobCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str = Field(min_length=1, max_length=100, examples=["io_task"])
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=5, ge=0, le=9, description="0 lowest, 9 highest")
    max_attempts: int | None = Field(default=None, ge=1, le=20)
    delay_seconds: int = Field(default=0, ge=0, le=7 * 86400)


class JobOut(BaseModel):
    id: uuid.UUID
    type: str
    status: str
    priority: int
    payload: dict[str, Any]
    attempts: int
    max_attempts: int
    result: dict[str, Any] | None
    last_error: str | None
    available_at: datetime
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


class JobPage(BaseModel):
    items: list[JobOut]
    next_cursor: str | None


class EventOut(BaseModel):
    id: int
    from_status: str | None
    to_status: str
    attempt: int
    actor: str | None
    reason: str | None
    detail: dict[str, Any] | None
    created_at: datetime


class EventList(BaseModel):
    items: list[EventOut]


class ErrorBody(BaseModel):
    code: str
    message: str
    request_id: str | None = None
    details: Any = None


class ErrorEnvelope(BaseModel):
    error: ErrorBody
