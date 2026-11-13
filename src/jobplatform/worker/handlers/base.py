"""Handler contract."""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


@dataclass(frozen=True, slots=True)
class JobContext:
    job_id: uuid.UUID
    owner: str
    type: str
    payload: dict[str, Any]
    attempt: int
    max_attempts: int
    worker_id: str
    session_factory: async_sessionmaker[AsyncSession]


AsyncHandler = Callable[[JobContext], Awaitable[dict[str, Any]]]
