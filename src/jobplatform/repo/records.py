"""Immutable snapshots of database rows."""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from jobplatform.domain.states import JobStatus


@dataclass(frozen=True, slots=True)
class JobRecord:
    id: uuid.UUID
    owner: str
    type: str
    payload: dict[str, Any]
    priority: int
    status: JobStatus
    attempts: int
    max_attempts: int
    version: int
    available_at: datetime
    lease_owner: str | None
    lease_expires_at: datetime | None
    result: dict[str, Any] | None
    last_error: str | None
    idempotency_key: str | None
    request_fingerprint: str | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    finished_at: datetime | None

    @classmethod
    def from_mapping(cls, m: Mapping[Any, Any]) -> JobRecord:
        return cls(
            id=m["id"],
            owner=m["owner"],
            type=m["type"],
            payload=m["payload"],
            priority=m["priority"],
            status=JobStatus(m["status"]),
            attempts=m["attempts"],
            max_attempts=m["max_attempts"],
            version=m["version"],
            available_at=m["available_at"],
            lease_owner=m["lease_owner"],
            lease_expires_at=m["lease_expires_at"],
            result=m["result"],
            last_error=m["last_error"],
            idempotency_key=m["idempotency_key"],
            request_fingerprint=m["request_fingerprint"],
            created_at=m["created_at"],
            updated_at=m["updated_at"],
            started_at=m["started_at"],
            finished_at=m["finished_at"],
        )

    def public_dict(self) -> dict[str, Any]:
        """JSON-safe representation shared by the API and the cache."""
        return {
            "id": str(self.id),
            "type": self.type,
            "status": self.status.value,
            "priority": self.priority,
            "payload": self.payload,
            "attempts": self.attempts,
            "max_attempts": self.max_attempts,
            "result": self.result,
            "last_error": self.last_error,
            "available_at": self.available_at.isoformat(),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
        }


@dataclass(frozen=True, slots=True)
class OutboxRecord:
    id: int
    job_id: uuid.UUID
    kind: str
    payload: dict[str, Any]
    priority: int
    available_at: datetime
    publish_attempts: int
