"""API key persistence. Only SHA-256 hashes are stored."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from jobplatform.db.models import ApiKey
from jobplatform.security import generate_api_key


@dataclass(frozen=True, slots=True)
class Principal:
    key_id: uuid.UUID
    owner: str
    rate_limit_capacity: int | None
    rate_limit_refill_per_sec: float | None


async def create_api_key(
    session: AsyncSession,
    owner: str,
    name: str = "default",
    rate_limit_capacity: int | None = None,
    rate_limit_refill_per_sec: float | None = None,
) -> tuple[str, uuid.UUID]:
    """Create a key and return (raw_key, id). The raw key is shown once and never stored."""
    raw, hashed = generate_api_key()
    key_id = uuid.uuid4()
    session.add(
        ApiKey(
            id=key_id,
            owner=owner,
            name=name,
            key_hash=hashed,
            rate_limit_capacity=rate_limit_capacity,
            rate_limit_refill_per_sec=rate_limit_refill_per_sec,
        )
    )
    await session.flush()
    return raw, key_id


async def find_principal(session: AsyncSession, key_hash: str) -> Principal | None:
    row = (
        await session.execute(
            select(
                ApiKey.id,
                ApiKey.owner,
                ApiKey.rate_limit_capacity,
                ApiKey.rate_limit_refill_per_sec,
            ).where(ApiKey.key_hash == key_hash, ApiKey.revoked_at.is_(None))
        )
    ).first()
    if row is None:
        return None
    return Principal(row.id, row.owner, row.rate_limit_capacity, row.rate_limit_refill_per_sec)
