import uuid
from datetime import UTC, datetime

import pytest

from jobplatform.api.errors import ApiError
from jobplatform.api.routes.jobs import decode_cursor, encode_cursor, fingerprint
from jobplatform.domain.states import JobStatus
from jobplatform.repo.records import JobRecord
from jobplatform.security import KEY_PREFIX, generate_api_key, hash_api_key


def _record(created: datetime, ident: uuid.UUID) -> JobRecord:
    return JobRecord(
        id=ident, owner="o", type="t", payload={}, priority=5, status=JobStatus.QUEUED,
        attempts=0, max_attempts=3, version=1, available_at=created, lease_owner=None,
        lease_expires_at=None, result=None, last_error=None, idempotency_key=None,
        request_fingerprint=None, created_at=created, updated_at=created,
        started_at=None, finished_at=None,
    )  # fmt: skip


def test_cursor_roundtrip() -> None:
    stamp = datetime(2026, 1, 2, 3, 4, 5, 678901, tzinfo=UTC)
    ident = uuid.uuid4()
    assert decode_cursor(encode_cursor(_record(stamp, ident))) == (stamp, ident)


@pytest.mark.parametrize("bad", ["", "!!!", "bm9wZQ", "YWJj"])
def test_bad_cursors_are_rejected(bad: str) -> None:
    with pytest.raises(ApiError) as err:
        decode_cursor(bad)
    assert err.value.status_code == 422


def test_fingerprint_is_stable_and_order_independent() -> None:
    a = fingerprint("t", {"x": 1, "y": 2}, 5, 3, 0)
    b = fingerprint("t", {"y": 2, "x": 1}, 5, 3, 0)
    assert a == b


@pytest.mark.parametrize(
    "other",
    [
        ("u", {"x": 1}, 5, 3, 0),
        ("t", {"x": 2}, 5, 3, 0),
        ("t", {"x": 1}, 6, 3, 0),
        ("t", {"x": 1}, 5, 4, 0),
        ("t", {"x": 1}, 5, 3, 10),
    ],
)
def test_fingerprint_changes_with_any_field(other: tuple) -> None:  # type: ignore[type-arg]
    assert fingerprint("t", {"x": 1}, 5, 3, 0) != fingerprint(*other)


def test_api_keys_are_hashed_and_unique() -> None:
    raw1, hash1 = generate_api_key()
    raw2, hash2 = generate_api_key()
    assert raw1.startswith(KEY_PREFIX) and raw1 != raw2
    assert hash1 == hash_api_key(raw1) and len(hash1) == 64
    assert raw1 not in hash1 and hash1 != hash2
