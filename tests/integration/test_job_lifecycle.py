"""Submit -> relay -> RabbitMQ -> worker -> succeeded, driven through the real API."""

from __future__ import annotations

import uuid

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from jobplatform.domain.states import JobStatus
from jobplatform.service import JobService
from tests.integration.conftest import (
    Stack,
    fetch_events,
    make_key,
    wait_for,
    wait_for_status,
)


async def test_submit_process_succeed(
    api: httpx.AsyncClient,
    api_key: dict[str, str],
    stack: Stack,
    service: JobService,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await stack.start_relay()
    await stack.start_worker()

    resp = await api.post(
        "/v1/jobs", json={"type": "io_task", "payload": {"duration_ms": 50}}, headers=api_key
    )
    assert resp.status_code == 201
    created = resp.json()
    assert created["status"] == "queued"
    assert resp.headers["X-Request-ID"]

    record = await wait_for_status(service, created["id"], JobStatus.SUCCEEDED)
    assert record.attempts == 1
    assert record.result is not None and record.result["slept_ms"] == 50
    assert record.lease_owner is None and record.finished_at is not None

    got = (await api.get(f"/v1/jobs/{created['id']}", headers=api_key)).json()
    assert got["status"] == "succeeded"
    assert got["result"]["slept_ms"] == 50

    events = await fetch_events(session_factory, created["id"])
    assert [(e["from_status"], e["to_status"]) for e in events] == [
        (None, "queued"),
        ("queued", "running"),
        ("running", "succeeded"),
    ]

    events_api = (await api.get(f"/v1/jobs/{created['id']}/events", headers=api_key)).json()
    assert [e["to_status"] for e in events_api["items"]] == ["queued", "running", "succeeded"]


async def test_outbox_row_marked_published(
    api: httpx.AsyncClient,
    api_key: dict[str, str],
    stack: Stack,
    service: JobService,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    resp = await api.post("/v1/jobs", json={"type": "io_task"}, headers=api_key)
    job_id = resp.json()["id"]
    async with session_factory() as session:
        pending = (
            await session.execute(text("SELECT count(*) FROM outbox WHERE published_at IS NULL"))
        ).scalar()
    assert pending == 1  # the API never publishes; the row waits for the relay

    await stack.start_relay()

    async def published() -> bool:
        async with session_factory() as session:
            n = (
                await session.execute(
                    text("SELECT count(*) FROM outbox WHERE published_at IS NULL")
                )
            ).scalar()
        return n == 0

    await wait_for(published, what="outbox row to be published")
    assert (await service.get(uuid.UUID(job_id))) is not None


async def test_list_filter_and_cursor_pagination(
    api: httpx.AsyncClient, api_key: dict[str, str]
) -> None:
    ids = []
    for i in range(7):
        r = await api.post(
            "/v1/jobs",
            json={"type": "io_task" if i % 2 == 0 else "flaky", "payload": {}},
            headers=api_key,
        )
        ids.append(r.json()["id"])

    seen: list[str] = []
    cursor = None
    pages = 0
    while True:
        params: dict[str, str | int] = {"limit": 3}
        if cursor:
            params["cursor"] = cursor
        body = (await api.get("/v1/jobs", params=params, headers=api_key)).json()
        seen += [j["id"] for j in body["items"]]
        pages += 1
        cursor = body["next_cursor"]
        if cursor is None:
            break
    assert pages == 3
    assert seen == list(reversed(ids))  # newest first, no gaps, no duplicates

    only_flaky = (await api.get("/v1/jobs", params={"type": "flaky"}, headers=api_key)).json()
    assert len(only_flaky["items"]) == 3
    queued = (await api.get("/v1/jobs", params={"status": "queued"}, headers=api_key)).json()
    assert len(queued["items"]) == 7
    done = (await api.get("/v1/jobs", params={"status": "succeeded"}, headers=api_key)).json()
    assert done["items"] == []

    bad = await api.get("/v1/jobs", params={"cursor": "not-a-cursor"}, headers=api_key)
    assert bad.status_code == 422 and bad.json()["error"]["code"] == "invalid_cursor"


async def test_error_format_auth_and_ownership(
    api: httpx.AsyncClient,
    api_key: dict[str, str],
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    no_auth = await api.post("/v1/jobs", json={"type": "io_task"})
    assert no_auth.status_code == 401
    body = no_auth.json()["error"]
    assert body["code"] == "unauthorized" and body["request_id"]

    bad_key = await api.get("/v1/jobs", headers={"Authorization": "Bearer jp_nope"})
    assert bad_key.status_code == 401

    unknown = await api.post("/v1/jobs", json={"type": "nope"}, headers=api_key)
    assert unknown.status_code == 422 and unknown.json()["error"]["code"] == "unknown_job_type"

    bad_payload = await api.post(
        "/v1/jobs", json={"type": "io_task", "payload": {"duration_ms": -5}}, headers=api_key
    )
    assert bad_payload.status_code == 422
    assert bad_payload.json()["error"]["code"] == "invalid_payload"

    bad_body = await api.post("/v1/jobs", json={"priority": 99}, headers=api_key)
    assert bad_body.status_code == 422
    assert bad_body.json()["error"]["code"] == "validation_error"

    custom_id = await api.get("/healthz", headers={"X-Request-ID": "abc-123"})
    assert custom_id.headers["X-Request-ID"] == "abc-123"

    job = (await api.post("/v1/jobs", json={"type": "io_task"}, headers=api_key)).json()
    bob = await make_key(session_factory, "bob")
    assert (await api.get(f"/v1/jobs/{job['id']}", headers=bob)).status_code == 404
    assert (await api.post(f"/v1/jobs/{job['id']}/cancel", headers=bob)).status_code == 404
    assert (await api.get(f"/v1/jobs/{uuid.uuid4()}", headers=api_key)).status_code == 404
    assert (await api.get("/v1/jobs/not-a-uuid", headers=api_key)).status_code == 422

    jobs_of_bob = (await api.get("/v1/jobs", headers=bob)).json()
    assert jobs_of_bob["items"] == []


async def test_health_and_metrics(api: httpx.AsyncClient) -> None:
    assert (await api.get("/healthz")).json() == {"status": "ok"}
    ready = await api.get("/readyz")
    assert ready.status_code == 200
    assert ready.json()["checks"] == {"postgres": "ok", "redis": "ok"}
    metrics_text = (await api.get("/metrics")).text
    assert "http_requests_total" in metrics_text
