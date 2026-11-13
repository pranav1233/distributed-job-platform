import asyncio
import os
import sys

import httpx

from tests.integration.conftest import Infra


async def test_cli_creates_a_working_api_key(api: httpx.AsyncClient, migrated: Infra) -> None:
    env = {**os.environ, "DATABASE_URL": migrated.database_url}
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "jobplatform.cli",
        "create-api-key",
        "--owner",
        "cli-user",
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=60)
    assert proc.returncode == 0
    raw_key = stdout.decode().strip()
    assert raw_key.startswith("jp_")
    resp = await api.get("/v1/jobs", headers={"Authorization": f"Bearer {raw_key}"})
    assert resp.status_code == 200
    # only the hash is stored
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(migrated.database_url)
    async with engine.connect() as conn:
        rows = (await conn.execute(text("SELECT key_hash FROM api_keys"))).scalars().all()
    await engine.dispose()
    assert raw_key not in rows and len(rows) == 1
