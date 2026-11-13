"""Helpers shared by the chaos and load test scripts (docker compose control, DB access)."""

from __future__ import annotations

import asyncio
import json
import os
import platform
import subprocess
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import asyncpg

ROOT = Path(__file__).resolve().parent.parent
API_URL = os.environ.get("API_URL", "http://localhost:8000")
PG_DSN = os.environ.get("PG_DSN", "postgresql://jobs:jobs@localhost:5432/jobs")


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def run(cmd: Sequence[str], env: dict[str, str] | None = None, check: bool = True) -> str:
    merged = {**os.environ, **(env or {})}
    proc = subprocess.run(
        list(cmd), cwd=ROOT, env=merged, capture_output=True, text=True, check=False
    )
    if check and proc.returncode != 0:
        raise RuntimeError(
            f"{' '.join(cmd)} failed ({proc.returncode}):\n{proc.stdout}\n{proc.stderr}"
        )
    return proc.stdout.strip()


async def arun(cmd: Sequence[str], env: dict[str, str] | None = None) -> str:
    return await asyncio.to_thread(run, cmd, env, False)


def compose_up(env: dict[str, str], fresh: bool = True) -> None:
    if fresh:
        run(["docker", "compose", "down", "-v", "--remove-orphans"], env)
    run(["docker", "compose", "up", "-d", "--build", "--wait"], env)


def create_api_key(owner: str, capacity: int = 1_000_000, refill: float = 1_000_000) -> str:
    out = run(
        [
            "docker", "compose", "run", "--rm", "--no-deps", "-T", "api",
            "python", "-m", "jobplatform.cli", "create-api-key",
            "--owner", owner,
            "--rate-limit-capacity", str(capacity),
            "--rate-limit-refill", str(refill),
        ]
    )  # fmt: skip
    key = out.splitlines()[-1].strip()
    if not key.startswith("jp_"):
        raise RuntimeError(f"unexpected CLI output: {out!r}")
    return key


async def pg() -> asyncpg.Connection:
    return await asyncpg.connect(PG_DSN)


def percentile(values: Sequence[float], p: float) -> float:
    """Nearest-rank percentile on a sorted copy."""
    if not values:
        return float("nan")
    ordered = sorted(values)
    rank = max(1, round(p / 100 * len(ordered)))
    return ordered[min(rank, len(ordered)) - 1]


def machine_specs() -> dict[str, Any]:
    specs: dict[str, Any] = {
        "platform": platform.platform(),
        "python": sys.version.split()[0],
        "machine": platform.machine(),
    }
    if sys.platform == "darwin":
        specs["cpu"] = run(["sysctl", "-n", "machdep.cpu.brand_string"], check=False)
        specs["logical_cpus"] = int(run(["sysctl", "-n", "hw.ncpu"], check=False) or 0)
        specs["memory_gb"] = round(
            int(run(["sysctl", "-n", "hw.memsize"], check=False) or 0) / 2**30
        )
    else:
        specs["logical_cpus"] = os.cpu_count()
    info = run(
        [
            "docker",
            "info",
            "--format",
            "{{.NCPU}}|{{.MemTotal}}|{{.ServerVersion}}|{{.OperatingSystem}}",
        ],
        check=False,
    ).split("|")
    if len(info) == 4:
        specs["docker_vm_cpus"] = int(info[0])
        specs["docker_vm_memory_gb"] = round(int(info[1]) / 2**30, 1)
        specs["docker_version"] = info[2]
        specs["docker_os"] = info[3]
    return specs


def dump_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str) + "\n")
