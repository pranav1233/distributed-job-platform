#!/usr/bin/env python3
"""Chaos test for the job platform.

Brings up the docker compose stack with short leases, submits thousands of jobs while it
randomly SIGKILLs workers, then SIGKILLs the API, restarts Redis, SIGKILLs the relay,
restarts RabbitMQ and SIGKILLs the reaper. When everything has settled it asserts, directly
against PostgreSQL:

  1. zero lost jobs (every job the API acknowledged exists)
  2. every job reached a terminal state, and none died
  3. exactly one side effect row per side_effect job
  4. every job's audit trail is a valid, unbroken chain ending in one `succeeded` event
  5. the outbox has nothing left pending

Usage: uv run python scripts/chaos_test.py [--jobs 3000] [--workers 4] [--no-start]
"""

from __future__ import annotations

import argparse
import asyncio
import random
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

from _common import (
    API_URL,
    ROOT,
    arun,
    compose_up,
    create_api_key,
    dump_json,
    log,
    machine_specs,
    pg,
    run,
)

STACK_ENV = {
    "LEASE_SECONDS": "6",
    "HEARTBEAT_INTERVAL_SECONDS": "2",
    "REAPER_INTERVAL_SECONDS": "1",
    "RETRY_BASE_DELAY_SECONDS": "1",
    "RETRY_MAX_DELAY_SECONDS": "10",
    "WORKER_CONCURRENCY": "16",
    "WORKER_PREFETCH": "24",
}
TERMINAL = ("succeeded", "dead", "cancelled")


@dataclass
class Chaos:
    kills: dict[str, int] = field(default_factory=dict)
    restarts: dict[str, int] = field(default_factory=dict)
    timeline: list[str] = field(default_factory=list)

    def note(self, what: str, started: float) -> None:
        line = f"t+{time.monotonic() - started:6.1f}s  {what}"
        self.timeline.append(line)
        log(what)


async def container_ids(service: str) -> list[str]:
    out = await arun(["docker", "compose", "ps", "-q", service])
    return [line for line in out.splitlines() if line]


async def sigkill(service: str, chaos: Chaos, started: float, downtime: float) -> None:
    ids = await container_ids(service)
    if not ids:
        return
    victim = random.choice(ids)
    await arun(["docker", "kill", "-s", "KILL", victim])
    chaos.kills[service] = chaos.kills.get(service, 0) + 1
    chaos.note(f"SIGKILL {service} ({victim[:12]})", started)
    await asyncio.sleep(downtime)
    await arun(["docker", "start", victim])


async def restart(service: str, chaos: Chaos, started: float) -> None:
    chaos.note(f"restart {service}", started)
    await arun(["docker", "compose", "restart", service])
    chaos.restarts[service] = chaos.restarts.get(service, 0) + 1
    chaos.note(f"{service} is back", started)


def make_jobs(n: int, rng: random.Random) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    for i in range(n):
        spec: dict[str, Any]
        roll = rng.random()
        if roll < 0.8:
            spec = {
                "type": "side_effect",
                "payload": {"effect": f"chaos-{i}", "duration_ms": rng.randint(300, 2500)},
            }
        elif roll < 0.9:
            spec = {
                "type": "flaky",
                "payload": {"fail_times": rng.randint(1, 2), "duration_ms": 200},
            }
        else:
            spec = {"type": "io_task", "payload": {"duration_ms": rng.randint(300, 2000)}}
        spec["max_attempts"] = (
            20  # kills consume attempts, so use the largest budget the API allows
        )
        spec["priority"] = rng.randint(0, 9)
        jobs.append(spec)
    return jobs


@dataclass
class SubmitStats:
    ids: dict[int, str] = field(default_factory=dict)
    retries: int = 0
    replays: int = 0
    failures: int = 0


async def submit_one(
    client: httpx.AsyncClient,
    key: str,
    run_id: str,
    i: int,
    spec: dict[str, Any],
    stats: SubmitStats,
) -> None:
    headers = {"Authorization": f"Bearer {key}", "Idempotency-Key": f"{run_id}-{i}"}
    for attempt in range(60):
        try:
            resp = await client.post("/v1/jobs", json=spec, headers=headers)
            if resp.status_code in (200, 201):
                if resp.headers.get("Idempotent-Replayed") == "true":
                    stats.replays += 1
                stats.ids[i] = resp.json()["id"]
                return
            if resp.status_code < 500 and resp.status_code != 429:
                stats.failures += 1
                log(f"submit {i} rejected: {resp.status_code} {resp.text[:200]}")
                return
        except httpx.HTTPError:
            pass
        stats.retries += 1
        await asyncio.sleep(min(0.25 * (attempt + 1), 2.0))
    stats.failures += 1


async def submit_all(
    key: str, run_id: str, specs: list[dict[str, Any]], seconds: float, stats: SubmitStats
) -> None:
    limits = httpx.Limits(max_connections=64)
    async with httpx.AsyncClient(base_url=API_URL, timeout=10, limits=limits) as client:
        sem = asyncio.Semaphore(48)
        tasks: list[asyncio.Task[None]] = []
        t0 = time.monotonic()

        async def bounded(i: int, spec: dict[str, Any]) -> None:
            async with sem:
                await submit_one(client, key, run_id, i, spec, stats)

        for i, spec in enumerate(specs):
            delay = t0 + i * seconds / len(specs) - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            tasks.append(asyncio.create_task(bounded(i, spec)))
        await asyncio.gather(*tasks)


async def chaos_driver(
    chaos: Chaos, started: float, submit_seconds: float, done: asyncio.Event, rng: random.Random
) -> None:
    """Scheduled infrastructure faults plus a steady random worker kill rate."""
    schedule = [
        (0.30, "api"),
        (0.45, "redis"),
        (0.60, "relay"),
        (0.75, "rabbitmq"),
        (0.90, "reaper"),
    ]

    async def faults() -> None:
        for frac, service in schedule:
            await asyncio.sleep(max(0.0, started + frac * submit_seconds - time.monotonic()))
            if done.is_set():
                return
            if service in ("redis", "rabbitmq"):
                await restart(service, chaos, started)
            else:
                await sigkill(service, chaos, started, downtime=3.0)

    async def worker_killer() -> None:
        await asyncio.sleep(4)
        while not done.is_set():
            await sigkill("worker", chaos, started, downtime=rng.uniform(1.0, 3.0))
            try:
                await asyncio.wait_for(done.wait(), timeout=rng.uniform(3.0, 6.0))
            except TimeoutError:
                continue

    await asyncio.gather(faults(), worker_killer())


async def wait_until_settled(
    expected: int, timeout: float, done: asyncio.Event, run_owner: str
) -> dict[str, int]:
    conn = await pg()
    try:
        deadline = time.monotonic() + timeout
        last_log = 0.0
        counts: dict[str, int] = {}
        while time.monotonic() < deadline:
            try:
                rows = await conn.fetch(
                    "SELECT status, count(*) AS n FROM jobs WHERE owner = $1 GROUP BY status",
                    run_owner,
                )
            except Exception as exc:
                log(f"db poll failed: {exc!r}")
                await asyncio.sleep(2)
                continue
            counts = {r["status"]: r["n"] for r in rows}
            total = sum(counts.values())
            terminal = sum(counts.get(s, 0) for s in TERMINAL)
            if time.monotonic() - last_log > 10:
                log(f"progress: {terminal}/{expected} terminal, db has {total} jobs, {counts}")
                last_log = time.monotonic()
            if total >= expected and terminal >= expected:
                done.set()
                return counts
            await asyncio.sleep(1.0)
        return counts
    finally:
        await conn.close()


@dataclass
class Check:
    name: str
    passed: bool
    detail: str


async def verify(
    owner: str,
    expected_ids: set[str],
    n_side_effect: int,
    stats: SubmitStats,
    specs: list[dict[str, Any]],
) -> tuple[list[Check], dict[str, Any]]:
    conn = await pg()
    checks: list[Check] = []
    info: dict[str, Any] = {}
    try:
        rows = await conn.fetch(
            "SELECT id::text, status, type, attempts FROM jobs WHERE owner = $1", owner
        )
        db_ids = {r["id"] for r in rows}
        missing = expected_ids - db_ids
        extra = db_ids - expected_ids
        checks.append(
            Check(
                "zero lost jobs",
                not missing and not extra and len(db_ids) == len(expected_ids),
                f"{len(expected_ids)} acknowledged by the API, {len(db_ids)} in Postgres, "
                f"{len(missing)} missing, {len(extra)} unexpected",
            )
        )

        by_status: dict[str, int] = {}
        for r in rows:
            by_status[r["status"]] = by_status.get(r["status"], 0) + 1
        info["by_status"] = by_status
        non_terminal = sum(n for s, n in by_status.items() if s not in TERMINAL)
        checks.append(
            Check(
                "every job reached a terminal state",
                non_terminal == 0,
                f"{by_status}",
            )
        )
        checks.append(
            Check(
                "no job exhausted its retries",
                by_status.get("dead", 0) == 0 and by_status.get("succeeded", 0) == len(db_ids),
                f"succeeded={by_status.get('succeeded', 0)} dead={by_status.get('dead', 0)}",
            )
        )

        effects = await conn.fetchrow(
            "SELECT count(*) AS n, count(DISTINCT job_id) AS d, "
            "coalesce(max(executions), 0) AS max_exec, "
            "count(*) FILTER (WHERE executions > 1) AS reexec, "
            "coalesce(sum(executions), 0) AS total_exec FROM job_effects"
        )
        assert effects is not None
        orphans = await conn.fetchval(
            "SELECT count(*) FROM job_effects e LEFT JOIN jobs j ON j.id = e.job_id "
            "WHERE j.id IS NULL"
        )
        no_effect = await conn.fetchval(
            "SELECT count(*) FROM jobs j LEFT JOIN job_effects e ON e.job_id = j.id "
            "WHERE j.owner = $1 AND j.type = 'side_effect' AND e.job_id IS NULL",
            owner,
        )
        checks.append(
            Check(
                "exactly one side effect per side_effect job",
                effects["n"] == effects["d"] == n_side_effect and orphans == 0 and no_effect == 0,
                f"{n_side_effect} side_effect jobs, {effects['n']} effect rows "
                f"({effects['d']} distinct), {no_effect} jobs without an effect, {orphans} orphans",
            )
        )
        info["effects"] = {
            "rows": effects["n"],
            "jobs_executed_more_than_once": effects["reexec"],
            "max_executions_of_one_job": effects["max_exec"],
            "total_handler_executions": effects["total_exec"],
        }

        broken = await conn.fetchval(
            """
            WITH ev AS (
              SELECT e.job_id, e.from_status, e.to_status,
                     lag(e.to_status) OVER (PARTITION BY e.job_id ORDER BY e.id) AS prev
              FROM job_events e JOIN jobs j ON j.id = e.job_id WHERE j.owner = $1)
            SELECT count(*) FROM ev
            WHERE (prev IS NULL AND from_status IS NOT NULL)
               OR (prev IS NOT NULL AND from_status IS DISTINCT FROM prev)
            """,
            owner,
        )
        bad_success = await conn.fetchval(
            """
            SELECT count(*) FROM jobs j
            WHERE j.owner = $1 AND j.status = 'succeeded'
              AND (SELECT count(*) FROM job_events e
                   WHERE e.job_id = j.id AND e.to_status = 'succeeded') <> 1
            """,
            owner,
        )
        checks.append(
            Check(
                "audit trails are unbroken, one `succeeded` event per job",
                broken == 0 and bad_success == 0,
                f"{broken} broken transitions, {bad_success} jobs without exactly one succeeded event",
            )
        )

        pending = await conn.fetchval("SELECT count(*) FROM outbox WHERE published_at IS NULL")
        checks.append(Check("outbox fully drained", pending == 0, f"{pending} rows pending"))

        recov = await conn.fetchrow(
            "SELECT count(*) FILTER (WHERE reason = 'lease_expired') AS reclaims, "
            "count(*) FILTER (WHERE reason = 'lease_released') AS releases, "
            "count(*) FILTER (WHERE reason = 'retry_scheduled') AS retries "
            "FROM job_events e JOIN jobs j ON j.id = e.job_id WHERE j.owner = $1",
            owner,
        )
        assert recov is not None
        attempts = sum(r["attempts"] for r in rows)
        info["recovery"] = {
            "lease_reclaims_by_reaper": recov["reclaims"],
            "leases_released_on_shutdown": recov["releases"],
            "handler_retries_scheduled": recov["retries"],
            "total_attempts": attempts,
            "jobs_needing_more_than_one_attempt": sum(1 for r in rows if r["attempts"] > 1),
        }
        info["submit"] = {
            "client_retries": stats.retries,
            "idempotent_replays": stats.replays,
            "submit_failures": stats.failures,
        }
        checks.append(
            Check(
                "every submit eventually acknowledged",
                stats.failures == 0 and len(stats.ids) == len(specs),
                f"{len(stats.ids)}/{len(specs)} acknowledged, {stats.retries} client retries, "
                f"{stats.replays} idempotent replays",
            )
        )
    finally:
        await conn.close()
    return checks, info


async def main_async(args: argparse.Namespace) -> int:
    rng = random.Random(args.seed)
    random.seed(args.seed)
    if not args.no_start:
        log("starting a fresh stack with short leases")
        compose_up({**STACK_ENV, "WORKER_REPLICAS": str(args.workers)})
    log("creating API key")
    key = create_api_key("chaos")
    run_id = uuid.uuid4().hex[:8]
    specs = make_jobs(args.jobs, rng)
    n_side_effect = sum(1 for s in specs if s["type"] == "side_effect")
    stats = SubmitStats()
    chaos = Chaos()
    done = asyncio.Event()
    started = time.monotonic()
    wall_start = time.time()

    log(f"submitting {args.jobs} jobs over {args.submit_seconds}s with chaos enabled")
    driver = asyncio.create_task(chaos_driver(chaos, started, args.submit_seconds, done, rng))
    await submit_all(key, run_id, specs, args.submit_seconds, stats)
    log(f"all submits acknowledged ({len(stats.ids)}), waiting for the system to settle")
    counts = await wait_until_settled(
        len(stats.ids), args.timeout if not stats.failures else 15, done, "chaos"
    )
    done.set()
    await driver
    elapsed = time.monotonic() - started

    # make sure nothing is left stopped before verifying
    for service in ("api", "worker", "relay", "reaper", "redis", "rabbitmq"):
        for cid in await container_ids(service):
            await arun(["docker", "start", cid])

    checks, info = await verify("chaos", set(stats.ids.values()), n_side_effect, stats, specs)

    print()
    print("=" * 78)
    print("CHAOS TEST REPORT")
    print("=" * 78)
    print(f"jobs submitted        : {args.jobs} ({n_side_effect} side_effect)")
    print(f"workers               : {args.workers} replicas x 16 concurrency, lease 6s")
    print(f"total time            : {elapsed:.1f}s")
    print(f"worker SIGKILLs       : {chaos.kills.get('worker', 0)}")
    print(
        f"other faults          : {', '.join(line.split('  ', 1)[1] for line in chaos.timeline if 'worker' not in line and 'back' not in line)}"
    )
    print(f"final status counts   : {counts}")
    print(f"recovery              : {info.get('recovery')}")
    print(f"side effects          : {info.get('effects')}")
    print(f"submit client         : {info.get('submit')}")
    print("-" * 78)
    for c in checks:
        print(f"[{'PASS' if c.passed else 'FAIL'}] {c.name}\n         {c.detail}")
    print("-" * 78)
    ok = all(c.passed for c in checks)
    print("RESULT:", "PASS" if ok else "FAIL")

    result = {
        "passed": ok,
        "command": f"uv run python scripts/chaos_test.py --jobs {args.jobs} --workers {args.workers} "
        f"--submit-seconds {args.submit_seconds} --seed {args.seed}",
        "started_at_epoch": wall_start,
        "elapsed_seconds": round(elapsed, 1),
        "jobs": args.jobs,
        "side_effect_jobs": n_side_effect,
        "workers": args.workers,
        "worker_kills": chaos.kills.get("worker", 0),
        "kills": chaos.kills,
        "restarts": chaos.restarts,
        "timeline": chaos.timeline,
        "final_status_counts": counts,
        "checks": [{"name": c.name, "passed": c.passed, "detail": c.detail} for c in checks],
        "details": info,
        "machine": machine_specs(),
        "stack_env": STACK_ENV,
    }
    out = ROOT / "bench-out" / "chaos.json"
    dump_json(out, result)
    log(f"wrote {out.relative_to(ROOT)}")
    if args.write_report:
        write_report(result, ROOT / "docs" / "CHAOS_RESULTS.md")
    return 0 if ok else 1


def write_report(result: dict[str, Any], path: Path) -> None:
    m = result["machine"]
    lines = [
        "# Chaos test results",
        "",
        "Produced by `make chaos` (`scripts/chaos_test.py`). The numbers below are copied from",
        f"`bench-out/chaos.json` of one real run on {time.strftime('%Y-%m-%d', time.localtime(result['started_at_epoch']))}.",
        "",
        "## Environment",
        "",
        f"- Machine: {m.get('cpu', m['machine'])}, {m.get('logical_cpus')} logical CPUs, {m.get('memory_gb')} GB RAM, {m['platform']}",
        f"- Docker: {m.get('docker_version')} ({m.get('docker_os')}), VM with {m.get('docker_vm_cpus')} CPUs and {m.get('docker_vm_memory_gb')} GB",
        "- Every service, the fault injector and the load generator share this one machine.",
        f"- Command: `{result['command']}`",
        f"- Stack settings: {', '.join(f'{k}={v}' for k, v in result['stack_env'].items())}, WORKER_REPLICAS={result['workers']}",
        "",
        "## Faults injected",
        "",
        f"- {result['worker_kills']} random worker SIGKILLs (each restarted 1 to 3 seconds later)",
        "- One SIGKILL each of the API, relay and reaper, one restart each of Redis and RabbitMQ",
        "",
        "```",
        *result["timeline"],
        "```",
        "",
        "## Outcome",
        "",
        f"- Jobs submitted: {result['jobs']} ({result['side_effect_jobs']} side_effect)",
        f"- Total time from first submit to all jobs terminal: {result['elapsed_seconds']}s",
        f"- Final status counts: `{result['final_status_counts']}`",
        f"- Recovery activity: `{result['details']['recovery']}`",
        f"- Side effects: `{result['details']['effects']}`",
        f"- Submit client: `{result['details']['submit']}`",
        "",
        "| Check | Result | Detail |",
        "| --- | --- | --- |",
    ]
    for c in result["checks"]:
        lines.append(f"| {c['name']} | {'PASS' if c['passed'] else 'FAIL'} | {c['detail']} |")
    lines += ["", f"Overall: **{'PASS' if result['passed'] else 'FAIL'}**", ""]
    path.write_text("\n".join(lines))
    log(f"wrote {path.relative_to(ROOT)}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--jobs", type=int, default=3000)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--submit-seconds", type=float, default=45.0)
    parser.add_argument("--timeout", type=float, default=900.0)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument(
        "--no-start", action="store_true", help="use the stack that is already running"
    )
    parser.add_argument("--write-report", action="store_true", help="write docs/CHAOS_RESULTS.md")
    args = parser.parse_args()
    sys.exit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    # keep `run` imported for ad-hoc debugging from a REPL
    _ = run
    main()
