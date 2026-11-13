#!/usr/bin/env python3
"""Load test: enqueue throughput, API latency, end-to-end latency, sustained jobs/sec.

For each worker count the script rebuilds the compose stack from scratch, then runs:

  A. steady-state latency: open-loop submissions at a fixed low rate, so the queue stays
     empty and end-to-end latency reflects platform overhead, not queueing
  B. burst: N jobs submitted as fast as the API accepts them by a pool of concurrent
     clients; then the backlog drains. Measures enqueue throughput, POST latency and the
     sustained processing rate
  C. read latency: GET /v1/jobs/{id}, first touch (cache miss) and repeat (cache hit)

All timestamps for job latency come from PostgreSQL (created_at, started_at, finished_at),
so client and worker clock differences do not matter.

Usage: uv run python scripts/load_test.py --workers 1 2 4 --write-report
"""

from __future__ import annotations

import argparse
import asyncio
import json
import multiprocessing as mp
import random
import statistics
import sys
import time
from concurrent.futures import ProcessPoolExecutor
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
    percentile,
    pg,
    run,
)

STACK_ENV = {
    "LOG_LEVEL": "WARNING",
    "WORKER_CONCURRENCY": "32",
    "WORKER_PREFETCH": "48",
    "LEASE_SECONDS": "30",
}


def latency_summary(values_s: list[float]) -> dict[str, float]:
    ms = [v * 1000 for v in values_s]
    return {
        "count": len(ms),
        "p50_ms": round(percentile(ms, 50), 2),
        "p95_ms": round(percentile(ms, 95), 2),
        "p99_ms": round(percentile(ms, 99), 2),
        "max_ms": round(max(ms), 2) if ms else float("nan"),
        "mean_ms": round(statistics.fmean(ms), 2) if ms else float("nan"),
    }


async def post_job(client: httpx.AsyncClient, headers: dict[str, str], duration_ms: int) -> float:
    started = time.perf_counter()
    resp = await client.post(
        "/v1/jobs",
        json={"type": "io_task", "payload": {"duration_ms": duration_ms}},
        headers=headers,
    )
    elapsed = time.perf_counter() - started
    if resp.status_code != 201:
        raise RuntimeError(f"submit failed: {resp.status_code} {resp.text[:200]}")
    return elapsed


async def wait_done(owner: str, since: float, expected: int, timeout: float = 600) -> None:
    conn = await pg()
    try:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            n = await conn.fetchval(
                "SELECT count(*) FROM jobs WHERE owner = $1 AND created_at >= to_timestamp($2) "
                "AND status IN ('succeeded','dead','cancelled')",
                owner,
                since,
            )
            if n >= expected:
                return
            await asyncio.sleep(0.25)
        raise TimeoutError(f"only {n}/{expected} jobs finished")
    finally:
        await conn.close()


async def job_times(owner: str, since: float) -> list[tuple[float, float, float]]:
    conn = await pg()
    try:
        rows = await conn.fetch(
            "SELECT extract(epoch FROM created_at) AS c, extract(epoch FROM started_at) AS s, "
            "extract(epoch FROM finished_at) AS f FROM jobs "
            "WHERE owner = $1 AND created_at >= to_timestamp($2) AND status = 'succeeded' "
            "ORDER BY finished_at",
            owner,
            since,
        )
    finally:
        await conn.close()
    return [(float(r["c"]), float(r["s"]), float(r["f"])) for r in rows]


# --------------------------------------------------------------------------- load generator
# One Python process saturates around a thousand HTTP requests per second, which would make
# the load generator the bottleneck. The burst and read phases therefore fan out over several
# processes that start together at a shared wall-clock instant.


async def _blast_async(spec: dict[str, Any]) -> dict[str, Any]:
    headers = {"Authorization": f"Bearer {spec['key']}"}
    limits = httpx.Limits(max_connections=spec["conc"], max_keepalive_connections=spec["conc"])
    latencies: list[float] = []
    errors = 0
    async with httpx.AsyncClient(base_url=spec["api_url"], timeout=60, limits=limits) as client:
        sem = asyncio.Semaphore(spec["conc"])

        async def one(arg: Any) -> None:
            nonlocal errors
            async with sem:
                t = time.perf_counter()
                if spec["kind"] == "post":
                    resp = await client.post(
                        "/v1/jobs",
                        json={"type": "io_task", "payload": {"duration_ms": spec["duration_ms"]}},
                        headers=headers,
                    )
                    ok = resp.status_code == 201
                else:
                    resp = await client.get(f"/v1/jobs/{arg}", headers=headers)
                    ok = resp.status_code == 200
                latencies.append(time.perf_counter() - t)
                if not ok:
                    errors += 1

        delay = spec["start_at"] - time.time()
        if delay > 0:
            await asyncio.sleep(delay)
        started = time.time()
        await asyncio.gather(*(one(a) for a in spec["args"]))
        ended = time.time()
    return {"latencies": latencies, "start": started, "end": ended, "errors": errors}


def _blast_proc(spec: dict[str, Any]) -> dict[str, Any]:
    return asyncio.run(_blast_async(spec))


async def blast(
    kind: str, args: list[Any], key: str, conc: int, procs: int, duration_ms: int = 0
) -> dict[str, Any]:
    """Run `len(args)` requests across `procs` processes. Returns latencies and wall time."""
    start_at = time.time() + 3.0
    chunks = [args[i::procs] for i in range(procs)]
    specs = [
        {
            "kind": kind,
            "args": chunk,
            "key": key,
            "conc": max(1, conc // procs),
            "api_url": API_URL,
            "duration_ms": duration_ms,
            "start_at": start_at,
        }
        for chunk in chunks
        if chunk
    ]
    loop = asyncio.get_running_loop()
    with ProcessPoolExecutor(max_workers=len(specs), mp_context=mp.get_context("spawn")) as pool:
        outs = await asyncio.gather(*(loop.run_in_executor(pool, _blast_proc, sp) for sp in specs))
    errors = sum(o["errors"] for o in outs)
    if errors:
        raise RuntimeError(f"{errors} requests failed during the {kind} blast")
    return {
        "latencies": [v for o in outs for v in o["latencies"]],
        "wall": max(o["end"] for o in outs) - min(o["start"] for o in outs),
    }


async def sample_docker_cpu(stop: asyncio.Event, samples: dict[str, list[float]]) -> None:
    while not stop.is_set():
        out = await arun(["docker", "stats", "--no-stream", "--format", "{{json .}}"])
        for line in out.splitlines():
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            name = str(row["Name"]).removeprefix("jobplatform-")
            service = name.rsplit("-", 1)[0]
            samples.setdefault(service, []).append(float(str(row["CPUPerc"]).rstrip("%")))
        await asyncio.sleep(0.5)


async def phase_steady(key: str, rate: float, seconds: float, duration_ms: int) -> dict[str, Any]:
    headers = {"Authorization": f"Bearer {key}"}
    since = time.time()
    total = int(rate * seconds)
    async with httpx.AsyncClient(base_url=API_URL, timeout=30) as client:
        tasks = []
        t0 = time.monotonic()
        for i in range(total):
            await asyncio.sleep(max(0.0, t0 + i / rate - time.monotonic()))
            tasks.append(asyncio.create_task(post_job(client, headers, duration_ms)))
        await asyncio.gather(*tasks)
    await wait_done("bench", since, total)
    times = await job_times("bench", since)
    e2e = [f - c for c, _s, f in times]
    queue_wait = [s - c for c, s, _f in times]
    return {
        "rate_per_s": rate,
        "jobs": total,
        "job_duration_ms": duration_ms,
        "end_to_end": latency_summary(e2e),
        "queue_wait_before_start": latency_summary(queue_wait),
    }


async def phase_burst(
    key: str, jobs: int, clients: int, procs: int, duration_ms: int
) -> dict[str, Any]:
    since = time.time()
    cpu: dict[str, list[float]] = {}
    stop = asyncio.Event()
    sampler = asyncio.create_task(sample_docker_cpu(stop, cpu))
    out = await blast("post", list(range(jobs)), key, clients, procs, duration_ms)
    latencies, enqueue_wall = out["latencies"], out["wall"]
    log(f"  enqueued {jobs} jobs in {enqueue_wall:.2f}s, waiting for the backlog to drain")
    await wait_done("bench", since, jobs)
    stop.set()
    await sampler

    times = await job_times("bench", since)
    finishes = [f for _c, _s, f in times]
    first_start = min(s for _c, s, _f in times)
    last_finish = max(finishes)
    lo, hi = int(0.1 * len(finishes)), int(0.9 * len(finishes))
    sustained = (
        (hi - lo) / (finishes[hi] - finishes[lo]) if finishes[hi] > finishes[lo] else float("nan")
    )
    e2e = [f - c for c, _s, f in times]
    return {
        "jobs": jobs,
        "clients": clients,
        "client_processes": procs,
        "job_duration_ms": duration_ms,
        "enqueue_seconds": round(enqueue_wall, 3),
        "enqueue_jobs_per_s": round(jobs / enqueue_wall, 1),
        "post_latency": latency_summary(latencies),
        "drain_seconds": round(last_finish - first_start, 2),
        "overall_jobs_per_s": round(len(times) / (last_finish - first_start), 1),
        "sustained_jobs_per_s": round(sustained, 1),
        "end_to_end_under_backlog": latency_summary(e2e),
        "avg_cpu_percent_by_service": {
            svc: round(statistics.fmean(vals), 1) for svc, vals in sorted(cpu.items())
        },
        "peak_cpu_percent_by_service": {
            svc: round(max(vals), 1) for svc, vals in sorted(cpu.items())
        },
        "succeeded": len(times),
    }


async def phase_reads(key: str, ids: list[str], clients: int, procs: int) -> dict[str, Any]:
    cold = await blast("get", ids, key, clients, procs)
    warm = await blast("get", ids, key, clients, procs)
    return {
        "requests_per_sweep": len(ids),
        "cold_cache_miss": {
            **latency_summary(cold["latencies"]),
            "req_per_s": round(len(ids) / cold["wall"], 1),
        },
        "warm_cache_hit": {
            **latency_summary(warm["latencies"]),
            "req_per_s": round(len(ids) / warm["wall"], 1),
        },
    }


async def run_for_workers(n: int, args: argparse.Namespace) -> dict[str, Any]:
    log(f"=== {n} worker(s): rebuilding the stack")
    compose_up({**STACK_ENV, "WORKER_REPLICAS": str(n)})
    key = create_api_key("bench")

    log("  warm-up")
    warm_since = time.time()
    headers = {"Authorization": f"Bearer {key}"}
    async with httpx.AsyncClient(base_url=API_URL, timeout=30) as client:
        await asyncio.gather(*(post_job(client, headers, 50) for _ in range(300)))
    await wait_done("bench", warm_since, 300)
    await asyncio.sleep(2)

    log("  phase A: steady-state end-to-end latency")
    steady = await phase_steady(key, args.steady_rate, args.steady_seconds, args.duration_ms)
    await asyncio.sleep(2)

    log("  phase B: burst enqueue and drain")
    burst_since = time.time()
    burst = await phase_burst(key, args.jobs, args.clients, args.client_procs, args.duration_ms)

    log("  phase C: read latency")
    conn = await pg()
    rows = await conn.fetch(
        "SELECT id::text FROM jobs WHERE owner = 'bench' AND created_at >= to_timestamp($1) LIMIT $2",
        burst_since,
        args.reads,
    )
    await conn.close()
    ids = [r["id"] for r in rows]
    random.shuffle(ids)
    reads = await phase_reads(key, ids, args.clients, args.client_procs)

    result = {
        "workers": n,
        "steady_state": steady,
        "burst": burst,
        "reads": reads,
        "stack_env": {**STACK_ENV, "WORKER_REPLICAS": str(n)},
    }
    log(
        f"  -> enqueue {burst['enqueue_jobs_per_s']}/s, sustained {burst['sustained_jobs_per_s']} jobs/s, "
        f"POST p95 {burst['post_latency']['p95_ms']} ms"
    )
    return result


def render_report(
    results: list[dict[str, Any]], args: argparse.Namespace, specs: dict[str, Any], command: str
) -> str:
    base = results[0]["burst"]["sustained_jobs_per_s"]
    L: list[str] = []
    L.append("# Benchmarks")
    L.append("")
    L.append("Every number here comes from a real run of `scripts/load_test.py`. Raw output is in")
    L.append("`bench-out/load-results.json`. Nothing was estimated or extrapolated.")
    L.append("")
    L.append("## Environment")
    L.append("")
    L.append(
        f"- Machine: {specs.get('cpu', specs['machine'])}, {specs.get('logical_cpus')} logical CPUs, {specs.get('memory_gb')} GB RAM"
    )
    L.append(f"- OS: {specs['platform']}")
    L.append(
        f"- Docker: {specs.get('docker_version')} ({specs.get('docker_os')}), VM limits {specs.get('docker_vm_cpus')} CPUs and {specs.get('docker_vm_memory_gb')} GB"
    )
    L.append(
        "- Everything runs on this one machine: PostgreSQL, Redis, RabbitMQ, the API (one uvicorn process), the workers, relay and reaper, Prometheus, Grafana, and the load generator itself."
    )
    L.append(
        "  Numbers are therefore a lower bound on what dedicated hosts would give, and workers compete with the datastores for the same cores."
    )
    L.append(f"- Command: `{command}`")
    L.append(
        f"- Worker settings: concurrency {STACK_ENV['WORKER_CONCURRENCY']} jobs per worker process, prefetch {STACK_ENV['WORKER_PREFETCH']}, lease {STACK_ENV['LEASE_SECONDS']}s, `LOG_LEVEL=WARNING`"
    )
    L.append(
        f"- Job type: `io_task` (simulated I/O, sleeps {args.duration_ms} ms). Theoretical ceiling per worker process = concurrency / duration = {int(int(STACK_ENV['WORKER_CONCURRENCY']) * 1000 / args.duration_ms)} jobs/s."
    )
    L.append(
        "- Stack is rebuilt from scratch (`docker compose down -v`, then `up --build`) before each worker count."
    )
    L.append("")
    L.append("## Method")
    L.append("")
    L.append(
        f"1. **Steady state (phase A)**: open-loop, {args.steady_rate:g} jobs/s for {args.steady_seconds:g}s ({int(args.steady_rate * args.steady_seconds)} jobs). The queue stays empty, so end-to-end latency (`finished_at - created_at`, both from PostgreSQL) is the platform overhead plus the {args.duration_ms} ms of work."
    )
    L.append(
        f"2. **Burst (phase B)**: {args.jobs} jobs submitted over {args.clients} concurrent HTTP connections spread across {args.client_procs} load generator processes, as fast as the API accepts them. Enqueue throughput = jobs / wall time of the submission phase. POST latency is measured client side. Sustained processing rate = jobs completed between the 10th and 90th percentile of completion times, divided by that interval, so ramp-up and tail are excluded. End-to-end latency in this phase includes queueing behind the backlog."
    )
    L.append(
        f"3. **Reads (phase C)**: `GET /v1/jobs/{{id}}` for {results[0]['reads']['requests_per_sweep']} distinct jobs over {args.clients} connections. The first sweep misses the Redis cache, the second hits it."
    )
    L.append("")
    L.append("## Results")
    L.append("")
    L.append("### Throughput")
    L.append("")
    L.append(
        "| Workers | Enqueue (jobs/s) | POST p50 / p95 / p99 (ms) | Sustained processed (jobs/s) | Overall processed (jobs/s) | Scaling vs 1 worker |"
    )
    L.append("| ---: | ---: | --- | ---: | ---: | ---: |")
    for r in results:
        b = r["burst"]
        p = b["post_latency"]
        L.append(
            f"| {r['workers']} | {b['enqueue_jobs_per_s']} | {p['p50_ms']} / {p['p95_ms']} / {p['p99_ms']} | {b['sustained_jobs_per_s']} | {b['overall_jobs_per_s']} | {b['sustained_jobs_per_s'] / base:.2f}x |"
        )
    L.append("")
    L.append("### End-to-end job latency")
    L.append("")
    L.append(
        "| Workers | Steady state p50 / p95 / p99 (ms) | Queue wait p50 / p95 (ms) | Under backlog p50 / p95 / p99 (ms) |"
    )
    L.append("| ---: | --- | --- | --- |")
    for r in results:
        s = r["steady_state"]
        e, w = s["end_to_end"], s["queue_wait_before_start"]
        u = r["burst"]["end_to_end_under_backlog"]
        L.append(
            f"| {r['workers']} | {e['p50_ms']} / {e['p95_ms']} / {e['p99_ms']} | {w['p50_ms']} / {w['p95_ms']} | {u['p50_ms']} / {u['p95_ms']} / {u['p99_ms']} |"
        )
    L.append("")
    L.append(
        f"Steady-state latency includes the {args.duration_ms} ms of handler time. Queue wait is `started_at - created_at`: outbox poll, publish with confirm, delivery and claim."
    )
    L.append("")
    L.append("### Read latency (GET /v1/jobs/{id})")
    L.append("")
    L.append(
        "| Workers | Cache miss p50 / p95 / p99 (ms) | req/s | Cache hit p50 / p95 / p99 (ms) | req/s |"
    )
    L.append("| ---: | --- | ---: | --- | ---: |")
    for r in results:
        c, w = r["reads"]["cold_cache_miss"], r["reads"]["warm_cache_hit"]
        L.append(
            f"| {r['workers']} | {c['p50_ms']} / {c['p95_ms']} / {c['p99_ms']} | {c['req_per_s']} | {w['p50_ms']} / {w['p95_ms']} / {w['p99_ms']} | {w['req_per_s']} |"
        )
    L.append("")
    L.append(
        "### Where the CPU went during the burst (docker stats, mean % of one core per container)"
    )
    L.append("")
    services = sorted({s for r in results for s in r["burst"]["avg_cpu_percent_by_service"]})
    L.append("| Workers | " + " | ".join(services) + " |")
    L.append("| ---: | " + " | ".join("---:" for _ in services) + " |")
    for r in results:
        cpu = r["burst"]["avg_cpu_percent_by_service"]
        L.append(f"| {r['workers']} | " + " | ".join(str(cpu.get(s, "")) for s in services) + " |")
    L.append("")
    L.append(
        "Worker rows are the mean across worker replicas that were sampled. A value near 100 for one container means that process is saturated on one core."
    )
    L.append("")
    return "\n".join(L)


async def main_async(args: argparse.Namespace) -> int:
    results = []
    for n in args.workers:
        results.append(await run_for_workers(n, args))
    specs = machine_specs()
    command = "uv run python scripts/load_test.py " + " ".join(sys.argv[1:])
    payload = {"command": command, "machine": specs, "args": vars(args), "results": results}
    out = ROOT / "bench-out" / "load-results.json"
    dump_json(out, payload)
    log(f"wrote {out.relative_to(ROOT)}")
    report = render_report(results, args, specs, command)
    if args.write_report:
        path = ROOT / "docs" / "BENCHMARKS.md"
        path.write_text(report + "\n")
        log(f"wrote {path.relative_to(ROOT)}")
    print(report)
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4])
    parser.add_argument("--jobs", type=int, default=8000)
    parser.add_argument("--clients", type=int, default=64, help="total concurrent HTTP connections")
    parser.add_argument("--client-procs", type=int, default=4, help="load generator processes")
    parser.add_argument("--duration-ms", type=int, default=200)
    parser.add_argument("--steady-rate", type=float, default=20.0)
    parser.add_argument("--steady-seconds", type=float, default=15.0)
    parser.add_argument("--reads", type=int, default=2000)
    parser.add_argument("--write-report", action="store_true")
    args = parser.parse_args()
    sys.exit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    _ = run, Path
    main()
