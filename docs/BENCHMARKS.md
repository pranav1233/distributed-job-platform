# Benchmarks

Every number here comes from a real run of `scripts/load_test.py`. Raw output is in
`bench-out/load-results.json`. Nothing was estimated or extrapolated.

## Environment

- Machine: Apple M4, 10 logical CPUs, 16 GB RAM
- OS: macOS-27.2.1-arm64-arm-64bit
- Docker: 29.9.3 (Docker Desktop), VM limits 10 CPUs and 7.7 GB
- Everything runs on this one machine: PostgreSQL, Redis, RabbitMQ, the API (one uvicorn process), the workers, relay and reaper, Prometheus, Grafana, and the load generator itself.
  Numbers are therefore a lower bound on what dedicated hosts would give, and workers compete with the datastores for the same cores.
- Command: `uv run python scripts/load_test.py --workers 1 2 4 --write-report`
- Worker settings: concurrency 32 jobs per worker process, prefetch 48, lease 30s, `LOG_LEVEL=WARNING`
- Job type: `io_task` (simulated I/O, sleeps 200 ms). Theoretical ceiling per worker process = concurrency / duration = 160 jobs/s.
- Stack is rebuilt from scratch (`docker compose down -v`, then `up --build`) before each worker count.

## Method

1. **Steady state (phase A)**: open-loop, 20 jobs/s for 15s (300 jobs). The queue stays empty, so end-to-end latency (`finished_at - created_at`, both from PostgreSQL) is the platform overhead plus the 200 ms of work.
2. **Burst (phase B)**: 8000 jobs submitted over 64 concurrent HTTP connections spread across 4 load generator processes, as fast as the API accepts them. Enqueue throughput = jobs / wall time of the submission phase. POST latency is measured client side. Sustained processing rate = jobs completed between the 10th and 90th percentile of completion times, divided by that interval, so ramp-up and tail are excluded. End-to-end latency in this phase includes queueing behind the backlog.
3. **Reads (phase C)**: `GET /v1/jobs/{id}` for 2000 distinct jobs over 64 connections. The first sweep misses the Redis cache, the second hits it.

## Results

### Throughput

| Workers | Enqueue (jobs/s) | POST p50 / p95 / p99 (ms) | Sustained processed (jobs/s) | Overall processed (jobs/s) | Scaling vs 1 worker |
| ---: | ---: | --- | ---: | ---: | ---: |
| 1 | 677.4 | 88.03 / 127.1 / 142.96 | 129.0 | 129.9 | 1.00x |
| 2 | 649.6 | 93.54 / 127.95 / 163.97 | 282.5 | 279.2 | 2.19x |
| 4 | 557.4 | 107.51 / 147.08 / 196.64 | 563.5 | 549.0 | 4.37x |

### End-to-end job latency

| Workers | Steady state p50 / p95 / p99 (ms) | Queue wait p50 / p95 (ms) | Under backlog p50 / p95 / p99 (ms) |
| ---: | --- | --- | --- |
| 1 | 244.29 / 272.13 / 280.9 | 37.28 / 63.61 | 24384.16 / 47266.84 / 49468.45 |
| 2 | 243.37 / 270.14 / 278.73 | 36.38 / 62.7 | 8058.86 / 15502.64 / 16192.98 |
| 4 | 244.44 / 270.07 / 274.07 | 36.6 / 63.04 | 295.21 / 357.2 / 401.52 |

Steady-state latency includes the 200 ms of handler time. Queue wait is `started_at - created_at`: outbox poll, publish with confirm, delivery and claim.

### Read latency (GET /v1/jobs/{id})

| Workers | Cache miss p50 / p95 / p99 (ms) | req/s | Cache hit p50 / p95 / p99 (ms) | req/s |
| ---: | --- | ---: | --- | ---: |
| 1 | 51.48 / 73.62 / 133.75 | 1114.3 | 18.55 / 38.49 / 44.24 | 2954.8 |
| 2 | 53.59 / 75.25 / 162.66 | 1058.7 | 19.33 / 60.1 / 106.72 | 2411.4 |
| 4 | 54.03 / 82.89 / 147.38 | 1082.6 | 18.84 / 35.54 / 97.59 | 2818.8 |

### Where the CPU went during the burst (docker stats, mean % of one core per container)

| Workers | api | grafana | postgres | prometheus | rabbitmq | reaper | redis | relay | worker |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 19.4 | 0.1 | 26.5 | 0.1 | 8.2 | 2.9 | 1.5 | 7.9 | 30.3 |
| 2 | 40.0 | 0.1 | 37.3 | 0.1 | 10.3 | 2.7 | 2.7 | 9.3 | 28.2 |
| 4 | 79.7 | 0.0 | 70.5 | 0.1 | 16.8 | 3.5 | 6.1 | 13.6 | 33.9 |

Worker rows are the mean across worker replicas that were sampled. A value near 100 for one container means that process is saturated on one core.

## Reading the numbers

- **The workers are the limit, up to a point.** One worker process can complete at most 160 jobs per second with 32 concurrent slots and 200 ms of work. It sustained 129.0 (81 percent of that ceiling) and four processes sustained 563.5 of a possible 640 (88 percent). The remainder is platform overhead inside each slot: the claim and completion transactions, the broker acknowledgement and event loop scheduling.
- **Scaling above 2x at two workers** (282.5 against 2 x 129.0) is not a real super-linear effect. Each configuration was measured once in this report, and per-slot overhead was a little lower in the two-worker run. An earlier complete run of the same command on the preceding commit measured 128.1, 279.0 and 558.7 jobs/s for 1, 2 and 4 workers, so run to run differences were about one to two percent.
- **Enqueue throughput falls as workers are added** (677 to 557 jobs/s) because everything shares the same cores. At four workers the single API process averaged 79.7 percent of one core and PostgreSQL 70.5 percent during the burst, so the next limit on this machine is the one API process and the database, not the workers. More API replicas are the obvious lever, and the API holds no state.
- **Latency under backlog** is dominated by queueing: 8000 jobs submitted in about 12 seconds against a processing rate of 129 jobs/s means the median job waits roughly 24 seconds with one worker. The steady-state table is the one that reflects platform overhead: about 44 ms on top of the 200 ms handler at the median, of which 37 ms is the time from creation to the start of execution (outbox poll interval of 50 ms, publish with confirm, delivery, claim).
- **Cache hits** answered in about 19 ms at the median under 64 concurrent connections against about 52 ms for misses that go to PostgreSQL. These are end-to-end HTTP latencies including the load generator, not server-side timings.

