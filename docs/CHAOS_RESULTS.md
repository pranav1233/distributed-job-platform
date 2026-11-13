# Chaos test results

Produced by `make chaos` (`scripts/chaos_test.py`). The numbers below are copied from
`bench-out/chaos.json` of one real run on 2026-11-12.

## Environment

- Machine: Apple M4, 10 logical CPUs, 16 GB RAM, macOS-27.2.1-arm64-arm-64bit
- Docker: 29.9.3 (Docker Desktop), VM with 10 CPUs and 7.7 GB
- Every service, the fault injector and the load generator share this one machine.
- Command: `uv run python scripts/chaos_test.py --jobs 3000 --workers 4 --submit-seconds 45.0 --seed 7`
- Stack settings: LEASE_SECONDS=6, HEARTBEAT_INTERVAL_SECONDS=2, REAPER_INTERVAL_SECONDS=1, RETRY_BASE_DELAY_SECONDS=1, RETRY_MAX_DELAY_SECONDS=10, WORKER_CONCURRENCY=16, WORKER_PREFETCH=24, WORKER_REPLICAS=4

## Faults injected

- 14 random worker SIGKILLs (each restarted 1 to 3 seconds later)
- One SIGKILL each of the API, relay and reaper, one restart each of Redis and RabbitMQ

```
t+   4.2s  SIGKILL worker (baed27d7c079)
t+  10.7s  SIGKILL worker (7bd2406ac0ff)
t+  13.7s  SIGKILL api (2361f257a657)
t+  17.5s  SIGKILL worker (d2b7e56f3b50)
t+  20.3s  restart redis
t+  20.6s  redis is back
t+  22.5s  SIGKILL worker (d2b7e56f3b50)
t+  27.2s  SIGKILL relay (c0623d16fc8a)
t+  29.6s  SIGKILL worker (baed27d7c079)
t+  33.8s  restart rabbitmq
t+  35.0s  SIGKILL worker (d2b7e56f3b50)
t+  35.1s  rabbitmq is back
t+  40.7s  SIGKILL reaper (a881407c75a9)
t+  41.4s  SIGKILL worker (d2b7e56f3b50)
t+  46.9s  SIGKILL worker (d2b7e56f3b50)
t+  54.4s  SIGKILL worker (642420fcfc03)
t+  60.9s  SIGKILL worker (642420fcfc03)
t+  67.0s  SIGKILL worker (d2b7e56f3b50)
t+  72.8s  SIGKILL worker (7bd2406ac0ff)
t+  79.1s  SIGKILL worker (d2b7e56f3b50)
t+  86.7s  SIGKILL worker (642420fcfc03)
```

## Outcome

- Jobs submitted: 3000 (2429 side_effect)
- Total time from first submit to all jobs terminal: 89.7s
- Final status counts: `{'succeeded': 3000}`
- Recovery activity: `{'lease_reclaims_by_reaper': 189, 'leases_released_on_shutdown': 0, 'handler_retries_scheduled': 435, 'total_attempts': 3624, 'jobs_needing_more_than_one_attempt': 455}`
- Side effects: `{'rows': 2429, 'jobs_executed_more_than_once': 157, 'max_executions_of_one_job': 3, 'total_handler_executions': 2598}`
- Submit client: `{'client_retries': 230, 'idempotent_replays': 0, 'submit_failures': 0}`

| Check | Result | Detail |
| --- | --- | --- |
| zero lost jobs | PASS | 3000 acknowledged by the API, 3000 in Postgres, 0 missing, 0 unexpected |
| every job reached a terminal state | PASS | {'succeeded': 3000} |
| no job exhausted its retries | PASS | succeeded=3000 dead=0 |
| exactly one side effect per side_effect job | PASS | 2429 side_effect jobs, 2429 effect rows (2429 distinct), 0 jobs without an effect, 0 orphans |
| audit trails are unbroken, one `succeeded` event per job | PASS | 0 broken transitions, 0 jobs without exactly one succeeded event |
| outbox fully drained | PASS | 0 rows pending |
| every submit eventually acknowledged | PASS | 3000/3000 acknowledged, 230 client retries, 0 idempotent replays |

Overall: **PASS**
