# Design notes

Technical decisions, with the reasoning and the alternatives that were considered. Where the
implementation departs from the original brief, the departure is called out explicitly.

## 1. Services

One image, four entrypoints: `python -m jobplatform.api | worker | relay | reaper`.

| Service | Talks to | Stateless | Safe to run N copies |
| --- | --- | --- | --- |
| api | Postgres, Redis | yes | yes |
| relay | Postgres, RabbitMQ, Redis (cache invalidation) | yes | yes (`FOR UPDATE SKIP LOCKED`) |
| worker | Postgres, RabbitMQ, Redis (cache invalidation) | yes | yes (conditional claim) |
| reaper | Postgres, RabbitMQ (queue depth only), Redis | yes | yes (`FOR UPDATE SKIP LOCKED`) |

The API never opens a connection to RabbitMQ. A broker outage therefore cannot fail a submit,
and `/readyz` deliberately does not check the broker.

## 2. Source of truth and the state machine

PostgreSQL is the only source of truth for job state. RabbitMQ carries dispatch intent and
nothing else; losing every message in the broker loses no job, because the reaper and the
outbox can regenerate the intent from rows.

States: `queued`, `running`, `succeeded`, `retry_scheduled`, `dead`, `cancelled`.

Transitions (see `domain/states.py`, the single table both code and tests read):

```
queued          -> running | cancelled
running         -> succeeded | retry_scheduled | dead | cancelled | queued
retry_scheduled -> queued | cancelled
dead            -> queued                       (operator requeue)
succeeded, cancelled: final
```

Departures from the brief's diagram, each needed for a real behaviour:

* `running -> queued` covers two cases the brief describes in prose: the reaper reclaiming an
  expired lease, and a worker releasing its lease on graceful shutdown.
* `dead -> queued` is the DLQ requeue endpoint.
* `running -> cancelled` lets a cancel request take effect on a running job.

Enforcement has three layers:

1. Every transition is a single `UPDATE ... WHERE id = :id AND status = :expected [AND fence]`
   (`repo/jobs.py::_transition`). Zero rows updated means the transition did not happen.
2. The matching `job_events` row, and any `outbox` row, are inserted in the same transaction.
3. Database constraints back the code: a `CHECK` on the status values, a `CHECK` that a lease
   exists if and only if the job is `running`, and a trigger that rejects `UPDATE` and
   `DELETE` on `job_events`.

`tests/integration/test_state_machine_db.py` runs every (state, operation) pair against real
Postgres and asserts an operation applies exactly from its legal source states and leaves no
trace (not even a version bump) otherwise.

Each transition also increments `jobs.version`. The version is what makes cache invalidation
race free (section 8).

## 3. Time

All deadlines (`lease_expires_at`, `available_at`, backoff) are computed with the database
clock (`now() + make_interval(...)`), never with a worker or API clock. Clock skew between
hosts cannot extend or shorten a lease. The rate limiter likewise uses Redis `TIME` inside
the Lua script.

## 4. Transactional outbox

`POST /v1/jobs` inserts the job, a `job_events` row and an `outbox` row in one transaction.
There is no code path in the API that publishes to RabbitMQ.

The relay loop (`relay/relay.py`):

0. **Promote** `retry_scheduled` jobs whose `available_at` has passed to `queued`, in their own
   committed transaction.
1. In a second transaction, `SELECT ... FROM outbox WHERE published_at IS NULL AND available_at <= now()
   ORDER BY available_at, id LIMIT n FOR UPDATE SKIP LOCKED`.
2. Drop rows that are stale (the job was cancelled, finished, or is already running); leave rows
   whose job is still `retry_scheduled`.
3. Publish the rest concurrently with publisher confirms and `mandatory=True`.
4. Mark confirmed rows `published_at`, count failures in `publish_attempts` and `last_error`,
   commit.

Decisions:

* **Promotion is a separate, earlier transaction.** The first implementation promoted inside
  the publishing transaction. An integration test exposed the race: a fast worker received the
  message before the relay committed, saw the job still `retry_scheduled`, failed its claim and
  dropped the delivery, after which nothing ever published again. Committing the promotion
  before publishing makes the invariant "a message in the queue implies the job is claimable"
  hold.
* **Row locks are held across the publish.** That is the literal `SKIP LOCKED` design the brief
  asks for and it is simple to reason about: a crash releases the locks and the batch is
  republished (at-least-once). The cost is a transaction that lives for one confirm round trip.
  Batches are bounded (`RELAY_BATCH_SIZE`, default 200) and publishes inside a batch run
  concurrently.
* **`mandatory=True`.** An unroutable message fails the confirm instead of vanishing.
* **Backoff on broker failure.** Failed rows simply stay pending, and the loop backs off
  exponentially up to 5 seconds. Nothing is dropped.
* **Polling, not LISTEN/NOTIFY.** A 50 ms poll on a partial index over unpublished rows is
  cheap and has no missed-notification failure mode. At larger scale this is where
  `LISTEN/NOTIFY` or logical decoding (Debezium) would replace polling.
* **Scheduled jobs reuse the same mechanism.** `delay_seconds` sets `available_at` in the
  future on both the job and its outbox row, so the relay's `available_at <= now()` filter is
  the scheduler.

## 5. At-least-once delivery, exactly-once effects

The platform does not claim exactly-once delivery. It composes three mechanisms:

* **Conditional claim.** `queued -> running` is a conditional UPDATE that also sets
  `lease_owner`, `lease_expires_at` and `attempts = attempts + 1`. A duplicate delivery of a
  job that is running, finished or cancelled fails the claim and is acked and dropped.
* **Fencing.** Every later transition (`complete`, `schedule_retry`, `mark_dead`, `release`,
  heartbeat) is guarded by `lease_owner = :me AND attempts = :attempt`. A worker that lost its
  lease (paused process, network partition) cannot overwrite the new owner's result.
* **Idempotent handlers.** Handlers must tolerate re-execution. The `side_effect` example
  writes `INSERT ... ON CONFLICT (job_id) DO UPDATE SET executions = executions + 1`, so the
  effect exists once and `executions` records how often the handler ran. The chaos test uses
  this table as proof.

The worker acks **after** the database transition commits. If it dies in between, the broker
redelivers, the claim fails (status already terminal), and the duplicate is dropped.

When the claim fails because the job is `running` under a live lease (a redelivery after a
worker crash), the message is acked and dropped as well. This is safe because recovery never
depends on the original message: the reaper reclaims the expired lease and writes a fresh
outbox row.

## 6. Leases, heartbeats and the reaper

* Lease length `LEASE_SECONDS` (default 30), heartbeat every `HEARTBEAT_INTERVAL_SECONDS`
  (default 10), so a lease survives two missed heartbeats.
* The heartbeat is a fenced UPDATE. If it affects zero rows the lease is gone (reclaimed,
  cancelled or finished), and the worker cancels the handler. This is also how a cancel
  request reaches a running job: the API flips the row to `cancelled`, and the worker notices at
  its next heartbeat.
* The reaper moves `running` rows with `lease_expires_at < now()` to `queued`, or to `dead` when
  `attempts >= max_attempts`, with an audit event and a new outbox row, in one transaction.
  Counting the crashed attempt means a job that deterministically kills its worker eventually
  lands in the DLQ instead of crash-looping forever.
* On graceful shutdown a worker releases unfinished leases with `attempts = attempts - 1`, so
  routine deploys do not consume retry budget.

## 7. Retries and the DLQ

* Backoff is `uniform(0, min(cap, base * 2^(attempt - 1)))` (full jitter), computed in the
  worker and stored as a future `available_at` plus a future-dated outbox row. Delay is driven
  by Postgres, not by RabbitMQ per-message TTLs, which only expire at the head of a queue and
  would create head-of-line blocking.
* Per job type: `max_attempts`, timeout, base and cap delay, and the set of non-retryable
  exception classes (`NonRetryableError`, pydantic `ValidationError` by default). Everything
  else is retryable.
* Exhausted or non-retryable jobs go to `dead` and a `dlq` outbox row is published to the
  dead-letter exchange.
* `GET /v1/dlq` reads **Postgres**, which is authoritative and owner scoped. The RabbitMQ DLQ
  holds an inspection copy with a 7 day TTL. Messages that are unparseable (no job id) are
  rejected by the worker with `requeue=False`, dead-letter through the queue's DLX, and exist
  only in the broker DLQ, since there is no job row to list.
* `POST /v1/dlq/{id}/requeue` moves `dead -> queued`, resets `attempts` to 0 and writes an
  outbox row.

## 8. Redis

Redis is never a source of truth. Both uses fail open.

**Rate limiting.** One Lua script (`ratelimit/token_bucket.py`) per request: refill from the
elapsed time, take a token if available, persist, set a TTL equal to the time until the bucket
is full again. It is atomic across any number of API instances. A burst of 60 concurrent
requests against a capacity of 10 admits exactly 10 (integration test). On any Redis error the
request is admitted, a metric is incremented and a warning is logged.

**Cache-aside for `GET /v1/jobs/{id}`.** TTL 3 s for active states, 300 s for terminal states.
Two problems need more than a naive `SET`/`DEL`:

* *Stale fill race.* A reader loads row version 3, a transition commits version 4 and deletes
  the key, then the reader writes version 3 back with a long TTL. To prevent it, every
  invalidation also records the new version in a tombstone key, and the fill script refuses to
  write a payload whose version is lower than the tombstone.
* *Stampede.* On a miss one caller takes a `SET NX` lock and loads from Postgres; the others
  poll the cache for up to 500 ms and then fall back to Postgres. 40 concurrent first reads
  hit Postgres once (integration test).

Invalidation runs after the transition commits, in the API inline (so a cancel is visible to
the next read) and in workers, relay and reaper as background tasks so a slow Redis never sits
on the job critical path.

## 9. Authentication

API keys are 256 bit random tokens (`jp_` prefix) stored as a SHA-256 hash. A fast unsalted
hash is appropriate for high entropy secrets; there is nothing to brute force, and it keeps the
hot path cheap. Resolved principals are cached in process for 30 seconds (5 seconds for
negative results), which bounds how long a revoked key keeps working.

Each key can carry its own token bucket parameters; keys without them use the defaults.

## 10. Idempotency keys

Scope is `(owner, key)`, enforced by a unique constraint, so two owners can reuse a key. The
insert uses `ON CONFLICT DO NOTHING`; the loser reads the winner's row. Concurrent requests with
the same key create exactly one job (25 parallel requests in the integration test).

A request fingerprint (hash of type, normalised payload, priority, attempts, delay) is stored.
Reusing a key with a **different** body returns `409 idempotency_key_reused` rather than
silently returning an unrelated job. Replays return `200` with `Idempotent-Replayed: true`.

## 11. Worker concurrency

* I/O-bound handlers are coroutines bounded by a semaphore (`WORKER_CONCURRENCY`).
* CPU-bound handlers run in a spawn-context `ProcessPoolExecutor`. Each running CPU job owns a
  slot in a shared flag array, so cancellation and timeout are cooperative: the parent sets the
  flag and the child raises at its next chunk boundary. The slot is recycled only after the
  child function returns.
* Slots are acquired before the claim, so a job never holds a lease while waiting for a free
  slot. A CPU job additionally waits for a CPU slot first.
* `WORKER_PREFETCH` defaults to 1.5 x concurrency so the next messages are already local when a
  slot frees up. The tradeoff: prefetched low-priority messages are not preempted by a later
  high-priority arrival. Lower the prefetch if strict priority matters more than throughput.

## 12. Priority queue

The work queue is a classic queue with `x-max-priority`. Priorities 0 to 9 map to the message
priority set by the relay.

## 13. Observability

Metrics are per process (each service exposes its own registry). Gauges that describe the whole
platform (jobs by status, queue depth, DLQ size, outbox lag) are collected by the reaper, so
they exist exactly once per reaper instance. The dashboard aggregates with `max`, which makes
extra reaper replicas harmless. Prometheus discovers scaled worker replicas through DNS service
discovery.

Logs are structured JSON (structlog) with `request_id` bound for the lifetime of an API request
and `job_id`, `owner`, `job_type`, `attempt`, `worker_id` bound for the lifetime of a job.

## 14. Testing strategy

* Unit tests need no containers: state table, backoff math (including property tests), the Lua
  token bucket (executed by an in-process Redis), cursors, fingerprints, key hashing, payload
  schemas, the CPU handler.
* Integration tests use real PostgreSQL 16, Redis 7 and RabbitMQ 3.13 through testcontainers
  (or service containers in CI, selected by `TEST_*_URL` environment variables). Relay, worker
  and reaper run in-process as asyncio tasks, so tests cover the real code paths quickly.
* The chaos test runs the real docker compose stack and kills real processes.

## 15. Infrastructure choices (AWS)

* Fargate for all four services from one ECR image. Workers get a 60 s stop timeout so SIGTERM
  can drain in-flight work, and CPU target-tracking autoscaling.
* RDS PostgreSQL 16 (single AZ by default, a variable turns on Multi-AZ), ElastiCache Redis 7
  with TLS, Amazon MQ for RabbitMQ 3.13 (single instance by default).
* Connection strings live in Secrets Manager as whole URLs and are injected as ECS secrets, so
  no credential appears in a task definition.
* One NAT gateway. This is a cost decision (see `docs/DEPLOY.md`); one per AZ is the
  production setting.
* Database migrations run as a one-off ECS task before a rollout, never from the application
  containers, so N replicas cannot race on DDL.

## 16. Housekeeping

Published outbox rows are pruned by the reaper after `OUTBOX_RETENTION_SECONDS` (default one
hour). `job_events` and `jobs` are retained; partitioning by `created_at` is the next step if
the tables grow beyond what one instance handles.
