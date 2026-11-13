"""Prometheus metrics shared by all services (each process exposes its own registry)."""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

_DURATION_BUCKETS = (0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300)
_LATENCY_BUCKETS = (0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10)

# Job lifecycle
JOB_TRANSITIONS = Counter(
    "jobs_transitions_total", "Job state transitions", ["from_status", "to_status"]
)
JOB_DURATION = Histogram(
    "job_duration_seconds",
    "Handler execution time",
    ["type", "outcome"],
    buckets=_DURATION_BUCKETS,
)
JOB_END_TO_END = Histogram(
    "job_end_to_end_seconds",
    "Time from job creation to success",
    ["type"],
    buckets=_DURATION_BUCKETS,
)
JOB_RETRIES = Counter("job_retries_total", "Retries scheduled", ["type"])
JOBS_BY_STATUS = Gauge("jobs_by_status", "Jobs per status", ["status"])

# Queue, DLQ, outbox
QUEUE_DEPTH = Gauge("queue_depth", "Ready messages in the work queue")
DLQ_SIZE = Gauge("dlq_size", "Dead jobs", ["source"])
OUTBOX_PENDING = Gauge("outbox_pending", "Unpublished outbox rows that are due")
OUTBOX_LAG = Gauge("outbox_lag_seconds", "Age of the oldest due, unpublished outbox row")
OUTBOX_PUBLISHED = Counter("outbox_published_total", "Outbox rows published", ["kind"])
OUTBOX_SKIPPED = Counter("outbox_skipped_total", "Outbox rows dropped as stale", ["reason"])
OUTBOX_PUBLISH_ERRORS = Counter("outbox_publish_errors_total", "Outbox publish failures")

# Worker
WORKER_INFLIGHT = Gauge("worker_inflight_jobs", "Jobs currently executing")
DUPLICATE_DELIVERIES = Counter(
    "worker_duplicate_deliveries_total", "Deliveries dropped because the claim failed"
)
POISON_MESSAGES = Counter("worker_poison_messages_total", "Unparseable messages dead-lettered")
HEARTBEAT_FAILURES = Counter("worker_heartbeat_failures_total", "Lease heartbeat failures")
LEASE_LOST = Counter("worker_lease_lost_total", "Jobs abandoned because the lease was lost")

# Reaper
LEASE_RECLAIMS = Counter("lease_reclaims_total", "Expired leases reclaimed", ["outcome"])

# API
HTTP_REQUESTS = Counter("http_requests_total", "HTTP requests", ["method", "route", "status"])
HTTP_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency",
    ["method", "route"],
    buckets=_LATENCY_BUCKETS,
)
RATE_LIMIT_REJECTIONS = Counter("rate_limit_rejections_total", "Requests rejected with 429")
RATE_LIMIT_FAIL_OPEN = Counter(
    "rate_limit_fail_open_total", "Requests admitted because the limiter was unavailable"
)
CACHE_HITS = Counter("cache_hits_total", "Job cache hits")
CACHE_MISSES = Counter("cache_misses_total", "Job cache misses")
CACHE_ERRORS = Counter("cache_errors_total", "Job cache errors (fail-open)")
