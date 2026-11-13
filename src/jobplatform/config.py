"""Runtime configuration, loaded from environment variables."""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=False)

    # Datastores
    database_url: str = "postgresql+asyncpg://jobs:jobs@localhost:5432/jobs"
    redis_url: str = "redis://localhost:6379/0"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    db_pool_size: int = 10
    db_max_overflow: int = 10

    # API
    api_host: str = "0.0.0.0"  # noqa: S104
    api_port: int = 8000
    default_rate_limit_capacity: int = 100
    default_rate_limit_refill_per_sec: float = 50.0
    cache_ttl_active_seconds: int = 3
    cache_ttl_terminal_seconds: int = 300
    cache_lock_seconds: int = 5
    api_key_cache_seconds: int = 30
    max_payload_bytes: int = 262_144

    # Worker
    worker_concurrency: int = 16
    worker_prefetch: int = 32
    worker_cpu_processes: int = Field(default=2, ge=1)
    lease_seconds: float = 30.0
    heartbeat_interval_seconds: float = 10.0
    shutdown_grace_seconds: float = 20.0
    retry_base_delay_seconds: float = 2.0
    retry_max_delay_seconds: float = 300.0
    worker_metrics_port: int = 9101

    # Relay
    relay_poll_interval_seconds: float = 0.05
    relay_batch_size: int = 200
    relay_metrics_port: int = 9102

    # Reaper
    reaper_interval_seconds: float = 2.0
    reaper_batch_size: int = 200
    reaper_metrics_port: int = 9103
    stats_interval_seconds: float = 5.0
    outbox_retention_seconds: float = 3600.0
    outbox_prune_batch: int = 5000

    # Messaging
    exchange_name: str = "jobs"
    queue_name: str = "jobs.work"
    dlx_name: str = "jobs.dlx"
    dlq_name: str = "jobs.dlq"
    max_priority: int = 9
    dlq_message_ttl_ms: int = 7 * 24 * 3600 * 1000

    # Observability
    log_level: str = "INFO"
    log_json: bool = True
    service_name: str = "jobplatform"


@lru_cache
def get_settings() -> Settings:
    return Settings()
