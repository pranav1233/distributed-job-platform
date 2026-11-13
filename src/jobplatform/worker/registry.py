"""Job type registry: handler, retry policy, timeout and payload schema per type."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ValidationError

from jobplatform.config import Settings
from jobplatform.domain.errors import NonRetryableError
from jobplatform.worker.handlers.base import AsyncHandler
from jobplatform.worker.handlers.flaky import handle_flaky
from jobplatform.worker.handlers.io_task import handle_io_task
from jobplatform.worker.handlers.side_effect import handle_side_effect
from jobplatform.worker.payloads import (
    CpuReportPayload,
    FlakyPayload,
    IoTaskPayload,
    SideEffectPayload,
)


@dataclass(frozen=True, slots=True)
class JobTypeSpec:
    name: str
    payload_model: type[BaseModel]
    max_attempts: int
    timeout_seconds: float
    base_delay_seconds: float
    max_delay_seconds: float
    handler: AsyncHandler | None = None  # coroutine handler, runs on the event loop
    cpu_handler: str | None = None  # name of a function run in the process pool
    # Exceptions in this tuple skip retries and send the job straight to dead.
    non_retryable: tuple[type[BaseException], ...] = (NonRetryableError, ValidationError)

    @property
    def cpu_bound(self) -> bool:
        return self.cpu_handler is not None

    def is_retryable(self, exc: BaseException) -> bool:
        return not isinstance(exc, self.non_retryable)

    def validate_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Validate and normalise a submitted payload (applies defaults)."""
        return self.payload_model.model_validate(payload).model_dump()


def build_registry(settings: Settings) -> dict[str, JobTypeSpec]:
    base = settings.retry_base_delay_seconds
    cap = settings.retry_max_delay_seconds
    specs = [
        JobTypeSpec(
            name="io_task",
            payload_model=IoTaskPayload,
            max_attempts=5,
            timeout_seconds=3600,
            base_delay_seconds=base,
            max_delay_seconds=cap,
            handler=handle_io_task,
        ),
        JobTypeSpec(
            name="cpu_report",
            payload_model=CpuReportPayload,
            max_attempts=3,
            timeout_seconds=600,
            base_delay_seconds=base,
            max_delay_seconds=cap,
            cpu_handler="cpu_report",
        ),
        JobTypeSpec(
            name="flaky",
            payload_model=FlakyPayload,
            max_attempts=5,
            timeout_seconds=60,
            base_delay_seconds=base,
            max_delay_seconds=cap,
            handler=handle_flaky,
        ),
        JobTypeSpec(
            name="side_effect",
            payload_model=SideEffectPayload,
            max_attempts=10,
            timeout_seconds=600,
            base_delay_seconds=base,
            max_delay_seconds=cap,
            handler=handle_side_effect,
        ),
    ]
    return {s.name: s for s in specs}
