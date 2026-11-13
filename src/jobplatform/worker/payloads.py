"""Payload schemas per job type. The API validates against these at submit time."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class _Payload(BaseModel):
    model_config = ConfigDict(extra="forbid")


class IoTaskPayload(_Payload):
    """Simulated long-running I/O call (sleeps for duration_ms)."""

    duration_ms: int = Field(default=100, ge=0, le=3_600_000)


class CpuReportPayload(_Payload):
    """CPU-bound report: aggregates `rows` synthetic records derived from `seed`."""

    rows: int = Field(default=200_000, ge=1, le=20_000_000)
    seed: int = Field(default=1, ge=0)


class FlakyPayload(_Payload):
    """Fails on the first `fail_times` attempts, then succeeds."""

    fail_times: int = Field(default=2, ge=0, le=100)
    error: Literal["retryable", "fatal"] = "retryable"
    duration_ms: int = Field(default=0, ge=0, le=60_000)


class SideEffectPayload(_Payload):
    """Records one idempotent side effect row for the job, then optionally sleeps."""

    effect: str = Field(default="default", min_length=1, max_length=200)
    duration_ms: int = Field(default=0, ge=0, le=600_000)
