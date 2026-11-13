import pytest
from pydantic import ValidationError

from jobplatform.config import Settings
from jobplatform.domain.errors import JobCancelledError, NonRetryableError, RetryableError
from jobplatform.worker.handlers.cpu import cpu_report
from jobplatform.worker.registry import build_registry


class _Never:
    def check(self) -> None:
        return None


class _Cancelled:
    def check(self) -> None:
        raise JobCancelledError("cancelled")


def test_registry_has_the_four_example_types() -> None:
    registry = build_registry(Settings())
    assert set(registry) == {"io_task", "cpu_report", "flaky", "side_effect"}
    assert registry["cpu_report"].cpu_bound and not registry["io_task"].cpu_bound


def test_payload_defaults_are_applied_and_unknown_fields_rejected() -> None:
    spec = build_registry(Settings())["io_task"]
    assert spec.validate_payload({}) == {"duration_ms": 100}
    with pytest.raises(ValidationError):
        spec.validate_payload({"duration_ms": 10, "surprise": 1})
    with pytest.raises(ValidationError):
        spec.validate_payload({"duration_ms": -1})


def test_retry_classification() -> None:
    spec = build_registry(Settings())["flaky"]
    assert spec.is_retryable(RetryableError("x"))
    assert spec.is_retryable(TimeoutError())
    assert spec.is_retryable(RuntimeError("unknown errors are retried"))
    assert not spec.is_retryable(NonRetryableError("x"))


def test_retry_delays_come_from_settings() -> None:
    spec = build_registry(Settings(retry_base_delay_seconds=7, retry_max_delay_seconds=70))[
        "io_task"
    ]
    assert (spec.base_delay_seconds, spec.max_delay_seconds) == (7, 70)


def test_cpu_report_is_deterministic_and_seed_sensitive() -> None:
    a = cpu_report({"rows": 60_000, "seed": 3}, 1, _Never())
    b = cpu_report({"rows": 60_000, "seed": 3}, 1, _Never())
    c = cpu_report({"rows": 60_000, "seed": 4}, 1, _Never())
    assert a == b
    assert a["checksum"] != c["checksum"]
    assert a["rows"] == 60_000 and a["min"] <= a["mean"] <= a["max"]


def test_cpu_report_stops_when_cancelled() -> None:
    with pytest.raises(JobCancelledError):
        cpu_report({"rows": 1_000_000, "seed": 1}, 1, _Cancelled())
