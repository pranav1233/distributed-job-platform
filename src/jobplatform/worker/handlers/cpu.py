"""CPU-bound handlers. These run in a separate process, so they are plain functions."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from typing import Any, Protocol

from jobplatform.worker.payloads import CpuReportPayload

_CHUNK = 50_000
_MODULUS = 2**31 - 1


class CancelCheck(Protocol):
    def check(self) -> None: ...


def cpu_report(payload: dict[str, Any], attempt: int, cancel: CancelCheck) -> dict[str, Any]:
    """Aggregate `rows` synthetic records using a linear congruential generator.

    Cancellation is checked once per chunk, which bounds cancel latency to one chunk.
    """
    params = CpuReportPayload.model_validate(payload)
    state = (params.seed % _MODULUS) or 1
    digest = hashlib.sha256()
    total = 0
    low = _MODULUS
    high = 0
    done = 0
    while done < params.rows:
        cancel.check()
        chunk = min(_CHUNK, params.rows - done)
        block = 0
        for _ in range(chunk):
            state = (state * 48271) % _MODULUS
            total += state
            block ^= state
            low = min(low, state)
            high = max(high, state)
        digest.update(block.to_bytes(8, "big"))
        done += chunk
    return {
        "rows": params.rows,
        "mean": total / params.rows,
        "min": low,
        "max": high,
        "checksum": digest.hexdigest(),
    }


CPU_HANDLERS: dict[str, Callable[[dict[str, Any], int, Any], dict[str, Any]]] = {
    "cpu_report": cpu_report,
}
