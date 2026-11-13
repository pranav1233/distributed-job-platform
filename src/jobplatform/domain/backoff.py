"""Retry backoff: exponential growth with full jitter."""

from __future__ import annotations

import random
from collections.abc import Callable


def backoff_ceiling(attempt: int, base: float, cap: float) -> float:
    """Upper bound of the delay window for the given failed attempt (1-based)."""
    if attempt < 1:
        raise ValueError("attempt is 1-based")
    if base <= 0 or cap <= 0:
        raise ValueError("base and cap must be positive")
    # min() before the shift keeps huge attempt numbers from building giant floats
    exponent = min(attempt - 1, 62)
    return float(min(cap, base * (2**exponent)))


def full_jitter_delay(
    attempt: int,
    base: float,
    cap: float,
    rng: Callable[[], float] = random.random,
) -> float:
    """Full jitter: uniform in [0, min(cap, base * 2^(attempt-1))].

    Spreading retries over the whole window avoids synchronized retry storms after a
    shared dependency recovers.
    """
    return backoff_ceiling(attempt, base, cap) * rng()
