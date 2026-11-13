import pytest
from hypothesis import given
from hypothesis import strategies as st

from jobplatform.domain.backoff import backoff_ceiling, full_jitter_delay


def test_ceiling_doubles_per_attempt() -> None:
    assert [backoff_ceiling(a, base=2, cap=1000) for a in range(1, 6)] == [2, 4, 8, 16, 32]


def test_ceiling_is_capped() -> None:
    assert backoff_ceiling(10, base=2, cap=60) == 60
    assert backoff_ceiling(1000, base=2, cap=60) == 60  # no overflow for absurd attempt counts


def test_full_jitter_spans_zero_to_ceiling() -> None:
    assert full_jitter_delay(3, 2, 100, rng=lambda: 0.0) == 0.0
    assert full_jitter_delay(3, 2, 100, rng=lambda: 0.5) == 4.0  # ceiling is 8
    assert full_jitter_delay(3, 2, 100, rng=lambda: 1.0) == 8.0


@pytest.mark.parametrize(("attempt", "base", "cap"), [(0, 1, 1), (1, 0, 1), (1, 1, 0), (-1, 1, 1)])
def test_invalid_inputs_are_rejected(attempt: int, base: float, cap: float) -> None:
    with pytest.raises(ValueError):
        backoff_ceiling(attempt, base, cap)


@given(
    attempt=st.integers(min_value=1, max_value=10_000),
    base=st.floats(min_value=0.001, max_value=60),
    cap=st.floats(min_value=0.001, max_value=3600),
)
def test_delay_is_always_within_bounds(attempt: int, base: float, cap: float) -> None:
    delay = full_jitter_delay(attempt, base, cap)
    assert 0 <= delay <= cap
    assert delay <= backoff_ceiling(attempt, base, cap)


@given(base=st.floats(min_value=0.01, max_value=10), cap=st.floats(min_value=10, max_value=500))
def test_ceiling_is_monotonic_in_attempt(base: float, cap: float) -> None:
    ceilings = [backoff_ceiling(a, base, cap) for a in range(1, 30)]
    assert ceilings == sorted(ceilings)


def test_jitter_actually_varies() -> None:
    samples = {round(full_jitter_delay(5, 1, 100), 6) for _ in range(200)}
    assert len(samples) > 100
