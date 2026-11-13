import itertools

import pytest
from hypothesis import given
from hypothesis import strategies as st

from jobplatform.domain.states import (
    CANCELLABLE,
    TERMINAL_STATES,
    TRANSITIONS,
    IllegalTransitionError,
    JobStatus,
    assert_transition,
    can_transition,
    is_terminal,
)

ALL = list(JobStatus)


def test_every_state_has_an_entry_in_the_table() -> None:
    assert set(TRANSITIONS) == set(JobStatus)


def test_declared_lifecycle_edges() -> None:
    expected = {
        (JobStatus.QUEUED, JobStatus.RUNNING),
        (JobStatus.QUEUED, JobStatus.CANCELLED),
        (JobStatus.RUNNING, JobStatus.SUCCEEDED),
        (JobStatus.RUNNING, JobStatus.RETRY_SCHEDULED),
        (JobStatus.RUNNING, JobStatus.DEAD),
        (JobStatus.RUNNING, JobStatus.CANCELLED),
        (JobStatus.RUNNING, JobStatus.QUEUED),
        (JobStatus.RETRY_SCHEDULED, JobStatus.QUEUED),
        (JobStatus.RETRY_SCHEDULED, JobStatus.CANCELLED),
        (JobStatus.DEAD, JobStatus.QUEUED),
    }
    actual = {(s, t) for s, targets in TRANSITIONS.items() for t in targets}
    assert actual == expected


@pytest.mark.parametrize(("source", "target"), list(itertools.product(ALL, ALL)))
def test_assert_transition_matches_the_table(source: JobStatus, target: JobStatus) -> None:
    if target in TRANSITIONS[source]:
        assert_transition(source, target)
        assert can_transition(source, target)
    else:
        assert not can_transition(source, target)
        with pytest.raises(IllegalTransitionError) as err:
            assert_transition(source, target)
        assert err.value.source is source and err.value.target is target


def test_succeeded_and_cancelled_are_final() -> None:
    assert TRANSITIONS[JobStatus.SUCCEEDED] == frozenset()
    assert TRANSITIONS[JobStatus.CANCELLED] == frozenset()


def test_dead_can_only_be_requeued() -> None:
    assert TRANSITIONS[JobStatus.DEAD] == {JobStatus.QUEUED}


def test_terminal_and_cancellable_sets() -> None:
    assert {JobStatus.SUCCEEDED, JobStatus.DEAD, JobStatus.CANCELLED} == TERMINAL_STATES
    assert all(is_terminal(s) for s in TERMINAL_STATES)
    assert not any(is_terminal(s) for s in set(JobStatus) - TERMINAL_STATES)
    assert CANCELLABLE.isdisjoint(TERMINAL_STATES)
    for status in CANCELLABLE:
        assert can_transition(status, JobStatus.CANCELLED)


@given(st.lists(st.sampled_from(ALL), min_size=1, max_size=40))
def test_any_walk_that_follows_can_transition_stays_inside_the_table(path: list[JobStatus]) -> None:
    """Random walks: accepting a step only when allowed never reaches an impossible edge."""
    current = JobStatus.QUEUED
    for nxt in path:
        if can_transition(current, nxt):
            assert nxt in TRANSITIONS[current]
            current = nxt
        else:
            with pytest.raises(IllegalTransitionError):
                assert_transition(current, nxt)
    assert current in JobStatus
