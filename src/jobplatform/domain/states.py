"""Job state machine.

The transition table is the single source of truth. Every database transition is a
conditional UPDATE guarded by the expected current status, so a transition that is
not in this table can never be applied, even under concurrent writers.
"""

from __future__ import annotations

from enum import StrEnum


class JobStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    RETRY_SCHEDULED = "retry_scheduled"
    DEAD = "dead"
    CANCELLED = "cancelled"


TERMINAL_STATES: frozenset[JobStatus] = frozenset(
    {JobStatus.SUCCEEDED, JobStatus.DEAD, JobStatus.CANCELLED}
)

# Allowed transitions. DEAD -> QUEUED is the operator-initiated DLQ requeue.
TRANSITIONS: dict[JobStatus, frozenset[JobStatus]] = {
    JobStatus.QUEUED: frozenset({JobStatus.RUNNING, JobStatus.CANCELLED}),
    JobStatus.RUNNING: frozenset(
        {
            JobStatus.SUCCEEDED,
            JobStatus.RETRY_SCHEDULED,
            JobStatus.DEAD,
            JobStatus.CANCELLED,
            JobStatus.QUEUED,  # lease reclaimed by the reaper, or released on shutdown
        }
    ),
    JobStatus.RETRY_SCHEDULED: frozenset({JobStatus.QUEUED, JobStatus.CANCELLED}),
    JobStatus.SUCCEEDED: frozenset(),
    JobStatus.DEAD: frozenset({JobStatus.QUEUED}),
    JobStatus.CANCELLED: frozenset(),
}

# States from which a cancel request is honoured.
CANCELLABLE: frozenset[JobStatus] = frozenset(
    {JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.RETRY_SCHEDULED}
)


class IllegalTransitionError(Exception):
    def __init__(self, source: JobStatus, target: JobStatus) -> None:
        super().__init__(f"illegal job transition {source.value} -> {target.value}")
        self.source = source
        self.target = target


def can_transition(source: JobStatus, target: JobStatus) -> bool:
    return target in TRANSITIONS[source]


def assert_transition(source: JobStatus, target: JobStatus) -> None:
    if not can_transition(source, target):
        raise IllegalTransitionError(source, target)


def is_terminal(status: JobStatus) -> bool:
    return status in TERMINAL_STATES
