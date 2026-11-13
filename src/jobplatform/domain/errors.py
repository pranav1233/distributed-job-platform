"""Exception types used to classify job failures."""

from __future__ import annotations


class JobError(Exception):
    """Base class for handler errors."""


class RetryableError(JobError):
    """A transient failure. The job is retried with backoff until attempts run out."""


class NonRetryableError(JobError):
    """A permanent failure. The job goes straight to the dead state."""


class JobCancelledError(JobError):
    """Raised inside a handler when cooperative cancellation is observed."""
