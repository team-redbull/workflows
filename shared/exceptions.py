"""Shared custom errors for the cluster orchestrator.

These live in the contract layer so both workflows and activities can reference
them by type (e.g. to mark certain failures non-retryable in a RetryPolicy —
the Temporal SDK converts activity-raised exceptions to ApplicationError with
`type` set to the class name).
"""


class OrchestratorError(Exception):
    """Base class for all orchestrator domain errors."""


class SegmentsManagerError(OrchestratorError):
    """The team's Segments Manager API returned an unexpected error."""


class SegmentNotFoundError(OrchestratorError):
    """The requested segment does not exist in the Segments Manager.

    Deterministic — retrying cannot fix a missing segment, so workflows list
    this type in non_retryable_error_types.
    """


class NextApiError(OrchestratorError):
    """The next (connectivity) service failed or returned a malformed payload.

    Strictly for problems with the next service itself — transient, retried by
    the activity RetryPolicy. Configuration problems (e.g. port policy) are NOT
    this error: they fail the worker at startup instead.
    """
