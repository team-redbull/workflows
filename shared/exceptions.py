"""Shared custom errors for the cluster orchestrator.

These live in the contract layer so both workflows and activities can reference
them by type (e.g. to mark certain failures non-retryable in a RetryPolicy).
"""


class OrchestratorError(Exception):
    """Base class for all orchestrator domain errors."""


class NoSegmentAvailableError(OrchestratorError):
    """No segment could be allocated for the cluster at the requested site.

    Raised when the Segments Manager has no available segment to assign and
    (after attempting generation) allocation still cannot succeed.
    """


class SegmentGeneratorError(OrchestratorError):
    """The external segment generator (IPAM) API failed or returned bad data."""


class SegmentManagerError(OrchestratorError):
    """The team's Segments Manager API returned an unexpected error."""
