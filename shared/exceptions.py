"""Shared custom errors for the cluster orchestrator.

These live in the contract layer so both workflows and activities can reference
them by type (e.g. to mark certain failures non-retryable in a RetryPolicy).
"""


class OrchestratorError(Exception):
    """Base class for all orchestrator domain errors."""


class DeploymentApiError(OrchestratorError):
    """The deployments API returned an unexpected status or malformed payload."""


class GitCommitError(OrchestratorError):
    """Committing the allocated segment to the GitOps repository failed."""
