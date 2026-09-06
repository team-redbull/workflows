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


class SegmentsManagerAuthError(OrchestratorError):
    """The Segments Manager rejected our credentials (401/403).

    Deterministic — a bad SEGMENTS_MANAGER_API_TOKEN never fixes itself, so
    workflows list this type in non_retryable_error_types (with unbounded
    retries elsewhere, an unclassified auth error would retry forever).
    """


class SegmentNotFoundError(OrchestratorError):
    """The requested segment does not exist in the Segments Manager.

    Deterministic — retrying cannot fix a missing segment, so workflows list
    this type in non_retryable_error_types.
    """


class SegmentValidationError(OrchestratorError):
    """The Segments Manager rejected the segment definition we asked it to
    create (unknown site, CIDR outside the site prefix, overlap with an
    existing segment, VLAN already taken at that site, ...).

    Deterministic — the same definition will be rejected on every retry, so
    workflows list this type in non_retryable_error_types. The Segments
    Manager's own message is carried through verbatim: it is the validator of
    record, so its wording is what the operator needs to fix the input.
    """


class SegmentConflictError(OrchestratorError):
    """The segment's CIDR already exists in the Segments Manager, but with
    different attributes than the ones we were asked to create it with.

    Distinct from a retried-but-already-applied create (identical attributes,
    which create_segment treats as success): here the caller's definition and
    the stored one genuinely disagree, and only a human can decide which is
    right. Deterministic — non-retryable.
    """


class SegmentConversionConflictError(OrchestratorError):
    """The Segments Manager refused to convert the segment's type (409):
    either the segment is Allocated (in use — never converted), or its stored
    type matches neither the requested new type nor the expected_type
    compare-and-set guard (a concurrent conversion re-typed it first).

    Deterministic — only an operator releasing the segment or re-running the
    conversion resolves it, so workflows list this type in
    non_retryable_error_types. Completed conversions from the same run stand:
    a re-run's search no longer matches them, so it picks up where this one
    stopped.
    """


class NextApiError(OrchestratorError):
    """The next (connectivity) service failed or returned a malformed payload.

    Strictly for problems with the next service itself — transient, retried by
    the activity RetryPolicy. Configuration problems (e.g. port policy) are NOT
    this error: they fail the worker at startup instead.
    """


class BmcSegmentNotConfiguredError(OrchestratorError):
    """The site has no BMC networks configured (SITE_NETWORKS).

    A site carries one BMC network per hardware vendor it hosts (`dell-bmc`,
    `cisco-bmc`) and at least one is required, so this means the whole site
    entry is absent. A site present with only ONE vendor is legitimate
    topology, not this error — it opens rules against that vendor alone. A
    site present with NEITHER (or with a misspelt key) never reaches here: it
    fails the worker at startup instead.

    Deterministic — a missing ConfigMap entry never fixes itself, so workflows
    list this type in non_retryable_error_types.
    """


class ClusterFileNotFoundError(OrchestratorError):
    """No `<cluster>.yaml` exists anywhere under the values repo's clusters
    root — the cluster the caller asked to allocate a segment for has no
    values file to write the allocation into.

    Deterministic — the file has to be created by whoever defines the cluster,
    so workflows list this type in non_retryable_error_types.
    """


class AmbiguousClusterFileError(OrchestratorError):
    """More than one `<cluster>.yaml` exists under the values repo's clusters
    root. Cluster file names are the identity the whole day1 stack keys on
    (Argo Application names, DHCP scope names), so a duplicate is a repo
    mistake a human must resolve — never something to guess about.

    Deterministic — non-retryable.
    """


class SegmentPoolExhaustedError(OrchestratorError):
    """The Segments Manager has no Available segment of the requested type at
    the site (its allocate endpoint answered 503).

    Deterministic in practice: a drained pool is refilled by an operator
    creating segments, not by retrying every minute forever — so this fails
    the run loudly instead of sitting RUNNING until someone notices.
    """


class ClusterValuesConflictError(OrchestratorError):
    """The cluster's values file already carries an allocation that disagrees
    with this one — a marker block with different values, or a `dhcp_values` /
    top-level `vlanId` key written by something other than this workflow.

    Appending anyway would create a duplicate top-level YAML key, which
    silently discards the first block (taking scopeName/pxe/gateway/failover
    with it). Deterministic — only a human can decide which allocation is
    right, so non-retryable.
    """


class ValuesRepoGitError(OrchestratorError):
    """A git operation against the values repo failed (clone, commit, push —
    including a rejected non-fast-forward push).

    Transient by classification: the retry re-clones from a temporary
    directory and re-applies the append, so a concurrent push simply converges
    on the next attempt. Deliberately NOT in non_retryable_error_types.
    """


class DhcpApiError(OrchestratorError):
    """The DHCP scope API failed or returned a malformed payload.

    Transient — retried by the activity RetryPolicy. A scope that does not
    exist yet is NOT this error: get_dhcp_scope reports that as a normal
    "not found yet" result for the workflow's bounded convergence poll.
    """
