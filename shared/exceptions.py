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
    """The Segments Manager has no Available segment at the site (its allocate
    endpoint answered 503) — the pool is shared by every type.

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
    """A git operation against the values repo failed (ls-remote, clone,
    commit, push — including a rejected non-fast-forward push).

    Transient by classification: the retry re-clones from a temporary
    directory and re-applies the append, so a concurrent push simply converges
    on the next attempt. Deliberately NOT in non_retryable_error_types. A
    branch that does not exist is NOT this error — see ValuesBranchNotFoundError.
    """


class ValuesBranchNotFoundError(OrchestratorError):
    """The values-repo branch the run names (`values_branch`) does not exist on
    the remote: `git ls-remote --exit-code --heads` found no
    `refs/heads/<branch>`.

    Deterministic — the caller (the day1 pipeline) passes the branch it runs
    on, so a missing one is a typo or a branch deleted after the trigger, and
    retrying cannot make it appear. Workflows list this type in
    non_retryable_error_types. Any OTHER ls-remote failure (auth, DNS, a
    timeout) stays ValuesRepoGitError and is retried.
    """


# --- server-lifecycle -------------------------------------------------------


class ServerScanError(OrchestratorError):
    """server-scan's inventory API failed or returned a malformed payload.

    Transient by classification — retried by the activity RetryPolicy. "No
    server matched" is NOT this error: that is ServerNotAvailableError below.
    """


class ServerScanAuthError(OrchestratorError):
    """server-scan rejected our credentials (401/403).

    Deterministic — a bad SERVER_SCAN_API_TOKEN never fixes itself, so the
    workflow lists this in non_retryable_error_types. Note the token only needs
    server-scan's VIEWER role: /servers/available is a GET.
    """


class ServerNotAvailableError(OrchestratorError):
    """server-scan has no assignable server matching the request (404).

    Deterministic for this run: the pool is refilled by hardware being freed or
    by a collector run, not by retrying every minute forever. Its message
    carries server-scan's own `detail`, which distinguishes "nothing matched
    the pattern" from "everything matching is CRITICAL/claimed/unreachable".
    """


class AmbiguousServerNameError(OrchestratorError):
    """More than one server-scan document carries the requested name (409).

    Server names are not unique — correlation is on (vendor, serial), so one
    hostname can legitimately span several documents. Only a human can say
    which machine was meant. Deterministic — non-retryable.
    """


class NoBondableInterfacesError(OrchestratorError):
    """No candidate offered two link-up NICs on two distinct physical ports.

    Deterministic for the candidates drawn: the interfaces a server reports do
    not change between retries of the same run. The message names each
    candidate's provider and the link states observed, because the most common
    cause is structural rather than a fault — HPE OneView reports no link state
    at all and Intersight vNICs usually report none, so servers from those
    collectors cannot satisfy a strict link-up rule.
    """


class UnknownBmcVendorError(OrchestratorError):
    """server-scan reported no BMC driver vocabulary for this server.

    `bmc_vendor` is null for a STANDALONE machine — server-scan leaves the
    driver choice to the caller by design — and install-server declines to
    guess, because an IPMI fallback would be silently wrong for a Redfish-only
    BMC. Deterministic — non-retryable.
    """


class BmcCredentialsMissingError(OrchestratorError):
    """No BMC username/password is configured for this server's vendor.

    server-scan never holds BMC credentials; they come from this worker's own
    Secret. A missing pair is a deployment gap, not a transient fault —
    non-retryable.
    """


class BmhResourceError(OrchestratorError):
    """A Kubernetes create/read against the target cluster failed.

    Transient by classification — API-server blips, throttling and rollouts all
    recover. An ALREADY EXISTS (409) is never this error: creates are
    idempotent and treat it as success.
    """


class BmhConflictError(OrchestratorError):
    """A required CRD is absent from the target cluster (404 on the resource type).

    Metal3's BareMetalHost or the Assisted Installer's NMStateConfig is not
    installed, so nothing this workflow writes can ever take effect.
    Deterministic — non-retryable.
    """


class BmhNotRegisteredError(OrchestratorError):
    """The BareMetalHost did not reach a registered state before the deadline.

    Machine convergence with a real deadline: storing the object only means the
    API server accepted it, while Ironic still has to reach the BMC. A wrong
    BMC address or credential surfaces only here. The three resources are left
    in place — they are what an operator needs to diagnose it.
    """


class InventorySegmentNotFoundError(OrchestratorError):
    """The MCE cluster has no INVENTORY segment allocated in the Segments Manager.

    The VLAN a server's inventory network uses belongs to the MCE, so without
    that allocation there is no VLAN to tag and the run cannot proceed.
    Deterministic — an operator allocates the segment, retrying does not.
    """
