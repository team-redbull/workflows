"""Shared custom errors for the cluster orchestrator.

These live in the contract layer so both workflows and activities can reference
them by type (e.g. to mark certain failures non-retryable in a RetryPolicy —
the Temporal SDK converts activity-raised exceptions to ApplicationError with
`type` set to the class name).

TWO KINDS live here, and the difference is which side may RAISE them:

  * ACTIVITY-raised — raised as ordinary exceptions from activities/<domain>/.
    The SDK converts them, and the permanent ones are listed in a workflow's
    `non_retryable_error_types` BY `__name__` rather than as a string literal:
    that list is matched against the type name, so a typo in it means "retry
    forever" with nothing to notice, while a wrong attribute is an ImportError
    at worker startup.
  * WORKFLOW-raised — NEVER raised as an exception. A non-FailureError raised
    in workflow code fails the workflow TASK, which retries forever and leaves
    the run hanging RUNNING, so workflow code raises
    `ApplicationError(..., type=ThatError.__name__)`. The class exists to own
    the type name and to document the failure in one place; listing it in
    `non_retryable_error_types` would be inert, because workflow failures are
    never retried. Each is marked WORKFLOW-RAISED below.

The segment-lifecycle workflows still spell their own workflow-raised types as
literals (`UnsupportedSegmentType`, `DhcpScopeNotConverged`, ...). Those have no
class yet; the rule above is what a new one follows.
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
    """WORKFLOW-RAISED. No candidate offered two usable NICs on distinct ports.

    Deterministic for the candidates drawn: the interfaces a server reports do
    not change between retries of the same run. The message names each
    candidate's provider and the link states observed, because the most common
    cause is structural rather than a fault — HPE OneView reports no link state
    at all and Intersight vNICs usually report none, so servers from those
    collectors cannot satisfy a strict link-up rule.
    """


class UnknownBmcVendorError(OrchestratorError):
    """WORKFLOW-RAISED. server-scan reported no BMC driver vocabulary for this server.

    `bmc_vendor` is null for a STANDALONE machine — server-scan leaves the
    driver choice to the caller by design — and install-server declines to
    guess, because an IPMI fallback would be silently wrong for a Redfish-only
    BMC. Deterministic — non-retryable.
    """


class BmcEndpointMissingError(OrchestratorError):
    """WORKFLOW-RAISED. server-scan reported no BMC host for this server.

    Distinct from UnknownBmcVendorError: the driver is known, the ADDRESS is
    not. Checked before anything is written, because an empty host still builds
    a syntactically valid address (`redfish-virtualmedia:///redfish/v1/Systems/1`)
    that the API server happily stores — so without this the run creates all
    three resources, spends the whole 10-minute registration deadline, and then
    blames the BMC credentials for a BMC address that was never collected.
    Deterministic: a collector run fills this in, not a retry.
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
    """A resource of this name already exists and does NOT match what we would create.

    An existing resource is normally success — the creates are idempotent, so a
    re-run converges rather than failing. This is the case that convergence
    cannot cover: the BareMetalHost or NMStateConfig already on the cluster
    names a different BMC, boot MAC, InfraEnv or VLAN, so returning success
    would report an installation that does not match what is actually there.
    A human decides whether the existing resource or the request is right.
    Deterministic — non-retryable.
    """


class BmhPrerequisiteMissingError(OrchestratorError):
    """The target cluster cannot accept the write at all (404 or 403).

    404 on a namespaced create means the resource TYPE or the NAMESPACE is
    absent — Metal3 / the Assisted Installer is not installed, or the InfraEnv's
    namespace does not exist. 403 means this worker's ServiceAccount lacks RBAC
    for the resource. Both are deployment gaps that no retry closes, and are
    classified exactly as SegmentsManagerAuthError already is.
    """


class BmhRequestInvalidError(OrchestratorError):
    """The API server rejected the resource body as invalid (422).

    Reached through a caller-supplied label that is not a valid label value, or
    a field the installed CRD version does not accept. The body is built the
    same way on every attempt, so retrying reproduces it exactly.
    """


class InvalidMacError(OrchestratorError):
    """A MAC selected for the bond is not a MAC.

    server-scan normalizes MACs before persisting them, so this guards against
    a malformed payload rather than an expected condition — but it is checked
    in the workflow BEFORE the first resource is written, so a bad payload
    costs nothing rather than leaving a Secret behind.
    """


class BmhNotRegisteredError(OrchestratorError):
    """WORKFLOW-RAISED. The BareMetalHost never reached a registered state.

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


class AmbiguousInventorySegmentError(OrchestratorError):
    """More than one INVENTORY segment is allocated to the same MCE cluster.

    Distinct from InventorySegmentNotFoundError: the data is wrong rather than
    missing, and picking one of the two would tag hosts onto a VLAN chosen by
    document order. A human resolves the duplicate allocation — non-retryable.
    """


class InventorySegmentMismatchError(OrchestratorError):
    """WORKFLOW-RAISED. The segment read back is allocated to another cluster.

    get_inventory_segment matches the cluster client-side, because the Segments
    Manager's list endpoint has no `cluster_name` filter. The workflow re-checks
    it anyway: this is the one value that decides which VLAN a host is tagged
    onto, and accepting another cluster's segment would bring the host up on a
    network the target MCE cannot reach. Deterministic — the answer is the same
    on every attempt.
    """


class InvalidServerNameError(OrchestratorError):
    """A server-scan name cannot be a Kubernetes resource name.

    Lowercasing is applied first, since vendor serials are upper case; what
    remains is a name carrying characters Kubernetes forbids. Deterministic —
    the name is renamed in the inventory, not waited out.
    """
