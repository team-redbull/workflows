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

initialize-segment and allocate-segment still spell their own workflow-raised
types as literals (`UnknownSite`, `AllocationNotConfirmed`, ...). Those have no
class yet; the rule above is what a new one follows — release-segment does, and
`UnsupportedSegmentType`, which it shares with allocate-segment, now has one.
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


# --- segment-lifecycle: release-segment ------------------------------------


class AmbiguousAllocationError(OrchestratorError):
    """The Segments Manager holds MORE THAN ONE Allocated segment of one type
    for one cluster. Its own invariant is one allocation per (cluster, site,
    type), so this is a data error in the system of record — releasing
    either segment would be a guess about which one the cluster really uses.

    Deterministic — only a human can decide, so workflows list this type in
    non_retryable_error_types (precedent: AmbiguousClusterFileError).
    """


class DhcpApiError(OrchestratorError):
    """The DHCP scope API (dhcp_scope_manager) failed or returned a malformed
    payload: a network error, a 5xx, a 503 "no DHCP backend configured", a 504
    from its PowerShell layer.

    Transient — retried. Deliberately NOT in non_retryable_error_types: an
    outage of the API or of the Windows DHCP server behind it is out-waited.
    A scope that does not exist is NOT this error — it is a normal
    DhcpScopeState(found=False).
    """


class DhcpApiAuthError(OrchestratorError):
    """The DHCP scope API rejected our bearer token (401/403) on a write.

    Deterministic — a wrong or rotated DHCP_API_TOKEN never fixes itself (the
    worker's copy has to be updated with the API's own token), so workflows
    list this type in non_retryable_error_types.
    """


class DhcpScopeInvalidError(OrchestratorError):
    """The DHCP scope API answered 400 INVALID_SCOPE: the network address we
    derived from the Segments Manager's CIDR is not an IPv4 address it
    accepts.

    Deterministic — the same address is rejected on every retry, and the
    cause is bad data or a bug, not an outage. Non-retryable.
    """


class UnsupportedSegmentType(OrchestratorError):
    """WORKFLOW-RAISED. The run was asked for a segment type it does not handle.

    allocate-segment and release-segment both take HC only (allocate-segment
    allocates nothing else, so no other allocation was made by this system).
    Named without the Error suffix because it owns a type name that predates
    the class: allocate-segment still spells it as a literal, and both
    workflows must fail under the SAME name for the same condition.
    """


class DhcpScopeStillPresentError(OrchestratorError):
    """WORKFLOW-RAISED. release-segment deleted a cluster's DHCP scope and read
    it straight back as present.

    Something re-created it — almost certainly a Crossplane Request for the
    cluster that still exists, i.e. its Argo CD Application is not gone yet and
    the run was triggered too early. The segment is NOT released, so it can
    never be handed out while a scope still serves leases on it.
    """


class AllocationOwnerMismatchError(OrchestratorError):
    """WORKFLOW-RAISED. The segment release-segment located for a cluster is
    held by ANOTHER cluster by the time the run re-reads it before releasing.

    It was released and re-allocated in between. The Segments Manager's
    release checks no owner, so this re-read is the only guard; nothing is
    released.
    """


class ReleaseNotConfirmedError(OrchestratorError):
    """WORKFLOW-RAISED. After the release, the segment did not read back as
    Available with no cluster and no type."""


# --- server-lifecycle -------------------------------------------------------


class ServerScanError(OrchestratorError):
    """server-scan's inventory API failed or returned a malformed payload.

    Transient by classification — retried by the activity RetryPolicy. "No
    server matched" is NOT this error: that is ServerNotAvailableError below.
    """


class ServerScanAuthError(OrchestratorError):
    """server-scan rejected our credentials (401/403).

    Deterministic — a bad SERVER_SCAN_API_TOKEN never fixes itself, so the
    workflow lists this in non_retryable_error_types. install-server's token
    needs server-scan's ADMIN role: the install lock is taken and released
    through two of its mutation endpoints. provision-dell-server only reads, so
    a viewer token is enough there.
    """


class ServerReservedError(OrchestratorError):
    """server-scan refused the install lock: another run holds it (409).

    Deterministic for this candidate and non-retryable — the holder's lock lasts
    until it is released or expires, and waiting it out would stall the run on a
    machine some other MCE is installing. install-server treats it as a SKIP:
    the next candidate is tried. The message names the holder, its MCE and the
    expiry, because that is the run an operator goes and looks at.
    """


class ServerScanRequestInvalidError(OrchestratorError):
    """server-scan rejected a request body as invalid (400/422).

    Built from validated models, so this is a contract drift between the two
    repos — a field renamed or a bound tightened on server-scan's side. No retry
    can fix it.
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


class AgentNeverAppearedError(OrchestratorError):
    """WORKFLOW-RAISED. No candidate the run drew ever registered an Agent.

    The Agent CR is the success signal install-server waits on, and it is the
    only one that proves the whole chain at once: an Agent exists because the
    host booted the discovery ISO and reached assisted-service, which means the
    BMC accepted virtual media, the bond came up, the VLAN was right and DHCP
    answered. Nothing earlier proves any of that — a BareMetalHost Ironic has
    "registered" only means the BMC answered, and a host whose bond or VLAN is
    wrong registers perfectly and is never heard from again.

    This replaced BmhNotRegisteredError, which deadlined on Ironic registration
    instead. That signal was too weak in both directions: it passed hosts that
    would never boot, and on an unreachable BMC Metal3 sits in `registering`
    with operationalStatus OK and no errorType at all — observed for 14 hours
    straight on a mock BMC — so there was nothing to distinguish a slow host
    from a dead one except the deadline itself.

    ONE candidate timing out is a skip, not this: its resources are torn down
    and the next candidate is tried. This is what a run gets when EVERY
    candidate was torn down again for want of an Agent.
    """


class BmhTeardownError(OrchestratorError):
    """A rolled-back candidate's resources are still on the cluster.

    RETRYABLE, and deliberately not classified permanent. The usual cause is
    Metal3 still holding `baremetalhost.metal3.io` while it tries to deprovision
    through a BMC that never answered, which is exactly the state a rollback
    happens in — measured stuck past four minutes on a live cluster, with
    BMAC's own finalizer released inside one second.

    Raised only when the resources remain AFTER the teardown activity has done
    everything it can, finalizer removal included. That distinction matters: the
    server must not be reported back to the inventory while a BareMetalHost
    still points at it, or another MCE will draw the same machine.
    """


class InventorySegmentNotFoundError(OrchestratorError):
    """The MCE cluster has no INVENTORY segment allocated in the Segments Manager.

    The VLAN a server's inventory network uses belongs to the MCE and to nothing
    else — there is ONE inventory scope per cluster, found by cluster name — so
    without that allocation there is no VLAN to tag and no candidate the run
    could draw would help. Raised by the LOOKUP, therefore, and raised before a
    single candidate is considered.

    (While INVENTORY was briefly split per BMC protocol class this was
    workflow-raised instead: a missing class meant "this MCE takes no servers
    driven that way", which had to be a per-candidate skip rather than a failure.
    With one scope there is nothing to skip to.)

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


class PxeSiteNotConfiguredError(OrchestratorError):
    """No PXE map host is configured for the site an IPMI server boots at.

    An IPMI BMC cannot mount virtual media, so a UCS blade boots from the
    network and the site's PXE VM must be told which iPXE script to hand its
    MACs. PXE_MAP_URLS on this MCE's worker has no entry for that site, so
    there is nobody to tell. Deterministic — an operator adds the entry.
    install-server treats it as a SKIP: a Redfish candidate in the same draw
    needs no PXE map and may still be installable.
    """


class InfraEnvNotFoundError(OrchestratorError):
    """The InfraEnv the run fills does not exist in the target namespace.

    Its iPXE script URL is what an IPMI server is pointed at, so without it
    there is nothing to boot. Deterministic — the InfraEnv is created by
    whoever defines it, not by a retry.
    """


class InfraEnvNotReadyError(OrchestratorError):
    """The InfraEnv exists but publishes no iPXE script URL yet.

    assisted-service fills `status.bootArtifacts.ipxeScript` once it has
    generated the discovery image, which follows the InfraEnv's creation by
    seconds to minutes. Transient by classification — retried.
    """


class PxeMapError(OrchestratorError):
    """The site's PXE map service failed or was unreachable. Retried."""


class PxeMapAuthError(OrchestratorError):
    """The PXE map service rejected PXE_MAP_TOKEN (401/403). Deterministic."""


class PxeMapRequestRejectedError(OrchestratorError):
    """The PXE map service refused the request (400/404/422).

    The body is the same on every attempt, so a retry reproduces it. A 404 on
    the PUT path usually means the configured URL points at the wrong service.
    Deterministic — non-retryable.
    """


# -- Server-provisioning domain (provision-dell-server) -----------------------


class IdracError(OrchestratorError):
    """An iDRAC answered unexpectedly or could not be reached mid-call.

    Transient by classification — an iDRAC restarting to apply a job answers
    nothing for minutes — so it is retried.
    """


class IdracAuthError(OrchestratorError):
    """An iDRAC rejected a credential the run had already established works.

    Deterministic: after the probe (or after the template enforced the target
    password) a 401 means someone changed the password underneath the run.
    Retrying would only feed the iDRAC's failed-login counter until it blocks
    this worker's address.
    """


class IdracRequestRejectedError(OrchestratorError):
    """An iDRAC refused a configuration request (400/405/409/422).

    Deterministic — the same body is refused on every attempt. The iDRAC's own
    message is in the error: a controller that does not support the requested
    RAID level, a drive in the wrong state, a pending job on the controller.
    """


class IdracCredentialsMissingError(OrchestratorError):
    """The limb has no root password configured at the position the run named.

    Only possible when IDRAC_FACTORY_PASSWORDS shrinks while a run is in
    flight. Deterministic — the run is restarted against the new config.
    """


class OmeError(OrchestratorError):
    """OpenManage Enterprise answered unexpectedly or was unreachable. Retried."""


class OmeAuthError(OrchestratorError):
    """OME rejected OME_USERNAME/OME_PASSWORD (401/403). Deterministic."""


class OmeRequestRejectedError(OrchestratorError):
    """OME refused a request body (400/404/409/422). Deterministic."""


class TemplateNotConfiguredError(OrchestratorError):
    """DELL_TEMPLATES has no template for this (model, iDRAC firmware).

    Deterministic, and deliberately not defaulted: the template sets the root
    password and the BIOS, and one captured on other firmware can carry
    attributes this iDRAC does not have. An operator adds the entry.
    """


class TemplateNotFoundError(OrchestratorError):
    """DELL_TEMPLATES names a template OME does not hold. Deterministic."""


class ProfileConflictError(OrchestratorError):
    """The device already carries a profile from a DIFFERENT template.

    Deterministic, and never overwritten: a machine someone templated by hand is
    an operator's call to redo, not the workflow's.
    """


class ServerNamerError(OrchestratorError):
    """The naming service answered unexpectedly or was unreachable. Retried."""


class ServerNamerRejectedError(OrchestratorError):
    """The naming service refused the request (4xx). Deterministic."""


class RegionMissingError(OrchestratorError):
    """WORKFLOW-RAISED. A run carries no region.

    The router resolves one from the iDRAC prefix on every run it starts, so
    only a run started around it gets here. The region is part of the name
    server-scan reads the site from, so it is never guessed.
    """


class IdracUnreachableError(OrchestratorError):
    """WORKFLOW-RAISED. The iDRAC never answered within the reachability deadline.

    The usual cause is a mistyped IP or an iDRAC not yet cabled — the run
    waits a while for the second, and then says so rather than run forever.
    """


class IdracCredentialsRejectedError(OrchestratorError):
    """WORKFLOW-RAISED. Every configured root password was rejected, round after round.

    Each round waits out the iDRAC's IP-blocking penalty first, so this is not
    a lockout — the machine has a root password nobody configured.
    """


class NotADellServerError(OrchestratorError):
    """WORKFLOW-RAISED. The address answers Redfish but is not a Dell PowerEdge."""


class ServerAlreadyInstalledError(OrchestratorError):
    """WORKFLOW-RAISED. server-scan says this service tag is in use by a cluster.

    Checked before anything touches the machine: provisioning reboots it and
    re-templates its BIOS, so an iDRAC IP typed one digit wrong must not take
    down a production node.
    """


class OmeDiscoveryFailedError(OrchestratorError):
    """WORKFLOW-RAISED. OME's discovery job failed, or finished without the device."""


class TemplateDeployFailedError(OrchestratorError):
    """WORKFLOW-RAISED. The template deployment job failed or did not finish in time."""


class TemplatePasswordNotAppliedError(OrchestratorError):
    """WORKFLOW-RAISED. After the template deployed, root still does not accept
    the target password — the template does not carry it, or did not apply it."""


class StorageLayoutUnsupportedError(OrchestratorError):
    """WORKFLOW-RAISED. The machine's storage cannot be taken to the required
    layout WITHOUT DESTROYING SOMETHING: no BOSS controller, a BOSS without
    exactly two drives, a BOSS volume that is not the RAID 1, or a PERC drive
    already in a volume. The workflow never deletes a volume."""


class StorageJobFailedError(OrchestratorError):
    """WORKFLOW-RAISED. A staged RAID / non-RAID job failed or did not finish in time."""


class StorageNotConvergedError(OrchestratorError):
    """WORKFLOW-RAISED. Every storage job completed, yet reading the storage back
    does not show the RAID 1 on the BOSS and every PERC drive Non-RAID."""


class ServerNameNotAppliedError(OrchestratorError):
    """WORKFLOW-RAISED. The OME profile never carried a name matching the
    convention for this machine (region and service tag included) in time."""
