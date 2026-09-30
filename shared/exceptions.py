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


class ConfigurationDriftError(OrchestratorError):
    """WORKFLOW-RAISED. The template deployed, the reboot ran, and BIOS
    attributes it set still do not match it.

    The deployment reporting success is not evidence: SCP Import is a "continue
    on error" operation, so an attribute the target's firmware does not know
    fails while everything else applies. The run stages the drift over Redfish
    and lets the storage reboot apply it; this is what remains after that, which
    means the machine will not take the setting at all.
    """


class TemplateUnsafeError(OrchestratorError):
    """WORKFLOW-RAISED. The configured template carries attributes this workflow
    refuses to deploy — iDRAC network settings, storage, or user accounts.

    Checked BEFORE anything touches the machine, because the worst of the three
    is unrecoverable remotely: a template carrying the reference server's iDRAC
    address moves every target onto it, or resets it to DHCP, and the machine is
    then reachable only at the rack. See template_policy.py for all three groups
    and docs/design/dell-scp-template-r660.md for why each is fatal.
    """


class TemplateDeployFailedError(OrchestratorError):
    """WORKFLOW-RAISED. The template deployment job failed or did not finish in time."""


class RootPasswordNotSetError(OrchestratorError):
    """WORKFLOW-RAISED. The iDRAC accepted the password change but root still
    does not accept the target password.

    Raised BEFORE OME discovers the machine, which is the point: OME must only
    ever be handed the password root will keep, so a change that did not take
    has to stop the run rather than strand OME on a stale credential later.
    """


class IdracForcePasswordChangeError(OrchestratorError):
    """WORKFLOW-RAISED. The iDRAC has Force Change of Password pending, so root's
    password is correct but the account may do nothing until it is changed.

    A refusal, not a retry: the condition is cleared by a human at the iDRAC (or
    by the factory order that set it), and no amount of waiting moves it.

    Deliberately NOT worked around, though Dell's own reference client can —
    `ChangeIdracUserPasswordREDFISH.py --force-change-enabled` writes
    `Users.2.Password` to the DellAttributes resource, which FCP does not block.
    That route cannot first READ the account, and this workflow's one hard rule
    about accounts is that it touches root and never the slot OME uses
    (CLAUDE.md §4). Writing a password into slot 2 unread, on the assumption
    that the convention holds, is the single change that loses a server for
    good. So the run stops and names the fix instead. If these turn out to be
    common in a real batch, the trade is worth revisiting WITH that evidence.
    """


class TemplatePasswordNotAppliedError(OrchestratorError):
    """WORKFLOW-RAISED. After the template deployed, root no longer accepts the
    target password.

    This used to mean the template failed to SET the password. Since 2026-09-30
    the run sets it over Redfish before OME discovery and the template is
    audited to carry no `Users.*` component at all, so it now means the
    opposite: something in the deployment MOVED root's password away from the
    target. Kept as a guard because the cost is one login and the failure mode
    it catches — a machine OME can no longer reach — is expensive.
    """


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

