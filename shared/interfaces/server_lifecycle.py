"""Server-lifecycle activity signatures — the typed contract, no implementations.

The real implementations live in activities/server_lifecycle/activities.py and
are registered against these names on the server-lifecycle activity queue.
Workflows import THESE for type-checked activity references.

Note what is NOT here: resolving the inventory VLAN. That reads the Segments
Manager, whose credential already lives on the segment-lifecycle limb, so
install-server calls `get_inventory_segment` on the SEGMENT-LIFECYCLE queue
instead of this domain duplicating the token (see shared/interfaces/
segment_lifecycle.py).
"""

from __future__ import annotations

from temporalio import activity

from shared.models.server_lifecycle import (
    AcquireServerRequest,
    AcquiredServer,
    AgentRef,
    AgentState,
    BmhRef,
    BmhResourceRequest,
    BmhState,
    CreatedResource,
    ReleaseServerRequest,
    ReserveServerRequest,
    ServerReservation,
    TeardownResult,
)


@activity.defn
async def acquire_servers(request: AcquireServerRequest) -> list[AcquiredServer]:
    """Ask server-scan for assignable candidates (GET /servers/available).

    Returns up to `request.count` servers that server-scan considers
    installable: unclaimed by any cluster, not in maintenance, reachable, and
    at the requested health. It returns a LIST even in `name` mode (one item),
    because the workflow's bond selection may reject a candidate and move to
    the next — drawing several at once is what makes one unusable server a
    retry rather than a failed run.

    The health filter is applied server-side rather than here: without it the
    endpoint fills HEALTHY then WARNING then MAJOR, and a WARNING server would
    be returned — and live-rechecked, at a vendor manager's expense — only to
    be discarded.

    Raises ServerNotAvailableError (404, nothing assignable matched),
    AmbiguousServerNameError (409, the name spans several documents) or
    ServerScanAuthError (401/403) — all deterministic and marked non-retryable
    by the workflow; anything else is transient ServerScanError.
    """
    ...


@activity.defn
async def reserve_server(request: ReserveServerRequest) -> ServerReservation:
    """Take server-scan's install lock on one chosen candidate (ADR-0035).

    The guard `/servers/available` cannot give: it hands candidates out
    unlocked, and a machine stays AVAILABLE there until a cluster reports it.
    Without the lock a server installed into MCE-A is drawn again by a run
    for MCE-B, whose BareMetalHost probe only sees MCE-B — two clusters then
    drive one BMC and nothing reports a conflict.

    Re-taking a lock this run already holds EXTENDS it (same holder and
    workflow id), so a retry is never a lost race and the same call renews the
    lock before the Agent wait and after a success.

    Raises ServerReservedError (409, a live lock held by another run — the
    workflow skips the candidate), ServerNotAvailableError (404, the server left
    the inventory), ServerScanAuthError (401/403 — the token lacks the ADMIN
    role) or ServerScanRequestInvalidError (400/422); all non-retryable. A 409
    for a lost revision race, and anything else, is transient ServerScanError.
    """
    ...


@activity.defn
async def release_server(request: ReleaseServerRequest) -> ServerReservation:
    """Give the install lock back after a candidate was rolled back.

    Not needed for correctness — the lock expires on its own — but without it
    a rolled-back machine stays undrawable for the rest of its TTL. Never a
    failure when there is nothing of ours to release: an expired lock, a server
    gone from the inventory (404) or a lock another run has since taken (409)
    all answer held=False with `detail` saying which. Only the holder +
    workflow id release, so another run's lock is never cleared by accident.

    Raises ServerScanAuthError / ServerScanRequestInvalidError (non-retryable);
    anything else is transient ServerScanError.
    """
    ...


@activity.defn
async def create_bmc_secret(request: BmhResourceRequest) -> CreatedResource:
    """Create the BMC credentials Secret (`{vendor}-cred-{server}`).

    The credentials come from this worker's own configuration, per vendor —
    server-scan never holds them. Idempotent: an existing Secret is success
    (changed=False), NOT an overwrite, so a re-run never rotates a credential
    an operator has since corrected by hand. It is the one resource whose
    contents are NOT compared, deliberately: the only way to compare a Secret is
    to read a credential back out of the cluster, and since nothing here would
    rewrite it there is nothing a comparison could act on.

    Raises BmcCredentialsMissingError when no username/password is configured
    for the server's vendor — deterministic, non-retryable.
    """
    ...


@activity.defn
async def create_baremetal_host(request: BmhResourceRequest) -> CreatedResource:
    """Create the BareMetalHost (metal3.io/v1alpha1).

    `bootMACAddress` is the first bond member. The `bmc.address` is rebuilt
    from what server-scan parsed (scheme, host, port, path) mapped onto the
    matching Ironic driver — never from a fixed per-vendor template, so a BMC
    behind a Route with a DNS host and its own path works unchanged.

    Idempotent, but not blindly: an existing BareMetalHost of the same name is
    success (changed=False) only once its BMC address, credentials Secret, boot
    MAC and InfraEnv label MATCH what this request would have created. A
    disagreement is BmhConflictError — reporting success would describe an
    installation that is not the one on the cluster — and nothing is overwritten
    either way, so an operator's hand-correction survives.

    Permanent (all non-retryable): BmhPrerequisiteMissingError when the cluster
    has no such resource type, no such namespace, or this worker's
    ServiceAccount lacks RBAC (404/403); BmhRequestInvalidError when the API
    server rejects the body (400/422); InvalidServerNameError when the
    server-scan name cannot be a Kubernetes resource name even lowercased.
    Anything else is transient BmhResourceError — including 401, since a
    projected ServiceAccount token is rotated under the pod.
    """
    ...


@activity.defn
async def create_nmstate_config(request: BmhResourceRequest) -> CreatedResource:
    """Create the NMStateConfig (`nmstate-config-{server}`).

    Builds an 802.3ad bond over the selected members with the DHCP VLAN riding
    it. Interface names are logical placeholders (`nic1`, `nic2`): the config
    binds MAC -> name in `spec.interfaces` and the agent renames the NIC to
    match before applying `spec.config`, so no per-server-type NIC-name profile
    is needed.

    Idempotent and error-classified exactly as create_baremetal_host; the
    identity compared on an existing resource is its MAC set (unordered — the
    bond is the same wiring whichever member became nic1), its VLAN id and its
    InfraEnv label.
    """
    ...


@activity.defn
async def get_baremetal_host(ref: BmhRef) -> BmhState:
    """Read one BareMetalHost's status back, by name and namespace.

    Serves two callers. The registration poll uses it as its observation: a
    BareMetalHost that does not exist yet is NOT an error, it reports
    found=False, which is the normal answer while the API server has stored the
    object but Ironic has not reached the BMC, and the workflow's bounded timer
    loop owns the waiting. Candidate selection uses the same read to skip a
    server that is ALREADY installed — server-scan cannot report that, because
    nothing changes its lifecycle state until a cluster reports the node.
    """
    ...


@activity.defn
async def find_agent_for_host(ref: AgentRef) -> AgentState:
    """Whether an Agent has registered for this host yet, matched by MAC.

    THE SUCCESS SIGNAL of an install. An Agent exists because the host booted
    the discovery ISO and reached assisted-service, which proves in one
    observation what nothing earlier can: that the BMC accepted virtual media,
    that the bond formed, that the VLAN was the right one and that DHCP
    answered. A BareMetalHost Ironic has registered proves only that the BMC
    answered — a host whose bond or VLAN is wrong registers perfectly well and
    is then never heard from again.

    Lists Agents in the namespace and matches
    `status.inventory.interfaces[].macAddress` against the bond MACs, because
    BMAC names an Agent after the host's inventory UUID and `agent.spec` holds
    no back-reference to the BareMetalHost. Name-based matching would break.

    Absent is NOT an error: found=False is the normal answer for most of the
    wait, and the workflow's bounded timer owns the deadline. Errors are
    classified as for every other cluster read — notably 403, so a missing
    `list` verb on agents fails the run instead of looking like a host that
    never booted.
    """
    ...


@activity.defn
async def teardown_bmh_resources(ref: BmhRef) -> TeardownResult:
    """Remove one candidate's resources so the machine returns to the inventory.

    Called when a host never produced an Agent, to roll that candidate back
    before the next one is tried. Ordering is NOT arbitrary and was established
    against a live cluster:

      1. Annotate the BareMetalHost `baremetalhost.metal3.io/detached`. This has
         to happen BEFORE the delete — metal3 honours the annotation during
         normal reconcile, but once `deletionTimestamp` is set the host is on
         the delete path and applying it then changes nothing (measured: no
         effect after four minutes).
      2. Delete the NMStateConfig. It has no finalizer and no ownerReference, so
         nothing removes it implicitly and it goes immediately.
      3. Delete the BareMetalHost, then wait, bounded.
      4. If it still stands, drop the `baremetalhost.metal3.io` finalizer. Metal3
         holds it while trying to deprovision through the BMC — and a rollback
         happens exactly when that BMC never answered, so this is the expected
         path here rather than an exceptional one. BMAC's own
         `bmac.agent-install.openshift.io/deprovision` finalizer releases on its
         own within a second and is left alone.
      5. Confirm the Secret is gone. It carries an ownerReference to the
         BareMetalHost, so the garbage collector takes it once the host really
         goes — which is only after the finalizer clears. It is deleted
         explicitly if it somehow outlives the host, because the alternative is
         a credential left behind on every rollback.

    Idempotent throughout: a resource already absent is success, which is what
    lets Temporal retry this. Raises BmhTeardownError (retryable) only when
    something is STILL there afterwards — the server must not be reported back
    to the inventory while a BareMetalHost still points at it.
    """
    ...
