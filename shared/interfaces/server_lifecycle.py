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
    BmhRef,
    BmhResourceRequest,
    BmhState,
    CreatedResource,
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
