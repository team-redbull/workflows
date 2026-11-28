"""UPI-server activity signatures — the limb INSIDE each UPI cluster (upi-lifecycle domain).

Implemented in activities/upi_server/activities.py, registered on
`upi_server_activity_queue(<cluster>)` by that cluster's `upi-server-worker`. The
worker acts on its OWN cluster's API as its own ServiceAccount (node-image, CSRs,
Nodes), holds the BMC credentials and server-scan's token, and serves the ISOs it
builds from its own volume over HTTP (ISO_BASE_URL) for the BMCs to fetch.
"""

from __future__ import annotations

from temporalio import activity

from shared.models.server_lifecycle import AcquiredServer
from shared.models.upi_lifecycle import (
    AllocateUpiServerRequest,
    FinishedUpiNode,
    FinishUpiNodeRequest,
    NodeIso,
    NodeIsoRequest,
    NodeJoined,
    NodeJoinRequest,
    UpiNodeInspectRequest,
    UpiNodeState,
    UpiServerLockRequest,
    VirtualMediaBoot,
    VirtualMediaBootRequest,
)


@activity.defn
async def allocate_upi_server(request: AllocateUpiServerRequest) -> AcquiredServer:
    """Take the NAMED server for this run: server-scan's `POST /servers/available`.

    The server comes back locked (holder `add-server-to-upi` + the workflow id,
    `mce_cluster` = the UPI cluster) and live-checked, with its BMC endpoint and
    interfaces. NOT idempotent — server-scan ties a lock to its request — but a
    retry after a lost answer meets the run's own lock and is refused, which is
    why the workflow calls it once. Raises ServerNotAvailableError,
    ServerScanAuthError, AmbiguousServerNameError, ServerScanRequestInvalidError
    — deterministic; any other failure is the retryable ServerScanError.
    """
    ...


@activity.defn
async def extend_upi_server_lock(request: UpiServerLockRequest) -> None:
    """Extend the run's server-scan lock (same holder + workflow id = an extension).

    Called once the Node is Ready, for a day, so the server is not drawn again
    before server-scan's collectors see it in the cluster. Idempotent. Raises
    ServerReservedError (another holder's lock) and ServerScanAuthError —
    deterministic; anything else is retryable.
    """
    ...


@activity.defn
async def release_upi_server(request: UpiServerLockRequest) -> None:
    """Give the run's server-scan lock back (a server the run cannot use). Idempotent:
    a lock already gone is success."""
    ...


@activity.defn
async def inspect_upi_node(request: UpiNodeInspectRequest) -> UpiNodeState:
    """The Node of that name in this cluster, if any: exists, Ready. Observe-only.
    Raises UpiNodeAccessError (403) — deterministic; an outage is retryable."""
    ...


@activity.defn
async def build_node_iso(request: NodeIsoRequest) -> NodeIso:
    """Render the host's nodes-config.yaml and build its ISO with `oc adm node-image create`.

    bond0 (`bond_mode`) over the two members, by MAC, with `bond0.<vlan>` on DHCP;
    the install disk from `root_device` (else DEFAULT_ROOT_DEVICE). Built in a
    scratch folder of ISO_DIR and renamed to `<hostname>.iso` only when complete,
    beside `<hostname>.sha256` (the config's hash): an ISO already there for the
    SAME config is returned with `built=False`, so a retry builds nothing twice.
    Heartbeats while `oc` runs. Raises NodeImageBuildError (oc exited non-zero —
    its stderr, bounded) — deterministic; a timeout is the retryable
    UpiNodeApiError.
    """
    ...


@activity.defn
async def boot_from_virtual_media(request: VirtualMediaBootRequest) -> VirtualMediaBoot:
    """Mount the ISO through the server's Redfish virtual media and boot it once.

    Finds the system (server-scan's BMC path, else the first of
    `/redfish/v1/Systems`) and the manager's CD/DVD virtual media slot; ejects
    whatever another image left there, inserts `iso_url` and READS IT BACK; sets
    a one-time boot override to Cd; then powers on (`On`) or restarts
    (`ForceRestart`). Credentials are the worker's, per vendor. Raises
    BmcCredentialsMissingError, RedfishAuthError, VirtualMediaUnavailableError —
    deterministic; a busy or unreachable BMC is the retryable RedfishError.
    """
    ...


@activity.defn
async def await_node_joined(request: NodeJoinRequest) -> NodeJoined:
    """Approve THIS host's CSRs until its Node is Ready, heartbeating.

    Every 15 s: a pending CSR is approved only when it is this host's — a
    node-bootstrapper client CSR whose subject is `system:node:<hostname>`, or a
    serving CSR requested by `system:node:<hostname>` — never any other. Done
    when the Node `<hostname>` is Ready. Its clock is in the heartbeat details.
    Raises NodeJoinTimeoutError (deadline: the Node's last state and the CSRs
    seen) and UpiNodeAccessError — deterministic; an outage is retryable.
    """
    ...


@activity.defn
async def finish_upi_node(request: FinishUpiNodeRequest) -> FinishedUpiNode:
    """After the join: set the Node's labels (read back), eject the virtual media and
    delete the ISO. The labels are required; the eject and the delete are best
    effort, each failure a note. Idempotent. Raises UpiNodeAccessError —
    deterministic; an outage is retryable.
    """
    ...
