"""add-server-to-upi — ONE bare-metal server joined to a running UPI cluster as a Node.

What the operator did by hand ("Creation MCE 4.20", page 2) and cluster-expender
tried as a CLI: `oc adm node-image create` with the server's network, the ISO on
the server's virtual media, the boot, the CSRs, the Node. Designed with the
operator 2026-11-27: bond0 over the two link-UP NICs (802.3ad by default), the
cluster's node VLAN on it, the address by DHCP. Every call to the cluster runs on
the upi-server worker INSIDE it (`upi-server-activity-<cluster>`).

Shape of the run:

  1. resolving-network   — the cluster's node segment in the Segments Manager (its
                           MCE segment, else its UPI one), on the segment-lifecycle
                           queue: the VLAN for `bond0.<vlan>`. A segment that says
                           `dhcp: false` is refused — the node takes its address by
                           DHCP. Nothing is allocated before this.
  2. allocating-server   — server-scan's `POST /servers/available` for the NAMED
                           server: locked for the run (TTL _LOCK_TTL), live-checked.
  3. selecting-bond      — select_bond_members (add-server-to-mce's policy, reused):
                           two link-UP NICs on two distinct physical ports, nic1
                           the boot MAC. A server whose BMC is IPMI (no virtual
                           media) or unknown is refused and its lock given back.
  4. checking-cluster    — a Node of that hostname already Ready: the server is in
                           (a re-run), straight to finishing.
  5. building-iso        — the host's nodes-config.yaml and its ISO, by the worker.
  6. booting             — the ISO on the BMC's virtual media, read back, one-time
                           boot to CD, power on or restart.
  7. joining             — this host's CSRs approved (only this host's) until its
                           Node is Ready (heartbeating, `join_timeout_minutes`).
  8. finishing           — the Node's labels, the media ejected, the ISO deleted;
                           server-scan's lock extended for a day, so the server is
                           not drawn again before server-scan sees it in the cluster.
  9. completed.

On a failure after the allocation the lock is LEFT to its TTL (half-done work may
still name the machine — add-server-to-mce's rule), and nothing is undone: a re-run
of the same server meets its own lock as the workflow id's and resumes. A cancel
undoes nothing either.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from shared.bmc_address import BMC_VENDORS, boots_from_network
    from shared.consts import SEGMENT_LIFECYCLE_ACTIVITY_QUEUE, upi_server_activity_queue
    from shared.exceptions import (
        AmbiguousAllocationError,
        AmbiguousServerNameError,
        BmcCredentialsMissingError,
        NodeImageBuildError,
        NodeJoinTimeoutError,
        NoBondableInterfacesError,
        RedfishAuthError,
        SegmentsManagerAuthError,
        SegmentWithoutDhcpError,
        ServerNotAvailableError,
        ServerReservedError,
        ServerScanAuthError,
        ServerScanRequestInvalidError,
        UnknownBmcVendorError,
        UpiNetworkNotFoundError,
        UpiNodeAccessError,
        VirtualMediaUnavailableError,
        VirtualMediaUnsupportedError,
    )
    from shared.interfaces.segment_lifecycle import find_cluster_allocation
    from shared.interfaces.upi_server import (
        allocate_upi_server,
        await_node_joined,
        boot_from_virtual_media,
        build_node_iso,
        extend_upi_server_lock,
        finish_upi_node,
        inspect_upi_node,
        release_upi_server,
    )
    from shared.models.segment_lifecycle import ClusterAllocationLookupRequest, SegmentEntry, SegmentType
    from shared.models.server_lifecycle import AcquiredServer
    from shared.models.upi_lifecycle import (
        AddServerToUpiInput,
        AddServerToUpiProgress,
        AddServerToUpiResult,
        AddServerToUpiRunArgs,
        AllocateUpiServerRequest,
        FinishedUpiNode,
        FinishUpiNodeRequest,
        NodeIso,
        NodeIsoRequest,
        NodeJoined,
        NodeJoinRequest,
        UpiNode,
        UpiNodeInspectRequest,
        UpiServerLockRequest,
        VirtualMediaBootRequest,
    )
    from workflow_domains.server_lifecycle.bond_selection import describe_candidate, select_bond_members

# server-scan's install lock while the server boots and installs; a day once it joined.
_LOCK_TTL = timedelta(hours=4)
_LOCK_HOLD_AFTER_JOIN = timedelta(hours=24)
# A short call to the cluster, server-scan or a BMC.
_SHORT_TIMEOUT = timedelta(minutes=3)
# server-scan's allocation live-checks the server while locked: up to ~22 min.
_ALLOCATE_TIMEOUT = timedelta(minutes=30)
_HEARTBEAT = timedelta(minutes=2)
_BUILD_TIMEOUT = timedelta(minutes=40)
_SEGMENT_TIMEOUT = timedelta(seconds=90)
# The node segment, in this order: an MCE's, then a standalone UPI cluster's.
_NODE_SEGMENT_TYPES = (SegmentType.MCE, SegmentType.UPI)

_PERMANENT_ACTIVITY_ERRORS: tuple[type[Exception], ...] = (
    SegmentsManagerAuthError,
    AmbiguousAllocationError,
    ServerNotAvailableError,
    ServerScanAuthError,
    ServerScanRequestInvalidError,
    AmbiguousServerNameError,
    ServerReservedError,
    UpiNodeAccessError,
    NodeImageBuildError,
    BmcCredentialsMissingError,
    RedfishAuthError,
    VirtualMediaUnavailableError,
    NodeJoinTimeoutError,
)
_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
    non_retryable_error_types=[error.__name__ for error in _PERMANENT_ACTIVITY_ERRORS],
)


@workflow.defn
class AddServerToUpiWorkflow:
    def __init__(self) -> None:
        self._phase = "pending"
        self._input: AddServerToUpiInput | None = None
        self._hostname: str | None = None
        self._vlan_id: int | None = None
        self._bond_macs: list[str] = []
        self._iso_url: str | None = None

    @workflow.query
    def progress(self) -> AddServerToUpiProgress:
        request = self._input
        return AddServerToUpiProgress(
            phase=self._phase,
            cluster=request.cluster if request else None,
            server_name=request.server_name if request else None,
            hostname=self._hostname,
            vlan_id=self._vlan_id,
            bond_macs=list(self._bond_macs),
            iso_url=self._iso_url,
        )

    @workflow.run
    async def run(self, run_args: AddServerToUpiRunArgs) -> AddServerToUpiResult:
        request = run_args.input
        self._input = request
        cluster = request.cluster
        queue = upi_server_activity_queue(cluster)
        hostname = (request.hostname or request.server_name).lower()
        self._hostname = hostname
        workflow_id = workflow.info().workflow_id

        # Step 1 — the VLAN of the cluster's node network.
        self._phase = "resolving-network"
        segment = await self._node_segment(cluster)
        self._vlan_id = segment.vlan_id

        # Step 2 — the server, locked for this run.
        self._phase = "allocating-server"
        server: AcquiredServer = await workflow.execute_activity(
            allocate_upi_server,
            AllocateUpiServerRequest(server_name=request.server_name, cluster=cluster, workflow_id=workflow_id,
                                     ttl_seconds=int(_LOCK_TTL.total_seconds())),
            task_queue=queue,
            start_to_close_timeout=_ALLOCATE_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )

        # Step 3 — two NICs on two ports, and a BMC with virtual media.
        self._phase = "selecting-bond"
        vendor = (server.bmc_vendor or "").upper()
        if vendor not in BMC_VENDORS:
            await self._give_back(server, cluster, workflow_id, queue)
            raise ApplicationError(
                f"server-scan reports BMC vendor {server.bmc_vendor!r} for {server.name}: no driver to boot it "
                "with. Its server-scan lock was given back; nothing was written",
                type=UnknownBmcVendorError.__name__,
            )
        if boots_from_network(vendor):
            await self._give_back(server, cluster, workflow_id, queue)
            raise ApplicationError(
                f"{server.name}'s BMC is driven over IPMI ({vendor}), which has no virtual media for a "
                "node-image ISO. Its server-scan lock was given back; nothing was written",
                type=VirtualMediaUnsupportedError.__name__,
            )
        members = select_bond_members(server, allow_link_down=True)
        if members is None:
            await self._give_back(server, cluster, workflow_id, queue)
            raise ApplicationError(
                f"{server.name} has no two NICs with a MAC on distinct physical ports: "
                f"{describe_candidate(server)}. Its server-scan lock was given back; nothing was written",
                type=NoBondableInterfacesError.__name__,
            )
        self._bond_macs = [m.mac for m in members]

        # Step 4 — already in? (a re-run after the join)
        self._phase = "checking-cluster"
        node_state = await workflow.execute_activity(
            inspect_upi_node, UpiNodeInspectRequest(hostname=hostname),
            task_queue=queue, start_to_close_timeout=_SHORT_TIMEOUT, retry_policy=_RETRY_POLICY,
        )
        iso: NodeIso | None = None
        joined: NodeJoined | None = None
        if not (node_state.exists and node_state.ready):
            # Step 5 — the ISO.
            self._phase = "building-iso"
            iso = await workflow.execute_activity(
                build_node_iso,
                NodeIsoRequest(hostname=hostname, root_device=request.root_device, members=members,
                               bond_mode=request.bond_mode, vlan_id=segment.vlan_id),
                task_queue=queue,
                start_to_close_timeout=_BUILD_TIMEOUT,
                heartbeat_timeout=_HEARTBEAT,
                retry_policy=_RETRY_POLICY,
            )
            self._iso_url = iso.url

            # Step 6 — the boot.
            self._phase = "booting"
            await workflow.execute_activity(
                boot_from_virtual_media, VirtualMediaBootRequest(server=server, iso_url=iso.url),
                task_queue=queue, start_to_close_timeout=_SHORT_TIMEOUT, retry_policy=_RETRY_POLICY,
            )

            # Step 7 — the join.
            self._phase = "joining"
            join_seconds = request.join_timeout_minutes * 60
            joined = await workflow.execute_activity(
                await_node_joined, NodeJoinRequest(hostname=hostname, timeout_seconds=join_seconds),
                task_queue=queue,
                start_to_close_timeout=timedelta(seconds=join_seconds + 600),
                heartbeat_timeout=_HEARTBEAT,
                retry_policy=_RETRY_POLICY,
            )

        # Step 8 — labels, media, ISO, and the lock held until server-scan sees the node.
        self._phase = "finishing"
        finished: FinishedUpiNode = await workflow.execute_activity(
            finish_upi_node,
            FinishUpiNodeRequest(server=server, hostname=hostname,
                                 iso_file_name=iso.file_name if iso else f"{hostname}.iso",
                                 node_labels=request.node_labels),
            task_queue=queue, start_to_close_timeout=_SHORT_TIMEOUT, retry_policy=_RETRY_POLICY,
        )
        await workflow.execute_activity(
            extend_upi_server_lock,
            UpiServerLockRequest(server=server, cluster=cluster, workflow_id=workflow_id,
                                 ttl_seconds=int(_LOCK_HOLD_AFTER_JOIN.total_seconds())),
            task_queue=queue, start_to_close_timeout=_SHORT_TIMEOUT, retry_policy=_RETRY_POLICY,
        )

        self._phase = "completed"
        node = joined.node if joined else UpiNode(name=hostname, ready=True)
        workflow.logger.info("%s joined %s as Node %s (vlan %d)", server.name, cluster, hostname, segment.vlan_id)
        return AddServerToUpiResult(
            cluster=cluster,
            server_name=server.name,
            server_id=server.id,
            hostname=hostname,
            vlan_id=segment.vlan_id,
            segment=segment.segment,
            bond_macs=list(self._bond_macs),
            bmc_vendor=vendor,
            iso_url=iso.url if iso else None,
            node=node,
            already_joined=joined is None,
            csrs_approved=list(joined.csrs_approved) if joined else [],
            finished=finished,
        )

    async def _give_back(self, server: AcquiredServer, cluster: str, workflow_id: str, queue: str) -> None:
        """Release the run's server-scan lock on a server it cannot use."""
        await workflow.execute_activity(
            release_upi_server,
            UpiServerLockRequest(server=server, cluster=cluster, workflow_id=workflow_id,
                                 ttl_seconds=int(_LOCK_TTL.total_seconds())),
            task_queue=queue,
            start_to_close_timeout=_SHORT_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )

    async def _node_segment(self, cluster: str) -> SegmentEntry:
        """The cluster's node segment: its MCE one, else its UPI one."""
        for segment_type in _NODE_SEGMENT_TYPES:
            lookup = await workflow.execute_activity(
                find_cluster_allocation,
                ClusterAllocationLookupRequest(cluster=cluster, type=segment_type),
                task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
                start_to_close_timeout=_SEGMENT_TIMEOUT,
                retry_policy=_RETRY_POLICY,
            )
            if lookup.found and lookup.entry is not None:
                if lookup.entry.dhcp is False:
                    raise ApplicationError(
                        f"{cluster}'s {segment_type.value} segment {lookup.entry.segment} (vlan "
                        f"{lookup.entry.vlan_id}) says dhcp: false in the Segments Manager, and a node added "
                        "here takes its address by DHCP on bond0.<vlan>. Nothing was allocated",
                        type=SegmentWithoutDhcpError.__name__,
                    )
                return lookup.entry
        raise ApplicationError(
            f"{cluster} holds no {' or '.join(t.value for t in _NODE_SEGMENT_TYPES)} segment in the Segments "
            "Manager, so there is no VLAN for its node's bond (create-upi-cluster allocates it). Nothing was allocated",
            type=UpiNetworkNotFoundError.__name__,
        )
