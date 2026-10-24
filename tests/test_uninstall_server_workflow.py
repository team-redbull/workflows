"""uninstall-server: real UninstallServerWorkflow, real remove-server child, mock limb.

Every limb activity is registered ONLY on the target MCE's queue — as in
production, a dispatch anywhere else would hang the run — and remove-server's
inventory-segment read on the segment-lifecycle queue. The child is the REAL
RemoveServerWorkflow on its own queue, so "every path ends in remove-server" is
exercised end to end. The time-skipping environment collapses the 30 s Machine
poll and the 20-minute / 1-hour deadlines.
"""

from __future__ import annotations

import ast
import asyncio
import pathlib
import uuid

import pytest
from temporalio import activity, workflow
from temporalio.client import Client, WorkflowFailureError
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import UnsandboxedWorkflowRunner, Worker

from shared import exceptions
from shared.consts import (
    REMOVE_SERVER_WORKFLOW_QUEUE,
    SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
    UNINSTALL_SERVER_WORKFLOW_QUEUE,
    server_lifecycle_activity_queue,
)
from shared.exceptions import OrchestratorError, ServerReservedError
from shared.models.segment_lifecycle import SegmentEntry, SegmentType
from shared.models.server_lifecycle import (
    AgentBinding,
    AgentBindingState,
    AgentDeleteRequest,
    AgentDeletion,
    AgentMachineRef,
    AgentMachineState,
    AgentNameRef,
    BmhRef,
    BmhState,
    BoundHostState,
    HostAgent,
    HostedClusterRef,
    HostedNodeRef,
    HostedNodeState,
    HostPowerRequest,
    InventoryHostState,
    MachineDeletion,
    MachineRef,
    MachineState,
    PxeHostEntriesRef,
    PxeHostEntriesRemoval,
    ReserveServerRequest,
    ServerMembership,
    ServerMembershipRequest,
    ServerReservation,
    TeardownResult,
    UninstallServerInput,
    UninstallServerRunArgs,
)
from workflow_domains.server_lifecycle import uninstall_server
from workflow_domains.server_lifecycle.remove_server import RemoveServerWorkflow
from workflow_domains.server_lifecycle.uninstall_server import UninstallServerWorkflow

MCE = "ocp4-mce-bat-yam-01"
QUEUE = server_lifecycle_activity_queue(MCE)
SERVER = "ocp-cisco-m6-bat-yam-64c-512gb-FCH0001"
NS = "inventory"
INPUT = UninstallServerInput(server_name=SERVER, mce_cluster=MCE, namespace=NS)

HC = HostedClusterRef(namespace="clusters", name="hc-01", hcp_namespace="clusters-hc-01")
MACHINE = MachineRef(namespace="clusters-hc-01", name="hc-01-np-x7k2p")
NODE = "ocp-cisco-m6-bat-yam-64c-512gb-fch0001"

INSTALLED = ServerMembership(
    found=True,
    server_id="srv_123",
    server_name=SERVER,
    lifecycle_state="INSTALLED",
    cluster_name="hc-01",
    mce_name="mce-bat-yam-01",  # server-scan may carry it without `ocp4-`
)
IN_INVENTORY = INSTALLED.model_copy(
    update={"lifecycle_state": "INSTALLED_TO_INVENTORY", "cluster_name": None}
)

BOUND_AGENT = HostAgent(
    name="agent-1",
    namespace=NS,
    cluster_deployment="clusters-hc-01/hc-01",
    state="added-to-existing-cluster",
    approved=True,
    agent_machine="clusters-hc-01/hc-01-np-am-x7k2p",
    agent_machine_label=True,
)
LIVE_MACHINE = MachineState(found=True, node_name=NODE, phase="Running")
DELETING = MachineState(
    found=True,
    deleting=True,
    deletion_timestamp="2026-10-24T08:00:00Z",
    node_name=NODE,
    phase="Deleting",
    pending_hooks=["pre-terminate.delete.hook.machine.cluster.x-k8s.io/agentmachine"],
    conditions=["DrainingSucceeded=False (Draining): Cannot evict pod as it would violate the pod's disruption budget"],
)
GONE = MachineState(found=False)

HOSTED_NODE = BoundHostState(
    bmh_found=True,
    bmh_operational_status="OK",
    infra_env="cisco-m6-bat-yam-64c-512gb",
    agents=[BOUND_AGENT],
    machine=MACHINE,
    machine_state=LIVE_MACHINE,
    hosted_cluster=HC,
)
INVENTORY_HOST = BoundHostState(
    bmh_found=True,
    bmh_operational_status="OK",
    infra_env="cisco-m6-bat-yam-64c-512gb",
    agents=[HostAgent(name="agent-1", namespace=NS, state="known-unbound", approved=True)],
)

AGENT_BOUND = AgentBindingState(
    found=True, cluster_deployment="clusters-hc-01/hc-01", agent_machine_label=True,
    state="added-to-existing-cluster", approved=True,
)
AGENT_UNBINDING = AgentBindingState(found=True, state="unbinding-pending-user-action", approved=True)
AGENT_DISCOVERING = AgentBindingState(found=True, state="discovering-unbound", approved=True)
AGENT_FREE = AgentBindingState(found=True, state="known-unbound", approved=True)

# A machine going away the normal way: draining with the Agent bound, the
# provider releases it, the Machine goes, the host comes back in discovery.
MACHINE_TIMELINE = [DELETING, DELETING, DELETING, GONE]
AGENT_TIMELINE = [AGENT_BOUND, AGENT_BOUND, AGENT_UNBINDING, AGENT_DISCOVERING, AGENT_FREE]

SEGMENT = SegmentEntry(
    segment="10.20.90.0/24", site="bat-yam", vlan_id=24, status="Allocated",
    type=SegmentType.INVENTORY.value, cluster_name=MCE,
)
ON_THE_MCE = InventoryHostState(
    bmh_found=True, nmstate_found=True, powered_on=True,
    agents=[AgentBinding(name="agent-1")], macs=["00:25:b5:00:00:01"],
)


def make_mocks(
    *,
    membership: ServerMembership = INSTALLED,
    host: BoundHostState = HOSTED_NODE,
    deletion: MachineDeletion | None = None,
    machines: list[MachineState] | None = None,
    agents: list[AgentBindingState] | None = None,
    agent_machine_lingers: bool = False,
    node_found: bool = False,
    reserve_error: Exception | None = None,
    hold_error: Exception | None = None,
    inventory: InventoryHostState = ON_THE_MCE,
):
    """Each sequence is consumed one item per call; its last item repeats."""
    machine_seq = list(machines or MACHINE_TIMELINE)
    agent_seq = list(agents or AGENT_TIMELINE)
    calls: dict[str, list] = {
        k: []
        for k in (
            "lookup_server_membership", "inspect_bound_host", "reserve_server",
            "ensure_machine_deleting", "get_machine_state", "get_agent_binding",
            "get_agent_machine_state", "check_hosted_node", "get_baremetal_host",
            "inspect_inventory_host", "set_host_online", "teardown_bmh_resources",
            "delete_agents", "remove_host_pxe_entries", "order",
        )
    }

    def _next(seq: list, name: str):
        return seq[min(len(calls[name]) - 1, len(seq) - 1)]

    def _mark(step: str) -> None:
        # Collapse consecutive polls into one entry, so the order reads as phases.
        if not calls["order"] or calls["order"][-1] != step:
            calls["order"].append(step)

    @activity.defn
    async def lookup_server_membership(request: ServerMembershipRequest) -> ServerMembership:
        calls["lookup_server_membership"].append(request)
        _mark("lookup")
        return membership

    @activity.defn
    async def inspect_bound_host(ref: BmhRef) -> BoundHostState:
        calls["inspect_bound_host"].append(ref)
        _mark("inspect")
        return host

    @activity.defn
    async def reserve_server(request: ReserveServerRequest) -> ServerReservation:
        calls["reserve_server"].append(request)
        first = len(calls["reserve_server"]) == 1
        _mark("reserve" if first else "hold")
        error = reserve_error if first else hold_error
        if error is not None:
            raise error
        return ServerReservation(
            server_id=request.server_id, held=True,
            expires_at="2026-10-25T09:00:00Z" if not first else "2026-10-24T10:00:00Z",
        )

    @activity.defn
    async def ensure_machine_deleting(ref: MachineRef) -> MachineDeletion:
        calls["ensure_machine_deleting"].append(ref)
        _mark("delete-machine")
        if deletion is not None:
            return deletion
        if host.machine_state and host.machine_state.deleting:
            return MachineDeletion(found=True, already_deleting=True)
        return MachineDeletion(found=True, deleted=True)

    @activity.defn
    async def get_machine_state(ref: MachineRef) -> MachineState:
        calls["get_machine_state"].append(ref)
        _mark("await-machine")
        return _next(machine_seq, "get_machine_state")

    @activity.defn
    async def get_agent_binding(ref: AgentNameRef) -> AgentBindingState:
        calls["get_agent_binding"].append(ref)
        return _next(agent_seq, "get_agent_binding")

    @activity.defn
    async def get_agent_machine_state(ref: AgentMachineRef) -> AgentMachineState:
        calls["get_agent_machine_state"].append(ref)
        _mark("verify-agentmachine")
        return AgentMachineState(found=agent_machine_lingers, deleting=agent_machine_lingers)

    @activity.defn
    async def check_hosted_node(ref: HostedNodeRef) -> HostedNodeState:
        calls["check_hosted_node"].append(ref)
        _mark("verify-node")
        return HostedNodeState(found=node_found, node_name=ref.node_name)

    @activity.defn
    async def get_baremetal_host(ref: BmhRef) -> BmhState:
        # Shared by verifying-agent and the child's power-off poll.
        calls["get_baremetal_host"].append(ref)
        return BmhState(found=True, provisioning_state="externally provisioned",
                        operational_status="OK", powered_on=False)

    # --- remove-server, the child --------------------------------------------
    @activity.defn
    async def inspect_inventory_host(ref: BmhRef) -> InventoryHostState:
        calls["inspect_inventory_host"].append(ref)
        first = len(calls["inspect_inventory_host"]) == 1
        _mark("child-inspect" if first else "child-verify")
        return inventory if first else InventoryHostState()

    @activity.defn
    async def set_host_online(request: HostPowerRequest) -> bool:
        calls["set_host_online"].append(request)
        _mark("child-power-off")
        return True

    @activity.defn
    async def teardown_bmh_resources(ref: BmhRef) -> TeardownResult:
        calls["teardown_bmh_resources"].append(ref)
        _mark("child-teardown")
        return TeardownResult(removed=[f"BareMetalHost/{ref.server_name.lower()}"])

    @activity.defn
    async def delete_agents(request: AgentDeleteRequest) -> AgentDeletion:
        calls["delete_agents"].append(request)
        _mark("child-delete-agents")
        return AgentDeletion(deleted=list(request.names))

    @activity.defn
    async def remove_host_pxe_entries(ref: PxeHostEntriesRef) -> PxeHostEntriesRemoval:
        calls["remove_host_pxe_entries"].append(ref)
        _mark("child-pxe")
        return PxeHostEntriesRemoval(cleared=[f"bat-yam/{m.replace(':', '')}" for m in ref.macs])

    @activity.defn
    async def get_inventory_segment(mce_cluster: str) -> SegmentEntry:
        return SEGMENT

    return calls, {
        QUEUE: [
            lookup_server_membership, inspect_bound_host, reserve_server,
            ensure_machine_deleting, get_machine_state, get_agent_binding,
            get_agent_machine_state, check_hosted_node, get_baremetal_host,
            inspect_inventory_host, set_host_online, teardown_bmh_resources,
            delete_agents, remove_host_pxe_entries,
        ],
        SEGMENT_LIFECYCLE_ACTIVITY_QUEUE: [get_inventory_segment],
    }


_BLOCKER_QUEUE = "test-blocker"


@workflow.defn(name="Blocker")
class _Blocker:
    """Holds a workflow id open forever — another remove-server of the same server."""

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: False)


class _Harness:
    def __init__(self, activities) -> None:
        self._activities = activities

    async def __aenter__(self) -> Client:
        self._env = await WorkflowEnvironment.start_time_skipping()
        config = self._env.client.config()
        config["data_converter"] = pydantic_data_converter
        client = Client(**config)
        self._workers = [
            Worker(client, task_queue=UNINSTALL_SERVER_WORKFLOW_QUEUE, workflows=[UninstallServerWorkflow]),
            Worker(client, task_queue=REMOVE_SERVER_WORKFLOW_QUEUE, workflows=[RemoveServerWorkflow]),
            Worker(client, task_queue=_BLOCKER_QUEUE, workflows=[_Blocker],
                   workflow_runner=UnsandboxedWorkflowRunner()),
            *(
                Worker(client, task_queue=queue, activities=activities)
                for queue, activities in self._activities.items()
            ),
        ]
        for worker in self._workers:
            await worker.__aenter__()
        return client

    async def __aexit__(self, *exc_info) -> None:
        for worker in reversed(self._workers):
            await worker.__aexit__(*exc_info)
        await self._env.__aexit__(*exc_info)


async def _execute(client: Client, request: UninstallServerInput = INPUT, workflow_id: str | None = None):
    return await asyncio.wait_for(
        client.execute_workflow(
            UninstallServerWorkflow.run,
            UninstallServerRunArgs(input=request),
            id=workflow_id or f"test-{uuid.uuid4()}",
            task_queue=UNINSTALL_SERVER_WORKFLOW_QUEUE,
        ),
        timeout=60,
    )


async def _fails(client: Client, **kwargs) -> ApplicationError:
    with pytest.raises(WorkflowFailureError) as excinfo:
        await _execute(client, **kwargs)
    cause = excinfo.value.cause
    if isinstance(cause, ActivityError):
        cause = cause.cause
    assert isinstance(cause, ApplicationError)
    return cause


CHILD = ["child-inspect", "child-power-off", "child-teardown", "child-delete-agents",
         "child-pxe", "child-verify"]
HOSTED_HALF = ["delete-machine", "await-machine", "verify-agentmachine", "verify-node"]


class TestAHostedClustersNode:
    async def test_it_leaves_the_hosted_cluster_then_the_inventory(self):
        calls, acts = make_mocks()
        async with _Harness(acts) as client:
            result = await _execute(client, workflow_id="uninstall-server-test-happy")

        assert calls["order"] == ["lookup", "inspect", "reserve", *HOSTED_HALF, "hold", *CHILD]
        assert result.hosted and result.bound_at_start
        assert result.agent_state_at_start == "added-to-existing-cluster"
        assert result.machine_deleted_by_run is True
        assert (result.hosted_cluster, result.hcp_namespace) == ("clusters/hc-01", "clusters-hc-01")
        assert result.machine == "clusters-hc-01/hc-01-np-x7k2p"
        assert result.agent_machine == "clusters-hc-01/hc-01-np-am-x7k2p"
        assert result.node_name == NODE
        assert result.skipped == []
        # The Machine was deleted once — the ONE write on the MCE — and the Node
        # checked by the name captured at inspection, on the hosted cluster.
        assert calls["ensure_machine_deleting"] == [MACHINE]
        assert calls["check_hosted_node"] == [HostedNodeRef(hosted_cluster=HC, node_name=NODE)]
        # The inventory half is remove-server's, awaited, on its own id.
        assert result.remove_server_workflow_id == f"remove-server-{SERVER.lower()}"
        assert result.remove_server.found and result.remove_server.agents_removed == ["agent-1"]
        assert result.reservation_expires_at == "2026-10-25T09:00:00Z"

    async def test_the_lock_is_taken_before_any_write_and_held_a_day_before_the_child(self):
        calls, acts = make_mocks()
        async with _Harness(acts) as client:
            await _execute(client, workflow_id="uninstall-server-lock")

        reserve, hold = calls["reserve_server"]
        assert reserve.holder == hold.holder == "uninstall-server"
        assert reserve.workflow_id == hold.workflow_id == "uninstall-server-lock"
        assert (reserve.server_id, reserve.mce_cluster) == ("srv_123", MCE)
        assert reserve.infra_env == "cisco-m6-bat-yam-64c-512gb"
        # The TTL covers the whole hosted-cluster half; the hold is a day.
        assert reserve.ttl_seconds == int(uninstall_server._RESERVATION_TTL.total_seconds())
        assert hold.ttl_seconds == 86_400
        order = calls["order"]
        assert order.index("reserve") < order.index("delete-machine")
        assert order.index("hold") < order.index("child-inspect")

    async def test_a_scale_down_already_deleting_the_machine_is_waited_on_not_redone(self):
        _calls, acts = make_mocks(host=HOSTED_NODE.model_copy(update={"machine_state": DELETING}))
        async with _Harness(acts) as client:
            result = await _execute(client)

        assert result.machine_deleted_by_run is False
        assert any(s.startswith("deleting-machine: Machine") and "already deleting" in s
                   for s in result.skipped)
        assert result.remove_server.found

    async def test_a_machine_with_no_node_skips_the_node_check(self):
        no_node = LIVE_MACHINE.model_copy(update={"node_name": None})
        calls, acts = make_mocks(
            host=HOSTED_NODE.model_copy(update={"machine_state": no_node}),
            machines=[GONE],
            agents=[AGENT_FREE],
        )
        async with _Harness(acts) as client:
            result = await _execute(client)

        assert calls["check_hosted_node"] == []
        assert result.node_name is None
        assert any(s.startswith("verifying-node:") for s in result.skipped)
        assert result.remove_server.found

    async def test_a_re_run_mid_unbinding_resumes_the_hosted_half(self):
        """The provider clears the binding FIRST; only the AgentMachine still names the Agent."""
        mid_unbinding = HOSTED_NODE.model_copy(update={
            "agents": [BOUND_AGENT.model_copy(update={
                "cluster_deployment": None, "agent_machine_label": False, "state": "unbinding",
            })],
            "machine_state": DELETING,
        })
        calls, acts = make_mocks(
            host=mid_unbinding,
            machines=[DELETING, GONE],
            agents=[AGENT_UNBINDING, AGENT_DISCOVERING, AGENT_FREE],
        )
        async with _Harness(acts) as client:
            result = await _execute(client)

        assert result.hosted and not result.bound_at_start
        assert result.agent_state_at_start == "unbinding"
        assert result.machine_deleted_by_run is False
        assert calls["order"] == ["lookup", "inspect", "reserve", *HOSTED_HALF, "hold", *CHILD]
        assert calls["check_hosted_node"]  # the Node still checked: it was captured

    async def test_a_drain_that_never_finishes_fails_with_nothing_more_deleted(self):
        calls, acts = make_mocks(machines=[DELETING], agents=[AGENT_BOUND])
        async with _Harness(acts) as client:
            error = await _fails(client)

        assert error.type == "MachineDrainTimeoutError"
        assert "disruption budget" in error.message
        assert "pre-terminate.delete.hook.machine.cluster.x-k8s.io/agentmachine" in error.message
        assert "re-run" in error.message
        # Polled for the whole 20 minutes, the Machine deleted once, nothing after.
        assert len(calls["get_machine_state"]) > 10
        assert len(calls["ensure_machine_deleting"]) == 1
        assert calls["order"][-1] == "await-machine"
        assert len(calls["reserve_server"]) == 1  # no day-long hold
        assert calls["inspect_inventory_host"] == []  # no child

    async def test_an_agent_that_never_unbinds_fails_naming_its_state(self):
        calls, acts = make_mocks(machines=[DELETING, GONE], agents=[AGENT_BOUND, AGENT_UNBINDING])
        async with _Harness(acts) as client:
            error = await _fails(client)

        assert error.type == "AgentUnbindTimeoutError"
        assert "unbinding-pending-user-action" in error.message
        assert calls["inspect_inventory_host"] == []

    async def test_an_orphaned_node_fails_and_is_never_deleted(self):
        calls, acts = make_mocks(node_found=True)
        async with _Harness(acts) as client:
            error = await _fails(client)

        assert error.type == "HostedNodeStillPresentError"
        assert f"oc delete node {NODE}" in error.message
        assert calls["order"][-1] == "verify-node"
        assert calls["inspect_inventory_host"] == []

    async def test_a_lingering_agentmachine_is_named(self):
        _calls, acts = make_mocks(agent_machine_lingers=True)
        async with _Harness(acts) as client:
            error = await _fails(client)

        assert error.type == "ServerStillPresentError"
        assert "clusters-hc-01/hc-01-np-am-x7k2p" in error.message

    async def test_an_agent_left_unclean_after_the_machine_went_is_refused_to_the_child(self):
        not_approved = AGENT_FREE.model_copy(update={"approved": False})
        calls, acts = make_mocks(agents=[AGENT_BOUND, AGENT_UNBINDING, AGENT_FREE, not_approved])
        async with _Harness(acts) as client:
            error = await _fails(client)

        assert error.type == "AgentStillBoundError"
        assert "not approved" in error.message
        assert calls["inspect_inventory_host"] == []


class TestAnInventoryHost:
    async def test_it_skips_the_hosted_half_and_still_removes(self):
        calls, acts = make_mocks(membership=IN_INVENTORY, host=INVENTORY_HOST)
        async with _Harness(acts) as client:
            result = await _execute(client)

        assert calls["order"] == ["lookup", "inspect", "reserve", "hold", *CHILD]
        assert not result.hosted and not result.bound_at_start
        assert result.agent_state_at_start == "known-unbound"
        assert [s.split(":")[0] for s in result.skipped] == ["hosted-cluster half"]
        assert calls["ensure_machine_deleting"] == []
        assert result.remove_server.found and result.remove_server.powered_off

    async def test_an_available_server_is_accepted(self):
        available = INSTALLED.model_copy(update={
            "lifecycle_state": "AVAILABLE", "cluster_name": None, "mce_name": None,
        })
        _calls, acts = make_mocks(membership=available, host=INVENTORY_HOST)
        async with _Harness(acts) as client:
            result = await _execute(client)
        assert result.remove_server.found

    async def test_nothing_on_the_mce_completes_through_the_childs_found_false(self):
        calls, acts = make_mocks(
            membership=INSTALLED.model_copy(update={"lifecycle_state": "AVAILABLE",
                                                    "cluster_name": None, "mce_name": None}),
            host=BoundHostState(),
            inventory=InventoryHostState(),
        )
        async with _Harness(acts) as client:
            result = await _execute(client)

        assert result.remove_server.found is False
        assert calls["order"] == ["lookup", "inspect", "reserve", "hold", "child-inspect"]

    async def test_a_stale_installed_reading_after_an_unbind_is_not_a_contradiction(self):
        """A re-run's normal view: server-scan has not caught up with the unbinding yet."""
        _calls, acts = make_mocks(membership=INSTALLED, host=INVENTORY_HOST)
        async with _Harness(acts) as client:
            result = await _execute(client)
        assert result.remove_server.found


class TestRefusedBeforeAnyWrite:
    @pytest.mark.parametrize(
        ("membership", "type_", "words"),
        [
            (ServerMembership(found=False), "ServerNotInServerScanError", "has no server named"),
            (INSTALLED.model_copy(update={"mce_name": "mce-other"}),
             "ClusterMembershipMismatchError", "mce-other"),
            (INSTALLED.model_copy(update={"contested_with": "ocp4-hc-other"}),
             "ClusterMembershipMismatchError", "contested"),
        ],
    )
    async def test_what_server_scan_alone_contradicts(self, membership, type_, words):
        calls, acts = make_mocks(membership=membership)
        async with _Harness(acts) as client:
            error = await _fails(client)

        assert error.type == type_ and words in error.message
        assert "Nothing was changed" in error.message
        assert calls["order"] == ["lookup"]

    @pytest.mark.parametrize(
        ("membership", "host", "type_", "words"),
        [
            (INSTALLED.model_copy(update={"cluster_name": "hc-other"}), HOSTED_NODE,
             "ClusterMembershipMismatchError", "hc-other"),
            (IN_INVENTORY, HOSTED_NODE, "ClusterMembershipMismatchError", "INSTALLED_TO_INVENTORY"),
            (INSTALLED, HOSTED_NODE.model_copy(update={"agents": [
                BOUND_AGENT.model_copy(update={"agent_machine": None})]}),
             "NotHostedClusterNodeError", "no AgentMachine"),
            (INSTALLED, HOSTED_NODE.model_copy(update={"hosted_cluster": None}),
             "NotHostedClusterNodeError", "no HostedCluster"),
            (INSTALLED, HOSTED_NODE.model_copy(update={"agents": [
                BOUND_AGENT, BOUND_AGENT.model_copy(update={"name": "agent-2"})]}),
             "MultipleBoundAgentsError", "agent-2"),
        ],
    )
    async def test_what_the_mce_contradicts(self, membership, host, type_, words):
        calls, acts = make_mocks(membership=membership, host=host)
        async with _Harness(acts) as client:
            error = await _fails(client)

        assert error.type == type_ and words in error.message
        assert calls["order"] == ["lookup", "inspect"]

    async def test_a_lock_another_run_holds_fails_the_run(self):
        calls, acts = make_mocks(reserve_error=ServerReservedError(
            f"{SERVER} is reserved by 'install-server' for MCE 'ocp4-mce-bat-yam-01'"
        ))
        async with _Harness(acts) as client:
            error = await _fails(client)

        assert error.type == "ServerReservedError"
        assert "install-server" in error.message and "Nothing was changed" in error.message
        assert calls["order"] == ["lookup", "inspect", "reserve"]
        assert len(calls["reserve_server"]) == 1  # non-retryable


class TestTheInventoryHalf:
    async def test_a_hold_refused_at_the_end_still_removes_the_host(self):
        _calls, acts = make_mocks(
            membership=IN_INVENTORY, host=INVENTORY_HOST,
            hold_error=ServerReservedError("reserved by 'install-server' on another MCE"),
        )
        async with _Harness(acts) as client:
            result = await _execute(client)

        assert result.reservation_expires_at is None
        assert result.remove_server.found

    async def test_remove_server_already_running_is_a_named_error(self):
        calls, acts = make_mocks(membership=IN_INVENTORY, host=INVENTORY_HOST)
        child_id = f"remove-server-{SERVER.lower()}"
        async with _Harness(acts) as client:
            await client.start_workflow("Blocker", id=child_id, task_queue=_BLOCKER_QUEUE)
            error = await _fails(client)

        assert error.type == "RemoveServerAlreadyRunningError"
        assert child_id in error.message
        assert calls["inspect_inventory_host"] == []

    async def test_a_failed_child_fails_the_uninstall_with_its_cause(self):
        # remove-server's own refusal: an Agent bound since this run inspected.
        rebound = ON_THE_MCE.model_copy(update={
            "agents": [AgentBinding(name="agent-1", cluster_deployment="clusters-hc-02/hc-02")]
        })
        _calls, acts = make_mocks(membership=IN_INVENTORY, host=INVENTORY_HOST, inventory=rebound)
        async with _Harness(acts) as client:
            error = await _fails(client)

        assert error.type == "RemoveServerFailedError"
        assert "AgentBoundToClusterError" in error.message
        assert f"remove-server-{SERVER.lower()}" in error.message


async def test_every_step_runs_on_the_target_mces_queue():
    """Registered only on that MCE's queue: anywhere else and the run could not finish."""
    calls, acts = make_mocks()
    async with _Harness(acts) as client:
        result = await _execute(client)
    assert result.mce_cluster == MCE
    assert calls["inspect_bound_host"] == [BmhRef(server_name=SERVER, namespace=NS)]


# --- error classification, as install-server's is guarded ---------------------

_MODULE = pathlib.Path(uninstall_server.__file__)


def _workflow_raised() -> set[str]:
    return {
        name
        for name, obj in vars(exceptions).items()
        if isinstance(obj, type)
        and issubclass(obj, OrchestratorError)
        and (obj.__doc__ or "").startswith("WORKFLOW-RAISED")
    }


def test_the_non_retryable_list_names_only_activity_raised_classes():
    listed = set(uninstall_server._RETRY_POLICY.non_retryable_error_types or [])
    assert listed and all(isinstance(getattr(exceptions, n, None), type) for n in listed)
    assert not listed & _workflow_raised()


def test_every_raised_type_comes_from_a_class_never_a_literal():
    tree = ast.parse(_MODULE.read_text())
    keywords = [
        keyword.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "ApplicationError"
        for keyword in node.keywords
        if keyword.arg == "type"
    ]
    assert keywords
    for value in keywords:
        assert isinstance(value, ast.Attribute) and value.attr == "__name__", ast.unparse(value)
        assert isinstance(getattr(exceptions, value.value.id, None), type), value.value.id
