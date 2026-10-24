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
    AgentMachineCleanup,
    AgentMachineCleanupRequest,
    AgentMachineRef,
    AgentMachineState,
    AgentNameRef,
    AgentRelease,
    AgentReleaseRequest,
    BmhRef,
    BmhState,
    BoundHostState,
    HostAgent,
    HostedClusterRef,
    HostedNode,
    HostedNodeAction,
    HostedNodeDeletion,
    HostedNodeDrain,
    HostedNodeQuery,
    HostedNodeRef,
    HostedNodes,
    HostedNodeState,
    HostPowerRequest,
    InventoryHostState,
    MachineDeletion,
    MachineHookRelease,
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

AM = "clusters-hc-01/hc-01-np-am-x7k2p"
BOUND_AGENT = HostAgent(
    name="agent-1",
    namespace=NS,
    cluster_deployment="clusters-hc-01/hc-01",
    state="added-to-existing-cluster",
    approved=True,
    agent_machine=AM,
    agent_machine_label=True,
    hostname=NODE,
    hcp_namespaces=["clusters-hc-01"],
)
LIVE_MACHINE = MachineState(
    found=True, node_name=NODE, phase="Running", provider_hook=True,
    agent_machine=AM, agent_machine_found=True,
)
DELETING = MachineState(
    found=True,
    deleting=True,
    deletion_timestamp="2026-10-24T08:00:00Z",
    node_name=NODE,
    phase="Deleting",
    pending_hooks=["pre-terminate.delete.hook.machine.cluster.x-k8s.io/agentmachine"],
    conditions=["DrainingSucceeded=False (Draining): Cannot evict pod as it would violate the pod's disruption budget"],
    provider_hook=True,
    agent_machine=AM,
    agent_machine_found=True,
)
GONE = MachineState(found=False)

# The server as its hosted cluster sees it: the Node its Machine names.
THE_NODE = HostedNode(name=NODE, provider_id="agent://agent-1", ready=True,
                      matched_by=["provider-id", "name"])

HOSTED_NODE = BoundHostState(
    bmh_found=True,
    bmh_operational_status="OK",
    infra_env="cisco-m6-bat-yam-64c-512gb",
    agents=[BOUND_AGENT],
    machine=MACHINE,
    machine_state=LIVE_MACHINE,
    hosted_cluster=HC,
    machines_matched=["clusters-hc-01/hc-01-np-x7k2p"],
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
    rechecks: list[BoundHostState] | None = None,
    deletion: MachineDeletion | None = None,
    machines: list[MachineState] | None = None,
    agents: list[AgentBindingState] | None = None,
    agent_machine_lingers: bool = False,
    node_found: bool = False,
    nodes: list[list[HostedNode]] | None = None,
    nodes_unresolved: str | None = None,
    drains: list[HostedNodeDrain] | None = None,
    cleanups: list[AgentMachineCleanup] | None = None,
    stuck_finalizer: bool = False,
    reserve_error: Exception | None = None,
    hold_error: Exception | None = None,
    inventory: InventoryHostState = ON_THE_MCE,
):
    """Each sequence is consumed one item per call; its last item repeats.

    `nodes` is what find_hosted_nodes answers, call after call: by default the
    hosted node's own Node at the first look and none once its Machine went.
    `rechecks` is what inspect_bound_host answers after the lock is taken (an
    inventory host's last look); by default `host` again.
    """
    recheck_seq = list(rechecks or [host])
    machine_seq = list(machines or MACHINE_TIMELINE)
    agent_seq = list(agents or AGENT_TIMELINE)
    nodes_seq = list(nodes if nodes is not None else ([[THE_NODE], []] if host.machine else [[]]))
    drain_seq = list(drains or [HostedNodeDrain(found=True, remaining=[])])
    cleanup_seq = list(cleanups or [AgentMachineCleanup()])
    calls: dict[str, list] = {
        k: []
        for k in (
            "lookup_server_membership", "inspect_bound_host", "reserve_server",
            "ensure_machine_deleting", "get_machine_state", "get_agent_binding",
            "get_agent_machine_state", "check_hosted_node", "get_baremetal_host",
            "inspect_inventory_host", "set_host_online", "teardown_bmh_resources",
            "delete_agents", "remove_host_pxe_entries", "find_hosted_nodes",
            "drain_hosted_node", "delete_hosted_node", "release_agent",
            "release_machine_hook", "clean_up_agent_machines", "order",
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
        if not calls["reserve_server"]:
            _mark("inspect")
            return host
        _mark("verify-inventory")
        return recheck_seq[min(len(calls["inspect_bound_host"]) - 2, len(recheck_seq) - 1)]

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
        _mark("verify-agentmachine-old")
        return AgentMachineState(found=agent_machine_lingers, deleting=agent_machine_lingers)

    @activity.defn
    async def check_hosted_node(ref: HostedNodeRef) -> HostedNodeState:
        calls["check_hosted_node"].append(ref)
        _mark("verify-node-old")
        return HostedNodeState(found=node_found, node_name=ref.node_name)

    @activity.defn
    async def find_hosted_nodes(query: HostedNodeQuery) -> HostedNodes:
        calls["find_hosted_nodes"].append(query)
        _mark("verify-node" if "reserve" in calls["order"] else "find-nodes")
        if nodes_unresolved is not None:
            return HostedNodes(reason=nodes_unresolved)
        return HostedNodes(hosted_cluster=query.hosted_cluster or HC,
                           nodes=_next(nodes_seq, "find_hosted_nodes"))

    @activity.defn
    async def drain_hosted_node(action: HostedNodeAction) -> HostedNodeDrain:
        calls["drain_hosted_node"].append(action)
        _mark("drain-node")
        return _next(drain_seq, "drain_hosted_node")

    @activity.defn
    async def delete_hosted_node(action: HostedNodeAction) -> HostedNodeDeletion:
        calls["delete_hosted_node"].append(action)
        _mark("delete-node")
        return HostedNodeDeletion(found=True, deleted=True)

    @activity.defn
    async def release_agent(request: AgentReleaseRequest) -> AgentRelease:
        calls["release_agent"].append(request)
        _mark("release-agent")
        return AgentRelease(found=True, released=True)

    @activity.defn
    async def release_machine_hook(ref: MachineRef) -> MachineHookRelease:
        calls["release_machine_hook"].append(ref)
        _mark("release-hook")
        return MachineHookRelease(found=True, released=True)

    @activity.defn
    async def clean_up_agent_machines(request: AgentMachineCleanupRequest) -> AgentMachineCleanup:
        calls["clean_up_agent_machines"].append(request)
        _mark("verify-agentmachine")
        if agent_machine_lingers:
            return AgentMachineCleanup(remaining=[f"{AM} (deleting; finalizers: someone-else)"])
        if stuck_finalizer:
            if request.clear_finalizers:
                return AgentMachineCleanup(finalizers_cleared=[AM])
            first = len(calls["clean_up_agent_machines"]) == 1
            return AgentMachineCleanup(
                deleted=[AM] if first else [],
                remaining=[f"{AM} (deleting; finalizers: agentmachine.agent-install.openshift.io/deprovision)"],
            )
        return _next(cleanup_seq, "clean_up_agent_machines")

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
            delete_agents, remove_host_pxe_entries, find_hosted_nodes,
            drain_hosted_node, delete_hosted_node, release_agent,
            release_machine_hook, clean_up_agent_machines,
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

        assert calls["order"] == ["lookup", "inspect", "find-nodes", "reserve", *HOSTED_HALF,
                                  "hold", *CHILD]
        assert result.hosted and result.bound_at_start
        assert result.agent_state_at_start == "added-to-existing-cluster"
        assert result.machine_deleted_by_run is True
        assert (result.hosted_cluster, result.hcp_namespace) == ("clusters/hc-01", "clusters-hc-01")
        assert result.machine == "clusters-hc-01/hc-01-np-x7k2p"
        assert result.agent_machine == "clusters-hc-01/hc-01-np-am-x7k2p"
        assert result.node_name == NODE
        assert result.skipped == []
        # Nothing was left over, so nothing was done on HyperShift's behalf: the
        # Machine was deleted once and everything else went the normal way.
        assert result.leftovers_at_start == [] and result.cleaned_up == []
        assert calls["ensure_machine_deleting"] == [MACHINE]
        assert calls["release_agent"] == calls["release_machine_hook"] == []
        assert calls["drain_hosted_node"] == calls["delete_hosted_node"] == []
        # The Node is looked up by every identity — CAPI's providerID, the
        # Machine's nodeRef, the Agent's hostname — before any write and again
        # once the Machine went.
        first, *after = calls["find_hosted_nodes"]
        assert first == HostedNodeQuery(hosted_cluster=HC, server_name=SERVER, agent_name="agent-1",
                                        hostname=NODE, node_name=NODE)
        assert after and all(q.hosted_cluster == HC for q in after)
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
        assert reserve.ttl_seconds == int(uninstall_server._LEFTOVERS_RESERVATION_TTL.total_seconds())
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

    async def test_a_machine_with_no_node_still_has_its_node_looked_for(self):
        """No nodeRef is not proof of no Node: CAPI's providerID and the hostname are checked."""
        no_node = LIVE_MACHINE.model_copy(update={"node_name": None})
        calls, acts = make_mocks(
            host=HOSTED_NODE.model_copy(update={"machine_state": no_node}),
            machines=[GONE],
            agents=[AGENT_FREE],
            nodes=[[]],
        )
        async with _Harness(acts) as client:
            result = await _execute(client)

        assert len(calls["find_hosted_nodes"]) == 2  # before any write, and at the end
        assert all(q.agent_name == "agent-1" and q.hostname == NODE and q.node_name is None
                   for q in calls["find_hosted_nodes"])
        assert result.node_name is None
        assert any(s.startswith("checking-hosted-node:") and "has no Node" in s
                   for s in result.skipped)
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
        assert calls["order"] == ["lookup", "inspect", "find-nodes", "reserve", *HOSTED_HALF,
                                  "hold", *CHILD]
        assert calls["release_agent"] == []  # the provider released it: its Machine is there

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

    async def test_a_node_capi_left_behind_is_deleted_once_not_ready(self):
        stale = THE_NODE.model_copy(update={"ready": False})
        calls, acts = make_mocks(nodes=[[THE_NODE], [stale], []])
        async with _Harness(acts) as client:
            result = await _execute(client)

        assert calls["delete_hosted_node"] == [
            HostedNodeAction(hosted_cluster=HC, node_name=NODE, provider_id="agent://agent-1")
        ]
        assert any(c.startswith(f"deleted Node {NODE}") for c in result.cleaned_up)
        assert result.remove_server.found

    async def test_a_node_still_ready_after_the_host_left_fails_and_is_not_deleted(self):
        calls, acts = make_mocks(nodes=[[THE_NODE]])
        async with _Harness(acts) as client:
            error = await _fails(client)

        assert error.type == "HostedNodeStillPresentError"
        assert NODE in error.message and "still Ready" in error.message
        assert calls["delete_hosted_node"] == []
        assert calls["order"][-1] == "verify-node"
        assert calls["inspect_inventory_host"] == []

    async def test_a_lingering_agentmachine_is_named(self):
        _calls, acts = make_mocks(agent_machine_lingers=True)
        async with _Harness(acts) as client:
            error = await _fails(client)

        assert error.type == "ServerStillPresentError"
        assert AM in error.message and "someone-else" in error.message

    async def test_an_agent_left_unclean_after_the_machine_went_is_refused_to_the_child(self):
        not_approved = AGENT_FREE.model_copy(update={"approved": False})
        calls, acts = make_mocks(agents=[AGENT_BOUND, AGENT_UNBINDING, AGENT_FREE, not_approved])
        async with _Harness(acts) as client:
            error = await _fails(client)

        assert error.type == "AgentStillBoundError"
        assert "not approved" in error.message
        assert calls["inspect_inventory_host"] == []


# --- what a broken HyperShift deletion leaves behind ----------------------------------

# The Agent provider unbinds only from a Machine's pre-terminate hook, through
# its AgentMachine; each shape below is one where that can no longer happen.
NO_MACHINE = HOSTED_NODE.model_copy(update={
    "machine": None, "machine_state": None, "machines_matched": [],
})
ONLY_THE_BINDING = NO_MACHINE.model_copy(update={
    "agents": [BOUND_AGENT.model_copy(update={"agent_machine": None})],
})
# Its AgentMachine is gone, so nothing will ever remove the provider's hook.
AT_THE_HOOK = DELETING.model_copy(update={
    "agent_machine_found": False,
    "at_pre_terminate_hook": True,
    "conditions": ["Deleting=True (WaitingForPreTerminateHook): agentmachine"],
})
NO_AGENT_MACHINE = HOSTED_NODE.model_copy(update={
    "agents": [BOUND_AGENT.model_copy(update={"agent_machine": None})],
    "machine_state": LIVE_MACHINE.model_copy(update={"agent_machine_found": False}),
})
LOST_NODE = THE_NODE.model_copy(update={"matched_by": ["provider-id"]})
AGENT_RECLAIM_FAILED = AgentBindingState(
    found=True, state="unbinding-pending-user-action", approved=True,
)
RELEASED_AND_FREE = [AGENT_BOUND, AGENT_UNBINDING, AGENT_DISCOVERING, AGENT_FREE]

# The NodePool's replacement for the deleted Machine, holding this server again.
NEW_AM = "clusters-hc-01/hc-01-np-am-q9z4d"
RETAKEN = AgentBindingState(
    found=True, cluster_deployment="clusters-hc-01/hc-01", agent_machine_label=True,
    agent_machine=NEW_AM, state="binding", approved=True,
)


class TestHyperShiftLeftovers:
    async def test_an_agentmachine_with_no_machine_is_released_and_removed(self):
        calls, acts = make_mocks(
            host=NO_MACHINE, nodes=[[]], agents=RELEASED_AND_FREE,
            cleanups=[AgentMachineCleanup(deleted=[AM])],
        )
        async with _Harness(acts) as client:
            result = await _execute(client)

        assert result.leftovers_at_start == [f"AgentMachine {AM} has no Machine"]
        # Nothing to delete the normal way: the Agent is released by the run —
        # the provider's own write — and the AgentMachine deleted after it.
        assert calls["ensure_machine_deleting"] == []
        assert calls["release_agent"] == [AgentReleaseRequest(
            agent=AgentNameRef(namespace=NS, name="agent-1"),
            cluster_deployment="clusters-hc-01/hc-01",
        )]
        order = calls["order"]
        assert order.index("release-agent") < order.index("verify-agentmachine")
        assert order[:4] == ["lookup", "inspect", "find-nodes", "reserve"]
        [cleanup, *_] = calls["clean_up_agent_machines"]
        assert cleanup.known == [AgentMachineRef(namespace="clusters-hc-01", name="hc-01-np-am-x7k2p")]
        assert [c.split(" — ")[0] for c in result.cleaned_up] == [
            "released Agent agent-1 from clusters-hc-01/hc-01",
            f"deleted AgentMachine {AM}",
        ]
        assert result.remove_server.found

    async def test_a_machine_whose_agentmachine_is_gone_has_the_providers_hook_released(self):
        calls, acts = make_mocks(
            host=NO_AGENT_MACHINE,
            machines=[AT_THE_HOOK, AT_THE_HOOK, GONE],
            agents=[AGENT_BOUND, AGENT_RECLAIM_FAILED, AGENT_FREE],
        )
        async with _Harness(acts) as client:
            result = await _execute(client)

        assert result.leftovers_at_start == [
            f"Machine clusters-hc-01/hc-01-np-x7k2p has no AgentMachine ({AM} is gone)"
        ]
        assert calls["ensure_machine_deleting"] == [MACHINE]
        # Released at the hook, then — once the Agent reached a state the
        # provider itself waits for — the hook removed.
        assert len(calls["release_agent"]) == 1
        assert calls["release_machine_hook"] == [MACHINE]
        order = calls["order"]
        assert order.index("release-agent") < order.index("release-hook")
        assert any(c.startswith("removed the Agent provider's pre-terminate hook")
                   for c in result.cleaned_up)
        assert result.remove_server.found

    async def test_the_hook_waits_for_a_state_the_provider_would_release_in(self):
        """`unbinding` is not one of them: the run holds the hook as the provider would."""
        calls, acts = make_mocks(
            host=NO_AGENT_MACHINE,
            machines=[AT_THE_HOOK],
            agents=[AGENT_BOUND, AgentBindingState(found=True, state="unbinding", approved=True)],
        )
        async with _Harness(acts) as client:
            error = await _fails(client)

        assert error.type == "AgentUnbindTimeoutError"
        assert calls["release_machine_hook"] == []

    async def test_a_node_with_no_machine_or_agentmachine_is_drained_released_and_deleted(self):
        stale = LOST_NODE.model_copy(update={"ready": False})
        calls, acts = make_mocks(
            host=ONLY_THE_BINDING,
            nodes=[[LOST_NODE], [stale], []],
            drains=[
                HostedNodeDrain(found=True, cordoned=True, evicted=["app/web-1", "app/web-2"],
                                remaining=["app/web-1", "app/web-2"]),
                HostedNodeDrain(found=True, remaining=[]),
            ],
            agents=RELEASED_AND_FREE,
        )
        async with _Harness(acts) as client:
            result = await _execute(client)

        assert result.leftovers_at_start == [
            ("Agent agent-1 is still bound to clusters-hc-01/hc-01 with no AgentMachine or "
             "Machine behind it"),
            f"Node {NODE} has no Machine behind it (Ready)",
        ]
        # Drained BEFORE the release that reboots the host; the Node deleted
        # only after the host left it.
        order = calls["order"]
        assert order.index("drain-node") < order.index("release-agent") < order.index("delete-node")
        assert calls["drain_hosted_node"][0] == HostedNodeAction(
            hosted_cluster=HC, node_name=NODE, provider_id="agent://agent-1"
        )
        assert [c.split(" — ")[0] for c in result.cleaned_up] == [
            f"drained Node {NODE} of hosted cluster clusters/hc-01",
            "released Agent agent-1 from clusters-hc-01/hc-01",
            f"deleted Node {NODE} of hosted cluster clusters/hc-01",
        ]
        assert "2 Pod(s) evicted" in result.cleaned_up[0]
        assert result.remove_server.found

    async def test_a_lost_node_that_never_drains_fails_before_the_release(self):
        calls, acts = make_mocks(
            host=ONLY_THE_BINDING,
            nodes=[[LOST_NODE]],
            drains=[HostedNodeDrain(
                found=True, remaining=["app/db-0"],
                blocked=["app/db-0: Cannot evict pod as it would violate the pod's disruption budget."],
            )],
        )
        async with _Harness(acts) as client:
            error = await _fails(client)

        assert error.type == "NodeDrainTimeoutError"
        assert "app/db-0" in error.message and "disruption budget" in error.message
        assert "uncordon" in error.message
        assert calls["release_agent"] == [] and calls["inspect_inventory_host"] == []

    async def test_a_not_ready_lost_node_is_not_drained(self):
        stale = LOST_NODE.model_copy(update={"ready": False})
        calls, acts = make_mocks(host=ONLY_THE_BINDING, nodes=[[stale], []], agents=RELEASED_AND_FREE)
        async with _Harness(acts) as client:
            result = await _execute(client)

        assert calls["drain_hosted_node"] == []
        assert any(s.startswith("draining-node:") and "NotReady" in s for s in result.skipped)

    async def test_an_agentmachine_stuck_on_the_providers_finalizer_has_it_dropped(self):
        calls, acts = make_mocks(host=NO_MACHINE, nodes=[[]], agents=RELEASED_AND_FREE,
                                 stuck_finalizer=True)
        async with _Harness(acts) as client:
            result = await _execute(client)

        flags = [r.clear_finalizers for r in calls["clean_up_agent_machines"]]
        # The provider gets the grace period first; then the run drops it.
        assert flags[0] is False and flags[-1] is True and flags.count(True) == 1
        assert any(c.startswith(f"dropped the Agent provider's finalizer from AgentMachine {AM}")
                   for c in result.cleaned_up)
        assert result.remove_server.found

    async def test_a_machine_taking_the_host_again_fails_the_verification(self):
        calls, acts = make_mocks(
            host=NO_MACHINE, nodes=[[]], agents=RELEASED_AND_FREE,
            cleanups=[AgentMachineCleanup(held_by_machine=[
                "clusters-hc-01/hc-01-np-am-new (Machine clusters-hc-01/hc-01-np-new)"])],
        )
        async with _Harness(acts) as client:
            error = await _fails(client)

        assert error.type == "ServerStillPresentError"
        assert "hc-01-np-new" in error.message and "replacement" in error.message
        assert calls["inspect_inventory_host"] == []

    async def test_a_ready_node_nothing_links_any_more_is_refused_before_any_write(self):
        calls, acts = make_mocks(
            membership=INSTALLED, host=INVENTORY_HOST,
            nodes=[[THE_NODE.model_copy(update={"matched_by": ["name"]})]],
        )
        async with _Harness(acts) as client:
            error = await _fails(client)

        assert error.type == "HostedNodeStillPresentError"
        assert "Nothing was changed" in error.message and NODE in error.message
        assert calls["order"] == ["lookup", "inspect", "find-nodes"]

    async def test_a_stale_node_nothing_links_any_more_is_deleted(self):
        stale = THE_NODE.model_copy(update={"ready": False, "matched_by": ["name"]})
        calls, acts = make_mocks(membership=INSTALLED, host=INVENTORY_HOST,
                                 nodes=[[stale], [stale], []])
        async with _Harness(acts) as client:
            result = await _execute(client)

        assert result.hosted and calls["delete_hosted_node"]
        assert calls["release_agent"] == []  # nothing bound to release
        assert any(s.startswith("verifying-agent:") for s in result.skipped)
        assert result.remove_server.found

    async def test_two_nodes_that_are_the_server_are_refused(self):
        other = THE_NODE.model_copy(update={"name": "worker-7", "matched_by": ["provider-id"]})
        calls, acts = make_mocks(nodes=[[THE_NODE, other]])
        async with _Harness(acts) as client:
            error = await _fails(client)

        assert error.type == "ConflictingHostedObjectsError"
        assert "worker-7" in error.message
        assert calls["order"] == ["lookup", "inspect", "find-nodes"]

    async def test_a_cluster_server_scan_names_but_the_mce_lacks_is_a_skip(self):
        _calls, acts = make_mocks(
            membership=INSTALLED, host=INVENTORY_HOST,
            nodes_unresolved="no HostedCluster on this MCE is named hc-01",
        )
        async with _Harness(acts) as client:
            result = await _execute(client)

        assert "checking-hosted-node: no HostedCluster on this MCE is named hc-01" in result.skipped
        assert result.remove_server.found


class TestTheNodePoolTakingTheServerBack:
    """Replicas are not the run's: the NodePool replaces the Machine it deletes — with
    this very server when it is the only free Agent the NodePool's selector matches."""

    async def test_the_run_stops_and_never_releases_it_from_the_replacement(self):
        labelled = HOSTED_NODE.model_copy(update={
            "agents": [BOUND_AGENT.model_copy(update={"labelled_agent_machine": AM})],
        })
        calls, acts = make_mocks(
            host=labelled,
            machines=[DELETING, GONE],
            agents=[AGENT_BOUND.model_copy(update={"agent_machine": AM}), AGENT_UNBINDING, RETAKEN],
        )
        async with _Harness(acts) as client:
            error = await _fails(client)

        assert error.type == "AgentRetakenError"
        assert NEW_AM in error.message and "Free another Agent" in error.message
        assert calls["ensure_machine_deleting"] == [MACHINE]
        # Bound into the same control plane, so a release would have passed its
        # own checks — and unbound a node the provider is installing.
        assert calls["release_agent"] == [] and calls["release_machine_hook"] == []
        assert calls["inspect_inventory_host"] == []  # no child
        assert len(calls["reserve_server"]) == 1  # no day-long hold

    async def test_a_label_naming_the_agentmachine_already_gone_is_not_a_retake(self):
        """A broken deletion can leave the provider's label behind: that Agent is the run's to release."""
        stale = BOUND_AGENT.model_copy(update={"agent_machine": None, "labelled_agent_machine": AM})
        calls, acts = make_mocks(
            host=NO_AGENT_MACHINE.model_copy(update={"agents": [stale]}),
            machines=[AT_THE_HOOK, AT_THE_HOOK, GONE],
            agents=[AGENT_BOUND.model_copy(update={"agent_machine": AM}), AGENT_RECLAIM_FAILED, AGENT_FREE],
        )
        async with _Harness(acts) as client:
            result = await _execute(client)

        assert len(calls["release_agent"]) == 1 and calls["release_machine_hook"] == [MACHINE]
        assert result.remove_server.found

    def test_a_limb_predating_the_field_never_reads_as_a_retake(self):
        # Built in-process, so an unsent key is really absent — as in an old limb's payload.
        old_inspection = HostAgent(name="agent-1", namespace=NS, agent_machine=AM)
        assert uninstall_server._started_from(old_inspection) is None
        assert uninstall_server._retaken_by(RETAKEN, None) is None

        started = uninstall_server._started_from(old_inspection.model_copy(
            update={"labelled_agent_machine": None}))
        assert started == frozenset({AM})
        old_poll = AgentBindingState(found=True, cluster_deployment="clusters-hc-01/hc-01",
                                     agent_machine_label=True)
        assert uninstall_server._retaken_by(old_poll, started) is None
        assert uninstall_server._retaken_by(RETAKEN, started) == NEW_AM
        assert uninstall_server._retaken_by(AGENT_UNBINDING.model_copy(
            update={"agent_machine": None}), started) is None


class TestAnInventoryHost:
    async def test_it_skips_the_hosted_half_and_still_removes(self):
        calls, acts = make_mocks(membership=IN_INVENTORY, host=INVENTORY_HOST)
        async with _Harness(acts) as client:
            result = await _execute(client)

        # Looked at again, fresh, right before remove-server: a free Agent is
        # what any NodePool's new Machine takes.
        assert calls["order"] == ["lookup", "inspect", "reserve", "verify-inventory", "hold", *CHILD]
        assert len(calls["inspect_bound_host"]) == 2
        assert not result.hosted and not result.bound_at_start
        assert result.agent_state_at_start == "known-unbound"
        assert [s.split(":")[0] for s in result.skipped] == [
            "checking-hosted-node", "hosted-cluster half",
        ]
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
        assert calls["order"] == ["lookup", "inspect", "reserve", "verify-inventory", "hold",
                                  "child-inspect"]

    async def test_a_stale_installed_reading_after_an_unbind_is_not_a_contradiction(self):
        """A re-run's normal view: server-scan has not caught up with the unbinding yet."""
        calls, acts = make_mocks(membership=INSTALLED, host=INVENTORY_HOST)
        async with _Harness(acts) as client:
            result = await _execute(client)
        assert result.remove_server.found
        # The cluster server-scan names is still asked whether the server is a Node there.
        [query] = calls["find_hosted_nodes"]
        assert query.hosted_cluster is None and query.cluster_name == "hc-01"


    async def test_taken_by_a_nodepool_after_the_inspection_it_is_left_to_install(self):
        taken = INVENTORY_HOST.model_copy(update={"agents": [HostAgent(
            name="agent-1", namespace=NS, cluster_deployment="clusters-hc-01/hc-01",
            state="binding", approved=True, agent_machine=AM, agent_machine_label=True,
            hcp_namespaces=["clusters-hc-01"],
        )]})
        calls, acts = make_mocks(membership=IN_INVENTORY, host=INVENTORY_HOST, rechecks=[taken])
        async with _Harness(acts) as client:
            error = await _fails(client)

        assert error.type == "AgentRetakenError"
        assert AM in error.message and "clusters-hc-01/hc-01" in error.message
        assert calls["inspect_inventory_host"] == []  # no child: nothing powered off or deleted
        assert len(calls["reserve_server"]) == 1  # no day-long hold

    async def test_a_machine_linked_since_the_inspection_stops_the_removal(self):
        linked = INVENTORY_HOST.model_copy(update={
            "agents": [HostAgent(name="agent-1", namespace=NS, state="known-unbound", approved=True,
                                 hcp_namespaces=["clusters-hc-01"])],
            "machines_matched": ["clusters-hc-01/hc-01-np-x7k2p"],
        })
        calls, acts = make_mocks(membership=IN_INVENTORY, host=INVENTORY_HOST, rechecks=[linked])
        async with _Harness(acts) as client:
            error = await _fails(client)

        assert error.type == "ServerStillPresentError"
        assert "Machine clusters-hc-01/hc-01-np-x7k2p is linked to it" in error.message
        assert calls["inspect_inventory_host"] == []

    async def test_a_run_recorded_before_the_recheck_goes_straight_to_the_hold(self, monkeypatch):
        monkeypatch.setattr(
            workflow, "patched",
            lambda patch_id: patch_id != uninstall_server._INVENTORY_VERIFY_PATCH,
        )
        calls, acts = make_mocks(membership=IN_INVENTORY, host=INVENTORY_HOST)
        async with _Harness(acts) as client:
            result = await _execute(client)

        assert calls["order"] == ["lookup", "inspect", "reserve", "hold", *CHILD]
        assert result.remove_server.found


KEEP = INPUT.model_copy(update={"keep_inventory": True})


class TestKeepInventory:
    """keep_inventory: out of the hosted cluster, and no further — no remove-server."""

    async def test_a_hosted_node_leaves_its_cluster_and_stays_in_the_inventory(self):
        calls, acts = make_mocks()
        async with _Harness(acts) as client:
            result = await _execute(client, KEEP)

        # The whole hosted-cluster half and the day-long hold, then nothing more.
        assert calls["order"] == ["lookup", "inspect", "find-nodes", "reserve", *HOSTED_HALF, "hold"]
        assert result.kept_in_inventory and result.hosted and result.machine_deleted_by_run
        assert result.remove_server is None and result.remove_server_workflow_id is None
        for untouched in ("inspect_inventory_host", "set_host_online", "teardown_bmh_resources",
                          "delete_agents", "remove_host_pxe_entries"):
            assert calls[untouched] == [], untouched
        assert result.reservation_expires_at == "2026-10-25T09:00:00Z"
        assert result.skipped[-1].startswith("removing-from-inventory: keep_inventory was asked")
        assert "InfraEnv cisco-m6-bat-yam-64c-512gb" in result.skipped[-1]

    async def test_an_inventory_host_has_nothing_to_do_and_takes_no_lock(self):
        calls, acts = make_mocks(membership=IN_INVENTORY, host=INVENTORY_HOST)
        async with _Harness(acts) as client:
            result = await _execute(client, KEEP)

        assert calls["order"] == ["lookup", "inspect"]
        assert calls["reserve_server"] == []
        assert result.kept_in_inventory and not result.hosted and result.remove_server is None
        assert [s.split(":")[0] for s in result.skipped] == [
            "checking-hosted-node", "hosted-cluster half", "removing-from-inventory",
        ]

    async def test_the_default_still_takes_it_out_of_the_mce(self):
        _calls, acts = make_mocks()
        async with _Harness(acts) as client:
            result = await _execute(client)
        assert not result.kept_in_inventory and result.remove_server.found


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
                BOUND_AGENT.model_copy(update={"agent_machine": None, "hcp_namespaces": []})]}),
             "NotHostedClusterNodeError", "no AgentMachine or Machine"),
            (INSTALLED, HOSTED_NODE.model_copy(update={"machines_matched": [
                "clusters-hc-01/hc-01-np-x7k2p", "clusters-hc-01/hc-01-np-old"]}),
             "ConflictingHostedObjectsError", "hc-01-np-old"),
            (INSTALLED, HOSTED_NODE.model_copy(update={"agents": [
                BOUND_AGENT.model_copy(update={"hcp_namespaces": ["clusters-hc-01", "clusters-hc-02"]})]}),
             "ConflictingHostedObjectsError", "clusters-hc-02"),
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
        assert calls["order"] == ["lookup", "inspect", "find-nodes", "reserve"]
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


async def test_a_run_recorded_before_the_leftover_patch_keeps_its_flow(monkeypatch):
    """`workflow.patched` False — a run replaying from older history — takes the old path."""
    monkeypatch.setattr(
        workflow, "patched",
        lambda patch_id: patch_id != uninstall_server._LEFTOVERS_PATCH,
    )
    calls, acts = make_mocks(machines=[DELETING, GONE], agents=[AGENT_BOUND, AGENT_FREE])
    async with _Harness(acts) as client:
        result = await _execute(client)

    assert calls["order"] == ["lookup", "inspect", "reserve", "delete-machine", "await-machine",
                              "verify-agentmachine-old", "verify-node-old", "hold", *CHILD]
    assert calls["find_hosted_nodes"] == calls["clean_up_agent_machines"] == []
    assert calls["check_hosted_node"] == [HostedNodeRef(hosted_cluster=HC, node_name=NODE)]
    assert calls["reserve_server"][0].ttl_seconds == int(uninstall_server._RESERVATION_TTL.total_seconds())
    assert result.remove_server.found


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
