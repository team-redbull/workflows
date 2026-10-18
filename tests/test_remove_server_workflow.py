"""remove-server: real RemoveServerWorkflow, mock activities, time-skipping env.

The mocks are registered ONLY on the target MCE's activity queue — every step
of a removal acts on that MCE — so a dispatch anywhere else would hang the run.
The time-skipping environment collapses the power-off poll's durable timers.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest
from temporalio import activity
from temporalio.client import Client, WorkflowFailureError
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from shared.consts import REMOVE_SERVER_WORKFLOW_QUEUE, server_lifecycle_activity_queue
from shared.exceptions import AgentBoundToClusterError
from shared.models.server_lifecycle import (
    AgentBinding,
    AgentDeleteRequest,
    AgentDeletion,
    BmhRef,
    BmhState,
    HostPowerRequest,
    InventoryHostState,
    RemoveServerInput,
    RemoveServerRunArgs,
    TeardownResult,
)
from workflow_domains.server_lifecycle.remove_server import RemoveServerWorkflow

MCE = "ocp4-mce-bat-yam-01"
QUEUE = server_lifecycle_activity_queue(MCE)
SERVER = "ocp-cisco-m6-bat-yam-64c-512gb-FCH0001"
NS = "inventory"
INPUT = RemoveServerInput(server_name=SERVER, mce_cluster=MCE, namespace=NS)

UNBOUND = AgentBinding(name="agent-1")
BOUND = AgentBinding(name="agent-1", cluster_deployment="clusters/hc-01")
FULL = InventoryHostState(bmh_found=True, nmstate_found=True, powered_on=True, agents=[UNBOUND])


def make_mocks(
    *,
    state: InventoryHostState = FULL,
    polls_until_off: int = 2,
    never_powers_off: bool = False,
    power_error: Exception | None = None,
    after: InventoryHostState | None = None,
):
    """`state` is what the first inspection finds; `after` what every later
    one finds (the final check) — gone by default."""
    leftover = after if after is not None else InventoryHostState()
    calls: dict[str, list] = {
        k: []
        for k in (
            "inspect_inventory_host",
            "set_host_online",
            "get_baremetal_host",
            "teardown_bmh_resources",
            "delete_agents",
            "order",
        )
    }

    @activity.defn
    async def inspect_inventory_host(ref: BmhRef) -> InventoryHostState:
        calls["inspect_inventory_host"].append(ref)
        first = len(calls["inspect_inventory_host"]) == 1
        calls["order"].append("inspect" if first else "verify")
        return state if first else leftover

    @activity.defn
    async def set_host_online(request: HostPowerRequest) -> bool:
        calls["set_host_online"].append(request)
        calls["order"].append("power-off")
        if power_error is not None:
            raise power_error
        return True

    @activity.defn
    async def get_baremetal_host(ref: BmhRef) -> BmhState:
        calls["get_baremetal_host"].append(ref)
        on = never_powers_off or len(calls["get_baremetal_host"]) < polls_until_off
        return BmhState(found=True, provisioning_state="provisioned", powered_on=on)

    @activity.defn
    async def teardown_bmh_resources(ref: BmhRef) -> TeardownResult:
        calls["teardown_bmh_resources"].append(ref)
        calls["order"].append("teardown")
        return TeardownResult(
            removed=[f"NMStateConfig/nmstate-config-{ref.server_name.lower()}",
                     f"BareMetalHost/{ref.server_name.lower()}"]
        )

    @activity.defn
    async def delete_agents(request: AgentDeleteRequest) -> AgentDeletion:
        calls["delete_agents"].append(request)
        calls["order"].append("delete-agents")
        return AgentDeletion(deleted=list(request.names))

    return calls, [
        inspect_inventory_host,
        set_host_online,
        get_baremetal_host,
        teardown_bmh_resources,
        delete_agents,
    ]


class _Harness:
    def __init__(self, activities) -> None:
        self._activities = activities

    async def __aenter__(self) -> Client:
        self._env = await WorkflowEnvironment.start_time_skipping()
        config = self._env.client.config()
        config["data_converter"] = pydantic_data_converter
        client = Client(**config)
        self._workers = [
            Worker(client, task_queue=REMOVE_SERVER_WORKFLOW_QUEUE, workflows=[RemoveServerWorkflow]),
            Worker(client, task_queue=QUEUE, activities=self._activities),
        ]
        for worker in self._workers:
            await worker.__aenter__()
        return client

    async def __aexit__(self, *exc_info) -> None:
        for worker in reversed(self._workers):
            await worker.__aexit__(*exc_info)
        await self._env.__aexit__(*exc_info)


async def _execute(client: Client, request: RemoveServerInput = INPUT):
    return await asyncio.wait_for(
        client.execute_workflow(
            RemoveServerWorkflow.run,
            RemoveServerRunArgs(input=request),
            id=f"test-{uuid.uuid4()}",
            task_queue=REMOVE_SERVER_WORKFLOW_QUEUE,
        ),
        timeout=60,
    )


def _error(exc: WorkflowFailureError) -> ApplicationError:
    cause = exc.cause
    if isinstance(cause, ActivityError):
        cause = cause.cause
    assert isinstance(cause, ApplicationError)
    return cause


async def test_an_inventory_host_is_powered_off_then_removed_agent_last():
    calls, acts = make_mocks()
    async with _Harness(acts) as client:
        result = await _execute(client)

    assert calls["order"] == ["inspect", "power-off", "teardown", "delete-agents", "verify"]
    assert result.found and result.powered_off
    assert result.agents_removed == ["agent-1"]
    assert result.resources_removed == [
        f"NMStateConfig/nmstate-config-{SERVER.lower()}",
        f"BareMetalHost/{SERVER.lower()}",
    ]
    power = calls["set_host_online"][0]
    assert power.online is False
    # Re-checked by the activity right before it acts.
    assert power.agents_must_be_unbound == ["agent-1"]
    assert calls["delete_agents"] == [AgentDeleteRequest(namespace=NS, names=["agent-1"])]
    # Waited for metal3 to report the host off before deleting anything.
    assert len(calls["get_baremetal_host"]) == 2


async def test_a_host_bound_to_a_cluster_is_refused_untouched():
    calls, acts = make_mocks(state=FULL.model_copy(update={"agents": [BOUND]}))
    async with _Harness(acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client)

    error = _error(excinfo.value)
    assert error.type == "AgentBoundToClusterError"
    assert "clusters/hc-01" in error.message
    assert calls["order"] == ["inspect"]


async def test_a_binding_made_after_the_inspection_is_still_caught():
    calls, acts = make_mocks(power_error=AgentBoundToClusterError("bound since"))
    async with _Harness(acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client)

    assert _error(excinfo.value).type == "AgentBoundToClusterError"
    # Once — non-retryable — and nothing after it.
    assert len(calls["set_host_online"]) == 1
    assert calls["teardown_bmh_resources"] == [] and calls["delete_agents"] == []


async def test_a_host_that_never_powers_off_fails_with_nothing_deleted():
    calls, acts = make_mocks(never_powers_off=True)
    async with _Harness(acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client)

    error = _error(excinfo.value)
    assert error.type == "HostPowerOffTimeoutError"
    assert "NOTHING was deleted" in error.message
    assert calls["teardown_bmh_resources"] == [] and calls["delete_agents"] == []


async def test_nothing_on_the_mce_is_a_successful_no_op():
    calls, acts = make_mocks(state=InventoryHostState())
    async with _Harness(acts) as client:
        result = await _execute(client)

    assert result.found is False
    assert calls["order"] == ["inspect"]


async def test_a_host_without_an_agent_still_has_its_resources_removed():
    calls, acts = make_mocks(state=FULL.model_copy(update={"agents": []}))
    async with _Harness(acts) as client:
        result = await _execute(client)

    assert calls["order"] == ["inspect", "power-off", "teardown", "verify"]
    assert result.agents_removed == []


AGENT_ONLY = InventoryHostState(
    bmh_found=False, nmstate_found=False, agents=[UNBOUND],
    server_scan_macs=["00:25:b5:00:00:01", "00:25:b5:00:00:02"],
)


async def test_an_agent_left_without_its_bmh_is_still_deleted():
    """Found by server-scan's MACs; nothing to power off through, so deleted on its own."""
    calls, acts = make_mocks(state=AGENT_ONLY)
    async with _Harness(acts) as client:
        result = await _execute(client)

    assert calls["order"] == ["inspect", "delete-agents", "verify"]
    assert result.powered_off is False
    assert result.agents_removed == ["agent-1"]


async def test_an_agent_that_comes_back_fails_the_run_and_says_why():
    """A host still running discovery re-registers: never reported as removed."""
    returned = InventoryHostState(agents=[AgentBinding(name="agent-2")])
    calls, acts = make_mocks(state=AGENT_ONLY, after=returned)
    async with _Harness(acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client)

    error = _error(excinfo.value)
    assert error.type == "ServerStillPresentError"
    assert "Agent agent-2" in error.message
    assert "power it off through its BMC" in error.message
    # Polled for the whole bounded wait, not given up on the first look.
    assert calls["order"].count("verify") > 1


async def test_a_resource_that_lingers_is_named_in_the_failure():
    lingering = InventoryHostState(bmh_found=True)
    calls, acts = make_mocks(after=lingering)
    async with _Harness(acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client)

    error = _error(excinfo.value)
    assert error.type == "ServerStillPresentError"
    assert "BareMetalHost" in error.message
    # Powered off through its BMH, so no "power it off" hint.
    assert "BMC" not in error.message


async def test_every_step_runs_on_the_target_mces_queue():
    """Registered only on that MCE's queue: anywhere else and the run could not finish."""
    calls, acts = make_mocks()
    async with _Harness(acts) as client:
        result = await _execute(client)
    assert result.mce_cluster == MCE
    # The first inspection and the final check, both of this server.
    assert calls["inspect_inventory_host"] == [BmhRef(server_name=SERVER, namespace=NS)] * 2
