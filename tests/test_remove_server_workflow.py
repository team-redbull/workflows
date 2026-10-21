"""remove-server: real RemoveServerWorkflow, mock activities, time-skipping env.

The cluster mocks are registered ONLY on the target MCE's activity queue —
every step that acts on the MCE goes there — and the inventory-segment read
(the site for the PXE cleanup) on the segment-lifecycle queue, as in
production; a dispatch anywhere else would hang the run. The time-skipping
environment collapses the power-off poll's durable timers.
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

from shared.consts import (
    REMOVE_SERVER_WORKFLOW_QUEUE,
    SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
    server_lifecycle_activity_queue,
)
from shared.exceptions import (
    AgentBoundToClusterError,
    InventorySegmentNotFoundError,
    PxeMapAuthError,
    PxeMapError,
)
from shared.models.segment_lifecycle import SegmentEntry, SegmentType
from shared.models.server_lifecycle import (
    AgentBinding,
    AgentDeleteRequest,
    AgentDeletion,
    BmhRef,
    BmhState,
    HostPowerRequest,
    InventoryHostState,
    PxeHostEntriesRef,
    PxeHostEntriesRemoval,
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

SEGMENT = SegmentEntry(
    segment="10.20.90.0/24",
    site="bat-yam",
    vlan_id=24,
    status="Allocated",
    type=SegmentType.INVENTORY.value,
    cluster_name=MCE,
)

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
    pxe_kept: set[str] | None = None,
    pxe_error: Exception | None = None,
    legacy_limb: bool = False,
    segment: SegmentEntry | None = None,
    segment_error: Exception | None = None,
):
    """`state` is what the first inspection finds; `after` what every later
    one finds (the final check) — gone by default. `pxe_kept` names the MACs
    the PXE VM keeps because another MCE's boot entry owns them."""
    leftover = after if after is not None else InventoryHostState()
    kept = pxe_kept or set()
    calls: dict[str, list] = {
        k: []
        for k in (
            "inspect_inventory_host",
            "set_host_online",
            "get_baremetal_host",
            "teardown_bmh_resources",
            "delete_agents",
            "remove_host_pxe_entries",
            "get_inventory_segment",
            "order",
        )
    }

    @activity.defn
    async def get_inventory_segment(mce_cluster: str) -> SegmentEntry:
        calls["get_inventory_segment"].append(mce_cluster)
        calls["order"].append("segment")
        if segment_error is not None:
            raise segment_error
        return segment or SEGMENT

    @activity.defn
    async def inspect_inventory_host(ref: BmhRef) -> InventoryHostState:
        calls["inspect_inventory_host"].append(ref)
        first = len(calls["inspect_inventory_host"]) == 1
        calls["order"].append("inspect" if first else "verify")
        found = state if first else leftover
        if legacy_limb:
            # A limb that predates `macs` sends a payload without the key.
            return found.model_dump(exclude={"macs"})
        return found

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

    @activity.defn
    async def remove_host_pxe_entries(ref: PxeHostEntriesRef) -> PxeHostEntriesRemoval:
        calls["remove_host_pxe_entries"].append(ref)
        calls["order"].append("pxe")
        if pxe_error is not None:
            raise pxe_error
        keys = [mac.replace(":", "") for mac in ref.macs]
        site = ref.site or "bat-yam"
        return PxeHostEntriesRemoval(
            cleared=[f"{site}/{k}" for k in keys if k not in kept],
            kept=[f"{site}/{k}: chains to ocp4-mce-other" for k in keys if k in kept],
        )

    return calls, {
        QUEUE: [
            inspect_inventory_host,
            set_host_online,
            get_baremetal_host,
            teardown_bmh_resources,
            delete_agents,
            remove_host_pxe_entries,
        ],
        SEGMENT_LIFECYCLE_ACTIVITY_QUEUE: [get_inventory_segment],
    }


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
    macs=["00:25:b5:00:00:01", "00:25:b5:00:00:02"],
)


async def test_an_agent_left_without_its_bmh_is_still_deleted():
    """Found by server-scan's MACs; nothing to power off through, so deleted on its own."""
    calls, acts = make_mocks(state=AGENT_ONLY)
    async with _Harness(acts) as client:
        result = await _execute(client)

    assert calls["order"] == ["inspect", "delete-agents", "segment", "pxe", "verify"]
    assert result.powered_off is False
    assert result.agents_removed == ["agent-1"]
    # Its MACs are still cleared from the PXE map: found by server-scan's.
    assert calls["remove_host_pxe_entries"][0].macs == AGENT_ONLY.macs


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


WITH_MACS = FULL.model_copy(
    update={"macs": ["00:25:b5:00:00:01", "00:25:b5:00:00:02", "00:25:b5:00:00:09"]}
)


class TestThePxeMapIsCleared:
    """A removed host must not be able to PXE-boot this MCE's discovery image again."""

    async def test_every_mac_is_cleared_after_the_agent_and_before_the_check(self):
        calls, acts = make_mocks(state=WITH_MACS)
        async with _Harness(acts) as client:
            result = await _execute(client)

        assert calls["order"] == [
            "inspect", "power-off", "teardown", "delete-agents", "segment", "pxe", "verify",
        ]
        # Every MAC the host is known by, conditional on THIS MCE, and only on
        # the MCE's own site's VM — the one install-server mapped it on.
        assert calls["get_inventory_segment"] == [MCE]
        assert calls["remove_host_pxe_entries"] == [
            PxeHostEntriesRef(
                server_name=SERVER, mce_cluster=MCE, macs=WITH_MACS.macs, site="bat-yam"
            )
        ]
        assert result.pxe_cleared == [
            "bat-yam/0025b5000001", "bat-yam/0025b5000002", "bat-yam/0025b5000009",
        ]
        assert result.pxe_kept == []

    async def test_another_mces_entry_is_kept_and_reported_not_failed(self):
        calls, acts = make_mocks(state=WITH_MACS, pxe_kept={"0025b5000002"})
        async with _Harness(acts) as client:
            result = await _execute(client)

        assert result.pxe_kept == ["bat-yam/0025b5000002: chains to ocp4-mce-other"]
        assert "bat-yam/0025b5000002" not in result.pxe_cleared

    async def test_nothing_on_the_mce_leaves_the_pxe_map_alone_too(self):
        # Nothing ties the MACs to this MCE any more; another may own them now.
        calls, acts = make_mocks(
            state=InventoryHostState(server_scan_macs=["00:25:b5:00:00:01"], macs=["00:25:b5:00:00:01"])
        )
        async with _Harness(acts) as client:
            result = await _execute(client)

        assert result.found is False
        assert calls["remove_host_pxe_entries"] == []

    async def test_a_host_with_no_known_mac_skips_the_step(self):
        calls, acts = make_mocks(state=FULL.model_copy(update={"macs": []}))
        async with _Harness(acts) as client:
            await _execute(client)
        assert calls["remove_host_pxe_entries"] == []

    async def test_a_limb_that_predates_macs_falls_back_to_server_scans(self):
        legacy = WITH_MACS.model_copy(update={"server_scan_macs": ["00:25:b5:00:00:07"]})
        calls, acts = make_mocks(state=legacy, legacy_limb=True)
        async with _Harness(acts) as client:
            await _execute(client)
        assert calls["remove_host_pxe_entries"][0].macs == ["00:25:b5:00:00:07"]

    async def test_a_rejected_pxe_token_is_reported_and_the_removal_completes(self):
        # Best effort: the host is already off the MCE by now.
        calls, acts = make_mocks(state=WITH_MACS, pxe_error=PxeMapAuthError("check PXE_MAP_TOKEN"))
        async with _Harness(acts) as client:
            result = await _execute(client)

        assert result.found and result.agents_removed == ["agent-1"]
        assert result.pxe_failed == ["bat-yam: PxeMapAuthError: check PXE_MAP_TOKEN"]
        assert result.pxe_cleared == []
        # Once — non-retryable — and the final check still ran.
        assert len(calls["remove_host_pxe_entries"]) == 1
        assert calls["order"][-1] == "verify"

    async def test_an_unreachable_pxe_vm_is_given_up_on_after_the_deadline(self):
        calls, acts = make_mocks(state=WITH_MACS, pxe_error=PxeMapError("connection refused"))
        async with _Harness(acts) as client:
            result = await _execute(client)

        assert len(result.pxe_failed) == 1 and result.pxe_failed[0].startswith("bat-yam: ")
        # Retried (transient) - but bounded, not forever.
        assert len(calls["remove_host_pxe_entries"]) > 1
        assert calls["order"][-1] == "verify"

    async def test_no_inventory_segment_skips_the_cleanup_and_says_why(self):
        calls, acts = make_mocks(
            state=WITH_MACS, segment_error=InventorySegmentNotFoundError("no INVENTORY segment")
        )
        async with _Harness(acts) as client:
            result = await _execute(client)

        assert calls["remove_host_pxe_entries"] == []
        assert "InventorySegmentNotFoundError" in result.pxe_failed[0]
        assert len(calls["get_inventory_segment"]) == 1

    async def test_a_segment_of_another_cluster_is_not_used(self):
        calls, acts = make_mocks(
            state=WITH_MACS, segment=SEGMENT.model_copy(update={"cluster_name": "ocp4-mce-other"})
        )
        async with _Harness(acts) as client:
            result = await _execute(client)

        assert calls["remove_host_pxe_entries"] == []
        assert "ocp4-mce-other" in result.pxe_failed[0]


async def test_every_step_runs_on_the_target_mces_queue():
    """Registered only on that MCE's queue: anywhere else and the run could not finish."""
    calls, acts = make_mocks()
    async with _Harness(acts) as client:
        result = await _execute(client)
    assert result.mce_cluster == MCE
    # The first inspection and the final check, both of this server.
    assert calls["inspect_inventory_host"] == [BmhRef(server_name=SERVER, namespace=NS)] * 2
