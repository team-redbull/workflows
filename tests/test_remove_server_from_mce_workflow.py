"""remove-server-from-mce: real RemoveServerFromMceWorkflow, mock activities, time-skipping env.

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
from temporalio import activity, workflow
from temporalio.client import Client, WorkflowFailureError
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from shared.consts import (
    REMOVE_SERVER_FROM_MCE_WORKFLOW_QUEUE,
    SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
    server_lifecycle_activity_queue,
)
from shared.exceptions import (
    AgentBoundToClusterError,
    AmbiguousServerNameError,
    InventorySegmentNotFoundError,
    PxeMapAuthError,
    PxeMapError,
    ServerScanAuthError,
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
    ReleaseServerRequest,
    RemoveServerFromMceInput,
    RemoveServerFromMceRunArgs,
    ServerLockRecord,
    ServerMembership,
    ServerMembershipRequest,
    ServerReservation,
    TeardownResult,
)
from workflow_domains.server_lifecycle import add_server_to_mce, remove_server_from_mce, uninstall_server_from_hc
from workflow_domains.server_lifecycle.remove_server_from_mce import RemoveServerFromMceWorkflow

MCE = "ocp4-mce-bat-yam-01"
QUEUE = server_lifecycle_activity_queue(MCE)
SERVER = "ocp-cisco-m6-bat-yam-64c-512gb-FCH0001"
NS = "inventory"
INPUT = RemoveServerFromMceInput(server_name=SERVER, mce_cluster=MCE, namespace=NS)

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

# server-scan's record of the server: no live lock unless a test gives one.
MEMBERSHIP = ServerMembership(
    found=True, server_id="srv_123", server_name=SERVER, lifecycle_state="INSTALLED_TO_INVENTORY",
    mce_name=MCE,
)
INSTALL_RUN = f"add-server-to-mce-{MCE}_{SERVER}"
# add-server-to-mce's day-long hold after it installed the server into THIS MCE.
INSTALL_LOCK = ServerLockRecord(
    live=True, holder="add-server-to-mce", workflow_id=INSTALL_RUN, mce_cluster=MCE,
    expires_at="2026-11-01T09:00:00Z",
)


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
    lock: ServerLockRecord | None = None,
    membership: ServerMembership = MEMBERSHIP,
    membership_error: Exception | None = None,
    legacy_membership: bool = False,
    release_detail: str | None = None,
    release_error: Exception | None = None,
):
    """`state` is what the first inspection finds; `after` what every later
    one finds (the final check) — gone by default. `pxe_kept` names the MACs
    the PXE VM keeps because another MCE's boot entry owns them. `lock` is
    server-scan's install lock on the server (none live by default)."""
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
            "lookup_server_membership",
            "release_server",
            "order",
        )
    }

    @activity.defn
    async def lookup_server_membership(request: ServerMembershipRequest) -> ServerMembership:
        calls["lookup_server_membership"].append(request)
        calls["order"].append("lock")
        if membership_error is not None:
            raise membership_error
        record = membership.model_copy(update={"lock": lock or ServerLockRecord()})
        if legacy_membership:
            # A limb that predates the lock read sends a payload without the key.
            return record.model_dump(exclude={"lock"})
        return record

    @activity.defn
    async def release_server(request: ReleaseServerRequest) -> ServerReservation:
        calls["release_server"].append(request)
        calls["order"].append("release")
        if release_error is not None:
            raise release_error
        return ServerReservation(server_id=request.server_id, held=False, detail=release_detail)

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
            lookup_server_membership,
            release_server,
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
            Worker(client, task_queue=REMOVE_SERVER_FROM_MCE_WORKFLOW_QUEUE, workflows=[RemoveServerFromMceWorkflow]),
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


async def _execute(client: Client, request: RemoveServerFromMceInput = INPUT):
    return await asyncio.wait_for(
        client.execute_workflow(
            RemoveServerFromMceWorkflow.run,
            RemoveServerFromMceRunArgs(input=request),
            id=f"test-{uuid.uuid4()}",
            task_queue=REMOVE_SERVER_FROM_MCE_WORKFLOW_QUEUE,
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

    assert calls["order"] == ["inspect", "power-off", "teardown", "delete-agents", "verify", "lock"]
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
    # Not even the lock: nothing ties the server to this MCE, and an install
    # that has locked it may not have written anything yet.
    assert calls["order"] == ["inspect"]
    assert not result.lock_released and result.lock_detail is None


async def test_a_host_without_an_agent_still_has_its_resources_removed():
    calls, acts = make_mocks(state=FULL.model_copy(update={"agents": []}))
    async with _Harness(acts) as client:
        result = await _execute(client)

    assert calls["order"] == ["inspect", "power-off", "teardown", "verify", "lock"]
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

    assert calls["order"] == ["inspect", "delete-agents", "segment", "pxe", "verify", "lock"]
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
            "inspect", "power-off", "teardown", "delete-agents", "segment", "pxe", "verify", "lock",
        ]
        # Every MAC the host is known by, conditional on THIS MCE, and only on
        # the MCE's own site's VM — the one add-server-to-mce mapped it on.
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
    assert calls["lookup_server_membership"] == [ServerMembershipRequest(server_name=SERVER)]


class TestServerScansLock:
    """Once nothing is left on the MCE, add-server-to-mce's lock FOR THIS MCE goes back (2026-10-31)."""

    async def test_add_server_to_mces_lock_for_this_mce_is_released_after_the_check(self):
        calls, acts = make_mocks(state=WITH_MACS, lock=INSTALL_LOCK)
        async with _Harness(acts) as client:
            result = await _execute(client)

        assert calls["order"][-3:] == ["verify", "lock", "release"]
        # With the holder and run read, so server-scan compares and deletes exactly that lock.
        assert calls["release_server"] == [ReleaseServerRequest(
            server_id="srv_123", server_name=SERVER, holder="add-server-to-mce", workflow_id=INSTALL_RUN,
        )]
        assert result.lock_released is True
        assert result.lock_detail.startswith("released 'add-server-to-mce'") and INSTALL_RUN in result.lock_detail

    async def test_the_mce_is_compared_whatever_its_spelling(self):
        unprefixed = INSTALL_LOCK.model_copy(update={"mce_cluster": "mce-bat-yam-01"})
        calls, acts = make_mocks(lock=unprefixed)
        async with _Harness(acts) as client:
            result = await _execute(client)
        assert result.lock_released and len(calls["release_server"]) == 1

    async def test_another_mces_lock_is_left_alone(self):
        elsewhere = INSTALL_LOCK.model_copy(update={"mce_cluster": "ocp4-mce-other"})
        calls, acts = make_mocks(lock=elsewhere)
        async with _Harness(acts) as client:
            result = await _execute(client)

        assert calls["release_server"] == []
        assert result.lock_released is False
        assert "another MCE" in result.lock_detail and "ocp4-mce-other" in result.lock_detail

    async def test_uninstall_server_from_hcs_lock_is_left_to_its_own_run(self):
        """remove-server-from-mce as uninstall-server-from-hc's child: the parent releases its own lock after it."""
        parents = ServerLockRecord(live=True, holder="uninstall-server-from-hc",
                                   workflow_id=f"uninstall-server-from-hc-{SERVER.lower()}", mce_cluster=MCE)
        calls, acts = make_mocks(lock=parents)
        async with _Harness(acts) as client:
            result = await _execute(client)

        assert calls["release_server"] == []
        assert result.lock_released is False
        assert result.lock_detail.startswith("left to uninstall-server-from-hc")

    async def test_any_other_holders_lock_is_left_alone(self):
        operator = ServerLockRecord(live=True, holder="operator", workflow_id="by-hand", mce_cluster=MCE)
        calls, acts = make_mocks(lock=operator)
        async with _Harness(acts) as client:
            result = await _execute(client)
        assert calls["release_server"] == [] and not result.lock_released
        assert "not an add-server-to-mce run" in result.lock_detail

    async def test_no_live_lock_is_nothing_to_release(self):
        lapsed = INSTALL_LOCK.model_copy(update={"live": False})
        calls, acts = make_mocks(lock=lapsed)
        async with _Harness(acts) as client:
            result = await _execute(client)
        assert calls["release_server"] == [] and not result.lock_released
        assert result.lock_detail == "no live lock in server-scan: nothing to release"

    async def test_nothing_found_on_the_mce_releases_nothing(self):
        calls, acts = make_mocks(state=InventoryHostState(), lock=INSTALL_LOCK)
        async with _Harness(acts) as client:
            result = await _execute(client)
        assert calls["lookup_server_membership"] == calls["release_server"] == []
        assert result.found is False and not result.lock_released

    async def test_a_failed_pxe_cleanup_keeps_the_lock(self):
        # The host's MACs may still chain to this MCE's discovery image.
        calls, acts = make_mocks(state=WITH_MACS, lock=INSTALL_LOCK,
                                 pxe_error=PxeMapAuthError("check PXE_MAP_TOKEN"))
        async with _Harness(acts) as client:
            result = await _execute(client)

        assert result.found and result.pxe_failed
        assert calls["lookup_server_membership"] == calls["release_server"] == []
        assert result.lock_released is False and "PXE cleanup failed" in result.lock_detail

    async def test_a_lock_taken_since_the_read_is_reported_not_cleared(self):
        # server-scan's 409 `held_by`: another run took the machine between the read and the release.
        calls, acts = make_mocks(lock=INSTALL_LOCK,
                                 release_detail="left alone, it is not ours: held by another run")
        async with _Harness(acts) as client:
            result = await _execute(client)
        assert len(calls["release_server"]) == 1
        assert result.lock_released is False and "not ours" in result.lock_detail

    async def test_a_refused_release_is_reported_and_the_removal_completes(self):
        calls, acts = make_mocks(lock=INSTALL_LOCK, release_error=ServerScanAuthError("needs ADMIN"))
        async with _Harness(acts) as client:
            result = await _execute(client)
        assert result.found and result.agents_removed == ["agent-1"]
        assert len(calls["release_server"]) == 1  # non-retryable
        assert not result.lock_released and "ServerScanAuthError: needs ADMIN" in result.lock_detail

    async def test_an_ambiguous_name_is_reported_and_the_removal_completes(self):
        calls, acts = make_mocks(membership_error=AmbiguousServerNameError("srv_1, srv_9"))
        async with _Harness(acts) as client:
            result = await _execute(client)
        assert len(calls["lookup_server_membership"]) == 1  # non-retryable
        assert calls["release_server"] == []
        assert not result.lock_released and result.lock_detail.startswith("not read: AmbiguousServerNameError")

    async def test_a_limb_that_predates_the_lock_read_releases_nothing(self):
        calls, acts = make_mocks(lock=INSTALL_LOCK, legacy_membership=True)
        async with _Harness(acts) as client:
            result = await _execute(client)
        assert calls["release_server"] == [] and not result.lock_released
        assert "predates the lock read" in result.lock_detail

    async def test_a_run_recorded_before_the_release_leaves_the_lock(self, monkeypatch):
        monkeypatch.setattr(
            workflow, "patched", lambda patch_id: patch_id != remove_server_from_mce._RELEASE_LOCK_PATCH
        )
        calls, acts = make_mocks(lock=INSTALL_LOCK)
        async with _Harness(acts) as client:
            result = await _execute(client)
        assert calls["order"][-1] == "verify"
        assert calls["lookup_server_membership"] == calls["release_server"] == []
        assert not result.lock_released and result.lock_detail is None


def test_the_lock_holders_are_the_workflows_own():
    # Each workflow's holder now, and the one its runs started before the
    # 2026-11-01 renames still hold their lock under.
    assert remove_server_from_mce._INSTALL_HOLDERS == {
        add_server_to_mce._RESERVATION_HOLDER,
        add_server_to_mce._LEGACY_RESERVATION_HOLDER,
    }
    assert remove_server_from_mce._UNINSTALL_HOLDERS == {
        uninstall_server_from_hc._RESERVATION_HOLDER,
        uninstall_server_from_hc._LEGACY_RESERVATION_HOLDER,
    }


@pytest.mark.parametrize("holder", ["install-server", "add-server-to-mce"])
async def test_an_install_lock_under_either_name_is_released(holder):
    """A machine installed before the rename holds `install-server`'s lock: it goes back too."""
    _, acts = make_mocks(
        state=WITH_MACS, lock=INSTALL_LOCK.model_copy(update={"holder": holder})
    )
    async with _Harness(acts) as client:
        result = await _execute(client)

    assert result.lock_released, result.lock_detail


@pytest.mark.parametrize("holder", ["uninstall-server", "uninstall-server-from-hc"])
async def test_an_uninstall_lock_under_either_name_is_left_to_it(holder):
    _, acts = make_mocks(
        lock=ServerLockRecord(live=True, holder=holder, workflow_id="uninstall-x", mce_cluster=MCE)
    )
    async with _Harness(acts) as client:
        result = await _execute(client)

    assert not result.lock_released
    assert result.lock_detail.startswith("left to uninstall-server-from-hc")
