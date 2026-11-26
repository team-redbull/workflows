"""add-server-to-mce's KubeVirt branch with the REAL create-vm as its child, on the fake KV fleet.

tests/test_add_server_to_mce_vm_workflow.py pins what the parent does around a
stand-in child. Here the child is the real CreateVmWorkflow, with its zone lock,
the day1 list and the real limb code of a KV cluster (tests/kv_fleet.py), so
these prove what only the two together can:

  * the VM the child made is the one the parent installs — its name, its MAC —
    and the run id the parent keeps IS the VM's `redbull.io/created-by` label;
  * a run that does not end with an Agent deletes that VM, on that cluster,
    with the limb's own delete — which refuses any other identity;
  * a parent cancelled, or TERMINATED, while its child holds a VM leaves none.

The MCE's side stays mocked (make_mock_activities): what is real here is the
VM's side. The clock is moved by hand (tests/manual_time.py): the zone lock is
long-lived and idle.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

import pytest
from temporalio import activity
from temporalio.client import WorkflowExecutionStatus, WorkflowFailureError, WorkflowHandle
from temporalio.exceptions import ApplicationError, is_cancelled_exception
from temporalio.worker import Worker

from activities.vm_lifecycle import vm_spec
from shared.consts import (
    ADD_SERVER_TO_MCE_WORKFLOW_QUEUE,
    REMOVE_SERVER_FROM_MCE_WORKFLOW_QUEUE,
    SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
    server_lifecycle_activity_queue,
)
from shared.exceptions import AgentNeverAppearedError, CreateVmFailedError, NoKvClusterFitsError
from shared.models.server_lifecycle import (
    AddServerToMceInput,
    AddServerToMceRunArgs,
    AgentRef,
    AgentState,
    KubevirtBmcConfig,
    KubevirtBmcConfigRequest,
)
from shared.models.vm_lifecycle import CreateVmProgress
from shared.workflow_ids import create_vm_child_workflow_id
from tests.kv_fleet import KvFleet, node
from tests.manual_time import ManualTime
from tests.test_add_server_to_mce_vm_workflow import HOST_TEMPLATE, KV_LABELS, SEGMENT
from tests.test_add_server_to_mce_workflow import _FakeRemoveServerFromMce, make_mock_activities
from workflow_domains.server_lifecycle.add_server_to_mce import AddServerToMceWorkflow
from workflow_domains.vm_lifecycle.create_vm import CreateVmWorkflow

MCE = "ocp4-prod-mce-five-a"
INFRA_ENV = "c200-kv32-az1"
NAMESPACE = "inventory"
A, B = "ocp4-prod-kv-five-az1-a", "ocp4-prod-kv-five-az1-b"
ARGS = AddServerToMceRunArgs(input=AddServerToMceInput(infra_env=INFRA_ENV, mce_cluster=MCE, namespace=NAMESPACE))


def _fleet(*names: str, **cluster_kwargs) -> KvFleet:
    fleet = KvFleet()
    if cluster_kwargs.get("storage") == "wffc":
        # The disk never binds: each of the wait's polls really takes a second, so
        # the child is still in its ten-minute wait when the test acts on it.
        fleet.poll_seconds = 1.0
    for name in names:
        fleet.add(name, "az1", nodes=[node(f"{name}-n1", 100, 10_000)], **cluster_kwargs)
    return fleet


class Harness(ManualTime):
    """The fake KV fleet with the real create-vm, plus add-server-to-mce and its MCE's mocked limb."""

    def __init__(self, fleet: KvFleet, *, agent: bool = True, real_time: bool = False, **knobs) -> None:
        self.real_time = real_time
        self.fleet = fleet
        knobs.setdefault("segment", SEGMENT)
        self.calls, segment_acts, server_acts = make_mock_activities(agent_labels=KV_LABELS, **knobs)
        calls = self.calls

        @activity.defn
        async def read_kubevirt_bmc_config(request: KubevirtBmcConfigRequest) -> KubevirtBmcConfig:
            calls["order"].append("read_kubevirt_bmc_config")
            return KubevirtBmcConfig(host_template=HOST_TEMPLATE)

        @activity.defn(name="find_agent_for_host")
        async def no_agent_ever(ref: AgentRef) -> AgentState:
            calls["find_agent_for_host"].append(ref)
            return AgentState(found=False)

        if not agent:  # the VM's name is drawn inside the run, so no test can name it beforehand
            server_acts = [a for a in server_acts if a.__name__ != "find_agent_for_host"] + [no_agent_ever]
        self._segment_acts, self._server_acts = segment_acts, [*server_acts, read_kubevirt_bmc_config]

    async def __aenter__(self) -> Harness:
        await super().__aenter__()
        self._running = self.fleet.running(self.client)
        await self._running.__aenter__()
        self._workers = [
            Worker(self.client, task_queue=ADD_SERVER_TO_MCE_WORKFLOW_QUEUE, workflows=[AddServerToMceWorkflow]),
            Worker(self.client, task_queue=server_lifecycle_activity_queue(MCE), activities=self._server_acts),
            Worker(self.client, task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE, activities=self._segment_acts),
            Worker(self.client, task_queue=REMOVE_SERVER_FROM_MCE_WORKFLOW_QUEUE, workflows=[_FakeRemoveServerFromMce]),
        ]
        for worker in self._workers:
            await worker.__aenter__()
        return self

    async def __aexit__(self, *exc) -> None:
        for worker in reversed(self._workers):
            await worker.__aexit__(*exc)
        await self._running.__aexit__(*exc)
        await super().__aexit__(*exc)

    async def advance(self, duration: timedelta) -> None:
        await super().advance(duration)
        if not self.real_time:
            self.fleet.skew += duration

    async def start(self) -> WorkflowHandle:
        return await self.client.start_workflow(
            AddServerToMceWorkflow.run, ARGS,
            id=f"add-server-to-mce-{MCE}_{INFRA_ENV}_{uuid.uuid4().hex[:6]}",
            task_queue=ADD_SERVER_TO_MCE_WORKFLOW_QUEUE,
        )

    def child(self, parent: WorkflowHandle) -> WorkflowHandle:
        return self.client.get_workflow_handle(create_vm_child_workflow_id(parent.id))

    async def until(self, condition, *, tries: int = 600):
        seen = None
        for _ in range(tries):
            try:
                seen = await condition()
            except Exception:  # noqa: BLE001 — not started yet
                seen = None
            if seen:
                return seen
            await asyncio.sleep(0.02)
        raise AssertionError("the state waited for was never reached")

    async def until_child_phase(self, parent: WorkflowHandle, phase: str) -> CreateVmProgress:
        async def at_phase():
            progress = await self.child(parent).query(CreateVmWorkflow.progress)
            return progress if progress.phase == phase else None

        return await self.until(at_phase)


def _application_error(error: WorkflowFailureError) -> ApplicationError:
    cause = error.cause
    cause = getattr(cause, "cause", cause) if not isinstance(cause, ApplicationError) else cause
    assert isinstance(cause, ApplicationError), cause
    return cause


class TestTheVmTheChildMadeIsTheOneInstalled:
    async def test_its_name_its_mac_and_its_run_id(self):
        fleet = _fleet(A, B)
        async with Harness(fleet) as h:
            handle = await h.start()
            result = await h.result(handle)
            child = await h.child(handle).describe()

        cluster = fleet.clusters[A]  # the oldest cluster of the zone with room
        assert result.kv_cluster == A and cluster.vm_names == [result.server_name]
        assert fleet.clusters[B].vm_names == []
        vm = cluster.vms[(NAMESPACE, result.server_name)]
        # The MAC KubeMacPool gave the VM is the BareMetalHost's boot MAC and the NMStateConfig's.
        assert result.boot_mac == vm_spec.mac_of(vm) and cluster.in_range(result.boot_mac)
        request = h.calls["create_baremetal_host"][0]
        assert (request.server_name, request.bond_members[0].mac) == (result.server_name, result.boot_mac)
        assert request.bmc.host == HOST_TEMPLATE.replace("{kv_cluster}", A)
        assert request.bmc.path == f"/redfish/v1/Systems/{result.server_name}"
        # What the parent keeps as the child's run id IS the VM's label: what a delete finds it by.
        assert result.create_vm_run_id == child.run_id == vm_spec.created_by(vm)
        assert result.create_vm_workflow_id == create_vm_child_workflow_id(handle.id)
        # It stays, Halted: kubevirt-redfish and Ironic own its power from here.
        assert vm["spec"]["runStrategy"] == "Halted"
        assert h.calls["allocate_server"] == [] and h.calls["reserve_server"] == []

    async def test_two_runs_at_once_get_two_vms(self):
        """Each run's child takes the zone's lock in turn: two VMs, two hosts."""
        fleet = _fleet(A)
        async with Harness(fleet) as h:
            first, second = await h.start(), await h.start()
            one, two = await h.results(first, second)

        assert one.server_name != two.server_name and one.boot_mac != two.boot_mac
        assert sorted(fleet.clusters[A].vm_names) == sorted([one.server_name, two.server_name])
        assert {r.server_name for r in h.calls["create_baremetal_host"]} == {one.server_name, two.server_name}

    async def test_no_kv_cluster_with_room_fails_the_run_with_nothing_made(self):
        fleet = KvFleet()
        fleet.add(A, "az1", nodes=[node(f"{A}-n1", 8, 64)])  # no node holds a kv32 VM
        async with Harness(fleet) as h:
            handle = await h.start()
            failure = _application_error(await h.failure(handle))

        assert failure.type == CreateVmFailedError.__name__
        assert NoKvClusterFitsError.__name__ in failure.message
        assert fleet.clusters[A].vm_names == [] and h.calls["create_nmstate_config"] == []


class TestARunThatDoesNotCompleteLeavesNoVm:
    async def test_no_agent_the_vm_is_deleted_on_its_cluster(self):
        fleet = _fleet(A)
        async with Harness(fleet, agent=False) as h:
            handle = await h.start()
            created = await h.until(lambda: _vm_made(fleet, A))
            failure = _application_error(await h.failure(handle, step=timedelta(minutes=5), limit=120))

        assert failure.type == AgentNeverAppearedError.__name__
        assert f"{created} was deleted on KV cluster {A}" in failure.message
        cluster = fleet.clusters[A]
        assert cluster.vm_names == [] and cluster.deleted == [created]
        assert cluster.datavolumes == {}  # its root disk with it
        order = h.calls["order"]
        assert "delete_host_resources" in order  # the host came out of the MCE first

    async def test_a_cancel_while_the_child_holds_a_vm_leaves_none(self):
        """The child is waiting for the VM's disk: cancelled with its parent, it removes its own VM."""
        fleet = _fleet(A, storage="wffc")  # the root disk never binds: the child waits on it
        async with Harness(fleet) as h:
            handle = await h.start()
            created = await h.until(lambda: _vm_made(fleet, A))
            await h.until_child_phase(handle, "awaiting-root-disk")
            await handle.cancel()
            outcome = (await h.results(handle, return_exceptions=True))[0]
            progress = await handle.query(AddServerToMceWorkflow.progress)
            child = await h.child(handle).describe()

        assert isinstance(outcome, WorkflowFailureError) and is_cancelled_exception(outcome.cause)
        assert child.status == WorkflowExecutionStatus.CANCELED
        assert fleet.clusters[A].vm_names == [] and fleet.clusters[A].deleted == [created]
        assert "leaves no VM" in progress.cancel_rollback
        assert h.calls["create_nmstate_config"] == []

    async def test_a_cancel_while_waiting_for_the_agent_deletes_the_vm(self):
        fleet = _fleet(A)
        async with Harness(fleet, agent=False) as h:
            handle = await h.start()
            created = await h.until(lambda: _vm_made(fleet, A))

            async def awaiting_agent():
                return (await handle.query(AddServerToMceWorkflow.progress)).phase == "awaiting-agent"

            await h.until(awaiting_agent)
            await handle.cancel()
            outcome = (await h.results(handle, return_exceptions=True, step=timedelta(seconds=30)))[0]
            progress = await handle.query(AddServerToMceWorkflow.progress)

        assert isinstance(outcome, WorkflowFailureError) and is_cancelled_exception(outcome.cause)
        assert fleet.clusters[A].vm_names == [] and fleet.clusters[A].deleted == [created]
        assert f"{created} was deleted on KV cluster {A}" in progress.cancel_rollback

    async def test_a_terminated_parent_leaves_none_either(self):
        """Terminate runs no code in the parent: the child's REQUEST_CANCEL policy cancels it, and
        create-vm, cancelled, removes its own VM. On a real dev server — the time-skipping one
        does not apply a parent's close policy."""
        fleet = _fleet(A, storage="wffc")
        async with Harness(fleet, real_time=True) as h:
            handle = await h.start()
            created = await h.until(lambda: _vm_made(fleet, A))
            await h.until_child_phase(handle, "awaiting-root-disk")
            await handle.terminate()

            async def child_closed():
                return (await h.child(handle).describe()).status != WorkflowExecutionStatus.RUNNING

            await h.until(child_closed, tries=1500)
            child = await h.child(handle).describe()

        assert child.status == WorkflowExecutionStatus.CANCELED
        assert fleet.clusters[A].vm_names == [] and fleet.clusters[A].deleted == [created]


async def _vm_made(fleet: KvFleet, cluster: str) -> str | None:
    names = fleet.clusters[cluster].created
    return names[0] if names else None


@pytest.mark.parametrize("run", range(1))
async def test_create_vm_never_continues_as_new_so_its_first_run_id_is_its_vms_label(run):
    """What the parent relies on to delete the VM: the child handle's `first_execution_run_id`
    is the run that labels the VM, as long as create-vm is one execution from start to end."""
    import inspect

    from workflow_domains.vm_lifecycle import create_vm

    assert "continue_as_new" not in inspect.getsource(create_vm)
