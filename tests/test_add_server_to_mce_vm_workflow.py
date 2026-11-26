"""add-server-to-mce's KubeVirt branch: real AddServerToMceWorkflow, a fake create-vm child.

An InfraEnv whose agentLabels say `redbull.io/size: kv<cpu>` is filled with VMs:
the run has ONE made (create-vm, as an awaited child) and installs exactly that
one, with no call to server-scan at all. What these tests pin is what the run
does AROUND that child: what it refuses before any VM exists, what it writes on
the MCE for the VM, and that a run which does not end with an Agent leaves no VM
— on a failure, on a missing Agent, and on a cancel wherever it lands.

The child here is a stand-in (`_FakeCreateVm`, the `_FakeRemoveServerFromMce`
precedent); tests/test_add_server_to_mce_vm_real_child.py runs the REAL create-vm
under the same parent, on the fake KV fleet.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

import pytest
from temporalio import activity, workflow
from temporalio.client import Client, WorkflowExecutionStatus, WorkflowFailureError, WorkflowHandle
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import ApplicationError, is_cancelled_exception
from temporalio.service import RPCError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from shared.consts import (
    ADD_SERVER_TO_MCE_WORKFLOW_QUEUE,
    CREATE_VM_WORKFLOW_QUEUE,
    REMOVE_SERVER_FROM_MCE_WORKFLOW_QUEUE,
    SEGMENT_LIFECYCLE_ACTIVITY_QUEUE,
    server_lifecycle_activity_queue,
    vm_lifecycle_activity_queue,
)
from shared.exceptions import (
    AgentNeverAppearedError,
    BmcCredentialsMissingError,
    BmhConflictError,
    BmhDeletionTimeoutError,
    CreateVmAlreadyRunningError,
    CreateVmFailedError,
    InfraEnvAgentLabelsError,
    KubevirtBmcNotConfiguredError,
    KvClusterAccessError,
    MceNameNotConventionalError,
    NamedServerOnKubevirtInfraEnvError,
    NoKvClusterFitsError,
    VmNameClashError,
)
from shared.models.segment_lifecycle import SegmentEntry, SegmentType
from shared.models.server_lifecycle import (
    AddServerToMceInput,
    AddServerToMceRunArgs,
    BmhResourceRequest,
    CreatedResource,
    KubevirtBmcConfig,
    KubevirtBmcConfigRequest,
    RemoveServerFromMceResult,
    RemoveServerFromMceRunArgs,
)
from shared.models.vm_lifecycle import CreateVmResult, CreateVmRunArgs, VmDeleted, VmDeleteRequest
from shared.workflow_ids import create_vm_child_workflow_id, remove_server_from_mce_workflow_id
from tests.test_add_server_to_mce_workflow import make_mock_activities
from workflow_domains.server_lifecycle.add_server_to_mce import AddServerToMceWorkflow

INFRA_ENV = "c200-kv32-az1"
MCE = "ocp4-prod-mce-five-a"
MCE_QUEUE = server_lifecycle_activity_queue(MCE)
NAMESPACE = "inventory"
VLAN = 1010
KV_CLUSTER = "ocp4-prod-kv-five-az1-a"
KV_QUEUE = vm_lifecycle_activity_queue(KV_CLUSTER)
VM = "ocp-c200-kv32-five-k7q2m9xd"
MAC = "02:b0:96:22:25:eb"
HOST_TEMPLATE = "kubevirt-redfish-kubevirt-redfish.apps.{kv_cluster}.example.com"

# The InfraEnv's spec.agentLabels, as the convention writes them for a VM pool.
KV_LABELS = {
    "redbull.io/flavor": "c200",
    "redbull.io/size": "kv32",
    "redbull.io/site": "five",
    "topology.kubernetes.io/zone": "az1",
}
SEGMENT = SegmentEntry(
    segment="10.20.90.0/24", site="five", vlan_id=VLAN, status="Allocated",
    type=SegmentType.INVENTORY.value, cluster_name=MCE,
)
INPUT = AddServerToMceInput(infra_env=INFRA_ENV, mce_cluster=MCE, namespace=NAMESPACE)
ARGS = AddServerToMceRunArgs(input=INPUT)

# What server-scan would have been asked; a VM run asks it nothing.
SERVER_SCAN_CALLS = (
    "allocate_server", "acquire_servers", "lookup_install_candidate", "reserve_server",
    "release_server", "put_server_in_maintenance", "lookup_server_membership",
)

# The fake child's script for the test in hand. A module global: the fake runs
# unsandboxed in this process, and one test runs at a time.
CHILD: dict = {}


@workflow.defn(name="CreateVmWorkflow", sandboxed=False)
class _FakeCreateVm:
    """create-vm as its parent meets it: a VM made, a failure, or a run still going.

    `mode`: `ok` returns the VM; `fail` ends as create-vm does when no KV cluster
    has room; `hold` runs until it is cancelled — and then ends CANCELED, as the
    real one does once it has removed its own VM; `gate` returns the VM only once
    the test signals `release`.
    """

    def __init__(self) -> None:
        self._released = False

    @workflow.signal
    def release(self) -> None:
        self._released = True

    @workflow.run
    async def run(self, run_args: CreateVmRunArgs) -> CreateVmResult:
        request = run_args.input
        CHILD.setdefault("inputs", []).append(request)
        CHILD.setdefault("run_ids", []).append(workflow.info().run_id)
        mode = CHILD.get("mode", "ok")
        if mode == "fail":
            raise ApplicationError(
                "No KV cluster of zone az1 takes a kv32 VM: 2 over the buffer", type=NoKvClusterFitsError.__name__
            )
        if mode == "hold":
            try:
                await workflow.wait_condition(lambda: False)
            except asyncio.CancelledError:
                CHILD["cancelled"] = True
                raise
        if mode == "gate":
            await workflow.wait_condition(lambda: self._released)
        return CreateVmResult(
            vm_name=CHILD.get("vm_name", VM), mac=CHILD.get("mac", MAC), namespace=request.namespace,
            flavor=request.flavor, cpu=request.cpu, memory_gib=request.memory_gib, kv_cluster=KV_CLUSTER,
            buffer_percent=15, day1_revision="0123abc", infra_env=request.infra_env,
            mce_cluster=request.mce_cluster, site=request.site, zone=request.zone,
        )


@workflow.defn(name="RemoveServerFromMceWorkflow", sandboxed=False)
class _FakeRemoveServer:
    """remove-server-from-mce as a cancelled VM install meets it: the host taken out — or,
    with CHILD["remove"] == "fail", a BareMetalHost metal3 would not let go within its wait."""

    @workflow.run
    async def run(self, run_args: RemoveServerFromMceRunArgs) -> RemoveServerFromMceResult:
        request = run_args.input
        CHILD.setdefault("removals", []).append(request.server_name)
        if CHILD.get("remove") == "fail":
            raise ApplicationError(
                f"BareMetalHost {request.server_name} is still deleting after 15 min",
                type=BmhDeletionTimeoutError.__name__,
            )
        return RemoveServerFromMceResult(
            server_name=request.server_name, namespace=request.namespace,
            mce_cluster=request.mce_cluster, found=True, powered_off=True,
        )


def make_vm_mocks(
    *,
    labels: dict[str, str] | None = None,
    configured: bool = True,
    delete_error: Exception | None = None,
    bmh_error: Exception | None = None,
    **knobs,
):
    """make_mock_activities for a KubeVirt InfraEnv, plus the two activities only a VM run calls.

    `configured` False: the MCE's worker has no kubevirt-redfish host or user
    (read_kubevirt_bmc_config refuses). `delete_error` is what the KV cluster's
    worker answers a VM delete with; `bmh_error` what create_baremetal_host is
    refused with.
    """
    knobs.setdefault("segment", SEGMENT)
    calls, segment_acts, server_acts = make_mock_activities(
        agent_labels=KV_LABELS if labels is None else labels, **knobs
    )
    calls["read_kubevirt_bmc_config"] = []
    calls["delete_vm_on_cluster"] = []

    @activity.defn
    async def read_kubevirt_bmc_config(request: KubevirtBmcConfigRequest) -> KubevirtBmcConfig:
        calls["read_kubevirt_bmc_config"].append(request)
        calls["order"].append("read_kubevirt_bmc_config")
        if not configured:
            raise KubevirtBmcNotConfiguredError("KUBEVIRT_BMC_HOST_TEMPLATE not set; no VM was created")
        return KubevirtBmcConfig(host_template=HOST_TEMPLATE)

    @activity.defn
    async def delete_vm_on_cluster(request: VmDeleteRequest) -> VmDeleted:
        calls["delete_vm_on_cluster"].append(request)
        calls["order"].append("delete_vm_on_cluster")
        if delete_error is not None:
            raise delete_error
        return VmDeleted(deleted=True, names=[request.name or VM])

    server_acts = [*server_acts, read_kubevirt_bmc_config]
    if bmh_error is not None:

        @activity.defn(name="create_baremetal_host")
        async def refused_baremetal_host(request: BmhResourceRequest) -> CreatedResource:
            calls["create_baremetal_host"].append(request)
            calls["order"].append("create_baremetal_host")
            raise bmh_error

        server_acts = [a for a in server_acts if a.__name__ != "create_baremetal_host"]
        server_acts.append(refused_baremetal_host)
    return calls, segment_acts, server_acts, [delete_vm_on_cluster]


class _Harness:
    """The brain, the MCE's limb, the segment limb, the KV cluster's limb and the two children."""

    def __init__(
        self, mocks, *, mce_queue: str = MCE_QUEUE, child: bool = True, kv_worker: bool = True,
        real_time: bool = False,
    ) -> None:
        self._calls, self._segment_acts, self._server_acts, self._kv_acts = mocks
        self._mce_queue = mce_queue
        self._child = child
        self._kv_worker = kv_worker
        # A real Temporal dev server on the wall clock, for what the time-skipping
        # test server does not do as Temporal does (a parent's close policy).
        self._real_time = real_time
        self.brain: Worker | None = None

    async def __aenter__(self) -> Client:
        CHILD.clear()
        if self._real_time:
            self._env = await WorkflowEnvironment.start_local()
        else:
            self._env = await WorkflowEnvironment.start_time_skipping()
        self.env = self._env
        config = self._env.client.config()
        config["data_converter"] = pydantic_data_converter
        self.client = Client(**config)
        self.brain = self._new_brain()
        self._workers = [
            Worker(self.client, task_queue=self._mce_queue, activities=self._server_acts),
            Worker(self.client, task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE, activities=self._segment_acts),
            Worker(self.client, task_queue=REMOVE_SERVER_FROM_MCE_WORKFLOW_QUEUE, workflows=[_FakeRemoveServer]),
        ]
        if self._child:
            self._workers.append(Worker(self.client, task_queue=CREATE_VM_WORKFLOW_QUEUE, workflows=[_FakeCreateVm]))
        if self._kv_worker:
            self._workers.append(Worker(self.client, task_queue=KV_QUEUE, activities=self._kv_acts))
        await self.brain.__aenter__()
        for worker in self._workers:
            await worker.__aenter__()
        return self.client

    def _new_brain(self) -> Worker:
        # No cache: every workflow task replays the run from its history, so each
        # test also proves the branch is deterministic.
        return Worker(
            self.client, task_queue=ADD_SERVER_TO_MCE_WORKFLOW_QUEUE,
            workflows=[AddServerToMceWorkflow], max_cached_workflows=0,
        )

    async def stop_brain(self) -> None:
        await self.brain.__aexit__(None, None, None)
        self.brain = None

    async def start_brain(self) -> None:
        self.brain = self._new_brain()
        await self.brain.__aenter__()

    async def __aexit__(self, *exc_info) -> None:
        for worker in reversed(self._workers):
            await worker.__aexit__(*exc_info)
        if self.brain is not None:
            await self.brain.__aexit__(*exc_info)
        await self._env.__aexit__(*exc_info)


async def _start(client: Client, args: AddServerToMceRunArgs = ARGS, workflow_id: str | None = None) -> WorkflowHandle:
    return await client.start_workflow(
        AddServerToMceWorkflow.run, args, id=workflow_id or f"add-server-to-mce-{MCE}_{INFRA_ENV}_{uuid.uuid4().hex[:6]}",
        task_queue=ADD_SERVER_TO_MCE_WORKFLOW_QUEUE,
    )


async def _result(handle: WorkflowHandle):
    return await asyncio.wait_for(handle.result(), timeout=60)  # a misclassified error would retry for ever


async def _failure(handle: WorkflowHandle) -> ApplicationError:
    with pytest.raises(WorkflowFailureError) as excinfo:
        await _result(handle)
    cause = excinfo.value.cause
    cause = getattr(cause, "cause", cause) if not isinstance(cause, ApplicationError) else cause
    assert isinstance(cause, ApplicationError), cause
    return cause


async def _progress(handle: WorkflowHandle):
    return await handle.query(AddServerToMceWorkflow.progress)


async def _until_phase(handle: WorkflowHandle, phase: str) -> None:
    for _ in range(400):
        if (await _progress(handle)).phase == phase:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"the run never reached {phase}: it is at {(await _progress(handle)).phase}")


async def _cancelled(handle: WorkflowHandle):
    """The run ended CANCELED; its final progress."""
    with pytest.raises(WorkflowFailureError) as excinfo:
        await _result(handle)
    assert is_cancelled_exception(excinfo.value.cause), excinfo.value.cause
    return await _progress(handle)


async def _child_status(client: Client, parent: WorkflowHandle) -> WorkflowExecutionStatus | None:
    try:
        return (await client.get_workflow_handle(create_vm_child_workflow_id(parent.id)).describe()).status
    except RPCError:
        return None


def _no_server_scan(calls) -> None:
    assert {name: calls[name] for name in SERVER_SCAN_CALLS if calls[name]} == {}


class TestAVmIsMadeThenInstalled:
    async def test_the_run_has_one_vm_made_and_installs_exactly_that_one(self):
        mocks = make_vm_mocks()
        calls = mocks[0]
        async with _Harness(mocks) as client:
            handle = await _start(client)
            result = await _result(handle)
            history = await handle.fetch_history()

        assert (result.server_name, result.kv_cluster, result.boot_mac) == (VM, KV_CLUSTER, MAC)
        assert result.server_id is None  # server-scan allocated nothing
        assert result.bond_macs == [MAC] and result.vlan_id == VLAN
        assert result.bmc_vendor == "KUBEVIRT"
        assert result.bmc_address == (
            f"redfish-virtualmedia://kubevirt-redfish-kubevirt-redfish.apps.{KV_CLUSTER}.example.com"
            f"/redfish/v1/Systems/{VM}"
        )
        assert result.agent_name == f"agent-{VM}"
        assert result.create_vm_workflow_id == create_vm_child_workflow_id(handle.id)
        # The VM's `redbull.io/created-by` label: the child's own run id.
        assert result.create_vm_run_id == CHILD["run_ids"][0]
        _no_server_scan(calls)
        # The NMStateConfig first: the host must not boot an image from before its own network config.
        assert calls["order"] == [
            "check_infra_env", "read_kubevirt_bmc_config",
            "create_nmstate_config", "create_bmc_secret", "create_baremetal_host",
        ]
        assert calls["replace_conflicting_nmstate_config"] == []
        assert calls["delete_vm_on_cluster"] == []
        await Replayer(
            workflows=[AddServerToMceWorkflow], data_converter=pydantic_data_converter
        ).replay_workflow(history)

    async def test_create_vm_is_asked_for_what_the_labels_and_the_mce_name_say(self):
        mocks = make_vm_mocks()
        async with _Harness(mocks) as client:
            await _result(await _start(client))

        asked = CHILD["inputs"][0]
        assert (asked.flavor, asked.cpu, asked.memory_gib, asked.zone) == ("c200", 32, 240, "az1")
        assert (asked.site, asked.lifecycle) == ("five", "prod")  # the MCE's name
        assert (asked.mce_cluster, asked.infra_env, asked.namespace) == (MCE, INFRA_ENV, NAMESPACE)

    async def test_the_host_is_one_nic_on_the_mces_inventory_vlan(self):
        mocks = make_vm_mocks()
        calls = mocks[0]
        async with _Harness(mocks) as client:
            await _result(await _start(client))

        request = calls["create_baremetal_host"][0]
        assert request == calls["create_nmstate_config"][0] == calls["create_bmc_secret"][0]
        assert (request.server_name, request.bmc_vendor, request.vlan_id) == (VM, "KUBEVIRT", VLAN)
        assert [(m.logical_name, m.mac) for m in request.bond_members] == [("enp1s0", MAC)]
        assert request.bmc.path == f"/redfish/v1/Systems/{VM}"

    async def test_the_infraenvs_own_name_need_not_follow_the_convention(self):
        """The LABELS say it is a VM InfraEnv; create-vm is asked for what they state."""
        mocks = make_vm_mocks()
        odd = AddServerToMceRunArgs(input=INPUT.model_copy(update={"infra_env": "warm-pool-vms"}))
        async with _Harness(mocks) as client:
            result = await _result(await _start(client, odd))
        assert result.server_name == VM
        assert CHILD["inputs"][0].infra_env == "warm-pool-vms"


class TestNothingIsMadeWhenItCannotBeInstalled:
    """Everything that can fail without a VM fails before create-vm is started."""

    async def _refused(self, mocks, args=ARGS, **harness):
        async with _Harness(mocks, **harness) as client:
            handle = await _start(client, args)
            failure = await _failure(handle)
            child = await _child_status(client, handle)
        calls = mocks[0]
        assert child is None, "create-vm was started"
        _no_server_scan(calls)
        assert calls["create_baremetal_host"] == calls["delete_vm_on_cluster"] == []
        return failure

    async def test_a_named_server_is_refused(self):
        named = AddServerToMceRunArgs(input=INPUT.model_copy(update={"server_name": VM}))
        failure = await self._refused(make_vm_mocks(), named)
        assert failure.type == NamedServerOnKubevirtInfraEnvError.__name__
        assert "no server to name" in failure.message

    @pytest.mark.parametrize(
        ("labels", "words"),
        [
            ({**KV_LABELS, "redbull.io/size": "kv7"}, "an odd number"),
            ({**KV_LABELS, "redbull.io/size": "kvx"}, "is not `kv<cpu>`"),
            ({**KV_LABELS, "redbull.io/size": "kv512"}, "2..256"),
            ({k: v for k, v in KV_LABELS.items() if k != "topology.kubernetes.io/zone"}, "topology.kubernetes.io/zone is missing"),
            ({**KV_LABELS, "topology.kubernetes.io/zone": "zone-a"}, "is not `az<N>`"),
            ({**KV_LABELS, "redbull.io/flavor": "C200"}, "redbull.io/flavor"),
            ({**KV_LABELS, "redbull.io/site": "haifa"}, "is not its MCE's site"),
        ],
    )
    async def test_labels_that_cannot_say_which_vm(self, labels, words):
        failure = await self._refused(make_vm_mocks(labels=labels))
        assert failure.type == InfraEnvAgentLabelsError.__name__
        assert words in failure.message and "No VM was created" in failure.message

    async def test_every_label_problem_is_named_at_once(self):
        failure = await self._refused(make_vm_mocks(labels={"redbull.io/size": "kv7"}))
        for words in ("redbull.io/flavor is missing", "an odd number", "zone is missing", "redbull.io/site is missing"):
            assert words in failure.message

    async def test_an_mce_whose_name_does_not_say_its_site(self):
        mce = "ocp4-mce-bat-yam-01"
        mocks = make_vm_mocks(segment=SEGMENT.model_copy(update={"cluster_name": mce}))
        args = AddServerToMceRunArgs(input=INPUT.model_copy(update={"mce_cluster": mce}))
        failure = await self._refused(mocks, args, mce_queue=server_lifecycle_activity_queue(mce))
        assert failure.type == MceNameNotConventionalError.__name__
        assert "ocp4-<lifecycle>-mce-<site>-<id>" in failure.message

    async def test_a_worker_not_set_up_for_vms(self):
        failure = await self._refused(make_vm_mocks(configured=False))
        assert failure.type == KubevirtBmcNotConfiguredError.__name__

    async def test_a_worker_from_before_the_branch_makes_the_run_wait_with_no_vm(self):
        """An older limb has no read_kubevirt_bmc_config: retried until the new one polls."""
        mocks = make_vm_mocks()
        calls, segment_acts, server_acts, kv_acts = mocks
        older = [a for a in server_acts if a.__name__ != "read_kubevirt_bmc_config"]
        async with _Harness((calls, segment_acts, older, kv_acts)) as client:
            handle = await _start(client)
            await _until_phase(handle, "checking-kubevirt-bmc")
            for _ in range(200):
                pending = (await handle.describe()).raw_description.pending_activities
                if pending and pending[0].attempt >= 3:
                    break
                await asyncio.sleep(0.05)
            description = await handle.describe()
            child = await _child_status(client, handle)
            await handle.terminate()

        assert description.status == WorkflowExecutionStatus.RUNNING
        assert description.raw_description.pending_activities[0].attempt >= 3
        assert child is None


class TestTheChildDoesNotMakeAVm:
    async def test_its_failure_fails_the_run_with_its_own_reason(self):
        mocks = make_vm_mocks()
        calls = mocks[0]
        async with _Harness(mocks) as client:
            CHILD["mode"] = "fail"
            handle = await _start(client)
            failure = await _failure(handle)

        assert failure.type == CreateVmFailedError.__name__
        assert NoKvClusterFitsError.__name__ in failure.message and "2 over the buffer" in failure.message
        assert create_vm_child_workflow_id(handle.id) in failure.message
        assert calls["create_nmstate_config"] == calls["delete_vm_on_cluster"] == []

    async def test_one_already_running_under_its_id(self):
        """The child of an earlier run of the same id, still going.

        On a real dev server: the time-skipping one jumps an idle run to its
        ten-year timeout while a result is awaited, and the earlier run is idle.
        """
        mocks = make_vm_mocks()
        workflow_id = f"add-server-to-mce-{MCE}_{INFRA_ENV}_7"
        async with _Harness(mocks, real_time=True) as client:
            CHILD["mode"] = "hold"
            earlier = await client.start_workflow(
                "CreateVmWorkflow", CreateVmRunArgs(input={  # type: ignore[arg-type]
                    "mce_cluster": MCE, "infra_env": INFRA_ENV, "namespace": NAMESPACE, "flavor": "c200",
                    "site": "five", "zone": "az1", "cpu": 32, "memory_gib": 240, "lifecycle": "prod",
                }),
                id=create_vm_child_workflow_id(workflow_id), task_queue=CREATE_VM_WORKFLOW_QUEUE,
            )
            failure = await _failure(await _start(client, workflow_id=workflow_id))
            # The run that was already there is not this run's to stop: it goes on.
            still = (await earlier.describe()).status
            await earlier.terminate()

        assert still == WorkflowExecutionStatus.RUNNING

        assert failure.type == CreateVmAlreadyRunningError.__name__
        assert mocks[0]["create_nmstate_config"] == []

    async def test_a_person_cancelling_the_child_fails_the_run_it_does_not_cancel_it(self):
        mocks = make_vm_mocks()
        async with _Harness(mocks) as client:
            CHILD["mode"] = "hold"
            handle = await _start(client)
            await _until_phase(handle, "creating-vm")
            for _ in range(200):
                if await _child_status(client, handle) is not None:
                    break
                await asyncio.sleep(0.05)
            await client.get_workflow_handle(create_vm_child_workflow_id(handle.id)).cancel()
            failure = await _failure(handle)

        assert failure.type == CreateVmFailedError.__name__
        assert "it was cancelled" in failure.message


class TestARunThatDoesNotEndWithAnAgentLeavesNoVm:
    async def test_no_agent_the_host_comes_out_of_the_mce_then_the_vm_is_deleted(self):
        mocks = make_vm_mocks(agent_never_for={VM})
        calls = mocks[0]
        async with _Harness(mocks) as client:
            handle = await _start(client)
            failure = await _failure(handle)
            progress = await _progress(handle)

        assert failure.type == AgentNeverAppearedError.__name__
        assert f"The VM {VM} made on KV cluster {KV_CLUSTER}" in failure.message
        assert f"The VM {VM} was deleted on KV cluster {KV_CLUSTER}" in failure.message
        # A VM's words, not a bare-metal draw's (found in the 2026-11-26 lab).
        assert "it never registered an Agent" in failure.message
        assert "returned to the inventory" not in failure.message and "the bond" not in failure.message
        # Never the VM while its BareMetalHost may exist: metal3 deprovisions through its BMC.
        order = calls["order"]
        assert order.index("delete_host_resources") < order.index("delete_vm_on_cluster")
        deleted = calls["delete_vm_on_cluster"][0]
        assert (deleted.kv_cluster, deleted.namespace, deleted.name, deleted.mac) == (KV_CLUSTER, NAMESPACE, VM, MAC)
        assert deleted.request_id == CHILD["run_ids"][0]  # the VM's created-by label
        _no_server_scan(calls)
        assert progress.vm_rollback and "deleted" in progress.vm_rollback
        # Neither of the page's two bare-metal patterns: it renders as written.
        assert not failure.message.startswith("server-scan allocated")
        assert "candidate(s)" not in failure.message

    async def test_a_baremetalhost_metal3_will_not_let_go_keeps_the_vm(self):
        mocks = make_vm_mocks(agent_never_for={VM}, bmh_stays_deleting={VM})
        calls = mocks[0]
        async with _Harness(mocks) as client:
            failure = await _failure(await _start(client))

        assert failure.type == AgentNeverAppearedError.__name__
        assert "BmhDeletionTimeoutError" in failure.message
        assert f"The VM {VM} was KEPT on KV cluster {KV_CLUSTER}" in failure.message
        assert f"redbull.io/created-by={CHILD['run_ids'][0]}" in failure.message
        assert calls["delete_vm_on_cluster"] == []

    async def test_a_host_of_the_vms_name_already_on_the_mce(self):
        mocks = make_vm_mocks(already_installed={VM})
        calls = mocks[0]
        async with _Harness(mocks) as client:
            failure = await _failure(await _start(client))

        assert failure.type == VmNameClashError.__name__
        assert "another machine's" in failure.message and "was deleted" in failure.message
        # Nothing written or removed on the MCE: the host there is not this run's.
        assert calls["create_nmstate_config"] == calls["create_baremetal_host"] == []
        assert calls["delete_host_resources"] == []
        assert len(calls["delete_vm_on_cluster"]) == 1

    async def test_a_conflicting_nmstateconfig_touches_nothing_on_the_mce(self):
        conflict = BmhConflictError(f"NMStateConfig {VM} already exists and does not match this request: MACs")
        mocks = make_vm_mocks(nmstate_error=conflict)
        calls = mocks[0]
        async with _Harness(mocks) as client:
            handle = await _start(client)
            failure = await _failure(handle)
            progress = await _progress(handle)

        assert failure.type == BmhConflictError.__name__  # the worker's own failure, unchanged
        assert calls["delete_host_resources"] == [] and calls["create_baremetal_host"] == []
        assert len(calls["delete_vm_on_cluster"]) == 1
        assert "Nothing on the MCE was removed" in progress.vm_rollback
        assert "was deleted" in progress.vm_rollback

    async def test_a_refused_baremetalhost_takes_the_rest_out_then_the_vm(self):
        """The Secret has no host to name it, so it is deleted by its own name."""
        refusal = BmcCredentialsMissingError("No BMC credentials configured for vendor KUBEVIRT")
        mocks = make_vm_mocks(bmh_error=refusal, secret_outlives=True)
        calls = mocks[0]
        async with _Harness(mocks) as client:
            handle = await _start(client)
            failure = await _failure(handle)
            progress = await _progress(handle)

        assert failure.type == BmcCredentialsMissingError.__name__
        order = calls["order"]
        assert order.index("delete_host_resources") < order.index("delete_vm_on_cluster")
        assert f"kubevirt-cred-{VM}" in [ref.name for ref in calls["delete_bmc_secret"]]
        assert "Removed from the MCE" in progress.vm_rollback

    async def test_a_vm_its_cluster_will_not_delete_is_named_with_its_label(self):
        mocks = make_vm_mocks(agent_never_for={VM}, delete_error=KvClusterAccessError("403: delete virtualmachines"))
        async with _Harness(mocks) as client:
            failure = await _failure(await _start(client))

        assert failure.type == AgentNeverAppearedError.__name__
        assert "could NOT be deleted (KvClusterAccessError" in failure.message
        assert f"redbull.io/created-by={CHILD['run_ids'][0]}" in failure.message


class TestACancelLeavesNoVm:
    async def test_during_the_child_the_child_is_stopped_and_removes_its_own(self):
        mocks = make_vm_mocks()
        calls = mocks[0]
        async with _Harness(mocks) as client:
            CHILD["mode"] = "hold"
            handle = await _start(client)
            await _until_phase(handle, "creating-vm")
            for _ in range(200):
                if await _child_status(client, handle) is not None:
                    break
                await asyncio.sleep(0.05)
            await handle.cancel()
            progress = await _cancelled(handle)
            child = await _child_status(client, handle)

        assert child == WorkflowExecutionStatus.CANCELED and CHILD.get("cancelled")
        assert "leaves no VM" in progress.cancel_rollback
        assert calls["delete_vm_on_cluster"] == [] and calls["create_nmstate_config"] == []

    async def test_while_waiting_for_the_agent_the_host_is_taken_out_then_the_vm_deleted(self):
        mocks = make_vm_mocks(agent_never_for={VM})
        calls = mocks[0]
        async with _Harness(mocks) as client:
            handle = await _start(client)
            await _until_phase(handle, "awaiting-agent")
            await handle.cancel()
            progress = await _cancelled(handle)
            removal = await client.get_workflow_handle(
                remove_server_from_mce_workflow_id(VM), result_type=RemoveServerFromMceResult
            ).result()

        assert removal.found  # remove-server-from-mce took the host out
        assert len(calls["delete_vm_on_cluster"]) == 1
        assert calls["delete_vm_on_cluster"][0].request_id == CHILD["run_ids"][0]
        assert f"{VM} taken out of the MCE" in progress.cancel_rollback
        assert "was deleted" in progress.cancel_rollback
        _no_server_scan(calls)

    async def test_once_the_child_has_completed_the_cancel_still_finds_the_vm(self):
        """The cancel and the child's completion in ONE workflow task: the VM is known, and deleted.

        The brain is stopped while the child completes, so both reach the run in
        the same task — the interleaving in which awaiting the child's handle
        directly would erase the cancel or lose the result.
        """
        mocks = make_vm_mocks()
        calls = mocks[0]
        harness = _Harness(mocks)
        async with harness as client:
            CHILD["mode"] = "gate"
            handle = await _start(client)
            await _until_phase(handle, "creating-vm")
            for _ in range(200):
                if await _child_status(client, handle) is not None:
                    break
                await asyncio.sleep(0.05)
            await harness.stop_brain()
            child = client.get_workflow_handle(create_vm_child_workflow_id(handle.id))
            await child.signal("release")
            await asyncio.wait_for(child.result(), timeout=30)
            await handle.cancel()
            await harness.start_brain()
            progress = await _cancelled(handle)

        assert len(calls["delete_vm_on_cluster"]) == 1
        assert calls["delete_vm_on_cluster"][0].name == VM
        assert "was deleted" in progress.cancel_rollback
        assert calls["create_baremetal_host"] == []

    async def test_after_the_agent_nothing_is_undone(self):
        """There is no wait after the Agent for a VM, so the run is simply complete."""
        mocks = make_vm_mocks()
        async with _Harness(mocks) as client:
            handle = await _start(client)
            result = await _result(handle)
        assert result.agent_name and mocks[0]["delete_vm_on_cluster"] == []

    async def test_a_cancel_during_the_no_agent_teardown_still_waits_for_the_host(self):
        """The VM is deleted only after remove-server-from-mce took the host out."""
        mocks = make_vm_mocks(agent_never_for={VM}, bmh_stays_deleting={VM})
        calls = mocks[0]
        harness = _Harness(mocks)
        async with harness as client:
            handle = await _start(client)
            await _until_phase(handle, "awaiting-agent")
            await harness.env.sleep(timedelta(hours=1, minutes=2))  # past the Agent deadline
            await _until_phase(handle, "rolling-back")
            await handle.cancel()
            progress = await _cancelled(handle)

        assert f"{VM} taken out of the MCE" in progress.cancel_rollback
        assert len(calls["delete_vm_on_cluster"]) == 1


    async def test_a_host_that_cannot_be_taken_out_keeps_the_vm(self):
        """NEVER the VM while its BareMetalHost may exist: remove-server-from-mce failing
        (metal3 still holds the host) leaves the VM, named with its label."""
        mocks = make_vm_mocks(agent_never_for={VM})
        calls = mocks[0]
        async with _Harness(mocks) as client:
            CHILD["remove"] = "fail"
            handle = await _start(client)
            await _until_phase(handle, "awaiting-agent")
            await handle.cancel()
            progress = await _cancelled(handle)

        assert CHILD["removals"] == [VM]
        assert calls["delete_vm_on_cluster"] == []
        assert "could not take" in progress.cancel_rollback and "BmhDeletionTimeoutError" in progress.cancel_rollback
        assert f"the VM is KEPT on KV cluster {KV_CLUSTER}" in progress.cancel_rollback
        assert f"redbull.io/created-by={CHILD['run_ids'][0]}" in progress.cancel_rollback

    async def test_a_terminated_child_is_said_to_have_run_no_rollback(self):
        """A create-vm someone TERMINATED made no promise about its VM: the failure names the label."""
        mocks = make_vm_mocks()
        async with _Harness(mocks, real_time=True) as client:
            CHILD["mode"] = "hold"
            handle = await _start(client)
            await _until_phase(handle, "creating-vm")
            for _ in range(200):
                if await _child_status(client, handle) is not None:
                    break
                await asyncio.sleep(0.05)
            await client.get_workflow_handle(create_vm_child_workflow_id(handle.id)).terminate()
            failure = await _failure(handle)

        assert failure.type == CreateVmFailedError.__name__
        assert "without its own rollback" in failure.message
        assert f"redbull.io/created-by={CHILD['run_ids'][0]}" in failure.message


class TestAParentThatIsKilled:
    async def test_a_terminated_run_cancels_its_child(self):
        """REQUEST_CANCEL: terminate runs no code in the parent, so the child's policy does it."""
        mocks = make_vm_mocks()
        async with _Harness(mocks, real_time=True) as client:
            CHILD["mode"] = "hold"
            handle = await _start(client)
            await _until_phase(handle, "creating-vm")
            for _ in range(200):
                if await _child_status(client, handle) is not None:
                    break
                await asyncio.sleep(0.05)
            await handle.terminate()
            child = client.get_workflow_handle(create_vm_child_workflow_id(handle.id))
            with pytest.raises(WorkflowFailureError):
                await asyncio.wait_for(child.result(), timeout=30)
            status = (await child.describe()).status

        assert status == WorkflowExecutionStatus.CANCELED and CHILD.get("cancelled")


class TestBareMetalIsUntouched:
    async def test_a_bare_metal_run_records_no_marker_of_the_branch(self):
        """The patch is asked only for a KubeVirt InfraEnv, so every other history keeps its shape."""
        from tests.test_add_server_to_mce_workflow import INPUT as METAL_INPUT
        from tests.test_add_server_to_mce_workflow import _Harness as MetalHarness

        calls, segment_acts, server_acts = make_mock_activities()
        async with MetalHarness(segment_acts, server_acts) as client:
            handle = await client.start_workflow(
                AddServerToMceWorkflow.run, AddServerToMceRunArgs(input=METAL_INPUT),
                id=f"test-{uuid.uuid4()}", task_queue=ADD_SERVER_TO_MCE_WORKFLOW_QUEUE,
            )
            await _result(handle)
            history = await handle.fetch_history()

        markers = [
            str(event.marker_recorded_event_attributes)
            for event in history.events
            if event.HasField("marker_recorded_event_attributes")
        ]
        assert any("server-scan-install-lock" in marker for marker in markers)  # the patches ARE read this way
        assert not any("add-server-creates-vm" in marker for marker in markers)
        assert len(calls["allocate_server"]) == 1

    async def test_a_limb_from_before_the_labels_draws_from_server_scan_as_it_did(self):
        """No `agent_labels` key at all: nothing says the InfraEnv is KubeVirt."""
        mocks = make_vm_mocks(infra_env_predates_labels=True)
        calls = mocks[0]
        async with _Harness(mocks) as client:
            handle = await _start(client)
            await _result(handle)
            child = await _child_status(client, handle)

        assert child is None and len(calls["allocate_server"]) == 1
