"""create-vm: real CreateVmWorkflow, mock activities, time-skipping env.

One workflow queue and the ONE vm-lifecycle activity queue. What is pinned is
classification and what each activity received, not the sequence of calls:
each permanent error fails the run with its type after one attempt, a
transient one is retried, the deadline rolls the VM back before failing, and
nothing else ever deletes.
"""

from __future__ import annotations

import asyncio
import re
import uuid

import pytest
from temporalio import activity
from temporalio.client import Client, WorkflowFailureError
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from shared.consts import CREATE_VM_WORKFLOW_QUEUE, VM_LIFECYCLE_ACTIVITY_QUEUE
from shared.exceptions import (
    KvAgentAuthError,
    KvVmApiUnavailableError,
    VmDisappearedError,
    VmIdentityConflictError,
    VmNamespaceNotFoundError,
    VmRequestRejectedError,
)
from shared.models.vm_lifecycle import (
    CreateVmInput,
    CreateVmRunArgs,
    KvClusterEntry,
    KvClusterList,
    KvClusterListRequest,
    KvClusterRank,
    KvPlacement,
    KvPlacementRequest,
    VmCreated,
    VmCreateRequest,
    VmDeleted,
    VmDeleteRequest,
    VmReadyOutcome,
    VmReadyRequest,
)
from workflow_domains.vm_lifecycle.create_vm import CreateVmWorkflow, mac_tail, suffix_is_taken

MCE = "ocp4-prod-mce-bat-yam-a"
INFRA_ENV = "x201-zone-a-8"
INPUT = CreateVmInput(mce_cluster=MCE, infra_env=INFRA_ENV, namespace="inventory", site="bat-yam", zone="a", cpu=8, memory_gib=64)
A1, A2 = "ocp4-kubevirt-bat-yam-a1", "ocp4-kubevirt-bat-yam-a2"
GIB = 1024**3


def _entry(name: str) -> KvClusterEntry:
    return KvClusterEntry(name=name, cluster_name=name.removeprefix("ocp4-"), hosted_cluster=f"openshift-{name.removeprefix('ocp4-')}",
                          path=f"sites/bat-yam/mces/ocp4-seed-bat-yam-a/hostedClusters/{name}.yaml")


def _placement(chosen: str | None = A2, *, known_macs=("02:a1:b2:00:00:01",)) -> KvPlacement:
    def row(name, fits, reason=None, fill=0.5):
        return KvClusterRank(name=name, cluster_name=name.removeprefix("ocp4-"), reachable=reason not in {"unreachable", "no-agent"},
                             fits=fits, skip_reason=reason, cpu_fill_after=fill, memory_fill_after=fill,
                             agent_host=f"kv-capacity.apps.{name.removeprefix('ocp4-')}.test")
    if chosen is None:
        ranked = [row(A1, False, "fit-buffer"), row(A2, False, "no-agent")]
    else:
        ranked = [row(A2, True, fill=0.6), row(A1, True, fill=0.4)]
    return KvPlacement(ranked=ranked, chosen=chosen, buffer_percent=30, pod_cpu_millicores=7000,
                       pod_memory_bytes=65 * GIB, known_macs=list(known_macs))


def make_mocks(
    *,
    clusters: list[KvClusterEntry] | None = None,
    placement: KvPlacement | None = None,
    rank_error: Exception | None = None,
    create_errors: list[Exception | None] | None = None,
    created_flag: bool = True,
    ready: VmReadyOutcome | None = None,
    ready_error: Exception | None = None,
    delete_error: Exception | None = None,
):
    calls: dict[str, list] = {k: [] for k in ("list", "rank", "create", "ready", "delete", "order")}
    create_errors = list(create_errors or [])

    @activity.defn
    async def list_kv_clusters(request: KvClusterListRequest) -> KvClusterList:
        calls["list"].append(request)
        calls["order"].append("list")
        return KvClusterList(clusters=[_entry(A1), _entry(A2)] if clusters is None else clusters, revision="abc123")

    @activity.defn
    async def rank_kv_clusters(request: KvPlacementRequest) -> KvPlacement:
        calls["rank"].append(request)
        calls["order"].append("rank")
        if rank_error is not None:
            raise rank_error
        return placement if placement is not None else _placement()

    @activity.defn
    async def create_vm_on_cluster(request: VmCreateRequest) -> VmCreated:
        calls["create"].append(request)
        calls["order"].append("create")
        error = create_errors.pop(0) if create_errors else None
        if error is not None:
            raise error
        return VmCreated(name=request.name, namespace=request.namespace, mac=f"02:a1:b2:{mac_tail(request.suffix_hex)}",
                         cpu=request.cpu, memory_gib=request.memory_gib, created=created_flag, kv_cluster=request.kv_cluster.name)

    @activity.defn
    async def await_vm_ready(request: VmReadyRequest) -> VmReadyOutcome:
        calls["ready"].append(request)
        calls["order"].append("ready")
        if ready_error is not None:
            raise ready_error
        return ready if ready is not None else VmReadyOutcome(result="done", root_disk_phase="Succeeded", root_disk_bound=True, admitted=True, polls=4)

    @activity.defn
    async def delete_vm_on_cluster(request: VmDeleteRequest) -> VmDeleted:
        calls["delete"].append(request)
        calls["order"].append("delete")
        if delete_error is not None:
            raise delete_error
        return VmDeleted(deleted=True)

    return calls, [list_kv_clusters, rank_kv_clusters, create_vm_on_cluster, await_vm_ready, delete_vm_on_cluster]


class _Harness:
    def __init__(self, activities) -> None:
        self._activities = activities

    async def __aenter__(self) -> Client:
        self._env = await WorkflowEnvironment.start_time_skipping()
        config = self._env.client.config()
        config["data_converter"] = pydantic_data_converter
        client = Client(**config)
        self._workers = [
            Worker(client, task_queue=CREATE_VM_WORKFLOW_QUEUE, workflows=[CreateVmWorkflow]),
            Worker(client, task_queue=VM_LIFECYCLE_ACTIVITY_QUEUE, activities=self._activities),
        ]
        for worker in self._workers:
            await worker.__aenter__()
        return client

    async def __aexit__(self, *exc_info) -> None:
        for worker in reversed(self._workers):
            await worker.__aexit__(*exc_info)
        await self._env.__aexit__(*exc_info)


async def _execute(client: Client, wf_input: CreateVmInput = INPUT):
    return await asyncio.wait_for(
        client.execute_workflow(
            CreateVmWorkflow.run, CreateVmRunArgs(input=wf_input), id=f"test-{uuid.uuid4()}", task_queue=CREATE_VM_WORKFLOW_QUEUE,
        ),
        timeout=60,
    )


def _error(exc: WorkflowFailureError) -> ApplicationError:
    cause = exc.cause
    if isinstance(cause, ActivityError):
        cause = cause.cause
    assert isinstance(cause, ApplicationError)
    return cause


class TestTheDraw:
    def test_a_suffix_whose_tail_is_in_the_zone_is_taken(self):
        assert suffix_is_taken("0a1b2c", ["02:A1:B2:0A:1B:2C"])
        assert suffix_is_taken("0a1b2c", ["06:00:00:0a:1b:2c"])  # under any prefix
        assert not suffix_is_taken("0a1b2d", ["02:a1:b2:0a:1b:2c"])
        assert not suffix_is_taken("0a1b2c", [])

    def test_the_tail_is_three_octets(self):
        assert mac_tail("0a1b2c") == "0a:1b:2c"


async def test_the_vm_is_created_on_the_chosen_cluster_and_its_disk_awaited():
    calls, acts = make_mocks()
    async with _Harness(acts) as client:
        result = await _execute(client)

    assert calls["order"] == ["list", "rank", "create", "ready"]
    assert calls["list"] == [KvClusterListRequest(site="bat-yam", zone="a")]
    rank = calls["rank"][0]
    assert [c.name for c in rank.candidates] == [A1, A2]
    assert (rank.candidates[0].cluster_name, rank.candidates[0].hosted_cluster, rank.candidates[0].site, rank.candidates[0].zone) == (
        "kubevirt-bat-yam-a1", "openshift-kubevirt-bat-yam-a1", "bat-yam", "a")
    assert (rank.cpu, rank.memory_gib) == (8, 64)
    create = calls["create"][0]
    assert create.kv_cluster.name == A2 and create.kv_cluster.agent_host == "kv-capacity.apps.kubevirt-bat-yam-a2.test"
    assert re.fullmatch(r"[0-9a-f]{6}", create.suffix_hex)
    assert create.name == f"ocp-{INFRA_ENV}-{create.suffix_hex}"
    assert (create.namespace, create.cpu, create.memory_gib, create.labels) == ("inventory", 8, 64, {})
    ready = calls["ready"][0]
    assert (ready.kv_cluster.name, ready.namespace, ready.name, ready.deadline_minutes) == (A2, "inventory", create.name, 10)
    assert calls["delete"] == []
    assert (result.vm_name, result.mac, result.kv_cluster.name, result.redraws, result.created) == (
        create.name, f"02:a1:b2:{mac_tail(create.suffix_hex)}", A2, 0, True)
    assert (result.root_disk_phase, result.namespace, result.cpu, result.memory_gib) == ("Succeeded", "inventory", 8, 64)
    assert (result.infra_env, result.mce_cluster, result.site, result.zone) == (INFRA_ENV, MCE, "bat-yam", "a")
    assert result.placement.chosen == A2 and result.placement.ranked[0].name == A2


async def test_a_create_that_already_landed_is_reported_not_retried_into_a_second_vm():
    calls, acts = make_mocks(created_flag=False)
    async with _Harness(acts) as client:
        result = await _execute(client)
    assert result.created is False and len(calls["create"]) == 1


@pytest.mark.parametrize("clusters", [[], [_entry("ocp4-kubevirt-bat-yam-b1")], [_entry("ocp4-prod-bat-yam-a1")]])
async def test_no_kubevirt_cluster_of_the_zone_fails_before_any_read(clusters):
    calls, acts = make_mocks(clusters=clusters)
    async with _Harness(acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client)
    error = _error(excinfo.value)
    assert error.type == "NoKvClusterError"
    assert "sites/bat-yam/mces/ocp4-seed-bat-yam-a/hostedClusters/" in error.message
    assert "ocp4-kubevirt-bat-yam-a" in error.message
    assert calls["rank"] == [] and calls["create"] == []


async def test_a_sibling_zones_file_in_the_folder_is_not_a_candidate():
    calls, acts = make_mocks(clusters=[_entry(A1), _entry("ocp4-kubevirt-bat-yam-b1")], placement=_placement(A1))
    async with _Harness(acts) as client:
        result = await _execute(client)
    assert [c.name for c in calls["rank"][0].candidates] == [A1]
    assert result.kv_cluster.name == A1


async def test_a_limb_choosing_outside_the_candidates_fails_the_run_rather_than_hanging_it():
    calls, acts = make_mocks(clusters=[_entry(A1)], placement=_placement(A2))
    async with _Harness(acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client)
    error = _error(excinfo.value)
    assert error.type == "NoKvClusterFitsError" and "limb bug" in error.message
    assert calls["create"] == []


async def test_nothing_fitting_fails_with_the_table_summarised_per_reason():
    calls, acts = make_mocks(placement=_placement(None))
    async with _Harness(acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client)
    error = _error(excinfo.value)
    assert error.type == "NoKvClusterFitsError"
    assert f"{A1} would go past the headroom buffer" in error.message
    assert f"{A2} have no kv-capacity agent" in error.message
    assert "8 vCPU / 64 GiB" in error.message and "30% buffer" in error.message
    assert calls["create"] == [] and calls["delete"] == []


async def test_a_name_taken_in_between_is_redrawn_up_to_three_times():
    calls, acts = make_mocks(create_errors=[VmIdentityConflictError("held"), VmIdentityConflictError("held")])
    async with _Harness(acts) as client:
        result = await _execute(client)
    assert len(calls["create"]) == 3
    assert len({c.name for c in calls["create"]}) == 3
    assert result.redraws == 2 and result.vm_name == calls["create"][-1].name
    assert calls["delete"] == []


async def test_three_taken_draws_exhaust_the_run_leaving_nothing():
    calls, acts = make_mocks(create_errors=[VmIdentityConflictError("held")] * 3)
    async with _Harness(acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client)
    error = _error(excinfo.value)
    assert error.type == "VmNameExhaustedError"
    assert len(calls["create"]) == 3 and calls["ready"] == [] and calls["delete"] == []


async def test_an_agent_outage_on_the_create_is_retried():
    calls, acts = make_mocks(create_errors=[KvVmApiUnavailableError("down")])
    async with _Harness(acts) as client:
        result = await _execute(client)
    assert len(calls["create"]) == 2
    # The retry is the SAME name: Temporal retried the activity, the run drew nothing new.
    assert calls["create"][0].name == calls["create"][1].name and result.redraws == 0


@pytest.mark.parametrize(
    ("kwargs", "error_type", "attempted"),
    [
        ({"rank_error": KvAgentAuthError("every cluster refused")}, "KvAgentAuthError", "rank"),
        ({"create_errors": [VmNamespaceNotFoundError("no inventory")]}, "VmNamespaceNotFoundError", "create"),
        ({"create_errors": [VmRequestRejectedError("memory")]}, "VmRequestRejectedError", "create"),
        ({"ready_error": VmDisappearedError("gone")}, "VmDisappearedError", "ready"),
    ],
)
async def test_each_permanent_error_fails_the_run_after_one_attempt_and_deletes_nothing(kwargs, error_type, attempted):
    calls, acts = make_mocks(**kwargs)
    async with _Harness(acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client)
    assert _error(excinfo.value).type == error_type
    assert len(calls[attempted]) == 1
    assert calls["delete"] == []


async def test_a_root_disk_that_never_binds_rolls_the_vm_back_then_fails():
    outcome = VmReadyOutcome(result="deadline", root_disk_phase="Pending", root_disk_bound=False, admitted=True,
                             detail="root disk x-root phase Pending, bound False, admitted True", polls=75)
    calls, acts = make_mocks(ready=outcome)
    async with _Harness(acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client)
    error = _error(excinfo.value)
    assert error.type == "VmRootDiskNotBoundError"
    assert calls["order"] == ["list", "rank", "create", "ready", "delete"]
    create = calls["create"][0]
    delete = calls["delete"][0]
    assert (delete.kv_cluster.name, delete.namespace, delete.name, delete.mac) == (A2, "inventory", create.name, f"02:a1:b2:{mac_tail(create.suffix_hex)}")
    assert "phase Pending" in error.message and "WaitForFirstConsumer" in error.message and "deleted" in error.message


async def test_a_replaced_vm_is_not_deleted_and_the_conflict_is_the_failure():
    outcome = VmReadyOutcome(result="deadline", root_disk_phase="Pending", detail="x", polls=75)
    calls, acts = make_mocks(ready=outcome, delete_error=VmIdentityConflictError("the VM's MAC is not the one given"))
    async with _Harness(acts) as client:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _execute(client)
    assert _error(excinfo.value).type == "VmIdentityConflictError"
    assert len(calls["delete"]) == 1


async def test_progress_reports_the_phase_and_what_was_chosen():
    calls, acts = make_mocks()
    async with _Harness(acts) as client:
        handle = await client.start_workflow(
            CreateVmWorkflow.run, CreateVmRunArgs(input=INPUT), id=f"test-{uuid.uuid4()}", task_queue=CREATE_VM_WORKFLOW_QUEUE,
        )
        result = await asyncio.wait_for(handle.result(), timeout=60)
        progress = await handle.query(CreateVmWorkflow.progress)
    assert progress.phase == "completed"
    assert progress.kv_candidates == [A1, A2]
    assert (progress.kv_cluster, progress.vm_name, progress.mac, progress.redraws) == (A2, result.vm_name, result.mac, 0)
