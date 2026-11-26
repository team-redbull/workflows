"""delete-vm (workflow_domains/vm_lifecycle/delete_vm.py): the REAL workflow on the fake KV
fleet (tests/kv_fleet.py), whose clusters run the REAL limb code over an in-memory API with
storage that behaves — a deleted VM takes its DataVolume, a DataVolume its claim, a claim
(once no pod mounts it) its PersistentVolume, which a `Retain` policy leaves Released.

The clock is moved by hand (tests/manual_time.py) and the workflow keeps its production
waits. The scenarios at the bottom are also what tests/test_delete_vm_replay.py records."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
from temporalio.client import WorkflowExecutionStatus, WorkflowFailureError, WorkflowHandle
from temporalio.exceptions import CancelledError

from activities.vm_lifecycle import vm_spec
from shared.consts import DELETE_VM_WORKFLOW_QUEUE
from shared.exceptions import (
    KvClusterAccessError,
    KvClusterApiError,
    KvClusterMismatchError,
    KvClusterWorkerAbsentError,
    VmDeletionTimeoutError,
    VmIdentityConflictError,
    VmRequestRejectedError,
    VmVolumesLeftInStorageError,
)
from shared.models.vm_lifecycle import DeleteVmInput, DeleteVmProgress, DeleteVmResult, DeleteVmRunArgs
from shared.workflow_ids import delete_vm_workflow_id
from tests.kv_fleet import FakeKvCluster, KvFleet, stopped_vm
from tests.test_create_vm_workflow import A, B, MCE, Harness, cause, fleet_of
from workflow_domains.vm_lifecycle import delete_vm
from workflow_domains.vm_lifecycle.delete_vm import DeleteVmWorkflow

NS = "inventory"
RUN = "0196f6a2-1111-7222-8333-444455556666"


async def put_vm(cluster: FakeKvCluster, serial: str = "k7q2m9xd", *, run: str = RUN, mce: str = MCE,
                 flavor: str = "c200") -> str:
    """A VM on the cluster as a completed create-vm run leaves it: Halted, labelled, its root
    disk imported and bound to a PersistentVolume. Its name."""
    body = vm_spec.build(
        namespace=NS, flavor=flavor, cpu=32, memory_gib=240, site="five", zone="az1", mce=mce, serial=serial,
        request_id=run, workflow_id="create-vm-of-the-test", kv_cluster=cluster.name,
        redfish_label=cluster.values.redfish_label, storage_class_name="cephfs",
        network_name=cluster.values.network_name, node_selector={}, root_disk_size="120Gi",
    )
    saved = list(cluster.calls)
    created = await cluster.create_vm(NS, body)
    cluster.calls = saved
    name = vm_spec.name_of(created)
    cluster.bind_now(NS, name)
    return name


def delete_input(vm_name: str, *, kv_cluster: str = A, **overrides) -> DeleteVmInput:
    values = dict(kv_cluster=kv_cluster, namespace=NS, vm_name=vm_name, mce_cluster=MCE, why="its host left the MCE")
    values.update(overrides)
    return DeleteVmInput(**values)


class DeleteHarness(Harness):
    async def begin(self, request: DeleteVmInput) -> WorkflowHandle:
        """Start delete-vm on its own id, as its parent will."""
        return await self.client.start_workflow(
            DeleteVmWorkflow.run, DeleteVmRunArgs(input=request),
            id=delete_vm_workflow_id(request.kv_cluster, request.vm_name), task_queue=DELETE_VM_WORKFLOW_QUEUE,
            result_type=DeleteVmResult,
        )

    async def delete(self, request: DeleteVmInput, **kwargs) -> DeleteVmResult:
        return await self.result(await self.begin(request), **kwargs)

    async def fails(self, request: DeleteVmInput, **kwargs) -> WorkflowFailureError:
        return await self.failure(await self.begin(request), **kwargs)

    async def progress(self, handle: WorkflowHandle) -> DeleteVmProgress:
        return await handle.query(DeleteVmWorkflow.progress)


def writes(cluster: FakeKvCluster) -> list[str]:
    return [call for call in cluster.calls if call.startswith("delete_")]


def nothing_left(cluster: FakeKvCluster) -> bool:
    return not (cluster.vms or cluster.datavolumes or cluster.pvcs or cluster.pvs)


# --- the delete ----------------------------------------------------------------------------------


class TestTheDelete:
    async def test_a_vm_goes_with_its_datavolume_its_claim_and_its_volume(self):
        fleet = fleet_of(A)
        cluster = fleet.clusters[A]
        cluster.volume_release_polls = 3  # the CSI driver takes a few polls to delete the volume
        name = await put_vm(cluster)
        [volume] = cluster.pvs.values()
        handle_of_volume = volume["spec"]["csi"]["volumeHandle"]
        async with DeleteHarness(fleet) as h:
            handle = await h.begin(delete_input(name, created_by=RUN))
            result = await h.result(handle)
            progress = await h.progress(handle)
        assert handle.id == f"delete-vm-{A}_{name}"
        assert result.model_dump(exclude={"volumes_released"}) == {
            "kv_cluster": A, "namespace": NS, "vm_name": name, "found": True, "vm_deleted": True, "was_running": False,
            "datavolumes_deleted": [f"{name}-root"], "claims_deleted": [f"{name}-root"], "import_sources_deleted": [],
            "polls": 4,
        }
        [released] = result.volumes_released
        assert (released.name, released.claim) == (volume["metadata"]["name"], f"{name}-root")
        assert (released.driver, released.volume_handle, released.reclaim_policy) == ("fake.csi.redbull.io", handle_of_volume, "Delete")
        assert nothing_left(cluster)
        assert progress.phase == "completed" and progress.found and progress.remaining == "nothing is left"
        assert progress.deleted == [f"VirtualMachine {name}", f"claim {name}-root"]
        assert progress.volumes == [released.name]

    async def test_the_vm_create_vm_made_is_the_one_delete_vm_takes(self):
        """End to end with the real create-vm: the run id and the MAC its result carries are
        the identity delete-vm is given."""
        fleet = fleet_of(A)
        cluster = fleet.clusters[A]
        async with DeleteHarness(fleet) as h:
            created_handle = await h.start()
            created = await h.result(created_handle)
            run_id = (await created_handle.describe()).run_id
            assert len(cluster.pvs) == 1 and created.kv_cluster == A
            result = await h.delete(delete_input(created.vm_name, created_by=run_id, mac=created.mac))
        assert result.found and result.vm_deleted and len(result.volumes_released) == 1
        assert nothing_left(cluster)

    async def test_nothing_there_completes_without_a_write(self):
        fleet = fleet_of(A)
        cluster = fleet.clusters[A]
        neighbour = await put_vm(cluster, "neighbr1")
        async with DeleteHarness(fleet) as h:
            handle = await h.begin(delete_input("ocp-c200-kv32-five-gone0000"))
            result = await h.result(handle)
            assert (await h.progress(handle)).found is False
        assert result.found is False and not result.vm_deleted and result.volumes_released == []
        assert writes(cluster) == [] and (NS, neighbour) in cluster.vms
        assert "get_pv" not in cluster.calls  # it never reached the wait

    async def test_a_boot_iso_left_inserted_goes_with_its_import_source(self):
        fleet = fleet_of(A)
        cluster = fleet.clusters[A]
        name = await put_vm(cluster)
        iso = cluster.add_bootiso(NS, name, cdi=True)
        stray = cluster.add_bootiso(NS, name, inserted=False)  # an earlier one nobody ejected
        async with DeleteHarness(fleet) as h:
            result = await h.delete(delete_input(name))
        assert result.claims_deleted == sorted([f"{name}-root", iso, stray])
        assert result.import_sources_deleted == [f"{iso}-populator"]
        assert len(result.volumes_released) == 3 and nothing_left(cluster) and cluster.import_sources == set()

    async def test_a_running_vm_is_deleted_too_and_the_result_says_so(self):
        fleet = fleet_of(A)
        cluster = fleet.clusters[A]
        cluster.vm_shutdown_polls = 5  # the guest takes five polls to shut down
        name = await put_vm(cluster)
        cluster.start(NS, name, f"{A}-n1")
        async with DeleteHarness(fleet) as h:
            handle = await h.begin(delete_input(name))
            result = await h.result(handle)
            assert (await h.progress(handle)).was_running
        assert result.was_running and result.vm_deleted and result.polls >= 6
        assert nothing_left(cluster) and cluster.pods == []

    async def test_only_the_vm_asked_for_goes(self):
        fleet = fleet_of(A)
        cluster = fleet.clusters[A]
        name, neighbour = await put_vm(cluster), await put_vm(cluster, "neighbr1", run="another-run")
        neighbours_iso = cluster.add_bootiso(NS, neighbour)
        async with DeleteHarness(fleet) as h:
            await h.delete(delete_input(name))
        assert list(cluster.vms) == [(NS, neighbour)]
        assert set(cluster.pvcs) == {(NS, f"{neighbour}-root"), (NS, neighbours_iso)} and len(cluster.pvs) == 2

    async def test_it_goes_to_the_named_clusters_own_worker(self):
        fleet = fleet_of(A, B)
        on_a, on_b = await put_vm(fleet.clusters[A], "samename"), await put_vm(fleet.clusters[B], "samename")
        assert on_a == on_b  # a name is unique on its cluster only
        async with DeleteHarness(fleet) as h:
            result = await h.delete(delete_input(on_b, kv_cluster=B))
        assert result.kv_cluster == B and nothing_left(fleet.clusters[B])
        assert (NS, on_a) in fleet.clusters[A].vms and fleet.clusters[A].calls == []

    async def test_a_second_start_while_one_runs_is_refused(self):
        from temporalio.exceptions import WorkflowAlreadyStartedError

        fleet = fleet_of(A)
        fleet.poll_seconds = 0.05
        cluster = fleet.clusters[A]
        name = await put_vm(cluster)
        cluster.add_claim_pod(NS, "holder", f"{name}-root")
        async with DeleteHarness(fleet) as h:
            first = await h.begin(delete_input(name))
            with pytest.raises(WorkflowAlreadyStartedError):
                await h.begin(delete_input(name))
            cluster.remove_pod("holder")
            assert (await h.result(first)).found


# --- delete, then fail -----------------------------------------------------------------------------


class TestDeleteThenFail:
    async def test_a_retain_volume_fails_the_run_after_everything_else_went(self):
        fleet = fleet_of(A)
        cluster = fleet.clusters[A]
        cluster.reclaim_policy = "Retain"
        name = await put_vm(cluster)
        [volume] = cluster.pvs.values()
        async with DeleteHarness(fleet) as h:
            handle = await h.begin(delete_input(name))
            failed = cause(await h.failure(handle))
            progress = await h.progress(handle)
            assert failed.type == VmVolumesLeftInStorageError.__name__
            assert cluster.vms == {} and cluster.datavolumes == {} and cluster.pvcs == {}  # deleted first
            assert volume["status"]["phase"] == "Released" and list(cluster.pvs) == [volume["metadata"]["name"]]
            for said in (volume["metadata"]["name"], volume["spec"]["csi"]["volumeHandle"], "fake.csi.redbull.io",
                         "reclaim policy Retain", "storage array", "deletes no PersistentVolume"):
                assert said in failed.message, said
            assert progress.phase == "failed" and "Retain" in progress.remaining
            # A re-run fails the same way, deleting nothing, until a person has removed the volume…
            again = cause(await h.fails(delete_input(name)))
            assert again.type == VmVolumesLeftInStorageError.__name__ and volume["metadata"]["name"] in again.message
            cluster.pvs.clear()
            # …and then completes: nothing of the VM is left.
            assert (await h.delete(delete_input(name))).found is False

    async def test_a_snapshot_fails_the_run_after_everything_went(self):
        fleet = fleet_of(A)
        cluster = fleet.clusters[A]
        name = await put_vm(cluster)
        cluster.add_snapshot(NS, "nightly-backup", f"{name}-root")
        async with DeleteHarness(fleet) as h:
            failed = cause(await h.fails(delete_input(name)))
            assert failed.type == VmVolumesLeftInStorageError.__name__
            assert nothing_left(cluster) and list(cluster.snapshots) == [(NS, "nightly-backup")]  # never deleted
            assert "nightly-backup" in failed.message and f"of claim {name}-root" in failed.message
            assert "snapcontent-nightly-backup" in failed.message and "gone" in failed.message
            # The snapshot's source is recognised by its NAME once the claim is gone: the same failure.
            again = cause(await h.fails(delete_input(name)))
            assert again.type == VmVolumesLeftInStorageError.__name__ and "nightly-backup" in again.message
            cluster.snapshots.clear()
            assert (await h.delete(delete_input(name))).found is False

    async def test_a_volume_the_driver_never_deletes_fails_at_the_deadline_and_a_re_run_resumes(self):
        fleet = fleet_of(A)
        cluster = fleet.clusters[A]
        name = await put_vm(cluster)
        [volume_name] = cluster.pvs
        cluster.stuck_volumes.add(volume_name)
        async with DeleteHarness(fleet) as h:
            failed = cause(await h.fails(delete_input(name)))
            assert failed.type == VmVolumesLeftInStorageError.__name__
            assert volume_name in failed.message and "CSI driver has not deleted" in failed.message
            assert "Retain" not in failed.message.split("reclaim policy Delete")[0]
            assert cluster.vms == {} and cluster.pvcs == {} and volume_name in cluster.pvs
            # The re-run: the VM and its claim are gone; the volume is found by its claimRef.
            cluster.stuck_volumes.clear()
            result = await h.delete(delete_input(name))
        assert result.found and not result.vm_deleted and result.claims_deleted == []
        assert [volume.name for volume in result.volumes_released] == [volume_name] and nothing_left(cluster)

    async def test_a_claim_a_pod_holds_fails_at_the_deadline_naming_the_pod(self):
        fleet = fleet_of(A)
        cluster = fleet.clusters[A]
        name = await put_vm(cluster)
        cluster.add_claim_pod(NS, "backup-job-7", f"{name}-root")
        async with DeleteHarness(fleet) as h:
            failed = cause(await h.fails(delete_input(name)))
            assert failed.type == VmDeletionTimeoutError.__name__
            for said in (f"claim {name}-root", "Terminating", "backup-job-7", "kubernetes.io/pvc-protection", "15 min"):
                assert said in failed.message, said
            assert cluster.vms == {} and len(cluster.pvcs) == 1  # the VM went; the claim is held
            cluster.remove_pod("backup-job-7")
            result = await h.delete(delete_input(name))  # the re-run finds what is left and finishes
        assert result.found and not result.vm_deleted and len(result.volumes_released) == 1 and nothing_left(cluster)

    async def test_a_look_alike_claim_is_left_and_fails_the_run(self):
        fleet = fleet_of(A)
        cluster = fleet.clusters[A]
        name = await put_vm(cluster, flavor="c" + "2" * 30)
        cut = (f"{name}-bootiso-1795000000-012345")[:49] + "ab12ctruncated"
        cluster.add_bootiso(NS, name, inserted=False, name=cut)
        async with DeleteHarness(fleet) as h:
            failed = cause(await h.fails(delete_input(name)))
        assert failed.type == VmVolumesLeftInStorageError.__name__
        assert cut in failed.message and "cut short" in failed.message
        assert cluster.vms == {} and list(cluster.pvcs) == [(NS, cut)] and len(cluster.pvs) == 1


# --- identity, classification, outages ---------------------------------------------------------------


class TestWhatStopsItAndWhatDoesNot:
    @pytest.mark.parametrize(("labels", "fields"), [
        ({"redbull.io/created-by": None}, {}),
        ({"redbull.io/mce": "ocp4-prod-mce-five-b"}, {}),
        ({}, {"created_by": "another-run"}),
        ({}, {"mac": "02:ff:ff:ff:ff:ff"}),
    ], ids=["not-made-by-create-vm", "another-mces", "another-runs", "another-mac"])
    async def test_a_vm_that_is_not_the_one_asked_for_fails_the_run_with_nothing_deleted(self, labels, fields):
        fleet = fleet_of(A)
        cluster = fleet.clusters[A]
        name = await put_vm(cluster)
        for key, value in labels.items():
            vm_labels = cluster.vms[(NS, name)]["metadata"]["labels"]
            vm_labels.pop(key) if value is None else vm_labels.__setitem__(key, value)
        async with DeleteHarness(fleet) as h:
            failed = cause(await h.fails(delete_input(name, **fields)))
        assert failed.type == VmIdentityConflictError.__name__ and "Nothing was deleted" in failed.message
        assert cluster.calls == ["get_vm"]  # one attempt, one read
        assert (NS, name) in cluster.vms and len(cluster.pvcs) == 1 and len(cluster.pvs) == 1

    async def test_a_hand_made_vm_is_never_deleted(self):
        fleet = fleet_of(A)
        cluster = fleet.clusters[A]
        cluster.add_vm(stopped_vm("someones-vm", 8, 60))
        async with DeleteHarness(fleet) as h:
            failed = cause(await h.fails(delete_input("someones-vm")))
        assert failed.type == VmIdentityConflictError.__name__ and (NS, "someones-vm") in cluster.vms

    @pytest.mark.parametrize("error", [
        KvClusterAccessError("this cluster's API refused its vm-lifecycle worker's ServiceAccount (403): the test's"),
        KvClusterMismatchError("this worker is deployed for another KV cluster: the test's"),
        VmRequestRejectedError("this cluster's API refused the request (422): the test's"),
    ], ids=lambda error: type(error).__name__)
    @pytest.mark.parametrize("method", ["get_vm", "delete_vm", "get_pv"], ids=["inspecting", "deleting", "waiting"])
    async def test_a_permanent_error_fails_the_run_after_one_attempt(self, error, method):
        fleet = fleet_of(A)
        cluster = fleet.clusters[A]
        name = await put_vm(cluster)
        if method == "get_pv":  # the wait's own read: the inspection and the delete do not make it
            cluster.fail(method, error)
        else:
            cluster.fail(method, error, AssertionError("a permanent error was retried"))
        async with DeleteHarness(fleet) as h:
            failed = cause(await h.fails(delete_input(name)))
        assert failed.type == type(error).__name__
        assert cluster.calls.count(method) == 1

    async def test_an_outage_is_out_waited_at_every_step(self):
        fleet = fleet_of(A)
        cluster = fleet.clusters[A]
        name = await put_vm(cluster)
        outage = [KvClusterApiError("this cluster's API failed (503): the test's outage") for _ in range(3)]
        cluster.fail("list_pvs", *outage)   # the inspection's
        cluster.fail("delete_vm", *outage[:2])  # the delete's
        cluster.fail("get_pv", *outage[:2])     # the wait's
        async with DeleteHarness(fleet) as h:
            result = await h.delete(delete_input(name), step=timedelta(seconds=10))
        assert result.found and result.vm_deleted and nothing_left(cluster)
        assert cluster.calls.count("delete_vm") == 3 and cluster.calls.count("list_pvs") >= 4

    async def test_a_cluster_with_no_worker_fails_the_run_before_anything_is_written(self):
        """The inspection's schedule-to-start limit: five minutes, never retried by Temporal."""
        fleet = fleet_of(A)
        cluster = fleet.clusters[A]
        name = await put_vm(cluster)
        async with DeleteHarness(fleet, down={A}) as h:
            handle = await h.begin(delete_input(name))
            await h.until_phase(handle, "inspecting")
            await asyncio.sleep(0.2)
            await h.advance(delete_vm._INSPECT_SCHEDULE_TO_START_TIMEOUT - timedelta(seconds=20))
            assert (await handle.describe()).status == WorkflowExecutionStatus.RUNNING
            failed = cause(await h.failure(handle, step=timedelta(seconds=10)))
            assert (await h.progress(handle)).phase == "failed"
        assert failed.type == KvClusterWorkerAbsentError.__name__
        for said in (f"vm-lifecycle-activity-{A}", "within 5 min", "nothing was deleted"):
            assert said in failed.message, said
        assert cluster.calls == [] and (NS, name) in cluster.vms

    async def test_once_the_delete_was_sent_a_worker_that_is_away_is_waited_for(self):
        """No schedule-to-start limit after the first write: the delete's next attempt waits for
        the cluster's worker for as long as it is away, and then finishes the job."""
        fleet = fleet_of(A)
        cluster = fleet.clusters[A]
        name = await put_vm(cluster)
        cluster.delays["delete_vm"] = 30.0  # the delete is in flight when the worker goes
        async with DeleteHarness(fleet) as h:
            handle = await h.begin(delete_input(name))
            await h.until_phase(handle, "deleting")
            while "delete_vm" not in cluster.calls:
                await asyncio.sleep(0.02)
            await fleet.stop_worker(A)  # cut off mid-delete, as a pod at the end of its grace period
            cluster.delays.clear()
            await h.advance(timedelta(minutes=20))  # far past any pick-up bound the inspection has
            assert (await handle.describe()).status == WorkflowExecutionStatus.RUNNING
            assert (await h.progress(handle)).phase == "deleting"
            await fleet.start_worker(A)
            result = await h.result(handle, step=timedelta(seconds=30))
        assert result.found and result.vm_deleted and nothing_left(cluster)

    async def test_a_wait_resumed_after_the_claims_went_still_waits_for_the_volume(self):
        """The worker restarts mid-wait, after the claim is gone: the next attempt cannot find
        the volume through a claim any more, and must not report the VM gone over it."""
        fleet = fleet_of(A)
        fleet.poll_seconds = 0.05
        cluster = fleet.clusters[A]
        name = await put_vm(cluster)
        [volume_name] = cluster.pvs
        cluster.stuck_volumes.add(volume_name)
        key = (A, NS, f"{name} gone")
        async with DeleteHarness(fleet) as h:
            handle = await h.begin(delete_input(name))
            await h.until_phase(handle, "awaiting-release")
            while cluster.pvcs or cluster.calls.count("get_pv") < 3:
                await asyncio.sleep(0.02)
            await fleet.restart_worker(A)
            polls_before = cluster.calls.count("get_pv")
            for _ in range(200):  # the second attempt is polling, and the run is still waiting
                if fleet.gone_attempts.get(key, 0) >= 2 and cluster.calls.count("get_pv") >= polls_before + 3:
                    break
                await h.advance(timedelta(seconds=2))
                await asyncio.sleep(0.05)
            assert fleet.gone_attempts[key] >= 2
            assert (await handle.describe()).status == WorkflowExecutionStatus.RUNNING
            assert cluster.pvcs == {} and volume_name in cluster.pvs
            cluster.stuck_volumes.clear()
            result = await h.result(handle, step=timedelta(seconds=2))
        assert [volume.name for volume in result.volumes_released] == [volume_name] and nothing_left(cluster)

    async def test_a_cancel_during_the_wait_ends_the_run_cancelled_and_undoes_nothing(self):
        fleet = fleet_of(A)
        fleet.poll_seconds = 0.05
        cluster = fleet.clusters[A]
        name = await put_vm(cluster)
        cluster.add_claim_pod(NS, "holder", f"{name}-root")
        async with DeleteHarness(fleet) as h:
            handle = await h.begin(delete_input(name))
            await h.until_phase(handle, "awaiting-release")
            await handle.cancel()
            failed = await h.failure(handle)
            assert isinstance(failed.cause, CancelledError)
            assert cluster.vms == {}  # a delete has no rollback
            cluster.remove_pod("holder")
            assert (await h.delete(delete_input(name))).found  # the re-run finishes it


def test_the_production_waits_are_the_plans():
    assert delete_vm._INSPECT_SCHEDULE_TO_START_TIMEOUT == timedelta(minutes=5)
    assert delete_vm._GONE_DEADLINE_MINUTES == 15 and delete_vm._GONE_HEARTBEAT_TIMEOUT == timedelta(minutes=2)
    assert delete_vm._GONE_TIMEOUT > timedelta(minutes=delete_vm._GONE_DEADLINE_MINUTES)


def test_it_has_no_route():
    """Child only (the operator, 2026-11-26): remove-server-from-mce is the one public way to remove a VM."""
    from workflow_domains.vm_lifecycle.router import router

    assert not any("delete" in route.path for route in router.routes)


# --- the scenarios the replay test records -----------------------------------------------------------


async def _clean(cluster: FakeKvCluster) -> str:
    cluster.volume_release_polls = 3
    return await put_vm(cluster)


async def _with_a_boot_iso(cluster: FakeKvCluster) -> str:
    name = await put_vm(cluster)
    cluster.add_bootiso(NS, name, cdi=True)
    return name


async def _nothing(cluster: FakeKvCluster) -> str:
    return "ocp-c200-kv32-five-gone0000"


async def _retain(cluster: FakeKvCluster) -> str:
    cluster.reclaim_policy = "Retain"
    return await put_vm(cluster)


async def _snapshot(cluster: FakeKvCluster) -> str:
    name = await put_vm(cluster)
    cluster.add_snapshot(NS, "nightly-backup", f"{name}-root")
    return name


async def _held(cluster: FakeKvCluster) -> str:
    name = await put_vm(cluster)
    cluster.add_claim_pod(NS, "backup-job-7", f"{name}-root")
    return name


async def _not_ours(cluster: FakeKvCluster) -> str:
    cluster.add_vm(stopped_vm("someones-vm", 8, 60))
    return "someones-vm"


async def _outage(cluster: FakeKvCluster) -> str:
    name = await put_vm(cluster)
    cluster.down = True  # the cluster's API is down for the run's first ten minutes
    return name


# name -> (what is on the cluster, the type the run fails with or None)
SCENARIOS = {
    "clean-delete": (_clean, None),
    "boot-iso-left": (_with_a_boot_iso, None),
    "nothing-there": (_nothing, None),
    "retain-volume-left": (_retain, VmVolumesLeftInStorageError.__name__),
    "snapshot-left": (_snapshot, VmVolumesLeftInStorageError.__name__),
    "claim-held-at-the-deadline": (_held, VmDeletionTimeoutError.__name__),
    "not-create-vms-vm": (_not_ours, VmIdentityConflictError.__name__),
    "api-outage-out-waited": (_outage, None),
    "no-worker-on-the-cluster": (put_vm, KvClusterWorkerAbsentError.__name__),
}


async def record(scenario: str):
    """Run one scenario on the production constants and return its history."""
    prepare, ends = SCENARIOS[scenario]
    fleet: KvFleet = fleet_of(A)
    cluster = fleet.clusters[A]
    name = await prepare(cluster)
    async with DeleteHarness(fleet, down={A} if scenario == "no-worker-on-the-cluster" else None) as h:
        handle = await h.begin(delete_input(name, created_by=RUN if scenario == "clean-delete" else None))
        if scenario == "no-worker-on-the-cluster":
            await h.until_phase(handle, "inspecting")
            await asyncio.sleep(0.2)
            await h.advance(delete_vm._INSPECT_SCHEDULE_TO_START_TIMEOUT)
        if scenario == "api-outage-out-waited":
            await h.advance(timedelta(minutes=10))
            cluster.down = False
        if ends is None:
            await h.result(handle, step=timedelta(seconds=30))
        else:
            assert cause(await h.failure(handle, step=timedelta(seconds=30))).type == ends
        return await handle.fetch_history()
