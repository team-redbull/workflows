"""create-vm: the REAL CreateVmWorkflow and KvZoneLockWorkflow over a fleet of fake KV clusters.

Every cluster of the fleet has its own worker on its own queue running the REAL limb code
(tests/kv_fleet.py); only the cluster behind the API is fake. The clock moves when the
test says so (tests/manual_time.py). What is pinned is what the design promises
(docs/design/create-vm-placement.md): the oldest cluster that fits gets the VM, later
ones are not read; parallel runs never overbook a cluster; a cluster that cannot answer
is a row, never a failure; the lock is given back on every path; each permanent error
fails the run with its type after one attempt and a transient one is retried; a run
that does not complete leaves no VM behind.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

import pytest
from temporalio import activity
from temporalio.client import WorkflowExecutionStatus, WorkflowFailureError, WorkflowHandle
from temporalio.exceptions import ActivityError, ApplicationError, CancelledError
from temporalio.worker import UnsandboxedWorkflowRunner, Worker

from activities.vm_lifecycle import vm_spec
from activities.vm_lifecycle.kubevirt_api import MacRefused, NameTaken
from shared.consts import CREATE_VM_WORKFLOW_QUEUE, vm_lifecycle_activity_queue
from shared.exceptions import (
    KvClusterAccessError,
    KvClusterApiError,
    KvZoneLeaseExpiredError,
    KvZoneLockTimeoutError,
    NoKvClusterError,
    NoKvClusterFitsError,
    ValuesRepoGitError,
    VmDisappearedError,
    VmLeaseExpiredError,
    VmMacExhaustedError,
    VmNamespaceNotFoundError,
    VmRequestRejectedError,
    VmRootDiskNotBoundError,
)
from shared.models.vm_lifecycle import (
    CreateVmInput,
    CreateVmProgress,
    CreateVmResult,
    CreateVmRunArgs,
    KvClusterFigures,
    KvFiguresRequest,
    KvZoneLockProgress,
    KvZoneLockRequest,
    VmCreated,
    VmCreateRequest,
)
from shared.workflow_ids import create_vm_slot_workflow_id, kv_zone_lock_workflow_id
from tests.kv_fleet import GIB, FakeKvCluster, KvFleet, node, stopped_vm
from tests.manual_time import ManualTime
from workflow_domains.vm_lifecycle import create_vm, kv_zone_lock
from workflow_domains.vm_lifecycle.create_vm import CreateVmWorkflow
from workflow_domains.vm_lifecycle.kv_zone_lock import KvZoneLockWorkflow

SITE = "five"
MCE = "ocp4-prod-mce-five-a"
A, B, C = "ocp4-prod-kv-five-a", "ocp4-prod-kv-five-b", "ocp4-prod-kv-five-c"


def vm_input(infra_env: str = "c200-kv32-five-az1", *, mce: str = MCE, namespace: str = "inventory") -> CreateVmInput:
    flavor, size, *site, zone = infra_env.split("-")
    cpu = int(size.removeprefix("kv"))
    return CreateVmInput(mce_cluster=mce, infra_env=infra_env, namespace=namespace, flavor=flavor, site="-".join(site),
                         zone=zone, cpu=cpu, memory_gib=cpu * 8)


def big(name: str) -> list[dict]:
    """One node that holds anything: the tests about the BUFFER size the cluster, not the node."""
    return [node(f"{name}-n1", 100, 10_000)]


class Harness(ManualTime):
    def __init__(self, fleet: KvFleet, *, down: set[str] | None = None, lock_worker: bool = True,
                 quick: bool = False) -> None:
        self.fleet = fleet
        self._down = down
        self._lock_worker = lock_worker
        # With the `quick` fixture's shrunk timeouts the workflows must see the patched module
        # constants, which the sandbox (re-importing the workflow modules) would not.
        self._runner = UnsandboxedWorkflowRunner() if quick else None
        self._slot = 0

    async def __aenter__(self) -> Harness:
        await super().__aenter__()
        self._running = self.fleet.running(
            self.client, down=self._down, lock_worker=self._lock_worker, workflow_runner=self._runner
        )
        await self._running.__aenter__()
        return self

    async def __aexit__(self, *exc) -> None:
        await self._running.__aexit__(*exc)
        await super().__aexit__(*exc)

    async def start(self, request: CreateVmInput | None = None, *, workflow_id: str | None = None) -> WorkflowHandle:
        request = request or vm_input()
        self._slot += 1
        return await self.client.start_workflow(
            CreateVmWorkflow.run, CreateVmRunArgs(input=request),
            id=workflow_id or create_vm_slot_workflow_id(request.mce_cluster, request.infra_env, self._slot) + f"-{uuid.uuid4().hex[:6]}",
            task_queue=CREATE_VM_WORKFLOW_QUEUE,
        )

    async def create(self, request: CreateVmInput | None = None, **kwargs) -> CreateVmResult:
        return await self.result(await self.start(request), **kwargs)

    async def progress(self, handle: WorkflowHandle) -> CreateVmProgress:
        return await handle.query(CreateVmWorkflow.progress)

    async def lock(self, site: str = SITE, zone: str = "az1") -> KvZoneLockProgress:
        return await self.client.get_workflow_handle(kv_zone_lock_workflow_id(site, zone)).query(KvZoneLockWorkflow.progress)

    async def until_phase(self, handle: WorkflowHandle, *phases: str, tries: int = 400) -> CreateVmProgress:
        seen = None
        for _ in range(tries):
            try:
                seen = await self.progress(handle)
            except Exception:  # noqa: BLE001 — not started yet
                seen = None
            if seen is not None and seen.phase in phases:
                return seen
            await asyncio.sleep(0.02)
        raise AssertionError(f"the run never reached {phases}; last phase: {seen.phase if seen else None}")

    async def until_lock(self, condition, *, site: str = SITE, zone: str = "az1", tries: int = 400) -> KvZoneLockProgress:
        seen = None
        for _ in range(tries):
            try:
                seen = await self.lock(site, zone)
            except Exception:  # noqa: BLE001 — the lock is not started yet
                seen = None
            if seen is not None and condition(seen):
                return seen
            await asyncio.sleep(0.02)
        raise AssertionError(f"the lock never reached the state waited for; last: {seen}")

    async def lock_is_free(self, **kwargs) -> KvZoneLockProgress:
        return await self.until_lock(lambda p: p.holder is None and p.waiting == 0, **kwargs)


REAL_TIME = timedelta(0)  # for results(): no nudging — the shrunk timeouts pass on the wall clock


@pytest.fixture
def quick(monkeypatch):
    """The workflows' waits, shrunk to pass in REAL time (use with `Harness(..., quick=True)`).

    For the one test where MANY runs wait on dead workers at once: the clock
    is jumped past the unserved tasks it finds (tests/manual_time.py), one
    more lock at a time, which is no way to drive two dozen runs. Every other
    test about dead workers and leases runs on the production waits. The lease here is
    generous for a slow machine: no run of that test should come near it.
    """
    monkeypatch.setattr(create_vm, "_FIGURES_SCHEDULE_TO_START_TIMEOUT", timedelta(seconds=0.5))
    monkeypatch.setattr(create_vm, "_FIGURES_SCHEDULE_TO_CLOSE_TIMEOUT", timedelta(seconds=5))
    monkeypatch.setattr(kv_zone_lock, "_LEASE", timedelta(seconds=30))
    monkeypatch.setattr(create_vm, "_LEASE_GUARD", timedelta(seconds=1))
    monkeypatch.setattr(create_vm, "_CREATE_MARGIN", timedelta(seconds=0.5))


def test_the_production_waits_are_the_designs():
    """What `quick` shrinks, pinned at its real value (docs/design/create-vm-placement.md §4, §6.3.2)."""
    assert create_vm._FIGURES_SCHEDULE_TO_START_TIMEOUT == timedelta(seconds=30)
    assert create_vm._FIGURES_TIMEOUT == timedelta(seconds=60)
    assert create_vm._FIGURES_SCHEDULE_TO_CLOSE_TIMEOUT == timedelta(seconds=120)
    assert kv_zone_lock._LEASE == timedelta(minutes=5)
    assert create_vm._LEASE_GUARD == timedelta(seconds=45) and create_vm._CREATE_MARGIN == timedelta(seconds=10)
    assert create_vm._LOCK_WAIT == timedelta(minutes=30) and create_vm._LOCK_REASK == timedelta(minutes=2)
    assert create_vm._MAX_LEASES == 3 and create_vm._READY_DEADLINE_MINUTES == 10
    assert create_vm._CANCEL_SETTLE == create_vm._CREATE_TIMEOUT + timedelta(seconds=30)
    assert create_vm._ROLLBACK_DELETE_DEADLINE == timedelta(minutes=10)
    # A lease outlasts the worst single figures read, and the guard outlasts one API call (20 s).
    assert kv_zone_lock._LEASE > create_vm._FIGURES_SCHEDULE_TO_CLOSE_TIMEOUT
    assert create_vm._LEASE_GUARD > timedelta(seconds=25)


def cause(error: WorkflowFailureError) -> ApplicationError:
    """The ApplicationError a failed run ended with: workflow-raised, or behind the activity that raised it."""
    failure = error.cause
    if isinstance(failure, ActivityError):
        failure = failure.cause
    assert isinstance(failure, ApplicationError), f"not an ApplicationError: {failure!r}"
    return failure


def fleet_of(*names: str, zone: str = "az1", buffer_percent: int = 15, **cluster_kwargs) -> KvFleet:
    fleet = KvFleet(buffer_percent=buffer_percent)
    for name in names:
        fleet.add(name, zone, nodes=big(name), **cluster_kwargs)
    return fleet


# --- the happy path ---------------------------------------------------------------------------


class TestOneVm:
    async def test_the_vm_is_created_halted_on_the_zones_cluster_and_the_zone_is_free_again(self):
        fleet = fleet_of(A)
        async with Harness(fleet) as h:
            handle = await h.start()
            result = await h.result(handle)
            cluster = fleet.clusters[A]
            assert result.kv_cluster == A and cluster.vm_names == [result.vm_name]
            assert result.vm_name == "ocp-c200-kv32-five-" + result.mac.replace(":", "")
            assert result.mac.startswith(cluster.values.mac_prefix + ":")
            assert (result.flavor, result.cpu, result.memory_gib, result.namespace) == ("c200", 32, 256, "inventory")
            assert (result.site, result.zone, result.mce_cluster, result.infra_env) == (SITE, "az1", MCE, "c200-kv32-five-az1")
            assert (result.buffer_percent, result.day1_revision, result.leases, result.draws, result.created) == (15, "0123abc", 1, 1, True)
            assert result.root_disk_phase == "Succeeded" and result.not_read == []
            assert [(row.name, row.fits, row.skip_reason) for row in result.table] == [(A, True, None)]
            assert result.table[0].figures.allocatable_cpu_millicores == 100_000
            assert result.table[0].cpu_fill_after == pytest.approx(0.30)
            # The VM is what the request said, labelled with THIS RUN's id, and Halted.
            vm = cluster.vms[("inventory", result.vm_name)]
            description = await handle.describe()
            assert vm_spec.created_by(vm) == description.run_id
            assert vm["metadata"]["annotations"]["redbull.io/workflow-id"] == handle.id
            assert vm["metadata"]["labels"]["redbull.io/mce"] == MCE and vm["spec"]["runStrategy"] == "Halted"
            # day1 was asked once, for this zone; the lock granted once and is free.
            assert [(asked.site, asked.zone) for asked in fleet.listed] == [(SITE, "az1")]
            lock = await h.lock_is_free()
            assert (lock.grants, lock.expired, lock.skipped) == (1, 0, 0)
            progress = await h.progress(handle)
            assert progress.phase == "completed" and progress.vm_name == result.vm_name and progress.kv_cluster == A
            assert progress.zone_lock == kv_zone_lock_workflow_id(SITE, "az1") and progress.lease_ends_at is None

    async def test_the_request_carries_what_the_vm_is_and_no_mac(self):
        fleet = fleet_of(A)
        seen = []
        cluster = fleet.clusters[A]
        original = cluster.create_vm

        async def spy(namespace, body):
            seen.append((namespace, body))
            return await original(namespace, body)

        cluster.create_vm = spy
        async with Harness(fleet) as h:
            result = await h.create(vm_input("c200n-kv8-five-az1"))
        namespace, body = seen[0]
        assert namespace == "inventory"
        assert body["metadata"]["labels"] | {"redbull.io/created-by": "x"} == {
            "redfish-enabled": "true", "redbull.io/flavor": "c200n", "redbull.io/size": "kv8", "redbull.io/site": "five",
            "topology.kubernetes.io/zone": "az1", "redbull.io/role": "worker", "redbull.io/mce": MCE,
            "redbull.io/created-by": "x",
        }
        domain = body["spec"]["template"]["spec"]["domain"]
        assert domain["cpu"] == {"cores": 8} and domain["memory"] == {"guest": "64Gi"}
        assert result.vm_name.startswith("ocp-c200n-kv8-five-0e")

    async def test_the_oldest_cluster_that_fits_is_chosen_and_the_later_ones_are_not_read(self):
        fleet = fleet_of(C, A, B)  # added out of order: day1's order is by the id letter, not by this
        async with Harness(fleet) as h:
            result = await h.create()
        assert result.kv_cluster == A and result.not_read == [B, C]
        assert [row.name for row in result.table] == [A]
        assert fleet.clusters[B].calls == [] and fleet.clusters[C].calls == []

    async def test_a_full_cluster_is_passed_over_for_the_next(self):
        fleet = fleet_of(A, B, C)
        fleet.clusters[A].add_pod(f"{A}-n1", 60_000, 0)  # 60 + 30 > 85
        async with Harness(fleet) as h:
            result = await h.create()
        assert result.kv_cluster == B and result.not_read == [C]
        assert [(row.name, row.skip_reason) for row in result.table] == [(A, "fit-buffer"), (B, None)]
        assert "90000 m of 100000 m" in result.table[0].skip_detail
        assert fleet.clusters[A].vms == {} and fleet.clusters[C].calls == []

    async def test_the_buffer_is_the_one_day1s_reader_handed_over(self):
        fleet = fleet_of(A, B, buffer_percent=50)
        fleet.clusters[A].add_pod(f"{A}-n1", 30_000, 0)  # 30 + 30 = 60 > 50
        async with Harness(fleet) as h:
            result = await h.create()
        assert (result.kv_cluster, result.buffer_percent) == (B, 50)
        assert "50% buffer allows 50%" in result.table[0].skip_detail

    async def test_vms_of_two_sizes_share_the_zone_and_its_room(self):
        fleet = fleet_of(A)
        async with Harness(fleet) as h:
            big_vm = await h.create(vm_input("c200-kv64-five-az1"))   # 60 cores
            small = await h.create(vm_input("c200-kv16-five-az1"))    # + 15 = 75 <= 85
            failed = await h.failure(await h.start(vm_input("c200-kv16-five-az1")))  # + 15 = 90 > 85
        assert big_vm.kv_cluster == small.kv_cluster == A
        assert cause(failed).type == NoKvClusterFitsError.__name__
        assert len(fleet.clusters[A].vms) == 2 and (await fleet.clusters[A].requested_cpu_millicores()) == 75_000


# --- runs at once -------------------------------------------------------------------------------


class TestParallelRuns:
    async def test_ten_runs_at_once_fill_a_then_b_then_find_no_capacity(self):
        """The design's example (§6.5): two 100-core clusters, the 15% buffer, ten 32-vCPU VMs."""
        fleet = fleet_of(A, B)
        async with Harness(fleet) as h:
            handles = [await h.start() for _ in range(10)]
            outcomes = await h.results(*handles, return_exceptions=True, limit=120)
            placed = [o.kv_cluster for o in outcomes if isinstance(o, CreateVmResult)]
            failed = [o for o in outcomes if isinstance(o, WorkflowFailureError)]
            assert sorted(placed) == [A, A, B, B]
            assert len(failed) == 6 and {cause(f).type for f in failed} == {NoKvClusterFitsError.__name__}
            for name in (A, B):
                assert len(fleet.clusters[name].vms) == 2
                assert await fleet.clusters[name].requested_cpu_millicores() == 60_000  # never past 85
            lock = await h.lock_is_free()
            assert (lock.grants, lock.expired) == (10, 0)
            # The runs that found nothing say why, for both clusters.
            said = cause(failed[0]).message
            assert f"{A}, {B} would go past the placement buffer" in said and "Nothing was created" in said

    async def test_the_order_of_arrival_is_the_order_of_filling(self):
        fleet = fleet_of(A, B)
        async with Harness(fleet) as h:
            handles = []
            for _ in range(4):
                handles.append(await h.start())
                await asyncio.sleep(0.05)
            outcomes = await h.results(*handles)
        assert [o.kv_cluster for o in outcomes] == [A, A, B, B]

    async def test_a_run_waits_for_the_zone_while_another_holds_it(self):
        fleet = fleet_of(A)
        fleet.clusters[A].delays["create_vm"] = 1.0  # the first run holds the lock for a real second
        async with Harness(fleet) as h:
            first = await h.start()
            await h.until_phase(first, "creating-vm")
            second = await h.start()
            waiting = await h.until_phase(second, "awaiting-zone-lock")
            assert waiting.table == [] and waiting.kv_cluster is None
            lock = await h.until_lock(lambda p: p.waiting == 1)
            assert lock.holder.workflow_id == first.id
            assert fleet.clusters[A].calls.count("list_nodes") == 1  # the waiting run has read nothing
            one, two = await h.results(first, second)
        assert one.vm_name != two.vm_name and len(fleet.clusters[A].vms) == 2
        # The second run's figures already counted the first run's VM, as a stopped VM.
        assert two.table[0].figures.stopped_vms == 1 and one.table[0].figures.stopped_vms == 0

    async def test_the_lock_is_released_before_the_disk_wait_so_the_zone_does_not_wait_ten_minutes(self):
        fleet = fleet_of(A)
        fleet.clusters[A].storage = "wffc"  # the first run's disk never binds: it sits in its ten-minute wait
        fleet.poll_seconds = 0.05
        async with Harness(fleet) as h:
            slow = await h.start()
            await h.until_phase(slow, "awaiting-root-disk")
            assert (await h.lock_is_free()).grants == 1
            fleet.clusters[A].storage = "immediate"
            quick = await h.create()  # served at once, while the first is still waiting for its disk
            assert quick.kv_cluster == A
            assert (await h.progress(slow)).phase == "awaiting-root-disk"
            failed = await h.failure(slow, limit=120)
            assert cause(failed).type == VmRootDiskNotBoundError.__name__
            assert fleet.clusters[A].vm_names == [quick.vm_name]  # the slow run's VM was removed, the quick one's stays


# --- clusters that cannot answer ------------------------------------------------------------------


class TestClustersThatCannotAnswer:
    async def test_a_cluster_with_no_worker_is_skipped_after_thirty_seconds(self):
        fleet = fleet_of(A, B)
        async with Harness(fleet, down={A}) as h:
            handle = await h.start()
            await h.until_phase(handle, "choosing-kv-cluster")
            await h.advance(timedelta(seconds=25))
            assert (await h.progress(handle)).table == []  # still waiting for a worker on A's queue
            await h.advance(timedelta(seconds=6))
            result = await h.result(handle)
        assert result.kv_cluster == B
        assert [(row.name, row.skip_reason) for row in result.table] == [(A, "no-worker"), (B, None)]
        assert vm_lifecycle_activity_queue(A) in result.table[0].skip_detail and "within 30 s" in result.table[0].skip_detail
        assert result.table[0].figures is None and fleet.clusters[A].calls == [] and fleet.clusters[A].vms == {}

    async def test_a_worker_that_comes_back_inside_the_thirty_seconds_is_not_skipped(self):
        fleet = fleet_of(A, B)
        async with Harness(fleet, down={A}) as h:
            handle = await h.start()
            await h.until_phase(handle, "choosing-kv-cluster")
            await h.advance(timedelta(seconds=20))
            await fleet.start_worker(A)  # a restart, inside the pick-up bound
            result = await h.result(handle)
        assert result.kv_cluster == A and fleet.clusters[B].calls == []

    async def test_a_worker_that_cannot_read_its_cluster_is_unreachable_and_not_retried(self):
        fleet = fleet_of(A, B)
        fleet.clusters[A].fail("list_nodes", KvClusterApiError("this cluster's API failed while listing nodes (503): etcd"))
        async with Harness(fleet) as h:
            result = await h.create()
        assert result.kv_cluster == B
        assert (result.table[0].name, result.table[0].skip_reason) == (A, "unreachable")
        assert "503" in result.table[0].skip_detail and result.table[0].figures.readable is False
        assert fleet.clusters[A].calls.count("list_nodes") == 1  # asked once: the lock is not held for retries

    async def test_a_worker_deployed_under_another_name_never_passes_its_numbers_off(self):
        fleet = fleet_of(B)
        # day1 also lists A, and the worker polling A's queue is really B's twin, misnamed.
        fleet.list_in_day1(A, SITE, "az1")
        imposter = FakeKvCluster("ocp4-prod-kv-five-z", nodes=big("z"), mac_prefix="0e:0f:01")
        async with Harness(fleet) as h:
            async with Worker(h.client, task_queue=vm_lifecycle_activity_queue(A), activities=fleet.cluster_activities(imposter)):
                result = await h.create()
        assert result.kv_cluster == B and imposter.vms == {}
        assert result.table[0].skip_reason == "unreachable" and "ocp4-prod-kv-five-z" in result.table[0].skip_detail

    async def test_a_worker_that_only_ever_fails_is_given_up_on_not_waited_for(self):
        """Whatever keeps a figures read from answering, the cluster ends as a row."""
        fleet = fleet_of(B)
        fleet.list_in_day1(A, SITE, "az1")
        attempts = []

        @activity.defn(name="read_kv_cluster_figures")
        async def broken(request: KvFiguresRequest) -> KvClusterFigures:
            attempts.append(request)
            raise RuntimeError("an image that cannot answer")

        async with Harness(fleet) as h:
            async with Worker(h.client, task_queue=vm_lifecycle_activity_queue(A), activities=[broken]):
                # Two minutes of retries (the bound of the whole read), then the next cluster.
                result = await h.result(await h.start(), step=timedelta(seconds=10))
        assert result.kv_cluster == B and len(attempts) >= 2
        assert result.table[0].skip_reason == "unreachable" and "gave no figures" in result.table[0].skip_detail

    async def test_a_cluster_with_no_usable_node_and_one_whose_nodes_are_too_small(self):
        fleet = KvFleet()
        fleet.add(A, "az1", nodes=[node("a1", 100, 1000, cordoned=True), node("a2", 100, 1000, ready=False)])
        fleet.add(B, "az1", nodes=[node(f"b{i}", 16, 128) for i in range(20)])  # 320 cores, none holds 30
        fleet.add(C, "az1", nodes=big(C))
        async with Harness(fleet) as h:
            result = await h.create()
        assert result.kv_cluster == C
        assert [(row.name, row.skip_reason) for row in result.table] == [(A, "no-nodes"), (B, "fit-node"), (C, None)]
        assert "2 nodes, none usable" in result.table[0].skip_detail

    async def test_when_nothing_fits_the_run_fails_listing_every_cluster_and_creates_nothing(self):
        fleet = KvFleet()
        fleet.add(A, "az1", nodes=big(A)).add_pod(f"{A}-n1", 60_000, 0)
        fleet.add(B, "az1", nodes=[node("b1", 16, 128)])
        fleet.add(C, "az1", nodes=big(C))
        fleet.add("ocp4-prod-kv-five-d", "az1", nodes=[node("d1", 64, 512, cordoned=True)])
        async with Harness(fleet, down={C}) as h:
            handle = await h.start()
            failed = await h.failure(handle, step=timedelta(seconds=10))
            error = cause(failed)
            assert error.type == NoKvClusterFitsError.__name__
            for words in (f"{A} would go past the placement buffer", f"{B} have no single node with the room",
                          f"{C} have no vm-lifecycle worker answering", "ocp4-prod-kv-five-d have no usable node",
                          "32 vCPU / 256 GiB", "pod request 30000 m / 257 GiB", "15% buffer", "Nothing was created"):
                assert words in error.message, words
            assert all(cluster.vms == {} for cluster in fleet.clusters.values())
            assert (await h.lock_is_free()).grants == 1
            progress = await h.progress(handle)
            assert [row.skip_reason for row in progress.table] == ["fit-buffer", "fit-node", "no-worker", "no-nodes"]


# --- day1 -------------------------------------------------------------------------------------


class TestTheListFromDay1:
    async def test_a_zone_with_no_cluster_fails_at_once_naming_the_folder(self):
        fleet = fleet_of(A)
        async with Harness(fleet) as h:
            failed = await h.failure(await h.start(vm_input("c200-kv32-five-az2")))
            error = cause(failed)
            assert error.type == NoKvClusterError.__name__
            assert "sites/five/mces/ocp4-seed-five-2/hostedClusters/" in error.message and "0123abc" in error.message
            assert fleet.clusters[A].calls == []
            # The lock was never asked for: nothing of the zone exists.
            with pytest.raises(Exception):  # noqa: B017, PT011
                await h.lock(SITE, "az2")

    async def test_a_file_day1_lists_with_a_problem_is_never_read_and_is_reported(self):
        fleet = fleet_of(B)
        wrong_zone = fleet.add(A, "az1", nodes=big(A), in_day1=False)
        fleet.list_in_day1(A, SITE, "az1", confirmed_zone=None,
                           problems=["nodepool workers labels its nodes topology.kubernetes.io/zone=az2, not this seed's az1"])
        fleet.list_in_day1("ocp4-test-kubevirt-five", SITE, "az1", problems=["its name is not ocp4-<lifecycle>-kv-<site>-<id>"])
        async with Harness(fleet) as h:
            result = await h.create()
        assert result.kv_cluster == B and wrong_zone.calls == []
        rows = {row.name: row for row in result.table}
        assert rows[A].skip_reason == "day1-problem" and "az2" in rows[A].skip_detail
        assert rows["ocp4-test-kubevirt-five"].skip_reason == "day1-problem"

    async def test_only_problem_files_is_no_cluster_and_names_them(self):
        fleet = KvFleet()
        fleet.add(A, "az1", nodes=big(A), in_day1=False)
        fleet.list_in_day1(A, SITE, "az1", confirmed_zone=None, problems=["its NodePools label no zone"])
        async with Harness(fleet) as h:
            failed = await h.failure(await h.start())
        error = cause(failed)
        assert error.type == NoKvClusterError.__name__
        assert "listed with a problem and never used" in error.message and A in error.message

    async def test_an_entry_confirmed_for_another_zone_is_not_this_zones(self):
        """Belt and braces: whatever the list holds, only entries day1 confirmed for THIS zone are read."""
        fleet = fleet_of(B)
        other = fleet.add(A, "az1", nodes=big(A), in_day1=False)
        fleet.list_in_day1(A, SITE, "az1", confirmed_zone="az2")
        async with Harness(fleet) as h:
            result = await h.create()
        assert result.kv_cluster == B and other.calls == []

    async def test_an_unreachable_day1_is_waited_out(self):
        fleet = fleet_of(A)
        fleet.list_failures = [ValuesRepoGitError("git ls-remote failed (exit 128): could not resolve host")] * 2
        async with Harness(fleet) as h:
            result = await h.create()
        assert result.kv_cluster == A and len(fleet.listed) == 3

    async def test_a_zone_the_convention_does_not_know_fails_the_run_instead_of_hanging_it(self):
        """Only a run started around the API can carry one; it must still END."""
        fleet = fleet_of(A)
        async with Harness(fleet) as h:
            request = vm_input().model_copy(update={"zone": "a"})
            failed = await h.failure(await h.start(request))
        assert cause(failed).type == NoKvClusterError.__name__ and "naming convention" in cause(failed).message
        assert fleet.listed == []


# --- the create, on the chosen cluster -------------------------------------------------------------


class TestTheCreate:
    async def test_a_taken_mac_is_drawn_again_by_the_worker_without_the_hub(self):
        fleet = fleet_of(A)
        cluster = fleet.clusters[A]
        prefix = cluster.values.mac_prefix
        cluster.add_vm(stopped_vm("someone", 8, 64, mac=f"{prefix}:01:02:03", labels={}))
        cluster.draws = [bytes([1, 2, 3]), bytes([4, 5, 6])]
        async with Harness(fleet) as h:
            result = await h.create()
        assert (result.mac, result.draws) == (f"{prefix}:04:05:06", 2)
        assert cluster.calls.count("create_vm") == 1

    async def test_three_taken_draws_fail_the_run_with_nothing_created_and_the_zone_free(self):
        fleet = fleet_of(A)
        cluster = fleet.clusters[A]
        cluster.fail("create_vm", NameTaken("exists"), MacRefused("failed to allocate requested mac address"), NameTaken("exists"))
        async with Harness(fleet) as h:
            failed = await h.failure(await h.start())
            assert cause(failed).type == VmMacExhaustedError.__name__ and "after 3 draws" in cause(failed).message
            assert cluster.vms == {} and cluster.calls.count("create_vm") == 3
            assert (await h.lock_is_free()).grants == 1

    @pytest.mark.parametrize(
        ("error", "calls"),
        [(VmRequestRejectedError("422: networks[0] is invalid"), 1), (KvClusterAccessError("403: virtualmachines is forbidden"), 1)],
    )
    async def test_a_permanent_refusal_fails_the_run_after_one_attempt(self, error, calls):
        fleet = fleet_of(A, B)
        fleet.clusters[A].fail("create_vm", error)
        async with Harness(fleet) as h:
            failed = await h.failure(await h.start())
            assert cause(failed).type == type(error).__name__
            assert fleet.clusters[A].calls.count("create_vm") == calls
            # The create is never moved to another cluster, and the zone is free.
            assert fleet.clusters[B].calls == [] and fleet.clusters[A].vms == {}
            await h.lock_is_free()

    async def test_a_missing_namespace_fails_the_run_and_creates_nothing(self):
        fleet = fleet_of(A, namespaces=())
        async with Harness(fleet) as h:
            failed = await h.failure(await h.start())
            assert cause(failed).type == VmNamespaceNotFoundError.__name__ and "inventory" in cause(failed).message
            await h.lock_is_free()

    async def test_a_transient_failure_before_the_create_is_retried_on_the_same_cluster(self):
        fleet = fleet_of(A, B)
        # The create's own namespace read fails twice (the figures read does not make that call).
        fleet.clusters[A].fail("namespace_exists", KvClusterApiError("503"), KvClusterApiError("503"))
        async with Harness(fleet) as h:
            result = await h.create()
        assert result.kv_cluster == A and result.created is True and len(fleet.clusters[A].vms) == 1
        assert fleet.clusters[A].calls.count("namespace_exists") == 3 and fleet.clusters[A].calls.count("create_vm") == 1
        assert fleet.clusters[B].calls == []  # out-waited where it was chosen, never moved

    async def test_a_create_that_fails_transiently_is_retried_and_makes_one_vm(self):
        fleet = fleet_of(A, B)
        fleet.clusters[A].fail("create_vm", KvClusterApiError("503 etcd"), KvClusterApiError("500"))
        async with Harness(fleet) as h:
            result = await h.create()
        assert result.kv_cluster == A and len(fleet.clusters[A].vms) == 1 and fleet.clusters[B].calls == []
        assert fleet.clusters[A].calls.count("create_vm") == 3

    async def test_a_create_whose_answer_was_lost_is_not_made_twice(self):
        fleet = fleet_of(A)
        cluster = fleet.clusters[A]
        original = cluster.create_vm
        lost = {"once": True}

        async def created_then_lost(namespace, body):
            created = await original(namespace, body)
            if lost.pop("once", False):
                raise KvClusterApiError("this cluster's API could not be reached while creating: read timed out")
            return created

        cluster.create_vm = created_then_lost
        async with Harness(fleet) as h:
            result = await h.create()
        assert len(cluster.vms) == 1 and result.created is False and result.draws == 0
        assert cluster.vm_names == [result.vm_name]

    async def test_a_create_that_reaches_the_worker_after_the_lease_makes_nothing(self):
        fleet = fleet_of(A)
        fleet.skew = timedelta(hours=1)  # the cluster's clock is past every lease
        async with Harness(fleet) as h:
            failed = await h.failure(await h.start())
            assert cause(failed).type == VmLeaseExpiredError.__name__ and "nothing was created" in cause(failed).message
            assert fleet.clusters[A].vms == {} and fleet.clusters[A].calls.count("create_vm") == 0
            await h.lock_is_free()

    async def test_the_worker_stops_creating_a_guard_before_the_lock_moves_on(self):
        fleet = fleet_of(A)
        requests = []
        cluster = fleet.clusters[A]
        activities = fleet.cluster_activities(cluster)
        fleet.cluster_activities = lambda _cluster: activities  # keep the same four…

        real_create = activities[1]

        @activity.defn(name="create_vm_on_cluster")
        async def spy(request: VmCreateRequest) -> VmCreated:  # …but see what the create was sent
            requests.append(request)
            return await real_create(request)

        activities[1] = spy
        async with Harness(fleet) as h:
            handle = await h.start()
            await h.result(handle)
            lock = await h.lock()
            description_run = (await handle.describe()).run_id
        sent = requests[0]
        assert sent.request_id == description_run and sent.workflow_id == handle.id
        assert (sent.flavor, sent.cpu, sent.memory_gib, sent.site, sent.zone, sent.mce, sent.namespace) == (
            "c200", 32, 256, "five", "az1", MCE, "inventory")
        assert not hasattr(sent, "mac") and not hasattr(sent, "name")
        assert lock.grants == 1
        # create_before is the lease's end less the guard: the worker stops creating before the lock moves on.
        lease_end = sent.create_before + create_vm._LEASE_GUARD
        assert timedelta(0) < lease_end - (await_now := sent.create_before) and await_now.tzinfo is not None
        assert create_vm._LEASE_GUARD == timedelta(seconds=45) and kv_zone_lock._LEASE == timedelta(minutes=5)


# --- the lease ------------------------------------------------------------------------------------


def dead_and_alive(dead: int, alive: int = 1) -> tuple[KvFleet, list[str], list[str]]:
    """A zone whose first `dead` clusters (the oldest) have no worker, and then `alive` that do."""
    letters = "abcdefghijklmnopqrstuvwxyz"
    fleet = KvFleet()
    names = [f"ocp4-prod-kv-five-{letter}" for letter in letters[: dead + alive]]
    # More than 26 clusters in one zone: a second lifecycle keeps the names within the convention.
    names += [f"ocp4-prep-kv-five-{letter}" for letter in letters[: max(0, dead + alive - 26)]]
    for name in names:
        fleet.add(name, "az1", nodes=big(name))
    ordered = [entry.name for entry in fleet.entries(SITE, "az1")]
    return fleet, ordered[:dead], ordered[dead:]


class TestTheLease:
    """On the production waits: a lease is five minutes, a cluster without a worker costs thirty seconds."""

    async def test_clusters_without_workers_can_outlast_a_lease_and_the_run_carries_on_under_the_next(self):
        """Eleven dead clusters cost 330 s against a 300 s lease: the scan resumes where it stopped."""
        fleet, dead, alive = dead_and_alive(11)
        async with Harness(fleet, down=set(dead)) as h:
            result = await h.result(await h.start(), step=timedelta(seconds=10))
            assert result.kv_cluster == alive[0] and result.leases == 2
            assert [(row.name, row.skip_reason) for row in result.table] == [(name, "no-worker") for name in dead] + [(alive[0], None)]
            assert len(fleet.clusters[alive[0]].vms) == 1
            assert (await h.lock_is_free()).grants == 2

    async def test_a_run_never_creates_on_figures_read_under_a_lease_that_is_almost_over(self):
        """Nine dead clusters: the one that fits is reached at 270 s of a 300 s lease — inside the
        55 s the create needs before the lease's end. Its figures are not acted on: the run asks
        for the lock again and reads that cluster AGAIN before creating."""
        fleet, dead, alive = dead_and_alive(9)
        target = fleet.clusters[alive[0]]
        async with Harness(fleet, down=set(dead)) as h:
            result = await h.result(await h.start(), step=timedelta(seconds=10))
            assert result.kv_cluster == alive[0] and result.leases == 2
            assert target.calls.count("list_nodes") == 2 and target.calls.count("create_vm") == 1
            assert len(target.vms) == 1 and [row.name for row in result.table].count(alive[0]) == 1
            assert (await h.lock_is_free()).grants == 2

    async def test_with_time_to_spare_the_cluster_that_fits_is_read_once(self):
        """Eight dead clusters: reached at 240 s, a full minute before the lease's end. One lease."""
        fleet, dead, alive = dead_and_alive(8)
        target = fleet.clusters[alive[0]]
        async with Harness(fleet, down=set(dead)) as h:
            result = await h.result(await h.start(), step=timedelta(seconds=10))
            assert result.kv_cluster == alive[0] and result.leases == 1
            assert target.calls.count("list_nodes") == 1
            lock = await h.lock_is_free()
            assert (lock.grants, lock.expired) == (1, 0)

    async def test_a_zone_that_outlasts_every_lease_fails_with_nothing_created(self):
        fleet, dead, alive = dead_and_alive(31)  # 930 s of dead workers against three 300 s leases
        async with Harness(fleet, down=set(dead)) as h:
            failed = await h.failure(await h.start(), step=timedelta(seconds=15), limit=120)
            error = cause(failed)
            assert error.type == KvZoneLeaseExpiredError.__name__
            assert "3 zone leases ran out" in error.message and "have no vm-lifecycle worker answering" in error.message
            assert "Nothing was created" in error.message
            assert all(cluster.vms == {} for cluster in fleet.clusters.values())
            assert fleet.clusters[alive[0]].calls == []  # never reached
            await h.lock_is_free()

    async def test_a_lease_is_asked_for_only_while_there_is_a_cluster_left_to_read(self):
        """All dead, and fewer than a lease's worth: one lease, then NoKvClusterFits — not a second lease."""
        fleet, dead, _ = dead_and_alive(3, 0)
        async with Harness(fleet, down=set(dead)) as h:
            failed = await h.failure(await h.start(), step=timedelta(seconds=10))
            assert cause(failed).type == NoKvClusterFitsError.__name__
            assert (await h.lock_is_free()).grants == 1


# --- the lock, from the run's side --------------------------------------------------------------------


class TestWaitingForTheLock:
    async def test_a_lock_never_granted_fails_the_run_after_thirty_minutes_and_it_leaves_the_line(self):
        fleet = fleet_of(A)
        async with Harness(fleet, lock_worker=False) as h:  # nobody serves the zone's lock
            handle = await h.start()
            await h.until_phase(handle, "awaiting-zone-lock")
            await h.advance(timedelta(minutes=29))  # (the lock's own workflow task is the unserved one)
            assert (await h.progress(handle)).phase == "awaiting-zone-lock"
            await h.advance(timedelta(minutes=2))
            failed = await h.failure(handle)
            error = cause(failed)
            assert error.type == KvZoneLockTimeoutError.__name__
            assert "was not granted within 30 min" in error.message and "left the line" in error.message
            assert "nothing was created" in error.message and kv_zone_lock_workflow_id(SITE, "az1") in error.message
            assert fleet.clusters[A].calls == []
            # When the lock is served at last, the run that gave up is not in its line — however often it
            # had asked — and no grant is wasted on it.
            await fleet.start_lock_worker()
            lock = await h.lock_is_free()
            assert (lock.grants, lock.skipped) == (0, 0)
            assert (await h.create()).kv_cluster == A

    async def test_a_waiting_run_asks_again_so_a_lock_someone_terminated_does_not_strand_it(self):
        fleet = fleet_of(A)
        async with Harness(fleet, lock_worker=False) as h:
            handle = await h.start()
            await h.until_phase(handle, "awaiting-zone-lock")
            lock_id = kv_zone_lock_workflow_id(SITE, "az1")
            await h.client.get_workflow_handle(lock_id).terminate()  # the lock, with this run in its line, is gone
            await fleet.start_lock_worker()
            await asyncio.sleep(0.3)
            assert (await h.progress(handle)).phase == "awaiting-zone-lock"  # nothing tells the run; it waits
            await h.advance(create_vm._LOCK_REASK + timedelta(seconds=5))
            result = await h.result(handle)  # its next ask started the lock afresh
            assert result.kv_cluster == A and result.leases == 1
            assert (await h.lock_is_free()).grants == 1

    async def test_a_grant_the_run_is_not_waiting_for_is_given_straight_back(self):
        fleet = fleet_of(A)
        fleet.clusters[A].storage = "wffc"
        fleet.poll_seconds = 0.05
        async with Harness(fleet) as h:
            handle = await h.start()
            await h.until_phase(handle, "awaiting-root-disk")
            await h.lock_is_free()
            run_id = (await handle.describe()).run_id
            # A request in this run's name reaches the lock although the run already released (a late duplicate).
            await h.client.get_workflow_handle(kv_zone_lock_workflow_id(SITE, "az1")).signal(
                KvZoneLockWorkflow.request_lock, KvZoneLockRequest(workflow_id=handle.id, run_id=run_id))
            lock = await h.until_lock(lambda p: p.grants == 2 and p.holder is None)
            assert lock.expired == 0  # handed back at once, not left to run out
            await h.failure(handle, limit=120)

    async def test_a_run_terminated_while_holding_costs_the_zone_one_lease_at_most(self):
        fleet = fleet_of(A)
        fleet.clusters[A].delays["namespace_exists"] = 4  # the holder is stuck in its create, before it creates
        async with Harness(fleet) as h:
            stuck = await h.start()
            await h.until_phase(stuck, "creating-vm")
            waiting = await h.start()
            await h.until_phase(waiting, "awaiting-zone-lock")
            await stuck.terminate()  # no code runs: nothing releases the lock
            fleet.clusters[A].delays.clear()
            await asyncio.sleep(4.2)  # the dead run's create attempt ends on the worker; nothing is outstanding
            assert fleet.clusters[A].vms == {} or len(fleet.clusters[A].vms) == 1
            await h.advance(kv_zone_lock._LEASE - timedelta(seconds=30))
            assert (await h.progress(waiting)).phase == "awaiting-zone-lock"
            await h.advance(timedelta(seconds=40))
            result = await h.result(waiting)  # served when the dead holder's lease ended
            assert result.kv_cluster == A
            assert (await h.lock_is_free()).expired == 1


# --- a run that does not complete leaves no VM ---------------------------------------------------------


class TestRollback:
    async def test_a_root_disk_that_never_binds_gets_the_vm_removed_then_fails_the_run(self):
        fleet = fleet_of(A, storage="wffc")
        cluster = fleet.clusters[A]
        async with Harness(fleet) as h:
            handle = await h.start()
            failed = await h.failure(handle, limit=120)
            error = cause(failed)
            assert error.type == VmRootDiskNotBoundError.__name__
            for words in ("within 10 min", "The VM was deleted again", "WaitForFirstConsumer", "bound False", "ReadWriteMany"):
                assert words in error.message, words
            assert cluster.vms == {} and cluster.datavolumes == {} and len(cluster.created) == 1
            assert cluster.deleted == cluster.created
            progress = await h.progress(handle)
            assert progress.phase == "removing-vm" and progress.vm_name == cluster.created[0]
            # The removed VM no longer weighs on the cluster: the next run's figures are clean.
            assert (await cluster.figures()).stopped_vms == 0

    async def test_a_vm_someone_deleted_under_the_wait_fails_the_run_and_nothing_is_removed(self):
        fleet = fleet_of(A, storage="wffc")
        fleet.poll_seconds = 0.05
        cluster = fleet.clusters[A]
        async with Harness(fleet) as h:
            handle = await h.start()
            await h.until_phase(handle, "awaiting-root-disk")
            cluster.vms.clear()
            failed = await h.failure(handle)
            assert cause(failed).type == VmDisappearedError.__name__
            assert "delete_vm" not in cluster.calls

    async def test_a_wait_that_fails_for_good_removes_the_vm_it_would_orphan(self):
        fleet = fleet_of(A)
        cluster = fleet.clusters[A]
        cluster.fail("get_vm", KvClusterAccessError("403: virtualmachines.kubevirt.io is forbidden"))
        async with Harness(fleet) as h:
            failed = await h.failure(await h.start())
            assert cause(failed).type == KvClusterAccessError.__name__
            assert cluster.vms == {} and cluster.deleted == cluster.created

    async def test_when_even_the_removal_fails_the_run_still_ends_and_says_where_the_vm_is(self):
        fleet = fleet_of(A)
        cluster = fleet.clusters[A]
        cluster.fail("get_vm", KvClusterAccessError("403 get"))      # the wait is refused for good…
        cluster.fail("delete_vm", KvClusterAccessError("403 delete"))  # …and so is the removal
        async with Harness(fleet) as h:
            handle = await h.start()
            failed = await h.failure(handle)
            # The run fails with what failed the WAIT, not with the removal's error…
            assert cause(failed).type == KvClusterAccessError.__name__ and "403 get" in cause(failed).message
            assert len(cluster.vms) == 1 and cluster.calls.count("delete_vm") == 1  # …and the VM is still there


async def cancelled(h: Harness, handle: WorkflowHandle, **kwargs) -> CreateVmProgress:
    """The run ended CANCELED (not failed, not completed); its last progress."""
    failed = await h.failure(handle, **kwargs)
    assert isinstance(failed.cause, CancelledError), f"not a cancel: {failed.cause!r}"
    assert (await handle.describe()).status == WorkflowExecutionStatus.CANCELED
    return await h.progress(handle)


class TestCancel:
    async def test_cancelled_while_waiting_for_the_lock_it_leaves_the_line(self):
        fleet = fleet_of(A)
        fleet.clusters[A].delays["create_vm"] = 3.0
        async with Harness(fleet) as h:
            holder = await h.start()
            await h.until_phase(holder, "creating-vm")
            waiting = await h.start()
            await h.until_phase(waiting, "awaiting-zone-lock")
            await h.until_lock(lambda p: p.waiting == 1)
            await waiting.cancel()
            progress = await cancelled(h, waiting)
            assert "nothing to undo" in progress.cancel_rollback
            still = await h.until_lock(lambda p: p.waiting == 0)
            assert still.holder is not None and still.holder.workflow_id == holder.id  # the holder is undisturbed
            await h.result(holder)
            lock = await h.lock_is_free()
            assert (lock.grants, lock.skipped, lock.expired) == (1, 0, 0)
            assert len(fleet.clusters[A].vms) == 1

    async def test_cancelled_while_choosing_it_releases_the_zone_and_has_created_nothing(self):
        fleet = fleet_of(A)
        fleet.clusters[A].delays["list_nodes"] = 3.0
        async with Harness(fleet) as h:
            handle = await h.start()
            await h.until_phase(handle, "choosing-kv-cluster")
            await handle.cancel()
            progress = await cancelled(h, handle)
            assert "before anything was sent to a cluster" in progress.cancel_rollback
            lock = await h.lock_is_free()
            assert (lock.grants, lock.expired) == (1, 0)  # given back, not left to run out
            assert fleet.clusters[A].vms == {} and "create_vm" not in fleet.clusters[A].calls

    async def test_cancelled_during_the_create_it_waits_for_the_call_to_settle_then_removes_the_vm(self):
        fleet = fleet_of(A)
        cluster = fleet.clusters[A]
        cluster.delays["create_vm"] = 1.0
        async with Harness(fleet) as h:
            handle = await h.start()
            await h.until_phase(handle, "creating-vm")
            await handle.cancel()
            # The zone is free at once; the VM lands a moment later, although nobody waits for it.
            assert (await h.lock_is_free()).expired == 0
            for _ in range(100):
                if cluster.vms:
                    break
                await asyncio.sleep(0.05)
            assert len(cluster.vms) == 1
            await asyncio.sleep(0.3)  # the create's activity is over: the clock can be jumped again
            await h.advance(create_vm._CANCEL_SETTLE - timedelta(seconds=10))
            assert len(cluster.vms) == 1 and (await handle.describe()).status == WorkflowExecutionStatus.RUNNING
            await h.advance(timedelta(seconds=15))
            progress = await cancelled(h, handle)
            assert cluster.vms == {} and cluster.deleted == cluster.created
            assert "cancelled; removed" in progress.cancel_rollback and cluster.created[0] in progress.cancel_rollback

    async def test_cancelled_during_the_create_when_nothing_landed_there_is_nothing_to_remove(self):
        fleet = fleet_of(A)
        cluster = fleet.clusters[A]
        cluster.delays["list_vms"] = 0  # the figures read passes…
        async with Harness(fleet) as h:
            original = cluster.namespace_exists

            async def stuck(namespace):  # …and the create hangs before it creates anything
                await asyncio.sleep(1.0)
                raise KvClusterApiError("503")

            cluster.namespace_exists = stuck
            handle = await h.start()
            await h.until_phase(handle, "creating-vm")
            await handle.cancel()
            await asyncio.sleep(1.3)  # the attempt in flight ends (failing); a cancelled activity is not retried
            await h.advance(create_vm._CANCEL_SETTLE + timedelta(seconds=5))
            cluster.namespace_exists = original
            progress = await cancelled(h, handle)
            assert cluster.vms == {} and cluster.created == []
            assert "removed nothing" in progress.cancel_rollback

    async def test_cancelled_while_waiting_for_the_disk_it_removes_its_vm_at_once(self):
        fleet = fleet_of(A, storage="wffc")
        fleet.poll_seconds = 0.05
        cluster = fleet.clusters[A]
        async with Harness(fleet) as h:
            handle = await h.start()
            await h.until_phase(handle, "awaiting-root-disk")
            assert len(cluster.vms) == 1
            await handle.cancel()
            progress = await cancelled(h, handle)  # no settle wait: the create had answered
            assert cluster.vms == {} and cluster.datavolumes == {}
            assert "cancelled; removed" in progress.cancel_rollback

    async def test_a_cancel_whose_removal_cannot_run_still_ends_the_run_and_names_the_vm(self):
        fleet = fleet_of(A, storage="wffc")
        fleet.poll_seconds = 0.05
        cluster = fleet.clusters[A]
        async with Harness(fleet) as h:
            handle = await h.start()
            await h.until_phase(handle, "awaiting-root-disk")
            run_id = (await handle.describe()).run_id
            await fleet.stop_worker(A)  # the cluster's worker goes away with the VM on it
            await handle.cancel()
            await h.until_phase(handle, "removing-vm")
            await asyncio.sleep(0.3)
            await h.advance(create_vm._ROLLBACK_DELETE_DEADLINE - timedelta(minutes=1))
            assert (await handle.describe()).status == WorkflowExecutionStatus.RUNNING  # still trying to remove it
            await h.advance(timedelta(minutes=2))
            progress = await cancelled(h, handle)  # ended once the bounded removal gave up
            assert len(cluster.vms) == 1
            assert "could NOT be removed" in progress.cancel_rollback
            assert f"redbull.io/created-by={run_id}" in progress.cancel_rollback

    async def test_cancelled_while_the_deadline_rollback_is_removing_the_vm_the_removal_is_finished(self):
        fleet = fleet_of(A, storage="wffc")
        cluster = fleet.clusters[A]
        cluster.delays["delete_vm"] = 1.0
        async with Harness(fleet) as h:
            handle = await h.start()
            await h.until_phase(handle, "removing-vm")  # the ten-minute disk wait is over; the delete is running
            await handle.cancel()
            progress = await cancelled(h, handle)
            assert cluster.vms == {} and cluster.datavolumes == {}
            assert progress.cancel_rollback.startswith("cancelled")

    async def test_a_completed_run_keeps_its_vm(self):
        fleet = fleet_of(A)
        async with Harness(fleet) as h:
            handle = await h.start()
            first = await h.result(handle)
            await handle.cancel()  # a cancel of a closed run is accepted by Temporal and does nothing
            assert (await handle.describe()).status == WorkflowExecutionStatus.COMPLETED
            assert fleet.clusters[A].vm_names == [first.vm_name]


# --- slots ------------------------------------------------------------------------------------------


class TestSlotReuse:
    async def test_a_second_run_on_the_same_workflow_id_makes_a_second_vm_never_the_first_again(self):
        fleet = fleet_of(A)
        slot = create_vm_slot_workflow_id(MCE, "c200-kv32-five-az1", 1)
        async with Harness(fleet) as h:
            first_handle = await h.start(workflow_id=slot)
            first = await h.result(first_handle)
            first_run = (await first_handle.describe()).run_id
            second_handle = await h.start(workflow_id=slot)
            second = await h.result(second_handle)
            second_run = (await second_handle.describe()).run_id
        cluster = fleet.clusters[A]
        assert first.vm_name != second.vm_name and len(cluster.vms) == 2 and second.created is True
        owners = {vm_spec.name_of(vm): vm_spec.created_by(vm) for vm in cluster.vms.values()}
        assert owners == {first.vm_name: first_run, second.vm_name: second_run} and first_run != second_run

    async def test_a_slot_reused_after_its_run_was_terminated_in_the_line_is_served_once(self):
        fleet = fleet_of(A)
        fleet.clusters[A].delays["create_vm"] = 2.0
        slot = create_vm_slot_workflow_id(MCE, "c200-kv32-five-az1", 7)
        async with Harness(fleet) as h:
            holder = await h.start()
            await h.until_phase(holder, "creating-vm")
            dead = await h.start(workflow_id=slot)
            await h.until_phase(dead, "awaiting-zone-lock")
            await h.until_lock(lambda p: p.waiting == 1)
            await dead.terminate()
            reused = await h.start(workflow_id=slot)
            first, second = await h.results(holder, reused)
            assert first.vm_name != second.vm_name and len(fleet.clusters[A].vms) == 2
            lock = await h.lock_is_free()
            assert (lock.grants, lock.skipped, lock.expired) == (2, 1, 0)


# --- zones, sites, and a fleet of dozens ----------------------------------------------------------------


def a_fleet(sites: tuple[str, ...], zones: tuple[str, ...], per_zone: int, *, cores: int = 100) -> KvFleet:
    """`per_zone` KV clusters in every zone of every site, one roomy node each."""
    fleet = KvFleet()
    for site in sites:
        for zone_index, zone in enumerate(zones):
            for index in range(per_zone):
                # Ids run on through the site's zones: a cluster's name carries no zone.
                letter = "abcdefghijklmnopqrstuvwxyz"[zone_index * per_zone + index]
                name = f"ocp4-prod-kv-{site}-{letter}"
                fleet.add(name, zone, nodes=[node(f"{name}-n1", cores, 10_000)])
    return fleet


def infra_env(site: str, zone: str, cpu: int = 32) -> CreateVmInput:
    return vm_input(f"c200-kv{cpu}-{site}-{zone}", mce=f"ocp4-prod-mce-{site}-a")


class TestZonesAndSites:
    async def test_a_vm_only_ever_lands_on_a_cluster_of_its_own_site_and_zone(self):
        fleet = a_fleet(("five", "bat-yam"), ("az1", "az2"), 2)
        async with Harness(fleet) as h:
            results = {}
            for site in ("five", "bat-yam"):
                for zone in ("az1", "az2"):
                    results[(site, zone)] = await h.create(infra_env(site, zone))
            for (site, zone), result in results.items():
                own = [entry.name for entry in fleet.entries(site, zone)]
                assert result.kv_cluster == own[0] and result.not_read == own[1:]
                vm = fleet.clusters[result.kv_cluster].vms[("inventory", result.vm_name)]
                labels = vm["metadata"]["labels"]
                assert (labels["redbull.io/site"], labels["topology.kubernetes.io/zone"]) == (site, zone)
                assert result.vm_name.startswith(f"ocp-c200-kv32-{site}-")
                assert (await h.lock(site, zone)).grants == 1
            assert sorted((asked.site, asked.zone) for asked in fleet.listed) == sorted(results)
            assert sum(len(cluster.vms) for cluster in fleet.clusters.values()) == 4

    async def test_a_zone_held_up_holds_up_no_other_zone_and_no_other_site(self):
        fleet = a_fleet(("five", "bat-yam"), ("az1", "az2"), 1)
        slow = fleet.entries("five", "az1")[0].name
        fleet.clusters[slow].delays["create_vm"] = 30
        async with Harness(fleet) as h:
            stuck = await h.start(infra_env("five", "az1"))
            await h.until_phase(stuck, "creating-vm")
            behind = await h.start(infra_env("five", "az1"))
            await h.until_phase(behind, "awaiting-zone-lock")
            others = [await h.start(infra_env(site, zone)) for site, zone in (("five", "az2"), ("bat-yam", "az1"), ("bat-yam", "az2"))]
            done = await h.results(*others, step=timedelta(0))  # no time passes: nobody waited for anything
            assert [r.site for r in done] == ["five", "bat-yam", "bat-yam"] and [r.zone for r in done] == ["az2", "az1", "az2"]
            assert (await h.progress(behind)).phase == "awaiting-zone-lock"
            await stuck.terminate()
            await behind.terminate()

    async def test_a_fleet_of_dozens_fills_every_zone_oldest_first_and_never_past_its_buffer(self):
        """Three sites, four zones each, three KV clusters per zone: 36 clusters, 72 runs at once."""
        sites, zones = ("five", "bat-yam", "site2"), ("az1", "az2", "az3", "az4")
        fleet = a_fleet(sites, zones, 3)
        assert len(fleet.clusters) == 36
        async with Harness(fleet) as h:
            handles = {}
            for _ in range(6):  # six per zone: two fit on each of its three clusters
                for site in sites:
                    for zone in zones:
                        handles.setdefault((site, zone), []).append(await h.start(infra_env(site, zone)))
            flat = [handle for group in handles.values() for handle in group]
            results = dict(zip([h_.id for h_ in flat], await h.results(*flat, limit=300), strict=True))
            for (site, zone), group in handles.items():
                own = [entry.name for entry in fleet.entries(site, zone)]
                placed = sorted(results[handle.id].kv_cluster for handle in group)
                assert placed == sorted(own * 2), f"{site}/{zone}: {placed}"
                lock = await h.lock(site, zone)
                assert (lock.grants, lock.expired, lock.skipped, lock.waiting) == (6, 0, 0, 0)
            names, macs = [], []
            for name, cluster in fleet.clusters.items():
                assert len(cluster.vms) == 2
                assert await cluster.requested_cpu_millicores() == 60_000 <= 85_000
                for vm in cluster.vms.values():
                    mac = vm_spec.mac_of(vm)
                    assert mac.startswith(cluster.values.mac_prefix + ":")
                    assert vm["metadata"]["labels"]["redbull.io/site"] == cluster.values.site
                    names.append(vm_spec.name_of(vm))
                    macs.append(mac)
            assert len(set(names)) == len(names) == 72 and len(set(macs)) == 72
            # One more per zone: every zone is full, and says so for its own three clusters only.
            extra = await h.failure(await h.start(infra_env("bat-yam", "az3")))
            assert cause(extra).type == NoKvClusterFitsError.__name__
            for entry in fleet.entries("bat-yam", "az3"):
                assert entry.name in cause(extra).message
            assert fleet.entries("five", "az1")[0].name not in cause(extra).message

    async def test_a_fleet_with_dead_unreadable_and_mislisted_clusters_still_places_every_vm_it_can(self, quick):
        """Per zone: the oldest cluster has no worker, the next cannot read its API, one file is
        mislisted in day1; the last two are healthy. Every run still lands, on a healthy one."""
        sites, zones = ("five", "bat-yam"), ("az1", "az2", "az3")
        fleet = a_fleet(sites, zones, 4)
        down: set[str] = set()
        healthy: dict[tuple[str, str], list[str]] = {}
        for site in sites:
            for zone in zones:
                own = [entry.name for entry in fleet.entries(site, zone)]
                down.add(own[0])
                unreadable = fleet.clusters[own[1]]
                unreadable.fail("list_nodes", *[KvClusterApiError("503: etcd leader changed")] * 50)
                fleet.list_in_day1(f"ocp4-test-kubevirt-{site}", site, zone, problems=["its name is not ocp4-<lifecycle>-kv-<site>-<id>"])
                healthy[(site, zone)] = own[2:]
        async with Harness(fleet, down=down, quick=True) as h:
            handles = {key: [await h.start(infra_env(*key)) for _ in range(4)] for key in healthy}
            flat = [handle for group in handles.values() for handle in group]
            outcomes = await h.results(*flat, step=REAL_TIME, limit=300)
            by_id = dict(zip([handle.id for handle in flat], outcomes, strict=True))
            for key, group in handles.items():
                placed = sorted(by_id[handle.id].kv_cluster for handle in group)
                assert placed == sorted(healthy[key] * 2), f"{key}: {placed}"
                for handle in group:
                    reasons = {row.name: row.skip_reason for row in by_id[handle.id].table}
                    own = [entry.name for entry in fleet.entries(*key) if entry.id]
                    assert reasons[own[0]] == "no-worker" and reasons[own[1]] == "unreachable"
                    assert reasons[f"ocp4-test-kubevirt-{key[0]}"] == "day1-problem"
            for name in down:
                assert fleet.clusters[name].vms == {}
