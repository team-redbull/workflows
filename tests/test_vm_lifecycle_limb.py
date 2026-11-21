"""The vm-lifecycle worker inside a KV cluster: its operations (vm_operations.py) on a fake
cluster, its Kubernetes client wrapper (kubevirt_api.py) on fake SDK clients, the thin
activities, and its settings. docs/design/create-vm-placement.md §6.3.3, §7.5."""

from __future__ import annotations

import asyncio
import json
import os
from datetime import UTC, datetime, timedelta
from unittest import mock

import pytest
from kubernetes import client as k8s_client
from pydantic import ValidationError
from temporalio.testing import ActivityEnvironment

from activities.vm_lifecycle import activities as limb
from activities.vm_lifecycle import kubevirt_api, vm_operations, vm_spec
from activities.vm_lifecycle.cluster_figures import pod_usage
from activities.vm_lifecycle.kubevirt_api import KubevirtApi, MacRefused, NameTaken, NotFound
from shared.exceptions import (
    KvClusterAccessError,
    KvClusterApiError,
    KvClusterMismatchError,
    VmDisappearedError,
    VmIdentityConflictError,
    VmLeaseExpiredError,
    VmMacExhaustedError,
    VmNamespaceNotFoundError,
    VmRequestRejectedError,
)
from shared.models.vm_lifecycle import KvFiguresRequest, VmCreateRequest, VmDeleteRequest, VmReadyRequest
from shared.settings.vm_lifecycle import VmLifecycleActivitySettings
from tests.kv_fleet import GIB, FakeKvCluster, node, stopped_vm

KV = "ocp4-prod-kv-five-a"
RUN = "0196f6a2-1111-7222-8333-444455556666"
NOW = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)


def cluster(**kwargs) -> FakeKvCluster:
    kwargs.setdefault("nodes", [node("n1", 100, 1024)])
    kwargs.setdefault("mac_prefix", "0e:03:f2")
    return FakeKvCluster(KV, **kwargs)


def create_request(**overrides) -> VmCreateRequest:
    values = dict(
        kv_cluster=KV, namespace="inventory", flavor="c200", cpu=32, memory_gib=256, site="five", zone="az1",
        mce="ocp4-prod-mce-five-a", request_id=RUN, workflow_id="create-vm-ocp4-prod-mce-five-a_c200-kv32-five-az1_1",
        create_before=NOW + timedelta(minutes=4),
    )
    values.update(overrides)
    return VmCreateRequest(**values)


async def create(kv: FakeKvCluster, request: VmCreateRequest | None = None, *, now: datetime = NOW):
    return await vm_operations.create_vm(kv, kv.values, request or create_request(), now=lambda: now, random_bytes=kv.random_bytes)


# --- read_figures --------------------------------------------------------------------------------


class TestReadFigures:
    async def test_the_figures_are_measured_from_the_four_lists(self):
        kv = cluster(nodes=[node("n1", 64, 512), node("n2", 64, 512)])
        kv.add_pod("n1", 10_000, 20 * GIB)
        kv.add_vm(stopped_vm("vm-a", 32, 256))
        figures = await vm_operations.read_figures(kv, kv.values, KvFiguresRequest(kv_cluster=KV))
        assert figures.readable and figures.kv_cluster == KV
        assert (figures.usable_nodes, figures.allocatable_cpu_millicores) == (2, 128_000)
        assert (figures.pods_cpu_millicores, figures.stopped_vms, figures.stopped_vms_cpu_millicores) == (10_000, 1, 30_000)
        assert sorted(kv.calls) == ["list_nodes", "list_unfinished_pods", "list_vmis", "list_vms"]

    async def test_only_labelled_vms_are_asked_for(self):
        kv = cluster()
        seen = []
        original = kv.list_vms

        async def spy(label_selector=None):
            seen.append(label_selector)
            return await original(label_selector)

        kv.list_vms = spy
        kv.add_vm(stopped_vm("unlabelled", 8, 64, labels={}))
        figures = await vm_operations.read_figures(kv, kv.values, KvFiguresRequest(kv_cluster=KV))
        assert seen == ["redfish-enabled=true"] and figures.stopped_vms == 0

    async def test_a_request_for_another_cluster_is_not_answered_with_this_ones_numbers(self):
        kv = cluster()
        figures = await vm_operations.read_figures(kv, kv.values, KvFiguresRequest(kv_cluster="ocp4-prod-kv-five-b"))
        assert figures.readable is False and figures.kv_cluster == "ocp4-prod-kv-five-b"
        assert KV in figures.detail and "kvCluster" in figures.detail
        assert kv.calls == []  # its own cluster was not even read

    @pytest.mark.parametrize(
        "error",
        [KvClusterApiError("this cluster's API failed while listing pods (503): etcd leader changed"),
         KvClusterAccessError("403 nodes is forbidden"), RuntimeError("boom"), ValueError("quantity 'x' is not one")],
    )
    @pytest.mark.parametrize("method", ["list_nodes", "list_unfinished_pods", "list_vms", "list_vmis"])
    async def test_an_api_that_cannot_be_read_is_a_value_never_an_exception(self, method, error):
        kv = cluster()
        kv.fail(method, error)
        figures = await vm_operations.read_figures(kv, kv.values, KvFiguresRequest(kv_cluster=KV))
        assert figures.readable is False and type(error).__name__ in figures.detail
        assert figures.allocatable_cpu_millicores == 0 and figures.node_headroom == []

    async def test_reads_that_hang_are_cut_off_together(self, monkeypatch):
        monkeypatch.setattr(vm_operations, "FIGURES_TIMEOUT_SECONDS", 0.05)
        kv = cluster()
        kv.delays["list_unfinished_pods"] = 5
        started = asyncio.get_running_loop().time()
        figures = await vm_operations.read_figures(kv, kv.values, KvFiguresRequest(kv_cluster=KV))
        assert asyncio.get_running_loop().time() - started < 2
        assert figures.readable is False and "did not answer the four list calls within" in figures.detail

    async def test_the_detail_is_bounded(self):
        kv = cluster()
        kv.fail("list_nodes", KvClusterApiError("x" * 5000))
        figures = await vm_operations.read_figures(kv, kv.values, KvFiguresRequest(kv_cluster=KV))
        assert len(figures.detail) < 500 and "more characters" in figures.detail

    async def test_a_cancellation_is_not_swallowed(self):
        kv = cluster()
        kv.delays["list_nodes"] = 5
        task = asyncio.create_task(vm_operations.read_figures(kv, kv.values, KvFiguresRequest(kv_cluster=KV)))
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


# --- create_vm -----------------------------------------------------------------------------------


class TestCreateVm:
    async def test_the_worker_draws_the_mac_names_the_vm_and_creates_it_halted(self):
        kv = cluster()
        kv.draws = [bytes([0xA1, 0xB2, 0xC3])]
        created = await create(kv)
        assert (created.name, created.mac, created.namespace) == ("ocp-c200-kv32-five-0e03f2a1b2c3", "0e:03:f2:a1:b2:c3", "inventory")
        assert (created.created, created.draws, created.kv_cluster, created.cpu, created.memory_gib) == (True, 1, KV, 32, 256)
        vm = kv.vms[("inventory", created.name)]
        assert vm["spec"]["runStrategy"] == "Halted" and vm_spec.created_by(vm) == RUN
        assert vm["metadata"]["labels"]["topology.kubernetes.io/zone"] == "az1"
        assert vm["metadata"]["annotations"]["redbull.io/workflow-id"].startswith("create-vm-")
        assert ("inventory", f"{created.name}-root") in kv.datavolumes

    async def test_the_new_vm_is_in_the_next_figures_as_a_stopped_vm(self):
        """What replaces any memory between runs."""
        kv = cluster()
        before = await kv.figures()
        await create(kv)
        after = await kv.figures()
        assert (before.stopped_vms, after.stopped_vms) == (0, 1)
        assert after.stopped_vms_cpu_millicores == 30_000 and after.stopped_vms_memory_bytes == 257 * GIB

    async def test_a_retry_finds_the_vm_its_request_already_created_and_draws_nothing(self):
        kv = cluster()
        first = await create(kv)
        kv.draws = [bytes([9, 9, 9])]
        again = await create(kv)
        assert (again.name, again.mac, again.created, again.draws) == (first.name, first.mac, False, 0)
        assert len(kv.vms) == 1 and kv.draws == [bytes([9, 9, 9])]  # nothing drawn, nothing created
        assert kv.calls.count("create_vm") == 1

    async def test_a_retry_after_the_lease_still_returns_the_vm_it_created(self):
        kv = cluster()
        first = await create(kv)
        late = await create(kv, now=NOW + timedelta(hours=1))
        assert late.name == first.name and late.created is False

    async def test_another_runs_vm_is_never_taken_for_this_ones(self):
        kv = cluster()
        other = await create(kv, create_request(request_id="another-run-id"))
        mine = await create(kv)
        assert mine.name != other.name and mine.created is True and len(kv.vms) == 2

    @pytest.mark.parametrize("late_by", [timedelta(0), timedelta(seconds=1), timedelta(hours=1)])
    async def test_nothing_is_created_once_the_lease_is_over(self, late_by):
        kv = cluster()
        with pytest.raises(VmLeaseExpiredError, match="nothing was created"):
            await create(kv, now=NOW + timedelta(minutes=4) + late_by)
        assert kv.vms == {} and "create_vm" not in kv.calls

    async def test_the_lease_is_checked_again_right_before_each_create(self):
        """A lookup or a redraw that ate the lease must not be followed by a create."""
        kv = cluster()
        clock = {"now": NOW}
        original = kv.namespace_exists

        async def slow_namespace_read(namespace):
            clock["now"] = NOW + timedelta(minutes=10)  # the lease ends during this call
            return await original(namespace)

        kv.namespace_exists = slow_namespace_read
        with pytest.raises(VmLeaseExpiredError):
            await vm_operations.create_vm(kv, kv.values, create_request(), now=lambda: clock["now"], random_bytes=kv.random_bytes)
        assert kv.vms == {}

    async def test_a_mac_a_vm_of_the_cluster_already_has_is_drawn_again_without_a_create(self):
        kv = cluster()
        kv.add_vm(stopped_vm("someone", 8, 64, namespace="server-scan-vms", mac="0E:03:F2:01:02:03", labels={}))
        kv.namespaces.add("server-scan-vms")
        kv.draws = [bytes([1, 2, 3]), bytes([4, 5, 6])]
        created = await create(kv)
        assert (created.mac, created.draws) == ("0e:03:f2:04:05:06", 2)
        assert kv.calls.count("create_vm") == 1  # the taken MAC cost no round trip to the API

    async def test_mac_uniqueness_never_depends_on_kubemacpool(self):
        """Plain KubeVirt has no KubeMacPool, and OpenShift Virtualization's lets a duplicate MAC
        through on a Halted VM: the worker's own check is the only guard, in every namespace."""
        kv = cluster(kubemacpool=False, namespaces=("inventory", "other", "server-scan-vms"))
        kv.add_vm(stopped_vm("in-another-namespace", 8, 64, namespace="other", mac="0e:03:f2:0a:0a:0a", labels={}))
        kv.add_vm(stopped_vm("running-somewhere", 8, 64, namespace="server-scan-vms", mac="0E:03:F2:0B:0B:0B"))
        kv.start("server-scan-vms", "running-somewhere", "n1")
        kv.draws = [bytes([0x0A, 0x0A, 0x0A]), bytes([0x0B, 0x0B, 0x0B]), bytes([0x0C, 0x0C, 0x0C])]
        created = await create(kv)
        assert (created.mac, created.draws) == ("0e:03:f2:0c:0c:0c", 3)
        assert kv.calls.count("create_vm") == 1  # the API was never asked to take a used MAC
        assert len(set(kv.macs())) == len(kv.macs()) == 3

    async def test_without_kubemacpool_a_hundred_creates_on_a_crowded_prefix_never_share_a_mac(self):
        import random

        kv = cluster(kubemacpool=False)
        # A tiny random space (the last octet only), so draws collide all the time.
        rng = random.Random(7)
        kv.random_bytes = lambda count: bytes([0, 0, rng.randrange(256)])
        made = 0
        for index in range(150):
            try:
                await create(kv, create_request(request_id=f"run-{index}"))
                made += 1
            except VmMacExhaustedError:
                pass  # three taken draws in a row: nothing created, never a duplicate
        assert made > 100 and len(kv.macs()) == made == len(set(kv.macs())) == len(kv.vms)

    async def test_an_already_exists_is_drawn_again(self):
        kv = cluster()
        kv.fail("create_vm", NameTaken("already exists"))
        kv.draws = [bytes([1, 1, 1]), bytes([2, 2, 2])]
        created = await create(kv)
        assert (created.mac, created.draws) == ("0e:03:f2:02:02:02", 2) and len(kv.vms) == 1

    async def test_kubemacpool_refusing_the_mac_is_drawn_again(self):
        kv = cluster()
        kv.fail("create_vm", MacRefused("failed to allocate requested mac address"))
        created = await create(kv)
        assert created.draws == 2 and len(kv.vms) == 1

    async def test_a_name_that_exists_in_the_namespace_is_refused_by_the_cluster_itself(self):
        # Same flavor, size, site and MAC -> the same name: the fake API answers AlreadyExists.
        kv = cluster(kubemacpool=False)
        kv.draws = [bytes([7, 7, 7])]
        first = await create(kv, create_request(request_id="run-1"))
        # Pretend the MAC list missed it (no fixed MAC readable): only the name collides.
        kv.vms[("inventory", first.name)]["spec"]["template"]["spec"]["domain"]["devices"]["interfaces"][0].pop("macAddress")
        kv.draws = [bytes([7, 7, 7]), bytes([8, 8, 8])]
        second = await create(kv, create_request(request_id="run-2"))
        assert second.draws == 2 and second.name.endswith("0e03f2080808")

    async def test_three_taken_draws_fail_with_nothing_created(self):
        kv = cluster()
        kv.add_vm(stopped_vm("a", 8, 64, mac="0e:03:f2:01:01:01"))
        kv.draws = [bytes([1, 1, 1]), bytes([2, 2, 2]), bytes([3, 3, 3])]
        kv.fail("create_vm", NameTaken("exists"), MacRefused("failed to allocate requested mac address"))
        with pytest.raises(VmMacExhaustedError) as raised:
            await create(kv)
        said = str(raised.value)
        assert "after 3 draws" in said and "0e:03:f2:01:01:01" in said and "Nothing was created" in said
        assert len(kv.vms) == 1 and kv.calls.count("create_vm") == 2

    async def test_a_missing_namespace_is_named_and_nothing_is_drawn(self):
        kv = cluster(namespaces=())
        with pytest.raises(VmNamespaceNotFoundError, match="inventory"):
            await create(kv)
        assert "create_vm" not in kv.calls

    async def test_a_namespace_deleted_under_the_create_is_still_named(self):
        kv = cluster()
        kv.fail("create_vm", NotFound('namespaces "inventory" not found'))
        original = kv.namespace_exists
        answers = [True, False]

        async def vanishing(namespace):
            await original(namespace)
            return answers.pop(0)

        kv.namespace_exists = vanishing
        with pytest.raises(VmNamespaceNotFoundError, match="vanished"):
            await create(kv)

    async def test_a_cluster_that_serves_no_virtual_machines_is_waited_out(self):
        kv = cluster()
        kv.fail("create_vm", NotFound("the server could not find the requested resource"))
        with pytest.raises(KvClusterApiError, match="OpenShift Virtualization is not installed"):
            await create(kv)

    @pytest.mark.parametrize(
        "error", [VmRequestRejectedError("422: spec.template.spec.networks is invalid"), KvClusterAccessError("403"),
                  KvClusterApiError("503")],
    )
    async def test_what_the_api_refuses_reaches_the_workflow_classified(self, error):
        kv = cluster()
        kv.fail("create_vm", error)
        with pytest.raises(type(error)):
            await create(kv)
        assert kv.vms == {}

    async def test_a_create_whose_answer_was_lost_is_found_by_the_retry(self):
        """The API created the VM, then the call failed: the next attempt must not make a second."""
        kv = cluster()
        original = kv.create_vm

        async def created_then_lost(namespace, body):
            await original(namespace, body)
            raise KvClusterApiError("this cluster's API could not be reached while creating: read timed out")

        kv.create_vm = created_then_lost
        with pytest.raises(KvClusterApiError):
            await create(kv)
        kv.create_vm = original
        again = await create(kv)
        assert again.created is False and len(kv.vms) == 1

    async def test_a_request_for_another_cluster_is_refused(self):
        kv = cluster()
        with pytest.raises(KvClusterMismatchError, match="kvCluster"):
            await create(kv, create_request(kv_cluster="ocp4-prod-kv-five-b"))
        assert kv.calls == []

    async def test_a_vm_of_another_site_is_refused(self):
        kv = cluster()
        with pytest.raises(KvClusterMismatchError, match="site five .* site six"):
            await create(kv, create_request(site="six"))
        assert kv.calls == []

    async def test_a_request_that_cannot_name_a_vm_is_rejected_before_the_cluster_is_asked(self):
        kv = cluster()
        with pytest.raises(VmRequestRejectedError, match="cannot name a VM"):
            await create(kv, create_request(flavor="Not-A-Flavor"))
        assert kv.calls == []

    async def test_the_clusters_own_values_shape_the_vm(self):
        kv = FakeKvCluster(KV, nodes=[node("n1", 100, 1024)], mac_prefix="0e:0f:ff", node_selector={"role": "vm"})
        created = await create(kv)
        vm = kv.vms[("inventory", created.name)]
        assert created.mac.startswith("0e:0f:ff:")
        assert vm["spec"]["template"]["spec"]["nodeSelector"] == {"role": "vm"}
        assert vm["spec"]["dataVolumeTemplates"][0]["spec"]["storage"]["storageClassName"] == "cephfs"

    async def test_an_attempt_starts_no_call_to_the_api_past_its_budget(self):
        """So an attempt is over before Temporal may start the next one beside it."""
        kv = cluster()
        clock = {"seconds": 0.0}
        original = kv.list_vms

        async def slow_list(label_selector=None):
            clock["seconds"] += vm_operations.CREATE_BUDGET_SECONDS + 1  # the first call ate the whole budget
            return await original(label_selector)

        kv.list_vms = slow_list
        with pytest.raises(KvClusterApiError, match="stops before reading the VM namespace"):
            await vm_operations.create_vm(kv, kv.values, create_request(), now=lambda: NOW, random_bytes=kv.random_bytes,
                                          monotonic=lambda: clock["seconds"])
        assert kv.vms == {} and "namespace_exists" not in kv.calls and "create_vm" not in kv.calls

    async def test_the_budget_is_checked_right_before_the_create_too(self):
        kv = cluster()
        clock = {"seconds": 0.0}
        original = kv.namespace_exists

        async def slow_namespace_read(namespace):
            clock["seconds"] += vm_operations.CREATE_BUDGET_SECONDS + 1
            return await original(namespace)

        kv.namespace_exists = slow_namespace_read
        with pytest.raises(KvClusterApiError, match="stops before creating the VM"):
            await vm_operations.create_vm(kv, kv.values, create_request(), now=lambda: NOW, random_bytes=kv.random_bytes,
                                          monotonic=lambda: clock["seconds"])
        assert kv.vms == {} and "create_vm" not in kv.calls
        # The retry, on a healthy API, simply creates it.
        assert (await create(kv)).created is True

    async def test_a_budget_spent_after_the_create_is_made_good_by_the_retry(self):
        kv = cluster()
        clock = {"seconds": 0.0}
        original = kv.create_vm

        async def slow_create(namespace, body):
            clock["seconds"] += vm_operations.CREATE_BUDGET_SECONDS + 1
            return await original(namespace, body)

        kv.create_vm = slow_create
        with pytest.raises(KvClusterApiError, match="stops before reading the run's VMs back"):
            await vm_operations.create_vm(kv, kv.values, create_request(), now=lambda: NOW, random_bytes=kv.random_bytes,
                                          monotonic=lambda: clock["seconds"])
        assert len(kv.vms) == 1  # it landed; the attempt just could not confirm it in time
        kv.create_vm = original
        again = await create(kv)
        assert again.created is False and len(kv.vms) == 1

    async def test_the_run_holds_exactly_one_vm_before_its_name_is_recorded(self):
        """Read-back: after the create, the VMs carrying the run's id are listed again."""
        kv = cluster()
        selectors = []
        original = kv.list_vms

        async def spy(label_selector=None):
            selectors.append(label_selector)
            return await original(label_selector)

        kv.list_vms = spy
        created = await create(kv)
        assert selectors == [None, f"redbull.io/created-by={RUN}"] and created.created is True

    async def test_a_create_that_landed_after_its_caller_gave_up_is_met_by_the_read_back(self):
        """Attempt 1's create reached the API server late; attempt 2 saw no VM and made its own.
        Attempt 2's read-back finds both, keeps the OLDER and deletes its own."""
        kv = cluster()
        original = kv.create_vm
        late = {}

        async def the_first_attempts_vm_lands_first(namespace, body):
            if not late:
                earlier = vm_spec.build(
                    namespace="inventory", flavor="c200", cpu=32, memory_gib=256, site="five", zone="az1",
                    mce="ocp4-prod-mce-five-a", mac="0e:03:f2:00:00:01", request_id=RUN, workflow_id=None,
                    redfish_label=kv.values.redfish_label, storage_class_name="cephfs", network_name=kv.values.network_name,
                    node_selector={}, root_disk_size="120Gi",
                )
                late["name"] = (await original(namespace, earlier))["metadata"]["name"]
            return await original(namespace, body)

        kv.create_vm = the_first_attempts_vm_lands_first
        answered = await create(kv)
        assert (answered.name, answered.mac, answered.created) == (late["name"], "0e:03:f2:00:00:01", False)
        assert kv.vm_names == [late["name"]] and len(kv.datavolumes) == 1  # the surplus VM and its disk are gone
        assert (await kv.figures()).stopped_vms == 1

    async def test_two_vms_of_one_run_met_at_the_lookup_converge_on_the_oldest(self):
        kv = cluster()
        first = await create(kv, create_request(request_id="twice"))
        # A second VM of the same run, made by an attempt that never saw the first.
        twin = dict(kv.vms[("inventory", first.name)])
        twin = {**twin, "metadata": {**twin["metadata"], "name": "ocp-c200-kv32-five-0e03f2ffffff", "uid": "twin",
                                    "creationTimestamp": "2099-01-01T00:00:00+00:00"}}
        kv.vms[("inventory", "ocp-c200-kv32-five-0e03f2ffffff")] = twin
        for _ in range(2):  # whichever attempt meets them, and however often
            answered = await create(kv, create_request(request_id="twice"))
            assert (answered.name, answered.created, answered.draws) == (first.name, False, 0)
        assert kv.vm_names == [first.name]

    async def test_ten_thousand_draws_never_leave_the_prefix_or_repeat_a_name(self):
        kv = cluster()
        names = set()
        for index in range(300):
            created = await create(kv, create_request(request_id=f"run-{index}"))
            assert created.mac.startswith("0e:03:f2:") and created.name.endswith(created.mac.replace(":", ""))
            names.add(created.name)
        assert len(names) == 300 == len(kv.vms) == len(set(kv.macs()))


# --- wait_ready ----------------------------------------------------------------------------------


async def wait(kv: FakeKvCluster, name: str, *, deadline_minutes: int = 10, started_at=None, polls: int = 0, beats=None):
    clock = {"elapsed": 0.0}
    started = started_at or NOW

    async def sleep(seconds: float) -> None:
        clock["elapsed"] += seconds

    return await vm_operations.wait_ready(
        kv, kv.values, VmReadyRequest(kv_cluster=KV, namespace="inventory", name=name, deadline_minutes=deadline_minutes),
        started_at=started, polls=polls, now=lambda: NOW + timedelta(seconds=clock["elapsed"]),
        heartbeat=lambda at, count: (beats if beats is not None else []).append((at, count)), sleep=sleep,
    )


class TestWaitReady:
    async def test_done_once_the_root_disk_is_bound_and_the_vm_admitted(self):
        kv = cluster(bind_after_polls=3)
        created = await create(kv)
        beats: list = []
        outcome = await wait(kv, created.name, beats=beats)
        assert (outcome.result, outcome.root_disk_phase, outcome.root_disk_bound, outcome.admitted) == ("done", "Succeeded", True, True)
        # The disk is Succeeded on the third read; KubeVirt marks the VM Stopped after it, seen on the fourth.
        assert outcome.polls == 4 and outcome.mac == created.mac
        assert [count for _, count in beats] == [1, 2, 3, 4]  # one heartbeat per poll, carrying the clock

    async def test_a_disk_that_never_binds_is_a_deadline_result_not_an_exception(self):
        kv = cluster(storage="wffc")
        created = await create(kv)
        outcome = await wait(kv, created.name, deadline_minutes=10)
        assert outcome.result == "deadline" and outcome.root_disk_bound is False
        assert outcome.root_disk_phase == "WaitForFirstConsumer" and "bound False" in outcome.detail
        assert outcome.polls == 600 // 8 + 1  # every 8 s until ten minutes have passed

    async def test_a_resumed_wait_keeps_its_clock_and_its_poll_count(self):
        kv = cluster(storage="wffc")
        created = await create(kv)
        # An earlier attempt started nine minutes ago and polled 67 times before its worker died.
        outcome = await wait(kv, created.name, started_at=NOW - timedelta(minutes=9), polls=67)
        assert outcome.result == "deadline" and outcome.polls == 67 + 9  # one more minute of polls, not ten

    async def test_a_vm_that_disappears_is_permanent(self):
        kv = cluster(storage="wffc")
        created = await create(kv)
        del kv.vms[("inventory", created.name)]
        with pytest.raises(VmDisappearedError, match="is gone"):
            await wait(kv, created.name)

    async def test_bound_but_not_admitted_is_not_ready(self):
        kv = cluster(bind_after_polls=1)
        created = await create(kv)
        original = kv.get_vm

        async def not_admitted(namespace, name):
            vm = await original(namespace, name)
            vm["status"]["printableStatus"] = "Provisioning"
            return vm

        kv.get_vm = not_admitted
        outcome = await wait(kv, created.name, deadline_minutes=1)
        assert outcome.result == "deadline" and outcome.root_disk_bound and not outcome.admitted

    async def test_an_api_failure_while_waiting_is_raised_for_temporal_to_retry(self):
        kv = cluster()
        created = await create(kv)
        kv.fail("get_vm", KvClusterApiError("503"))
        with pytest.raises(KvClusterApiError):
            await wait(kv, created.name)

    async def test_a_wait_for_another_clusters_vm_is_refused(self):
        kv = cluster()
        with pytest.raises(KvClusterMismatchError):
            await vm_operations.wait_ready(
                kv, kv.values, VmReadyRequest(kv_cluster="ocp4-prod-kv-five-b", namespace="inventory", name="x"),
                started_at=NOW, polls=0, now=lambda: NOW, heartbeat=lambda *_: None,
            )


# --- delete_vm -----------------------------------------------------------------------------------


def delete_request(**overrides) -> VmDeleteRequest:
    values = dict(kv_cluster=KV, namespace="inventory", request_id=RUN)
    values.update(overrides)
    return VmDeleteRequest(**values)


class TestDeleteVm:
    async def test_the_runs_vm_and_its_root_disk_go(self):
        kv = cluster()
        created = await create(kv)
        deleted = await vm_operations.delete_vm(kv, kv.values, delete_request(name=created.name, mac=created.mac))
        assert deleted.deleted and deleted.names == [f"inventory/{created.name}"]
        assert kv.vms == {} and kv.datavolumes == {}

    async def test_without_a_name_the_request_id_finds_it(self):
        """A cancel cut the create's answer off: the run never learned the name."""
        kv = cluster()
        created = await create(kv)
        others = await create(kv, create_request(request_id="another-run"))
        deleted = await vm_operations.delete_vm(kv, kv.values, delete_request())
        assert deleted.names == [f"inventory/{created.name}"] and list(kv.vm_names) == [others.name]

    async def test_nothing_there_is_success(self):
        kv = cluster()
        deleted = await vm_operations.delete_vm(kv, kv.values, delete_request(name="ocp-c200-kv32-five-0e03f2000000", mac="0e:03:f2:00:00:00"))
        assert deleted.deleted is False and deleted.names == []
        again = await vm_operations.delete_vm(kv, kv.values, delete_request())
        assert again.deleted is False

    async def test_deleting_twice_is_deleting_once(self):
        kv = cluster()
        created = await create(kv)
        request = delete_request(name=created.name, mac=created.mac)
        assert (await vm_operations.delete_vm(kv, kv.values, request)).deleted is True
        assert (await vm_operations.delete_vm(kv, kv.values, request)).deleted is False

    async def test_a_vm_of_that_name_with_another_mac_is_someone_elses(self):
        kv = cluster()
        created = await create(kv)
        with pytest.raises(VmIdentityConflictError, match="left alone"):
            await vm_operations.delete_vm(kv, kv.values, delete_request(name=created.name, mac="0e:03:f2:ff:ff:ff"))
        assert len(kv.vms) == 1

    async def test_a_vm_of_that_name_created_by_another_run_is_someone_elses(self):
        kv = cluster()
        created = await create(kv, create_request(request_id="another-run"))
        with pytest.raises(VmIdentityConflictError, match="another-run"):
            await vm_operations.delete_vm(kv, kv.values, delete_request(name=created.name, mac=created.mac))
        assert len(kv.vms) == 1

    async def test_a_vm_without_our_label_is_never_deleted_whatever_its_name(self):
        kv = cluster()
        kv.add_vm(stopped_vm("hand-made", 8, 64, mac="0e:03:f2:00:00:01"))
        with pytest.raises(VmIdentityConflictError):
            await vm_operations.delete_vm(kv, kv.values, delete_request(name="hand-made", mac="0e:03:f2:00:00:01"))
        assert kv.vm_names == ["hand-made"]

    async def test_a_vm_replaced_between_the_read_and_the_delete_is_left(self):
        """The delete carries the UID it read: a new object of the same name is not it."""
        kv = cluster()
        created = await create(kv)
        original = kv.delete_vm

        async def replaced_meanwhile(namespace, name, uid=None):
            kv.vms[(namespace, name)]["metadata"]["uid"] = "a-new-object"
            return await original(namespace, name, uid)

        kv.delete_vm = replaced_meanwhile
        deleted = await vm_operations.delete_vm(kv, kv.values, delete_request(name=created.name, mac=created.mac))
        assert deleted.deleted is False and len(kv.vms) == 1

    async def test_a_delete_for_another_cluster_is_refused(self):
        kv = cluster()
        with pytest.raises(KvClusterMismatchError):
            await vm_operations.delete_vm(kv, kv.values, delete_request(kv_cluster="ocp4-prod-kv-five-b"))

    async def test_an_api_failure_is_raised_for_temporal_to_retry(self):
        kv = cluster()
        created = await create(kv)
        kv.fail("delete_vm", KvClusterApiError("503"))
        with pytest.raises(KvClusterApiError):
            await vm_operations.delete_vm(kv, kv.values, delete_request(name=created.name, mac=created.mac))
        assert (await vm_operations.delete_vm(kv, kv.values, delete_request(name=created.name, mac=created.mac))).deleted


# --- kubevirt_api: the kubernetes client wrapper --------------------------------------------------


def api_error(status: int, message: str = "", reason: str = "") -> k8s_client.ApiException:
    error = k8s_client.ApiException(status=status, reason=reason or "Error")
    error.body = json.dumps({"kind": "Status", "message": message, "reason": reason, "code": status}) if message else None
    return error


class _Raw:
    """What the SDK returns with `_preload_content=False`."""

    def __init__(self, payload) -> None:
        self.data = json.dumps(payload).encode()


class FakeCore:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.pages: dict[str, list] = {}
        self.errors: dict[str, list] = {}
        self.namespaces = {"inventory"}
        self.pvcs: dict[tuple[str, str], str] = {}

    def _call(self, name: str, kwargs: dict):
        self.calls.append((name, kwargs))
        if self.errors.get(name):
            raise self.errors[name].pop(0)

    def _page(self, name: str, kwargs: dict):
        self._call(name, kwargs)
        pages = self.pages.get(name, [[]])
        index = int(kwargs.get("_continue") or 0)
        metadata = {"continue": str(index + 1)} if index + 1 < len(pages) else {}
        return _Raw({"items": pages[index], "metadata": metadata})

    def list_node(self, **kwargs):
        return self._page("list_node", kwargs)

    def list_pod_for_all_namespaces(self, **kwargs):
        return self._page("list_pod_for_all_namespaces", kwargs)

    def read_namespace(self, name, **kwargs):
        self._call("read_namespace", kwargs)
        if name not in self.namespaces:
            raise api_error(404, f'namespaces "{name}" not found', "NotFound")
        return _Raw({"metadata": {"name": name}})

    def read_namespaced_persistent_volume_claim(self, name, namespace, **kwargs):
        self._call("read_pvc", kwargs)
        if (namespace, name) not in self.pvcs:
            raise api_error(404, "not found", "NotFound")
        return _Raw({"status": {"phase": self.pvcs[(namespace, name)]}})


class FakeCustom:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple, dict]] = []
        self.pages: dict[str, list] = {}
        self.objects: dict[tuple[str, str, str], dict] = {}
        self.errors: dict[str, list] = {}

    def _call(self, name: str, args: tuple, kwargs: dict):
        self.calls.append((name, args, kwargs))
        if self.errors.get(name):
            raise self.errors[name].pop(0)

    def list_cluster_custom_object(self, group, version, plural, **kwargs):
        self._call("list", (group, version, plural), kwargs)
        pages = self.pages.get(plural, [[]])
        index = int(kwargs.get("_continue") or 0)
        return {"items": pages[index], "metadata": {"continue": str(index + 1)} if index + 1 < len(pages) else {}}

    def get_namespaced_custom_object(self, group, version, namespace, plural, name, **kwargs):
        self._call("get", (group, version, namespace, plural, name), kwargs)
        if (plural, namespace, name) not in self.objects:
            raise api_error(404, "not found", "NotFound")
        return self.objects[(plural, namespace, name)]

    def create_namespaced_custom_object(self, group, version, namespace, plural, body, **kwargs):
        self._call("create", (group, version, namespace, plural), kwargs)
        self.objects[(plural, namespace, body["metadata"]["name"])] = body
        return body

    def delete_namespaced_custom_object(self, group, version, namespace, plural, name, **kwargs):
        self._call("delete", (group, version, namespace, plural, name), kwargs)
        if (plural, namespace, name) not in self.objects:
            raise api_error(404, "not found", "NotFound")
        del self.objects[(plural, namespace, name)]
        return {}


@pytest.fixture
def kube():
    core, custom = FakeCore(), FakeCustom()
    return KubevirtApi(core=core, custom=custom), core, custom


class TestKubevirtApiReads:
    async def test_lists_are_paged_and_decoded_from_the_raw_json(self, kube):
        api, core, _ = kube
        core.pages["list_node"] = [[{"metadata": {"name": "n1"}}], [{"metadata": {"name": "n2"}}]]
        assert [n["metadata"]["name"] for n in await api.list_nodes()] == ["n1", "n2"]
        assert [kwargs.get("_continue") for _, kwargs in core.calls] == [None, "1"]
        for _, kwargs in core.calls:
            assert kwargs["_preload_content"] is False and kwargs["limit"] == kubevirt_api.PAGE_SIZE
            assert kwargs["_request_timeout"] == kubevirt_api.REQUEST_TIMEOUT

    async def test_pods_are_reduced_page_by_page_and_only_unfinished_ones_are_asked_for(self, kube):
        api, core, _ = kube
        running = {"spec": {"nodeName": "n1", "containers": [{"resources": {"requests": {"cpu": "2", "memory": "1Gi"}}}]},
                   "status": {"phase": "Running"}}
        done = {"spec": {"nodeName": "n1", "containers": []}, "status": {"phase": "Succeeded"}}
        core.pages["list_pod_for_all_namespaces"] = [[running, done], [running], [running]]
        usages = await api.list_unfinished_pods(pod_usage)
        assert usages == [("n1", 2000, GIB)] * 3  # reduced to three small tuples; the finished one dropped
        assert core.calls[0][1]["field_selector"] == "status.phase!=Succeeded,status.phase!=Failed"
        assert len(core.calls) == 3

    async def test_vms_are_listed_cluster_wide_with_the_selector_asked_for(self, kube):
        api, _, custom = kube
        custom.pages["virtualmachines"] = [[{"metadata": {"name": "a"}}], [{"metadata": {"name": "b"}}]]
        custom.pages["virtualmachineinstances"] = [[{"metadata": {"name": "a"}}]]
        assert len(await api.list_vms("redfish-enabled=true")) == 2
        assert len(await api.list_vms()) == 2
        assert len(await api.list_vmis()) == 1
        first, second, third, fourth, fifth = custom.calls
        assert first[1] == ("kubevirt.io", "v1", "virtualmachines") and first[2]["label_selector"] == "redfish-enabled=true"
        assert second[2]["_continue"] == "1"
        assert "label_selector" not in third[2]
        assert fifth[1] == ("kubevirt.io", "v1", "virtualmachineinstances")
        assert all(call[2]["_request_timeout"] == kubevirt_api.REQUEST_TIMEOUT for call in custom.calls)

    async def test_absent_objects_are_none_not_errors(self, kube):
        api, core, _ = kube
        assert await api.get_vm("inventory", "nope") is None
        assert await api.get_datavolume("inventory", "nope-root") is None
        assert await api.pvc_phase("inventory", "nope-root") is None
        assert await api.namespace_exists("nope") is False and await api.namespace_exists("inventory") is True
        core.pvcs[("inventory", "vm-root")] = "Bound"
        assert await api.pvc_phase("inventory", "vm-root") == "Bound"

    async def test_the_blocking_sdk_never_runs_on_the_event_loop(self, kube):
        import threading

        api, core, _ = kube
        loop_thread = threading.get_ident()
        seen = []
        original = core.list_node

        def spy(**kwargs):
            seen.append(threading.get_ident())
            return original(**kwargs)

        core.list_node = spy
        await api.list_nodes()
        assert seen and seen[0] != loop_thread


class TestKubevirtApiClassification:
    @pytest.mark.parametrize(
        ("status", "expected"),
        [(500, KvClusterApiError), (502, KvClusterApiError), (503, KvClusterApiError), (504, KvClusterApiError),
         (401, KvClusterApiError), (429, KvClusterApiError), (410, KvClusterApiError), (409, KvClusterApiError),
         (403, KvClusterAccessError), (400, VmRequestRejectedError), (422, VmRequestRejectedError)],
    )
    async def test_every_status_is_classified(self, kube, status, expected):
        api, core, _ = kube
        core.errors["list_node"] = [api_error(status, "the API's own sentence")]
        with pytest.raises(expected, match="the API's own sentence"):
            await api.list_nodes()

    @pytest.mark.parametrize("error", [TimeoutError("read timed out"), ConnectionResetError("reset"), OSError("no route"), ValueError("not JSON")])
    async def test_a_dropped_or_garbled_answer_is_transient(self, kube, error):
        api, core, _ = kube
        core.errors["list_node"] = [error]
        with pytest.raises(KvClusterApiError, match=type(error).__name__):
            await api.list_nodes()

    async def test_an_error_body_is_bounded(self, kube):
        api, core, _ = kube
        core.errors["list_node"] = [api_error(500, "x" * 3_000_000)]
        with pytest.raises(KvClusterApiError) as raised:
            await api.list_nodes()
        assert len(str(raised.value)) < 700 and "more characters" in str(raised.value)

    async def test_a_create_that_already_exists_asks_for_another_draw(self, kube):
        api, _, custom = kube
        custom.errors["create"] = [api_error(409, 'virtualmachines.kubevirt.io "x" already exists', "AlreadyExists")]
        with pytest.raises(NameTaken, match="already exists"):
            await api.create_vm("inventory", {"metadata": {"name": "x"}})

    @pytest.mark.parametrize("status", [400, 403, 409, 422, 500])
    async def test_kubemacpools_refusal_is_recognised_at_any_status_and_first(self, kube, status):
        api, _, custom = kube
        custom.errors["create"] = [api_error(
            status, 'admission webhook "mutatevirtualmachines.kubemacpool.io" denied the request: Failed to create '
                    "virtual machine allocation error: Failed to allocate mac to the vm object: failed to allocate "
                    "requested mac address", "Invalid")]
        with pytest.raises(MacRefused, match="failed to allocate requested mac address"):
            await api.create_vm("inventory", {"metadata": {"name": "x"}})

    async def test_a_404_on_create_is_for_the_caller_to_tell_apart(self, kube):
        api, _, custom = kube
        custom.errors["create"] = [api_error(404, 'namespaces "inventory" not found', "NotFound")]
        with pytest.raises(NotFound):
            await api.create_vm("inventory", {"metadata": {"name": "x"}})

    async def test_an_invalid_vm_is_rejected_with_the_webhooks_words(self, kube):
        api, _, custom = kube
        custom.errors["create"] = [api_error(422, "spec.template.spec.domain.devices.interfaces[0].name: must match a network", "Invalid")]
        with pytest.raises(VmRequestRejectedError, match="must match a network"):
            await api.create_vm("inventory", {"metadata": {"name": "x"}})

    async def test_a_conflict_that_is_not_already_exists_is_transient(self, kube):
        api, _, custom = kube
        custom.errors["create"] = [api_error(409, "the object has been modified", "Conflict")]
        with pytest.raises(KvClusterApiError):
            await api.create_vm("inventory", {"metadata": {"name": "x"}})


class TestKubevirtApiWrites:
    async def test_a_create_goes_to_the_namespace_with_a_timeout(self, kube):
        api, _, custom = kube
        body = {"metadata": {"name": "vm-a"}, "spec": {}}
        assert await api.create_vm("inventory", body) == body
        name, args, kwargs = custom.calls[0]
        assert (name, args) == ("create", ("kubevirt.io", "v1", "inventory", "virtualmachines"))
        assert kwargs["_request_timeout"] == kubevirt_api.REQUEST_TIMEOUT

    async def test_a_delete_carries_the_uid_it_read_as_a_precondition(self, kube):
        api, _, custom = kube
        custom.objects[("virtualmachines", "inventory", "vm-a")] = {}
        assert await api.delete_vm("inventory", "vm-a", "uid-1") is True
        body = custom.calls[0][2]["body"]
        assert body.preconditions.uid == "uid-1"
        assert await api.delete_vm("inventory", "vm-a", "uid-1") is False  # already gone

    async def test_a_failed_uid_precondition_means_the_vm_meant_is_gone(self, kube):
        api, _, custom = kube
        custom.errors["delete"] = [api_error(409, "Precondition failed: UID in precondition: a, UID in object meta: b", "Conflict")]
        assert await api.delete_vm("inventory", "vm-a", "uid-1") is False

    async def test_a_datavolume_delete_is_idempotent(self, kube):
        api, _, custom = kube
        custom.objects[("datavolumes", "inventory", "vm-a-root")] = {}
        assert await api.delete_datavolume("inventory", "vm-a-root") is True
        assert await api.delete_datavolume("inventory", "vm-a-root") is False
        assert custom.calls[0][1][:2] == ("cdi.kubevirt.io", "v1beta1")

    async def test_a_forbidden_delete_is_permanent(self, kube):
        api, _, custom = kube
        custom.errors["delete"] = [api_error(403, "virtualmachines.kubevirt.io is forbidden")]
        with pytest.raises(KvClusterAccessError, match="ClusterRole"):
            await api.delete_vm("inventory", "vm-a", "uid-1")


# --- the thin activities -----------------------------------------------------------------------


@pytest.fixture
def bound(monkeypatch):
    """The real activities, bound to a fake cluster instead of the pod's own API."""
    kv = cluster()
    monkeypatch.setattr(limb, "_api", kv)
    monkeypatch.setattr(limb, "_cluster", kv.values)
    monkeypatch.setattr(vm_operations, "READY_POLL_SECONDS", 0.001)  # the wait really sleeps between polls
    return kv


class TestTheActivities:
    def test_the_worker_is_bound_to_its_own_cluster_from_its_settings(self):
        assert limb._cluster.kv_cluster == "ocp4-prod-kv-five-a" and limb._cluster.site == "five"
        assert limb._cluster.mac_prefix == "0e:03:f2" and limb._cluster.storage_class_name == "test-cephfs"
        assert limb._cluster.redfish_label == ("redfish-enabled", "true")
        assert isinstance(limb._api, KubevirtApi)

    async def test_figures_create_wait_and_delete_end_to_end(self, bound):
        env = ActivityEnvironment()
        figures = await env.run(limb.read_kv_cluster_figures, KvFiguresRequest(kv_cluster=KV))
        assert figures.readable and figures.stopped_vms == 0
        created = await env.run(limb.create_vm_on_cluster, create_request(create_before=datetime.now(UTC) + timedelta(minutes=4)))
        assert created.created and created.mac.startswith("0e:03:f2:")
        beats: list = []
        env.on_heartbeat = lambda *details: beats.append(details)
        ready = await env.run(limb.await_vm_ready, VmReadyRequest(kv_cluster=KV, namespace="inventory", name=created.name))
        assert ready.result == "done" and beats and beats[0][0]["polls"] == 1
        deleted = await env.run(limb.delete_vm_on_cluster, delete_request(name=created.name, mac=created.mac))
        assert deleted.deleted and bound.vms == {}

    async def test_the_create_uses_the_wall_clock_for_the_lease(self, bound):
        env = ActivityEnvironment()
        with pytest.raises(VmLeaseExpiredError):
            await env.run(limb.create_vm_on_cluster, create_request(create_before=datetime.now(UTC) - timedelta(seconds=1)))

    async def test_the_macs_are_drawn_from_the_systems_randomness(self, bound):
        env = ActivityEnvironment()
        macs = set()
        for index in range(20):
            request = create_request(request_id=f"run-{index}", create_before=datetime.now(UTC) + timedelta(minutes=4))
            macs.add((await env.run(limb.create_vm_on_cluster, request)).mac)
        assert len(macs) == 20

    async def test_the_wait_resumes_its_clock_from_the_heartbeat(self, bound, monkeypatch):
        bound.storage = "wffc"
        env = ActivityEnvironment()
        created = await env.run(limb.create_vm_on_cluster, create_request(create_before=datetime.now(UTC) + timedelta(minutes=4)))
        monkeypatch.setattr(vm_operations, "READY_POLL_SECONDS", 0.001)
        # The attempt before this one started eleven minutes ago: the deadline is already past.
        env.info = env.info.__class__(**{**env.info.__dict__, "heartbeat_details": [
            {"started_at": (datetime.now(UTC) - timedelta(minutes=11)).isoformat(), "polls": 80}]})
        outcome = await env.run(limb.await_vm_ready, VmReadyRequest(kv_cluster=KV, namespace="inventory", name=created.name))
        assert (outcome.result, outcome.polls) == ("deadline", 81)

    async def test_an_unreadable_heartbeat_detail_starts_the_clock_afresh(self, bound):
        env = ActivityEnvironment()
        created = await env.run(limb.create_vm_on_cluster, create_request(create_before=datetime.now(UTC) + timedelta(minutes=4)))
        env.info = env.info.__class__(**{**env.info.__dict__, "heartbeat_details": [{"started_at": "not a time"}]})
        outcome = await env.run(limb.await_vm_ready, VmReadyRequest(kv_cluster=KV, namespace="inventory", name=created.name))
        assert outcome.result == "done" and outcome.polls == 2  # counted from zero, not from the detail

    def test_the_figures_bound_sits_under_the_workflows_timeouts(self):
        from workflow_domains.vm_lifecycle import create_vm

        assert vm_operations.FIGURES_TIMEOUT_SECONDS == kubevirt_api.REQUEST_TIMEOUT[1] == 20
        assert vm_operations.FIGURES_TIMEOUT_SECONDS < create_vm._FIGURES_TIMEOUT.total_seconds()
        # One figures attempt (pick-up + run) fits the bound of the whole call, and that fits a lease.
        attempt = create_vm._FIGURES_SCHEDULE_TO_START_TIMEOUT + create_vm._FIGURES_TIMEOUT
        assert attempt <= create_vm._FIGURES_SCHEDULE_TO_CLOSE_TIMEOUT
        # One call to the cluster's API, at its worst: connect + read.
        one_call = sum(kubevirt_api.REQUEST_TIMEOUT)
        # A create attempt starts no call past its budget, so it is over — whatever its last
        # call does — before Temporal may start the next attempt beside it…
        assert vm_operations.CREATE_BUDGET_SECONDS + one_call < create_vm._CREATE_TIMEOUT.total_seconds()
        # …and before a cancel's rollback stops waiting for it to settle.
        assert vm_operations.CREATE_BUDGET_SECONDS + one_call < create_vm._CANCEL_SETTLE.total_seconds()
        # The lease is checked right before the create call: that call ends inside the guard,
        # with room left for the two clusters' clocks to disagree.
        assert one_call + 15 <= create_vm._LEASE_GUARD.total_seconds()
        assert vm_operations.READY_POLL_SECONDS < create_vm._READY_HEARTBEAT_TIMEOUT.total_seconds()
        assert create_vm._READY_DEADLINE_MINUTES * 60 < create_vm._READY_TIMEOUT.total_seconds()


# --- settings ----------------------------------------------------------------------------------


def settings(**overrides) -> VmLifecycleActivitySettings:
    values = dict(kv_cluster="ocp4-prod-kv-five-a", vm_mac_prefix="0e:03:f2", vm_storage_class_name="cephfs")
    values.update(overrides)
    with mock.patch.dict(os.environ):
        os.environ.pop("VM_NETWORK_NAME", None)
        return VmLifecycleActivitySettings(**values)


class TestSettings:
    def test_the_site_is_read_from_the_clusters_name(self):
        assert settings().site == "five"
        assert settings(kv_cluster="ocp4-prep-kv-bat-yam-c").site == "bat-yam"

    def test_the_name_is_never_canonicalised(self):
        with pytest.raises(ValidationError):
            settings(kv_cluster="prod-kv-five-a")  # no `ocp4-`: it must equal the day1 file name as it is
        assert settings(kv_cluster="ocp4-prod-kv-five-a").kv_cluster == "ocp4-prod-kv-five-a"

    @pytest.mark.parametrize("name", ["ocp4-prod-mce-five-a", "ocp4-seed-five-1", "ocp4-prod-kv-five", "", "ocp4-prod-kv-five-A"])
    def test_only_a_kv_clusters_name_starts_the_worker(self, name):
        with pytest.raises(ValidationError):
            settings(kv_cluster=name)

    def test_the_three_required_keys_have_no_default(self, monkeypatch):
        for key in ("KV_CLUSTER", "VM_MAC_PREFIX", "VM_STORAGE_CLASS_NAME"):
            monkeypatch.delenv(key)
        with pytest.raises(ValidationError) as raised:
            VmLifecycleActivitySettings()
        assert {error["loc"][0] for error in raised.value.errors()} == {
            "kv_cluster", "vm_mac_prefix", "vm_storage_class_name",
        }

    @pytest.mark.parametrize(("prefix", "stored"), [("0e:03:f2", "0e:03:f2"), ("0E:00:02", "0e:00:02"), (" 0e:0f:ff ", "0e:0f:ff")])
    def test_the_prefix_is_three_octets_behind_0e(self, prefix, stored):
        assert settings(vm_mac_prefix=prefix).vm_mac_prefix == stored

    @pytest.mark.parametrize(
        "prefix", ["02:03:f2", "0a:03:f2", "0f:03:f2", "0e:03", "0e:03:f2:a1", "0e03f2", "0e-03-f2", "0e:03:fz", "", "0e:3:f2"],
    )
    def test_any_other_prefix_stops_the_worker(self, prefix):
        with pytest.raises(ValidationError):
            settings(vm_mac_prefix=prefix)

    def test_the_vm_values_are_parsed_from_the_environment(self, monkeypatch):
        monkeypatch.setenv("VM_NETWORK_NAME", "inventory/vlan1010")
        monkeypatch.setenv("VM_NODE_SELECTOR", '{"node-role.kubernetes.io/worker": ""}')
        monkeypatch.setenv("VM_REDFISH_LABEL", "redfish-enabled=true")
        monkeypatch.setenv("VM_ROOT_DISK_SIZE", "2Gi")
        parsed = VmLifecycleActivitySettings()
        assert parsed.vm_network_name == "inventory/vlan1010"
        assert parsed.vm_node_selector == {"node-role.kubernetes.io/worker": ""}
        assert parsed.redfish_label == ("redfish-enabled", "true") and parsed.vm_root_disk_size == "2Gi"

    def test_the_defaults_are_the_platforms_vm(self):
        parsed = settings()
        assert parsed.vm_network_name == "openshift-cnv/vm-external-network"  # the platform's vm.yaml
        assert parsed.vm_node_selector == {} and parsed.vm_root_disk_size == "120Gi"
        assert parsed.redfish_label == ("redfish-enabled", "true")

    @pytest.mark.parametrize(
        ("key", "value"),
        [("vm_network_name", ""), ("vm_network_name", "vm-external-network"), ("vm_network_name", "a/b/c"),
         ("vm_network_name", "Openshift-CNV/net"), ("vm_redfish_label", "redfish-enabled"), ("vm_redfish_label", "a=b,c=d"),
         ("vm_root_disk_size", "120"), ("vm_root_disk_size", "0Gi"), ("vm_root_disk_size", "big"), ("vm_storage_class_name", "  ")],
    )
    def test_a_value_that_could_not_build_a_vm_stops_the_worker(self, key, value):
        with pytest.raises(ValidationError):
            settings(**{key: value})

    def test_the_chart_renders_vm_yamls_network_by_default(self):
        """vm.yaml is the target: with no value the chart renders its network, and a cluster
        only ever names ANOTHER NetworkAttachmentDefinition — never another network type."""
        import pathlib
        import shutil
        import subprocess

        if shutil.which("helm") is None:
            pytest.skip("needs helm")
        chart = pathlib.Path(__file__).resolve().parent.parent / "deploy" / "charts" / "vm-lifecycle-worker"
        base = ["helm", "template", "x", str(chart), "--set", "kvCluster=ocp4-prod-kv-five-a", "--set",
                "config.vmMacPrefix=0e:03:f2", "--set", "vm.storageClassName=basic", "--set", "temporal.host=t:7233"]
        default = subprocess.run(base, capture_output=True, text=True)
        assert default.returncode == 0 and 'VM_NETWORK_NAME: "openshift-cnv/vm-external-network"' in default.stdout
        other = subprocess.run([*base, "--set", "vm.networkName=inventory/vlan1010"], capture_output=True, text=True)
        assert other.returncode == 0 and 'VM_NETWORK_NAME: "inventory/vlan1010"' in other.stdout
        bare = subprocess.run([*base, "--set", "vm.networkName=vm-external-network"], capture_output=True, text=True)
        assert bare.returncode != 0 and "<namespace>/<name>" in bare.stderr

    def test_the_buffer_is_not_this_workers(self):
        assert "headroom_buffer_percent" not in VmLifecycleActivitySettings.model_fields
        assert "day1_repo_url" not in VmLifecycleActivitySettings.model_fields
