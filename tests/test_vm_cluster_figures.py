"""One KV cluster's figures from its own API's lists (activities/vm_lifecycle/cluster_figures.py):
pure arithmetic over fake API objects. docs/design/create-vm-placement.md §6.2-6.3."""

from __future__ import annotations

import pytest

from activities.vm_lifecycle import cluster_figures
from activities.vm_lifecycle.cluster_figures import PodUsage, compute, millicores, pod_request, pod_usage, to_bytes
from tests.kv_fleet import GIB, REDFISH_LABEL, node, pod, stopped_vm

MIB = 1024**2


def _figures(nodes, pods=(), vms=(), vmis=(), **kwargs):
    usages = [usage for usage in (pod_usage(p) for p in pods) if usage is not None]
    return compute(kv_cluster="ocp4-prod-kv-five-az1-a", nodes=list(nodes), pods=usages, vms=list(vms), vmis=list(vmis),
                   redfish_label=REDFISH_LABEL, **kwargs)


def _vmi(name, phase, namespace="inventory"):
    return {"metadata": {"name": name, "namespace": namespace}, "status": {"phase": phase}}


class TestQuantities:
    @pytest.mark.parametrize(
        ("quantity", "expected"),
        [("7", 7000), ("500m", 500), ("0.5", 500), ("1.5", 1500), (2, 2000), ("100m", 100), ("1", 1000), (None, 0), ("", 0),
         ("1500u", 2)],  # rounded UP, as the scheduler's MilliValue does
    )
    def test_cpu_becomes_integer_millicores(self, quantity, expected):
        assert millicores(quantity) == expected and isinstance(millicores(quantity), int)

    @pytest.mark.parametrize(
        ("quantity", "expected"),
        [("128Gi", 128 * GIB), ("1000M", 10**9), ("64Mi", 64 * MIB), ("1G", 10**9), (1024, 1024), ("1.5Gi", 3 * GIB // 2),
         ("130735548Ki", 130735548 * 1024), (None, 0)],
    )
    def test_memory_becomes_integer_bytes(self, quantity, expected):
        assert to_bytes(quantity) == expected and isinstance(to_bytes(quantity), int)

    def test_nothing_leaves_as_a_quantity_string(self):
        figures = _figures([node("n1", "31500m", "130735548Ki")], [pod("p", "n1", "250m", "512Mi")])
        dumped = figures.model_dump()
        numbers = [value for key, value in dumped.items() if key.endswith(("_millicores", "_bytes"))]
        assert numbers and all(isinstance(value, int) for value in numbers)
        assert all(isinstance(h["cpu_millicores"], int) and isinstance(h["memory_bytes"], int) for h in dumped["node_headroom"])
        assert figures.allocatable_cpu_millicores == 31500 and figures.allocatable_memory_bytes == 130735548 * 1024


class TestUsableNodes:
    def test_only_ready_uncordoned_untainted_nodes_count(self):
        nodes = [
            node("ready", 64, 512),
            node("not-ready", 64, 512, ready=False),
            node("cordoned", 64, 512, cordoned=True),
            node("tainted", 64, 512, taints=[{"key": "dedicated", "value": "infra", "effect": "NoSchedule"}]),
            node("evicting", 64, 512, taints=[{"key": "node.kubernetes.io/unreachable", "effect": "NoExecute"}]),
            node("prefer", 64, 512, taints=[{"key": "soft", "effect": "PreferNoSchedule"}]),
        ]
        figures = _figures(nodes)
        assert (figures.nodes, figures.usable_nodes) == (6, 2)
        assert [h.name for h in figures.node_headroom] == ["prefer", "ready"]
        assert figures.allocatable_cpu_millicores == 128_000 and figures.allocatable_memory_bytes == 1024 * GIB

    def test_a_node_without_a_ready_condition_is_not_ready(self):
        bare = {"metadata": {"name": "n"}, "status": {"allocatable": {"cpu": "8", "memory": "8Gi"}}}
        assert _figures([bare]).usable_nodes == 0

    def test_the_vm_templates_node_selector_narrows_the_nodes_a_vm_could_go_to(self):
        nodes = [node("vm-1", 64, 512, labels={"role": "vm"}), node("vm-2", 64, 512, labels={"role": "vm", "x": "y"}),
                 node("other", 64, 512, labels={"role": "infra"}), node("bare", 64, 512)]
        figures = _figures(nodes, [pod("on-other", "other", "10", "10Gi"), pod("on-vm", "vm-1", "4", "8Gi")],
                           node_selector={"role": "vm"})
        assert [h.name for h in figures.node_headroom] == ["vm-1", "vm-2"]
        assert figures.pods_cpu_millicores == 4000  # the pod on a node no VM can go to is not this pool's load

    def test_a_cluster_with_no_usable_node_reports_zeroes_not_an_error(self):
        figures = _figures([node("n", 64, 512, cordoned=True)], [pod("p", "n", "4", "8Gi")])
        assert figures.readable and figures.usable_nodes == 0 and figures.node_headroom == []
        assert figures.allocatable_cpu_millicores == 0 and figures.pods_cpu_millicores == 0


class TestPodRequests:
    def test_a_pods_request_is_the_sum_of_its_containers(self):
        assert pod_request(pod("p", "n", None, None, containers=[("500m", "1Gi"), ("1500m", "512Mi"), (None, None)])) == (
            2000, GIB + 512 * MIB)

    def test_a_larger_ordinary_init_container_is_what_the_scheduler_counts(self):
        init = [{"name": "big", "resources": {"requests": {"cpu": "4", "memory": "1Gi"}}},
                {"name": "small", "resources": {"requests": {"cpu": "1", "memory": "8Gi"}}}]
        # CPU: the 4-core init beats the 2 cores of containers; memory: the 8 GiB init beats 2 GiB.
        assert pod_request(pod("p", "n", "2", "2Gi", init=init)) == (4000, 8 * GIB)

    def test_a_sidecar_init_container_runs_with_the_containers(self):
        init = [{"name": "mesh", "restartPolicy": "Always", "resources": {"requests": {"cpu": "500m", "memory": "256Mi"}}}]
        assert pod_request(pod("p", "n", "2", "2Gi", init=init)) == (2500, 2 * GIB + 256 * MIB)

    def test_an_ordinary_init_container_runs_beside_the_sidecars_started_before_it(self):
        init = [
            {"name": "mesh", "restartPolicy": "Always", "resources": {"requests": {"cpu": "1", "memory": "1Gi"}}},
            {"name": "migrate", "resources": {"requests": {"cpu": "4", "memory": "1Gi"}}},
        ]
        # During `migrate`: 4 + 1 = 5 cores; running: 2 + 1 = 3 cores. The peak counts.
        assert pod_request(pod("p", "n", "2", "2Gi", init=init)) == (5000, 3 * GIB)

    def test_the_pods_overhead_is_added(self):
        assert pod_request(pod("p", "n", "2", "2Gi", overhead={"cpu": "250m", "memory": "120Mi"})) == (2250, 2 * GIB + 120 * MIB)

    def test_a_pod_level_request_replaces_the_containers_sum(self):
        with_pod_level = pod("p", "n", "2", "2Gi")
        with_pod_level["spec"]["resources"] = {"requests": {"cpu": "3"}}
        assert pod_request(with_pod_level) == (3000, 2 * GIB)  # memory still from the containers

    def test_a_pod_with_no_requests_counts_for_nothing(self):
        assert pod_request(pod("p", "n", None, None)) == (0, 0)
        assert pod_request({"metadata": {"name": "odd"}}) == (0, 0)

    def test_finished_pods_are_left_out_even_if_the_api_listed_them(self):
        assert pod_usage(pod("done", "n", "4", "8Gi", phase="Succeeded")) is None
        assert pod_usage(pod("failed", "n", "4", "8Gi", phase="Failed")) is None
        assert pod_usage(pod("pending", None, "4", "8Gi", phase="Pending")) == PodUsage(None, 4000, 8 * GIB)

    def test_a_pod_bound_to_no_node_or_to_an_unusable_one_is_on_no_usable_node(self):
        nodes = [node("n1", 64, 512), node("n2", 64, 512, cordoned=True)]
        pods = [pod("unscheduled", None, "8", "64Gi", phase="Pending"), pod("on-cordoned", "n2", "8", "64Gi"),
                pod("gone-node", "n9", "8", "64Gi"), pod("counted", "n1", "8", "64Gi")]
        figures = _figures(nodes, pods)
        assert figures.pods_cpu_millicores == 8000 and figures.pods_memory_bytes == 64 * GIB

    def test_headroom_is_per_node_as_a_pair(self):
        nodes = [node("n1", 64, 512), node("n2", 64, 512)]
        pods = [pod("a", "n1", "60", "100Gi"), pod("b", "n2", "10", "500Gi"), pod("c", "n2", "4", "2Gi")]
        figures = _figures(nodes, pods)
        assert [(h.name, h.cpu_millicores, h.memory_bytes) for h in figures.node_headroom] == [
            ("n1", 4000, 412 * GIB), ("n2", 50_000, 10 * GIB)]
        assert figures.pods_cpu_millicores == 74_000 and figures.pods_memory_bytes == 602 * GIB

    def test_an_overcommitted_node_has_negative_headroom(self):
        figures = _figures([node("n1", 8, 16)], [pod("a", "n1", "10", "20Gi")])
        assert (figures.node_headroom[0].cpu_millicores, figures.node_headroom[0].memory_bytes) == (-2000, -4 * GIB)


class TestStoppedVms:
    def test_a_stopped_labelled_vm_is_counted_at_what_its_pod_will_request(self):
        figures = _figures([node("n1", 128, 1024)], vms=[stopped_vm("vm-a", 32, 240)])
        # 32 vCPUs -> 30 cores; 240 GiB guest -> 240 GiB + 300 + 8 × 32 + 240 × 1024 / 512 MiB.
        assert (figures.stopped_vms, figures.stopped_vms_cpu_millicores, figures.stopped_vms_memory_bytes) == (
            1, 30_000, 240 * GIB + 1036 * MIB)
        # …and it is on NO node: the node's headroom is untouched.
        assert figures.node_headroom[0].cpu_millicores == 128_000

    def test_any_namespace_and_any_run_strategy_count(self):
        vms = [stopped_vm("a", 8, 60, namespace="inventory"), stopped_vm("b", 8, 60, namespace="server-scan-vms"),
               stopped_vm("c", 8, 60, run_strategy="Manual"), stopped_vm("d", 8, 60, run_strategy="Always"),
               stopped_vm("e", 8, 60, run_strategy="RerunOnFailure")]
        figures = _figures([node("n1", 128, 1024)], vms=vms)
        assert figures.stopped_vms == 5 and figures.stopped_vms_cpu_millicores == 5 * 7000

    def test_a_vm_without_the_redfish_label_is_not_ours_to_count(self):
        vms = [stopped_vm("ours", 8, 60), stopped_vm("theirs", 8, 60, labels={}),
               stopped_vm("off", 8, 60, labels={"redfish-enabled": "false"})]
        assert _figures([node("n1", 128, 1024)], vms=vms).stopped_vms == 1

    @pytest.mark.parametrize(
        ("phase", "counted_as_stopped"),
        [("Running", False), ("Scheduled", False), ("Unknown", False),
         ("Pending", True), ("Scheduling", True), ("Succeeded", True), ("Failed", True), (None, True)],
    )
    def test_a_vm_with_a_launcher_pod_on_a_node_is_counted_once_through_its_pod(self, phase, counted_as_stopped):
        vms = [stopped_vm("vm-a", 32, 240)]
        vmis = [_vmi("vm-a", phase)] if phase else []
        figures = _figures([node("n1", 128, 1024)], vms=vms, vmis=vmis)
        assert figures.stopped_vms == (1 if counted_as_stopped else 0)

    def test_a_running_vm_is_not_counted_twice(self):
        # Its launcher pod is in the pod sum; the VM itself must not be added again.
        launcher = pod("virt-launcher-vm-a", "n1", "30", "246789Mi", namespace="inventory")
        figures = _figures([node("n1", 128, 1024)], [launcher], [stopped_vm("vm-a", 32, 240)], [_vmi("vm-a", "Running")])
        assert figures.stopped_vms == 0 and figures.pods_cpu_millicores == 30_000
        assert figures.pods_cpu_millicores + figures.stopped_vms_cpu_millicores == 30_000

    def test_a_vmi_of_the_same_name_in_another_namespace_is_another_vm(self):
        figures = _figures([node("n1", 128, 1024)], vms=[stopped_vm("vm-a", 8, 60, namespace="inventory")],
                           vmis=[_vmi("vm-a", "Running", namespace="elsewhere")])
        assert figures.stopped_vms == 1

    def test_vcpus_are_cores_times_sockets_times_threads(self):
        vm = stopped_vm("vm-a", 4, 64)
        vm["spec"]["template"]["spec"]["domain"]["cpu"].update({"sockets": 2, "threads": 2})
        assert cluster_figures.vcpus(vm) == 16
        # The overhead follows the VM's vCPUs (16), not its cores (4): 300 + 8 × 16 + 64 × 2 MiB.
        assert cluster_figures.stopped_vm_request(vm) == (15_000, 64 * GIB + 556 * MIB)

    def test_guest_memory_falls_back_to_the_memory_request(self):
        vm = stopped_vm("vm-a", 8, 60)
        del vm["spec"]["template"]["spec"]["domain"]["memory"]
        vm["spec"]["template"]["spec"]["domain"]["resources"] = {"requests": {"memory": "32Gi"}}
        assert cluster_figures.stopped_vm_request(vm) == (7000, 32 * GIB + (300 + 64 + 64) * MIB)

    def test_a_vm_that_states_no_size_still_counts_a_core_and_the_overhead(self):
        bare = {"metadata": {"name": "odd", "namespace": "inventory", "labels": {"redfish-enabled": "true"}}, "spec": {}}
        figures = _figures([node("n1", 128, 1024)], vms=[bare])
        # One vCPU and no guest memory: a core, and the overhead's fixed and per-vCPU parts.
        assert (figures.stopped_vms, figures.stopped_vms_cpu_millicores, figures.stopped_vms_memory_bytes) == (
            1, 1000, 308 * MIB)


class TestTheWholePicture:
    def test_a_cluster_of_three_nodes(self):
        nodes = [node("n1", 64, 512), node("n2", 64, 512), node("n3", 64, 512, cordoned=True)]
        pods = [pod("virt-launcher-run", "n1", "30", "241Gi", namespace="inventory"), pod("dns", "n2", "100m", "70Mi"),
                pod("on-cordoned", "n3", "20", "100Gi"), pod("job", "n1", "8", "8Gi", phase="Succeeded")]
        vms = [stopped_vm("run", 32, 240), stopped_vm("halted-1", 32, 240), stopped_vm("halted-2", 8, 60)]
        figures = _figures(nodes, pods, vms, [_vmi("run", "Running")])
        assert figures.model_dump(exclude={"node_headroom"}) == {
            "kv_cluster": "ocp4-prod-kv-five-az1-a", "readable": True, "detail": None, "nodes": 3, "usable_nodes": 2,
            "allocatable_cpu_millicores": 128_000, "allocatable_memory_bytes": 1024 * GIB,
            "pods_cpu_millicores": 30_100, "pods_memory_bytes": 241 * GIB + 70 * MIB,
            "stopped_vms": 2, "stopped_vms_cpu_millicores": 37_000,
            "stopped_vms_memory_bytes": (240 + 60) * GIB + (1036 + 484) * MIB,
        }
