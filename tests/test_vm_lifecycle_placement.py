"""The fit rule create-vm decides with (workflow_domains/vm_lifecycle/placement.py): pure, on
figures a KV cluster's worker would report. docs/design/create-vm-placement.md §6.4."""

from __future__ import annotations

import ast
import pathlib

import pytest

from shared.models.vm_lifecycle import KvClusterFigures, KvClusterRow, KvNodeHeadroom
from shared.infraenv_names import vm_memory_gib
from shared.vm_requests import (
    GIB,
    MIB,
    gib_free,
    gib_needed,
    launcher_overhead_bytes,
    pod_cpu_millicores,
    pod_memory_bytes,
    pod_memory_bytes_of_guest,
    request_cores,
)
from workflow_domains.vm_lifecycle import placement
from workflow_domains.vm_lifecycle.placement import SKIP_REASONS, judge, skip_reason, summarise_skips


def figures(
    nodes: list[tuple[int, int]],
    *,
    pods: tuple[int, int] = (0, 0),
    stopped: tuple[int, int, int] = (0, 0, 0),
    allocatable: tuple[int, int] | None = None,
    readable: bool = True,
    name: str = "ocp4-prod-kv-five-az1-a",
) -> KvClusterFigures:
    """Figures from node HEADROOM pairs (cores, GiB). `allocatable` defaults to headroom + pods."""
    headroom = [KvNodeHeadroom(name=f"n{i}", cpu_millicores=c * 1000, memory_bytes=m * GIB) for i, (c, m) in enumerate(nodes)]
    alloc_cpu = sum(c for c, _ in nodes) * 1000 + pods[0] * 1000 if allocatable is None else allocatable[0] * 1000
    alloc_mem = sum(m for _, m in nodes) * GIB + pods[1] * GIB if allocatable is None else allocatable[1] * GIB
    return KvClusterFigures(
        kv_cluster=name, readable=readable, nodes=len(nodes), usable_nodes=len(nodes),
        allocatable_cpu_millicores=alloc_cpu, allocatable_memory_bytes=alloc_mem,
        pods_cpu_millicores=pods[0] * 1000, pods_memory_bytes=pods[1] * GIB,
        stopped_vms=stopped[0], stopped_vms_cpu_millicores=stopped[1] * 1000, stopped_vms_memory_bytes=stopped[2] * GIB,
        node_headroom=headroom,
    )


KV32 = (pod_cpu_millicores(32), pod_memory_bytes(32, 240))   # 30 cores, 240 GiB + 1,036 MiB
KV8 = (pod_cpu_millicores(8), pod_memory_bytes(8, 60))       # 7 cores, 60 GiB + 484 MiB
KV32_MEMORY = KV32[1]


def exact(nodes: list[tuple[int, int]], **changes: int) -> KvClusterFigures:
    """Figures from node headroom pairs in MILLICORES and BYTES, for a boundary at the byte."""
    headroom = [KvNodeHeadroom(name=f"n{i}", cpu_millicores=c, memory_bytes=m) for i, (c, m) in enumerate(nodes)]
    return KvClusterFigures(
        kv_cluster="ocp4-prod-kv-five-az1-a", readable=True, nodes=len(nodes), usable_nodes=len(nodes),
        allocatable_cpu_millicores=sum(c for c, _ in nodes), allocatable_memory_bytes=sum(m for _, m in nodes),
        node_headroom=headroom,
    ).model_copy(update=changes)


class TestTheVmInRequestTerms:
    @pytest.mark.parametrize(
        ("cpu", "memory_gib", "cores", "overhead_mib"),
        [(8, 60, 7, 484), (16, 120, 15, 668), (32, 240, 30, 1036), (64, 480, 60, 1772)],
    )
    def test_the_designs_table(self, cpu, memory_gib, cores, overhead_mib):
        assert vm_memory_gib(cpu) == memory_gib
        assert pod_cpu_millicores(cpu) == cores * 1000
        assert pod_memory_bytes(cpu, memory_gib) == memory_gib * GIB + overhead_mib * MIB

    @pytest.mark.parametrize(
        ("cpu", "guest_gib", "measured_overhead_mib"),
        [(8, 62, 481), (16, 124, 669), (32, 248, 1045), (64, 496, 1797)],
    )
    def test_the_overhead_is_never_under_what_a_launcher_pod_requested_and_at_most_10_mib_over(
        self, cpu, guest_gib, measured_overhead_mib
    ):
        """The four pods measured on cluster-qcl8n (2026-11-22, VMs of 7.75 GiB per vCPU built by
        vm_spec.py): what each launcher pod requested over its guest. Under it, placement would
        approve a VM whose pod has no node; far over it, room is wasted."""
        formula_mib = launcher_overhead_bytes(cpu, guest_gib * GIB) // MIB
        assert measured_overhead_mib <= formula_mib <= measured_overhead_mib + 10

    def test_the_flat_gib_it_replaced_was_too_little_from_32_vcpus_up(self):
        assert launcher_overhead_bytes(16, 120 * GIB) < GIB < launcher_overhead_bytes(32, 240 * GIB)

    def test_a_vm_read_from_a_cluster_is_sized_by_the_same_rule(self):
        assert pod_memory_bytes_of_guest(32, 240 * GIB) == pod_memory_bytes(32, 240)
        # A guest that is not a whole number of GiB (someone else's VM): the page tables' share is floored.
        assert pod_memory_bytes_of_guest(2, 1000) == 1000 + 300 * MIB + 16 * MIB + 1

    def test_a_need_never_reads_smaller_than_the_room_it_was_refused(self):
        assert (gib_needed(KV32_MEMORY), gib_free(KV32_MEMORY - 1)) == ("241.02 GiB", "241.01 GiB")
        assert (gib_needed(240 * GIB), gib_free(240 * GIB)) == ("240.00 GiB", "240.00 GiB")
        assert (gib_needed(240 * GIB + 1), gib_free(240 * GIB - 1)) == ("240.01 GiB", "239.99 GiB")
        assert gib_free(-10 * GIB - GIB // 2) == "-10.50 GiB"

    @pytest.mark.parametrize(("cpu", "cores"), [(1, 1), (2, 1), (3, 2), (4, 3), (15, 14), (16, 15), (17, 15), (256, 240)])
    def test_the_cpu_request_is_fifteen_sixteenths_rounded_down_and_never_zero(self, cpu, cores):
        assert request_cores(cpu) == cores


class TestTheFitRule:
    def test_an_empty_cluster_takes_the_vm(self):
        verdict = judge(figures([(100, 1000)]), *KV32, 15)
        assert verdict.fits and verdict.reason is None
        assert verdict.cpu_fill_after == pytest.approx(0.30)

    def test_an_unreadable_cluster_is_unreachable_whatever_its_numbers(self):
        answer = figures([(100, 1000)], readable=False).model_copy(update={"detail": "its API timed out"})
        verdict = judge(answer, *KV32, 15)
        assert (verdict.reason, verdict.detail) == ("unreachable", "its API timed out")

    def test_no_usable_node_is_no_nodes(self):
        empty = KvClusterFigures(kv_cluster="x", readable=True, nodes=3, usable_nodes=0)
        assert skip_reason(empty, *KV32, 15) == "no-nodes"
        assert "3 nodes" in judge(empty, *KV32, 15).detail

    def test_the_order_of_the_checks_is_the_designs(self):
        # Unreadable beats everything; no node beats the fit; the node beats the buffer.
        unreadable_and_empty = KvClusterFigures(kv_cluster="x", readable=False, nodes=0, usable_nodes=0)
        assert skip_reason(unreadable_and_empty, *KV32, 15) == "unreachable"
        # One 20-core node, half full: no node holds 30 cores AND the buffer is passed.
        assert skip_reason(figures([(10, 500)], pods=(10, 0)), *KV32, 15) == "fit-node"


class TestOneNodeMustHoldIt:
    def test_room_spread_over_nodes_is_not_room(self):
        # 200 free cores over ten nodes of 20: no 30-core pod fits anywhere.
        assert skip_reason(figures([(20, 1000)] * 10), *KV32, 0) == "fit-node"

    def test_cpu_and_memory_must_be_on_the_same_node(self):
        # The largest free CPU and the largest free memory are on two different nodes.
        answer = figures([(64, 100), (10, 600)])
        assert skip_reason(answer, *KV32, 0) == "fit-node"
        detail = judge(answer, *KV32, 0).detail
        assert "30000 m" in detail and "241.02 GiB together" in detail
        assert "n0, has 64000 m / 100.00 GiB free" in detail

    def test_one_node_with_both_is_enough(self):
        assert skip_reason(figures([(64, 100), (10, 600), (30, 242)]), *KV32, 0) is None

    def test_an_exact_fit_fits_and_one_byte_or_one_millicore_less_does_not(self):
        assert skip_reason(exact([(30_000, KV32_MEMORY)]), *KV32, 0) is None
        assert skip_reason(exact([(30_000, KV32_MEMORY - 1)]), *KV32, 0) == "fit-node"
        assert skip_reason(exact([(29_999, KV32_MEMORY)]), *KV32, 0) == "fit-node"
        # A whole GiB over the guest is no longer the VM's size: 241 GiB is 20 MiB short of it.
        assert skip_reason(figures([(30, 241)]), *KV32, 0) == "fit-node"

    def test_two_vms_fill_the_cpus_of_an_eight_gib_per_core_node_at_seven_and_a_half_per_vcpu(self):
        """Why the ratio is 7.5 (design §6.1.1): a 64-core / 512 GiB node holds two 32-vCPU VMs
        on CPU, and now on memory too. At 8 GiB per vCPU the second never fitted on memory."""
        one_running = exact([(64_000 - 30_000, 512 * GIB - KV32_MEMORY)])
        assert skip_reason(one_running, *KV32, 0) is None
        at_eight = pod_memory_bytes(32, 256)
        assert skip_reason(exact([(64_000 - 30_000, 512 * GIB - at_eight)]), 30_000, at_eight, 0) == "fit-node"
        # A third has no CPU left on the node: 4 cores free.
        two_running = exact([(4_000, 512 * GIB - 2 * KV32_MEMORY)])
        assert skip_reason(two_running, *KV32, 0) == "fit-node"

    def test_memory_runs_out_first_on_a_node_with_less_memory_per_core(self):
        # 64 cores / 384 GiB (6 per core): one VM running, CPU for a second, memory for none.
        one_running = exact([(34_000, 384 * GIB - KV32_MEMORY)])
        verdict = judge(one_running, *KV32, 0)
        assert verdict.reason == "fit-node" and "142.98 GiB free" in verdict.detail

    def test_an_overcommitted_node_has_negative_room_and_is_no_room(self):
        assert skip_reason(figures([(-5, -10)], allocatable=(100, 1000)), *KV8, 0) == "fit-node"

    def test_stopped_vms_are_on_no_node_so_the_node_check_does_not_see_them(self):
        # The design's known limit (§11.6): the node check passes, only the buffer counts them.
        assert skip_reason(figures([(64, 600)], stopped=(1, 30, 242)), *KV32, 0) is None


class TestTheBuffer:
    @pytest.mark.parametrize(
        ("already", "fits"),
        [(0, True), (30, True), (55, True), (56, False), (60, False), (85, False)],
    )
    def test_cpu_stays_at_or_under_one_minus_the_buffer(self, already, fits):
        # 100 cores, 15% buffer: 85 cores of requests at most. 55 + 30 = 85 fits, 56 + 30 does not.
        answer = figures([(100 - already, 10_000)], pods=(already, 0))
        assert (skip_reason(answer, *KV32, 15) is None) is fits

    def test_stopped_vms_count_toward_the_buffer(self):
        # Two VMs created a moment ago, not started: 60 cores taken, the third would be 90 > 85.
        answer = figures([(100, 10_000)], stopped=(2, 60, 484))
        verdict = judge(answer, *KV32, 15)
        assert verdict.reason == "fit-buffer"
        assert "90000 m of 100000 m" in verdict.detail and "15% buffer allows 85%" in verdict.detail

    def test_memory_is_checked_as_well_as_cpu(self):
        # Plenty of CPU; 1000 GiB, 609 requested, + 241.02 = 850.02 > 850.
        answer = figures([(1000, 391)], pods=(0, 609))
        verdict = judge(answer, *KV32, 15)
        assert verdict.reason == "fit-buffer"
        assert "memory after placing: 850.02 GiB of 1000.00 GiB allocatable" in verdict.detail
        assert skip_reason(figures([(1000, 392)], pods=(0, 608)), *KV32, 15) is None  # 608 + 241.02 = 849.02

    def test_the_memory_boundary_is_exact_to_the_byte(self):
        # 1000 GiB allocatable, 15%: 850 GiB of requests at most, the VM's own included.
        room = exact([(1_000_000, 1000 * GIB)])
        at = room.model_copy(update={"pods_memory_bytes": 850 * GIB - KV32_MEMORY})
        over = room.model_copy(update={"pods_memory_bytes": 850 * GIB - KV32_MEMORY + 1})
        assert skip_reason(at, *KV32, 15) is None
        assert skip_reason(over, *KV32, 15) == "fit-buffer"

    def test_stopped_vms_and_pods_add_up_on_memory(self):
        # Neither alone passes the buffer; together with the VM they do.
        room = exact([(1_000_000, 1000 * GIB)])
        half = (850 * GIB - KV32_MEMORY) // 2 + 1
        assert skip_reason(room.model_copy(update={"pods_memory_bytes": half}), *KV32, 15) is None
        assert skip_reason(room.model_copy(update={"stopped_vms_memory_bytes": half}), *KV32, 15) is None
        both = room.model_copy(update={"pods_memory_bytes": half, "stopped_vms_memory_bytes": half})
        assert skip_reason(both, *KV32, 15) == "fit-buffer"

    def test_cpu_is_reported_before_memory_when_both_pass_the_buffer(self):
        both = exact([(100_000, 1000 * GIB)], pods_cpu_millicores=60_000, pods_memory_bytes=700 * GIB)
        both = both.model_copy(update={"node_headroom": [KvNodeHeadroom(name="n0", cpu_millicores=40_000, memory_bytes=300 * GIB)]})
        assert judge(both, *KV32, 15).detail.startswith("CPU after placing")

    def test_a_buffer_of_zero_lets_the_cluster_fill_and_ninety_leaves_a_tenth(self):
        assert skip_reason(exact([(30_000, KV32_MEMORY)]), *KV32, 0) is None
        assert skip_reason(exact([(300_000, 10 * KV32_MEMORY)]), *KV32, 90) is None
        assert skip_reason(exact([(299_999, 10 * KV32_MEMORY)]), *KV32, 90) == "fit-buffer"
        assert skip_reason(exact([(300_000, 10 * KV32_MEMORY - 1)]), *KV32, 90) == "fit-buffer"

    def test_a_cluster_with_nothing_allocatable_takes_nothing(self):
        # Usable nodes that report no allocatable (a broken kubelet): no division, no fit.
        broken = exact([(0, 0)])
        verdict = judge(broken, *KV8, 15)
        assert verdict.reason == "fit-node" and verdict.cpu_fill_after is None and verdict.memory_fill_after is None

    def test_the_boundary_is_exact_in_integers(self):
        # 3 millicores allocatable short of the boundary must not fit; floats would round this.
        base = figures([(100, 10_000)])
        at = base.model_copy(update={"allocatable_cpu_millicores": 100_000, "pods_cpu_millicores": 55_000})
        over = base.model_copy(update={"allocatable_cpu_millicores": 99_997, "pods_cpu_millicores": 55_000})
        assert skip_reason(at, *KV32, 15) is None
        assert skip_reason(over, *KV32, 15) == "fit-buffer"

    @pytest.mark.parametrize("buffer", [0, 15, 30, 50, 90])
    def test_the_buffer_is_a_share_of_the_whole_cluster(self, buffer):
        # 1000 cores over ten nodes; the VM takes 30: it fits while requested + 30 <= (100 - buffer)% of 1000.
        limit = (100 - buffer) * 10 - 30
        assert skip_reason(figures([(100, 10_000)] * 10, pods=(limit, 0), allocatable=(1000, 100_000)), *KV32, buffer) is None
        assert skip_reason(figures([(100, 10_000)] * 10, pods=(limit + 1, 0), allocatable=(1000, 100_000)), *KV32, buffer) == "fit-buffer"

    def test_a_node_may_fill_completely_the_buffer_is_never_per_node(self):
        # One node takes the VM to 100% of itself; the cluster as a whole stays under the buffer.
        assert skip_reason(exact([(30_000, KV32_MEMORY), (100_000, 1000 * GIB), (100_000, 1000 * GIB)]), *KV32, 15) is None


class TestTheDesignsExample:
    """§6.5: ten 32-vCPU VMs over two 100-core clusters with the 15% buffer."""

    def test_two_two_then_no_capacity(self):
        taken = {"a": 0, "b": 0}
        placed: list[str | None] = []
        for _ in range(10):
            chosen = None
            for name in ("a", "b"):  # oldest first, first fit
                answer = figures([(100, 100_000)], stopped=(taken[name] // 30, taken[name], 0))
                if skip_reason(answer, *KV32, 15) is None:
                    chosen = name
                    break
            if chosen:
                taken[chosen] += 30
            placed.append(chosen)
        assert placed == ["a", "a", "b", "b"] + [None] * 6
        assert taken == {"a": 60, "b": 60}


class TestTheTable:
    def test_a_row_carries_the_figures_and_the_verdict(self):
        answer = figures([(100, 1000)])
        row = placement.row("ocp4-prod-kv-five-az1-a", answer, judge(answer, *KV32, 15))
        assert row.fits and row.skip_reason is None and row.figures == answer and row.cpu_fill_after == pytest.approx(0.3)

    def test_the_summary_names_the_clusters_per_reason_in_the_tables_order(self):
        table = [
            KvClusterRow(name="-a", skip_reason="fit-buffer"),
            KvClusterRow(name="-b", skip_reason="no-worker"),
            KvClusterRow(name="-c", skip_reason="fit-buffer"),
            KvClusterRow(name="-d", fits=True),
        ]
        said = summarise_skips(table)
        assert said == f"-a, -c {SKIP_REASONS['fit-buffer']}; -b {SKIP_REASONS['no-worker']}"

    def test_every_reason_the_rule_or_the_run_records_has_its_words(self):
        """A reason without words would reach the operator as a bare key."""
        recorded = {placement.DAY1_PROBLEM, placement.NO_WORKER, placement.UNREACHABLE, placement.NO_NODES,
                    placement.FIT_NODE, placement.FIT_BUFFER, placement.CREATE_FAILED}
        assert recorded == set(SKIP_REASONS)
        # …and the workflow records no reason by a literal of its own.
        source = pathlib.Path(__file__).resolve().parent.parent / "workflow_domains/vm_lifecycle/create_vm.py"
        literals = {
            keyword.value.value
            for node in ast.walk(ast.parse(source.read_text()))
            if isinstance(node, ast.Call)
            for keyword in node.keywords
            if keyword.arg == "skip_reason" and isinstance(keyword.value, ast.Constant)
        }
        assert literals == set()


class TestTheDesignsNodeTable:
    """docs/design/create-vm-placement.md §6.1.1: how many VMs of each size ONE KV node takes,
    counted by the fit rule itself. Every KV node has 8 GiB of RAM per core; what a node gives
    VMs is less — the kernel keeps 1.62% of the RAM, day1's kubelet reserve is 15,100 Mi, and
    the node's own pods (OVN, virt-handler, CSI…) request 6,721 Mi (the operator's R660 nodes,
    measured 2026-11-24). The CPU side: 4 cores reserved, a VM requesting 15/16 of its vCPUs."""

    SIZES = (8, 16, 32, 64)

    @staticmethod
    def vms_one_node_takes(node_cpus: int, cpu: int, memory_gib: int) -> int:
        allocatable_cpu = (node_cpus - 4) * 1000
        allocatable_memory = node_cpus * 8 * GIB * 98_377 // 100_000 - 15_100 * MIB
        own_pods_memory = 6_721 * MIB
        vm = (pod_cpu_millicores(cpu), pod_memory_bytes(cpu, memory_gib))
        placed = 0
        while True:
            used_cpu, used_memory = placed * vm[0], own_pods_memory + placed * vm[1]
            node = exact([(allocatable_cpu - used_cpu, allocatable_memory - used_memory)]).model_copy(update={
                "allocatable_cpu_millicores": allocatable_cpu, "allocatable_memory_bytes": allocatable_memory,
                "pods_cpu_millicores": used_cpu, "pods_memory_bytes": used_memory,
            })
            if skip_reason(node, *vm, 0) is not None:
                return placed
            placed += 1

    @pytest.mark.parametrize(
        ("node_cpus", "per_size", "the_cpus_limit"),
        [(128, [16, 8, 4, 2], [17, 8, 4, 2]), (192, [24, 12, 6, 3], [26, 12, 6, 3])],
    )
    def test_at_seven_and_a_half_gib_per_vcpu_a_node_takes_what_its_cpus_allow(self, node_cpus, per_size, the_cpus_limit):
        assert [self.vms_one_node_takes(node_cpus, cpu, vm_memory_gib(cpu)) for cpu in self.SIZES] == per_size
        assert [(node_cpus - 4) * 1000 // pod_cpu_millicores(cpu) for cpu in self.SIZES] == the_cpus_limit
        # kv16, kv32 and kv64 reach the CPU's limit; kv8 stays under it at any ratio above about 7.1 (accepted).
        assert per_size[1:] == the_cpus_limit[1:]

    @pytest.mark.parametrize(("node_cpus", "per_size"), [(128, [15, 7, 3, 1]), (192, [23, 11, 5, 2])])
    def test_at_eight_gib_per_vcpu_every_size_was_one_vm_short(self, node_cpus, per_size):
        assert [self.vms_one_node_takes(node_cpus, cpu, cpu * 8) for cpu in self.SIZES] == per_size

    # Node sizes, in steps of 8 CPUs up to 256, where the memory stops one VM short of what the
    # CPUs allow at 7.5 GiB per vCPU — with this model's 4 reserved cores and fixed reserve. NOT
    # the node types in use (128 and 192 CPUs, above). A node type of one of these sizes, or of
    # any size never computed, needs the design's table recomputed BEFORE it joins a KV cluster.
    SHORT = {64: [16], 184: [16, 32, 64], 200: [16], 216: [16, 32], 232: [16]}

    @pytest.mark.parametrize("node_cpus", range(64, 257, 8))
    def test_where_the_ratio_reaches_the_cpus_limit_and_where_it_does_not(self, node_cpus):
        short = [
            cpu for cpu in (16, 32, 64)
            if self.vms_one_node_takes(node_cpus, cpu, vm_memory_gib(cpu)) < (node_cpus - 4) * 1000 // pod_cpu_millicores(cpu)
        ]
        assert short == self.SHORT.get(node_cpus, [])

    def test_the_smallest_shortfall_is_a_fifth_of_a_gib(self):
        # 64 CPUs / 512 GiB: four kv16 need 482.61 GiB and the node gives its VMs 482.38.
        assert (60_000 // pod_cpu_millicores(16), self.vms_one_node_takes(64, 16, 120)) == (4, 3)
