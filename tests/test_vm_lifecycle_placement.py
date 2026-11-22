"""The fit rule create-vm decides with (workflow_domains/vm_lifecycle/placement.py): pure, on
figures a KV cluster's worker would report. docs/design/create-vm-placement.md §6.4."""

from __future__ import annotations

import ast
import pathlib

import pytest

from shared.models.vm_lifecycle import KvClusterFigures, KvClusterRow, KvNodeHeadroom
from shared.vm_requests import GIB, pod_cpu_millicores, pod_memory_bytes, request_cores
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


KV32 = (pod_cpu_millicores(32), pod_memory_bytes(256))   # 30 cores, 257 GiB
KV8 = (pod_cpu_millicores(8), pod_memory_bytes(64))      # 7 cores, 65 GiB


class TestTheVmInRequestTerms:
    @pytest.mark.parametrize(
        ("cpu", "memory_gib", "cores", "request_gib"),
        [(8, 64, 7, 65), (16, 128, 15, 129), (32, 256, 30, 257), (64, 512, 60, 513)],
    )
    def test_the_designs_table(self, cpu, memory_gib, cores, request_gib):
        assert pod_cpu_millicores(cpu) == cores * 1000
        assert pod_memory_bytes(memory_gib) == request_gib * GIB

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
        assert "30000 m" in detail and "257 GiB" in detail

    def test_one_node_with_both_is_enough(self):
        assert skip_reason(figures([(64, 100), (10, 600), (30, 257)]), *KV32, 0) is None

    def test_an_exact_fit_fits(self):
        assert skip_reason(figures([(30, 257)]), *KV32, 0) is None
        assert skip_reason(figures([(30, 256)]), *KV32, 0) == "fit-node"

    def test_memory_runs_out_before_cpu_on_an_eight_gib_per_core_node(self):
        # The design's example: a 64-core / 512 GiB node holds one 32-vCPU VM on memory, two on CPU.
        one_running = figures([(64 - 30, 512 - 257)], pods=(30, 257))
        assert skip_reason(one_running, *KV32, 0) == "fit-node"
        assert "GiB free" in judge(one_running, *KV32, 0).detail

    def test_an_overcommitted_node_has_negative_room_and_is_no_room(self):
        assert skip_reason(figures([(-5, -10)], allocatable=(100, 1000)), *KV8, 0) == "fit-node"

    def test_stopped_vms_are_on_no_node_so_the_node_check_does_not_see_them(self):
        # The design's known limit (§11.6): the node check passes, only the buffer counts them.
        assert skip_reason(figures([(64, 600)], stopped=(1, 30, 257)), *KV32, 0) is None


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
        answer = figures([(100, 10_000)], stopped=(2, 60, 514))
        verdict = judge(answer, *KV32, 15)
        assert verdict.reason == "fit-buffer"
        assert "90000 m of 100000 m" in verdict.detail and "15% buffer allows 85%" in verdict.detail

    def test_memory_is_checked_as_well_as_cpu(self):
        # Plenty of CPU; 1000 GiB, 600 requested, + 257 = 857 > 850.
        answer = figures([(1000, 400)], pods=(0, 600))
        verdict = judge(answer, *KV32, 15)
        assert verdict.reason == "fit-buffer" and "memory after placing: 857 GiB of 1000 GiB" in verdict.detail
        assert skip_reason(figures([(1000, 407)], pods=(0, 593)), *KV32, 15) is None  # 593 + 257 = 850 of 1000

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
        assert skip_reason(figures([(30, 257), (100, 1000), (100, 1000)]), *KV32, 15) is None


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
                    placement.FIT_NODE, placement.FIT_BUFFER}
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
