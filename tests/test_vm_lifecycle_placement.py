"""Where a VM goes (activities/vm_lifecycle/placement.py): the pod-request formulas, fit per
node, buffer per cluster, Halted reservations, best-fit on the dominant resource."""

from __future__ import annotations

from activities.vm_lifecycle import placement
from activities.vm_lifecycle.kv_capacity_api import KvProbeFailure
from activities.vm_lifecycle.placement import GIB, pod_cpu_millicores, pod_memory_bytes, rank
from shared.models.vm_lifecycle import (
    KvCapacityReport,
    KvClusterRef,
    KvHaltedReservation,
    KvNodeCapacity,
    KvVmIdentity,
)
from workflow_domains.vm_lifecycle.create_vm import _SKIP_SUMMARY


def _ref(n: int) -> KvClusterRef:
    return KvClusterRef(
        name=f"ocp4-kubevirt-s-a{n}", cluster_name=f"kubevirt-s-a{n}", hosted_cluster=f"openshift-kubevirt-s-a{n}",
        site="s", zone="a",
    )


def _node(name: str, *, cores: int, gib: int, used_cores: float = 0, used_gib: int = 0, schedulable: bool = True) -> KvNodeCapacity:
    return KvNodeCapacity(
        name=name, schedulable=schedulable, allocatable_cpu_millicores=cores * 1000, allocatable_memory_bytes=gib * GIB,
        requested_cpu_millicores=int(used_cores * 1000), requested_memory_bytes=used_gib * GIB,
    )


def _report(n: int, nodes: list[KvNodeCapacity], *, halted=(), vms=()) -> KvCapacityReport:
    return KvCapacityReport(
        cluster=f"kubevirt-s-a{n}", observed_at="2026-11-05T10:00:00Z", nodes=nodes,
        halted_reservations=list(halted), vms=list(vms),
    )


class TestTheFormulas:
    def test_the_cpu_request_is_vm_yamls_fifteen_sixteenths(self):
        # vm.yaml: `div (mul cpu 15) 16`, integer division.
        assert pod_cpu_millicores(32) == 30_000
        assert pod_cpu_millicores(64) == 60_000
        assert pod_cpu_millicores(8) == 7_000
        assert pod_cpu_millicores(1) == 1_000  # floored: 15/16 of one core would be zero

    def test_the_memory_request_is_the_guest_plus_the_launcher_pad(self):
        assert pod_memory_bytes(256) == 257 * GIB
        assert pod_memory_bytes(64) == 65 * GIB


class TestFitPerNodeBufferPerCluster:
    def test_free_cores_spread_over_nodes_do_not_hold_a_big_vm(self):
        # 10 nodes × 20 free cores = 200 free, and no node has 60.
        nodes = [_node(f"n{i}", cores=64, gib=512, used_cores=44, used_gib=100) for i in range(10)]
        table = rank([_ref(1)], [_report(1, nodes)], cpu=64, memory_gib=256, buffer_percent=0)
        row = table.ranked[0]
        assert table.chosen is None
        assert (row.fits, row.skip_reason) == (False, "fit-node")
        assert row.max_node_cpu_headroom_millicores == 20_000

    def test_the_buffer_is_checked_on_the_cluster_after_placing(self):
        # One empty 64-core / 512 GiB node: the pod (60 cores) fits the node,
        # but 60/64 = 94% is past a 30% buffer.
        table = rank([_ref(1)], [_report(1, [_node("n1", cores=64, gib=512)])], cpu=64, memory_gib=256, buffer_percent=30)
        row = table.ranked[0]
        assert (row.fits, row.skip_reason) == (False, "fit-buffer")
        assert row.cpu_fill_after == 60_000 / 64_000
        table = rank([_ref(1)], [_report(1, [_node("n1", cores=64, gib=512)])], cpu=64, memory_gib=256, buffer_percent=0)
        assert table.chosen == "ocp4-kubevirt-s-a1"

    def test_the_buffer_binds_on_memory_as_well(self):
        # Plenty of CPU, memory at the wall: 257 of 300 GiB after.
        table = rank([_ref(1)], [_report(1, [_node("n1", cores=256, gib=300)])], cpu=64, memory_gib=256, buffer_percent=30)
        row = table.ranked[0]
        assert (row.fits, row.skip_reason) == (False, "fit-buffer")
        assert "memory" in row.skip_detail

    def test_halted_reservations_count_toward_the_fill_but_not_the_node_headroom(self):
        nodes = [_node("n1", cores=64, gib=512)]
        halted = [KvHaltedReservation(namespace="inventory", name="ocp-x-1", cpu_millicores=30_000, memory_bytes=129 * GIB)]
        table = rank([_ref(1)], [_report(1, nodes, halted=halted)], cpu=32, memory_gib=128, buffer_percent=0)
        row = table.ranked[0]
        assert row.halted_reservations == 1
        assert row.cpu_fill_before == 30_000 / 64_000
        assert row.cpu_fill_after == 60_000 / 64_000
        assert row.max_node_cpu_headroom_millicores == 64_000  # the Halted VM sits on no node
        assert row.fits
        # A second reservation of the same size takes the cluster past 100%: no fit.
        table = rank([_ref(1)], [_report(1, nodes, halted=halted * 2)], cpu=32, memory_gib=128, buffer_percent=0)
        assert table.ranked[0].skip_reason == "fit-buffer"

    def test_unschedulable_nodes_are_neither_capacity_nor_headroom(self):
        nodes = [_node("n1", cores=64, gib=512, schedulable=False), _node("n2", cores=8, gib=64)]
        table = rank([_ref(1)], [_report(1, nodes)], cpu=32, memory_gib=128, buffer_percent=0)
        assert table.ranked[0].skip_reason == "fit-node"
        table = rank([_ref(1)], [_report(1, [_node("n1", cores=64, gib=512, schedulable=False)])], cpu=2, memory_gib=16, buffer_percent=0)
        assert table.ranked[0].skip_reason == "no-nodes"


class TestBestFit:
    def test_the_fullest_after_placement_wins_on_the_dominant_resource(self):
        # a1: 64 cores, 20 used → 50/64 = 78% after (cpu dominant)
        # a2: 64 cores, 5 used → 35/64 = 55% after
        # a3: 64 cores, 10 used, memory 160 of 300 GiB used → memory 289/300 = 96% after
        reports = [
            _report(1, [_node("n", cores=64, gib=512, used_cores=20)]),
            _report(2, [_node("n", cores=64, gib=512, used_cores=5)]),
            _report(3, [_node("n", cores=64, gib=300, used_cores=10, used_gib=160)]),
        ]
        table = rank([_ref(1), _ref(2), _ref(3)], reports, cpu=32, memory_gib=128, buffer_percent=0)
        assert [row.name for row in table.ranked] == ["ocp4-kubevirt-s-a3", "ocp4-kubevirt-s-a1", "ocp4-kubevirt-s-a2"]
        assert table.chosen == "ocp4-kubevirt-s-a3"
        assert all(row.fits for row in table.ranked)

    def test_a_tie_goes_by_name(self):
        reports = [_report(2, [_node("n", cores=64, gib=512)]), _report(1, [_node("n", cores=64, gib=512)])]
        table = rank([_ref(2), _ref(1)], reports, cpu=32, memory_gib=128, buffer_percent=0)
        assert table.chosen == "ocp4-kubevirt-s-a1"

    def test_first_fit_is_not_the_rule(self):
        # The first candidate fits; a later, fuller one is still chosen.
        reports = [_report(1, [_node("n", cores=64, gib=512)]), _report(2, [_node("n", cores=64, gib=512, used_cores=20)])]
        table = rank([_ref(1), _ref(2)], reports, cpu=32, memory_gib=128, buffer_percent=0)
        assert table.chosen == "ocp4-kubevirt-s-a2"

    def test_skipped_clusters_follow_the_fitting_ones_in_name_order(self):
        reports = [
            KvProbeFailure("no-agent", "no route"),
            _report(2, [_node("n", cores=64, gib=512)]),
            KvProbeFailure("unreachable", "timeout"),
        ]
        table = rank([_ref(3), _ref(2), _ref(1)], reports, cpu=32, memory_gib=128, buffer_percent=0)
        assert [(row.name, row.fits, row.skip_reason) for row in table.ranked] == [
            ("ocp4-kubevirt-s-a2", True, None),
            ("ocp4-kubevirt-s-a1", False, "unreachable"),
            ("ocp4-kubevirt-s-a3", False, "no-agent"),
        ]
        assert table.ranked[1].reachable is False
        assert table.ranked[1].skip_detail == "timeout"


class TestKnownMacs:
    def test_every_reachable_reports_macs_lowercased_once(self):
        reports = [
            _report(1, [_node("n", cores=64, gib=512)], vms=[
                KvVmIdentity(namespace="inventory", name="a", mac="02:A1:B2:00:00:01"),
                KvVmIdentity(namespace="inventory", name="b", mac=None),
            ]),
            KvProbeFailure("unreachable", "x"),
            _report(3, [_node("n", cores=64, gib=512)], vms=[
                KvVmIdentity(namespace="other", name="c", mac="02:a1:b2:00:00:01"),
                KvVmIdentity(namespace="other", name="d", mac="02:a1:b2:00:00:02"),
            ]),
        ]
        table = rank([_ref(1), _ref(2), _ref(3)], reports, cpu=2, memory_gib=16, buffer_percent=0)
        assert table.known_macs == ["02:a1:b2:00:00:01", "02:a1:b2:00:00:02"]


def test_the_workflows_skip_summary_mirrors_the_placement_reasons():
    # The workflow cannot import the limb, so it keeps a copy of the wording;
    # this is what keeps the two from drifting (the _REJECTION_SUMMARY precedent).
    assert _SKIP_SUMMARY == placement.SKIP_REASONS
    reasons_the_module_records = {"unreachable", "no-agent", "refused-token", "malformed", "no-nodes", "fit-node", "fit-buffer"}
    assert set(placement.SKIP_REASONS) == reasons_the_module_records


class TestEdges:
    """The boundaries of the fit test, the buffer and the ranking."""

    def test_a_fill_exactly_at_the_ceiling_fits(self):
        # 64 cores, 51.2 used after a 7-core pod → 80.0% exactly; buffer 20 allows 80%.
        node = KvNodeCapacity(name="n", schedulable=True, allocatable_cpu_millicores=64_000,
                              allocatable_memory_bytes=512 * GIB, requested_cpu_millicores=44_200, requested_memory_bytes=0)
        table = rank([_ref(1)], [_report(1, [node])], cpu=8, memory_gib=64, buffer_percent=20)
        assert table.ranked[0].cpu_fill_after == 0.8
        assert table.chosen == "ocp4-kubevirt-s-a1"
        # One millicore more is past it.
        node = node.model_copy(update={"requested_cpu_millicores": 44_201})
        table = rank([_ref(1)], [_report(1, [node])], cpu=8, memory_gib=64, buffer_percent=20)
        assert table.ranked[0].skip_reason == "fit-buffer"

    def test_buffer_zero_allows_a_full_cluster_and_buffer_ninety_almost_nothing(self):
        node = _node("n", cores=7, gib=65)  # exactly the pod of an 8-vCPU VM
        assert rank([_ref(1)], [_report(1, [node])], cpu=8, memory_gib=64, buffer_percent=0).chosen is not None
        big = _node("n", cores=640, gib=6500)
        assert rank([_ref(1)], [_report(1, [big])], cpu=8, memory_gib=64, buffer_percent=90).chosen is not None
        assert rank([_ref(1)], [_report(1, [_node("n", cores=64, gib=512)])], cpu=8, memory_gib=64, buffer_percent=90).ranked[0].skip_reason == "fit-buffer"

    def test_an_overcommitted_node_has_negative_headroom_and_never_fits(self):
        node = _node("n", cores=64, gib=512, used_cores=70, used_gib=600)
        row = rank([_ref(1)], [_report(1, [node])], cpu=1, memory_gib=8, buffer_percent=0).ranked[0]
        assert (row.skip_reason, row.max_node_cpu_headroom_millicores) == ("fit-node", -6_000)
        assert row.cpu_fill_before is not None and row.cpu_fill_before > 1

    def test_a_cluster_with_zero_allocatable_is_no_nodes_or_fit_node_never_a_division_error(self):
        empty = KvNodeCapacity(name="n", schedulable=True, allocatable_cpu_millicores=0, allocatable_memory_bytes=0,
                               requested_cpu_millicores=0, requested_memory_bytes=0)
        row = rank([_ref(1)], [_report(1, [empty])], cpu=1, memory_gib=8, buffer_percent=0).ranked[0]
        assert row.skip_reason == "fit-node"
        assert row.cpu_fill_before is None and row.memory_fill_after is None
        row = rank([_ref(1)], [_report(1, [])], cpu=1, memory_gib=8, buffer_percent=0).ranked[0]
        assert (row.skip_reason, row.reachable) == ("no-nodes", True)

    def test_cpu_one_is_a_whole_core_and_the_sixteenth_is_rounded_away(self):
        assert pod_cpu_millicores(1) == pod_cpu_millicores(2) == 1_000  # 15/16 of two is 1.875 → 1
        assert pod_cpu_millicores(16) == 15_000 and pod_cpu_millicores(17) == 15_000
        assert pod_cpu_millicores(256) == 240_000

    def test_the_dominant_resource_is_per_cluster(self):
        # a1 is CPU-bound (80% after), a2 memory-bound (90% after): a2 wins on its own dominant resource.
        reports = [
            _report(1, [_node("n", cores=64, gib=1024, used_cores=44)]),       # cpu 51/64 = 80%, mem 65/1024
            _report(2, [_node("n", cores=640, gib=200, used_cores=10, used_gib=115)]),  # mem 180/200 = 90%
        ]
        table = rank([_ref(1), _ref(2)], reports, cpu=8, memory_gib=64, buffer_percent=0)
        assert table.chosen == "ocp4-kubevirt-s-a2"
        assert [round(placement._score(row), 2) for row in table.ranked] == [0.9, 0.8]

    def test_the_choice_does_not_depend_on_the_candidates_order(self):
        reports = {
            1: _report(1, [_node("n", cores=64, gib=512, used_cores=20)]),
            2: _report(2, [_node("n", cores=64, gib=512, used_cores=40)]),
            3: _report(3, [_node("n", cores=64, gib=512, used_cores=30)]),
        }
        for order in ([1, 2, 3], [3, 2, 1], [2, 1, 3]):
            table = rank([_ref(n) for n in order], [reports[n] for n in order], cpu=8, memory_gib=64, buffer_percent=0)
            assert table.chosen == "ocp4-kubevirt-s-a2"
            assert [row.name for row in table.ranked] == ["ocp4-kubevirt-s-a2", "ocp4-kubevirt-s-a3", "ocp4-kubevirt-s-a1"]

    def test_every_candidate_is_exactly_one_row(self):
        candidates = [_ref(n) for n in range(1, 41)]
        outcomes = [
            KvProbeFailure("unreachable", "x") if n % 5 == 0 else _report(n, [_node("n", cores=64, gib=512, used_cores=n)])
            for n in range(1, 41)
        ]
        table = rank(candidates, outcomes, cpu=8, memory_gib=64, buffer_percent=0)
        assert sorted(row.name for row in table.ranked) == sorted(ref.name for ref in candidates)
        assert len({row.name for row in table.ranked}) == 40
        assert table.chosen == "ocp4-kubevirt-s-a39"  # the fullest reachable one (40 is unreachable)

    def test_candidates_and_outcomes_must_pair_up(self):
        import pytest

        with pytest.raises(ValueError):
            rank([_ref(1), _ref(2)], [_report(1, [_node("n", cores=64, gib=512)])], cpu=8, memory_gib=64, buffer_percent=0)

    def test_a_halted_reservation_alone_can_push_a_cluster_past_the_buffer_before_any_node_is_full(self):
        # The node has room (100 of 512 GiB used), but five Halted 65 GiB VMs are waiting to start.
        halted = [KvHaltedReservation(namespace="inventory", name=f"ocp-x-{i}", cpu_millicores=7_000, memory_bytes=65 * GIB) for i in range(5)]
        table = rank([_ref(1)], [_report(1, [_node("n", cores=64, gib=512, used_gib=100)], halted=halted)], cpu=8, memory_gib=64, buffer_percent=20)
        row = table.ranked[0]
        assert row.max_node_memory_headroom_bytes == 412 * GIB  # the node itself is far from full
        assert (row.skip_reason, row.halted_reservations) == ("fit-buffer", 5)
        assert row.memory_fill_after == (100 + 325 + 65) / 512

    def test_unreachable_rows_carry_no_fills_and_reachable_ones_always_do(self):
        table = rank([_ref(1), _ref(2)], [KvProbeFailure("refused-token", "401"), _report(2, [_node("n", cores=64, gib=512)])], cpu=8, memory_gib=64, buffer_percent=0)
        failed, ok = (row for row in table.ranked if row.name == "ocp4-kubevirt-s-a1"), (row for row in table.ranked if row.name == "ocp4-kubevirt-s-a2")
        failed, ok = next(failed), next(ok)
        assert (failed.reachable, failed.cpu_fill_before, failed.max_node_cpu_headroom_millicores) == (False, None, None)
        assert ok.reachable and ok.cpu_fill_before == 0.0 and ok.cpu_fill_after == 7 / 64

    def test_known_macs_ignore_empty_and_dedupe_across_case_and_clusters(self):
        reports = [
            _report(1, [_node("n", cores=64, gib=512)], vms=[
                KvVmIdentity(namespace="a", name="x", mac="02:A1:B2:00:00:0A"),
                KvVmIdentity(namespace="a", name="y", mac=""),
                KvVmIdentity(namespace="a", name="z", mac=None),
            ]),
            _report(2, [_node("n", cores=64, gib=512)], vms=[KvVmIdentity(namespace="b", name="x", mac="02:a1:b2:00:00:0a")]),
        ]
        assert placement.known_macs(reports) == ["02:a1:b2:00:00:0a"]
        assert placement.known_macs([KvProbeFailure("unreachable", "x")]) == []
