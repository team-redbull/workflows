"""scale-down-nodepool's choice of the servers that leave — the pure policy module."""

from __future__ import annotations

import pytest

from shared.exceptions import (
    NodePoolReplicasDriftError,
    ScaleDownMarkConflictError,
    ScaleDownPolicyInvalidError,
)
from shared.models.server_lifecycle import ScaleDownMachine, ScaleDownPolicy
from workflow_domains.server_lifecycle import scale_down_selection as selection

MARK = "scale-down-nodepool-ocp4-prod-alpha"


def _machine(name: str, *, nodepool: str = "np-a", **fields) -> ScaleDownMachine:
    defaults = dict(
        nodepool=nodepool, namespace="hcp-alpha-openshift-alpha", name=name, node=f"node-{name}",
        node_found=True, ready=True, unschedulable=False, workload_pods=5, requested_permille=500,
        used_permille=500,
    )
    defaults.update(fields)
    return ScaleDownMachine(**defaults)


def _chosen(machines, remove: int, policy: ScaleDownPolicy, nodepool: str = "np-a") -> list[str]:
    shrink = {nodepool: (len([m for m in machines if m.nodepool == nodepool]), remove)}
    return [c.machine for c in selection.choose(machines, shrink, {nodepool: policy}, MARK)]


@pytest.mark.parametrize(
    ("value", "policy"),
    [(None, None), ("", None), ("fewest-pods", ScaleDownPolicy.FEWEST_PODS),
     ("least-requested", ScaleDownPolicy.LEAST_REQUESTED), (" least-utilized ", ScaleDownPolicy.LEAST_UTILIZED)],
)
def test_the_policy_values(value, policy):
    assert selection.policy_of(value) is policy


def test_an_unknown_value_is_refused_naming_every_nodepool():
    refusal = selection.invalid_policies({"np-a": "fewest", "np-b": None, "np-c": "random"})
    assert refusal is not None and refusal.error is ScaleDownPolicyInvalidError
    assert "np-a: 'fewest'" in refusal.message and "np-c: 'random'" in refusal.message
    assert "np-b" not in refusal.message
    assert selection.invalid_policies({"np-a": "fewest-pods", "np-b": None}) is None


def test_fewest_pods_takes_the_lightest_drain():
    machines = [_machine("m1", workload_pods=9), _machine("m2", workload_pods=1), _machine("m3", workload_pods=4)]
    assert _chosen(machines, 2, ScaleDownPolicy.FEWEST_PODS) == ["m2", "m3"]


def test_least_requested_takes_the_lowest_requested_share():
    machines = [_machine("m1", requested_permille=300), _machine("m2", requested_permille=800),
                _machine("m3", requested_permille=100)]
    assert _chosen(machines, 1, ScaleDownPolicy.LEAST_REQUESTED) == ["m3"]


def test_least_utilized_takes_the_lowest_live_use():
    machines = [_machine("m1", used_permille=50, requested_permille=900),
                _machine("m2", used_permille=600, requested_permille=10)]
    assert _chosen(machines, 1, ScaleDownPolicy.LEAST_UTILIZED) == ["m1"]


@pytest.mark.parametrize("policy", list(ScaleDownPolicy))
def test_every_policy_takes_the_broken_ones_first(policy):
    machines = [
        _machine("healthy", workload_pods=0, requested_permille=0, used_permille=0),
        _machine("cordoned", unschedulable=True, workload_pods=50),
        _machine("notready", ready=False, workload_pods=50),
        _machine("nonode", node=None, node_found=False),
    ]
    chosen = selection.choose(machines, {"np-a": (4, 3)}, {"np-a": policy}, MARK)
    # No Node first; then NotReady and cordoned, by name.
    assert [c.machine for c in chosen] == ["nonode", "cordoned", "notready"]
    assert [c.reason for c in chosen] == ["no Node", "cordoned", "NotReady"]


def test_this_runs_own_mark_comes_first_so_a_rerun_keeps_its_choice():
    machines = [_machine("m1", workload_pods=0), _machine("m2", workload_pods=9, delete_mark=MARK)]
    chosen = selection.choose(machines, {"np-a": (2, 1)}, {"np-a": ScaleDownPolicy.FEWEST_PODS}, MARK)
    assert [(c.machine, c.reason) for c in chosen] == [("m2", "already marked by this run")]


def test_ties_go_by_machine_name():
    machines = [_machine("m3"), _machine("m1"), _machine("m2")]
    assert _chosen(machines, 2, ScaleDownPolicy.FEWEST_PODS) == ["m1", "m2"]


def test_each_nodepool_loses_its_own_count_by_its_own_policy():
    machines = [
        _machine("a1", nodepool="np-a", workload_pods=1), _machine("a2", nodepool="np-a", workload_pods=2),
        _machine("b1", nodepool="np-b", requested_permille=900), _machine("b2", nodepool="np-b", requested_permille=100),
    ]
    chosen = selection.choose(
        machines,
        {"np-a": (2, 1), "np-b": (2, 1)},
        {"np-a": ScaleDownPolicy.FEWEST_PODS, "np-b": ScaleDownPolicy.LEAST_REQUESTED},
        MARK,
    )
    assert [(c.nodepool, c.machine, c.policy) for c in chosen] == [
        ("np-a", "a1", "fewest-pods"), ("np-b", "b2", "least-requested"),
    ]


def test_a_reading_names_the_policys_value():
    [choice] = selection.choose(
        [_machine("m1", requested_permille=412)], {"np-a": (1, 1)}, {"np-a": ScaleDownPolicy.LEAST_REQUESTED}, MARK
    )
    assert choice.reason == "requested 41.2%"


def test_a_machine_already_deleting_is_a_scale_in_flight():
    machines = [_machine("m1"), _machine("m2", deleting=True)]
    refusal = selection.refusal(machines, {"np-a": (2, 1)}, MARK)
    assert refusal is not None and refusal.error is NodePoolReplicasDriftError and "m2" in refusal.message


def test_someone_elses_mark_is_refused():
    machines = [_machine("m1"), _machine("m2", delete_mark="yes")]
    refusal = selection.refusal(machines, {"np-a": (2, 1)}, MARK)
    assert refusal is not None and refusal.error is ScaleDownMarkConflictError and "m2 ('yes')" in refusal.message


def test_an_empty_mark_is_still_a_mark():
    refusal = selection.refusal([_machine("m1", delete_mark="")], {"np-a": (1, 1)}, MARK)
    assert refusal is not None and refusal.error is ScaleDownMarkConflictError


def test_this_runs_own_mark_is_no_conflict():
    assert selection.refusal([_machine("m1", delete_mark=MARK), _machine("m2")], {"np-a": (2, 1)}, MARK) is None


def test_a_machine_count_that_is_not_the_replicas_is_refused():
    refusal = selection.refusal([_machine("m1")], {"np-a": (3, 1)}, MARK)
    assert refusal is not None and refusal.error is NodePoolReplicasDriftError
    assert "3 replicas but 1 Machines" in refusal.message
