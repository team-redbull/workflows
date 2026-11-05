"""scale-down-nodepool's limb: the candidates a policy ranks, the delete mark, and the watch.

What matters: a NodePool's Machines are found by HyperShift's own annotation
and nothing else, the readings are the scheduler's (requests) and the drain's
(workload pods), the mark is a compare-and-set that never touches someone
else's, and the watch says which Machine CAPI took.
"""

from __future__ import annotations

import base64
import dataclasses
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import ClassVar

import pytest
from kubernetes import client as k8s_client
from temporalio.testing import ActivityEnvironment

from activities.server_lifecycle import activities, cluster_api, hosted_cluster_api, machine_selection
from shared.exceptions import (
    BmhResourceError,
    MachineMarkConflictError,
    NodeMetricsPendingError,
    NodeMetricsUnavailableError,
)
from shared.models.server_lifecycle import (
    MachineMarkRequest,
    MarkedMachinesOutcome,
    MarkedMachinesWatchRequest,
    ScaleDownCandidatesRequest,
    ScaleDownNodePoolRef,
    ScaleDownPolicy,
)

HC_NS, HC = "hcp-alpha", "openshift-alpha"
HCP = f"{HC_NS}-{HC}"
MARK = "scale-down-nodepool-ocp4-alpha"
KUBECONFIG = """
apiVersion: v1
kind: Config
current-context: admin
contexts:
- name: admin
  context: {cluster: hc, user: admin}
clusters:
- name: hc
  cluster:
    server: https://api.alpha.external.example:6443
    certificate-authority-data: Q0EgUEVN
users:
- name: admin
  user:
    token: secret-token
"""


def _machine(name: str, nodepool: str = "np-a", *, node: str | None = None, mark: str | None = None,
             deleting: bool = False, namespace: str = HC_NS) -> dict:
    annotations = {machine_selection.NODEPOOL_ANNOTATION: f"{namespace}/{nodepool}"}
    if mark is not None:
        annotations[machine_selection.DELETE_MACHINE_ANNOTATION] = mark
    metadata = {"name": name, "namespace": HCP, "resourceVersion": "7", "annotations": annotations,
                "creationTimestamp": "2026-11-01T10:00:00Z"}
    if deleting:
        metadata["deletionTimestamp"] = "2026-11-03T10:00:00Z"
    return {"metadata": metadata, "status": {"nodeRef": {"name": node}} if node else {}}


def _node(name: str, *, ready: bool = True, unschedulable: bool = False, cpu: str = "8", memory: str = "32Gi") -> dict:
    return {
        "metadata": {"name": name},
        "spec": {"unschedulable": unschedulable},
        "status": {"allocatable": {"cpu": cpu, "memory": memory},
                   "conditions": [{"type": "Ready", "status": "True" if ready else "False"}]},
    }


def _pod(name: str, node: str, *, cpu: str = "1", memory: str = "1Gi", daemonset: bool = False,
         phase: str = "Running") -> dict:
    owners = [{"kind": "DaemonSet", "controller": True}] if daemonset else []
    return {
        "metadata": {"name": name, "namespace": "app", "ownerReferences": owners},
        "spec": {"nodeName": node, "containers": [{"resources": {"requests": {"cpu": cpu, "memory": memory}}}]},
        "status": {"phase": phase},
    }


# --- the pure readings -----------------------------------------------------------------


class TestTheReadings:
    def test_a_pod_requests_what_the_scheduler_counts(self):
        pod = {"spec": {
            "containers": [{"resources": {"requests": {"cpu": "250m", "memory": "1Gi"}}},
                           {"resources": {"requests": {"cpu": "500m"}}}],
            "initContainers": [{"resources": {"requests": {"cpu": "2", "memory": "512Mi"}}}],
            "overhead": {"cpu": "100m"},
        }}
        requests = machine_selection.pod_requests(pod)
        # max(0.75, 2) + 0.1 cpu; max(1Gi, 512Mi) memory.
        assert requests == {"cpu": Decimal("2.1"), "memory": Decimal(1024**3)}

    def test_workload_pods_are_what_a_drain_moves(self):
        on_node = machine_selection.pods_on([
            _pod("web", "n1"), _pod("ds", "n1", daemonset=True), _pod("done", "n1", phase="Succeeded"),
            _pod("elsewhere", "n2"),
        ], "n1")
        assert [p["metadata"]["name"] for p in on_node] == ["web", "ds"]
        assert machine_selection.workload_pods(on_node) == 1

    def test_the_share_is_the_larger_of_cpu_and_memory_per_thousand(self):
        capacity = machine_selection.allocatable(_node("n1", cpu="8", memory="32Gi"))
        assert machine_selection.share_permille({"cpu": Decimal(2), "memory": Decimal(16 * 1024**3)}, capacity) == 500
        assert machine_selection.share_permille({}, {}) is None

    def test_a_machine_is_its_nodepools_by_the_exact_annotation(self):
        machines = [_machine("m1"), _machine("m2", "np-b"), _machine("m3", namespace="other"), _machine("m0")]
        assert [m["metadata"]["name"] for m in machine_selection.machines_of(machines, f"{HC_NS}/np-a")] == [
            "m0", "m1",
        ]

    @pytest.mark.parametrize(
        ("status", "healthy"),
        [
            ({"nodeRef": {"name": "n1"}}, True),
            ({}, False),  # no nodeRef
            ({"nodeRef": {"name": "n1"}, "failureReason": "CreateError"}, False),
            ({"nodeRef": {"name": "n1"}, "failureMessage": "boom"}, False),
            ({"nodeRef": {"name": "n1"}, "conditions": [{"type": "NodeHealthy", "status": "False"}]}, False),
            ({"nodeRef": {"name": "n1"}, "conditions": [{"type": "HealthCheckSucceeded", "status": "False"}]}, False),
            # Unknown is not unhealthy, as in CAPI's isMachineHealthy.
            ({"nodeRef": {"name": "n1"}, "conditions": [{"type": "NodeHealthy", "status": "Unknown"}]}, True),
            ({"nodeRef": {"name": "n1"}, "conditions": [{"type": "Ready", "status": "False"}]}, True),
        ],
    )
    def test_capi_healthy_is_capis_own_is_machine_healthy(self, status, healthy):
        assert machine_selection.capi_healthy({"metadata": {"name": "m"}, "status": status}) is healthy

    def test_the_owning_machineset_is_read_from_the_owner_reference(self):
        machine = _machine("m1")
        machine["metadata"]["ownerReferences"] = [{"kind": "MachineSet", "name": "np-a-6f9c", "controller": True}]
        assert machine_selection.machine_set(machine) == "np-a-6f9c"
        assert machine_selection.machine_set(_machine("m2")) is None

    def test_an_empty_mark_is_still_a_mark(self):
        assert machine_selection.delete_mark(_machine("m1", mark="")) == ""
        assert machine_selection.delete_mark(_machine("m1")) is None

    def test_the_mark_patch_is_a_compare_and_set(self):
        assert machine_selection.mark_patch("7", MARK) == {"metadata": {
            "annotations": {machine_selection.DELETE_MACHINE_ANNOTATION: MARK}, "resourceVersion": "7"}}
        assert machine_selection.mark_patch("7", None)["metadata"]["annotations"] == {
            machine_selection.DELETE_MACHINE_ANNOTATION: None}


# --- the activities, against a fake MCE ---------------------------------------------------


class _FakeMce:
    def __init__(self, machines: list[dict]) -> None:
        self.machines = {m["metadata"]["name"]: m for m in machines}
        self.patches: list[tuple[str, dict]] = []
        self.hosted = {"metadata": {"namespace": HC_NS, "name": HC},
                       "status": {"kubeconfig": {"name": "alpha-admin-kubeconfig"}}}

    async def read_custom_object(self, *, plural, namespace, name, **_):
        if plural == "hostedclusters":
            return self.hosted if (namespace, name) == (HC_NS, HC) else None
        assert (plural, namespace) == ("machines", HCP)
        return self.machines.get(name)

    async def list_custom_objects(self, *, plural, namespace, **_):
        assert (plural, namespace) == ("machines", HCP)
        return list(self.machines.values())

    async def patch_custom_object(self, *, plural, namespace, name, body, **_):
        assert (plural, namespace) == ("machines", HCP)
        self.patches.append((name, body))
        machine = self.machines.get(name)
        if machine is None:
            return False
        for key, value in body["metadata"]["annotations"].items():
            if value is None:
                machine["metadata"]["annotations"].pop(key, None)
            else:
                machine["metadata"]["annotations"][key] = value
        return True

    async def read_secret_data(self, namespace, name):
        assert (namespace, name) == (HC_NS, "alpha-admin-kubeconfig")
        return {"kubeconfig": base64.b64encode(KUBECONFIG.encode()).decode()}


@pytest.fixture
def mce(monkeypatch):
    def _install(fake: _FakeMce) -> _FakeMce:
        for fn in ("read_custom_object", "list_custom_objects", "patch_custom_object", "read_secret_data"):
            monkeypatch.setattr(cluster_api, fn, getattr(fake, fn))
        return fake

    return _install


@pytest.fixture
def hosted(monkeypatch):
    seen: dict = {}

    def _install(nodes, pods, metrics=None):
        async def _read(kubeconfig, server, cluster, with_metrics):
            seen.update(kubeconfig=kubeconfig, server=server, cluster=cluster, with_metrics=with_metrics)
            return {"nodes": nodes, "pods": pods, "metrics": metrics if with_metrics else None}

        monkeypatch.setattr(hosted_cluster_api, "read_scale_down_objects", _read)
        return seen

    return _install


def _candidates_request(policy: ScaleDownPolicy = ScaleDownPolicy.FEWEST_PODS) -> ScaleDownCandidatesRequest:
    return ScaleDownCandidatesRequest(
        hosted_cluster_namespace=HC_NS, hosted_cluster=HC,
        nodepools=[ScaleDownNodePoolRef(name="np-a", policy=policy)],
    )


class TestTheCandidates:
    async def test_each_machine_of_the_nodepool_with_its_readings(self, mce, hosted):
        mce(_FakeMce([
            _machine("m1", node="n1"), _machine("m2", node="n2", mark=MARK), _machine("m3"),
            _machine("other", "np-b", node="n9"),
        ]))
        seen = hosted(
            [_node("n1"), _node("n2", ready=False)],
            [_pod("a", "n1", cpu="2", memory="4Gi"), _pod("b", "n1"), _pod("ds", "n1", daemonset=True)],
        )
        candidates = await activities.read_scale_down_candidates(_candidates_request())

        assert candidates.hcp_namespace == HCP
        assert seen["server"] == f"https://kube-apiserver.{HCP}.svc:6443" and seen["with_metrics"] is False
        by_name = {m.name: m for m in candidates.machines}
        assert sorted(by_name) == ["m1", "m2", "m3"]
        assert (by_name["m1"].node_found, by_name["m1"].ready, by_name["m1"].workload_pods) == (True, True, 2)
        # 4 cpu of 8 requested (DaemonSet included: the scheduler counts it) — 500 per thousand.
        assert by_name["m1"].requested_permille == 500 and by_name["m1"].used_permille is None
        assert by_name["m2"].delete_mark == MARK and not by_name["m2"].ready
        assert (by_name["m3"].node, by_name["m3"].node_found) == (None, False)
        # CAPI's own reading of the Machine: no nodeRef is unhealthy for it.
        assert by_name["m1"].capi_healthy and not by_name["m3"].capi_healthy

    async def test_least_utilized_reads_the_metrics_and_ranks_by_use(self, mce, hosted):
        mce(_FakeMce([_machine("m1", node="n1")]))
        seen = hosted([_node("n1")], [], metrics=[{"metadata": {"name": "n1"},
                                                   "usage": {"cpu": "800m", "memory": "8Gi"}}])
        [machine] = (await activities.read_scale_down_candidates(_candidates_request(ScaleDownPolicy.LEAST_UTILIZED))).machines
        assert seen["with_metrics"] is True and machine.used_permille == 250

    async def test_a_ready_node_with_no_metrics_yet_is_waited_for(self, mce, hosted):
        mce(_FakeMce([_machine("m1", node="n1")]))
        hosted([_node("n1")], [], metrics=[])
        with pytest.raises(NodeMetricsPendingError, match="n1"):
            await activities.read_scale_down_candidates(_candidates_request(ScaleDownPolicy.LEAST_UTILIZED))

    async def test_a_broken_node_needs_no_metrics(self, mce, hosted):
        mce(_FakeMce([_machine("m1", node="n1"), _machine("m2")]))
        hosted([_node("n1", ready=False)], [], metrics=[])
        machines = (await activities.read_scale_down_candidates(_candidates_request(ScaleDownPolicy.LEAST_UTILIZED))).machines
        assert len(machines) == 2


class TestTheMark:
    def _request(self, *names: str) -> MachineMarkRequest:
        return MachineMarkRequest(namespace=HCP, machines=list(names), mark=MARK)

    async def test_it_marks_at_the_version_read_and_reads_it_back(self, mce):
        fake = mce(_FakeMce([_machine("m1"), _machine("m2", mark=MARK)]))
        marks = await activities.mark_machines_for_deletion(self._request("m1", "m2"))
        assert (marks.changed, marks.unchanged) == (["m1"], ["m2"])
        assert fake.patches == [("m1", machine_selection.mark_patch("7", MARK))]

    @pytest.mark.parametrize(
        ("machine", "words"),
        [
            (_machine("m1", mark="yes"), "another delete mark"),
            (_machine("m1", deleting=True), "already deleting"),
        ],
    )
    async def test_someone_elses_or_a_deleting_machine_is_refused(self, mce, machine, words):
        fake = mce(_FakeMce([machine]))
        with pytest.raises(MachineMarkConflictError, match=words):
            await activities.mark_machines_for_deletion(self._request("m1"))
        assert fake.patches == []

    async def test_a_machine_gone_is_refused(self, mce):
        mce(_FakeMce([]))
        with pytest.raises(MachineMarkConflictError, match="is gone"):
            await activities.mark_machines_for_deletion(self._request("m1"))

    async def test_a_mark_that_does_not_read_back_is_retried(self, mce, monkeypatch):
        fake = mce(_FakeMce([_machine("m1")]))

        async def _lost(**kwargs):
            fake.patches.append((kwargs["name"], kwargs["body"]))
            return True

        monkeypatch.setattr(cluster_api, "patch_custom_object", _lost)
        with pytest.raises(BmhResourceError, match="did not read back"):
            await activities.mark_machines_for_deletion(self._request("m1"))

    async def test_unmark_takes_only_this_runs_mark_off(self, mce):
        fake = mce(_FakeMce([_machine("m1", mark=MARK), _machine("m2", mark="yes"), _machine("m3")]))
        marks = await activities.unmark_machines(self._request("m1", "m2", "m3", "gone"))
        assert (marks.changed, marks.unchanged) == (["m1"], ["m2", "m3", "gone"])
        assert machine_selection.delete_mark(fake.machines["m2"]) == "yes"
        assert machine_selection.delete_mark(fake.machines["m1"]) is None


class TestTheWatch:
    def _request(self, *marked: str, deadline: int = 10) -> MarkedMachinesWatchRequest:
        return MarkedMachinesWatchRequest(
            hcp_namespace=HCP, hosted_cluster_namespace=HC_NS, nodepools=["np-a"],
            marked=list(marked), deadline_minutes=deadline,
        )

    async def test_done_once_every_marked_machine_is_deleting_or_gone(self, mce):
        mce(_FakeMce([_machine("m1", mark=MARK, deleting=True), _machine("m3")]))
        watch = await ActivityEnvironment().run(activities.watch_marked_machines, self._request("m1", "m2"))
        assert watch.result is MarkedMachinesOutcome.DONE and watch.deleting == ["m1", "m2"]

    async def test_an_unmarked_machine_deleting_is_the_wrong_machine(self, mce):
        mce(_FakeMce([_machine("m1", mark=MARK), _machine("m3", deleting=True), _machine("x", "np-b", deleting=True)]))
        watch = await ActivityEnvironment().run(activities.watch_marked_machines, self._request("m1"))
        assert watch.result is MarkedMachinesOutcome.WRONG_MACHINE
        # Another NodePool's Machine deleting is not this run's business.
        assert watch.unexpected == ["m3"] and watch.pending == ["m1"]

    async def test_the_deadline_carries_over_from_the_last_heartbeat(self, mce):
        mce(_FakeMce([_machine("m1", mark=MARK)]))
        env = ActivityEnvironment()
        started = (datetime.now(UTC) - timedelta(minutes=2)).isoformat()
        env.info = dataclasses.replace(env.info, heartbeat_details=[started])
        heartbeats: list = []
        env.on_heartbeat = heartbeats.append
        watch = await env.run(activities.watch_marked_machines, self._request("m1", deadline=1))
        assert watch.result is MarkedMachinesOutcome.DEADLINE and watch.pending == ["m1"]
        assert heartbeats == [started]


# --- the hosted cluster's read -------------------------------------------------------------


class _FakeCore:
    def __init__(self, api_client):
        pass

    def list_node(self, _request_timeout):
        return {"items": [_node("n1")]}

    def list_pod_for_all_namespaces(self, field_selector, _request_timeout):
        assert field_selector == "status.phase!=Succeeded,status.phase!=Failed"
        return {"items": [_pod("a", "n1")]}


class _FakeCustom:
    outcome: ClassVar[object] = None

    def __init__(self, api_client):
        pass

    def list_cluster_custom_object(self, group, version, plural, _request_timeout):
        assert (group, version, plural) == ("metrics.k8s.io", "v1beta1", "nodes")
        if isinstance(_FakeCustom.outcome, Exception):
            raise _FakeCustom.outcome
        return {"items": [{"metadata": {"name": "n1"}, "usage": {"cpu": "1"}}]}


class TestTheHostedRead:
    SERVER = f"https://kube-apiserver.{HCP}.svc:6443"

    @pytest.fixture(autouse=True)
    def _fakes(self, monkeypatch):
        monkeypatch.setattr(k8s_client, "CoreV1Api", _FakeCore)
        monkeypatch.setattr(k8s_client, "CustomObjectsApi", _FakeCustom)
        _FakeCustom.outcome = None

    async def test_nodes_and_running_pods_and_no_metrics_unless_asked(self):
        objects = await hosted_cluster_api.read_scale_down_objects(KUBECONFIG, self.SERVER, "a/b", False)
        assert [n["metadata"]["name"] for n in objects["nodes"]] == ["n1"]
        assert [p["metadata"]["name"] for p in objects["pods"]] == ["a"] and objects["metrics"] is None

    async def test_the_metrics_when_asked(self):
        objects = await hosted_cluster_api.read_scale_down_objects(KUBECONFIG, self.SERVER, "a/b", True)
        assert objects["metrics"] == [{"metadata": {"name": "n1"}, "usage": {"cpu": "1"}}]

    async def test_no_metrics_api_is_permanent_and_names_no_secret(self):
        _FakeCustom.outcome = k8s_client.ApiException(status=404)
        with pytest.raises(NodeMetricsUnavailableError, match="metrics.k8s.io") as excinfo:
            await hosted_cluster_api.read_scale_down_objects(KUBECONFIG, self.SERVER, "a/b", True)
        assert "secret-token" not in str(excinfo.value)
