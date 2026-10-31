"""The cluster-upgrade limb's readings of API objects (activities/cluster_upgrade/upgrade_state.py).

Pure functions on plain dicts. Where a shape matters, the input is what a live
cluster returned: tests/fixtures/cluster_upgrade/ holds objects captured on the
sandbox (experiment 2, a KubeVirt HostedCluster at 4.19.53, MCE 2.17.3),
trimmed to the fields read and with nothing that names the sandbox.
"""

from __future__ import annotations

import json
import pathlib
from datetime import UTC, datetime, timedelta

import pytest

from activities.cluster_upgrade import upgrade_state

FIXTURES = pathlib.Path(__file__).parent / "fixtures" / "cluster_upgrade"
GATE = "ack-4.19-admissionregistration-v1beta1-api-removals-in-4.20"
VWC = "validatingwebhookconfigurations.v1beta1.admissionregistration.k8s.io"


def _fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


def _cv(*conditions: dict) -> dict:
    return {"status": {"conditions": list(conditions)}}


def _cond(kind: str, status: str, reason: str = "", message: str = "") -> dict:
    return {"type": kind, "status": status, "reason": reason, "message": message}


class TestUpgradeable:
    def test_a_4_19_cluster_installing_names_both_reasons(self):
        # Live: Upgradeable=False MultipleReasons, UpgradeableAdminAckRequired=False
        # and UpgradeableUpgradeInProgress=True — the latter True BY DESIGN.
        fields = upgrade_state.upgradeable(_fixture("clusterversion-4.19.53-installing.json"))
        assert fields["cluster_version_upgradeable"] is False
        assert fields["upgradeable_reason"] == "MultipleReasons"
        assert fields["upgradeable_reasons"] == ["AdminAckRequired", "UpdateInProgress"]

    def test_no_upgradeable_condition_means_upgradeable(self):
        # The 4.17 and 4.18 CVOs set none; HyperShift reports True then.
        fields = upgrade_state.upgradeable(_cv(_cond("Available", "True")))
        assert fields == {"cluster_version_upgradeable": True, "upgradeable_reason": None,
                          "upgradeable_message": None, "upgradeable_reasons": []}

    def test_the_same_cluster_acked_and_installed_is_upgradeable(self):
        # Live, after the ack and the install: the 4.19 CVO removes Upgradeable when
        # nothing blocks, and HyperShift said ClusterVersionUpgradeable=True FromClusterVersion.
        completed = _fixture("clusterversion-4.19.53-completed.json")
        assert "Upgradeable" not in [c["type"] for c in completed["status"]["conditions"]]
        assert upgrade_state.upgradeable(completed)["cluster_version_upgradeable"] is True
        assert (completed["status"]["history"][0]["version"], completed["status"]["history"][0]["state"]) == ("4.19.53", "Completed")
        assert upgrade_state.admin_ack_required(completed) is None

    def test_one_reason_sits_on_the_aggregate_alone(self):
        fields = upgrade_state.upgradeable(_cv(_cond("Upgradeable", "False", "AdminAckRequired", "ack it")))
        assert fields["upgradeable_reasons"] == ["AdminAckRequired"]
        assert fields["upgradeable_message"] == "ack it"


class TestAdminAckRequired:
    def test_read_from_its_own_condition_when_there_are_several(self):
        message = upgrade_state.admin_ack_required(_fixture("clusterversion-4.19.53-installing.json"))
        assert message.startswith("The admissionregistration.k8s.io/v1beta1 group version is deprecated in 4.19")

    def test_read_from_the_aggregate_when_it_is_the_only_reason(self):
        cv = _cv(_cond("Upgradeable", "False", "AdminAckRequired", "ack the v1beta1 removal"))
        assert upgrade_state.admin_ack_required(cv) == "ack the v1beta1 removal"

    @pytest.mark.parametrize("cv", [
        _cv(),
        _cv(_cond("Upgradeable", "True")),
        _cv(_cond("Upgradeable", "False", "UpdateInProgress", "x")),
    ])
    def test_none_once_acked_or_for_another_reason(self, cv):
        assert upgrade_state.admin_ack_required(cv) is None


class TestApiUsage:
    COUNT = {"status": {"last24h": [
        {"byNode": [
            {"nodeName": "kas-0", "byUser": [
                {"username": "system:serviceaccount:kyverno:kyverno", "userAgent": "kyverno/v1.10.0",
                 "requestCount": 3, "byVerb": [{"verb": "get", "requestCount": 3}]},
                {"username": "system:kube-controller-manager", "userAgent": "kube-controller-manager/v1.32",
                 "requestCount": 40, "byVerb": [{"verb": "list", "requestCount": 2}, {"verb": "watch", "requestCount": 38}]},
            ]},
        ]},
        {"byNode": [
            {"nodeName": "kas-0", "byUser": [
                {"username": "system:serviceaccount:kyverno:kyverno", "userAgent": "kyverno/v1.10.0",
                 "requestCount": 2, "byVerb": [{"verb": "update", "requestCount": 2}]},
            ]},
        ]},
        {},  # an hour nobody called it
    ]}}

    def test_summed_per_user_and_agent_over_the_day(self):
        usage = upgrade_state.api_usage(VWC, self.COUNT)
        assert [(u.username, u.requests, u.verbs) for u in usage] == [
            ("system:kube-controller-manager", 40, ["list", "watch"]),
            ("system:serviceaccount:kyverno:kyverno", 5, ["get", "update"]),
        ]
        assert {u.api for u in usage} == {VWC}

    def test_no_apirequestcount_means_nobody_called_it(self):
        assert upgrade_state.api_usage(VWC, None) == []


class TestAdminAckState:
    def test_gates_acks_usage_and_the_cvo_together(self):
        state = upgrade_state.admin_ack_state(
            admin_gates={"data": {GATE: "deprecated in 4.19"}},
            admin_acks={"data": None},  # as the release creates it: no data at all
            cluster_version=_fixture("clusterversion-4.19.53-installing.json"),
            api_request_counts={VWC: TestApiUsage.COUNT, "validatingadmissionpolicies.v1beta1.admissionregistration.k8s.io": None},
        )
        assert state.reachable and state.gates == {GATE: "deprecated in 4.19"} and state.acks == {}
        assert len(state.usage) == 2 and state.admin_ack_required

    def test_the_patch_touches_one_key(self):
        assert upgrade_state.ack_patch(GATE) == {"data": {GATE: "true"}}


class TestHostedCluster:
    HC = {
        "metadata": {"name": "lab-upgrade", "namespace": "upgrade-lab"},
        "spec": {"imageContentSources": [{"source": "quay.io/openshift-release-dev/ocp-release",
                                          "mirrors": ["registry-internal/openshift-release/openshift/release-images"]}]},
        "status": {
            "version": {"history": [{"state": "Partial", "version": "4.20.44"},
                                    {"state": "Completed", "version": "4.19.53"}]},
            "conditions": [_cond("Progressing", "False", "AsExpected"), _cond("ClusterVersionProgressing", "True")],
        },
    }
    NODEPOOL = {
        "metadata": {"name": "lab-upgrade-workers"},
        "spec": {"clusterName": "lab-upgrade", "replicas": 2,
                 "management": {"upgradeType": "InPlace", "inPlace": {"maxUnavailable": 1}},
                 "nodeLabels": {"topology.kubernetes.io/zone": "a"}},
        "status": {"version": "4.19.53", "replicas": 2, "conditions": [_cond("UpdatingVersion", "False")]},
    }

    def test_mid_upgrade_there_is_no_completed_version(self):
        state = upgrade_state.hosted_cluster_state(self.HC, [self.NODEPOOL])
        assert state.found and state.control_plane_version is None
        assert state.progressing is False and state.conditions["ClusterVersionProgressing"] == "True"
        assert state.image_content_mirrors == ["registry-internal/openshift-release/openshift/release-images"]

    def test_a_nodepool_reads_as_the_workflow_expects(self):
        (nodepool,) = upgrade_state.hosted_cluster_state(self.HC, [self.NODEPOOL]).nodepools
        assert (nodepool.version, nodepool.upgrade_type, nodepool.max_unavailable, nodepool.zone) == (
            "4.19.53", "InPlace", "1", "a")

    def test_only_its_own_nodepools(self):
        other = {**self.NODEPOOL, "spec": {**self.NODEPOOL["spec"], "clusterName": "someone-else"}}
        assert upgrade_state.hosted_cluster_state(self.HC, [other]).nodepools == []

    def test_absent(self):
        assert upgrade_state.hosted_cluster_state(None, []).found is False


def test_unhealthy_operators_name_what_is_wrong():
    operators = [
        {"metadata": {"name": "console"}, "status": {"conditions": [
            _cond("Available", "False", "RouteHealth_FailedGet"), _cond("Degraded", "True", "RouteHealthDegraded")]}},
        {"metadata": {"name": "dns"}, "status": {"conditions": [_cond("Available", "True"), _cond("Degraded", "False")]}},
        {"metadata": {"name": "insights"}, "status": {}},
    ]
    assert upgrade_state.operators_unhealthy(operators) == [
        "console (Available=False: RouteHealth_FailedGet; Degraded=True: RouteHealthDegraded)",
        "insights (Available=missing)",
    ]


def _node(name, ready="True", nodepool="np-a"):
    return {"metadata": {"name": name, "labels": {upgrade_state.NODEPOOL_LABEL: nodepool}},
            "status": {"conditions": [_cond("Ready", ready)]}}


def _pod(name, namespace, node, **labels):
    return {"metadata": {"name": name, "namespace": namespace, "labels": labels}, "spec": {"nodeName": node}}


def _pdb(name, namespace, allowed, **match):
    return {"metadata": {"name": name, "namespace": namespace},
            "spec": {"selector": {"matchLabels": match}}, "status": {"disruptionsAllowed": allowed}}


class TestHostedHealth:
    def test_nodes_and_their_nodepools(self):
        nodes = [_node("w-1"), _node("w-2", ready="Unknown"), _node("w-3", nodepool="np-b")]
        assert upgrade_state.nodes_not_ready(nodes) == ["w-2"]
        assert upgrade_state.nodes_in_scope(nodes, ["np-b"]) == {"w-3"}
        assert upgrade_state.nodes_in_scope(nodes, []) == {"w-1", "w-2", "w-3"}

    def test_a_csr_with_no_condition_is_pending(self):
        csrs = [{"status": {}}, {"status": {"conditions": [_cond("Approved", "True")]}}, {}]
        assert upgrade_state.pending_csrs(csrs) == 2

    def test_only_a_customer_pdb_at_zero_covering_an_in_scope_pod_blocks(self):
        pods = [_pod("pg-0", "db", "w-1", app="pg"), _pod("dl-0", "openshift-console", "w-1", app="downloads"),
                _pod("web-0", "shop", "w-9", app="web")]
        pdbs = [
            _pdb("pg", "db", 0, app="pg"),                          # blocks
            _pdb("downloads", "openshift-console", 0, app="downloads"),  # platform: the watch and the agent's (D17)
            _pdb("web", "shop", 0, app="web"),                       # its Pod is on a Node out of scope
            _pdb("cache", "db", 1, app="pg"),                        # allows a disruption
        ]
        assert upgrade_state.blocking_pdbs(pdbs, pods, {"w-1"}) == ["db/pg"]

    def test_match_expressions_select_as_kubernetes_does(self):
        selector = {"matchExpressions": [{"key": "tier", "operator": "In", "values": ["db"]},
                                         {"key": "canary", "operator": "DoesNotExist"}]}
        assert upgrade_state._selects(selector, {"tier": "db"})
        assert not upgrade_state._selects(selector, {"tier": "db", "canary": "1"})
        assert upgrade_state._selects({}, {"anything": "x"})

    def test_a_fail_closed_webhook_without_endpoints_is_dead(self):
        configurations = [{"metadata": {"name": "kyverno"}, "webhooks": [
            {"name": "validate.kyverno.svc", "failurePolicy": "Fail",
             "clientConfig": {"service": {"namespace": "kyverno", "name": "kyverno-svc"}}},
            {"name": "ignore.kyverno.svc", "failurePolicy": "Ignore",
             "clientConfig": {"service": {"namespace": "kyverno", "name": "kyverno-svc"}}},
            {"name": "by-url", "clientConfig": {"url": "https://example/validate"}},
        ]}, {"metadata": {"name": "ok"}, "webhooks": [
            {"name": "v", "clientConfig": {"service": {"namespace": "ok", "name": "svc"}}}]}]
        endpoints = [
            {"metadata": {"namespace": "kyverno", "name": "kyverno-svc"}, "subsets": [{"notReadyAddresses": [{}]}]},
            {"metadata": {"namespace": "ok", "name": "svc"}, "subsets": [{"addresses": [{"ip": "10.0.0.1"}]}]},
        ]
        assert upgrade_state.dead_webhooks(configurations, endpoints) == ["kyverno/validate.kyverno.svc"]

    def test_idms_mirrors_and_short_nodepools(self):
        idms = [{"spec": {"imageDigestMirrors": [{"source": "quay.io/x", "mirrors": ["registry-internal/x", "m2/x"]}]}}]
        assert upgrade_state.idms_mirrors(idms) == ["m2/x", "registry-internal/x"]
        nodepools = upgrade_state.hosted_cluster_state(TestHostedCluster.HC, [
            {**TestHostedCluster.NODEPOOL, "status": {"replicas": 1}}]).nodepools
        assert upgrade_state.nodepools_short(nodepools, []) == ["lab-upgrade-workers (1/2)"]


def test_a_live_apirequestcount_names_its_caller():
    # Captured on experiment 2's 4.19.53 hosted cluster after three
    # `oc get --raw /apis/admissionregistration.k8s.io/v1beta1/validatingadmissionpolicies`
    # as its admin: the hosted kube-apiserver records them (CONTEXT §8 fact 15).
    count = _fixture("apirequestcount-vap-v1beta1.json")
    assert count["status"]["removedInRelease"] == "1.33"  # a Kubernetes version: OCP 4.20
    (use,) = upgrade_state.api_usage(count["metadata"]["name"], count)
    assert (use.username, use.verbs, use.requests) == ("system:admin", ["list"], 3)
    assert use.user_agent.startswith("oc/")


@pytest.mark.parametrize(("data", "expected"), [
    # As the sandbox's MCE 2.17.3 holds it.
    ({"supported-versions": '{"versions":["4.22","4.21","4.20","4.19","4.18","4.17","4.16","4.15","4.14"]}'},
     (True, ["4.22", "4.21", "4.20", "4.19", "4.18", "4.17", "4.16", "4.15", "4.14"])),
    (None, (False, [])),
    ({"other": "x"}, (False, [])),
    ({"supported-versions": "not json"}, (False, [])),
])
def test_supported_versions(data, expected):
    assert upgrade_state.supported_versions(data) == expected


def test_an_argo_application_names_what_is_out_of_sync():
    application = {"status": {
        "sync": {"status": "OutOfSync", "revision": "abc123"}, "health": {"status": "Healthy"},
        "resources": [{"kind": "HostedCluster", "name": "c1", "status": "OutOfSync"},
                      {"kind": "NodePool", "name": "c1-a", "status": "Synced"}],
    }}
    state = upgrade_state.argo_application_state(application)
    assert (state.found, state.sync_status, state.health_status, state.revision) == (True, "OutOfSync", "Healthy", "abc123")
    assert state.out_of_sync == ["HostedCluster/c1"]
    assert upgrade_state.argo_application_state(None).found is False


class TestArgoSync:
    def _app(self, sync="OutOfSync", revision="old", out=("HostedCluster/c1",), operation=None):
        status = {"sync": {"status": sync, "revision": revision},
                  "resources": [{"kind": r.split("/")[0], "name": r.split("/")[1], "status": "OutOfSync"} for r in out]}
        if operation:
            status["operationState"] = operation
        return {"status": status}

    def test_refused_when_someone_else_s_change_is_pending(self):
        app = self._app(out=("HostedCluster/c1", "ConfigMap/extra"))
        assert "ConfigMap/extra" in upgrade_state.sync_refusal(app, ["HostedCluster/c1"])
        assert upgrade_state.sync_refusal(self._app(), ["HostedCluster/c1"]) is None
        assert upgrade_state.sync_refusal(None, []) == "the Application does not exist"

    def test_already_synced_at_the_revision_is_done(self):
        app = self._app(sync="Synced", revision="sha1", out=(), operation={"phase": "Succeeded"})
        assert upgrade_state.synced_at(app, "sha1") and not upgrade_state.synced_at(app, "sha2")

    def test_the_operation_s_phase_counts_only_for_its_revision(self):
        app = self._app(operation={"phase": "Running", "operation": {"sync": {"revision": "sha1"}}})
        assert upgrade_state.operation_phase(app, "sha1") == "Running"
        assert upgrade_state.operation_phase(app, "sha2") is None

    def test_the_patch_names_the_revision(self):
        assert upgrade_state.sync_operation("sha1", "cluster-upgrade-worker") == {
            "operation": {"initiatedBy": {"username": "cluster-upgrade-worker"}, "sync": {"revision": "sha1"}}}


class TestTheControlPlaneWatch:
    T0 = datetime(2026, 10, 31, 12, 0, tzinfo=UTC)

    def _hc(self, state="Completed", version="4.20.44", succeeding="True", progressing="False"):
        return {"status": {"version": {"history": [{"state": state, "version": version}]}, "conditions": [
            _cond("ClusterVersionSucceeding", succeeding, "" if succeeding == "True" else "MultipleErrors"),
            _cond("ValidReleaseInfo", "True"), _cond("Progressing", progressing), _cond("Available", "True")]}}

    def _cos(self, at, total, version="4.20.44"):
        return [{"status": {"versions": [{"name": "operator", "version": version if i < at else "4.19.53"}]}}
                for i in range(total)]

    def test_done_only_when_the_history_conditions_and_every_operator_agree(self):
        done, snap = upgrade_state.control_plane_progress(self._hc(), self._cos(33, 33), "4.20.44")
        assert done and snap.summary == "history[0] 4.20.44 Completed; 33/33 operators at 4.20.44"
        assert not upgrade_state.control_plane_progress(self._hc(), self._cos(32, 33), "4.20.44")[0]
        assert not upgrade_state.control_plane_progress(self._hc(state="Partial"), self._cos(33, 33), "4.20.44")[0]
        assert not upgrade_state.control_plane_progress(self._hc(), None, "4.20.44")[0]

    def test_early_noise_is_reported_not_final(self):
        done, snap = upgrade_state.control_plane_progress(
            self._hc(state="Partial", succeeding="False"), self._cos(4, 33), "4.20.44")
        assert not done
        assert snap.summary == ("history[0] 4.20.44 Partial; 4/33 operators at 4.20.44; "
                                "ClusterVersionSucceeding=False (MultipleErrors)")

    def test_the_clocks_carry_over_and_reset_only_on_a_change(self):
        _, first = upgrade_state.control_plane_progress(self._hc(state="Partial"), self._cos(4, 33), "4.20.44")
        a = upgrade_state.advance(None, first, self.T0)
        b = upgrade_state.advance(a, first, self.T0 + timedelta(minutes=10))
        assert (b.started_at, b.last_change_at) == (self.T0, self.T0)
        _, moved = upgrade_state.control_plane_progress(self._hc(state="Partial"), self._cos(5, 33), "4.20.44")
        c = upgrade_state.advance(b, moved, self.T0 + timedelta(minutes=11))
        assert (c.started_at, c.last_change_at) == (self.T0, self.T0 + timedelta(minutes=11))

    def test_stall_and_deadline(self):
        from shared.models.cluster_upgrade import ProgressSnapshot, WatchResult
        snap = ProgressSnapshot(started_at=self.T0, last_change_at=self.T0 + timedelta(minutes=100))
        verdict = lambda minutes, done=False: upgrade_state.watch_verdict(  # noqa: E731
            snap, done, self.T0 + timedelta(minutes=minutes), stall_minutes=20, deadline_minutes=180)
        assert verdict(110) is None
        assert verdict(121) is WatchResult.STALLED
        assert verdict(181) is WatchResult.DEADLINE
        assert verdict(181, done=True) is WatchResult.DONE


class TestTheNodePoolWatch:
    T0 = datetime(2026, 10, 31, 6, 0, tzinfo=UTC)

    def _node(self, name, config, state="Done", cordoned_at=None):
        node = {"metadata": {"name": name, "labels": {upgrade_state.NODEPOOL_LABEL: "np"},
                             "annotations": {"machineconfiguration.openshift.io/currentConfig": config,
                                             "machineconfiguration.openshift.io/state": state}},
                "spec": {}}
        if cordoned_at:
            node["spec"] = {"unschedulable": True, "taints": [
                {"key": "node.kubernetes.io/unschedulable", "effect": "NoSchedule", "timeAdded": cordoned_at}]}
        return node

    def _pod(self, name, namespace, node, reason=None, **labels):
        pod = _pod(name, namespace, node, **labels)
        pod["status"] = {"phase": "Running", "containerStatuses": [
            {"state": {"waiting": {"reason": reason}} if reason else {"running": {}}}],
            "conditions": [{"type": "Ready", "status": "False" if reason else "True"}]}
        return pod

    def test_what_experiment_1_s_stuck_drain_looked_like_is_named(self):
        node = self._node("w-2", "old", "Working", cordoned_at="2026-10-31T05:31:00Z")
        pods = [self._pod("downloads-a", "openshift-console", "w-2", "CrashLoopBackOff", app="downloads"),
                self._pod("downloads-b", "openshift-console", "w-2", "CrashLoopBackOff", app="downloads")]
        pdbs = [_pdb("downloads", "openshift-console", 0, app="downloads")]
        blocker = upgrade_state.drain_blocker(node, pods, pdbs, self.T0, 10)
        assert blocker == ("the drain of w-2 has waited 29 min on PDB openshift-console/downloads "
                           "(disruptionsAllowed 0; downloads-a CrashLoopBackOff, downloads-b CrashLoopBackOff)")

    def test_not_yet_long_enough_or_not_blocked_by_a_pdb(self):
        node = self._node("w-2", "old", "Working", cordoned_at="2026-10-31T05:55:00Z")
        pods = [self._pod("dl", "openshift-console", "w-2", "CrashLoopBackOff", app="downloads")]
        pdbs = [_pdb("downloads", "openshift-console", 0, app="downloads")]
        assert upgrade_state.drain_blocker(node, pods, pdbs, self.T0, 10) is None   # 5 minutes only
        later = self.T0 + timedelta(minutes=20)
        assert upgrade_state.drain_blocker(node, pods, [_pdb("downloads", "openshift-console", 1, app="downloads")],
                                           later, 10) is None
        assert upgrade_state.drain_blocker(self._node("w-1", "old"), pods, pdbs, later, 10) is None  # not cordoned

    def test_a_daemonset_pod_never_blocks(self):
        node = self._node("w-2", "old", "Working", cordoned_at="2026-10-31T05:00:00Z")
        pod = self._pod("node-exporter", "openshift-monitoring", "w-2", "CrashLoopBackOff", app="ne")
        pod["metadata"]["ownerReferences"] = [{"kind": "DaemonSet", "controller": True}]
        assert upgrade_state.drain_blocker(node, [pod], [_pdb("ne", "openshift-monitoring", 0, app="ne")],
                                           self.T0, 10) is None

    def test_progress_counts_nodes_at_the_target_config(self):
        nodepool = {"status": {"version": "4.17.58", "conditions": [_cond("UpdatingVersion", "True"),
                                                                     _cond("AllNodesHealthy", "False")]}}
        nodes = [self._node("w-1", "new"), self._node("w-2", "old", "Working", cordoned_at="2026-10-31T05:31:00Z")]
        done, snap = upgrade_state.nodepool_progress(nodepool, nodes, "new", "4.19.53")
        assert not done
        assert snap.summary == ("status.version 4.17.58; 1/2 nodes at the target config; "
                                "w-2 (cordoned, Working); UpdatingVersion=True")

    def test_a_node_no_upgrade_has_touched_counts_only_while_the_pool_is_at_rest(self):
        # Seen live on 4.19.53: a node that joined at install has no machineconfiguration annotations.
        fresh = {"metadata": {"name": "w-1", "labels": {upgrade_state.NODEPOOL_LABEL: "np"}}, "spec": {}}
        nodepool = {"status": {"version": "4.19.53"}}
        assert upgrade_state.nodepool_progress(nodepool, [fresh], "17374a13", "4.19.53",
                                               current_config="17374a13")[1].updated == 1
        assert upgrade_state.nodepool_progress(nodepool, [fresh], "new", "4.20.19",
                                               current_config="17374a13")[1].updated == 0

    def test_done_as_hypershift_reports_it(self):
        nodepool = {"status": {"version": "4.19.53", "conditions": [
            _cond("UpdatingVersion", "False"), _cond("AllNodesHealthy", "True"), _cond("SupportedVersionSkew", "True")]}}
        assert upgrade_state.nodepool_progress(nodepool, [self._node("w-1", "new")], "new", "4.19.53")[0]
        skewed = {"status": {**nodepool["status"], "conditions": nodepool["status"]["conditions"][:2]
                             + [_cond("SupportedVersionSkew", "False")]}}
        assert not upgrade_state.nodepool_progress(skewed, [], "new", "4.19.53")[0]


def test_the_evidence_bundle_names_the_drain_and_stays_under_its_cap():
    from shared.models.cluster_upgrade import ArgoApplicationState

    node = {"metadata": {"name": "w-2"}, "spec": {"unschedulable": True, "taints": [
        {"key": "node.kubernetes.io/unschedulable", "timeAdded": "2026-10-31T05:31:00Z"}]}}
    pod = {"metadata": {"name": "downloads-a", "namespace": "openshift-console", "labels": {"app": "d"}},
           "spec": {"nodeName": "w-2"},
           "status": {"containerStatuses": [{"state": {"waiting": {"reason": "CrashLoopBackOff"}}}]}}
    pdb = {"metadata": {"name": "downloads", "namespace": "openshift-console"},
           "status": {"disruptionsAllowed": 0, "currentHealthy": 0, "expectedPods": 2}}
    drain = upgrade_state.drain_text([node], [pod], [pdb])
    assert "pod openshift-console/downloads-a (CrashLoopBackOff)" in drain
    assert "pdb openshift-console/downloads: disruptionsAllowed 0, healthy 0/2" in drain
    huge = [{"metadata": {"name": f"e{i}"}, "involvedObject": {"kind": "Pod", "name": "x"},
             "reason": "BackOff", "message": "x" * 300} for i in range(5000)]
    sections, truncated = upgrade_state.evidence_sections(
        hosted_cluster=None, nodepools=[], cluster_operators=None, argo=ArgoApplicationState(found=False),
        data_plane_pods=[pod], warnings=huge, drain=drain)
    assert sections["data-plane"] == "openshift-console/downloads-a (CrashLoopBackOff) on w-2"
    assert sections["clusteroperators"] == "unreadable" and "drain" in sections
    assert sum(len(t.encode()) for t in sections.values()) <= upgrade_state.EVIDENCE_LIMIT_BYTES + 100
    assert truncated is False  # the events are capped at 50 lines first


class TestTheNodePoolGate:
    T0 = datetime(2026, 10, 31, 6, 0, tzinfo=UTC)

    def _objects(self, **override):
        objects = {"nodes": [_node("w-1"), _node("w-2")], "pods": [_pod("db-0", "shop", "w-1", app="db")],
                   "pdbs": [_pdb("db", "shop", 1, app="db")],
                   "statefulsets": [{"metadata": {"name": "db", "namespace": "shop"},
                                     "spec": {"replicas": 3}, "status": {"readyReplicas": 3}}]}
        return {**objects, **override}

    def _run(self, polls, soak=30, deadline=90):
        """(minutes after T0, checks) per poll → the verdict at the last poll."""
        snapshot = None
        for minutes, checks in polls:
            now = self.T0 + timedelta(minutes=minutes)
            snapshot = upgrade_state.advance(snapshot, upgrade_state.gate_snapshot(checks), now)
            verdict = upgrade_state.gate_verdict(snapshot, checks, now, soak_minutes=soak, deadline_minutes=deadline)
        return verdict

    def test_a_healthy_pool_passes_every_check(self):
        checks = upgrade_state.gate_checks("np-a", self._objects(), workload=True)
        assert [(c.name, c.passed) for c in checks] == [("nodepool-nodes", True), ("pdbs", True), ("statefulsets", True)]
        assert [c.name for c in upgrade_state.gate_checks("np-a", self._objects(), workload=False)] == ["nodepool-nodes"]

    def test_each_check_says_what_holds_it(self):
        cordoned = _node("w-2", ready="False")
        cordoned["spec"] = {"unschedulable": True}
        checks = upgrade_state.gate_checks("np-a", self._objects(
            nodes=[_node("w-1"), cordoned],
            # db-1 was evicted and waits for a node: its PDB counts though it is on no node yet.
            pods=[_pod("db-1", "shop", None, app="db"), _pod("web-0", "shop", "w-9", app="web")],
            pdbs=[_pdb("db", "shop", 0, app="db"), _pdb("web", "shop", 0, app="web")],
            statefulsets=[{"metadata": {"name": "db", "namespace": "shop"}, "spec": {"replicas": 3},
                           "status": {"readyReplicas": 2}},
                          {"metadata": {"name": "off", "namespace": "shop"}, "spec": {"replicas": 0}, "status": {}}],
        ), workload=True)
        failing = {c.name: c.observed for c in checks if not c.passed}
        assert failing == {"nodepool-nodes": "w-2 (NotReady, cordoned)", "pdbs": "at 0: shop/db",
                           "statefulsets": "shop/db (2/3 ready)"}

    def test_no_nodes_or_no_read_never_passes(self):
        (empty,) = upgrade_state.gate_checks("np-a", self._objects(nodes=[]), workload=False)
        assert not empty.passed and "no node carries hypershift.openshift.io/nodePool=np-a" in empty.observed
        (unread,) = upgrade_state.gate_checks("np-a", None, workload=True, unreadable="the hosted API was unreadable: 503")
        assert not unread.passed and unread.observed.endswith("503")

    def test_the_soak_runs_from_when_every_check_last_started_passing(self):
        good = upgrade_state.gate_checks("np-a", self._objects(), workload=True)
        bad = upgrade_state.gate_checks("np-a", self._objects(nodes=[_node("w-1", ready="False")]), workload=True)
        assert self._run([(0, bad), (5, good), (34, good)]) is None            # 29 min of 30
        assert self._run([(0, bad), (5, good), (35, good)]).result.value == "passed"
        assert self._run([(0, good), (20, bad), (25, good), (54, good)]) is None  # a break restarts it
        assert self._run([(0, good)], soak=0).result.value == "passed"         # generic: no soak

    def test_the_deadline_names_what_still_fails_or_the_unfinished_soak(self):
        good = upgrade_state.gate_checks("np-a", self._objects(), workload=True)
        bad = upgrade_state.gate_checks("np-a", self._objects(nodes=[_node("w-1", ready="False")]), workload=True)
        outcome = self._run([(0, bad), (90, bad)])
        assert outcome.result.value == "deadline" and [c.name for c in outcome.failing_checks] == ["nodepool-nodes"]
        outcome = self._run([(0, bad), (80, good), (90, good)])
        (soak,) = outcome.failing_checks
        assert (soak.name, soak.observed) == ("soak", "passing for 10 min")
