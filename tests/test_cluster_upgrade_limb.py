"""The cluster-upgrade limb's activities (activities/cluster_upgrade/).

The MCE's API (cluster_api) and the hosted cluster's (open_hosted_api) are
replaced by fakes holding plain dicts, as the server-lifecycle limb tests do; no
cluster, no Temporal worker. What the objects mean is tested in
test_cluster_upgrade_state.py against shapes captured live.
"""

from __future__ import annotations

import base64
import contextlib

import pytest

import activities.cluster_upgrade.activities as limb
from activities.cluster_upgrade import hosted_upgrade_api
from activities.server_lifecycle import cluster_api
from shared.exceptions import AdminGateNotFoundError, HostedClusterAccessError
from shared.models.cluster_upgrade import AdminAckRequest, GiveAdminAckRequest, HostedClusterRequest

GATE = "ack-4.19-admissionregistration-v1beta1-api-removals-in-4.20"
VAP = "validatingadmissionpolicies.v1beta1.admissionregistration.k8s.io"


def _hc(name="lab-upgrade", namespace="hcp-lab-upgrade"):
    return {
        "metadata": {"name": name, "namespace": namespace},
        "status": {"kubeconfig": {"name": f"{name}-admin-kubeconfig"},
                   "version": {"history": [{"state": "Completed", "version": "4.19.53"}]}},
    }


class _FakeMce:
    def __init__(self, hosted_clusters, nodepools=()):
        self.hosted_clusters = list(hosted_clusters)
        self.nodepools = list(nodepools)
        self.secret_reads = []

    async def list_cluster_custom_objects(self, *, group, version, plural, kind):
        assert plural == "hostedclusters"
        return self.hosted_clusters

    async def list_custom_objects(self, *, group, version, plural, kind, namespace):
        assert plural == "nodepools"
        return [np for np in self.nodepools if np["metadata"]["namespace"] == namespace]

    async def read_secret_data(self, namespace, name):
        self.secret_reads.append((namespace, name))
        return {"kubeconfig": base64.b64encode(b"apiVersion: v1\nkind: Config\n").decode()}


@pytest.fixture
def mce(monkeypatch):
    fake = _FakeMce([_hc()])
    for fn in ("list_cluster_custom_objects", "list_custom_objects", "read_secret_data"):
        monkeypatch.setattr(cluster_api, fn, getattr(fake, fn))
    return fake


class TestFindingTheHostedCluster:
    async def test_found_by_name_in_its_own_namespace_with_its_nodepools(self, mce):
        mce.nodepools = [
            {"metadata": {"name": "np", "namespace": "hcp-lab-upgrade"}, "spec": {"clusterName": "lab-upgrade"},
             "status": {"version": "4.19.53"}},
            {"metadata": {"name": "other", "namespace": "hcp-other"}, "spec": {"clusterName": "other"}},
        ]
        state = await limb.read_hosted_cluster(HostedClusterRequest(mce_cluster="m", cluster="lab-upgrade"))
        assert state.found and state.namespace == "hcp-lab-upgrade" and state.control_plane_version == "4.19.53"
        assert [np.name for np in state.nodepools] == ["np"]

    async def test_absent_is_not_found_not_an_error(self, mce):
        state = await limb.read_hosted_cluster(HostedClusterRequest(mce_cluster="m", cluster="nope"))
        assert state.found is False

    async def test_two_of_one_name_is_refused(self, mce):
        mce.hosted_clusters = [_hc(), _hc(namespace="clusters")]
        with pytest.raises(HostedClusterAccessError, match="2 HostedClusters are named lab-upgrade"):
            await limb.read_hosted_cluster(HostedClusterRequest(mce_cluster="m", cluster="lab-upgrade"))


class TestAdminAckActivities:
    async def test_read_goes_to_the_control_plane_service_with_the_admin_kubeconfig(self, mce, monkeypatch):
        seen = {}

        async def _read(kubeconfig, server, cluster, apis):
            seen.update(server=server, cluster=cluster, apis=apis, kubeconfig=kubeconfig)
            return {"cluster_version": {}, "cluster_operators": [], "admin_gates": {"data": {GATE: "m"}},
                    "admin_acks": {"data": None}, "api_request_counts": {VAP: None}}

        monkeypatch.setattr(hosted_upgrade_api, "read_upgrade_objects", _read)
        state = await limb.read_admin_acks(AdminAckRequest(mce_cluster="m", cluster="lab-upgrade", apis=[VAP]))
        assert state.reachable and state.gates == {GATE: "m"} and state.usage == []
        # HyperShift's control-plane namespace: <namespace>-<name>.
        assert seen["server"] == "https://kube-apiserver.hcp-lab-upgrade-lab-upgrade.svc:6443"
        assert seen["apis"] == [VAP] and seen["kubeconfig"].startswith("apiVersion: v1")
        assert mce.secret_reads == [("hcp-lab-upgrade", "lab-upgrade-admin-kubeconfig")]

    async def test_no_hosted_cluster_reads_as_unreachable(self, mce):
        state = await limb.read_admin_acks(AdminAckRequest(mce_cluster="m", cluster="nope", apis=[VAP]))
        assert state.reachable is False


class _FakeApiClient:
    def sanitize_for_serialization(self, obj):
        return obj


class _FakeHostedCore:
    """The hosted cluster's ConfigMaps, as dicts."""

    def __init__(self, gates, acks):
        self.maps = {("openshift-config-managed", "admin-gates"): {"data": gates}}
        if acks is not None:
            self.maps[("openshift-config", "admin-acks")] = {"data": acks}
        self.writes = []

    def read_namespaced_config_map(self, name, namespace, _request_timeout=None):
        from kubernetes import client as k8s_client
        if (namespace, name) not in self.maps:
            raise k8s_client.ApiException(status=404)
        return self.maps[(namespace, name)]

    def patch_namespaced_config_map(self, name, namespace, body, _request_timeout=None):
        self.writes.append(("patch", namespace, name, body))
        self.maps[(namespace, name)]["data"] = {**(self.maps[(namespace, name)].get("data") or {}), **body["data"]}

    def create_namespaced_config_map(self, namespace, body, _request_timeout=None):
        self.writes.append(("create", namespace, body.metadata.name, body.data))
        self.maps[(namespace, body.metadata.name)] = {"data": dict(body.data)}


class TestTheAckWrite:
    def _write(self, monkeypatch, gates, acks, gate=GATE):
        core = _FakeHostedCore(gates, acks)

        @contextlib.contextmanager
        def _open(kubeconfig, server, cluster):
            yield _FakeApiClient(), core

        monkeypatch.setattr(hosted_upgrade_api, "open_hosted_api", _open)
        return core, hosted_upgrade_api._give_admin_ack("kubeconfig", "https://kas", "lab-upgrade", gate)

    def test_one_key_merged_into_the_acks(self, monkeypatch):
        core, changed = self._write(monkeypatch, {GATE: "m"}, {"ack-4.18-old": "true"})
        assert changed is True
        assert core.writes == [("patch", "openshift-config", "admin-acks", {"data": {GATE: "true"}})]
        assert core.maps[("openshift-config", "admin-acks")]["data"] == {"ack-4.18-old": "true", GATE: "true"}

    def test_already_acked_writes_nothing(self, monkeypatch):
        core, changed = self._write(monkeypatch, {GATE: "m"}, {GATE: "true"})
        assert changed is False and core.writes == []

    def test_a_gate_the_cluster_does_not_hold_is_refused(self, monkeypatch):
        with pytest.raises(AdminGateNotFoundError, match="nothing was written"):
            self._write(monkeypatch, {GATE: "m"}, {}, gate="ack-4.19-something-else")

    def test_a_missing_acks_configmap_is_recreated_with_only_this_key(self, monkeypatch):
        core, changed = self._write(monkeypatch, {GATE: "m"}, None)
        assert changed is True and core.writes == [("create", "openshift-config", "admin-acks", {GATE: "true"})]


class TestHostedHealthActivity:
    async def test_every_reading_lands_in_its_field(self, mce, monkeypatch):
        from shared.models.cluster_upgrade import HostedHealthRequest

        mce.nodepools = [{"metadata": {"name": "np", "namespace": "hcp-lab-upgrade"},
                          "spec": {"clusterName": "lab-upgrade", "replicas": 2}, "status": {"replicas": 1}}]

        async def _upgrade(kubeconfig, server, cluster, apis):
            return {"cluster_version": {"status": {"conditions": [
                        {"type": "Upgradeable", "status": "False", "reason": "AdminAckRequired", "message": "ack"}]}},
                    "cluster_operators": [], "admin_gates": None, "admin_acks": None, "api_request_counts": {}}

        async def _health(kubeconfig, server, cluster):
            node = {"metadata": {"name": "w-1", "labels": {"hypershift.openshift.io/nodePool": "np"}},
                    "status": {"conditions": [{"type": "Ready", "status": "False"}]}}
            return {"nodes": [node], "csrs": [{}], "pdbs": [], "pods": [], "webhooks": [], "endpoints": [],
                    "image_digest_mirror_sets": [{"spec": {"imageDigestMirrors": [{"mirrors": ["m/x"]}]}}]}

        monkeypatch.setattr(hosted_upgrade_api, "read_upgrade_objects", _upgrade)
        monkeypatch.setattr(hosted_upgrade_api, "read_health_objects", _health)
        health = await limb.read_hosted_health(
            HostedHealthRequest(mce_cluster="m", cluster="lab-upgrade", nodepools=["np"]))
        assert health.reachable and health.upgradeable_reasons == ["AdminAckRequired"]
        assert health.nodes_not_ready == ["w-1"] and health.pending_csrs == 1
        assert health.nodepools_short == ["np (1/2)"] and health.idms_mirrors == ["m/x"]

    async def test_an_unreachable_api_server_is_reported_not_retried(self, mce, monkeypatch):
        from shared.exceptions import HostedClusterUnreachableError
        from shared.models.cluster_upgrade import HostedHealthRequest

        async def _down(*args):
            raise HostedClusterUnreachableError("connection refused")

        monkeypatch.setattr(hosted_upgrade_api, "read_upgrade_objects", _down)
        health = await limb.read_hosted_health(HostedHealthRequest(mce_cluster="m", cluster="lab-upgrade"))
        assert health.reachable is False


class TestTheArgoSync:
    """sync_argo_application on a scripted Application: each read takes the next state."""

    def _app(self, sync="OutOfSync", revision="old", out=("HostedCluster/c1",), phase=None, op_revision="sha1",
             message=""):
        status = {"sync": {"status": sync, "revision": revision},
                  "resources": [{"kind": r.split("/")[0], "name": r.split("/")[1], "status": "OutOfSync"} for r in out]}
        if phase:
            status["operationState"] = {"phase": phase, "message": message,
                                        "operation": {"sync": {"revision": op_revision}}}
        return {"status": status}

    async def _run(self, monkeypatch, states, allowed=("HostedCluster/c1",)):
        from shared.models.cluster_upgrade import ArgoSyncRequest

        reads, patches = list(states), []

        async def _read(**kwargs):
            assert kwargs["namespace"] == "openshift-gitops" and kwargs["name"] == "c1"
            return reads.pop(0) if len(reads) > 1 else reads[0]

        async def _patch(**kwargs):
            patches.append(kwargs["body"])
            return True

        monkeypatch.setattr(cluster_api, "read_custom_object", _read)
        monkeypatch.setattr(cluster_api, "patch_custom_object", _patch)
        monkeypatch.setattr(limb, "_SYNC_POLL_SECONDS", 0)
        monkeypatch.setattr(limb.activity, "heartbeat", lambda *a: None)
        request = ArgoSyncRequest(mce_cluster="m", cluster="c1", revision="sha1", allowed_out_of_sync=list(allowed))
        return patches, await limb.sync_argo_application(request)

    async def test_it_asks_for_the_revision_and_waits_for_the_operation(self, monkeypatch):
        patches, result = await self._run(monkeypatch, [
            self._app(), self._app(phase="Running"), self._app(sync="Synced", revision="sha1", out=(), phase="Succeeded"),
        ])
        assert result.phase == "Succeeded"
        assert patches == [{"operation": {"initiatedBy": {"username": "cluster-upgrade-worker"},
                                          "sync": {"revision": "sha1"}}}]

    async def test_already_synced_at_the_revision_asks_nothing(self, monkeypatch):
        patches, result = await self._run(monkeypatch, [self._app(sync="Synced", revision="sha1", out=(), phase="Succeeded")])
        assert result.phase == "Succeeded" and patches == []

    async def test_an_operation_already_running_is_waited_for_not_started_again(self, monkeypatch):
        patches, result = await self._run(monkeypatch, [
            self._app(phase="Running"), self._app(sync="Synced", revision="sha1", out=(), phase="Succeeded"),
        ])
        assert patches == [] and result.phase == "Succeeded"

    async def test_someone_else_s_pending_change_is_refused_before_anything_is_asked(self, monkeypatch):
        from shared.exceptions import ArgoSyncRefusedError

        with pytest.raises(ArgoSyncRefusedError, match="ConfigMap/extra"):
            await self._run(monkeypatch, [self._app(out=("HostedCluster/c1", "ConfigMap/extra"))])

    async def test_a_failed_operation_is_a_retryable_error_naming_argo_s_message(self, monkeypatch):
        from shared.exceptions import ArgoSyncFailedError

        with pytest.raises(ArgoSyncFailedError, match="ended Failed: one or more objects failed to apply"):
            await self._run(monkeypatch, [
                self._app(), self._app(phase="Failed", message="one or more objects failed to apply"),
            ])



class TestTheControlPlaneWatch:
    def _wire(self, monkeypatch, polls, heartbeat_details=()):
        """Each poll takes the next (hosted cluster, operators) pair; heartbeats are recorded."""
        from types import SimpleNamespace

        polls, beats = list(polls), []

        async def _hosted(cluster):
            return polls[0][0]

        async def _operators(hosted, cluster):
            return polls.pop(0)[1] if len(polls) > 1 else polls[0][1]

        monkeypatch.setattr(limb, "_hosted_cluster", _hosted)
        monkeypatch.setattr(limb, "_hosted_operators", _operators)
        monkeypatch.setattr(limb, "_WATCH_POLL_SECONDS", 0)
        monkeypatch.setattr(limb.activity, "heartbeat", lambda *details: beats.append(details))
        monkeypatch.setattr(limb.activity, "info", lambda: SimpleNamespace(heartbeat_details=list(heartbeat_details)))
        return beats

    def _hc(self, state):
        return {"status": {"version": {"history": [{"state": state, "version": "4.20.44"}]}, "conditions": [
            {"type": "ClusterVersionSucceeding", "status": "True"}, {"type": "ValidReleaseInfo", "status": "True"},
            {"type": "Progressing", "status": "False"}]}}

    def _cos(self, version):
        return [{"status": {"versions": [{"name": "operator", "version": version}]}}]

    async def test_done_once_the_control_plane_runs_the_version(self, monkeypatch):
        from shared.models.cluster_upgrade import WatchControlPlaneRequest, WatchResult

        beats = self._wire(monkeypatch, [(self._hc("Partial"), self._cos("4.19.53")),
                                         (self._hc("Completed"), self._cos("4.20.44"))])
        outcome = await limb.watch_control_plane(WatchControlPlaneRequest(
            mce_cluster="m", cluster="c1", version="4.20.44", stall_minutes=20, deadline_minutes=180))
        assert outcome.result is WatchResult.DONE and outcome.snapshot.updated == 1
        assert len(beats) == 2 and beats[0][0]["summary"].startswith("history[0] 4.20.44 Partial")

    async def test_a_retry_keeps_the_clocks_of_the_last_heartbeat(self, monkeypatch):
        from datetime import UTC, datetime, timedelta

        from shared.models.cluster_upgrade import ProgressSnapshot, WatchControlPlaneRequest, WatchResult

        hc, cos = self._hc("Partial"), self._cos("4.19.53")
        from activities.cluster_upgrade import upgrade_state
        _, seen = upgrade_state.control_plane_progress(hc, cos, "4.20.44")
        long_ago = datetime.now(UTC) - timedelta(minutes=25)
        last = seen.model_copy(update={"started_at": long_ago, "last_change_at": long_ago})
        self._wire(monkeypatch, [(hc, cos)], heartbeat_details=[last.model_dump(mode="json")])
        outcome = await limb.watch_control_plane(WatchControlPlaneRequest(
            mce_cluster="m", cluster="c1", version="4.20.44", stall_minutes=20, deadline_minutes=180))
        # Nothing changed since the dead worker's last look 25 minutes ago: a stall, at once.
        assert outcome.result is WatchResult.STALLED
        assert ProgressSnapshot.model_validate(outcome.snapshot).started_at == long_ago


class TestTheNodePoolWatch:
    async def test_a_drain_a_pdb_refuses_ends_the_watch_early_with_its_cause(self, monkeypatch):
        from datetime import UTC, datetime, timedelta
        from types import SimpleNamespace

        from shared.models.cluster_upgrade import WatchNodePoolRequest, WatchResult

        cordoned = (datetime.now(UTC) - timedelta(minutes=12)).strftime("%Y-%m-%dT%H:%M:%SZ")
        node = {"metadata": {"name": "w-2", "annotations": {"machineconfiguration.openshift.io/state": "Working"}},
                "spec": {"unschedulable": True, "taints": [
                    {"key": "node.kubernetes.io/unschedulable", "timeAdded": cordoned}]}}
        pod = {"metadata": {"name": "downloads-a", "namespace": "openshift-console", "labels": {"app": "downloads"}},
               "spec": {"nodeName": "w-2"},
               "status": {"containerStatuses": [{"state": {"waiting": {"reason": "CrashLoopBackOff"}}}]}}
        pdb = {"metadata": {"name": "downloads", "namespace": "openshift-console"},
               "spec": {"selector": {"matchLabels": {"app": "downloads"}}}, "status": {"disruptionsAllowed": 0}}
        nodepool = {"status": {"version": "4.17.58", "conditions": [{"type": "UpdatingVersion", "status": "True"}]}}

        async def _view(request):
            return nodepool, {"hypershift.openshift.io/nodePoolTargetConfigVersion": "new",
                              "hypershift.openshift.io/nodePoolCurrentConfigVersion": "old"}, \
                {"nodes": [node], "pods": [pod], "pdbs": [pdb]}

        monkeypatch.setattr(limb, "_nodepool_view", _view)
        monkeypatch.setattr(limb.activity, "heartbeat", lambda *a: None)
        monkeypatch.setattr(limb.activity, "info", lambda: SimpleNamespace(heartbeat_details=[]))
        outcome = await limb.watch_nodepool(WatchNodePoolRequest(
            mce_cluster="m", cluster="c1", nodepool="np", version="4.19.53",
            stall_minutes=30, deadline_minutes=120, drain_blocked_minutes=10))
        assert outcome.result is WatchResult.STALLED
        assert "PDB openshift-console/downloads (disruptionsAllowed 0; downloads-a CrashLoopBackOff)" in outcome.snapshot.summary


class TestTheNodePoolGate:
    def _quiet(self, monkeypatch):
        from types import SimpleNamespace

        monkeypatch.setattr(limb.activity, "heartbeat", lambda *a: None)
        monkeypatch.setattr(limb.activity, "info", lambda: SimpleNamespace(heartbeat_details=[]))

    def _request(self, **fields):
        from shared.models.cluster_upgrade import NodePoolGateRequest

        return NodePoolGateRequest(**{"mce_cluster": "m", "cluster": "lab-upgrade", "nodepool": "np", "version": "4.19.53",
                                      "workload_checks": False, "soak_minutes": 0, "deadline_minutes": 60, **fields})

    async def test_a_healthy_pool_with_no_soak_passes_at_once(self, mce, monkeypatch):
        seen = {}

        async def _read(kubeconfig, server, cluster, nodepool, label, workload):
            seen.update(server=server, nodepool=nodepool, label=label, workload=workload)
            return {"nodes": [{"metadata": {"name": "w-1"}, "spec": {},
                               "status": {"conditions": [{"type": "Ready", "status": "True"}]}}]}

        monkeypatch.setattr(hosted_upgrade_api, "read_gate_objects", _read)
        self._quiet(monkeypatch)
        outcome = await limb.watch_nodepool_gate(self._request())
        assert outcome.result.value == "passed" and outcome.failing_checks == []
        assert seen == {"server": "https://kube-apiserver.hcp-lab-upgrade-lab-upgrade.svc:6443", "nodepool": "np",
                        "label": "hypershift.openshift.io/nodePool", "workload": False}

    async def test_a_declared_check_holds_the_gate_until_its_deadline(self, mce, monkeypatch):
        from shared.models.cluster_upgrade import ReadinessCheck

        async def _read(*args):
            return {"nodes": [{"metadata": {"name": "w-1"}, "spec": {},
                               "status": {"conditions": [{"type": "Ready", "status": "True"}]}}],
                    "pods": [], "pdbs": [], "statefulsets": []}

        monkeypatch.setattr(hosted_upgrade_api, "read_gate_objects", _read)
        self._quiet(monkeypatch)
        outcome = await limb.watch_nodepool_gate(self._request(
            workload_checks=True, deadline_minutes=0,
            readiness_checks=[ReadinessCheck(name="api", http="https://app.example/healthz", expect_status=200)]))
        assert outcome.result.value == "deadline"
        assert [c.name for c in outcome.failing_checks] == ["readiness:api"]

    async def test_an_unreadable_cluster_fails_the_platform_check_saying_why(self, mce, monkeypatch):
        from shared.exceptions import HostedClusterUnreachableError

        async def _read(*args):
            raise HostedClusterUnreachableError("hosted cluster lab-upgrade: 503 on a list of Nodes")

        monkeypatch.setattr(hosted_upgrade_api, "read_gate_objects", _read)
        self._quiet(monkeypatch)
        outcome = await limb.watch_nodepool_gate(self._request(deadline_minutes=0))
        (check,) = outcome.failing_checks
        assert check.name == "nodepool-nodes" and "503 on a list of Nodes" in check.observed


def _action(kind, target, action_id="a-1", **args):
    from shared.models.cluster_upgrade import ProposedAction

    return ProposedAction(action_id=action_id, kind=kind, target=target, args=args)


class TestTheActionCatalog:
    """Tier 2 holds the tier-1 kinds with their allowlists (DESIGN §8.3, AGENT.md #22, #25)."""

    @pytest.mark.parametrize(("kind", "target", "where"), [
        ("delete_pod", "openshift-console/downloads-a", ("openshift-console", "downloads-a")),
        ("delete_hcp_pod", "cluster-version-operator-7d9f", (None, "cluster-version-operator-7d9f")),
        ("rerun_cronjob", "openshift-operator-lifecycle-manager/collect-profiles",
         ("openshift-operator-lifecycle-manager", "collect-profiles")),
    ])
    def test_in_the_catalog(self, kind, target, where):
        from activities.cluster_upgrade import action_catalog

        action = action_catalog.check(_action(kind, target, uid="u-1"))
        assert (action.namespace, action.name) == where
        assert action.uid == (None if kind == "rerun_cronjob" else "u-1")

    @pytest.mark.parametrize(("kind", "target", "why"), [
        ("delete_deployment", "openshift-console/console", "not in the catalog"),
        ("delete_pod", "shop/db-0", "only namespaces openshift-*"),
        ("delete_pod", "openshift-ovn-kubernetes/ovnkube-node-x", "never deleted"),
        ("delete_pod", "openshift-multus/multus-x", "never deleted"),
        ("delete_pod", "downloads-a", "<namespace>/<name>"),
        ("delete_pod", "openshift-console/", "<namespace>/<name>"),
        ("delete_hcp_pod", "etcd-0", "never deleted"),
        ("delete_hcp_pod", "kube-apiserver-5f6d", "never deleted"),
        ("delete_hcp_pod", "ns/pod", "pod in the cluster's HCP namespace"),
        ("rerun_cronjob", "default/backup", "only namespaces openshift-*"),
    ])
    def test_refused_whoever_approved_it(self, kind, target, why):
        from activities.cluster_upgrade import action_catalog
        from shared.exceptions import ProposedActionRefusedError

        with pytest.raises(ProposedActionRefusedError, match=why):
            action_catalog.check(_action(kind, target))

    def test_a_rerun_job_name_is_one_per_action_and_short(self):
        from activities.cluster_upgrade import action_catalog

        name = action_catalog.rerun_job_name("x" * 60, "a-1")
        assert name == action_catalog.rerun_job_name("x" * 60, "a-1") != action_catalog.rerun_job_name("x" * 60, "a-2")
        assert len(name) <= 52 and name.startswith("x" * 37 + "-rerun-")


class _FakeActionCore:
    """Pods and CronJobs/Jobs of a hosted cluster, enough for the tier-2 writes."""

    def __init__(self, pods=None, cronjobs=None):
        self.pods = dict(pods or {})  # (namespace, name) -> uid
        self.cronjobs = dict(cronjobs or {})
        self.jobs = {}

    def delete_namespaced_pod(self, name, namespace, body=None, _request_timeout=None):
        from kubernetes import client as k8s_client
        if (namespace, name) not in self.pods:
            raise k8s_client.ApiException(status=404)
        if body is not None and body.preconditions.uid != self.pods[(namespace, name)]:
            raise k8s_client.ApiException(status=409)
        del self.pods[(namespace, name)]

    def read_namespaced_cron_job(self, name, namespace, _request_timeout=None):
        from kubernetes import client as k8s_client
        if (namespace, name) not in self.cronjobs:
            raise k8s_client.ApiException(status=404)
        return self.cronjobs[(namespace, name)]

    def create_namespaced_job(self, namespace, body, _request_timeout=None):
        from kubernetes import client as k8s_client
        if (namespace, body["metadata"]["name"]) in self.jobs:
            raise k8s_client.ApiException(status=409)
        self.jobs[(namespace, body["metadata"]["name"])] = body


class TestTheActionWrites:
    def _open(self, monkeypatch, core):
        from kubernetes import client as k8s_client

        @contextlib.contextmanager
        def _open(kubeconfig, server, cluster):
            yield _FakeApiClient(), core

        monkeypatch.setattr(hosted_upgrade_api, "open_hosted_api", _open)
        monkeypatch.setattr(k8s_client, "BatchV1Api", lambda api_client: core)

    def test_a_pod_delete_is_idempotent_and_honours_the_uid(self, monkeypatch):
        core = _FakeActionCore(pods={("openshift-console", "downloads-a"): "u-1", ("openshift-console", "sts-0"): "u-new"})
        self._open(monkeypatch, core)
        delete = hosted_upgrade_api._delete_pod
        assert delete("k", "s", "c", "openshift-console", "downloads-a", "u-1") == "deleted"
        assert delete("k", "s", "c", "openshift-console", "downloads-a", "u-1") == "gone"
        # Recreated under the same name since the agent saw it: left alone.
        assert delete("k", "s", "c", "openshift-console", "sts-0", "u-old") == "replaced"
        assert ("openshift-console", "sts-0") in core.pods

    def test_a_cronjob_rerun_makes_the_job_oc_would_and_only_once(self, monkeypatch):
        cronjob = {"metadata": {"name": "collect-profiles", "namespace": "openshift-olm", "uid": "cj-uid"},
                   "spec": {"jobTemplate": {"metadata": {"labels": {"app": "p"}, "annotations": {"a": "b"}},
                                            "spec": {"template": {"spec": {"containers": [{"name": "c"}]}}}}}}
        core = _FakeActionCore(cronjobs={("openshift-olm", "collect-profiles"): cronjob})
        self._open(monkeypatch, core)
        rerun = hosted_upgrade_api._rerun_cronjob
        assert rerun("k", "s", "c", "openshift-olm", "collect-profiles", "collect-profiles-rerun-1a2b3c4d") == "created"
        assert rerun("k", "s", "c", "openshift-olm", "collect-profiles", "collect-profiles-rerun-1a2b3c4d") == "exists"
        assert rerun("k", "s", "c", "openshift-olm", "gone", "gone-rerun-1a2b3c4d") == "missing"
        (job,) = core.jobs.values()
        assert job["spec"] == cronjob["spec"]["jobTemplate"]["spec"]
        assert job["metadata"]["labels"] == {"app": "p"}
        assert job["metadata"]["annotations"] == {"a": "b", "cronjob.kubernetes.io/instantiate": "manual"}
        assert job["metadata"]["ownerReferences"] == [{"apiVersion": "batch/v1", "kind": "CronJob",
                                                       "name": "collect-profiles", "uid": "cj-uid", "controller": True}]


class TestExecuteProposedAction:
    def _request(self, kind, target, **args):
        from shared.models.cluster_upgrade import ProposedActionRequest

        return ProposedActionRequest(mce_cluster="m", cluster="lab-upgrade", action=_action(kind, target, **args),
                                     approved_by="alice")

    async def test_a_hosted_pod_goes_to_the_control_plane_service(self, mce, monkeypatch):
        seen = {}

        async def _delete(kubeconfig, server, cluster, namespace, name, uid):
            seen.update(server=server, namespace=namespace, name=name, uid=uid)
            return "deleted"

        monkeypatch.setattr(hosted_upgrade_api, "delete_pod", _delete)
        result = await limb.execute_proposed_action(self._request("delete_pod", "openshift-console/downloads-a", uid="u-1"))
        assert (result.action_id, result.executed, result.detail) == ("a-1", True, "deleted")
        assert seen == {"server": "https://kube-apiserver.hcp-lab-upgrade-lab-upgrade.svc:6443",
                        "namespace": "openshift-console", "name": "downloads-a", "uid": "u-1"}

    async def test_an_hcp_pod_is_deleted_in_the_hcp_namespace_on_the_mce(self, mce, monkeypatch):
        seen = []

        async def _delete(namespace, name, uid):
            seen.append((namespace, name, uid))
            return "gone"

        monkeypatch.setattr(cluster_api, "delete_pod", _delete)
        result = await limb.execute_proposed_action(self._request("delete_hcp_pod", "cluster-version-operator-7d9f"))
        assert seen == [("hcp-lab-upgrade-lab-upgrade", "cluster-version-operator-7d9f", None)]
        assert (result.executed, result.detail) == (False, "not executed: already gone")

    async def test_a_rerun_names_its_job(self, mce, monkeypatch):
        async def _rerun(kubeconfig, server, cluster, namespace, name, job):
            return "exists"

        monkeypatch.setattr(hosted_upgrade_api, "rerun_cronjob", _rerun)
        result = await limb.execute_proposed_action(self._request("rerun_cronjob", "openshift-olm/collect-profiles"))
        assert result.executed and result.detail.startswith("created Job collect-profiles-rerun-")
        assert result.detail.endswith("(found on retry)")

    async def test_refused_before_anything_is_read(self, mce, monkeypatch):
        from shared.exceptions import ProposedActionRefusedError

        with pytest.raises(ProposedActionRefusedError, match="only namespaces openshift-"):
            await limb.execute_proposed_action(self._request("delete_pod", "shop/db-0"))
        assert mce.secret_reads == []
