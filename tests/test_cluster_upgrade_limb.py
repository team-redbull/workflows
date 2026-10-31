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
from shared.exceptions import (
    AdminAckNotAppliedError,
    AdminGateNotFoundError,
    AmbiguousHostedClusterError,
    HostedClusterAccessError,
    HostedClusterNotFoundError,
    HostedClusterUnreachableError,
)
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
        self.lists = 0
        self.gets = []

    async def list_cluster_custom_objects(self, *, group, version, plural, kind):
        assert plural == "hostedclusters"
        self.lists += 1
        return self.hosted_clusters

    async def read_custom_object(self, *, group, version, plural, kind, namespace, name):
        self.gets.append((plural, namespace, name))
        if plural != "hostedclusters":
            return None  # no Application in these tests
        return next((hc for hc in self.hosted_clusters
                     if hc["metadata"]["name"] == name and hc["metadata"]["namespace"] == namespace), None)

    async def list_custom_objects(self, *, group, version, plural, kind, namespace):
        assert plural == "nodepools"
        return [np for np in self.nodepools if np["metadata"]["namespace"] == namespace]

    async def read_secret_data(self, namespace, name):
        self.secret_reads.append((namespace, name))
        return {"kubeconfig": base64.b64encode(b"apiVersion: v1\nkind: Config\n").decode()}


@pytest.fixture
def mce(monkeypatch):
    fake = _FakeMce([_hc()])
    for fn in ("list_cluster_custom_objects", "list_custom_objects", "read_secret_data", "read_custom_object"):
        monkeypatch.setattr(cluster_api, fn, getattr(fake, fn))
    return fake


class TestTheDay2Applications:
    async def test_the_cluster_s_day2_applications_by_their_label(self, monkeypatch):
        seen = {}

        async def _list(**kwargs):
            seen.update(kwargs)
            return [{"metadata": {"name": "b-day2"}}, {"metadata": {"name": "a-day2"}}, {"metadata": {}}]

        monkeypatch.setattr(cluster_api, "list_custom_objects", _list)
        monkeypatch.setattr(limb, "_settings", limb._settings.model_copy(update={"argo_namespace": "gitops-hc"}))
        result = await limb.list_day2_applications(HostedClusterRequest(mce_cluster="m", cluster="lab-upgrade"))
        assert result.names == ["a-day2", "b-day2"]
        assert (seen["plural"], seen["namespace"]) == ("applications", "gitops-hc")
        assert seen["label_selector"] == "day2.gitops/cluster=lab-upgrade"


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
        with pytest.raises(AmbiguousHostedClusterError, match="2 HostedClusters are named lab-upgrade"):
            await limb.read_hosted_cluster(HostedClusterRequest(mce_cluster="m", cluster="lab-upgrade"))


# The hostedclusters-setup chart's names for the day1 file `herzi.yaml`.
OWN = {"hosted_cluster": "openshift-herzi", "hosted_cluster_namespace": "hcp-herzi"}


class TestTheHostedClusterByItsOwnName:
    """A run sends the HostedCluster's own name and namespace from day1; the file's name stays `cluster`."""

    async def test_read_by_name_in_its_namespace_with_the_nodepools_naming_it(self, mce):
        mce.hosted_clusters = [_hc("openshift-herzi", "hcp-herzi"), _hc("herzi", "hcp-herzi")]
        mce.nodepools = [
            {"metadata": {"name": "herzi-np", "namespace": "hcp-herzi"}, "spec": {"clusterName": "openshift-herzi"},
             "status": {"version": "4.19.53"}},
            {"metadata": {"name": "stray", "namespace": "hcp-herzi"}, "spec": {"clusterName": "herzi"}},
        ]
        state = await limb.read_hosted_cluster(HostedClusterRequest(mce_cluster="m", cluster="herzi", **OWN))
        assert state.found and state.namespace == "hcp-herzi"
        assert [np.name for np in state.nodepools] == ["herzi-np"]
        # A get in the namespace day1 names: no listing across the MCE.
        assert mce.gets == [("hostedclusters", "hcp-herzi", "openshift-herzi")] and mce.lists == 0

    async def test_absent_under_its_own_name_is_not_found_even_if_one_bears_the_file_s(self, mce):
        mce.hosted_clusters = [_hc("herzi", "hcp-herzi")]
        state = await limb.read_hosted_cluster(HostedClusterRequest(mce_cluster="m", cluster="herzi", **OWN))
        assert state.found is False

    async def test_the_admin_kubeconfig_and_the_control_plane_follow_the_hostedcluster(self, mce, monkeypatch):
        mce.hosted_clusters = [_hc("openshift-herzi", "hcp-herzi")]
        seen = {}

        async def _read(kubeconfig, server, cluster, apis):
            seen.update(server=server)
            return {"cluster_version": {}, "admin_gates": {"data": {}},
                    "admin_acks": {"data": None}, "api_request_counts": {}}

        monkeypatch.setattr(hosted_upgrade_api, "read_admin_ack_objects", _read)
        await limb.read_admin_acks(AdminAckRequest(mce_cluster="m", cluster="herzi", **OWN))
        assert seen["server"] == "https://kube-apiserver.hcp-herzi-openshift-herzi.svc:6443"
        assert mce.secret_reads == [("hcp-herzi", "openshift-herzi-admin-kubeconfig")]

    async def test_the_evidence_shows_the_nodepools_of_the_hostedcluster_not_of_the_file_name(self, mce, monkeypatch):
        from shared.models.cluster_upgrade import EvidenceRequest

        mce.hosted_clusters = [_hc("openshift-herzi", "hcp-herzi")]
        mce.nodepools = [
            {"metadata": {"name": "herzi-np", "namespace": "hcp-herzi"}, "spec": {"clusterName": "openshift-herzi"},
             "status": {"version": "4.19.53"}},
            {"metadata": {"name": "stray", "namespace": "hcp-herzi"}, "spec": {"clusterName": "herzi"},
             "status": {"version": "4.18.56"}},
        ]

        async def _evidence(kubeconfig, server, cluster):
            return {"cluster_operators": [], "data_plane_pods": [], "warning_events": []}

        monkeypatch.setattr(hosted_upgrade_api, "read_evidence_objects", _evidence)
        bundle = await limb.collect_evidence(EvidenceRequest(mce_cluster="m", cluster="herzi", **OWN))
        assert "herzi-np at 4.19.53" in bundle.sections["nodepools"]
        assert "stray" not in bundle.sections["nodepools"]
        # The Application keeps the file's name.
        assert ("applications", "openshift-gitops", "herzi") in mce.gets


class TestAdminAckActivities:
    async def test_read_goes_to_the_control_plane_service_with_the_admin_kubeconfig(self, mce, monkeypatch):
        seen = {}

        async def _read(kubeconfig, server, cluster, apis):
            seen.update(server=server, cluster=cluster, apis=apis, kubeconfig=kubeconfig)
            return {"cluster_version": {}, "admin_gates": {"data": {GATE: "m"}},
                    "admin_acks": {"data": None}, "api_request_counts": {VAP: None}}

        monkeypatch.setattr(hosted_upgrade_api, "read_admin_ack_objects", _read)
        state = await limb.read_admin_acks(AdminAckRequest(mce_cluster="m", cluster="lab-upgrade", apis=[VAP]))
        assert state.reachable and state.gates == {GATE: "m"} and state.usage == []
        # HyperShift's control-plane namespace: <namespace>-<name>.
        assert seen["server"] == "https://kube-apiserver.hcp-lab-upgrade-lab-upgrade.svc:6443"
        assert seen["apis"] == [VAP] and seen["kubeconfig"].startswith("apiVersion: v1")
        assert mce.secret_reads == [("hcp-lab-upgrade", "lab-upgrade-admin-kubeconfig")]

    async def test_no_hosted_cluster_reads_as_unreachable(self, mce):
        state = await limb.read_admin_acks(AdminAckRequest(mce_cluster="m", cluster="nope", apis=[VAP]))
        assert state.reachable is False

    @pytest.mark.parametrize("error", [
        HostedClusterUnreachableError("the API server of hosted cluster lab-upgrade could not be reached"),
        HostedClusterAccessError("the API server of hosted cluster lab-upgrade refused the admin kubeconfig (403)"),
    ])
    async def test_a_hosted_api_it_cannot_read_is_unreachable_for_a_person_not_retried(self, mce, monkeypatch, error):
        """The workflow's "the hosted cluster's API is unreachable" needs-human, instead of a retry for ever."""
        async def _fail(*args):
            raise error

        monkeypatch.setattr(hosted_upgrade_api, "read_admin_ack_objects", _fail)
        state = await limb.read_admin_acks(AdminAckRequest(mce_cluster="m", cluster="lab-upgrade", apis=[VAP]))
        assert state.reachable is False

    async def test_no_admin_kubeconfig_published_yet_is_unreachable(self, mce):
        del mce.hosted_clusters[0]["status"]["kubeconfig"]
        state = await limb.read_admin_acks(AdminAckRequest(mce_cluster="m", cluster="lab-upgrade", apis=[VAP]))
        assert state.reachable is False and mce.secret_reads == []

    async def test_an_ack_for_a_hosted_cluster_not_on_the_mce_is_its_own_error(self, mce):
        with pytest.raises(HostedClusterNotFoundError, match="no HostedCluster openshift-herzi in hcp-herzi"):
            await limb.give_admin_ack(GiveAdminAckRequest(mce_cluster="m", cluster="herzi", gate=GATE, **OWN))


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

    def test_an_ack_that_does_not_read_back_is_a_retryable_error_of_its_own(self, monkeypatch):
        core = _FakeHostedCore({GATE: "m"}, {})
        core.patch_namespaced_config_map = lambda name, namespace, body, _request_timeout=None: None  # lost

        @contextlib.contextmanager
        def _open(kubeconfig, server, cluster):
            yield _FakeApiClient(), core

        monkeypatch.setattr(hosted_upgrade_api, "open_hosted_api", _open)
        with pytest.raises(AdminAckNotAppliedError, match="did not read back"):
            hosted_upgrade_api._give_admin_ack("kubeconfig", "https://kas", "lab-upgrade", GATE)


class TestHostedHealthActivity:
    async def test_every_reading_lands_in_its_field(self, mce, monkeypatch):
        from shared.models.cluster_upgrade import HostedHealthRequest

        mce.nodepools = [{"metadata": {"name": "np", "namespace": "hcp-lab-upgrade"},
                          "spec": {"clusterName": "lab-upgrade", "replicas": 2}, "status": {"replicas": 1}}]

        calls = []

        async def _health(kubeconfig, server, cluster):
            calls.append(server)
            node = {"metadata": {"name": "w-1", "labels": {"hypershift.openshift.io/nodePool": "np"}},
                    "status": {"conditions": [{"type": "Ready", "status": "False"}]}}
            return {"cluster_version": {"status": {"conditions": [
                        {"type": "Upgradeable", "status": "False", "reason": "AdminAckRequired", "message": "ack"}]}},
                    "cluster_operators": [],
                    "nodes": [node], "csrs": [{}], "pdbs": [], "pods": [], "webhooks": [], "endpoints": [],
                    "image_digest_mirror_sets": [{"spec": {"imageDigestMirrors": [{"mirrors": ["m/x"]}]}}]}

        async def _not_this(*args):
            raise AssertionError("the health read is one hosted session")

        monkeypatch.setattr(hosted_upgrade_api, "read_health_objects", _health)
        monkeypatch.setattr(hosted_upgrade_api, "read_admin_ack_objects", _not_this)
        health = await limb.read_hosted_health(
            HostedHealthRequest(mce_cluster="m", cluster="lab-upgrade", nodepools=["np"]))
        assert health.reachable and health.upgradeable_reasons == ["AdminAckRequired"]
        assert health.nodes_not_ready == ["w-1"] and health.pending_csrs == 1
        assert health.nodepools_short == ["np (1/2)"] and health.idms_mirrors == ["m/x"]
        assert calls == ["https://kube-apiserver.hcp-lab-upgrade-lab-upgrade.svc:6443"]

    async def test_an_unreachable_api_server_is_reported_not_retried(self, mce, monkeypatch):
        from shared.exceptions import HostedClusterUnreachableError
        from shared.models.cluster_upgrade import HostedHealthRequest

        async def _down(*args):
            raise HostedClusterUnreachableError("connection refused")

        monkeypatch.setattr(hosted_upgrade_api, "read_health_objects", _down)
        health = await limb.read_hosted_health(HostedHealthRequest(mce_cluster="m", cluster="lab-upgrade"))
        assert health.reachable is False

    async def test_an_admin_kubeconfig_it_cannot_use_is_reported_not_retried(self, mce, monkeypatch):
        """403 or TLS: HostedClusterAccessError reaches a person through the pre-flight."""
        from shared.models.cluster_upgrade import HostedHealthRequest

        async def _forbidden(*args):
            raise HostedClusterAccessError("the API server of hosted cluster lab-upgrade refused the admin kubeconfig (403)")

        monkeypatch.setattr(hosted_upgrade_api, "read_health_objects", _forbidden)
        health = await limb.read_hosted_health(HostedHealthRequest(mce_cluster="m", cluster="lab-upgrade"))
        assert health.reachable is False

    async def test_no_admin_kubeconfig_published_yet_is_reported(self, mce):
        from shared.models.cluster_upgrade import HostedHealthRequest

        del mce.hosted_clusters[0]["status"]["kubeconfig"]
        health = await limb.read_hosted_health(HostedHealthRequest(mce_cluster="m", cluster="lab-upgrade"))
        assert health.reachable is False and mce.secret_reads == []


class TestTheArgoSync:
    """sync_argo_application on a scripted Application: each read takes the next state."""

    def _app(self, sync="OutOfSync", revision="old", out=("HostedCluster/c1",), phase=None, op_revision="sha1",
             message="", requested=None, resource_version=None):
        status = {"sync": {"status": sync, "revision": revision},
                  "resources": [{"kind": r.split("/")[0], "name": r.split("/")[1], "status": "OutOfSync"} for r in out]}
        if phase:
            status["operationState"] = {"phase": phase, "message": message,
                                        "operation": {"sync": {"revision": op_revision}}}
        app = {"status": status}
        if requested:  # the Application's own `operation`: asked for, not ended
            app["operation"] = {"initiatedBy": {"username": "alice"}, "sync": {"revision": requested}}
        if resource_version:
            app["metadata"] = {"name": "c1", "resourceVersion": resource_version}
        return app

    async def _run(self, monkeypatch, states, allowed=("HostedCluster/c1",), patched=True):
        from shared.models.cluster_upgrade import ArgoSyncRequest

        reads, patches = list(states), []

        async def _read(**kwargs):
            assert kwargs["namespace"] == "openshift-gitops" and kwargs["name"] == "c1"
            return reads.pop(0) if len(reads) > 1 else reads[0]

        async def _patch(**kwargs):
            patches.append(kwargs["body"])
            return patched

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

    async def test_the_ask_carries_the_resource_version_it_was_read_at(self, monkeypatch):
        patches, _ = await self._run(monkeypatch, [
            self._app(resource_version="4711"), self._app(sync="Synced", revision="sha1", out=(), phase="Succeeded"),
        ])
        assert patches == [{"metadata": {"resourceVersion": "4711"},
                            "operation": {"initiatedBy": {"username": "cluster-upgrade-worker"},
                                          "sync": {"revision": "sha1"}}}]

    @pytest.mark.parametrize("app", [
        # someone's sync asked for, not started yet
        {"requested": "sha0"},
        # someone's sync running
        {"phase": "Running", "op_revision": "sha0"},
    ], ids=["asked", "running"])
    async def test_another_operation_in_progress_is_waited_out_never_replaced(self, monkeypatch, app):
        from shared.exceptions import ArgoOperationInProgressError
        from shared.models.cluster_upgrade import ArgoSyncRequest
        from workflow_domains.cluster_upgrade.dispatch import RETRY_POLICY

        patches = []

        async def _read(**kwargs):
            return self._app(**app)

        async def _patch(**kwargs):
            patches.append(kwargs)
            return True

        monkeypatch.setattr(cluster_api, "read_custom_object", _read)
        monkeypatch.setattr(cluster_api, "patch_custom_object", _patch)
        with pytest.raises(ArgoOperationInProgressError, match="sha0"):
            await limb.sync_argo_application(ArgoSyncRequest(
                mce_cluster="m", cluster="c1", revision="sha1", allowed_out_of_sync=["HostedCluster/c1"]))
        assert patches == []
        # Retried, so Temporal waits the other operation out.
        assert "ArgoOperationInProgressError" not in RETRY_POLICY.non_retryable_error_types

    async def test_our_own_operation_already_asked_is_waited_for_past_a_stale_failure(self, monkeypatch):
        """While `operation` is set, operationState is still the previous attempt's Failed: not this one's end."""
        patches, result = await self._run(monkeypatch, [
            self._app(phase="Failed", requested="sha1"),
            self._app(phase="Failed", requested="sha1"),
            self._app(sync="Synced", revision="sha1", out=(), phase="Succeeded"),
        ])
        assert patches == [] and result.phase == "Succeeded"

    async def test_an_application_gone_when_asked_is_refused_not_reported_synced(self, monkeypatch):
        from shared.exceptions import ArgoSyncRefusedError

        with pytest.raises(ArgoSyncRefusedError, match="the Application does not exist"):
            await self._run(monkeypatch, [self._app()], patched=False)

    async def test_an_application_gone_while_waiting_is_refused(self, monkeypatch):
        from shared.exceptions import ArgoSyncRefusedError

        with pytest.raises(ArgoSyncRefusedError, match="the Application does not exist"):
            await self._run(monkeypatch, [self._app(), None])


class TestTheMultiSourceSync:
    """The hcAppset's Application: the chart, then day1 as `ref: values`, in ARGO_NAMESPACE."""

    OURS = "4" * 40

    def _app(self) -> dict:
        import json
        import pathlib

        fixture = pathlib.Path(__file__).parent / "fixtures" / "cluster_upgrade" / "argo-application-multi-source.json"
        return json.loads(fixture.read_text())

    async def test_it_pins_day1_in_the_values_source_in_the_configured_namespace(self, monkeypatch):
        from shared.models.cluster_upgrade import ArgoSyncRequest

        before = self._app()
        running = self._app()
        running["status"]["operationState"] = {"phase": "Running", "operation": {"sync": {
            "sources": before["spec"]["sources"], "revisions": ["1" * 40, self.OURS]}}}
        done = self._app()
        done["status"]["sync"] = {"status": "Synced", "revisions": ["1" * 40, self.OURS]}
        done["status"]["resources"][0]["status"] = "Synced"
        reads, patches = [before, running, done], []

        async def _read(**kwargs):
            assert (kwargs["namespace"], kwargs["name"]) == ("gitops-hc", "ocp4-herzi")
            return reads.pop(0) if len(reads) > 1 else reads[0]

        async def _patch(**kwargs):
            assert kwargs["namespace"] == "gitops-hc"
            patches.append(kwargs["body"])
            return True

        monkeypatch.setattr(limb, "_settings", limb._settings.model_copy(update={"argo_namespace": "gitops-hc"}))
        monkeypatch.setattr(cluster_api, "read_custom_object", _read)
        monkeypatch.setattr(cluster_api, "patch_custom_object", _patch)
        monkeypatch.setattr(limb, "_SYNC_POLL_SECONDS", 0)
        monkeypatch.setattr(limb.activity, "heartbeat", lambda *a: None)
        result = await limb.sync_argo_application(ArgoSyncRequest(
            mce_cluster="m", cluster="ocp4-herzi", revision=self.OURS,
            allowed_out_of_sync=["HostedCluster/openshift-herzi"],
        ))
        assert result.phase == "Succeeded"
        (patch,) = patches
        assert patch["operation"]["sync"] == {"sources": before["spec"]["sources"], "revisions": ["1" * 40, self.OURS]}

    async def test_without_its_values_source_it_is_refused_and_nothing_is_asked(self, monkeypatch):
        from shared.exceptions import ArgoSyncRefusedError
        from shared.models.cluster_upgrade import ArgoSyncRequest

        app = self._app()
        app["spec"]["sources"][1]["ref"] = "day1"
        patches = []

        async def _read(**kwargs):
            return app

        async def _patch(**kwargs):
            patches.append(kwargs)

        monkeypatch.setattr(cluster_api, "read_custom_object", _read)
        monkeypatch.setattr(cluster_api, "patch_custom_object", _patch)
        with pytest.raises(ArgoSyncRefusedError, match="openshift-gitops/ocp4-herzi: none of its 2 sources"):
            await limb.sync_argo_application(ArgoSyncRequest(
                mce_cluster="m", cluster="ocp4-herzi", revision=self.OURS,
                allowed_out_of_sync=["HostedCluster/openshift-herzi"],
            ))
        assert patches == []



class TestTheControlPlaneWatch:
    def _wire(self, monkeypatch, polls, heartbeat_details=()):
        """Each poll takes the next (hosted cluster, operators) pair; heartbeats are recorded."""
        from types import SimpleNamespace

        polls, beats = list(polls), []

        async def _hosted(name, namespace=None):
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

        machine_set = {"metadata": {"name": "np", "annotations": {
            "hypershift.openshift.io/nodePoolTargetConfigVersion": "new",
            "hypershift.openshift.io/nodePoolCurrentConfigVersion": "old"}}}

        async def _view(request):
            return nodepool, machine_set, {"nodes": [node], "pods": [pod], "pdbs": [pdb]}

        monkeypatch.setattr(limb, "_nodepool_view", _view)
        monkeypatch.setattr(limb.activity, "heartbeat", lambda *a: None)
        monkeypatch.setattr(limb.activity, "info", lambda: SimpleNamespace(heartbeat_details=[]))
        outcome = await limb.watch_nodepool(WatchNodePoolRequest(
            mce_cluster="m", cluster="c1", nodepool="np", version="4.19.53",
            stall_minutes=30, deadline_minutes=120, drain_blocked_minutes=10))
        assert outcome.result is WatchResult.STALLED
        assert "PDB openshift-console/downloads (disruptionsAllowed 0; downloads-a CrashLoopBackOff)" in outcome.snapshot.summary
        # The MachineSet's target config is what the nodes are counted against.
        assert "0/1 nodes at the target config" in outcome.snapshot.summary


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
        ("delete_failed_job", "openshift-image-registry/image-pruner-29365920",
         ("openshift-image-registry", "image-pruner-29365920")),
    ])
    def test_in_the_catalog(self, kind, target, where):
        from activities.cluster_upgrade import action_catalog

        action = action_catalog.check(_action(kind, target, uid="u-1"))
        assert (action.namespace, action.name) == where
        assert action.uid == (None if kind == "rerun_cronjob" else "u-1")
        assert action.kind == kind

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
        ("delete_failed_job", "shop/nightly-report-1", "only namespaces openshift-*"),
        ("delete_job", "openshift-image-registry/image-pruner-1", "not in the catalog"),
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

    def test_an_invocation_id_makes_the_same_action_id_of_two_invocations_two_names(self):
        import hashlib

        from activities.cluster_upgrade import action_catalog

        first = action_catalog.rerun_job_name("image-pruner", "1", invocation_id="inv-a")
        assert first == action_catalog.rerun_job_name("image-pruner", "1", invocation_id="inv-a")
        assert first != action_catalog.rerun_job_name("image-pruner", "1", invocation_id="inv-b")
        assert first != action_catalog.rerun_job_name("image-pruner", "1")
        # Without one, the name a request always had: an in-flight retry still finds its Job.
        assert action_catalog.rerun_job_name("image-pruner", "1") == (
            f"image-pruner-rerun-{hashlib.sha256(b'1').hexdigest()[:8]}")


class _FakeActionCore:
    """Pods and CronJobs/Jobs of a hosted cluster, enough for the tier-2 writes."""

    def __init__(self, pods=None, cronjobs=None):
        self.pods = dict(pods or {})  # (namespace, name) -> uid
        self.cronjobs = dict(cronjobs or {})
        self.jobs = {}
        self.deleted_jobs = []

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

    def read_namespaced_job(self, name, namespace, _request_timeout=None):
        from kubernetes import client as k8s_client
        if (namespace, name) not in self.jobs:
            raise k8s_client.ApiException(status=404)
        return self.jobs[(namespace, name)]

    def delete_namespaced_job(self, name, namespace, body=None, _request_timeout=None):
        from kubernetes import client as k8s_client
        if (namespace, name) not in self.jobs:
            raise k8s_client.ApiException(status=404)
        if body.preconditions.uid != self.jobs[(namespace, name)]["metadata"]["uid"]:
            raise k8s_client.ApiException(status=409)
        self.deleted_jobs.append((name, body.propagation_policy))
        del self.jobs[(namespace, name)]

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

    def test_only_a_failed_job_is_deleted_and_only_the_one_seen(self, monkeypatch):
        def job(name, uid, condition):
            return {"metadata": {"name": name, "namespace": "openshift-image-registry", "uid": uid},
                    "status": {"conditions": [{"type": condition, "status": "True"}]}}

        core = _FakeActionCore()
        core.jobs = {("openshift-image-registry", "image-pruner-1"): job("image-pruner-1", "u-1", "Failed"),
                     ("openshift-image-registry", "image-pruner-2"): job("image-pruner-2", "u-2", "Complete")}
        self._open(monkeypatch, core)
        delete = hosted_upgrade_api._delete_failed_job
        # A Complete Job, or one recreated since the proposal, is left alone.
        assert delete("k", "s", "c", "openshift-image-registry", "image-pruner-2", None) == "not-failed"
        assert delete("k", "s", "c", "openshift-image-registry", "image-pruner-1", "u-old") == "replaced"
        assert delete("k", "s", "c", "openshift-image-registry", "image-pruner-1", "u-1") == "deleted"
        assert delete("k", "s", "c", "openshift-image-registry", "image-pruner-1", "u-1") == "gone"
        # Background propagation takes the Job's Pods with it.
        assert core.deleted_jobs == [("image-pruner-1", "Background")]
        assert ("openshift-image-registry", "image-pruner-2") in core.jobs

    CRONJOB = {"metadata": {"name": "collect-profiles", "namespace": "openshift-olm", "uid": "cj-uid"},
               "spec": {"jobTemplate": {"metadata": {"labels": {"app": "p"}, "annotations": {"a": "b"}},
                                        "spec": {"template": {"spec": {"containers": [{"name": "c"}]}}}}}}
    OURS = {"workflows.redbull/action-id": "1", "workflows.redbull/request": "f" * 16}

    def test_a_cronjob_rerun_makes_the_job_oc_would_and_only_once(self, monkeypatch):
        core = _FakeActionCore(cronjobs={("openshift-olm", "collect-profiles"): self.CRONJOB})
        self._open(monkeypatch, core)
        rerun = hosted_upgrade_api._rerun_cronjob
        job_name = "collect-profiles-rerun-1a2b3c4d"
        assert rerun("k", "s", "c", "openshift-olm", "collect-profiles", job_name, self.OURS) == "created"
        # A retry of the same request finds the Job it made: its annotations say so.
        assert rerun("k", "s", "c", "openshift-olm", "collect-profiles", job_name, self.OURS) == "exists"
        assert rerun("k", "s", "c", "openshift-olm", "gone", "gone-rerun-1a2b3c4d", self.OURS) == "missing"
        (job,) = core.jobs.values()
        assert job["spec"] == self.CRONJOB["spec"]["jobTemplate"]["spec"]
        assert job["metadata"]["labels"] == {"app": "p"}
        assert job["metadata"]["annotations"] == {"a": "b", "cronjob.kubernetes.io/instantiate": "manual", **self.OURS}
        assert job["metadata"]["ownerReferences"] == [{"apiVersion": "batch/v1", "kind": "CronJob",
                                                       "name": "collect-profiles", "uid": "cj-uid", "controller": True}]

    def test_a_job_of_that_name_another_request_made_is_reported_never_claimed(self, monkeypatch):
        """Action ids repeat across invocations ("1"): a later approval meets the earlier one's Job."""
        core = _FakeActionCore(cronjobs={("openshift-olm", "collect-profiles"): self.CRONJOB})
        job_name = "collect-profiles-rerun-1a2b3c4d"
        older = {"workflows.redbull/action-id": "1", "workflows.redbull/request": "0" * 16}
        core.jobs[("openshift-olm", job_name)] = {"metadata": {"name": job_name, "annotations": older}}
        self._open(monkeypatch, core)
        assert hosted_upgrade_api._rerun_cronjob(
            "k", "s", "c", "openshift-olm", "collect-profiles", job_name, self.OURS) == "conflict"
        # Nothing created, the older Job untouched.
        assert core.jobs == {("openshift-olm", job_name): {"metadata": {"name": job_name, "annotations": older}}}
        # A Job with none of our annotations (made by hand, or before them) is not ours either.
        core.jobs[("openshift-olm", job_name)] = {"metadata": {"name": job_name}}
        assert hosted_upgrade_api._rerun_cronjob(
            "k", "s", "c", "openshift-olm", "collect-profiles", job_name, self.OURS) == "conflict"

    def test_a_job_gone_between_the_409_and_its_read_is_not_claimed(self, monkeypatch):
        from kubernetes import client as k8s_client

        core = _FakeActionCore(cronjobs={("openshift-olm", "collect-profiles"): self.CRONJOB})

        def _taken(namespace, body, _request_timeout=None):
            raise k8s_client.ApiException(status=409)

        core.create_namespaced_job = _taken
        self._open(monkeypatch, core)
        assert hosted_upgrade_api._rerun_cronjob(
            "k", "s", "c", "openshift-olm", "collect-profiles", "collect-profiles-rerun-1", self.OURS) == "vanished"


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

    async def test_a_failed_job_goes_to_the_control_plane_service(self, mce, monkeypatch):
        seen = {}

        async def _delete(kubeconfig, server, cluster, namespace, name, uid):
            seen.update(server=server, namespace=namespace, name=name, uid=uid)
            return "not-failed"

        monkeypatch.setattr(hosted_upgrade_api, "delete_failed_job", _delete)
        result = await limb.execute_proposed_action(
            self._request("delete_failed_job", "openshift-image-registry/image-pruner-29365920", uid="u-9"))
        assert seen == {"server": "https://kube-apiserver.hcp-lab-upgrade-lab-upgrade.svc:6443",
                        "namespace": "openshift-image-registry", "name": "image-pruner-29365920", "uid": "u-9"}
        assert (result.executed, result.detail) == (False, "not executed: the Job has not failed (it is running or Complete)")

    def _in_activity(self, monkeypatch, activity_id="7", run_id="run-1"):
        from types import SimpleNamespace

        monkeypatch.setattr(limb.activity, "info", lambda: SimpleNamespace(
            workflow_id="upgrade-cluster-lab-upgrade", workflow_run_id=run_id, activity_id=activity_id))

    async def _rerun(self, monkeypatch, outcome, **identity):
        seen = {}

        async def _rerun(kubeconfig, server, cluster, namespace, name, job, annotations):
            seen.update(job=job, annotations=annotations)
            return outcome

        monkeypatch.setattr(hosted_upgrade_api, "rerun_cronjob", _rerun)
        self._in_activity(monkeypatch, **identity)
        result = await limb.execute_proposed_action(self._request("rerun_cronjob", "openshift-olm/collect-profiles"))
        return result, seen

    async def test_a_rerun_names_its_job_and_marks_it_with_who_asked(self, mce, monkeypatch):
        result, seen = await self._rerun(monkeypatch, "exists")
        assert result.executed and result.detail.startswith("created Job collect-profiles-rerun-")
        assert result.detail.endswith("(found on retry)")
        annotations = seen["annotations"]
        assert annotations["workflows.redbull/action-id"] == "a-1"
        assert annotations["workflows.redbull/cluster"] == "lab-upgrade"
        assert annotations["workflows.redbull/workflow-id"] == "upgrade-cluster-lab-upgrade"
        assert len(annotations["workflows.redbull/request"]) == 16

    async def test_the_request_mark_is_the_same_on_a_retry_and_differs_for_a_later_execution(self, mce, monkeypatch):
        _, first = await self._rerun(monkeypatch, "created")
        _, retry = await self._rerun(monkeypatch, "exists")
        _, later = await self._rerun(monkeypatch, "conflict", activity_id="12")
        assert first["annotations"] == retry["annotations"]
        assert later["annotations"]["workflows.redbull/request"] != first["annotations"]["workflows.redbull/request"]

    async def test_the_invocation_that_proposed_it_names_the_job(self, mce, monkeypatch):
        from activities.cluster_upgrade import action_catalog

        seen = {}

        async def _rerun(kubeconfig, server, cluster, namespace, name, job, annotations):
            seen["job"] = job
            return "created"

        monkeypatch.setattr(hosted_upgrade_api, "rerun_cronjob", _rerun)
        self._in_activity(monkeypatch)
        request = self._request("rerun_cronjob", "openshift-olm/collect-profiles").model_copy(
            update={"invocation_id": "upgrade-cluster-lab-upgrade/inv-7"})
        await limb.execute_proposed_action(request)
        assert seen["job"] == action_catalog.rerun_job_name(
            "collect-profiles", "a-1", invocation_id="upgrade-cluster-lab-upgrade/inv-7")
        assert seen["job"] != action_catalog.rerun_job_name("collect-profiles", "a-1")

    async def test_a_job_another_request_made_is_not_executed(self, mce, monkeypatch):
        result, _ = await self._rerun(monkeypatch, "conflict")
        assert result.executed is False
        assert result.detail.startswith("not executed: a Job collect-profiles-rerun-")
        assert "this approval did not make" in result.detail

    async def test_a_missing_hosted_cluster_is_named_by_its_own_name(self, mce, monkeypatch):
        from shared.models.cluster_upgrade import ProposedActionRequest

        result = await limb.execute_proposed_action(ProposedActionRequest(
            mce_cluster="m", cluster="herzi", action=_action("delete_pod", "openshift-console/downloads-a"),
            approved_by="alice", **OWN))
        assert (result.executed, result.detail) == (False, "not executed: HostedCluster openshift-herzi not found on the MCE")

    async def test_refused_before_anything_is_read(self, mce, monkeypatch):
        from shared.exceptions import ProposedActionRefusedError

        with pytest.raises(ProposedActionRefusedError, match="only namespaces openshift-"):
            await limb.execute_proposed_action(self._request("delete_pod", "shop/db-0"))
        assert mce.secret_reads == []


class TestWhatAPollReads:
    """A watch's poll reads only what it uses (one hosted session, no admin gates or acks)."""

    async def test_a_control_plane_poll_reads_only_the_cluster_operators(self, mce, monkeypatch):
        seen = []

        async def _operators(kubeconfig, server, cluster):
            seen.append(server)
            return [{"metadata": {"name": "dns"}}]

        async def _not_this(*args):
            raise AssertionError("a control-plane poll reads no admin gates, acks or health")

        monkeypatch.setattr(hosted_upgrade_api, "read_cluster_operators", _operators)
        for other in ("read_admin_ack_objects", "read_health_objects", "read_evidence_objects"):
            monkeypatch.setattr(hosted_upgrade_api, other, _not_this)
        assert await limb._hosted_operators(_hc(), "lab-upgrade") == [{"metadata": {"name": "dns"}}]
        assert seen == ["https://kube-apiserver.hcp-lab-upgrade-lab-upgrade.svc:6443"]

    async def test_the_evidence_takes_its_operators_from_its_own_session(self, mce, monkeypatch):
        from shared.models.cluster_upgrade import EvidenceRequest

        async def _evidence(kubeconfig, server, cluster):
            return {"cluster_operators": [{"metadata": {"name": "dns"}, "status": {"conditions": [
                        {"type": "Available", "status": "False", "reason": "NoPods"}]}}],
                    "data_plane_pods": [], "warning_events": []}

        async def _not_this(*args):
            raise AssertionError("the evidence needs no second session for its operators")

        monkeypatch.setattr(hosted_upgrade_api, "read_evidence_objects", _evidence)
        monkeypatch.setattr(hosted_upgrade_api, "read_cluster_operators", _not_this)
        bundle = await limb.collect_evidence(EvidenceRequest(mce_cluster="m", cluster="lab-upgrade"))
        assert bundle.sections["clusteroperators"] == "dns (Available=False: NoPods)"

    def test_the_operators_read_is_one_list_and_nothing_else(self, monkeypatch):
        from kubernetes import client as k8s_client

        calls = []

        class _Custom:
            def list_cluster_custom_object(self, **kwargs):
                calls.append(kwargs["plural"])
                return {"items": [{"metadata": {"name": "dns"}}]}

            def get_cluster_custom_object(self, **kwargs):
                raise AssertionError("no ClusterVersion, no APIRequestCount")

        @contextlib.contextmanager
        def _open(kubeconfig, server, cluster):
            yield _FakeApiClient(), None

        monkeypatch.setattr(hosted_upgrade_api, "open_hosted_api", _open)
        monkeypatch.setattr(k8s_client, "CustomObjectsApi", lambda api_client: _Custom())
        assert hosted_upgrade_api._read_cluster_operators("k", "s", "c") == [{"metadata": {"name": "dns"}}]
        assert calls == ["clusteroperators"]


class TestTheWarningEvents:
    """Events are listed by key, not by time: every page is followed so the newest are found."""

    def _wire(self, monkeypatch, pages):
        from kubernetes import client as k8s_client

        class _Core:
            def __init__(self):
                self.calls = []

            def list_namespaced_pod(self, namespace, _request_timeout=None):
                return {"items": []}

            def list_event_for_all_namespaces(self, field_selector, limit, _continue=None, _request_timeout=None):
                self.calls.append((field_selector, limit, _continue))
                items, token = pages[_continue]
                return {"metadata": {"continue": token} if token else {}, "items": items}

        class _Custom:
            def list_cluster_custom_object(self, **kwargs):
                return {"items": []}

        core = _Core()

        @contextlib.contextmanager
        def _open(kubeconfig, server, cluster):
            yield _FakeApiClient(), core

        monkeypatch.setattr(hosted_upgrade_api, "open_hosted_api", _open)
        monkeypatch.setattr(k8s_client, "CustomObjectsApi", lambda api_client: _Custom())
        return core

    @staticmethod
    def _event(i):
        return {"metadata": {"name": f"e{i}"}, "lastTimestamp": f"2026-10-31T{i // 60:02d}:{i % 60:02d}:00Z",
                "involvedObject": {"kind": "Pod", "namespace": "openshift-image-registry", "name": f"p{i}"},
                "reason": "BackOff", "message": f"event {i}"}

    def _pages(self):
        return {None: ([self._event(i) for i in range(200)], "t1"),
                "t1": ([self._event(i) for i in range(200, 400)], "t2"),
                "t2": ([self._event(400)], None)}

    def test_every_page_is_read_so_the_newest_event_is_found(self, monkeypatch):
        from activities.cluster_upgrade import upgrade_state

        core = self._wire(monkeypatch, self._pages())
        events = hosted_upgrade_api._read_evidence_objects("k", "s", "c")["warning_events"]
        assert len(events) == 401
        assert core.calls == [("type=Warning", 200, None), ("type=Warning", 200, "t1"), ("type=Warning", 200, "t2")]
        (newest,) = upgrade_state.warning_events(events, limit=1)
        assert newest.endswith("event 400")

    def test_the_pages_stop_at_the_cap(self, monkeypatch):
        monkeypatch.setattr(hosted_upgrade_api, "_EVENT_CAP", 300)
        core = self._wire(monkeypatch, self._pages())
        assert len(hosted_upgrade_api._read_evidence_objects("k", "s", "c")["warning_events"]) == 300
        assert len(core.calls) == 2


class TestTheAdminKubeconfigDecode:
    """One decode of the Secret, in hosted_cluster_api, the only module that handles admin kubeconfigs."""

    def test_the_text_comes_back(self):
        from activities.server_lifecycle import hosted_cluster_api

        data = {"kubeconfig": base64.b64encode(b"apiVersion: v1\n").decode()}
        assert hosted_cluster_api.admin_kubeconfig(data, "ns/s", "ns/hc") == "apiVersion: v1\n"

    @pytest.mark.parametrize(("data", "why"), [
        (None, "Secret ns/s of HostedCluster ns/hc does not exist"),
        ({}, "Secret ns/s of HostedCluster ns/hc has no 'kubeconfig' key"),
        ({"kubeconfig": "not base64!"}, "holds no readable kubeconfig"),
    ])
    def test_an_unusable_secret_is_an_access_error_naming_it_never_its_contents(self, data, why):
        from activities.server_lifecycle import hosted_cluster_api

        with pytest.raises(HostedClusterAccessError, match=why) as caught:
            hosted_cluster_api.admin_kubeconfig(data, "ns/s", "ns/hc")
        assert "not base64!" not in str(caught.value)

    async def test_the_limb_reads_the_secret_its_hostedcluster_names(self, mce):
        kubeconfig, server = await limb._hosted_access(_hc())
        assert kubeconfig.startswith("apiVersion: v1")
        assert server == "https://kube-apiserver.hcp-lab-upgrade-lab-upgrade.svc:6443"
        assert mce.secret_reads == [("hcp-lab-upgrade", "lab-upgrade-admin-kubeconfig")]

    async def test_no_status_kubeconfig_is_an_access_error(self, mce):
        hc = _hc()
        del hc["status"]["kubeconfig"]
        with pytest.raises(HostedClusterAccessError, match="publishes no status.kubeconfig"):
            await limb._hosted_access(hc)


class TestTheMceCallsAreBounded:
    """Every call through cluster_api carries a request timeout: a hung MCE read must not stall a watch."""

    _VERBS = ("create_", "read_", "list_", "patch_", "delete_", "get_")

    def test_every_kubernetes_call_in_cluster_api_carries_one(self):
        import ast
        import pathlib

        tree = ast.parse(pathlib.Path(cluster_api.__file__).read_text())
        calls = [
            node for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
            and node.func.attr.startswith(self._VERBS)
        ]
        assert len(calls) >= 17, "the scan no longer finds the SDK calls"
        assert [c.func.attr for c in calls if "_request_timeout" not in {k.arg for k in c.keywords}] == []

    async def test_a_label_selector_reaches_the_client_and_none_is_not_sent(self, monkeypatch):
        seen = []

        class _Api:
            def list_namespaced_custom_object(self, **kwargs):
                seen.append(kwargs)
                return {"items": []}

        monkeypatch.setattr(cluster_api, "_load_kube", lambda: None)
        monkeypatch.setattr(cluster_api.k8s_client, "CustomObjectsApi", _Api)
        await cluster_api.list_custom_objects(group="g", version="v", plural="p", kind="K", namespace="n",
                                              label_selector="a=b")
        await cluster_api.list_custom_objects(group="g", version="v", plural="p", kind="K", namespace="n")
        assert seen[0]["label_selector"] == "a=b"
        assert "label_selector" not in seen[1]

    async def test_the_timeout_reaches_the_client(self, monkeypatch):
        seen = {}

        class _Api:
            def get_namespaced_custom_object(self, **kwargs):
                seen.update(kwargs)
                return {"metadata": {"name": "x"}}

        monkeypatch.setattr(cluster_api, "_load_kube", lambda: None)
        monkeypatch.setattr(cluster_api.k8s_client, "CustomObjectsApi", _Api)
        await cluster_api.read_custom_object(group="g", version="v", plural="p", kind="K", namespace="n", name="x")
        assert seen["_request_timeout"] == (10, 60)

    async def test_a_patch_whose_resource_version_is_stale_is_retryable(self, monkeypatch):
        from kubernetes import client as k8s_client

        from shared.exceptions import BmhResourceError
        from workflow_domains.cluster_upgrade.dispatch import RETRY_POLICY

        class _Api:
            def patch_namespaced_custom_object(self, **kwargs):
                raise k8s_client.ApiException(status=409, reason="Conflict")

        monkeypatch.setattr(cluster_api, "_load_kube", lambda: None)
        monkeypatch.setattr(cluster_api.k8s_client, "CustomObjectsApi", _Api)
        with pytest.raises(BmhResourceError, match="409"):
            await cluster_api.patch_custom_object(
                group="argoproj.io", version="v1alpha1", plural="applications", kind="Application",
                namespace="openshift-gitops", name="c1", body={"metadata": {"resourceVersion": "1"}},
            )
        # Not a permanent error: the sync is read and asked again.
        assert "BmhResourceError" not in RETRY_POLICY.non_retryable_error_types
