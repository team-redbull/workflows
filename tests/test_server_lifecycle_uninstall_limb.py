"""uninstall-server's limb: server-scan's membership read, the CAPI / HyperShift reads,
the Machine delete, and the hosted cluster's Node read through its admin kubeconfig.

What matters: which AgentMachine belongs to the host (by label, or by its
`agentRef` once the provider has started unbinding), that the one write is a
no-op on a Machine already going, that the HostedCluster is found by
HyperShift's own namespace rule, and that nothing from a kubeconfig ever
reaches an error.
"""

from __future__ import annotations

import base64
import pathlib
from typing import ClassVar

import httpx
import pytest
import respx
import urllib3
from kubernetes import client as k8s_client

from activities.server_lifecycle import (
    activities,
    cluster_api,
    hosted_cluster_api,
    hosted_resources,
    server_scan,
)
from activities.server_lifecycle.bmh_resources import AGENT_BMH_LABEL
from shared.exceptions import (
    AgentBindingChangedError,
    AmbiguousServerNameError,
    HostedClusterAccessError,
    HostedClusterUnreachableError,
    ServerScanAuthError,
)
from shared.models.server_lifecycle import (
    AgentMachineCleanupRequest,
    AgentMachineRef,
    AgentNameRef,
    AgentReleaseRequest,
    BmhRef,
    HostedClusterRef,
    HostedNodeAction,
    HostedNodeQuery,
    HostedNodeRef,
    MachineRef,
    ServerMembershipRequest,
)

SERVER = "ocp-cisco-m6-bat-yam-64c-512gb-FCH0001"
BMH = SERVER.lower()
NS = "inventory"
HCP = "clusters-hc-01"
SS = "http://server-scan.test/api/v1"


# --- server-scan -------------------------------------------------------------------


class TestServerMembership:
    @respx.mock
    async def test_the_exact_name_match_is_read_from_its_detail(self):
        respx.get(f"{SS}/servers").mock(return_value=httpx.Response(200, json={"items": [
            {"id": "srv_1", "name": SERVER},
            {"id": "srv_2", "name": SERVER + "-OTHER"},
        ]}))
        respx.get(f"{SS}/servers/srv_1").mock(return_value=httpx.Response(200, json={
            "name": SERVER,
            "openshift": {"lifecycle_state": "INSTALLED", "cluster_name": "hc-01",
                          "mce_name": "mce-bat-yam-01", "contested_with": None},
            "maintenance": {"enabled": True, "reason": "fan swap"},
        }))
        other = respx.get(f"{SS}/servers/srv_2")

        membership = await server_scan.server_membership_by_name(SS, "t", SERVER.lower())

        assert membership.found and membership.server_id == "srv_1"
        assert (membership.lifecycle_state, membership.cluster_name, membership.mce_name) == (
            "INSTALLED", "hc-01", "mce-bat-yam-01",
        )
        assert membership.contested_with is None
        assert membership.maintenance and membership.maintenance_reason == "fan swap"
        assert not other.called

    @respx.mock
    async def test_an_unknown_name_is_found_false(self):
        respx.get(f"{SS}/servers").mock(return_value=httpx.Response(200, json={"items": []}))
        assert (await server_scan.server_membership_by_name(SS, "t", SERVER)).found is False

    @respx.mock
    async def test_two_documents_of_one_name_are_ambiguous_not_merged(self):
        respx.get(f"{SS}/servers").mock(return_value=httpx.Response(200, json={"items": [
            {"id": "srv_1", "name": SERVER}, {"id": "srv_9", "name": SERVER},
        ]}))
        respx.get(f"{SS}/servers/srv_1").mock(return_value=httpx.Response(200, json={"name": SERVER}))
        respx.get(f"{SS}/servers/srv_9").mock(return_value=httpx.Response(200, json={"name": SERVER}))
        with pytest.raises(AmbiguousServerNameError, match="srv_1, srv_9"):
            await server_scan.server_membership_by_name(SS, "t", SERVER)

    @respx.mock
    async def test_a_rejected_token_is_permanent(self):
        respx.get(f"{SS}/servers").mock(return_value=httpx.Response(401))
        with pytest.raises(ServerScanAuthError):
            await server_scan.server_membership_by_name(SS, "bad", SERVER)

    async def test_the_activity_wraps_it(self, monkeypatch):
        async def _membership(url, token, name):
            assert name == SERVER
            return server_scan.ServerMembership(found=False)

        monkeypatch.setattr(server_scan, "server_membership_by_name", _membership)
        result = await activities.lookup_server_membership(ServerMembershipRequest(server_name=SERVER))
        assert result.found is False


# --- the pure shapes ------------------------------------------------------------------


def _agent(name="agent-1", *, macs=(), label_am=None, label_ns=HCP, bound=None, state=None):
    agent = {
        "metadata": {"name": name, "labels": {AGENT_BMH_LABEL: BMH}, "annotations": {}},
        "spec": {"approved": True},
        "status": {"inventory": {"interfaces": [{"macAddress": m} for m in macs]}},
    }
    if label_am:
        agent["metadata"]["labels"][hosted_resources.AGENT_MACHINE_REF_LABEL] = label_am
        agent["metadata"]["annotations"][hosted_resources.AGENT_MACHINE_REF_NAMESPACE_ANNOTATION] = label_ns
    if bound:
        agent["spec"]["clusterDeploymentName"] = {"namespace": HCP, "name": bound}
    if state:
        agent["status"]["debugInfo"] = {"state": state}
    return agent


def _agent_machine(name="hc-01-np-am-x7k2p", namespace=HCP, *, agent_ref=None, machine="hc-01-np-x7k2p",
                   api_version="cluster.x-k8s.io/v1beta1"):
    am = {"metadata": {"name": name, "namespace": namespace}, "status": {}}
    if machine:
        am["metadata"]["ownerReferences"] = [
            {"kind": "MachineSet", "apiVersion": "cluster.x-k8s.io/v1beta1", "name": "not-me"},
            {"kind": "Machine", "apiVersion": api_version, "name": machine},
        ]
    if agent_ref:
        am["status"]["agentRef"] = agent_ref
    return am


class TestTheAgentMachineBehindAnAgent:
    def test_by_the_providers_label_and_its_namespace_annotation(self):
        same_name_elsewhere = _agent_machine(namespace="clusters-hc-02")
        mine = _agent_machine()
        agent = _agent(label_am="hc-01-np-am-x7k2p")
        assert hosted_resources.agent_machine_for(agent, NS, [same_name_elsewhere, mine]) is mine

    def test_by_agent_ref_once_the_provider_has_cleared_the_label(self):
        am = _agent_machine(agent_ref={"name": "agent-1", "namespace": NS})
        assert hosted_resources.agent_machine_for(_agent(), NS, [am]) is am

    def test_an_agent_ref_in_another_namespace_is_another_agent(self):
        am = _agent_machine(agent_ref={"name": "agent-1", "namespace": "other-infraenv"})
        assert hosted_resources.agent_machine_for(_agent(), NS, [am]) is None

    def test_the_owner_is_the_machine_not_any_owner(self):
        assert hosted_resources.machine_owner(_agent_machine(api_version="cluster.x-k8s.io/v1beta2")) == (
            "hc-01-np-x7k2p", "cluster.x-k8s.io/v1beta2",
        )
        assert hosted_resources.machine_owner(_agent_machine(machine=None)) is None


class TestMachineState:
    def test_a_v1beta1_machine_draining_reports_what_holds_it(self):
        state = hosted_resources.machine_state({
            "metadata": {
                "deletionTimestamp": "2026-10-24T08:00:00Z",
                "annotations": {
                    "pre-terminate.delete.hook.machine.cluster.x-k8s.io/agentmachine": "",
                    "pre-drain.delete.hook.machine.cluster.x-k8s.io/backup": "team-x",
                    "unrelated": "x",
                },
            },
            "status": {
                "phase": "Deleting",
                "nodeRef": {"kind": "Node", "name": "node-1"},
                "conditions": [
                    {"type": "Ready", "status": "True"},
                    {"type": "DrainingSucceeded", "status": "False", "reason": "Draining",
                     "message": "Cannot evict pod as it would violate the pod's disruption budget"},
                    {"type": "PreTerminateDeleteHookSucceeded", "status": "True"},
                ],
            },
        })
        assert state.found and state.deleting and state.deletion_timestamp == "2026-10-24T08:00:00Z"
        assert state.node_name == "node-1" and state.phase == "Deleting"
        assert state.pending_hooks == [
            "pre-drain.delete.hook.machine.cluster.x-k8s.io/backup",
            "pre-terminate.delete.hook.machine.cluster.x-k8s.io/agentmachine",
        ]
        assert state.conditions == [
            "DrainingSucceeded=False (Draining): Cannot evict pod as it would violate the pod's disruption budget"
        ]

    def test_a_v1beta2_deleting_condition_is_reported_while_true(self):
        state = hosted_resources.machine_state({
            "metadata": {"deletionTimestamp": "2026-10-24T08:00:00Z"},
            "status": {
                "conditions": [{"type": "Deleting", "status": "True",
                                "reason": "WaitingForPreTerminateHook", "message": "agentmachine"}],
                "deprecated": {"v1beta1": {"conditions": [
                    {"type": "DrainingSucceeded", "status": "True"}]}},
            },
        })
        assert state.conditions == ["Deleting=True (WaitingForPreTerminateHook): agentmachine"]

    def test_none_is_gone(self):
        assert hosted_resources.machine_state(None).found is False


class TestTheHostedCluster:
    def test_hypershifts_namespace_rule_with_dots(self):
        assert hosted_resources.hosted_control_plane_namespace("clusters", "a.b") == "clusters-a-b"

    def test_an_ambiguous_looking_namespace_is_settled_by_what_exists(self):
        # `clusters-a-b` could be clusters/a-b or clusters-a/b; only one exists.
        hosted = [{"metadata": {"namespace": "clusters-a", "name": "b"}}]
        assert hosted_resources.hosted_cluster_for("clusters-a-b", hosted) == HostedClusterRef(
            namespace="clusters-a", name="b", hcp_namespace="clusters-a-b"
        )
        assert hosted_resources.hosted_cluster_for("clusters-x", hosted) is None


# --- the activities, against a fake MCE ---------------------------------------------------


class _FakeMce:
    def __init__(self, *, agents=(), agent_machines=(), machines=None, hosted_clusters=(), secrets=None):
        self.namespaced = {("baremetalhosts", NS, BMH): {
            "metadata": {"labels": {"infraenvs.agent-install.openshift.io": "cisco-m6-bat-yam-64c-512gb"}},
            "spec": {"bootMACAddress": "00:25:b5:00:00:01"},
            "status": {"operationalStatus": "OK"},
        }}
        for agent in agents:
            self.namespaced[("agents", NS, agent["metadata"]["name"])] = agent
        for am in agent_machines:
            self.namespaced[("agentmachines", am["metadata"]["namespace"], am["metadata"]["name"])] = am
        for (namespace, name), machine in (machines or {}).items():
            self.namespaced[("machines", namespace, name)] = machine
        for hc in hosted_clusters:
            self.namespaced[("hostedclusters", hc["metadata"]["namespace"], hc["metadata"]["name"])] = hc
        self.secrets = secrets or {}
        self.deleted: list = []
        self.patched: list = []
        self.finalizers_cleared: list = []
        self.cluster_lists: list = []

    async def read_custom_object(self, *, plural, namespace, name, **_):
        return self.namespaced.get((plural, namespace, name))

    async def list_custom_objects(self, *, plural, namespace, **_):
        return [o for (p, ns, _n), o in self.namespaced.items() if p == plural and ns == namespace]

    async def list_cluster_custom_objects(self, *, plural, **_):
        self.cluster_lists.append(plural)
        return [o for (p, _ns, _n), o in self.namespaced.items() if p == plural]

    async def delete_custom_object(self, *, plural, namespace, name, **_):
        self.deleted.append((plural, namespace, name))
        existing = self.namespaced.get((plural, namespace, name))
        if existing is None:
            return False
        if (existing.get("metadata") or {}).get("finalizers"):
            existing["metadata"]["deletionTimestamp"] = "2026-10-24T09:00:00Z"
        else:
            del self.namespaced[(plural, namespace, name)]
        return True

    async def patch_custom_object(self, *, plural, namespace, name, body, **_):
        self.patched.append((plural, namespace, name, body))
        return (plural, namespace, name) in self.namespaced

    async def clear_custom_object_finalizers(self, *, plural, namespace, name, finalizers, **_):
        existing = self.namespaced.get((plural, namespace, name))
        if existing is None:
            return False
        self.finalizers_cleared.append((plural, namespace, name, sorted(finalizers)))
        remaining = [f for f in existing["metadata"].get("finalizers", []) if f not in finalizers]
        existing["metadata"]["finalizers"] = remaining
        if not remaining and existing["metadata"].get("deletionTimestamp"):
            del self.namespaced[(plural, namespace, name)]
        return True

    async def read_secret_data(self, namespace, name):
        return self.secrets.get((namespace, name))


@pytest.fixture
def mce(monkeypatch):
    def _install(fake: _FakeMce) -> _FakeMce:
        for fn in ("read_custom_object", "list_custom_objects", "list_cluster_custom_objects",
                   "delete_custom_object", "read_secret_data", "patch_custom_object",
                   "clear_custom_object_finalizers"):
            monkeypatch.setattr(cluster_api, fn, getattr(fake, fn))

        async def _macs(_url, _token, server_name):
            return []

        monkeypatch.setattr(server_scan, "server_macs_by_name", _macs)
        return fake

    return _install


HOSTED_CLUSTER = {"metadata": {"namespace": "clusters", "name": "hc-01"},
                  "status": {"kubeconfig": {"name": "hc-01-admin-kubeconfig"}}}
LIVE_MACHINE = {"metadata": {"name": "hc-01-np-x7k2p"}, "status": {"nodeRef": {"name": "node-1"}}}
REF = BmhRef(server_name=SERVER, namespace=NS)


class TestInspectBoundHost:
    async def test_a_bound_agent_resolves_to_its_machine_node_and_hosted_cluster(self, mce):
        mce(_FakeMce(
            agents=[_agent(label_am="hc-01-np-am-x7k2p", bound="hc-01", state="added-to-existing-cluster")],
            agent_machines=[_agent_machine()],
            machines={(HCP, "hc-01-np-x7k2p"): LIVE_MACHINE},
            hosted_clusters=[HOSTED_CLUSTER],
        ))
        state = await activities.inspect_bound_host(REF)

        assert state.bmh_found and state.bmh_operational_status == "OK"
        assert state.infra_env == "cisco-m6-bat-yam-64c-512gb"
        [agent] = state.agents
        assert agent.cluster_deployment == f"{HCP}/hc-01"
        assert agent.agent_machine == f"{HCP}/hc-01-np-am-x7k2p" and agent.agent_machine_label
        assert agent.state == "added-to-existing-cluster"
        assert state.machine == MachineRef(namespace=HCP, name="hc-01-np-x7k2p")
        assert state.machine_state.node_name == "node-1" and not state.machine_state.deleting
        assert state.hosted_cluster == HostedClusterRef(namespace="clusters", name="hc-01", hcp_namespace=HCP)

    async def test_mid_unbinding_the_agent_ref_still_finds_the_machine(self, mce):
        deleting = {**LIVE_MACHINE, "metadata": {**LIVE_MACHINE["metadata"],
                                                 "deletionTimestamp": "2026-10-24T08:00:00Z"}}
        mce(_FakeMce(
            agents=[_agent(state="unbinding")],
            agent_machines=[_agent_machine(agent_ref={"name": "agent-1", "namespace": NS})],
            machines={(HCP, "hc-01-np-x7k2p"): deleting},
            hosted_clusters=[HOSTED_CLUSTER],
        ))
        state = await activities.inspect_bound_host(REF)

        assert state.bound_agents == [] and [a.name for a in state.hosted_agents] == ["agent-1"]
        assert state.machine_state.deleting

    async def test_an_inventory_host_reads_no_hosted_cluster(self, mce):
        fake = mce(_FakeMce(agents=[_agent(state="known-unbound")]))
        state = await activities.inspect_bound_host(REF)

        assert state.hosted_agents == [] and state.machine is None and state.hosted_cluster is None
        assert fake.cluster_lists == ["agentmachines", "machines", "hostedclusters"]

    async def test_no_agent_lists_nothing_cluster_wide(self, mce):
        fake = mce(_FakeMce())
        state = await activities.inspect_bound_host(REF)
        assert state.agents == [] and fake.cluster_lists == []


class TestEnsureMachineDeleting:
    MACHINE = MachineRef(namespace=HCP, name="hc-01-np-x7k2p")

    async def test_a_live_machine_is_deleted(self, mce):
        fake = mce(_FakeMce(machines={(HCP, "hc-01-np-x7k2p"): LIVE_MACHINE}))
        result = await activities.ensure_machine_deleting(self.MACHINE)
        assert result.deleted and fake.deleted == [("machines", HCP, "hc-01-np-x7k2p")]

    async def test_a_machine_already_deleting_is_left_alone(self, mce):
        deleting = {"metadata": {"deletionTimestamp": "2026-10-24T08:00:00Z"}}
        fake = mce(_FakeMce(machines={(HCP, "hc-01-np-x7k2p"): deleting}))
        result = await activities.ensure_machine_deleting(self.MACHINE)
        assert result.already_deleting and not result.deleted and fake.deleted == []

    async def test_a_machine_already_gone_is_found_false(self, mce):
        fake = mce(_FakeMce())
        result = await activities.ensure_machine_deleting(self.MACHINE)
        assert not result.found and fake.deleted == []


class TestThePolls:
    async def test_agent_binding_and_agent_machine_state(self, mce):
        mce(_FakeMce(
            agents=[_agent(label_am="am", bound="hc-01", state="added-to-existing-cluster")],
            agent_machines=[_agent_machine(name="am")],
        ))
        binding = await activities.get_agent_binding(AgentNameRef(namespace=NS, name="agent-1"))
        assert binding.found and binding.cluster_deployment == f"{HCP}/hc-01" and binding.agent_machine_label
        assert (await activities.get_agent_binding(AgentNameRef(namespace=NS, name="gone"))).found is False
        assert (await activities.get_agent_machine_state(AgentMachineRef(namespace=HCP, name="am"))).found


# --- the hosted cluster's Node ----------------------------------------------------------

SECRET_MARKER = "c2VjcmV0LWtleS1tYXRlcmlhbA=="  # never allowed into an error
KUBECONFIG = f"""
apiVersion: v1
kind: Config
current-context: admin
contexts:
- name: admin
  context: {{cluster: hc, user: admin}}
clusters:
- name: hc
  cluster:
    server: https://api.hc-01.external.example:6443
    certificate-authority-data: {base64.b64encode(b"CA PEM").decode()}
users:
- name: admin
  user:
    client-certificate-data: {base64.b64encode(b"CERT PEM").decode()}
    client-key-data: {SECRET_MARKER}
"""
NODE_REF = HostedNodeRef(
    hosted_cluster=HostedClusterRef(namespace="clusters", name="hc-01", hcp_namespace=HCP),
    node_name="node-1",
)


class TestCheckHostedNode:
    async def test_it_reads_the_admin_kubeconfig_and_asks_the_service_not_the_route(self, mce, monkeypatch):
        secret = {("clusters", "hc-01-admin-kubeconfig"): {
            "kubeconfig": base64.b64encode(KUBECONFIG.encode()).decode()}}
        mce(_FakeMce(hosted_clusters=[HOSTED_CLUSTER], secrets=secret))
        seen = {}

        async def _node_exists(kubeconfig, server, node_name, cluster):
            seen.update(kubeconfig=kubeconfig, server=server, node=node_name, cluster=cluster)
            return False

        monkeypatch.setattr(hosted_cluster_api, "node_exists", _node_exists)
        state = await activities.check_hosted_node(NODE_REF)

        assert state.found is False
        assert seen["server"] == f"https://kube-apiserver.{HCP}.svc:6443"
        assert seen["kubeconfig"] == KUBECONFIG and seen["node"] == "node-1"

    async def test_a_hosted_cluster_that_is_gone_has_no_node(self, mce):
        mce(_FakeMce())
        assert (await activities.check_hosted_node(NODE_REF)).found is False

    async def test_no_published_kubeconfig_is_permanent(self, mce):
        mce(_FakeMce(hosted_clusters=[{"metadata": HOSTED_CLUSTER["metadata"], "status": {}}]))
        with pytest.raises(HostedClusterAccessError, match="publishes no status.kubeconfig"):
            await activities.check_hosted_node(NODE_REF)

    async def test_a_secret_without_the_key_is_permanent(self, mce):
        mce(_FakeMce(hosted_clusters=[HOSTED_CLUSTER],
                     secrets={("clusters", "hc-01-admin-kubeconfig"): {"other": "eA=="}}))
        with pytest.raises(HostedClusterAccessError, match="has no 'kubeconfig' key"):
            await activities.check_hosted_node(NODE_REF)


class _FakeCoreV1:
    """Stands in for CoreV1Api: records the configuration it was built with."""

    seen: ClassVar[dict] = {}
    outcome: ClassVar[Exception | None] = None

    def __init__(self, api_client):
        configuration = api_client.configuration
        _FakeCoreV1.seen = {
            "host": configuration.host,
            "verify_ssl": configuration.verify_ssl,
            "ca": pathlib.Path(configuration.ssl_ca_cert).read_bytes(),
            "cert": pathlib.Path(configuration.cert_file).read_bytes(),
            "key_path": configuration.key_file,
        }

    def read_node(self, name, _request_timeout):
        if _FakeCoreV1.outcome is not None:
            raise _FakeCoreV1.outcome
        return {"metadata": {"name": name}}


class TestTheHostedApiServerCall:
    SERVER = f"https://kube-apiserver.{HCP}.svc:6443"

    @pytest.fixture(autouse=True)
    def _fake_core(self, monkeypatch):
        monkeypatch.setattr(k8s_client, "CoreV1Api", _FakeCoreV1)
        _FakeCoreV1.outcome = None

    async def test_present_and_the_credentials_never_outlive_the_call(self):
        assert await hosted_cluster_api.node_exists(KUBECONFIG, self.SERVER, "node-1", "clusters/hc-01")
        seen = _FakeCoreV1.seen
        assert seen["host"] == self.SERVER and seen["verify_ssl"] is True
        assert (seen["ca"], seen["cert"]) == (b"CA PEM", b"CERT PEM")
        assert not pathlib.Path(seen["key_path"]).exists()  # the temp directory is gone

    async def test_a_404_is_absent(self):
        _FakeCoreV1.outcome = k8s_client.ApiException(status=404)
        assert not await hosted_cluster_api.node_exists(KUBECONFIG, self.SERVER, "node-1", "clusters/hc-01")

    @pytest.mark.parametrize(
        ("outcome", "error"),
        [
            (k8s_client.ApiException(status=403), HostedClusterAccessError),
            (k8s_client.ApiException(status=401), HostedClusterUnreachableError),
            (k8s_client.ApiException(status=503), HostedClusterUnreachableError),
            (urllib3.exceptions.NewConnectionError(None, "refused"), HostedClusterUnreachableError),
            (urllib3.exceptions.SSLError("certificate verify failed"), HostedClusterAccessError),
        ],
    )
    async def test_failures_are_classified(self, outcome, error):
        _FakeCoreV1.outcome = outcome
        with pytest.raises(error) as excinfo:
            await hosted_cluster_api.node_exists(KUBECONFIG, self.SERVER, "node-1", "clusters/hc-01")
        assert SECRET_MARKER not in str(excinfo.value)

    @pytest.mark.parametrize(
        ("kubeconfig", "words"),
        [
            ("{not yaml", "does not parse"),
            (KUBECONFIG.replace("certificate-authority-data", "insecure-skip-tls-verify-x"),
             "no certificate-authority-data"),
            (KUBECONFIG.replace("client-certificate-data", "x").replace("client-key-data", "y"),
             "neither a client certificate"),
            (KUBECONFIG.replace(f"client-key-data: {SECRET_MARKER}", "client-key-data: '%%%'"),
             "not base64"),
        ],
    )
    async def test_an_unusable_kubeconfig_is_permanent_and_never_quoted(self, kubeconfig, words):
        with pytest.raises(HostedClusterAccessError, match=words) as excinfo:
            await hosted_cluster_api.node_exists(kubeconfig, self.SERVER, "node-1", "clusters/hc-01")
        assert SECRET_MARKER not in str(excinfo.value)


# --- what a broken HyperShift deletion leaves behind ------------------------------------

PROVIDER_HOOK = hosted_resources.PROVIDER_MACHINE_HOOK
AM_FINALIZER = hosted_resources.AGENT_MACHINE_FINALIZER


def _machine(name="hc-01-np-x7k2p", namespace=HCP, *, provider_id=None, agent_machine=None,
             node=None, deleting=False, hook=False, conditions=()):
    machine = {
        "apiVersion": "cluster.x-k8s.io/v1beta1",
        "metadata": {"name": name, "namespace": namespace, "annotations": {}},
        "spec": {},
        "status": {"conditions": list(conditions)},
    }
    if provider_id:
        machine["spec"]["providerID"] = provider_id
    if agent_machine:
        machine["spec"]["infrastructureRef"] = {
            "apiVersion": "capi-provider.agent-install.openshift.io/v1beta1",
            "kind": "AgentMachine", "name": agent_machine,
        }
    if node:
        machine["status"]["nodeRef"] = {"name": node}
    if deleting:
        machine["metadata"]["deletionTimestamp"] = "2026-10-24T08:00:00Z"
    if hook:
        machine["metadata"]["annotations"][PROVIDER_HOOK] = ""
    return machine


class TestTheLinks:
    def test_a_machine_is_found_by_any_of_three_links(self):
        am = _agent_machine()
        by_owner = _machine()
        by_infra = _machine("by-infra", agent_machine="hc-01-np-am-x7k2p")
        by_provider_id = _machine("by-pid", provider_id="agent://agent-1")
        unrelated = _machine("other", provider_id="agent://agent-2", agent_machine="am-other")
        found = hosted_resources.machines_for("agent-1", am, [unrelated, by_provider_id, by_infra, by_owner])
        assert [m["metadata"]["name"] for m in found] == ["by-infra", "by-pid", "hc-01-np-x7k2p"]

    def test_with_the_agentmachine_gone_the_provider_id_still_finds_it(self):
        machine = _machine(provider_id="agent://agent-1", agent_machine="gone-am")
        assert hosted_resources.machines_for("agent-1", None, [machine]) == [machine]

    def test_every_agentmachine_that_names_the_agent(self):
        by_ref = _agent_machine("am-ref", agent_ref={"name": "agent-1", "namespace": NS})
        by_label = _agent_machine("am-label")
        other = _agent_machine("am-other", agent_ref={"name": "agent-2", "namespace": NS})
        agent = _agent(label_am="am-label")
        found = hosted_resources.agent_machines_naming(agent, "agent-1", NS, [by_ref, by_label, other])
        assert [a["metadata"]["name"] for a in found] == ["am-ref", "am-label"]
        # A deleted Agent: only agentRefs can still name it.
        assert hosted_resources.agent_machines_naming(None, "agent-1", NS, [by_ref, by_label]) == [by_ref]

    def test_the_hostname_is_the_requested_one_else_the_discovered_one(self):
        agent = _agent()
        agent["status"]["inventory"]["hostname"] = "discovered"
        assert hosted_resources.agent_hostname(agent) == "discovered"
        agent["spec"]["hostname"] = "requested"
        assert hosted_resources.agent_hostname(agent) == "requested"

    @pytest.mark.parametrize(
        ("conditions", "at_hook"),
        [
            ([{"type": "Deleting", "status": "True", "reason": "WaitingForPreTerminateHook"}], True),
            ([{"type": "Deleting", "status": "True", "reason": "DrainingNode"}], False),
            ([{"type": "PreTerminateDeleteHookSucceeded", "status": "False", "reason": "WaitingExternalHook"}], True),
            ([{"type": "PreTerminateDeleteHookSucceeded", "status": "True"}], False),
            ([], False),
        ],
    )
    def test_where_the_providers_hook_takes_over(self, conditions, at_hook):
        state = hosted_resources.machine_state(
            _machine(deleting=True, hook=True, agent_machine="am", conditions=conditions)
        )
        assert state.provider_hook and state.at_pre_terminate_hook is at_hook
        assert state.agent_machine == f"{HCP}/am"

    def test_the_agentmachine_an_agents_label_names(self):
        assert hosted_resources.labelled_agent_machine(_agent(label_am="am-1")) == f"{HCP}/am-1"
        assert hosted_resources.labelled_agent_machine(_agent(label_am="am-1", label_ns=None)) == "-/am-1"
        assert hosted_resources.labelled_agent_machine(_agent()) is None
        # The key is always sent, a None included: the workflow tells this limb
        # from one predating the field by its presence.
        for agent in (_agent(), None):
            binding = hosted_resources.agent_binding_state(agent)
            assert "agent_machine" in binding.model_fields_set and binding.agent_machine is None
        bound = hosted_resources.agent_binding_state(_agent(label_am="am-1", bound="hc-01"))
        assert bound.agent_machine == f"{HCP}/am-1"

    def test_the_release_is_the_providers_own_write(self):
        assert hosted_resources.agent_release_patch("42") == {
            "metadata": {
                "resourceVersion": "42",
                "labels": {"agentMachineRef": None},
                "annotations": {"agentMachineRefNamespace": None},
            },
            "spec": {
                "machineConfigPool": "",
                "ignitionEndpointTokenReference": None,
                "ignitionEndpointHTTPHeaders": None,
                "clusterDeploymentName": None,
            },
        }


def _node(name="node-1", *, provider_id="agent://agent-1", ready=True):
    node = {"metadata": {"name": name}, "spec": {},
            "status": {"conditions": [{"type": "Ready", "status": "True" if ready else "Unknown"}]}}
    if provider_id:
        node["spec"]["providerID"] = provider_id
    return node


class TestNodesAndPods:
    def test_a_node_is_the_servers_by_provider_id_or_by_name_without_another_one(self):
        names = {"node-1", "ocp-x"}
        assert hosted_resources.node_matches(_node(), "agent://agent-1", names) == ["provider-id", "name"]
        assert hosted_resources.node_matches(_node("worker-9"), "agent://agent-1", names) == ["provider-id"]
        assert hosted_resources.node_matches(_node(provider_id=None), "agent://agent-1", names) == ["name"]
        # Same name, another Agent's providerID: another machine.
        assert hosted_resources.node_matches(_node(provider_id="agent://agent-2"), "agent://agent-1", names) == []

    def test_ready_is_the_kubelets_ready_true(self):
        assert hosted_resources.node_ready(_node())
        assert not hosted_resources.node_ready(_node(ready=False))
        assert not hosted_resources.node_ready({"status": {}})

    def test_a_drain_moves_everything_but_daemonset_and_mirror_pods(self):
        def pod(**metadata):
            return {"metadata": {"name": "p", "namespace": "n", **metadata}}

        assert hosted_resources.pod_to_evict(pod())
        assert hosted_resources.pod_to_evict(pod(ownerReferences=[{"kind": "ReplicaSet", "controller": True}]))
        assert not hosted_resources.pod_to_evict(pod(ownerReferences=[{"kind": "DaemonSet", "controller": True}]))
        assert not hosted_resources.pod_to_evict(pod(annotations={"kubernetes.io/config.mirror": "x"}))


class TestInspectingLeftovers:
    async def test_a_machine_whose_agentmachine_is_gone_is_found_by_provider_id(self, mce):
        machine = _machine(provider_id="agent://agent-1", agent_machine="hc-01-np-am-x7k2p",
                           node="node-1", hook=True)
        mce(_FakeMce(
            agents=[_agent(label_am="hc-01-np-am-x7k2p", bound="hc-01", state="added-to-existing-cluster")],
            machines={(HCP, "hc-01-np-x7k2p"): machine},
            hosted_clusters=[HOSTED_CLUSTER],
        ))
        state = await activities.inspect_bound_host(REF)

        [agent] = state.hosted_agents
        assert agent.agent_machine is None and agent.hcp_namespaces == [HCP]
        # What the run started from, though that AgentMachine is gone: not a retake later.
        assert agent.labelled_agent_machine == f"{HCP}/hc-01-np-am-x7k2p"
        assert state.machine == MachineRef(namespace=HCP, name="hc-01-np-x7k2p")
        assert state.machine_state.provider_hook and state.machine_state.agent_machine_found is False
        assert state.hosted_cluster.name == "hc-01"

    async def test_a_binding_with_both_gone_still_resolves_its_hosted_cluster(self, mce):
        mce(_FakeMce(
            agents=[_agent(label_am="gone-am", bound="hc-01", state="added-to-existing-cluster")],
            hosted_clusters=[HOSTED_CLUSTER],
        ))
        state = await activities.inspect_bound_host(REF)

        [agent] = state.hosted_agents
        assert agent.hcp_namespaces == [HCP] and state.machine is None and state.machines_matched == []
        assert state.hosted_cluster == HostedClusterRef(namespace="clusters", name="hc-01", hcp_namespace=HCP)

    async def test_a_binding_into_no_control_plane_is_not_hosted(self, mce):
        mce(_FakeMce(agents=[_agent(bound="ztp-cluster", state="installed")]))
        state = await activities.inspect_bound_host(REF)
        assert state.bound_agents and state.hosted_agents == []

    async def test_every_machine_linked_is_reported(self, mce):
        mce(_FakeMce(
            agents=[_agent(label_am="hc-01-np-am-x7k2p", bound="hc-01")],
            agent_machines=[_agent_machine()],
            machines={
                (HCP, "hc-01-np-x7k2p"): _machine(),
                (HCP, "stray"): _machine("stray", provider_id="agent://agent-1"),
            },
            hosted_clusters=[HOSTED_CLUSTER],
        ))
        state = await activities.inspect_bound_host(REF)
        assert state.machines_matched == [f"{HCP}/hc-01-np-x7k2p", f"{HCP}/stray"]
        assert state.machine is None

    async def test_the_machine_poll_says_whether_its_agentmachine_exists(self, mce):
        mce(_FakeMce(
            agent_machines=[_agent_machine()],
            machines={(HCP, "a"): _machine("a", agent_machine="hc-01-np-am-x7k2p"),
                      (HCP, "b"): _machine("b", agent_machine="gone")},
        ))
        assert (await activities.get_machine_state(MachineRef(namespace=HCP, name="a"))).agent_machine_found
        assert (await activities.get_machine_state(MachineRef(namespace=HCP, name="b"))).agent_machine_found is False


AGENT_REF = AgentNameRef(namespace=NS, name="agent-1")


class TestTheProvidersWrites:
    async def test_the_agent_is_released_with_its_resource_version(self, mce):
        agent = _agent(label_am="gone-am", bound="hc-01")
        agent["metadata"]["resourceVersion"] = "77"
        fake = mce(_FakeMce(agents=[agent]))
        release = await activities.release_agent(
            AgentReleaseRequest(agent=AGENT_REF, cluster_deployment=f"{HCP}/hc-01")
        )
        assert release.released
        [(plural, namespace, name, body)] = fake.patched
        assert (plural, namespace, name) == ("agents", NS, "agent-1")
        assert body == hosted_resources.agent_release_patch("77")

    async def test_an_agent_already_released_is_left_alone(self, mce):
        fake = mce(_FakeMce(agents=[_agent(state="known-unbound")]))
        release = await activities.release_agent(
            AgentReleaseRequest(agent=AGENT_REF, cluster_deployment=f"{HCP}/hc-01")
        )
        assert release.found and not release.released and fake.patched == []

    async def test_an_agent_bound_elsewhere_is_never_released(self, mce):
        fake = mce(_FakeMce(agents=[_agent(bound="hc-02")]))
        with pytest.raises(AgentBindingChangedError, match="hc-02"):
            await activities.release_agent(
                AgentReleaseRequest(agent=AGENT_REF, cluster_deployment=f"{HCP}/hc-01")
            )
        assert fake.patched == []

    async def test_an_agent_a_live_machine_holds_is_never_released(self, mce):
        """The NodePool's replacement took it again — same control plane, so only the holder tells."""
        fake = mce(_FakeMce(
            agents=[_agent(label_am="hc-01-np-am-q9z4d", bound="hc-01", state="binding")],
            agent_machines=[_agent_machine("hc-01-np-am-q9z4d", machine="hc-01-np-q9z4d")],
            machines={(HCP, "hc-01-np-q9z4d"): _machine("hc-01-np-q9z4d")},
        ))
        with pytest.raises(AgentBindingChangedError,
                           match=f"AgentMachine {HCP}/hc-01-np-am-q9z4d of Machine {HCP}/hc-01-np-q9z4d"):
            await activities.release_agent(
                AgentReleaseRequest(agent=AGENT_REF, cluster_deployment=f"{HCP}/hc-01")
            )
        assert fake.patched == []

    async def test_an_agent_whose_agentmachine_no_machine_holds_is_released(self, mce):
        fake = mce(_FakeMce(
            agents=[_agent(label_am="hc-01-np-am-x7k2p", bound="hc-01")],
            agent_machines=[_agent_machine(machine=None)],
        ))
        release = await activities.release_agent(
            AgentReleaseRequest(agent=AGENT_REF, cluster_deployment=f"{HCP}/hc-01")
        )
        assert release.released and len(fake.patched) == 1

    async def test_without_the_namespace_annotation_the_holder_is_found_by_name(self, mce):
        fake = mce(_FakeMce(
            agents=[_agent(label_am="hc-01-np-am-q9z4d", label_ns=None, bound="hc-01")],
            agent_machines=[_agent_machine("hc-01-np-am-q9z4d", machine="hc-01-np-q9z4d")],
            machines={(HCP, "hc-01-np-q9z4d"): _machine("hc-01-np-q9z4d")},
        ))
        with pytest.raises(AgentBindingChangedError, match="a live Machine"):
            await activities.release_agent(
                AgentReleaseRequest(agent=AGENT_REF, cluster_deployment=f"{HCP}/hc-01")
            )
        assert fake.patched == []

    async def test_the_hook_is_removed_only_when_its_agentmachine_is_gone(self, mce):
        orphan = _machine("orphan", agent_machine="gone", deleting=True, hook=True)
        cared_for = _machine("cared-for", agent_machine="hc-01-np-am-x7k2p", deleting=True, hook=True)
        fake = mce(_FakeMce(agent_machines=[_agent_machine()],
                            machines={(HCP, "orphan"): orphan, (HCP, "cared-for"): cared_for}))

        released = await activities.release_machine_hook(MachineRef(namespace=HCP, name="orphan"))
        kept = await activities.release_machine_hook(MachineRef(namespace=HCP, name="cared-for"))

        assert released.released
        assert not kept.released and "the provider releases its own hook" in kept.reason
        assert fake.patched == [("machines", HCP, "orphan",
                                 {"metadata": {"annotations": {PROVIDER_HOOK: None}}})]

    async def test_a_machineless_agentmachine_is_deleted_one_a_machine_owns_is_not(self, mce):
        orphan = _agent_machine("orphan", machine=None, agent_ref={"name": "agent-1", "namespace": NS})
        owned = _agent_machine("owned", agent_ref={"name": "agent-1", "namespace": NS})
        fake = mce(_FakeMce(
            agents=[_agent(state="known-unbound")],
            agent_machines=[orphan, owned],
            machines={(HCP, "hc-01-np-x7k2p"): _machine()},
        ))
        cleanup = await activities.clean_up_agent_machines(AgentMachineCleanupRequest(agent=AGENT_REF))

        assert cleanup.deleted == [f"{HCP}/orphan"] and cleanup.remaining == []
        assert cleanup.held_by_machine == [f"{HCP}/owned (Machine {HCP}/hc-01-np-x7k2p)"]
        assert fake.deleted == [("agentmachines", HCP, "orphan")]

    async def test_the_providers_finalizer_is_dropped_only_when_asked_and_released(self, mce):
        stuck = _agent_machine("stuck", machine=None, agent_ref={"name": "agent-1", "namespace": NS})
        stuck["metadata"].update(deletionTimestamp="2026-10-24T08:00:00Z",
                                 finalizers=[AM_FINALIZER, "someone-else/keep"])
        fake = mce(_FakeMce(agents=[_agent(state="known-unbound")], agent_machines=[stuck]))

        waiting = await activities.clean_up_agent_machines(AgentMachineCleanupRequest(agent=AGENT_REF))
        assert waiting.finalizers_cleared == [] and waiting.remaining[0].startswith(f"{HCP}/stuck")

        cleared = await activities.clean_up_agent_machines(
            AgentMachineCleanupRequest(agent=AGENT_REF, clear_finalizers=True)
        )
        assert cleared.finalizers_cleared == [f"{HCP}/stuck"]
        assert fake.finalizers_cleared == [("agentmachines", HCP, "stuck", [AM_FINALIZER])]
        # Only the provider's own finalizer: another controller's stays.
        assert "someone-else/keep" in cleared.remaining[0]

    async def test_a_bound_agent_keeps_the_finalizer(self, mce):
        stuck = _agent_machine("stuck", machine=None, agent_ref={"name": "agent-1", "namespace": NS})
        stuck["metadata"].update(deletionTimestamp="2026-10-24T08:00:00Z", finalizers=[AM_FINALIZER])
        fake = mce(_FakeMce(agents=[_agent(label_am="stuck", bound="hc-01")], agent_machines=[stuck]))
        cleanup = await activities.clean_up_agent_machines(
            AgentMachineCleanupRequest(agent=AGENT_REF, clear_finalizers=True)
        )
        assert cleanup.finalizers_cleared == [] and fake.finalizers_cleared == []


HC_REF = HostedClusterRef(namespace="clusters", name="hc-01", hcp_namespace=HCP)
SECRETS = {("clusters", "hc-01-admin-kubeconfig"): {
    "kubeconfig": base64.b64encode(KUBECONFIG.encode()).decode()}}


class TestFindHostedNodes:
    async def test_by_cluster_name_and_every_identity(self, mce, monkeypatch):
        mce(_FakeMce(hosted_clusters=[HOSTED_CLUSTER], secrets=SECRETS))

        async def _list_nodes(kubeconfig, server, cluster):
            assert server == f"https://kube-apiserver.{HCP}.svc:6443" and kubeconfig == KUBECONFIG
            return [_node("worker-9"), _node(BMH, provider_id=None, ready=False),
                    _node("node-1", provider_id="agent://agent-2"), _node("unrelated", provider_id=None)]

        monkeypatch.setattr(hosted_cluster_api, "list_nodes", _list_nodes)
        found = await activities.find_hosted_nodes(HostedNodeQuery(
            cluster_name="HC-01", server_name=SERVER, agent_name="agent-1", hostname="node-1",
        ))
        assert found.hosted_cluster == HC_REF
        assert [(n.name, n.matched_by, n.ready) for n in found.nodes] == [
            ("worker-9", ["provider-id"], True),
            (BMH, ["name"], False),
        ]

    async def test_a_name_no_hosted_cluster_has_is_a_reason_not_an_error(self, mce):
        mce(_FakeMce(hosted_clusters=[HOSTED_CLUSTER]))
        found = await activities.find_hosted_nodes(HostedNodeQuery(cluster_name="hc-9", server_name=SERVER))
        assert found.hosted_cluster is None and "hc-9" in found.reason

    async def test_a_hosted_cluster_that_is_gone_has_no_nodes(self, mce):
        mce(_FakeMce())
        found = await activities.find_hosted_nodes(HostedNodeQuery(hosted_cluster=HC_REF, server_name=SERVER))
        assert found.nodes == [] and "gone" in found.reason


class _FakeHostedCore:
    """A hosted cluster's CoreV1Api: Nodes and Pods, recording every write."""

    nodes: ClassVar[dict] = {}
    pods: ClassVar[list] = []
    refuse: ClassVar[dict] = {}
    writes: ClassVar[list] = []

    def __init__(self, api_client):
        pass

    def read_node(self, name, _request_timeout):
        if name not in self.nodes:
            raise k8s_client.ApiException(status=404)
        return self.nodes[name]

    def patch_node(self, name, body, _request_timeout):
        self.writes.append(("cordon", name, body))
        self.nodes[name]["spec"].update(body["spec"])

    def list_pod_for_all_namespaces(self, field_selector, _request_timeout):
        node = field_selector.removeprefix("spec.nodeName=")
        return {"items": [p for p in self.pods if p["spec"]["nodeName"] == node]}

    def create_namespaced_pod_eviction(self, name, namespace, body, _request_timeout):
        if name in self.refuse:
            raise self.refuse[name]
        self.writes.append(("evict", f"{namespace}/{name}"))

    def delete_node(self, name, _request_timeout):
        self.writes.append(("delete", name))
        self.nodes.pop(name)


def _pod(name, node="node-1", **metadata):
    return {"metadata": {"name": name, "namespace": "app", **metadata}, "spec": {"nodeName": node}}


class TestTheHostedWrites:
    SERVER = f"https://kube-apiserver.{HCP}.svc:6443"

    @pytest.fixture(autouse=True)
    def _fake_core(self, monkeypatch):
        monkeypatch.setattr(k8s_client, "CoreV1Api", _FakeHostedCore)
        _FakeHostedCore.nodes = {"node-1": _node()}
        _FakeHostedCore.pods = []
        _FakeHostedCore.refuse = {}
        _FakeHostedCore.writes = []

    async def test_a_drain_pass_cordons_and_evicts_what_a_drain_moves(self):
        refused = k8s_client.ApiException(status=429)
        refused.body = '{"message": "Cannot evict pod as it would violate the pod\'s disruption budget."}'
        _FakeHostedCore.refuse = {"db-0": refused}
        _FakeHostedCore.pods = [
            _pod("web-1"), _pod("db-0"), _pod("going", deletionTimestamp="2026-10-24T09:00:00Z"),
            _pod("ds", ownerReferences=[{"kind": "DaemonSet", "controller": True}]),
            _pod("elsewhere", node="node-2"),
        ]
        outcome = await hosted_cluster_api.drain_node(KUBECONFIG, self.SERVER, "node-1", "agent://agent-1", "c/h")

        assert outcome["cordoned"] and outcome["evicted"] == ["app/web-1"]
        assert outcome["remaining"] == ["app/db-0", "app/going", "app/web-1"]
        assert outcome["blocked"] == ["app/db-0: Cannot evict pod as it would violate the pod's disruption budget."]
        assert _FakeHostedCore.writes == [("cordon", "node-1", {"spec": {"unschedulable": True}}),
                                          ("evict", "app/web-1")]

    async def test_a_node_now_carrying_another_provider_id_is_not_touched(self):
        assert await hosted_cluster_api.drain_node(
            KUBECONFIG, self.SERVER, "node-1", "agent://agent-2", "c/h"
        ) == {"found": True, "matches": False}
        assert await hosted_cluster_api.delete_node(
            KUBECONFIG, self.SERVER, "node-1", None, "c/h"
        ) == {"found": True, "matches": False}
        assert _FakeHostedCore.writes == []

    async def test_only_a_node_nothing_runs_as_is_deleted(self):
        ready = await hosted_cluster_api.delete_node(KUBECONFIG, self.SERVER, "node-1", "agent://agent-1", "c/h")
        assert ready == {"found": True, "ready": True} and _FakeHostedCore.writes == []

        _FakeHostedCore.nodes["node-1"] = _node(ready=False)
        deleted = await hosted_cluster_api.delete_node(KUBECONFIG, self.SERVER, "node-1", "agent://agent-1", "c/h")
        assert deleted == {"found": True, "deleted": True}
        assert _FakeHostedCore.writes == [("delete", "node-1")]

    async def test_a_node_already_gone_is_found_false(self):
        assert await hosted_cluster_api.drain_node(KUBECONFIG, self.SERVER, "gone", None, "c/h") == {"found": False}

    async def test_the_activities_reach_it_through_the_admin_kubeconfig(self, mce):
        mce(_FakeMce(hosted_clusters=[HOSTED_CLUSTER], secrets=SECRETS))
        _FakeHostedCore.nodes["node-1"] = _node(ready=False)
        action = HostedNodeAction(hosted_cluster=HC_REF, node_name="node-1", provider_id="agent://agent-1")
        drain = await activities.drain_hosted_node(action)
        assert drain.found and drain.remaining == []
        assert (await activities.delete_hosted_node(action)).deleted
