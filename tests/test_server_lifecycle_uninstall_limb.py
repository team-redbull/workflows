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
    AmbiguousServerNameError,
    HostedClusterAccessError,
    HostedClusterUnreachableError,
    ServerScanAuthError,
)
from shared.models.server_lifecycle import (
    AgentMachineRef,
    AgentNameRef,
    BmhRef,
    HostedClusterRef,
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
        return self.namespaced.pop((plural, namespace, name), None) is not None

    async def read_secret_data(self, namespace, name):
        return self.secrets.get((namespace, name))


@pytest.fixture
def mce(monkeypatch):
    def _install(fake: _FakeMce) -> _FakeMce:
        for fn in ("read_custom_object", "list_custom_objects", "list_cluster_custom_objects",
                   "delete_custom_object", "read_secret_data"):
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
        assert fake.cluster_lists == ["agentmachines"]  # no HostedCluster list needed

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
