"""remove-server's limb activities against a fake cluster API.

What matters is which Agents count as the host's, when an Agent counts as
bound, and that the two activities that ACT re-check the binding first.
"""

from __future__ import annotations

import httpx
import pytest
import respx

from activities.server_lifecycle import activities, cluster_api, server_scan
from activities.server_lifecycle.bmh_resources import AGENT_BMH_LABEL
from shared.exceptions import AgentBoundToClusterError, ServerScanAuthError
from shared.models.server_lifecycle import AgentDeleteRequest, BmhRef, HostPowerRequest

SERVER = "ocp-cisco-m6-bat-yam-64c-512gb-FCH0001"
BMH = SERVER.lower()
NS = "inventory"
REF = BmhRef(server_name=SERVER, namespace=NS)


def _agent(name: str, *, label: str | None = None, macs=(), bound_to: str | None = None) -> dict:
    agent = {
        "metadata": {"name": name, "labels": {AGENT_BMH_LABEL: label} if label else {}},
        "spec": {"approved": True},
        "status": {"inventory": {"interfaces": [{"macAddress": m} for m in macs]}},
    }
    if bound_to:
        agent["spec"]["clusterDeploymentName"] = {"name": bound_to, "namespace": "clusters"}
    return agent


class _FakeCluster:
    def __init__(self, *, bmh=True, nmstate=True, agents=()):
        self.objects: dict[tuple[str, str], dict] = {}
        if bmh:
            self.objects[("baremetalhosts", BMH)] = {
                "spec": {"bootMACAddress": "00:25:B5:00:00:01", "online": True},
                "status": {"poweredOn": True},
            }
        if nmstate:
            self.objects[("nmstateconfigs", f"nmstate-config-{BMH}")] = {
                "spec": {"interfaces": [{"name": "nic1", "macAddress": "00:25:b5:00:00:01"},
                                        {"name": "nic2", "macAddress": "00:25:b5:00:00:02"}]}
            }
        for agent in agents:
            self.objects[("agents", agent["metadata"]["name"])] = agent
        self.patches: list = []
        self.deleted: list = []

    async def read_custom_object(self, *, plural, name, **_):
        return self.objects.get((plural, name))

    async def list_custom_objects(self, *, plural, **_):
        return [obj for (p, _n), obj in self.objects.items() if p == plural]

    async def patch_custom_object(self, *, plural, name, body, **_):
        self.patches.append((plural, name, body))
        return (plural, name) in self.objects

    async def delete_custom_object(self, *, plural, name, **_):
        self.deleted.append((plural, name))
        return self.objects.pop((plural, name), None) is not None


@pytest.fixture
def cluster(monkeypatch):
    """Swap the cluster layer; server-scan answers `scan_macs` for the server."""

    def _install(fake: _FakeCluster, scan_macs: list[str] | None = None) -> _FakeCluster:
        for fn in ("read_custom_object", "list_custom_objects", "patch_custom_object",
                   "delete_custom_object"):
            monkeypatch.setattr(cluster_api, fn, getattr(fake, fn))

        async def _macs(_url, _token, server_name):
            assert server_name == SERVER
            return list(scan_macs or [])

        monkeypatch.setattr(server_scan, "server_macs_by_name", _macs)
        return fake

    return _install


class TestInspect:
    async def test_agents_are_matched_by_bmh_label_or_by_mac_and_others_ignored(self, cluster):
        cluster(_FakeCluster(agents=[
            _agent("by-label", label=BMH),
            _agent("by-mac", macs=["00:25:B5:00:00:02"]),       # case differs: still a match
            _agent("someone-else", macs=["aa:aa:aa:aa:aa:aa"]),
        ]))
        state = await activities.inspect_inventory_host(REF)

        assert state.bmh_found and state.nmstate_found and state.powered_on is True
        assert sorted(a.name for a in state.agents) == ["by-label", "by-mac"]
        assert state.bound_agents == []

    async def test_a_bound_agent_reports_its_cluster(self, cluster):
        cluster(_FakeCluster(agents=[_agent("a1", label=BMH, bound_to="hc-01")]))
        state = await activities.inspect_inventory_host(REF)
        assert [a.cluster_deployment for a in state.bound_agents] == ["clusters/hc-01"]

    async def test_an_agent_whose_bmh_is_gone_is_found_by_server_scans_macs(self, cluster):
        """No BMH, no NMStateConfig, no BMAC label — only server-scan knows its MACs."""
        cluster(
            _FakeCluster(bmh=False, nmstate=False, agents=[
                _agent("orphan", macs=["00:25:b5:00:00:07"]),
                _agent("someone-else", macs=["aa:aa:aa:aa:aa:aa"]),
            ]),
            scan_macs=["00:25:b5:00:00:07", "00:25:b5:00:00:08"],
        )
        state = await activities.inspect_inventory_host(REF)

        assert not state.bmh_found and not state.nmstate_found
        assert [a.name for a in state.agents] == ["orphan"]
        assert state.server_scan_macs == ["00:25:b5:00:00:07", "00:25:b5:00:00:08"]

    async def test_nothing_there_is_not_an_error(self, cluster):
        cluster(_FakeCluster(bmh=False, nmstate=False))
        state = await activities.inspect_inventory_host(REF)
        assert not (state.bmh_found or state.nmstate_found or state.agents)

    async def test_every_mac_the_host_is_known_by_is_reported_for_the_pxe_cleanup(self, cluster):
        cluster(_FakeCluster(), scan_macs=["00:25:b5:00:00:02", "00:25:b5:00:00:09"])
        state = await activities.inspect_inventory_host(REF)
        # BMH boot MAC (upper case there) + NMStateConfig + server-scan, deduped.
        assert state.macs == ["00:25:b5:00:00:01", "00:25:b5:00:00:02", "00:25:b5:00:00:09"]
        # Emitted explicitly, even when empty — the workflow keys on its presence.
        assert "macs" in state.model_fields_set

    async def test_no_mac_at_all_is_still_emitted(self, cluster):
        cluster(_FakeCluster(bmh=False, nmstate=False))
        state = await activities.inspect_inventory_host(REF)
        assert state.macs == [] and "macs" in state.model_fields_set


SS = "http://server-scan.test/api/v1"


class TestServerScanMacsByName:
    @respx.mock
    async def test_the_exact_name_match_contributes_every_mac(self):
        respx.get(f"{SS}/servers").mock(return_value=httpx.Response(200, json={"items": [
            {"id": "srv_1", "name": SERVER},
            {"id": "srv_2", "name": SERVER + "-OTHER"},      # a prefix hit, not this machine
        ]}))
        respx.get(f"{SS}/servers/srv_1").mock(return_value=httpx.Response(200, json={
            "name": SERVER,
            "identity": {"nic_macs": ["00:25:B5:00:00:01", "00:25:B5:00:00:09"]},
            "network": {"interfaces": [{"name": "eth0", "mac": "00:25:b5:00:00:02"}]},
        }))
        other = respx.get(f"{SS}/servers/srv_2")

        macs = await server_scan.server_macs_by_name(SS, "t", SERVER.lower())

        assert macs == ["00:25:b5:00:00:01", "00:25:b5:00:00:02", "00:25:b5:00:00:09"]
        assert not other.called

    @respx.mock
    async def test_an_unknown_server_is_no_macs_not_an_error(self):
        respx.get(f"{SS}/servers").mock(return_value=httpx.Response(200, json={"items": []}))
        assert await server_scan.server_macs_by_name(SS, "t", SERVER) == []

    @respx.mock
    async def test_a_rejected_token_is_permanent(self):
        respx.get(f"{SS}/servers").mock(return_value=httpx.Response(403))
        with pytest.raises(ServerScanAuthError):
            await server_scan.server_macs_by_name(SS, "bad", SERVER)


class TestPowerOff:
    async def test_it_patches_online_false_on_the_lowercased_bmh(self, cluster):
        fake = cluster(_FakeCluster(agents=[_agent("a1", label=BMH)]))
        assert await activities.set_host_online(HostPowerRequest(
            server_name=SERVER, namespace=NS, online=False, agents_must_be_unbound=["a1"]
        )) is True
        assert fake.patches == [("baremetalhosts", BMH, {"spec": {"online": False}})]

    async def test_an_agent_bound_since_the_inspection_stops_the_power_off(self, cluster):
        fake = cluster(_FakeCluster(agents=[_agent("a1", label=BMH, bound_to="hc-01")]))
        with pytest.raises(AgentBoundToClusterError, match="hc-01"):
            await activities.set_host_online(HostPowerRequest(
                server_name=SERVER, namespace=NS, online=False, agents_must_be_unbound=["a1"]
            ))
        assert fake.patches == []


class TestDeleteAgents:
    async def test_unbound_agents_are_deleted_and_missing_ones_reported_absent(self, cluster):
        fake = cluster(_FakeCluster(agents=[_agent("a1", label=BMH)]))
        result = await activities.delete_agents(AgentDeleteRequest(namespace=NS, names=["a1", "gone"]))
        assert result.deleted == ["a1"] and result.absent == ["gone"]
        assert fake.deleted == [("agents", "a1")]

    async def test_a_bound_agent_is_never_deleted(self, cluster):
        fake = cluster(_FakeCluster(agents=[_agent("a1", label=BMH, bound_to="hc-01")]))
        with pytest.raises(AgentBoundToClusterError):
            await activities.delete_agents(AgentDeleteRequest(namespace=NS, names=["a1"]))
        assert fake.deleted == []
