"""replace_conflicting_nmstate_config: replace a differing NMStateConfig — never while the server is in use.

The incident it exists for: a healthy server's install failed because an
NMStateConfig of its name already existed with other content (BmhConflictError
at create_nmstate_config), and an operator had to delete it by hand. Now the
limb replaces it itself, but only after checking that nothing — no
BareMetalHost by name or by boot MAC, no Agent bound or not — shows the server
installed or in progress. Driven against a small fake of cluster_api, as the
PXE tests drive theirs.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import pytest
from kubernetes import client as k8s_client

from activities.server_lifecycle import activities, cluster_api, server_scan
from activities.server_lifecycle.bmh_resources import (
    build_nmstate_config,
    nmstate_config_differences,
    server_in_use_signs,
)
from shared.exceptions import (
    BmhConflictError,
    BmhPrerequisiteMissingError,
    BmhResourceError,
    ServerLooksInstalledError,
)
from shared.models.server_lifecycle import (
    BmcEndpoint,
    BmhResourceRequest,
    BondMember,
    NmStateConfigReplaceRequest,
)

NAMESPACE = "inventory"
SERVER = "ocp-dell-r650-tlv-64c-1024gb-DEL0000485"
BMH_NAME = SERVER.lower()
NMSTATE_NAME = f"nmstate-config-{BMH_NAME}"

RESOURCE = BmhResourceRequest(
    server_name=SERVER,
    namespace=NAMESPACE,
    infra_env="dell-r650-tlv-64c-1024gb",
    bmc_vendor="DELL",
    bmc=BmcEndpoint(host="10.11.1.229", host_is_ip=True, scheme="redfish"),
    bond_members=[
        BondMember(logical_name="nic1", mac="aa:bb:cc:dd:ee:01", port_key="1/1"),
        BondMember(logical_name="nic2", mac="aa:bb:cc:dd:ee:02", port_key="1/2"),
    ],
    vlan_id=24,
)
REQUEST = NmStateConfigReplaceRequest(
    resource=RESOURCE,
    server_macs=["aa:bb:cc:dd:ee:01", "aa:bb:cc:dd:ee:02", "aa:bb:cc:dd:ee:03"],
)
# What server-scan holds for the name: NPAR partition MACs too.
SCAN_MACS = ["aa:bb:cc:dd:ee:01", "aa:bb:cc:dd:ee:02", "aa:bb:cc:dd:ee:41"]


def _stale_nmstate(vlan: int = 77, macs: tuple[str, ...] = ("aa:bb:cc:dd:ee:01", "aa:bb:cc:dd:ee:02")) -> dict:
    """An NMStateConfig of the server's name built for another VLAN / other MACs."""
    stale = RESOURCE.model_copy(
        update={
            "vlan_id": vlan,
            "bond_members": [
                BondMember(logical_name=f"nic{i}", mac=mac, port_key=str(i))
                for i, mac in enumerate(macs, start=1)
            ],
        }
    )
    body = build_nmstate_config(stale)
    body["metadata"].update(
        uid="uid-old", resourceVersion="41", creationTimestamp="2026-10-01T09:00:00Z"
    )
    return body


def _bmh(name: str, boot_mac: str, state: str = "provisioned") -> dict:
    return {
        "metadata": {"name": name, "creationTimestamp": "2026-10-30T08:00:00Z"},
        "spec": {"bootMACAddress": boot_mac},
        "status": {"provisioning": {"state": state}},
    }


def _agent(name: str, macs: list[str], bound_to: str | None = None, label: str | None = None) -> dict:
    return {
        "metadata": {"name": name, "labels": {"agent-install.openshift.io/bmh": label} if label else {}},
        "spec": {"clusterDeploymentName": {"name": bound_to, "namespace": "hc-ns"} if bound_to else {}},
        "status": {
            "inventory": {"interfaces": [{"macAddress": mac} for mac in macs]},
            "debugInfo": {"state": "known-unbound" if not bound_to else "added-to-existing-cluster"},
        },
    }


class FakeCluster:
    """One namespace of an MCE: objects by (plural, name), every write recorded."""

    def __init__(self, monkeypatch, objects: dict[tuple[str, str], dict] | None = None) -> None:
        self.objects = dict(objects or {})
        self.writes: list[tuple[str, ...]] = []
        self.lists: list[str] = []
        self.change_before_delete = False
        monkeypatch.setattr(cluster_api, "read_custom_object", self.read)
        monkeypatch.setattr(cluster_api, "list_custom_objects", self.list)
        monkeypatch.setattr(cluster_api, "delete_custom_object_exactly", self.delete_exactly)
        monkeypatch.setattr(cluster_api, "create_custom_object_if_absent", self.create)

        async def _scan_macs(_url: str, _token: str, name: str) -> list[str]:
            assert name == SERVER
            return list(SCAN_MACS)

        monkeypatch.setattr(server_scan, "server_macs_by_name", _scan_macs)

    async def read(self, *, plural: str, namespace: str, name: str, **_: Any) -> dict | None:
        assert namespace == NAMESPACE
        return self.objects.get((plural, name))

    async def list(self, *, plural: str, namespace: str, **_: Any) -> list[dict]:
        assert namespace == NAMESPACE
        self.lists.append(plural)
        return [obj for (p, _), obj in self.objects.items() if p == plural]

    async def delete_exactly(self, *, plural: str, name: str, uid: str, resource_version: str, **_: Any) -> bool:
        current = self.objects.get((plural, name))
        if current is None:
            return False
        if self.change_before_delete:
            raise BmhResourceError(f"{name} changed between its read and its delete")
        assert (current["metadata"]["uid"], current["metadata"]["resourceVersion"]) == (uid, resource_version)
        del self.objects[(plural, name)]
        self.writes.append(("delete", plural, name))
        return True

    async def create(self, *, plural: str, body: dict, differences, **_: Any):
        name = body["metadata"]["name"]
        if (plural, name) in self.objects:
            if differences(body, self.objects[(plural, name)]):
                raise BmhConflictError(f"{name} already exists and does not match")
            return None
        stored = json.loads(json.dumps(body))
        stored["metadata"].update(uid="uid-new", resourceVersion="1", creationTimestamp="2026-10-31T10:00:00Z")
        self.objects[(plural, name)] = stored
        self.writes.append(("create", plural, name))
        return None


@pytest.fixture
def stale(monkeypatch):
    """An MCE holding a stale NMStateConfig of the server's name, and nothing else of it."""
    return FakeCluster(monkeypatch, {("nmstateconfigs", NMSTATE_NAME): _stale_nmstate()})


class TestNothingToReplace:
    async def test_no_nmstateconfig_writes_nothing(self, monkeypatch):
        cluster = FakeCluster(monkeypatch)
        result = await activities.replace_conflicting_nmstate_config(REQUEST)
        assert (result.found, result.replaced) == (False, False)
        assert cluster.writes == []

    async def test_a_matching_one_converges_untouched(self, monkeypatch):
        matching = build_nmstate_config(RESOURCE)
        matching["metadata"].update(uid="u", resourceVersion="1")
        cluster = FakeCluster(monkeypatch, {("nmstateconfigs", NMSTATE_NAME): matching})

        result = await activities.replace_conflicting_nmstate_config(REQUEST)

        assert (result.found, result.replaced) == (True, False)
        assert cluster.writes == []


class TestReplacedWhenNothingIsInUse:
    async def test_it_is_deleted_recreated_from_the_install_and_read_back(self, stale, caplog):
        caplog.set_level(logging.WARNING)
        result = await activities.replace_conflicting_nmstate_config(REQUEST)

        assert result.found and result.replaced
        assert result.differences == ["VLAN id: want ['24'], found ['77']"]
        assert stale.writes == [
            ("delete", "nmstateconfigs", NMSTATE_NAME),
            ("create", "nmstateconfigs", NMSTATE_NAME),
        ]
        written = stale.objects[("nmstateconfigs", NMSTATE_NAME)]
        assert nmstate_config_differences(build_nmstate_config(RESOURCE), written) == []
        # A NEW object: what an IPMI blade's boot-entry publish waits on.
        assert written["metadata"]["creationTimestamp"] == "2026-10-31T10:00:00Z"
        assert "nmstateconfig-replaced" in caplog.text
        assert "VLAN id" in caplog.text

    async def test_hosts_and_agents_of_other_servers_do_not_count(self, monkeypatch):
        cluster = FakeCluster(
            monkeypatch,
            {
                ("nmstateconfigs", NMSTATE_NAME): _stale_nmstate(),
                ("baremetalhosts", "ocp-other"): _bmh("ocp-other", "11:22:33:44:55:66"),
                ("agents", "agent-other"): _agent("agent-other", ["11:22:33:44:55:66"], bound_to="hc-a"),
            },
        )
        result = await activities.replace_conflicting_nmstate_config(REQUEST)

        assert result.replaced
        assert cluster.lists == ["baremetalhosts", "agents"]

    async def test_a_retry_after_the_delete_leaves_the_create_to_the_install(self, stale):
        """Idempotent: absent now, so the run's create_nmstate_config makes it as always."""
        del stale.objects[("nmstateconfigs", NMSTATE_NAME)]
        result = await activities.replace_conflicting_nmstate_config(REQUEST)
        assert (result.found, result.replaced) == (False, False)
        assert stale.writes == []

    async def test_a_retry_after_the_create_finds_it_matching(self, stale):
        await activities.replace_conflicting_nmstate_config(REQUEST)
        again = await activities.replace_conflicting_nmstate_config(REQUEST)
        assert (again.found, again.replaced) == (True, False)
        assert len(stale.writes) == 2

    async def test_an_object_changed_since_its_read_is_read_again_not_deleted(self, stale):
        stale.change_before_delete = True
        with pytest.raises(BmhResourceError):
            await activities.replace_conflicting_nmstate_config(REQUEST)
        assert stale.writes == []


class TestRefusedWhileTheServerLooksInUse:
    """Any sign, and NOTHING is changed."""

    @pytest.mark.parametrize(
        ("extra", "says"),
        [
            pytest.param(
                {("baremetalhosts", BMH_NAME): _bmh(BMH_NAME, "aa:bb:cc:dd:ee:01")},
                f"BareMetalHost {BMH_NAME} exists (provisioning state 'provisioned'",
                id="bmh-named-after-it",
            ),
            pytest.param(
                {("baremetalhosts", "hand-made"): _bmh("hand-made", "AA:BB:CC:DD:EE:41")},
                "BareMetalHost hand-made boots from its MAC aa:bb:cc:dd:ee:41",
                id="bmh-booting-from-an-npar-mac-server-scan-holds",
            ),
            pytest.param(
                {("baremetalhosts", "hand-made"): _bmh("hand-made", "aa:bb:cc:dd:ee:99")},
                "BareMetalHost hand-made boots from its MAC aa:bb:cc:dd:ee:99",
                id="bmh-booting-from-a-mac-the-stale-config-names",
            ),
            pytest.param(
                {("agents", "agent-1"): _agent("agent-1", ["aa:bb:cc:dd:ee:02"], bound_to="hc-a")},
                "Agent agent-1 is this host's (bound to cluster hc-ns/hc-a",
                id="agent-bound-to-a-cluster",
            ),
            pytest.param(
                {("agents", "agent-1"): _agent("agent-1", ["aa:bb:cc:dd:ee:03"])},
                "unbound — the host runs discovery",
                id="agent-in-inventory",
            ),
            pytest.param(
                {("agents", "agent-1"): _agent("agent-1", ["12:34:56:78:9a:bc"], label=BMH_NAME)},
                "Agent agent-1 is this host's",
                id="agent-bmac-linked-to-its-name",
            ),
        ],
    )
    async def test_a_sign_refuses_and_changes_nothing(self, monkeypatch, extra, says):
        stale_config = _stale_nmstate(macs=("aa:bb:cc:dd:ee:01", "aa:bb:cc:dd:ee:99"))
        cluster = FakeCluster(monkeypatch, {("nmstateconfigs", NMSTATE_NAME): stale_config, **extra})

        with pytest.raises(ServerLooksInstalledError) as excinfo:
            await activities.replace_conflicting_nmstate_config(REQUEST)

        message = str(excinfo.value)
        assert says in message
        assert "looks installed or in progress" in message
        assert "Nothing was changed" in message
        assert "VLAN id: want ['24'], found ['77']" in message
        assert cluster.writes == []
        assert cluster.objects[("nmstateconfigs", NMSTATE_NAME)] is stale_config

    async def test_a_host_named_after_it_decides_without_listing_others(self, monkeypatch):
        # The `get` alone settles it, so an MCE whose Role predates the `list`
        # verb on baremetalhosts still refuses correctly here.
        cluster = FakeCluster(
            monkeypatch,
            {
                ("nmstateconfigs", NMSTATE_NAME): _stale_nmstate(),
                ("baremetalhosts", BMH_NAME): _bmh(BMH_NAME, "aa:bb:cc:dd:ee:01"),
            },
        )
        with pytest.raises(ServerLooksInstalledError):
            await activities.replace_conflicting_nmstate_config(REQUEST)
        assert "baremetalhosts" not in cluster.lists

    async def test_a_missing_list_verb_fails_closed(self, stale, monkeypatch):
        async def _forbidden(**_: Any):
            raise BmhPrerequisiteMissingError("Cannot list BareMetalHost: not authorized")

        monkeypatch.setattr(cluster_api, "list_custom_objects", _forbidden)
        with pytest.raises(BmhPrerequisiteMissingError):
            await activities.replace_conflicting_nmstate_config(REQUEST)
        assert stale.writes == []


class TestTheInUseRule:
    """server_in_use_signs alone, pure."""

    def test_nothing_on_the_mce_is_no_sign(self):
        assert server_in_use_signs(BMH_NAME, SCAN_MACS, [], []) == []

    def test_a_bmh_with_no_boot_mac_is_no_sign_unless_named_after_it(self):
        assert server_in_use_signs(BMH_NAME, SCAN_MACS, [{"metadata": {"name": "x"}}], []) == []


class TestDeleteExactly:
    """cluster_api.delete_custom_object_exactly against a stand-in client."""

    @pytest.fixture(autouse=True)
    def _no_kubeconfig(self, monkeypatch):
        monkeypatch.setattr(cluster_api, "_load_kube", lambda: None)

    def _api(self, monkeypatch, error: k8s_client.ApiException | None = None) -> list[dict]:
        calls: list[dict] = []

        class _Api:
            def delete_namespaced_custom_object(self, **kwargs):
                calls.append(kwargs)
                if error is not None:
                    raise error

        monkeypatch.setattr(cluster_api.k8s_client, "CustomObjectsApi", _Api)
        return calls

    async def _delete(self) -> bool:
        return await cluster_api.delete_custom_object_exactly(
            group="agent-install.openshift.io", version="v1beta1", plural="nmstateconfigs",
            kind="NMStateConfig", namespace=NAMESPACE, name=NMSTATE_NAME,
            uid="uid-old", resource_version="41",
        )

    async def test_it_sends_both_preconditions(self, monkeypatch):
        calls = self._api(monkeypatch)
        assert await self._delete() is True
        preconditions = calls[0]["body"].preconditions
        assert (preconditions.uid, preconditions.resource_version) == ("uid-old", "41")

    async def test_gone_already_is_false(self, monkeypatch):
        self._api(monkeypatch, k8s_client.ApiException(status=404))
        assert await self._delete() is False

    async def test_a_failed_precondition_is_transient(self, monkeypatch):
        self._api(monkeypatch, k8s_client.ApiException(status=409, reason="Conflict"))
        with pytest.raises(BmhResourceError, match="changed between its read and its delete"):
            await self._delete()

    async def test_forbidden_is_a_deployment_gap(self, monkeypatch):
        self._api(monkeypatch, k8s_client.ApiException(status=403, reason="Forbidden"))
        with pytest.raises(BmhPrerequisiteMissingError):
            await self._delete()
