"""register_pxe_boot and the PXE map client — the network-boot path for IPMI servers.

Mostly CLASSIFICATION, for the reason test_server_lifecycle_limb.py gives: the
workflow retries without bound, so a status sorted into the wrong bucket does
not fail a run, it retries every minute forever. The activity is driven with
the cluster layer swapped out and the PXE VM mocked with respx.
"""

from __future__ import annotations

import json

import httpx
import pytest
import respx
from pydantic import ValidationError

from activities.server_lifecycle import activities, cluster_api
from activities.server_lifecycle.pxe_map import map_key, put_mapping
from shared.bmc_address import BMC_VENDORS, boots_from_network
from shared.exceptions import (
    InfraEnvNotFoundError,
    InfraEnvNotReadyError,
    PxeMapAuthError,
    PxeMapError,
    PxeMapRequestRejectedError,
    PxeSiteNotConfiguredError,
)
from shared.models.server_lifecycle import PxeBootRequest
from shared.settings import ServerLifecycleActivitySettings

PXE = "http://pxe.bat-yam.test:8080"  # PXE_MAP_URLS["bat-yam"] in conftest
IPXE = "https://assisted-image-service.test/byid/abc/4.16/x86_64/ipxe-script"
MAC = "00:25:B5:00:00:01"

REQUEST = PxeBootRequest(
    server_name="ocp-cisco-m6-bat-yam-64c-512gb-FCH0001",
    namespace="multicluster-engine",
    infra_env="cisco-m6-bat-yam-64c-512gb",
    site="bat-yam",
    macs=["00:25:b5:00:00:01", "00:25:b5:00:00:02"],
)


class TestPxeMapUrlsSetting:
    def test_a_trailing_slash_is_dropped(self, monkeypatch):
        monkeypatch.setenv("PXE_MAP_URLS", '{"bat-yam": "http://pxe:8080/"}')
        assert ServerLifecycleActivitySettings().pxe_map_urls == {
            "bat-yam": "http://pxe:8080"
        }

    def test_a_url_without_a_scheme_is_refused_at_startup(self, monkeypatch):
        monkeypatch.setenv("PXE_MAP_URLS", '{"bat-yam": "pxe:8080"}')
        with pytest.raises(ValidationError, match="pxe_map_urls"):
            ServerLifecycleActivitySettings()

    def test_absent_means_no_site_has_one(self, monkeypatch):
        monkeypatch.delenv("PXE_MAP_URLS")
        assert ServerLifecycleActivitySettings().pxe_map_urls == {}


class TestWhoBootsFromTheNetwork:
    def test_only_a_ucs_managed_cisco_is_network_booted(self):
        assert {v for v in BMC_VENDORS if boots_from_network(v)} == {"CISCO"}

    def test_intersight_cisco_has_virtual_media(self):
        # Same hardware maker, different manager: Intersight exposes Redfish.
        assert not boots_from_network("INTERSIGHT")


class TestThePxeMapClient:
    def test_the_key_is_lower_case_hex_without_separators(self):
        assert map_key(MAC) == "0025b5000001"

    @respx.mock
    async def test_a_put_sends_the_ipxe_url_with_the_bearer_token(self):
        route = respx.put(f"{PXE}/pxe-map/0025b5000001").mock(
            return_value=httpx.Response(200)
        )
        await put_mapping(PXE, "secret", MAC, IPXE)

        sent = route.calls.last.request
        assert json.loads(sent.content) == {"ipxe_url": IPXE}
        assert sent.headers["Authorization"] == "Bearer secret"

    @respx.mock
    async def test_no_token_sends_no_auth_header(self):
        route = respx.put(f"{PXE}/pxe-map/0025b5000001").mock(
            return_value=httpx.Response(204)
        )
        await put_mapping(PXE, "", MAC, IPXE)
        assert "Authorization" not in route.calls.last.request.headers

    @pytest.mark.parametrize("status", [401, 403])
    @respx.mock
    async def test_a_rejected_token_is_permanent(self, status):
        respx.put(url__startswith=PXE).mock(return_value=httpx.Response(status))
        with pytest.raises(PxeMapAuthError):
            await put_mapping(PXE, "bad", MAC, IPXE)

    @pytest.mark.parametrize("status", [400, 404, 422])
    @respx.mock
    async def test_a_refused_request_is_permanent(self, status):
        respx.put(url__startswith=PXE).mock(return_value=httpx.Response(status, text="no"))
        with pytest.raises(PxeMapRequestRejectedError):
            await put_mapping(PXE, "t", MAC, IPXE)

    @respx.mock
    async def test_a_server_error_is_retried(self):
        respx.put(url__startswith=PXE).mock(return_value=httpx.Response(503))
        with pytest.raises(PxeMapError):
            await put_mapping(PXE, "t", MAC, IPXE)

    @respx.mock
    async def test_an_unreachable_vm_is_retried(self):
        respx.put(url__startswith=PXE).mock(side_effect=httpx.ConnectError("refused"))
        with pytest.raises(PxeMapError):
            await put_mapping(PXE, "t", MAC, IPXE)


@pytest.fixture
def infra_env(monkeypatch):
    """Swap the cluster read for a scripted InfraEnv (None = absent)."""
    reads: list[dict] = []

    def _install(obj: dict | None):
        async def _read(**kwargs):
            reads.append(kwargs)
            return obj

        monkeypatch.setattr(cluster_api, "read_custom_object", _read)
        return reads

    return _install


def _infra_env(ipxe: str | None = IPXE) -> dict:
    return {"status": {"bootArtifacts": {"ipxeScript": ipxe} if ipxe else {}}}


class TestRegisterPxeBoot:
    @respx.mock
    async def test_both_macs_are_pointed_at_the_infraenvs_ipxe_script(self, infra_env):
        reads = infra_env(_infra_env())
        routes = [
            respx.put(f"{PXE}/pxe-map/0025b5000001").mock(return_value=httpx.Response(200)),
            respx.put(f"{PXE}/pxe-map/0025b5000002").mock(return_value=httpx.Response(200)),
        ]

        result = await activities.register_pxe_boot(REQUEST)

        assert all(r.called for r in routes)
        for route in routes:
            assert json.loads(route.calls.last.request.content) == {"ipxe_url": IPXE}
        assert reads[0]["plural"] == "infraenvs"
        assert reads[0]["name"] == REQUEST.infra_env
        assert reads[0]["namespace"] == REQUEST.namespace
        assert result.pxe_map_url == PXE
        assert result.macs == REQUEST.macs

    @respx.mock
    async def test_an_unknown_site_fails_before_touching_the_cluster(self, infra_env):
        reads = infra_env(_infra_env())
        with pytest.raises(PxeSiteNotConfiguredError, match="PXE_MAP_URLS"):
            await activities.register_pxe_boot(
                REQUEST.model_copy(update={"site": "haifa"})
            )
        assert reads == []

    async def test_a_missing_infraenv_is_permanent(self, infra_env):
        infra_env(None)
        with pytest.raises(InfraEnvNotFoundError):
            await activities.register_pxe_boot(REQUEST)

    @respx.mock
    async def test_an_infraenv_without_an_ipxe_script_yet_is_retried(self, infra_env):
        infra_env(_infra_env(ipxe=None))
        route = respx.put(url__startswith=PXE)
        with pytest.raises(InfraEnvNotReadyError):
            await activities.register_pxe_boot(REQUEST)
        assert not route.called
