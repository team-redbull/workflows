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
from activities.server_lifecycle.bmh_resources import infraenv_image_wait_reason
from activities.server_lifecycle.pxe_map import (
    delete_mapping,
    delete_mapping_for_mce,
    map_key,
    put_infra_env_boot,
    put_mapping,
    put_mapping_to_infra_env,
)
from shared.bmc_address import BMC_VENDORS, boots_from_network
from shared.exceptions import (
    InfraEnvNotFoundError,
    InfraEnvNotReadyError,
    PxeMapAuthError,
    PxeMapError,
    PxeMapRequestRejectedError,
    PxeSiteNotConfiguredError,
)
from shared.models.server_lifecycle import (
    InfraEnvBootRequest,
    InfraEnvRef,
    PxeBootRequest,
    PxeHostEntriesRef,
    PxeMapRef,
)
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


MCE = "ocp4-mce-bat-yam-01"
ENTRY = f"{PXE}/pxe-map/{MCE}/{REQUEST.infra_env}"
ENTRY_HINT = "predates InfraEnv boot entries"


class TestTheBootEntryClient:
    @respx.mock
    async def test_a_mac_is_pointed_at_the_infra_env_entry(self):
        route = respx.put(f"{PXE}/pxe-map/0025b5000001").mock(return_value=httpx.Response(200))
        await put_mapping_to_infra_env(PXE, "secret", MAC, MCE, REQUEST.infra_env)

        sent = route.calls.last.request
        assert json.loads(sent.content) == {
            "infra_env": {"mce_cluster": MCE, "name": REQUEST.infra_env}
        }
        assert sent.headers["Authorization"] == "Bearer secret"

    @respx.mock
    async def test_an_old_pxe_map_api_refusing_the_body_is_permanent_and_says_so(self):
        respx.put(url__startswith=PXE).mock(
            return_value=httpx.Response(422, text='{"detail":"ipxe_url Field required"}')
        )
        with pytest.raises(PxeMapRequestRejectedError, match=ENTRY_HINT):
            await put_mapping_to_infra_env(PXE, "t", MAC, MCE, REQUEST.infra_env)

    @respx.mock
    async def test_the_entry_is_published_with_its_image_time(self):
        route = respx.put(ENTRY).mock(return_value=httpx.Response(200))
        written = await put_infra_env_boot(
            PXE, "secret", MCE, REQUEST.infra_env, IPXE, "2026-10-19T10:00:05Z"
        )
        assert written is True
        assert json.loads(route.calls.last.request.content) == {
            "ipxe_url": IPXE,
            "image_created_at": "2026-10-19T10:00:05Z",
        }

    @respx.mock
    async def test_a_newer_image_already_published_is_not_an_error(self):
        respx.put(ENTRY).mock(return_value=httpx.Response(409, text="newer"))
        assert (
            await put_infra_env_boot(PXE, "t", MCE, REQUEST.infra_env, IPXE, "2026-10-19T10:00:05Z")
            is False
        )

    @respx.mock
    async def test_a_pxe_map_api_without_the_route_is_permanent_and_says_so(self):
        respx.put(ENTRY).mock(return_value=httpx.Response(404, text='{"detail":"Not Found"}'))
        with pytest.raises(PxeMapRequestRejectedError, match=ENTRY_HINT):
            await put_infra_env_boot(PXE, "t", MCE, REQUEST.infra_env, IPXE, "2026-10-19T10:00:05Z")

    @pytest.mark.parametrize("status,error", [(401, PxeMapAuthError), (503, PxeMapError)])
    @respx.mock
    async def test_other_failures_are_classified_as_for_a_mac(self, status, error):
        respx.put(ENTRY).mock(return_value=httpx.Response(status))
        with pytest.raises(error):
            await put_infra_env_boot(PXE, "t", MCE, REQUEST.infra_env, IPXE, "2026-10-19T10:00:05Z")


class TestRegisterPxeBootToTheBootEntry:
    @respx.mock
    async def test_both_macs_chain_to_the_entry_without_reading_the_infraenv(self, infra_env):
        reads = infra_env(_infra_env())
        routes = [
            respx.put(f"{PXE}/pxe-map/0025b5000001").mock(return_value=httpx.Response(200)),
            respx.put(f"{PXE}/pxe-map/0025b5000002").mock(return_value=httpx.Response(200)),
        ]

        result = await activities.register_pxe_boot(
            REQUEST.model_copy(update={"mce_cluster": MCE})
        )

        for route in routes:
            assert json.loads(route.calls.last.request.content) == {
                "infra_env": {"mce_cluster": MCE, "name": REQUEST.infra_env}
            }
        # The entry is published later, once the image includes the host.
        assert reads == []
        assert result.macs == REQUEST.macs

    async def test_an_unknown_site_is_still_the_skip(self, infra_env):
        infra_env(_infra_env())
        with pytest.raises(PxeSiteNotConfiguredError):
            await activities.register_pxe_boot(
                REQUEST.model_copy(update={"mce_cluster": MCE, "site": "haifa"})
            )


NMSTATE_CREATED = "2026-10-19T10:00:05Z"
BOOT_REQUEST = InfraEnvBootRequest(
    server_name=REQUEST.server_name,
    namespace=REQUEST.namespace,
    infra_env=REQUEST.infra_env,
    mce_cluster=MCE,
    site="bat-yam",
)


def _image(
    created_at: str | None = "2026-10-19T10:00:09Z",
    image_created: str = "True",
    message: str = "Image has been created",
    ipxe: str | None = IPXE,
) -> dict:
    status: dict = {
        "conditions": [
            {"type": "ImageCreated", "status": image_created, "message": message}
        ],
        "bootArtifacts": {"ipxeScript": ipxe} if ipxe else {},
    }
    if created_at:
        status["createdTime"] = created_at
    return {"status": status}


def _nmstate(created_at: str | None = NMSTATE_CREATED) -> dict:
    return {"metadata": {"name": "nmstate-config-x", "creationTimestamp": created_at}}


@pytest.fixture
def cluster(monkeypatch):
    """Swap the cluster read for scripted objects BY PLURAL (missing = absent)."""
    reads: list[dict] = []

    def _install(objects: dict[str, dict | None]):
        async def _read(**kwargs):
            reads.append(kwargs)
            return objects.get(kwargs["plural"])

        monkeypatch.setattr(cluster_api, "read_custom_object", _read)
        return reads

    return _install


class TestPublishInfraEnvBoot:
    @respx.mock
    async def test_an_image_generated_after_the_nmstateconfig_is_published(self, cluster):
        reads = cluster({"infraenvs": _image(), "nmstateconfigs": _nmstate()})
        route = respx.put(ENTRY).mock(return_value=httpx.Response(200))

        state = await activities.publish_infra_env_boot(BOOT_REQUEST)

        assert state.published is True
        assert state.superseded is False
        assert state.image_created_at == "2026-10-19T10:00:09Z"
        assert json.loads(route.calls.last.request.content) == {
            "ipxe_url": IPXE,
            "image_created_at": "2026-10-19T10:00:09Z",
        }
        # The host's own NMStateConfig, by the name the create gave it.
        nmstate_read = next(r for r in reads if r["plural"] == "nmstateconfigs")
        assert nmstate_read["name"] == "nmstate-config-ocp-cisco-m6-bat-yam-64c-512gb-fch0001"
        assert nmstate_read["namespace"] == REQUEST.namespace

    @respx.mock
    async def test_an_image_from_before_the_nmstateconfig_is_not_published(self, cluster):
        cluster(
            {"infraenvs": _image(created_at="2026-10-19T10:00:01Z"), "nmstateconfigs": _nmstate()}
        )
        route = respx.put(url__startswith=PXE)

        state = await activities.publish_infra_env_boot(BOOT_REQUEST)

        assert state.published is False
        assert "before the NMStateConfig" in (state.detail or "")
        assert not route.called

    @respx.mock
    async def test_a_failed_regeneration_is_waited_on_with_its_message(self, cluster):
        cluster(
            {
                "infraenvs": _image(image_created="False", message="failed to validate nmstate"),
                "nmstateconfigs": _nmstate(),
            }
        )
        route = respx.put(url__startswith=PXE)

        state = await activities.publish_infra_env_boot(BOOT_REQUEST)

        assert state.published is False
        assert "failed to validate nmstate" in (state.detail or "")
        assert not route.called

    @respx.mock
    async def test_a_newer_image_already_on_the_pxe_vm_is_superseded_success(self, cluster):
        cluster({"infraenvs": _image(), "nmstateconfigs": _nmstate()})
        respx.put(ENTRY).mock(return_value=httpx.Response(409, text="newer"))

        state = await activities.publish_infra_env_boot(BOOT_REQUEST)

        assert state.published is True
        assert state.superseded is True

    async def test_an_unknown_site_fails_before_touching_the_cluster(self, cluster):
        reads = cluster({"infraenvs": _image(), "nmstateconfigs": _nmstate()})
        with pytest.raises(PxeSiteNotConfiguredError):
            await activities.publish_infra_env_boot(
                BOOT_REQUEST.model_copy(update={"site": "haifa"})
            )
        assert reads == []

    async def test_a_missing_infraenv_is_permanent(self, cluster):
        cluster({"nmstateconfigs": _nmstate()})
        with pytest.raises(InfraEnvNotFoundError):
            await activities.publish_infra_env_boot(BOOT_REQUEST)

    @respx.mock
    async def test_without_a_server_the_current_image_is_published_no_nmstate_read(self, cluster):
        # publish-infraenv-boot: an image from BEFORE any host's NMStateConfig
        # is still the InfraEnv's current one, and that is what is asked for.
        reads = cluster({"infraenvs": _image(created_at="2026-10-19T09:00:00Z")})
        route = respx.put(ENTRY).mock(return_value=httpx.Response(200))

        state = await activities.publish_infra_env_boot(
            BOOT_REQUEST.model_copy(update={"server_name": None})
        )

        assert state.published is True
        assert state.image_created_at == "2026-10-19T09:00:00Z"
        assert route.called
        assert [r["plural"] for r in reads] == ["infraenvs"]

    @respx.mock
    async def test_without_a_server_a_failed_image_is_still_waited_on(self, cluster):
        cluster({"infraenvs": _image(image_created="False", message="nmstate invalid")})
        route = respx.put(url__startswith=PXE)
        state = await activities.publish_infra_env_boot(
            BOOT_REQUEST.model_copy(update={"server_name": None})
        )
        assert state.published is False
        assert "nmstate invalid" in (state.detail or "")
        assert not route.called

    async def test_without_a_server_an_unknown_site_names_the_infraenv(self, cluster):
        cluster({"infraenvs": _image()})
        with pytest.raises(PxeSiteNotConfiguredError, match=f"{MCE}/{REQUEST.infra_env}"):
            await activities.publish_infra_env_boot(
                BOOT_REQUEST.model_copy(update={"server_name": None, "site": "haifa"})
            )

    @respx.mock
    async def test_a_missing_nmstateconfig_is_waited_on_not_published(self, cluster):
        cluster({"infraenvs": _image()})
        route = respx.put(url__startswith=PXE)
        state = await activities.publish_infra_env_boot(BOOT_REQUEST)
        assert state.published is False
        assert "NMStateConfig is not on the cluster" in (state.detail or "")
        assert not route.called


class TestTheImageWaitRule:
    def test_an_image_of_the_same_second_passes(self):
        # Both clocks are whole seconds; see the interface docstring for why
        # this window cannot hand the host an image without its config.
        assert infraenv_image_wait_reason(_image(created_at=NMSTATE_CREATED), _nmstate()) is None

    @pytest.mark.parametrize(
        "infra_env,expected",
        [
            (_image(created_at=None), "no status.createdTime"),
            (_image(ipxe=None), "no status.bootArtifacts.ipxeScript"),
            ({"status": {}}, "no ImageCreated condition"),
        ],
    )
    def test_each_missing_piece_is_named(self, infra_env, expected):
        assert expected in (infraenv_image_wait_reason(infra_env, _nmstate()) or "")

    @pytest.mark.parametrize("created", [None, "", "not-a-time", "2026-10-19T10:00:05"])
    def test_an_nmstateconfig_without_a_zoned_creation_time_never_passes(self, created):
        assert infraenv_image_wait_reason(_image(), _nmstate(created)) is not None


MAP_REF = PxeMapRef(
    server_name=REQUEST.server_name, site="bat-yam", macs=REQUEST.macs
)


class TestThePxeMapDelete:
    @respx.mock
    async def test_a_delete_addresses_the_mac_key_with_the_token(self):
        route = respx.delete(f"{PXE}/pxe-map/0025b5000001").mock(
            return_value=httpx.Response(204)
        )
        assert await delete_mapping(PXE, "secret", MAC) is True
        assert route.calls.last.request.headers["Authorization"] == "Bearer secret"

    @respx.mock
    async def test_an_entry_already_gone_is_success_not_an_error(self):
        respx.delete(url__startswith=PXE).mock(return_value=httpx.Response(404))
        assert await delete_mapping(PXE, "t", MAC) is False

    @pytest.mark.parametrize("status", [401, 403])
    @respx.mock
    async def test_a_rejected_token_is_permanent(self, status):
        respx.delete(url__startswith=PXE).mock(return_value=httpx.Response(status))
        with pytest.raises(PxeMapAuthError):
            await delete_mapping(PXE, "bad", MAC)

    @respx.mock
    async def test_a_server_error_is_retried(self):
        respx.delete(url__startswith=PXE).mock(return_value=httpx.Response(503))
        with pytest.raises(PxeMapError):
            await delete_mapping(PXE, "t", MAC)


class TestTheConditionalDelete:
    @respx.mock
    async def test_it_names_the_mce_and_a_204_means_cleared(self):
        route = respx.delete(f"{PXE}/pxe-map/0025b5000001").mock(return_value=httpx.Response(204))
        assert await delete_mapping_for_mce(PXE, "secret", MAC, MCE) is None
        sent = route.calls.last.request
        assert sent.url.params["mce_cluster"] == MCE
        assert sent.headers["Authorization"] == "Bearer secret"

    @respx.mock
    async def test_another_mces_entry_is_kept_with_the_reason(self):
        respx.delete(url__startswith=PXE).mock(
            return_value=httpx.Response(409, text='{"detail":"boot entry of MCE ocp4-mce-other"}')
        )
        assert "ocp4-mce-other" in await delete_mapping_for_mce(PXE, "t", MAC, MCE)

    @pytest.mark.parametrize("status,error", [(401, PxeMapAuthError), (503, PxeMapError)])
    @respx.mock
    async def test_other_failures_are_classified(self, status, error):
        respx.delete(url__startswith=PXE).mock(return_value=httpx.Response(status))
        with pytest.raises(error):
            await delete_mapping_for_mce(PXE, "t", MAC, MCE)


HOST_REF = PxeHostEntriesRef(
    server_name=REQUEST.server_name,
    mce_cluster=MCE,
    macs=["00:25:b5:00:00:01", "00:25:B5:00:00:01", "00:25:b5:00:00:02"],
)


class TestRemoveHostPxeEntries:
    @respx.mock
    async def test_every_configured_vm_and_every_mac_is_tried_once(self, monkeypatch):
        monkeypatch.setattr(
            activities._settings,
            "pxe_map_urls",
            {"bat-yam": PXE, "haifa": "http://pxe.haifa.test:8080"},
        )
        routes = respx.delete(path__regex=r"^/pxe-map/[0-9a-f]{12}$").mock(
            return_value=httpx.Response(204)
        )

        result = await activities.remove_host_pxe_entries(HOST_REF)

        # Deduped by key: the upper-case repeat is the same MAC.
        assert routes.call_count == 4
        assert result.cleared == [
            "bat-yam/0025b5000001", "bat-yam/0025b5000002",
            "haifa/0025b5000001", "haifa/0025b5000002",
        ]
        assert result.kept == []

    @respx.mock
    async def test_another_mces_entry_is_kept_not_raised(self):
        respx.delete(f"{PXE}/pxe-map/0025b5000001").mock(return_value=httpx.Response(204))
        respx.delete(f"{PXE}/pxe-map/0025b5000002").mock(
            return_value=httpx.Response(409, text="chains to ocp4-mce-other")
        )
        result = await activities.remove_host_pxe_entries(HOST_REF)
        assert result.cleared == ["bat-yam/0025b5000001"]
        assert result.kept == ["bat-yam/0025b5000002: chains to ocp4-mce-other"]

    @respx.mock
    async def test_with_a_site_only_that_sites_vm_is_called(self, monkeypatch):
        # The failure this fixes: the worker's one token belongs to its own
        # site's VM, so calling any other site's VM got a 401.
        monkeypatch.setattr(
            activities._settings,
            "pxe_map_urls",
            {"bat-yam": PXE, "haifa": "http://pxe.haifa.test:8080"},
        )
        own = respx.delete(url__startswith=PXE).mock(return_value=httpx.Response(204))
        other = respx.delete(url__startswith="http://pxe.haifa.test").mock(
            return_value=httpx.Response(401)
        )

        result = await activities.remove_host_pxe_entries(
            HOST_REF.model_copy(update={"site": "bat-yam"})
        )

        assert not other.called
        assert own.call_count == 2
        assert result.cleared == ["bat-yam/0025b5000001", "bat-yam/0025b5000002"]

    @respx.mock
    async def test_a_site_without_a_vm_is_reported_not_raised(self):
        route = respx.delete(url__startswith="http")
        result = await activities.remove_host_pxe_entries(
            HOST_REF.model_copy(update={"site": "haifa"})
        )
        assert result.cleared == [] and "haifa" in (result.detail or "")
        assert not route.called

    async def test_no_vm_configured_is_reported_not_raised(self, monkeypatch):
        monkeypatch.setattr(activities._settings, "pxe_map_urls", {})
        result = await activities.remove_host_pxe_entries(HOST_REF)
        assert result.cleared == [] and "PXE_MAP_URLS" in (result.detail or "")


class TestUnregisterPxeBoot:
    @respx.mock
    async def test_both_macs_are_deleted_and_a_missing_one_is_reported_absent(self):
        respx.delete(f"{PXE}/pxe-map/0025b5000001").mock(return_value=httpx.Response(200))
        respx.delete(f"{PXE}/pxe-map/0025b5000002").mock(return_value=httpx.Response(404))

        result = await activities.unregister_pxe_boot(MAP_REF)

        assert result.removed == ["00:25:b5:00:00:01"]
        assert result.absent == ["00:25:b5:00:00:02"]
        assert result.detail is None

    @respx.mock
    async def test_a_site_no_longer_configured_is_reported_not_raised(self):
        route = respx.delete(url__startswith="http")
        result = await activities.unregister_pxe_boot(
            MAP_REF.model_copy(update={"site": "haifa"})
        )
        assert result.removed == [] and "haifa" in (result.detail or "")
        assert not route.called


class TestCheckInfraEnv:
    REF = InfraEnvRef(name="cisco-m6-bat-yam-64c-512gb", namespace="inventory")

    async def test_an_existing_late_binding_infraenv_passes(self, infra_env):
        reads = infra_env({"spec": {}})
        state = await activities.check_infra_env(self.REF)
        assert (state.name, state.namespace, state.cluster_ref) == (
            self.REF.name, "inventory", None
        )
        assert (reads[0]["plural"], reads[0]["name"], reads[0]["namespace"]) == (
            "infraenvs", self.REF.name, "inventory"
        )

    async def test_an_early_bound_infraenv_reports_its_cluster(self, infra_env):
        infra_env({"spec": {"clusterRef": {"name": "ocp4-hc-01", "namespace": "x"}}})
        state = await activities.check_infra_env(self.REF)
        assert state.cluster_ref == "ocp4-hc-01"

    async def test_a_missing_infraenv_is_permanent_and_names_where_it_looked(self, infra_env):
        infra_env(None)
        with pytest.raises(InfraEnvNotFoundError, match="namespace inventory"):
            await activities.check_infra_env(self.REF)
