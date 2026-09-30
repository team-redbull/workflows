"""The server-provisioning limb's technology layers, tested with respx and no Temporal.

Mostly CLASSIFICATION and IDEMPOTENCY. The workflow's retry policy is unbounded,
so an error sorted as transient when it is permanent retries forever, and an
activity that is not idempotent does its mutation twice on a retry — for the
iDRAC that means a second pending job refused, or a reboot in the middle of an
apply. Plus the one iDRAC-specific rule: the probe must never send more failed
logins than it has to.
"""

from __future__ import annotations

import json

import httpx
import pytest
import respx
from pydantic import ValidationError

from activities.server_provisioning import idrac, ome, server_namer, server_scan
from shared.exceptions import (
    IdracAuthError,
    IdracError,
    IdracRequestRejectedError,
    OmeAuthError,
    OmeRequestRejectedError,
    ProfileConflictError,
    ServerNamerError,
    ServerNamerRejectedError,
    ServerScanAuthError,
    TemplateNotFoundError,
)
from shared.models.server_provisioning import ServerNameRequest, ServerScanLookup
from shared.settings import ServerProvisioningActivitySettings

IP = "10.1.2.3"
BASE = f"https://{IP}"
SYSTEM = f"{BASE}{idrac.SYSTEM}"
MANAGER = f"{BASE}{idrac.MANAGER}"
JOBS = f"{BASE}{idrac.JOBS}"


class TestProbe:
    @respx.mock
    async def test_first_accepting_password_wins_and_stops_the_probe(self):
        route = respx.get(SYSTEM).mock(
            side_effect=[httpx.Response(401), httpx.Response(200, json={})]
        )
        result = await idrac.probe_credentials(IP, "root", ["target", "calvin", "other"])
        assert result.credential == 1
        assert result.rejected == 1
        # The third password is never tried: every extra login risks the block.
        assert route.call_count == 2

    @respx.mock
    async def test_every_rejection_is_reported_not_raised(self):
        respx.get(SYSTEM).mock(return_value=httpx.Response(401))
        result = await idrac.probe_credentials(IP, "root", ["a", "b", "c"])
        assert result.reachable and result.credential is None and result.rejected == 3

    @respx.mock
    async def test_an_unreachable_idrac_ends_the_round_at_once(self):
        route = respx.get(SYSTEM).mock(side_effect=httpx.ConnectError("refused"))
        result = await idrac.probe_credentials(IP, "root", ["a", "b"])
        assert not result.reachable and result.credential is None
        assert route.call_count == 1

    @respx.mock
    async def test_a_5xx_ends_the_round_without_counting_as_a_rejection(self):
        respx.get(SYSTEM).mock(side_effect=[httpx.Response(401), httpx.Response(503)])
        result = await idrac.probe_credentials(IP, "root", ["a", "b", "c"])
        assert not result.reachable and result.rejected == 1 and "503" in (result.detail or "")


class TestIdentity:
    @respx.mock
    async def test_reads_service_tag_model_and_firmware(self):
        respx.get(SYSTEM).mock(
            return_value=httpx.Response(
                200,
                json={"SKU": "ABC1234", "Model": "PowerEdge R660", "Manufacturer": "Dell Inc.",
                      "BiosVersion": "1.6.6", "PowerState": "On"},
            )
        )
        respx.get(MANAGER).mock(return_value=httpx.Response(200, json={"FirmwareVersion": "7.10.70.00"}))
        identity = await idrac.read_identity(IP, "root", "pw")
        assert (identity.service_tag, identity.model, identity.idrac_firmware) == (
            "ABC1234", "PowerEdge R660", "7.10.70.00"
        )

    @respx.mock
    async def test_a_401_after_the_probe_is_permanent(self):
        respx.get(SYSTEM).mock(return_value=httpx.Response(401))
        with pytest.raises(IdracAuthError):
            await idrac.read_identity(IP, "root", "pw")

    @respx.mock
    async def test_a_5xx_is_transient(self):
        respx.get(SYSTEM).mock(return_value=httpx.Response(500))
        with pytest.raises(IdracError):
            await idrac.read_identity(IP, "root", "pw")


BOSS = "AHCI.SL.6-1"
PERC = "RAID.SL.3-1"
BOSS_DRIVES = [f"{idrac.SYSTEM}/Storage/{BOSS}/Drives/Disk.Direct.{n}:{BOSS}" for n in (0, 1)]
PERC_DRIVES = [f"{idrac.SYSTEM}/Storage/{PERC}/Drives/Disk.Bay.{n}:Enclosure.Internal.0-1:{PERC}" for n in (0, 1)]


class TestStorageLayout:
    @respx.mock
    async def test_reads_controllers_drives_status_and_volumes(self):
        storage = f"{idrac.SYSTEM}/Storage"
        respx.get(f"{BASE}{storage}").mock(
            return_value=httpx.Response(200, json={"Members": [{"@odata.id": f"{storage}/{BOSS}"}, {"@odata.id": f"{storage}/{PERC}"}]})
        )
        respx.get(f"{BASE}{storage}/{BOSS}").mock(
            return_value=httpx.Response(200, json={
                "Id": BOSS, "Name": "BOSS-N1 Monolithic",
                "Drives": [{"@odata.id": d} for d in BOSS_DRIVES],
                "Volumes": {"@odata.id": f"{storage}/{BOSS}/Volumes"},
            })
        )
        respx.get(f"{BASE}{storage}/{BOSS}/Volumes").mock(return_value=httpx.Response(200, json={"Members": []}))
        respx.get(f"{BASE}{storage}/{PERC}").mock(
            return_value=httpx.Response(200, json={
                "Id": PERC, "Name": "PERC H755 Front",
                "Drives": [{"@odata.id": d} for d in PERC_DRIVES],
                "Volumes": {"@odata.id": f"{storage}/{PERC}/Volumes"},
            })
        )
        respx.get(f"{BASE}{storage}/{PERC}/Volumes").mock(return_value=httpx.Response(404))
        for drive in BOSS_DRIVES + PERC_DRIVES:
            respx.get(f"{BASE}{drive}").mock(
                return_value=httpx.Response(200, json={"Oem": {"Dell": {"DellPhysicalDisk": {"RaidStatus": "Ready"}}}})
            )
        layout = await idrac.read_storage(IP, "root", "pw")
        assert [c.id for c in layout.controllers] == [BOSS, PERC]
        assert layout.controllers[0].name == "BOSS-N1 Monolithic"
        assert all(d.raid_status == "Ready" for c in layout.controllers for d in c.drives)
        assert layout.controllers[1].volumes == []


def _jobs(*members: dict) -> httpx.Response:
    return httpx.Response(200, json={"Members": list(members)})


class TestStageStorage:
    @respx.mock
    async def test_stages_the_mirror_and_the_conversion_as_two_jobs(self):
        respx.get(url__startswith=JOBS).mock(return_value=_jobs())
        volume = respx.post(f"{BASE}{idrac.SYSTEM}/Storage/{BOSS}/Volumes").mock(
            return_value=httpx.Response(202, headers={"Location": f"{idrac.JOBS}/JID_1"})
        )
        convert = respx.post(url__startswith=f"{BASE}{idrac.SYSTEM}/Oem/Dell").mock(
            return_value=httpx.Response(202, headers={"Location": f"{idrac.JOBS}/JID_2"})
        )
        job_ids = await idrac.stage_storage(IP, "root", "pw", BOSS, BOSS_DRIVES, PERC_DRIVES)
        assert job_ids == ["JID_1", "JID_2"]
        body = json.loads(volume.calls.last.request.content)
        assert body["RAIDType"] == "RAID1" and body["@Redfish.OperationApplyTime"] == "OnReset"
        assert len(body["Drives"]) == 2
        assert json.loads(convert.calls.last.request.content) == {
            "PDArray": [d.rsplit("/", 1)[-1] for d in PERC_DRIVES]
        }

    @respx.mock
    async def test_a_retry_reuses_the_pending_jobs_instead_of_staging_twice(self):
        respx.get(url__startswith=JOBS).mock(
            return_value=_jobs(
                {"Id": "JID_1", "Name": f"Configure: {BOSS}", "JobState": "Scheduled"},
                {"Id": "JID_2", "Name": f"Configure: {PERC}", "JobState": "Scheduled"},
                {"Id": "JID_0", "Name": f"Configure: {PERC}", "JobState": "Completed"},
            )
        )
        posts = respx.post(url__startswith=BASE).mock(return_value=httpx.Response(500))
        job_ids = await idrac.stage_storage(IP, "root", "pw", BOSS, BOSS_DRIVES, PERC_DRIVES)
        assert job_ids == ["JID_1", "JID_2"]
        assert posts.call_count == 0

    @respx.mock
    async def test_a_refused_request_is_permanent_and_carries_the_idrac_message(self):
        respx.get(url__startswith=JOBS).mock(return_value=_jobs())
        respx.post(f"{BASE}{idrac.SYSTEM}/Storage/{BOSS}/Volumes").mock(
            return_value=httpx.Response(
                400, json={"error": {"@Message.ExtendedInfo": [{"Message": "RAID level not supported"}]}}
            )
        )
        with pytest.raises(IdracRequestRejectedError, match="RAID level not supported"):
            await idrac.stage_storage(IP, "root", "pw", BOSS, BOSS_DRIVES, [])

    @respx.mock
    async def test_an_accepted_request_without_a_job_is_refused(self):
        respx.get(url__startswith=JOBS).mock(return_value=_jobs())
        respx.post(f"{BASE}{idrac.SYSTEM}/Storage/{BOSS}/Volumes").mock(return_value=httpx.Response(202))
        with pytest.raises(IdracRequestRejectedError, match="no job"):
            await idrac.stage_storage(IP, "root", "pw", BOSS, BOSS_DRIVES, [])


class TestApplyStaged:
    @respx.mock
    async def test_resets_while_a_job_waits_for_it(self):
        respx.get(f"{JOBS}/JID_1").mock(return_value=httpx.Response(200, json={"JobState": "Scheduled"}))
        respx.get(SYSTEM).mock(return_value=httpx.Response(200, json={"PowerState": "On"}))
        reset = respx.post(f"{BASE}{idrac.SYSTEM}/Actions/ComputerSystem.Reset").mock(
            return_value=httpx.Response(204)
        )
        result = await idrac.apply_staged(IP, "root", "pw", ["JID_1"])
        assert result.rebooted and result.reset_type == "ForceRestart"
        assert json.loads(reset.calls.last.request.content) == {"ResetType": "ForceRestart"}

    @respx.mock
    async def test_a_powered_off_machine_is_powered_on(self):
        respx.get(f"{JOBS}/JID_1").mock(return_value=httpx.Response(200, json={"JobState": "Scheduled"}))
        respx.get(SYSTEM).mock(return_value=httpx.Response(200, json={"PowerState": "Off"}))
        respx.post(f"{BASE}{idrac.SYSTEM}/Actions/ComputerSystem.Reset").mock(return_value=httpx.Response(204))
        assert (await idrac.apply_staged(IP, "root", "pw", ["JID_1"])).reset_type == "On"

    @respx.mock
    async def test_never_resets_once_the_jobs_are_running(self):
        respx.get(f"{JOBS}/JID_1").mock(return_value=httpx.Response(200, json={"JobState": "Running"}))
        reset = respx.post(url__startswith=BASE).mock(return_value=httpx.Response(204))
        result = await idrac.apply_staged(IP, "root", "pw", ["JID_1"])
        assert not result.rebooted
        assert reset.call_count == 0


OME = "https://ome.test"
API = f"{OME}/api"


def _login() -> None:
    respx.post(f"{API}/SessionService/Sessions").mock(
        return_value=httpx.Response(201, headers={"X-Auth-Token": "tok"}, json={"Id": "s1"})
    )
    respx.delete(url__startswith=f"{API}/SessionService/Sessions(").mock(return_value=httpx.Response(204))


class TestOme:
    @respx.mock
    async def test_bad_ome_credentials_are_permanent(self):
        respx.post(f"{API}/SessionService/Sessions").mock(return_value=httpx.Response(401))
        with pytest.raises(OmeAuthError):
            async with ome.session(OME, "u", "p"):
                pass

    @respx.mock
    async def test_find_device_compares_the_service_tag_exactly(self):
        _login()
        respx.get(url__startswith=f"{API}/DeviceService/Devices").mock(
            return_value=httpx.Response(200, json={"value": [
                {"Id": 1, "DeviceServiceTag": "ABC12345"},
                {"Id": 2, "DeviceServiceTag": "abc1234", "DeviceName": "idrac-x"},
            ]})
        )
        async with ome.session(OME, "u", "p") as client:
            device = await ome.find_device(client, "ABC1234")
        assert device.found and device.device_id == 2

    @respx.mock
    async def test_discovery_reuses_a_group_of_the_same_name(self):
        _login()
        respx.get(f"{API}/DiscoveryConfigService/DiscoveryConfigGroups").mock(
            return_value=httpx.Response(200, json={"value": [
                {"DiscoveryConfigGroupName": "provision-ABC1234-discover-abcd",
                 "DiscoveryConfigTaskParam": [{"TaskId": 77}]},
            ]})
        )
        create = respx.post(f"{API}/DiscoveryConfigService/DiscoveryConfigGroups").mock(
            return_value=httpx.Response(500)
        )
        async with ome.session(OME, "u", "p") as client:
            job = await ome.start_discovery(client, "provision-ABC1234-discover-abcd", IP, "root", "pw")
        assert job == 77 and create.call_count == 0

    @respx.mock
    async def test_discovery_sends_the_idrac_and_its_root_credential(self):
        _login()
        respx.get(f"{API}/DiscoveryConfigService/DiscoveryConfigGroups").mock(
            return_value=httpx.Response(200, json={"value": []})
        )
        create = respx.post(f"{API}/DiscoveryConfigService/DiscoveryConfigGroups").mock(
            return_value=httpx.Response(201, json={"DiscoveryConfigTaskParam": [{"TaskId": 78}]})
        )
        async with ome.session(OME, "u", "p") as client:
            assert await ome.start_discovery(client, "g", IP, "root", "pw") == 78
        body = json.loads(create.calls.last.request.content)
        model = body["DiscoveryConfigModels"][0]
        assert model["DiscoveryConfigTargets"] == [{"NetworkAddressDetail": IP}]
        credentials = json.loads(model["ConnectionProfile"])["credentials"][0]["credentials"]
        assert (credentials["username"], credentials["password"]) == ("root", "pw")

    @pytest.mark.parametrize(
        ("status_id", "finished", "succeeded"),
        [(2050, False, False), (2060, True, True), (2070, True, False), (2090, True, False)],
    )
    @respx.mock
    async def test_job_status_is_interpreted_on_the_limb(self, status_id, finished, succeeded):
        _login()
        respx.get(f"{API}/JobService/Jobs(5)").mock(
            return_value=httpx.Response(200, json={"LastRunStatus": {"Id": status_id, "Name": "x"}})
        )
        async with ome.session(OME, "u", "p") as client:
            state = await ome.get_job(client, 5)
        assert (state.finished, state.succeeded) == (finished, succeeded)

    @respx.mock
    async def test_a_missing_template_is_permanent(self):
        _login()
        respx.get(url__startswith=f"{API}/TemplateService/Templates").mock(
            return_value=httpx.Response(200, json={"value": []})
        )
        async with ome.session(OME, "u", "p") as client:
            with pytest.raises(TemplateNotFoundError):
                await ome.find_template_id(client, "ocp-r660")

    @respx.mock
    async def test_deploy_is_skipped_when_the_template_is_already_on_the_device(self):
        _login()
        respx.get(url__startswith=f"{API}/ProfileService/Profiles").mock(
            return_value=httpx.Response(200, json={"value": [
                {"Id": 9, "TargetId": 2, "ProfileName": "p", "TemplateName": "ocp-r660"}
            ]})
        )
        deploy = respx.post(url__startswith=f"{API}/TemplateService/Actions").mock(return_value=httpx.Response(500))
        async with ome.session(OME, "u", "p") as client:
            assert await ome.deploy_template(client, 3, "ocp-r660", 2) is None
        assert deploy.call_count == 0

    @respx.mock
    async def test_a_profile_from_another_template_is_never_overwritten(self):
        _login()
        respx.get(url__startswith=f"{API}/ProfileService/Profiles").mock(
            return_value=httpx.Response(200, json={"value": [
                {"Id": 9, "TargetId": 2, "ProfileName": "p", "TemplateName": "hand-made"}
            ]})
        )
        async with ome.session(OME, "u", "p") as client:
            with pytest.raises(ProfileConflictError):
                await ome.deploy_template(client, 3, "ocp-r660", 2)

    @respx.mock
    async def test_deploy_returns_the_job_ome_answers_with(self):
        _login()
        respx.get(url__startswith=f"{API}/ProfileService/Profiles").mock(
            return_value=httpx.Response(200, json={"value": []})
        )
        deploy = respx.post(f"{API}/TemplateService/Actions/TemplateService.Deploy").mock(
            return_value=httpx.Response(200, json=4242)
        )
        async with ome.session(OME, "u", "p") as client:
            assert await ome.deploy_template(client, 3, "ocp-r660", 2) == 4242
        body = json.loads(deploy.calls.last.request.content)
        assert body["Id"] == 3 and body["TargetIds"] == [2]

    @respx.mock
    async def test_a_refused_request_is_permanent(self):
        _login()
        respx.get(url__startswith=f"{API}/ProfileService/Profiles").mock(
            return_value=httpx.Response(200, json={"value": []})
        )
        respx.post(f"{API}/TemplateService/Actions/TemplateService.Deploy").mock(
            return_value=httpx.Response(400, json={"error": {"message": "bad target"}})
        )
        async with ome.session(OME, "u", "p") as client:
            with pytest.raises(OmeRequestRejectedError, match="bad target"):
                await ome.deploy_template(client, 3, "ocp-r660", 2)


SS = "http://server-scan.test/api/v1"


def _server(server_id: str, name: str, serial: str, state: str = "AVAILABLE", cluster: str | None = None) -> dict:
    return {
        "id": server_id,
        "name": name,
        "identity": {"serial": serial},
        "health": {"overall": "HEALTHY"},
        "reachable": True,
        "openshift": {"lifecycle_state": state, "cluster_name": cluster},
    }


class TestServerScan:
    NAME = "ocp-dell-r660-israel-128c-1024gb-10tb-ABC1234"

    def _mock(self, *servers: dict) -> None:
        respx.get(f"{SS}/servers").mock(
            return_value=httpx.Response(200, json={"items": [{"id": s["id"]} for s in servers]})
        )
        for s in servers:
            respx.get(f"{SS}/servers/{s['id']}").mock(return_value=httpx.Response(200, json=s))

    @respx.mock
    async def test_found_under_the_expected_name(self):
        self._mock(_server("s1", self.NAME, "ABC1234"))
        state = await server_scan.lookup(SS, "", ServerScanLookup(service_tag="ABC1234", expected_name=self.NAME))
        assert state.found and state.server_id == "s1" and state.health == "HEALTHY"

    @respx.mock
    async def test_a_prefix_hit_with_another_serial_is_not_this_machine(self):
        self._mock(_server("s1", self.NAME, "ABC12345"))
        state = await server_scan.lookup(SS, "", ServerScanLookup(service_tag="ABC1234", expected_name=self.NAME))
        assert not state.found and state.server_id is None

    @respx.mock
    async def test_the_old_name_is_not_found_yet(self):
        self._mock(_server("s1", "Profile from template 00001", "ABC1234"))
        state = await server_scan.lookup(SS, "", ServerScanLookup(service_tag="ABC1234", expected_name=self.NAME))
        assert not state.found and state.name == "Profile from template 00001"

    @respx.mock
    async def test_a_server_in_use_is_reported_as_claimed(self):
        self._mock(_server("s1", self.NAME, "ABC1234", "INSTALLED", "ocp4-prod"))
        state = await server_scan.lookup(SS, "", ServerScanLookup(service_tag="ABC1234"))
        assert state.claimed_by == "INSTALLED ocp4-prod"

    @respx.mock
    async def test_bad_token_is_permanent(self):
        respx.get(f"{SS}/servers").mock(return_value=httpx.Response(403))
        with pytest.raises(ServerScanAuthError):
            await server_scan.lookup(SS, "t", ServerScanLookup(service_tag="ABC1234"))


NAMER = "https://namer.test/rename"
NAME_REQUEST = ServerNameRequest(service_tag="ABC1234", ome_device_id=2, region="israel", idrac_ip=IP)


class TestNamer:
    @respx.mock
    async def test_sends_what_the_workflow_knows(self):
        route = respx.post(NAMER).mock(return_value=httpx.Response(200))
        await server_namer.request_name(NAMER, "tok", NAME_REQUEST)
        assert json.loads(route.calls.last.request.content)["service_tag"] == "ABC1234"
        assert route.calls.last.request.headers["Authorization"] == "Bearer tok"

    @respx.mock
    async def test_4xx_is_permanent_5xx_is_retried(self):
        respx.post(NAMER).mock(side_effect=[httpx.Response(422), httpx.Response(503)])
        with pytest.raises(ServerNamerRejectedError):
            await server_namer.request_name(NAMER, "", NAME_REQUEST)
        with pytest.raises(ServerNamerError):
            await server_namer.request_name(NAMER, "", NAME_REQUEST)


class TestSettings:
    def test_candidates_are_the_target_then_the_factory_passwords(self):
        settings = ServerProvisioningActivitySettings()
        assert settings.idrac_root_passwords == ["target-pass", "factory-a", "factory-b"]

    def test_the_target_is_not_tried_twice(self, monkeypatch):
        monkeypatch.setenv("IDRAC_FACTORY_PASSWORDS", '["target-pass", "calvin"]')
        assert ServerProvisioningActivitySettings().idrac_root_passwords == ["target-pass", "calvin"]

    def test_a_third_factory_password_is_refused(self, monkeypatch):
        monkeypatch.setenv("IDRAC_FACTORY_PASSWORDS", '["a", "b", "c"]')
        with pytest.raises(ValidationError, match="at most 2"):
            ServerProvisioningActivitySettings()

    def test_ome_must_be_https(self, monkeypatch):
        monkeypatch.setenv("OME_URL", "http://ome.test")
        with pytest.raises(ValidationError, match="https"):
            ServerProvisioningActivitySettings()

    def test_templates_must_name_something(self, monkeypatch):
        monkeypatch.setenv("DELL_TEMPLATES", '{"PowerEdge R660": {}}')
        with pytest.raises(ValidationError):
            ServerProvisioningActivitySettings()
