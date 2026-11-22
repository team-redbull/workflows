"""provision-dell-server's pure rules, with no Temporal and no I/O.

Three of them: the region an iDRAC's address belongs to (resolved at the API
edge), the shape a correctly named server has (checked after the naming service
ran), and the storage plan — the one that must NEVER propose destroying data.
"""

from __future__ import annotations

import pytest

from shared.models.server_provisioning import (
    IdracController,
    IdracDrive,
    IdracVolume,
    StorageLayout,
)
from workflow_domains.server_provisioning.regions import resolve_region
from workflow_domains.server_provisioning.server_name import (
    model_token,
    name_matches_convention,
)
from workflow_domains.server_provisioning.storage_plan import plan_storage


class TestRegion:
    @pytest.mark.parametrize(
        ("ip", "region"),
        [("1.1.1.1", "region1"), ("2.2.2.2", "region2"), ("134.1.2.1", "israel")],
    )
    def test_the_prefix_names_the_region(self, ip, region):
        assert resolve_region(ip) == region

    def test_an_address_under_no_prefix_has_no_region(self):
        assert resolve_region("192.0.2.10") is None

    def test_a_non_address_is_refused(self):
        with pytest.raises(ValueError):
            resolve_region("not-an-ip")


class TestServerName:
    NAME = "ocp-dell-r660-israel-128c-1024gb-10tb-ABC1234"

    def test_model_token_is_the_lowercased_model_number(self):
        assert model_token("PowerEdge R660") == "r660"

    def test_the_convention_matches(self):
        assert name_matches_convention(self.NAME, "PowerEdge R660", "israel", "ABC1234")

    def test_service_tag_case_does_not_matter(self):
        assert name_matches_convention(self.NAME.lower(), "PowerEdge R660", "israel", "ABC1234")

    @pytest.mark.parametrize(
        ("model", "region", "tag"),
        [
            ("PowerEdge R650", "israel", "ABC1234"),  # another model
            ("PowerEdge R660", "region1", "ABC1234"),  # another region
            ("PowerEdge R660", "israel", "ABC1235"),  # another machine
        ],
    )
    def test_a_wrong_token_does_not_match(self, model, region, tag):
        assert not name_matches_convention(self.NAME, model, region, tag)

    @pytest.mark.parametrize(
        "name",
        [
            "ocp-dell-r660-israel-128c-1024gb-ABC1234",  # no disk token
            "Profile from template 00001",  # OME's default, never renamed
            "ocp-dell-r660-israel-128c-1024gb-10tb-ABC1234-x",
        ],
    )
    def test_a_malformed_name_does_not_match(self, name):
        assert not name_matches_convention(name, "PowerEdge R660", "israel", "ABC1234")


def _drive(controller: str, n: int, status: str | None) -> IdracDrive:
    return IdracDrive(
        odata_id=f"/redfish/v1/Systems/System.Embedded.1/Storage/{controller}/Drives/Disk.{n}",
        raid_status=status,
    )


BOSS_ID = "AHCI.SL.6-1"
PERC_ID = "RAID.SL.3-1"


def _boss(volumes: list[IdracVolume] | None = None, drives: int = 2) -> IdracController:
    return IdracController(
        odata_id=f"/redfish/v1/Systems/System.Embedded.1/Storage/{BOSS_ID}",
        id=BOSS_ID,
        name="BOSS-N1 Monolithic",
        drives=[_drive(BOSS_ID, n, None) for n in range(drives)],
        volumes=volumes or [],
    )


def _perc(statuses: list[str | None], volumes: list[IdracVolume] | None = None) -> IdracController:
    return IdracController(
        odata_id=f"/redfish/v1/Systems/System.Embedded.1/Storage/{PERC_ID}",
        id=PERC_ID,
        name="PERC H755 Front",
        drives=[_drive(PERC_ID, n, s) for n, s in enumerate(statuses)],
        volumes=volumes or [],
    )


def _mirror(boss: IdracController) -> IdracVolume:
    return IdracVolume(
        odata_id=f"{boss.odata_id}/Volumes/Disk.Virtual.0",
        raid_type="RAID1",
        drives=[d.odata_id for d in boss.drives],
    )


class TestStoragePlan:
    def test_a_new_server_gets_the_mirror_and_every_ready_drive_converted(self):
        plan = plan_storage(StorageLayout(controllers=[_boss(), _perc(["Ready", "Ready", "NonRAID"])]))
        assert plan.problems == []
        assert plan.create_boss_raid1
        assert plan.boss_controller == BOSS_ID
        assert len(plan.boss_drives) == 2
        assert [d.rsplit("/", 1)[-1] for d in plan.non_raid_drives] == ["Disk.0", "Disk.1"]
        assert not plan.converged

    def test_a_provisioned_server_is_converged(self):
        boss = _boss()
        boss = boss.model_copy(update={"volumes": [_mirror(boss)]})
        plan = plan_storage(StorageLayout(controllers=[boss, _perc(["NonRAID", "NonRAID"])]))
        assert plan.converged

    def test_other_controllers_are_left_alone(self):
        nvme = IdracController(
            odata_id="/redfish/v1/Systems/System.Embedded.1/Storage/CPU.1",
            id="CPU.1",
            name="CPU.1",
            drives=[_drive("CPU.1", 0, None)],
        )
        plan = plan_storage(StorageLayout(controllers=[_boss(), nvme]))
        assert plan.problems == []
        assert plan.non_raid_drives == []

    def test_no_boss_is_a_problem(self):
        plan = plan_storage(StorageLayout(controllers=[_perc(["Ready"])]))
        assert plan.problems and "BOSS" in plan.problems[0]
        assert not plan.create_boss_raid1 and plan.non_raid_drives == []

    def test_a_boss_with_one_drive_is_a_problem(self):
        plan = plan_storage(StorageLayout(controllers=[_boss(drives=1)]))
        assert any("1 drive" in p for p in plan.problems)

    def test_a_foreign_boss_volume_is_never_replaced(self):
        boss = _boss()
        stray = IdracVolume(
            odata_id=f"{boss.odata_id}/Volumes/Disk.Virtual.0",
            raid_type="RAID0",
            drives=[boss.drives[0].odata_id],
        )
        plan = plan_storage(StorageLayout(controllers=[boss.model_copy(update={"volumes": [stray]})]))
        assert plan.problems
        assert not plan.create_boss_raid1

    def test_a_perc_volume_is_never_destroyed(self):
        vd = IdracVolume(
            odata_id=f"/redfish/v1/Systems/System.Embedded.1/Storage/{PERC_ID}/Volumes/Disk.Virtual.0",
            raid_type="RAID5",
        )
        plan = plan_storage(StorageLayout(controllers=[_boss(), _perc(["Online", "Online"], [vd])]))
        assert plan.problems
        # Nothing at all is staged while any problem stands.
        assert not plan.create_boss_raid1 and plan.non_raid_drives == []

    @pytest.mark.parametrize("status", ["Online", "Foreign", "Failed", None])
    def test_a_drive_neither_ready_nor_non_raid_is_a_problem(self, status):
        plan = plan_storage(StorageLayout(controllers=[_boss(), _perc([status])]))
        assert plan.problems
