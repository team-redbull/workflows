"""provision-dell-server's pure rules, with no Temporal and no I/O.

Three of them: the site an iDRAC's address belongs to (resolved at the API
edge), the shape a correctly named server has (checked after the naming service
ran), and the storage plan — the one that must NEVER propose destroying data.
"""

from __future__ import annotations

import pytest

from shared.models.server_provisioning import (
    IdracController,
    IdracDrive,
    IdracVolume,
    BiosComparison,
    StorageLayout,
    TemplateAttribute,
)
from workflow_domains.server_provisioning.sites import resolve_site
from workflow_domains.server_provisioning.template_policy import (
    NETWORK,
    STORAGE,
    USERS,
    audit_template,
    describe_drift,
    describe_hazards,
)
from workflow_domains.server_provisioning.server_name import (
    MAX_NAME_LENGTH,
    expected_shape,
    name_matches_convention,
    name_problem,
)
from workflow_domains.server_provisioning.storage_plan import plan_storage


class TestSite:
    @pytest.mark.parametrize(
        ("ip", "site"),
        [("1.1.1.1", "site1"), ("2.2.2.2", "site2"), ("134.1.2.1", "israel")],
    )
    def test_the_prefix_names_the_site(self, ip, site):
        assert resolve_site(ip) == site

    def test_an_address_under_no_prefix_has_no_site(self):
        assert resolve_site("192.0.2.10") is None

    def test_a_non_address_is_refused(self):
        with pytest.raises(ValueError):
            resolve_site("not-an-ip")


class TestServerName:
    """The platform naming convention's bare-metal node: ocp-<flavor>-mtl-<site>-<serial>,
    lowercase, at most 32 characters; for a Dell the serial is its service tag."""

    NAME = "ocp-c200-mtl-israel-abc1234"

    def test_the_convention_matches(self):
        assert name_matches_convention(self.NAME, "israel", "ABC1234")
        assert name_problem(self.NAME, "israel", "ABC1234") is None

    @pytest.mark.parametrize("flavor", ["c200", "c200n", "c200d", "c200x", "r660", "d7"])
    def test_any_flavor_of_the_conventions_shape_matches(self, flavor):
        """The naming service chooses the flavor; only its shape is checked (the operator, 2026-11-22)."""
        assert name_matches_convention(f"ocp-{flavor}-mtl-israel-abc1234", "israel", "ABC1234")

    def test_the_service_tag_is_compared_lowercased(self):
        """The iDRAC reports the tag in upper case; the name carries it lowercased."""
        assert name_matches_convention(self.NAME, "israel", "abc1234")
        assert expected_shape("israel", "ABC1234") == "ocp-<flavor>-mtl-israel-abc1234"

    @pytest.mark.parametrize(
        ("site", "tag"),
        [("site1", "ABC1234"), ("israel", "ABC1235")],  # another site; another machine
    )
    def test_a_wrong_token_does_not_match(self, site, tag):
        assert not name_matches_convention(self.NAME, site, tag)

    def test_a_name_with_upper_case_is_refused_saying_so(self):
        assert "lowercase" in name_problem("ocp-c200-mtl-israel-ABC1234", "israel", "ABC1234")

    def test_a_name_over_32_characters_is_refused_saying_so(self):
        name = "ocp-c200n-mtl-longsitename-abc1234"
        assert len(name) > MAX_NAME_LENGTH == 32
        assert "32" in name_problem(name, "longsitename", "ABC1234")
        assert name_matches_convention("ocp-c200n-mtl-site-name-abc1234", "site-name", "ABC1234")  # 31

    @pytest.mark.parametrize(
        "name",
        [
            "ocp-dell-r660-israel-128c-1024gb-10tb-abc1234",  # the shape before 2026-11-22
            "ocp-c200-kv32-israel-abc1234",  # a VM's size, not bare metal
            "ocp-c200-israel-abc1234",  # no size
            "ocp-200c-mtl-israel-abc1234",  # a flavor starts with a letter
            "ocp-c200-mtl-israel-abc1234-x",
            "Profile from template 00001",  # OME's default, never renamed
        ],
    )
    def test_a_malformed_name_does_not_match(self, name):
        assert not name_matches_convention(name, "israel", "ABC1234")


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


class TestTemplateAudit:
    """The template audit — the one guard whose failure mode is a machine that
    can only be recovered at the rack."""

    @staticmethod
    def _attr(group: str, name: str, ignored: bool = False) -> TemplateAttribute:
        return TemplateAttribute(attribute_id=1, name=name, group=group, is_ignored=ignored)

    def test_a_bios_only_template_is_safe(self):
        safe = [
            self._attr("BIOS,System Profile Settings", "System Profile"),
            self._attr("BIOS,Boot Settings", "Boot Mode"),
            self._attr("iDRAC,Web Server", "Timeout"),
            self._attr("System,Server Operating System", "Server Host Name"),
        ]
        assert audit_template(safe) == []

    def test_the_idracs_own_address_is_refused(self):
        """The worst case: deploying a reference server's address moves this
        machine's iDRAC, and nothing remote can bring it back."""
        hazards = audit_template([self._attr("iDRAC,IPv4 Information", "Address")])
        assert [h.group for h in hazards] == [NETWORK]

    def test_a_host_nic_is_not_an_idrac_network_setting(self):
        """A template legitimately configures the host's NICs. Only the iDRAC's
        own network is the hazard, so the group has to mention iDRAC."""
        assert audit_template([self._attr("NIC,Integrated 1 Port 1", "MAC Address")]) == []

    def test_a_virtual_disk_component_is_refused(self):
        hazards = audit_template([self._attr("Storage,RAID.Integrated.1-1", "Virtual Disk 0")])
        assert [h.group for h in hazards] == [STORAGE]

    def test_a_user_account_is_refused(self):
        hazards = audit_template([self._attr("iDRAC,Users,User 2", "Password")])
        assert [h.group for h in hazards] == [USERS]

    def test_ordinary_bios_settings_that_merely_sound_dangerous_are_allowed(self):
        """The audit FAILS a run, so a false positive stops provisioning. The
        BIOS is full of words that look like hazards out of context, and every
        one of these is a normal thing for a template to carry."""
        innocent = [
            self._attr("BIOS,System Security", "System Password"),
            self._attr("BIOS,System Security", "Setup Password"),
            self._attr("BIOS,Boot Settings", "Hard-Disk Drive Sequence"),
            self._attr("BIOS,SATA Settings", "Embedded SATA"),
            self._attr("BIOS,Integrated Devices", "Embedded NIC1"),
            self._attr("BIOS,Network Settings", "IPv4 Support"),
        ]
        assert audit_template(innocent) == []

    def test_an_ignored_attribute_is_not_a_hazard(self):
        """IsIgnored is how an operator keeps a captured attribute without
        deploying it — refusing it would make the audit unsatisfiable from the
        OME GUI."""
        assert audit_template([self._attr("iDRAC,IPv4 Information", "Address", ignored=True)]) == []

    def test_each_attribute_is_reported_once_even_if_it_fits_two_groups(self):
        """'iDRAC,Users,User 2' + 'DNS RAC Name' matches both network and users;
        one attribute must not be counted twice in the failure message."""
        hazards = audit_template([self._attr("iDRAC,Users,User 2", "DNS RAC Name")])
        assert len(hazards) == 1

    def test_the_message_groups_by_reason_and_names_every_attribute(self):
        hazards = audit_template([
            self._attr("iDRAC,IPv4 Information", "Address"),
            self._attr("iDRAC,IPv6 Information", "Gateway"),
            self._attr("Storage,RAID.Integrated.1-1", "Virtual Disk 0"),
        ])
        described = describe_hazards(hazards)
        assert "idrac-network (2)" in described and "storage (1)" in described
        assert "iDRAC,IPv4 Information,Address" in described
        assert "reachable only at the rack" in described


class TestDriftReporting:
    def test_a_long_drift_list_is_capped_so_the_failure_stays_readable(self):
        """A template on the wrong firmware can fail dozens of attributes, and
        the message goes verbatim into Temporal history."""
        many = [
            BiosComparison(display_name=f"Attr {n}", attribute_name=f"A{n}",
                           intended="on", actual="off")
            for n in range(25)
        ]
        described = describe_drift(many)
        assert "and 15 more" in described
        assert described.count("template says") == 10
