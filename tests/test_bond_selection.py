"""Unit tests for the two pure pieces install-server depends on most:
bond-member selection and BMC address construction.

Both are pure functions, so they need no Temporal harness — and both encode a
finding that is easy to regress into, which is why they are tested apart from
the workflow rather than only through it.
"""

from __future__ import annotations

import pytest

from shared.bmc_address import bmc_secret_name, build_bmc_address
from shared.workflow_ids import install_server_workflow_id
from shared.models.server_lifecycle import (
    AcquiredServer,
    BmcEndpoint,
    LinkState,
    ServerInterface,
)
from workflow_domains.server_lifecycle.bond_selection import select_bond_members

IP_BMC = BmcEndpoint(host="10.11.1.229", host_is_ip=True, scheme="redfish")


def _server(interfaces: list[ServerInterface], **overrides) -> AcquiredServer:
    return AcquiredServer(
        id="srv_test",
        name="ocp-dell-r650-tlv-64c-1024gb-DEL0000485",
        vendor=overrides.pop("vendor", "dell"),
        source_provider=overrides.pop("source_provider", "OPENMANAGE"),
        bmc_vendor=overrides.pop("bmc_vendor", "DELL"),
        bmc=IP_BMC,
        interfaces=interfaces,
        health_overall="HEALTHY",
        **overrides,
    )


def _nic(name: str, mac: str, location: str | None, link: LinkState) -> ServerInterface:
    return ServerInterface(name=name, mac=mac, location=location, link_state=link)


class TestPhysicalPortGrouping:
    """Two NPAR partitions of one port are one wire — bonding them is not a bond."""

    def test_partitions_of_one_port_collapse_to_one_member(self) -> None:
        # A Dell FQDD location is controller/port/partition: 1/1/1 and 1/1/2
        # are two partitions of ONE physical port, each with its own MAC.
        server = _server(
            [
                _nic("NIC.Integrated.1-1-1", "aa:bb:cc:dd:ee:01", "1/1/1", LinkState.UP),
                _nic("NIC.Integrated.1-1-2", "aa:bb:cc:dd:ee:02", "1/1/2", LinkState.UP),
            ]
        )
        assert select_bond_members(server) is None

    def test_two_real_ports_bond(self) -> None:
        server = _server(
            [
                _nic("NIC.Integrated.1-1-1", "aa:bb:cc:dd:ee:01", "1/1/1", LinkState.UP),
                _nic("NIC.Integrated.1-2-1", "aa:bb:cc:dd:ee:02", "1/2/1", LinkState.UP),
            ]
        )
        members = select_bond_members(server)
        assert members is not None
        assert [m.logical_name for m in members] == ["nic1", "nic2"]
        assert [m.mac for m in members] == ["aa:bb:cc:dd:ee:01", "aa:bb:cc:dd:ee:02"]
        assert [m.port_key for m in members] == ["1/1", "1/2"]

    def test_partitions_are_skipped_but_a_later_port_still_qualifies(self) -> None:
        # The second partition must not consume the slot a genuinely distinct
        # port would have filled.
        server = _server(
            [
                _nic("NIC.Slot.2-1-1", "aa:bb:cc:dd:ee:01", "2/1/1", LinkState.UP),
                _nic("NIC.Slot.2-1-2", "aa:bb:cc:dd:ee:02", "2/1/2", LinkState.UP),
                _nic("NIC.Slot.2-2-1", "aa:bb:cc:dd:ee:03", "2/2/1", LinkState.UP),
            ]
        )
        members = select_bond_members(server)
        assert members is not None
        assert [m.mac for m in members] == ["aa:bb:cc:dd:ee:01", "aa:bb:cc:dd:ee:03"]

    def test_absent_location_falls_back_to_the_interface_name(self) -> None:
        # Cisco and HPE never set `location`, so the name is the finest
        # distinction available — two named interfaces are two ports.
        server = _server(
            [
                _nic("Slot 1 port 1", "aa:bb:cc:dd:ee:01", None, LinkState.UP),
                _nic("Slot 1 port 2", "aa:bb:cc:dd:ee:02", None, LinkState.UP),
            ],
            vendor="hp",
            source_provider="ONEVIEW",
            bmc_vendor="HP",
        )
        members = select_bond_members(server)
        assert members is not None
        assert [m.port_key for m in members] == ["Slot 1 port 1", "Slot 1 port 2"]

    def test_a_non_dell_location_is_not_parsed_as_a_port_triple(self) -> None:
        # A standalone BMC's Id is whatever that vendor chose; it must not be
        # mistaken for controller/port/partition.
        server = _server(
            [
                _nic("eth0", "aa:bb:cc:dd:ee:01", "NIC.Embedded.1", LinkState.UP),
                _nic("eth1", "aa:bb:cc:dd:ee:02", "NIC.Embedded.2", LinkState.UP),
            ]
        )
        members = select_bond_members(server)
        assert members is not None
        assert [m.port_key for m in members] == ["eth0", "eth1"]


class TestStrictLinkUp:
    """UP is required, never merely 'not DOWN' — the accepted HPE/Intersight gap."""

    @pytest.mark.parametrize(
        "link", [LinkState.UNKNOWN, LinkState.DOWN, LinkState.DISABLED]
    )
    def test_a_non_up_port_is_never_bonded(self, link: LinkState) -> None:
        server = _server(
            [
                _nic("NIC.Integrated.1-1-1", "aa:bb:cc:dd:ee:01", "1/1/1", LinkState.UP),
                _nic("NIC.Integrated.1-2-1", "aa:bb:cc:dd:ee:02", "1/2/1", link),
            ]
        )
        assert select_bond_members(server) is None

    def test_a_oneview_server_never_qualifies(self) -> None:
        # OneView's portMap carries no link state at all, so server-scan stores
        # UNKNOWN unconditionally. This is the documented, accepted gap: such a
        # server fails selection rather than being silently passed over.
        server = _server(
            [
                _nic("Slot 1 port 1", "aa:bb:cc:dd:ee:01", None, LinkState.UNKNOWN),
                _nic("Slot 1 port 2", "aa:bb:cc:dd:ee:02", None, LinkState.UNKNOWN),
            ],
            vendor="hp",
            source_provider="ONEVIEW",
            bmc_vendor="HP",
        )
        assert select_bond_members(server) is None

    def test_a_down_port_is_stepped_over_for_a_later_up_one(self) -> None:
        server = _server(
            [
                _nic("NIC.Slot.2-1-1", "aa:bb:cc:dd:ee:01", "2/1/1", LinkState.UP),
                _nic("NIC.Slot.2-2-1", "aa:bb:cc:dd:ee:02", "2/2/1", LinkState.DOWN),
                _nic("NIC.Slot.2-3-1", "aa:bb:cc:dd:ee:03", "2/3/1", LinkState.UP),
            ]
        )
        members = select_bond_members(server)
        assert members is not None
        assert [m.mac for m in members] == ["aa:bb:cc:dd:ee:01", "aa:bb:cc:dd:ee:03"]


class TestDegenerateInterfaceSets:
    def test_no_interfaces_at_all(self) -> None:
        # A server whose NIC read failed carries unread_fields and no
        # interfaces — yet still reads HEALTHY overall, because an unread
        # category ranks below HEALTHY and never drags the rollup down.
        assert select_bond_members(_server([])) is None

    def test_a_single_up_port_is_not_a_bond(self) -> None:
        server = _server(
            [_nic("NIC.Integrated.1-1-1", "aa:bb:cc:dd:ee:01", "1/1/1", LinkState.UP)]
        )
        assert select_bond_members(server) is None

    def test_an_interface_with_no_mac_is_skipped(self) -> None:
        server = _server(
            [
                ServerInterface(
                    name="NIC.Integrated.1-1-1", mac=None, location="1/1/1",
                    link_state=LinkState.UP,
                ),
                _nic("NIC.Integrated.1-2-1", "aa:bb:cc:dd:ee:02", "1/2/1", LinkState.UP),
            ]
        )
        assert select_bond_members(server) is None


class TestBmcAddress:
    """The address is carried from what server-scan parsed, not rebuilt from a template."""

    def test_dell_uses_the_idrac_driver_and_the_reported_path(self) -> None:
        bmc = BmcEndpoint(
            host="10.11.1.229",
            host_is_ip=True,
            scheme="redfish",
            path="/redfish/v1/Systems/System.Embedded.1",
        )
        assert build_bmc_address("DELL", bmc) == (
            "idrac-virtualmedia://10.11.1.229/redfish/v1/Systems/System.Embedded.1"
        )

    def test_cisco_ucs_is_ipmi_with_its_port(self) -> None:
        bmc = BmcEndpoint(host="10.13.7.199", host_is_ip=True, scheme="ipmi", port=623)
        assert build_bmc_address("CISCO", bmc) == "ipmi://10.13.7.199:623"

    def test_intersight_and_cisco_take_different_drivers(self) -> None:
        # Both are Cisco hardware; how they are MANAGED decides the driver,
        # which is why server-scan reports INTERSIGHT as a fourth bmc_vendor.
        bmc = BmcEndpoint(host="10.10.3.132", host_is_ip=True, scheme="redfish")
        assert build_bmc_address("INTERSIGHT", bmc).startswith("redfish-virtualmedia://")
        assert build_bmc_address("CISCO", bmc).startswith("ipmi://")

    def test_a_dns_host_with_a_port_and_path_survives(self) -> None:
        # The kubevirt-redfish case: a virtual BMC behind an OpenShift Route
        # has a DNS name, a non-default port and a per-machine path. bmhgen
        # ran ipaddress.IPv4Address() on the host and rebuilt the path from a
        # vendor table, which discarded all three.
        bmc = BmcEndpoint(
            host="redfish.apps.example.com",
            host_is_ip=False,
            scheme="redfish",
            port=8443,
            path="/redfish/v1/Systems/vm-0042",
        )
        assert build_bmc_address("HP", bmc) == (
            "redfish-virtualmedia://redfish.apps.example.com:8443"
            "/redfish/v1/Systems/vm-0042"
        )

    def test_a_missing_path_falls_back_to_the_vendor_default(self) -> None:
        bmc = BmcEndpoint(host="10.0.0.5", host_is_ip=True, scheme="redfish")
        assert build_bmc_address("HP", bmc) == (
            "redfish-virtualmedia://10.0.0.5/redfish/v1/Systems/1"
        )

    def test_an_unmapped_vendor_raises_rather_than_guessing(self) -> None:
        with pytest.raises(KeyError):
            build_bmc_address("STANDALONE", IP_BMC)

    def test_secret_name_matches_the_operator_byte_for_byte(self) -> None:
        # A cluster already holding resources the Kopf operator created must be
        # converged by this workflow, not duplicated by it.
        assert bmc_secret_name("HP", "server01") == "hp-cred-server01"

    def test_an_ipv6_host_is_bracketed_exactly_once(self) -> None:
        # A URL authority needs an IPv6 literal bracketed, but server-scan may
        # already have done it — and `[[fd00::5]]` is not an address Ironic can
        # parse.
        bare = build_bmc_address("HP", BmcEndpoint(host="fd00::5", host_is_ip=True))
        pre_bracketed = build_bmc_address("HP", BmcEndpoint(host="[fd00::5]", host_is_ip=True))
        assert bare == pre_bracketed
        assert "[fd00::5]" in bare and "[[" not in bare


class TestWorkflowIdKeysOnTheCandidatePool:
    """The id must serialize whatever decides which servers a run can draw.

    It first keyed on (InfraEnv, MCE), which was wrong: the pool is
    `^ocp-<infraEnv>` with no MCE in it, so two MCEs filling an InfraEnv of the
    same name got different ids while drawing from the same pool — the one race
    the serialization exists to prevent.
    """

    def test_the_same_infraenv_on_two_mces_shares_one_id(self) -> None:
        assert install_server_workflow_id("dell-r650-tlv-64c-1024gb") == (
            install_server_workflow_id("dell-r650-tlv-64c-1024gb")
        )

    def test_different_infraenvs_do_not_serialize_against_each_other(self) -> None:
        assert install_server_workflow_id("dell-r650-tlv-64c-1024gb") != (
            install_server_workflow_id("hp-gen11-nyc-64c-128gb")
        )

    def test_a_named_server_is_a_pool_of_one_and_gets_its_own_id(self) -> None:
        # Naming a machine draws from a pool of one, so it need not queue
        # behind a pattern draw for the same InfraEnv.
        by_name = install_server_workflow_id("dell-r650-tlv-64c-1024gb", "ocp-dell-x")
        assert by_name == "install-server-name-ocp-dell-x"
        assert by_name != install_server_workflow_id("dell-r650-tlv-64c-1024gb")

    def test_one_machine_gets_one_id_however_the_caller_cased_its_name(self) -> None:
        """server-scan names carry an uppercase vendor serial.

        The run lowercases that to build the resource names, so an id that kept
        the original case would hand ONE machine TWO ids — both runs accepted,
        both racing onto the same BareMetalHost, which is the collision the id
        exists to prevent.
        """
        upper = install_server_workflow_id("ie", "ocp-hp-gen11-nyc-64c-128gb-HP0001592")
        lower = install_server_workflow_id("ie", "ocp-hp-gen11-nyc-64c-128gb-hp0001592")
        assert upper == lower

    def test_two_different_servers_still_get_different_ids(self) -> None:
        # Lowercasing cannot merge two machines: server-scan names differ by
        # more than case.
        assert install_server_workflow_id("ie", "ocp-a-HP0001592") != (
            install_server_workflow_id("ie", "ocp-a-HP0001593")
        )
