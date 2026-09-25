"""The three Kubernetes resources install-server writes, checked as structures.

These are what the Assisted Installer actually consumes, so the shape matters
more than the code path that produces it — and resource NAMES are checked
against the Kopf operator's, because a cluster already holding its resources
must be converged by this workflow rather than duplicated by it.
"""

from __future__ import annotations

import base64

import pytest

from activities.server_lifecycle.bmh_resources import (
    InvalidMacError,
    build_baremetal_host,
    build_bmc_secret,
    build_nmstate_config,
)
from shared.models.server_lifecycle import BmcEndpoint, BmhResourceRequest, BondMember

SERVER = "ocp-dell-r650-tlv-64c-1024gb-DEL0000485"
NAMESPACE = "multicluster-engine"
INFRA_ENV = "dell-r650-tlv-64c-1024gb"
VLAN = 24
INFRAENV_LABEL = "infraenvs.agent-install.openshift.io"


def _request(**overrides) -> BmhResourceRequest:
    defaults = dict(
        server_name=SERVER,
        namespace=NAMESPACE,
        infra_env=INFRA_ENV,
        bmc_vendor="DELL",
        bmc=BmcEndpoint(
            host="10.11.1.229",
            host_is_ip=True,
            scheme="redfish",
            path="/redfish/v1/Systems/System.Embedded.1",
        ),
        bond_members=[
            BondMember(logical_name="nic1", mac="aa:bb:cc:dd:ee:01", port_key="1/1"),
            BondMember(logical_name="nic2", mac="aa:bb:cc:dd:ee:02", port_key="1/2"),
        ],
        vlan_id=VLAN,
    )
    defaults.update(overrides)
    return BmhResourceRequest(**defaults)


def _by_name(interfaces: list[dict]) -> dict[str, dict]:
    return {iface["name"]: iface for iface in interfaces}


class TestBmcSecret:
    def test_credentials_are_base64_encoded_under_the_operators_name(self) -> None:
        secret = build_bmc_secret(_request(), "root", "calvin")
        assert secret["metadata"]["name"] == f"dell-cred-{SERVER}"
        assert secret["metadata"]["namespace"] == NAMESPACE
        assert base64.b64decode(secret["data"]["username"]).decode() == "root"
        assert base64.b64decode(secret["data"]["password"]).decode() == "calvin"


class TestBareMetalHost:
    def test_boots_from_the_first_bond_member(self) -> None:
        bmh = build_baremetal_host(_request())
        assert bmh["spec"]["bootMACAddress"] == "aa:bb:cc:dd:ee:01"

    def test_is_labelled_for_the_infraenv_and_references_its_secret(self) -> None:
        bmh = build_baremetal_host(_request())
        assert bmh["metadata"]["labels"][INFRAENV_LABEL] == INFRA_ENV
        assert bmh["spec"]["bmc"]["credentialsName"] == f"dell-cred-{SERVER}"
        assert (
            bmh["metadata"]["annotations"]["bmac.agent-install.openshift.io/hostname"]
            == SERVER
        )

    def test_inspection_and_cleaning_stay_disabled(self) -> None:
        # The Assisted Installer does its own inventory via customDeploy;
        # Ironic inspection and disk cleaning would only add a reboot cycle.
        bmh = build_baremetal_host(_request())
        assert bmh["metadata"]["annotations"]["inspect.metal3.io"] == "disabled"
        assert bmh["spec"]["automatedCleaningMode"] == "disabled"
        assert bmh["spec"]["customDeploy"] == {"method": "start_assisted_install"}

    def test_a_dns_bmc_host_is_accepted(self) -> None:
        # bmhgen ran ipaddress.IPv4Address() here, which rejects a BMC behind
        # an OpenShift Route outright.
        bmh = build_baremetal_host(
            _request(
                bmc_vendor="HP",
                bmc=BmcEndpoint(
                    host="redfish.apps.example.com",
                    host_is_ip=False,
                    scheme="redfish",
                    port=8443,
                    path="/redfish/v1/Systems/vm-0042",
                ),
            )
        )
        assert bmh["spec"]["bmc"]["address"] == (
            "redfish-virtualmedia://redfish.apps.example.com:8443"
            "/redfish/v1/Systems/vm-0042"
        )

    def test_extra_labels_are_merged_without_losing_the_infraenv_label(self) -> None:
        bmh = build_baremetal_host(_request(labels={"environment": "production"}))
        assert bmh["metadata"]["labels"][INFRAENV_LABEL] == INFRA_ENV
        assert bmh["metadata"]["labels"]["environment"] == "production"

    def test_a_malformed_mac_is_rejected_before_anything_is_written(self) -> None:
        with pytest.raises(InvalidMacError):
            build_baremetal_host(
                _request(
                    bond_members=[
                        BondMember(logical_name="nic1", mac="not-a-mac", port_key="1/1"),
                        BondMember(
                            logical_name="nic2", mac="aa:bb:cc:dd:ee:02", port_key="1/2"
                        ),
                    ]
                )
            )


class TestNMStateConfig:
    def test_binds_each_mac_to_its_logical_name(self) -> None:
        # spec.interfaces is the MAC -> name binding the agent applies before
        # spec.config is evaluated. It is why the names can be placeholders.
        nmstate = build_nmstate_config(_request())
        assert nmstate["spec"]["interfaces"] == [
            {"name": "nic1", "macAddress": "aa:bb:cc:dd:ee:01"},
            {"name": "nic2", "macAddress": "aa:bb:cc:dd:ee:02"},
        ]

    def test_builds_a_lacp_bond_over_both_members(self) -> None:
        nmstate = build_nmstate_config(_request())
        bond = _by_name(nmstate["spec"]["config"]["interfaces"])["bond0"]
        assert bond["type"] == "bond"
        assert bond["link-aggregation"]["mode"] == "802.3ad"
        assert bond["link-aggregation"]["port"] == ["nic1", "nic2"]
        assert bond["link-aggregation"]["options"]["lacp_rate"] == "fast"

    def test_only_the_vlan_carries_an_address(self) -> None:
        # Members and the bond are IP-disabled; DHCP rides the VLAN alone.
        interfaces = _by_name(build_nmstate_config(_request())["spec"]["config"]["interfaces"])
        for name in ("nic1", "nic2", "bond0"):
            assert interfaces[name]["ipv4"]["enabled"] is False
        vlan = interfaces[f"bond0.{VLAN}"]
        assert vlan["type"] == "vlan"
        assert vlan["vlan"] == {"base-iface": "bond0", "id": VLAN}
        assert vlan["ipv4"]["dhcp"] is True
        assert vlan["ipv6"]["enabled"] is False

    def test_is_named_and_labelled_as_the_operator_did(self) -> None:
        nmstate = build_nmstate_config(_request())
        assert nmstate["metadata"]["name"] == f"nmstate-config-{SERVER}"
        assert nmstate["metadata"]["labels"][INFRAENV_LABEL] == INFRA_ENV

    def test_a_non_lacp_mode_omits_lacp_rate(self) -> None:
        nmstate = build_nmstate_config(_request(bond_mode="active-backup"))
        bond = _by_name(nmstate["spec"]["config"]["interfaces"])["bond0"]
        assert bond["link-aggregation"]["mode"] == "active-backup"
        assert "lacp_rate" not in bond["link-aggregation"]["options"]
