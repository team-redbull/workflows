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
    baremetal_host_differences,
    build_baremetal_host,
    build_bmc_secret,
    build_nmstate_config,
    nmstate_config_differences,
)
from shared.bmc_address import bmc_secret_name, k8s_resource_name
from shared.exceptions import InvalidServerNameError
from shared.models.server_lifecycle import BmcEndpoint, BmhResourceRequest, BondMember

SERVER = "ocp-dell-r650-tlv-64c-1024gb-DEL0000485"
# What Kubernetes will actually hold: server-scan names carry an uppercase
# vendor serial, which is not a legal resource name.
SERVER_K8S = SERVER.lower()
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
        assert secret["metadata"]["name"] == f"dell-cred-{SERVER_K8S}"
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
        assert bmh["spec"]["bmc"]["credentialsName"] == f"dell-cred-{SERVER_K8S}"
        assert (
            bmh["metadata"]["annotations"]["bmac.agent-install.openshift.io/hostname"]
            == SERVER_K8S
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
        assert nmstate["metadata"]["name"] == f"nmstate-config-{SERVER_K8S}"
        assert nmstate["metadata"]["labels"][INFRAENV_LABEL] == INFRA_ENV

    def test_a_non_lacp_mode_omits_lacp_rate(self) -> None:
        nmstate = build_nmstate_config(_request(bond_mode="active-backup"))
        bond = _by_name(nmstate["spec"]["config"]["interfaces"])["bond0"]
        assert bond["link-aggregation"]["mode"] == "active-backup"
        assert "lacp_rate" not in bond["link-aggregation"]["options"]


class TestExistingResourceComparison:
    """An existing resource is success only when it MATCHES.

    The creates are idempotent so a re-run converges, but reporting success for
    a BareMetalHost that names a different BMC or boot MAC would return a
    result describing an installation that is not the one on the cluster.
    """

    def test_an_identical_baremetalhost_has_no_differences(self) -> None:
        desired = build_baremetal_host(_request())
        assert baremetal_host_differences(desired, desired) == []

    def test_a_resource_the_operator_created_converges_despite_mac_case(self) -> None:
        """The compatibility guarantee this whole migration rests on.

        bmhgen wrote MACs exactly as the vendor manager handed them over, and
        HP OneView and Dell OME return them UPPER case — nothing in that
        operator normalised them. server-scan lower-cases every MAC on ingest.
        Comparing the two literally turns every host bmhgen ever created into a
        non-retryable BmhConflictError on the SAME machine, clearable only by
        hand-editing each one.
        """
        desired = build_baremetal_host(_request())
        as_bmhgen_wrote_it = build_baremetal_host(_request())
        as_bmhgen_wrote_it["spec"]["bootMACAddress"] = (
            as_bmhgen_wrote_it["spec"]["bootMACAddress"].upper()
        )
        assert baremetal_host_differences(desired, as_bmhgen_wrote_it) == []

    def test_a_genuinely_different_mac_is_still_a_difference(self) -> None:
        # Case-folding must not blunt the check it is folding for.
        desired = build_baremetal_host(_request())
        existing = build_baremetal_host(_request())
        existing["spec"]["bootMACAddress"] = "AA:BB:CC:DD:EE:FF"
        (difference,) = baremetal_host_differences(desired, existing)
        assert "spec.bootMACAddress" in difference
        # Reported RAW, so an operator sees what is actually on the cluster.
        assert "AA:BB:CC:DD:EE:FF" in difference

    def test_a_different_bmc_address_is_a_difference(self) -> None:
        desired = build_baremetal_host(_request())
        existing = build_baremetal_host(
            _request(bmc=BmcEndpoint(host="10.99.99.99", host_is_ip=True, scheme="redfish"))
        )
        (difference,) = baremetal_host_differences(desired, existing)
        assert "spec.bmc.address" in difference
        assert "10.99.99.99" in difference

    def test_a_different_boot_mac_and_infraenv_are_both_reported(self) -> None:
        desired = build_baremetal_host(_request())
        existing = build_baremetal_host(
            _request(
                infra_env="some-other-infraenv",
                bond_members=[
                    BondMember(logical_name="nic1", mac="ff:ff:ff:ff:ff:01", port_key="1/1"),
                    BondMember(logical_name="nic2", mac="ff:ff:ff:ff:ff:02", port_key="1/2"),
                ],
            )
        )
        differences = baremetal_host_differences(desired, existing)
        assert any("bootMACAddress" in d for d in differences)
        assert any(INFRAENV_LABEL in d for d in differences)

    def test_status_and_extra_annotations_are_not_differences(self) -> None:
        # Convergence, not overwriting: an operator's additions stay.
        desired = build_baremetal_host(_request())
        existing = build_baremetal_host(_request())
        existing["status"] = {"provisioning": {"state": "provisioned"}}
        existing["metadata"]["annotations"]["note"] = "corrected by hand"
        assert baremetal_host_differences(desired, existing) == []

    def test_an_identical_nmstateconfig_has_no_differences(self) -> None:
        desired = build_nmstate_config(_request())
        assert nmstate_config_differences(desired, desired) == []

    def test_an_nmstateconfig_from_the_operator_converges_despite_mac_case(self) -> None:
        desired = build_nmstate_config(_request())
        as_bmhgen_wrote_it = build_nmstate_config(_request())
        for interface in as_bmhgen_wrote_it["spec"]["interfaces"]:
            interface["macAddress"] = interface["macAddress"].upper()
        assert nmstate_config_differences(desired, as_bmhgen_wrote_it) == []

    def test_a_genuinely_different_mac_set_is_still_a_difference(self) -> None:
        desired = build_nmstate_config(_request())
        existing = build_nmstate_config(_request())
        existing["spec"]["interfaces"][0]["macAddress"] = "AA:BB:CC:DD:EE:FF"
        (difference,) = nmstate_config_differences(desired, existing)
        assert "MAC set" in difference

    def test_a_different_vlan_is_a_difference(self) -> None:
        desired = build_nmstate_config(_request())
        existing = build_nmstate_config(_request(vlan_id=999))
        assert any("VLAN id" in d for d in nmstate_config_differences(desired, existing))

    def test_the_mac_set_is_compared_unordered(self) -> None:
        # The same two wires are the same bond whichever became nic1.
        desired = build_nmstate_config(_request())
        swapped = build_nmstate_config(
            _request(
                bond_members=[
                    BondMember(logical_name="nic1", mac="aa:bb:cc:dd:ee:02", port_key="1/2"),
                    BondMember(logical_name="nic2", mac="aa:bb:cc:dd:ee:01", port_key="1/1"),
                ]
            )
        )
        assert nmstate_config_differences(desired, swapped) == []

    def test_a_different_mac_set_is_a_difference(self) -> None:
        desired = build_nmstate_config(_request())
        existing = build_nmstate_config(
            _request(
                bond_members=[
                    BondMember(logical_name="nic1", mac="aa:bb:cc:dd:ee:01", port_key="1/1"),
                    BondMember(logical_name="nic2", mac="ff:ff:ff:ff:ff:99", port_key="1/3"),
                ]
            )
        )
        assert any("MAC set" in d for d in nmstate_config_differences(desired, existing))


class TestLabelPrecedence:
    def test_a_caller_cannot_override_the_infraenv_label(self) -> None:
        # The BareMetalHost and its NMStateConfig bind to an InfraEnv through
        # this label. A caller winning here would split them across InfraEnvs.
        bmh = build_baremetal_host(_request(labels={INFRAENV_LABEL: "somebody-elses-infraenv"}))
        assert bmh["metadata"]["labels"][INFRAENV_LABEL] == INFRA_ENV


class TestKubernetesNameCasing:
    """server-scan names carry an uppercase vendor serial; Kubernetes forbids one.

    bmhgen never met this: its names came from a CR a human wrote. Taking the
    name from the vendor's inventory instead is what introduced the gap, and
    the API server answers 422 identically on every attempt.
    """

    UPPER = "ocp-hp-gen11-nyc-64c-128gb-HP0001592"

    def test_every_resource_name_is_lowercased(self) -> None:
        request = _request(server_name=self.UPPER, bmc_vendor="HP")
        assert build_bmc_secret(request, "u", "p")["metadata"]["name"] == (
            "hp-cred-ocp-hp-gen11-nyc-64c-128gb-hp0001592"
        )
        assert build_baremetal_host(request)["metadata"]["name"] == (
            "ocp-hp-gen11-nyc-64c-128gb-hp0001592"
        )
        assert build_nmstate_config(request)["metadata"]["name"] == (
            "nmstate-config-ocp-hp-gen11-nyc-64c-128gb-hp0001592"
        )

    def test_the_hostname_annotation_is_lowercased_too(self) -> None:
        # It becomes the node's hostname, so it is DNS-cased for the same reason.
        bmh = build_baremetal_host(_request(server_name=self.UPPER, bmc_vendor="HP"))
        assert bmh["metadata"]["annotations"][
            "bmac.agent-install.openshift.io/hostname"
        ] == "ocp-hp-gen11-nyc-64c-128gb-hp0001592"

    def test_an_already_valid_name_is_unchanged(self) -> None:
        # The transform is the identity for anything bmhgen could have been
        # given, so its resources are still converged on, not duplicated.
        assert k8s_resource_name("server01") == "server01"
        assert bmc_secret_name("HP", "server01") == "hp-cred-server01"

    def test_a_name_lowercasing_cannot_rescue_is_refused(self) -> None:
        # Not rewritten into something installable — a server installed under a
        # name nobody can trace back to the inventory is worse than a failure.
        for bad in ("under_score", "-leading-dash", "trailing-dash-", "a" * 254):
            with pytest.raises(InvalidServerNameError):
                k8s_resource_name(bad)
