"""Builders for the three Kubernetes resources install-server creates.

Pure functions — no API calls, so they are unit-testable on their own. Ported
from BareMetalHostUCS's `src/yaml_generators.py`, with two deliberate changes:

  * NIC names are LOGICAL (`nic1`, `nic2`). NMStateConfig binds MAC -> name in
    `spec.interfaces` and the agent renames the NIC to match before applying
    `spec.config`, so the names never have to equal what the OS would pick.
    That is what removed the operator's per-server-type NIC-name profile
    ConfigMap, its pattern matching and its `mac_indices` selectors outright.
  * The BMC address is built from what server-scan parsed, not from a fixed
    per-vendor template — see shared/bmc_address.py.

Resource NAMES are byte-identical to the operator's, so a cluster already
holding resources it created is converged by this workflow rather than
duplicated by it.
"""

from __future__ import annotations

import base64
import re
from typing import Any

from shared.bmc_address import bmc_secret_name, build_bmc_address, nmstate_config_name
from shared.models.server_lifecycle import BmhResourceRequest

BMH_GROUP = "metal3.io"
BMH_VERSION = "v1alpha1"
BMH_PLURAL = "baremetalhosts"

NMSTATE_GROUP = "agent-install.openshift.io"
NMSTATE_VERSION = "v1beta1"
NMSTATE_PLURAL = "nmstateconfigs"

_INFRAENV_LABEL = "infraenvs.agent-install.openshift.io"

_MAC_RE = re.compile(r"(?:[0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}")


class InvalidMacError(ValueError):
    """A MAC that server-scan reported does not parse.

    server-scan normalizes MACs before persisting them, so this is a guard
    against a malformed payload rather than an expected condition.
    """


def _validate_macs(request: BmhResourceRequest) -> None:
    """Reject a malformed MAC before anything is written to the cluster.

    Note what is NOT validated here: the BMC host. The operator ran
    `ipaddress.IPv4Address()` on it, which rejects a BMC published as a DNS
    name — an OpenShift Route fronting a virtual Redfish BMC, for instance.
    server-scan already reports whether the host is an IP (`host_is_ip`), so
    the distinction is carried rather than enforced.
    """
    for member in request.bond_members:
        if not _MAC_RE.fullmatch(member.mac):
            raise InvalidMacError(
                f"{request.server_name}: interface {member.logical_name} has an "
                f"invalid MAC {member.mac!r}"
            )


def build_bmc_secret(request: BmhResourceRequest, username: str, password: str) -> dict[str, Any]:
    """The BMC credentials Secret the BareMetalHost references by name."""
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": bmc_secret_name(request.bmc_vendor, request.server_name),
            "namespace": request.namespace,
        },
        "type": "Opaque",
        "data": {
            "username": base64.b64encode(username.encode()).decode(),
            "password": base64.b64encode(password.encode()).decode(),
        },
    }


def build_baremetal_host(request: BmhResourceRequest) -> dict[str, Any]:
    """The BareMetalHost, booting from the first bond member.

    `inspect.metal3.io: disabled` and `automatedCleaningMode: disabled` are
    carried over from the operator: these hosts are installed by the Assisted
    Installer via `customDeploy`, which does its own inventory, so Ironic
    inspection and disk cleaning would only add a reboot cycle.
    """
    _validate_macs(request)
    return {
        "apiVersion": f"{BMH_GROUP}/{BMH_VERSION}",
        "kind": "BareMetalHost",
        "metadata": {
            "name": request.server_name,
            "namespace": request.namespace,
            "labels": {_INFRAENV_LABEL: request.infra_env, **request.labels},
            "annotations": {
                "inspect.metal3.io": "disabled",
                "bmac.agent-install.openshift.io/hostname": request.server_name,
            },
        },
        "spec": {
            "online": True,
            "bootMACAddress": request.bond_members[0].mac,
            "hardwareProfile": "empty",
            "customDeploy": {"method": "start_assisted_install"},
            "automatedCleaningMode": "disabled",
            "bootMode": "UEFI",
            "bmc": {
                "address": build_bmc_address(request.bmc_vendor, request.bmc),
                "credentialsName": bmc_secret_name(
                    request.bmc_vendor, request.server_name
                ),
                "disableCertificateVerification": True,
            },
        },
    }


def _ethernet_iface(name: str, mac: str) -> dict[str, Any]:
    """A bond member: up, and carrying no address of its own."""
    return {
        "name": name,
        "type": "ethernet",
        "state": "up",
        "mac-address": mac,
        "ipv4": {"enabled": False},
        "ipv6": {"enabled": False},
    }


def _bond_iface(request: BmhResourceRequest) -> dict[str, Any]:
    """The bond. `lacp_rate: fast` only applies to 802.3ad."""
    options = {"miimon": "100"}
    if request.bond_mode == "802.3ad":
        options["lacp_rate"] = "fast"
    return {
        "name": request.bond_name,
        "type": "bond",
        "state": "up",
        "ipv4": {"enabled": False},
        "ipv6": {"enabled": False},
        "link-aggregation": {
            "mode": request.bond_mode,
            "options": options,
            "port": [member.logical_name for member in request.bond_members],
        },
    }


def _vlan_iface(request: BmhResourceRequest) -> dict[str, Any]:
    """The DHCP VLAN riding the bond — the only interface with an address."""
    return {
        "name": f"{request.bond_name}.{request.vlan_id}",
        "type": "vlan",
        "state": "up",
        "vlan": {"base-iface": request.bond_name, "id": request.vlan_id},
        "ipv4": {
            "enabled": True,
            "dhcp": True,
            "auto-dns": True,
            "auto-gateway": True,
            "auto-routes": True,
        },
        "ipv6": {"enabled": False, "dhcp": False, "autoconf": False},
    }


def build_nmstate_config(request: BmhResourceRequest) -> dict[str, Any]:
    """The NMStateConfig: a bond of the selected members, VLAN-tagged, on DHCP.

    `spec.interfaces` is the MAC -> logical-name binding the agent applies
    before `spec.config` is evaluated; `spec.config` then refers only to the
    logical names.
    """
    _validate_macs(request)
    members = [
        _ethernet_iface(member.logical_name, member.mac)
        for member in request.bond_members
    ]
    return {
        "apiVersion": f"{NMSTATE_GROUP}/{NMSTATE_VERSION}",
        "kind": "NMStateConfig",
        "metadata": {
            "name": nmstate_config_name(request.server_name),
            "namespace": request.namespace,
            "labels": {_INFRAENV_LABEL: request.infra_env},
        },
        "spec": {
            "config": {
                "interfaces": [*members, _bond_iface(request), _vlan_iface(request)]
            },
            "interfaces": [
                {"name": member.logical_name, "macAddress": member.mac}
                for member in request.bond_members
            ],
        },
    }
