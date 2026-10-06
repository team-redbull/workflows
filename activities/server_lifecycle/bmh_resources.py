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
from collections.abc import Iterable
from typing import Any

from shared.bmc_address import (
    bmc_secret_name,
    build_bmc_address,
    k8s_resource_name,
    nmstate_config_name,
)
from shared.exceptions import InvalidMacError
from shared.models.server_lifecycle import BmhResourceRequest, mac_is_valid

BMH_GROUP = "metal3.io"
BMH_VERSION = "v1alpha1"
BMH_PLURAL = "baremetalhosts"

NMSTATE_GROUP = "agent-install.openshift.io"
NMSTATE_VERSION = "v1beta1"
NMSTATE_PLURAL = "nmstateconfigs"

AGENT_GROUP = "agent-install.openshift.io"
AGENT_VERSION = "v1beta1"
AGENT_PLURAL = "agents"

INFRAENV_GROUP = "agent-install.openshift.io"
INFRAENV_VERSION = "v1beta1"
INFRAENV_PLURAL = "infraenvs"

# Tells metal3 to stop managing a host WITHOUT deprovisioning it. A teardown
# sets this before deleting, because deprovisioning talks to the BMC and a
# rollback happens precisely when that BMC never answered.
#
# IT MUST BE SET BEFORE THE DELETE. Measured on a live cluster: applied after
# `deletionTimestamp` was already set, it changed nothing for four minutes —
# metal3 honours it while reconciling a live host, not while draining a dying
# one.
DETACHED_ANNOTATION = "baremetalhost.metal3.io/detached"

# The finalizer a teardown may have to drop itself, and ONLY this one.
# baremetal-operator holds it while trying to deprovision through the BMC, which
# never completes when the BMC is unreachable — stuck past four minutes when
# measured. BMAC's `bmac.agent-install.openshift.io/deprovision` is deliberately
# NOT here: it released on its own inside one second, so taking it would be
# robbing a controller that was already doing its job.
BMH_DEPROVISION_FINALIZERS = frozenset({"baremetalhost.metal3.io"})

_INFRAENV_LABEL = "infraenvs.agent-install.openshift.io"


def _validate_macs(request: BmhResourceRequest) -> None:
    """Reject a malformed MAC before anything is written to the cluster.

    Note what is NOT validated here: the BMC host. The operator ran
    `ipaddress.IPv4Address()` on it, which rejects a BMC published as a DNS
    name — an OpenShift Route fronting a virtual Redfish BMC, for instance.
    server-scan already reports whether the host is an IP (`host_is_ip`), so
    the distinction is carried rather than enforced.
    """
    for member in request.bond_members:
        if not mac_is_valid(member.mac):
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
            "name": k8s_resource_name(request.server_name),
            "namespace": request.namespace,
            # Caller labels FIRST: the InfraEnv label binds this host to the
            # InfraEnv whose NMStateConfig carries the matching label, so a
            # caller overriding it would split the two across InfraEnvs.
            "labels": {**request.labels, _INFRAENV_LABEL: request.infra_env},
            "annotations": {
                "inspect.metal3.io": "disabled",
                # The node's hostname, so it is DNS-cased for the same
                # reason the resource name is.
                "bmac.agent-install.openshift.io/hostname": k8s_resource_name(
                    request.server_name
                ),
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


def _mac(value: Any) -> Any:
    """One MAC for COMPARISON: case-folded, because a MAC has no case.

    This is what lets a resource bmhgen created be converged rather than
    reported as a conflict. bmhgen wrote MACs exactly as the vendor manager
    handed them over — HP OneView and Dell OME return them upper case, and
    nothing in that operator normalised them — while server-scan lower-cases
    every MAC on ingest. Comparing the two literally makes every host the
    operator ever created a non-retryable BmhConflictError on the SAME machine,
    which an operator could only clear by hand-editing each one.
    """
    return value.lower() if isinstance(value, str) else value


def _diff(
    field: str, desired: Any, existing: Any, *, normalize: Any = None
) -> str | None:
    """One human-readable difference, or None when the two agree.

    `normalize` applies only to the COMPARISON. The reported values stay raw,
    so a real disagreement shows what is actually on the cluster rather than a
    tidied version of it.
    """
    if normalize is not None:
        if normalize(desired) == normalize(existing):
            return None
    elif desired == existing:
        return None
    return f"{field}: want {desired!r}, found {existing!r}"


def _infraenv_label(resource: dict[str, Any]) -> Any:
    return ((resource.get("metadata") or {}).get("labels") or {}).get(_INFRAENV_LABEL)


def baremetal_host_differences(
    desired: dict[str, Any], existing: dict[str, Any]
) -> list[str]:
    """Which IDENTITY fields of an existing BareMetalHost disagree with ours.

    Only the fields that decide what machine this is and how it is reached are
    compared. Everything else — status, an operator's added annotation, a
    hardware profile corrected by hand — is left alone deliberately: an
    existing resource is meant to be converged on, not overwritten.
    """
    d_spec, e_spec = desired.get("spec") or {}, existing.get("spec") or {}
    d_bmc, e_bmc = d_spec.get("bmc") or {}, e_spec.get("bmc") or {}
    checks = [
        _diff("spec.bmc.address", d_bmc.get("address"), e_bmc.get("address")),
        _diff(
            "spec.bmc.credentialsName",
            d_bmc.get("credentialsName"),
            e_bmc.get("credentialsName"),
        ),
        _diff(
            "spec.bootMACAddress",
            d_spec.get("bootMACAddress"),
            e_spec.get("bootMACAddress"),
            normalize=_mac,
        ),
        _diff(
            f"metadata.labels[{_INFRAENV_LABEL}]",
            _infraenv_label(desired),
            _infraenv_label(existing),
        ),
    ]
    return [c for c in checks if c is not None]


def _vlan_interfaces(resource: dict[str, Any]) -> list[dict[str, Any]]:
    config = ((resource.get("spec") or {}).get("config") or {}).get("interfaces") or []
    return [i for i in config if i.get("type") == "vlan"]


def nmstate_config_differences(
    desired: dict[str, Any], existing: dict[str, Any]
) -> list[str]:
    """Which identity fields of an existing NMStateConfig disagree with ours.

    The MAC set is compared unordered: the bond is the same wiring whichever
    member was picked as nic1. The VLAN id is compared because a host tagged
    onto another MCE's inventory network comes up on the wrong segment and
    never reaches this cluster's assisted-installer service.
    """
    d_macs = {i.get("macAddress") for i in (desired.get("spec") or {}).get("interfaces") or []}
    e_macs = {i.get("macAddress") for i in (existing.get("spec") or {}).get("interfaces") or []}
    d_vlans = sorted(str((i.get("vlan") or {}).get("id")) for i in _vlan_interfaces(desired))
    e_vlans = sorted(str((i.get("vlan") or {}).get("id")) for i in _vlan_interfaces(existing))
    checks = [
        _diff(
            "spec.interfaces MAC set",
            sorted(filter(None, d_macs)),
            sorted(filter(None, e_macs)),
            normalize=lambda macs: sorted(_mac(m) for m in macs),
        ),
        _diff("VLAN id", d_vlans, e_vlans),
        _diff(
            f"metadata.labels[{_INFRAENV_LABEL}]",
            _infraenv_label(desired),
            _infraenv_label(existing),
        ),
    ]
    return [c for c in checks if c is not None]


def infraenv_ipxe_script_url(infra_env: dict[str, Any]) -> str | None:
    """The iPXE script URL an InfraEnv publishes, or None until it has one.

    assisted-service fills `status.bootArtifacts.ipxeScript` once the discovery
    image exists, so an InfraEnv seconds old legitimately has none yet.
    """
    artifacts = (infra_env.get("status") or {}).get("bootArtifacts") or {}
    return artifacts.get("ipxeScript") or None


def agent_matches_macs(agent: dict[str, Any], macs: Iterable[str]) -> bool:
    """Whether one Agent reports any of these MACs among its interfaces.

    MAC is the only usable link between an Agent and the BareMetalHost it came
    from. BMAC names an Agent after the host's own inventory UUID, and the live
    CRD's `agent.spec` carries `approved`, `clusterDeploymentName`, role and
    installation disk — no reference back to the host at all. So matching on
    name or on a spec field would work only by accident, and would break
    silently when BMAC changed either.

    Compared through `_mac` for the same reason every other MAC comparison in
    this module is: assisted-service reports what the NIC announced over the
    wire, server-scan lower-cases on ingest, and a MAC has no case. Comparing
    them literally would make a matching Agent invisible and time the install
    out at its deadline — with the host sitting there, correctly installed.
    """
    wanted = {_mac(mac) for mac in macs}
    interfaces = (
        ((agent.get("status") or {}).get("inventory") or {}).get("interfaces") or []
    )
    return any(_mac(iface.get("macAddress")) in wanted for iface in interfaces)
