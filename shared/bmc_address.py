"""BMC address and secret naming — pure string logic, no I/O.

Lives in the contract layer because both sides need it: the activity puts the
address on the BareMetalHost, and the workflow reports it in the run result.
Pure functions only, so a workflow can import it through the sandbox.

The address is BUILT FROM WHAT SERVER-SCAN REPORTED, not from a per-vendor
template. bmhgen took only an IP from the vendor manager and reconstructed
`redfish-virtualmedia://{ip}/redfish/v1/Systems/1` (or an iDRAC/IPMI variant)
around it, discarding any real port or path. server-scan already parsed the
address it collected into scheme/host/port/path, so only the DRIVER has to be
decided here — everything else is carried through.

That distinction is what lets a BMC behind an OpenShift Route work: it has a
DNS host, a non-default port and a path per machine, all of which a fixed
template would throw away. Nothing here validates the host as an IPv4 address
for the same reason.
"""

from __future__ import annotations

import re

from shared.exceptions import InvalidServerNameError
from shared.models.server_lifecycle import BmcEndpoint

# BMC driver per server-scan's `bmc_vendor` vocabulary. Cisco splits by how the
# server is managed, not by who made it: a UCS-managed blade is driven over
# IPMI while an Intersight-managed one exposes Redfish on its CIMC — which is
# exactly why server-scan reports INTERSIGHT as a fourth value beside CISCO
# rather than collapsing both into its own `cisco` vendor.
_DRIVER_BY_VENDOR = {
    "HP": "redfish-virtualmedia",
    "DELL": "idrac-virtualmedia",
    "INTERSIGHT": "redfish-virtualmedia",
    "CISCO": "ipmi",
}

# Where a vendor's Redfish system lives, when the BMC did not tell us. Used
# only as a fallback: a path server-scan actually collected always wins.
_DEFAULT_PATH_BY_VENDOR = {
    "HP": "/redfish/v1/Systems/1",
    "DELL": "/redfish/v1/Systems/System.Embedded.1",
    "INTERSIGHT": "/redfish/v1/Systems/1",
}

_DEFAULT_IPMI_PORT = 623


def build_bmc_address(bmc_vendor: str, bmc: BmcEndpoint) -> str:
    """The Ironic BMC address for a BareMetalHost's `spec.bmc.address`.

    Args:
        bmc_vendor: server-scan's `bmc_vendor` — HP, DELL, CISCO or INTERSIGHT.
        bmc: the BMC endpoint as server-scan parsed it.

    Returns:
        A driver URL, e.g. `redfish-virtualmedia://10.0.0.5/redfish/v1/Systems/1`
        or `ipmi://10.0.0.5:623`, preserving whatever host, port and path the
        collector actually observed.

    Raises:
        KeyError: the vendor has no driver mapping. Callers classify this as
            UnknownBmcVendorError rather than guessing a driver.
    """
    driver = _DRIVER_BY_VENDOR[bmc_vendor.upper()]
    host = f"[{bmc.host}]" if ":" in bmc.host else bmc.host

    if driver == "ipmi":
        # IPMI carries no path, and the port is part of the address that
        # Ironic expects to see spelled out.
        return f"{driver}://{host}:{bmc.port or _DEFAULT_IPMI_PORT}"

    authority = f"{host}:{bmc.port}" if bmc.port else host
    path = bmc.path or _DEFAULT_PATH_BY_VENDOR.get(bmc_vendor.upper(), "")
    return f"{driver}://{authority}{path}"


_RFC1123_NAME = re.compile(r"[a-z0-9]([-a-z0-9.]*[a-z0-9])?")


def k8s_resource_name(server_name: str) -> str:
    """One server-scan name as a Kubernetes object name.

    Lowercased, because server-scan names carry an uppercase vendor serial
    (`ocp-hp-gen11-nyc-64c-128gb-HP0001592`) and Kubernetes rejects any
    uppercase letter in a resource name outright — a 422 the API server raises
    identically on every attempt.

    bmhgen never needed this: its names came from a BareMetalHostGenerator CR,
    where a human had already written something Kubernetes accepts. Taking the
    name from the vendor's inventory instead is what introduced the gap. The
    transform is the identity for any name bmhgen could have been given, so
    resources it created are still converged on rather than duplicated.

    Only case is corrected. A name that is still not a valid RFC 1123 subdomain
    (an underscore, a leading dash) is a naming-convention problem for a human,
    not something to paper over by rewriting characters and installing the
    server under a name nobody can correlate back to the inventory.
    """
    lowered = server_name.lower()
    if not _RFC1123_NAME.fullmatch(lowered) or len(lowered) > 253:
        raise InvalidServerNameError(
            f"server-scan name {server_name!r} cannot be a Kubernetes resource "
            "name even lowercased: it must be alphanumerics, '-' or '.', start "
            "and end alphanumeric, and be at most 253 characters"
        )
    return lowered


def bmc_secret_name(bmc_vendor: str, server_name: str) -> str:
    """`{vendor}-cred-{server}` — the Secret the BareMetalHost references.

    The stem is byte-identical to what bmhgen produced, so a cluster already
    holding resources from the operator is converged by this workflow rather
    than duplicated by it. See k8s_resource_name for the one case bmhgen never
    met.
    """
    return f"{bmc_vendor.lower()}-cred-{k8s_resource_name(server_name)}"


def nmstate_config_name(server_name: str) -> str:
    """`nmstate-config-{server}` — likewise unchanged from bmhgen."""
    return f"nmstate-config-{k8s_resource_name(server_name)}"
