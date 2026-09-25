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


def bmc_secret_name(bmc_vendor: str, server_name: str) -> str:
    """`{vendor}-cred-{server}` — the Secret the BareMetalHost references.

    Kept byte-identical to what bmhgen produced, so a cluster already holding
    resources from the operator is converged by this workflow rather than
    duplicated by it.
    """
    return f"{bmc_vendor.lower()}-cred-{server_name}"


def nmstate_config_name(server_name: str) -> str:
    """`nmstate-config-{server}` — likewise unchanged from bmhgen."""
    return f"nmstate-config-{server_name}"
