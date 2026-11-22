"""Which region an iDRAC belongs to, read off its address — pure, no I/O.

Technicians submit IPs, not regions, and the address plan already says where a
machine is: each region's iDRACs live under their own prefix. The router
resolves the region here ONCE, at the edge, and the run carries it as input —
so editing this table never changes what an in-flight run does (it would be a
non-deterministic change if the workflow looked it up on replay), and an
address matching no prefix is a 422 to the technician rather than a failed run
an hour later.

The region is the `<region>` token of the server name, and server-scan parses a
server's SITE out of that name (its INVENTORY_SITES), so every value below must
be a code server-scan knows.
"""

from __future__ import annotations

import ipaddress

# iDRAC network -> region. Longest prefix wins, so a narrower network can carve
# an exception out of a wider one.
#
# TODO(operator): these are the examples from the design discussion — replace
# them with the real iDRAC networks and server-scan site codes before use.
REGION_BY_IDRAC_PREFIX: dict[str, str] = {
    "1.0.0.0/8": "region1",
    "2.0.0.0/8": "region2",
    "134.1.0.0/16": "israel",
}

_NETWORKS = sorted(
    ((ipaddress.ip_network(prefix), region) for prefix, region in REGION_BY_IDRAC_PREFIX.items()),
    key=lambda entry: entry[0].prefixlen,
    reverse=True,
)


def resolve_region(idrac_ip: str) -> str | None:
    """The region whose iDRAC prefix contains this address, or None.

    Raises ValueError when `idrac_ip` is not an IP address at all.
    """
    address = ipaddress.ip_address(idrac_ip)
    for network, region in _NETWORKS:
        if address in network:
            return region
    return None
