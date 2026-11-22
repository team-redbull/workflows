"""Which site an iDRAC belongs to, read off its address — pure, no I/O.

Technicians submit IPs, not sites, and the address plan already says where a
machine is: each site's iDRACs live under their own prefix. The router
resolves the site here ONCE, at the edge, and the run carries it as input —
so editing this table never changes what an in-flight run does (it would be a
non-deterministic change if the workflow looked it up on replay), and an
address matching no prefix is a 422 to the technician rather than a failed run
an hour later.

The site is the `<site>` token of the server name
(`ocp-<flavor>-mtl-<site>-<service tag>`, the platform naming convention), and
server-scan parses a server's SITE out of that name (its INVENTORY_SITES), so
every value below must be a code server-scan knows. A bare-metal name is at
most 32 characters, so with a 7-character service tag and a 5-character flavor
a site may have at most 10. (A "region" until 2026-11-22.)
"""

from __future__ import annotations

import ipaddress

# iDRAC network -> site. Longest prefix wins, so a narrower network can carve
# an exception out of a wider one.
#
# TODO(operator): these are the examples from the design discussion — replace
# them with the real iDRAC networks and server-scan site codes before use.
SITE_BY_IDRAC_PREFIX: dict[str, str] = {
    "1.0.0.0/8": "site1",
    "2.0.0.0/8": "site2",
    "134.1.0.0/16": "israel",
}

_NETWORKS = sorted(
    ((ipaddress.ip_network(prefix), site) for prefix, site in SITE_BY_IDRAC_PREFIX.items()),
    key=lambda entry: entry[0].prefixlen,
    reverse=True,
)


def resolve_site(idrac_ip: str) -> str | None:
    """The site whose iDRAC prefix contains this address, or None.

    Raises ValueError when `idrac_ip` is not an IP address at all.
    """
    address = ipaddress.ip_address(idrac_ip)
    for network, site in _NETWORKS:
        if address in network:
            return site
    return None
