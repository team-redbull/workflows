"""Derive a cluster's dhcp_values block from its allocated segment.

The block carries the NETWORK and, when the type's policy defines any, the
EXCLUSIONS — nothing else. startRange/endRange are deliberately NOT written:
dhcp_scope_manager derives .1-.253 whenever both bounds are absent (stopping
short of the .254 gateway it derives for a /24), and the exclusions carve the
ends back out of that range. Writing bounds here would duplicate a derivation
that already lives in the DHCP API, its CI validator and the chart, and the
three have to agree exactly; a segment whose type excludes nothing simply gets
a block with a network and no exclusions at all.

The ONE policy input is that type's entry in DHCP_EXCLUSION_OCTET_RANGES
(validated fail-fast at worker startup): last-octet ranges excluded from
distribution, possibly empty.

/24 only, ASSERTED rather than assumed: the octet policy addresses the last
octet, and the DHCP stack derives both the .1-.253 range and the .254 gateway
for a /24 only (dhcp_scope_manager requires an explicit gateway + subnetMask
and explicit bounds for anything else). The first non-/24 segment stops the
run with UnsupportedSegmentPrefix instead of quietly writing a block whose
inherited defaults do not fit it.
"""

from __future__ import annotations

import ipaddress

from temporalio.exceptions import ApplicationError

from shared.models.segment_lifecycle import DhcpExclusion, DhcpValues


def build_dhcp_values(
    segment: str, exclusion_octet_ranges: list[tuple[int, int]]
) -> DhcpValues:
    """Turn an allocated /24 CIDR + that type's exclusion policy into a
    DhcpValues. An empty policy yields a block with no exclusions.

    Raises ApplicationError (non-retryable — these never fix themselves):
      * InvalidSegmentCidr — the Segments Manager handed back something that
        is not a valid network CIDR.
      * UnsupportedSegmentPrefix — a valid network, but not a /24.
    """
    try:
        network = ipaddress.ip_network(segment, strict=True)
    except ValueError as exc:
        raise ApplicationError(
            f"Allocated segment {segment!r} is not a valid network CIDR: {exc}",
            type="InvalidSegmentCidr",
            non_retryable=True,
        ) from exc
    if network.prefixlen != 24:
        raise ApplicationError(
            f"Allocated segment {segment} is a /{network.prefixlen}; only /24 "
            "segments are supported (the exclusion policy speaks in last "
            "octets, and the DHCP stack derives the .1-.253 range and the "
            ".254 gateway for a /24 only). Supporting another mask is a "
            "deliberate refactor, not a config change.",
            type="UnsupportedSegmentPrefix",
            non_retryable=True,
        )

    # "10.20.90.0" -> prefix "10.20.90"; the policy speaks in last octets.
    network_address = str(network.network_address)
    octet_prefix = network_address.rsplit(".", 1)[0]

    return DhcpValues(
        # The DHCP scope's identity: the network address with the mask STRIPPED.
        network=network_address,
        # Emitted verbatim — the settings validator guarantees ascending,
        # non-overlapping ranges, which the DHCP API requires (any other order
        # makes Crossplane diff and PUT on every reconcile poll).
        exclusions=[
            DhcpExclusion(
                start_address=f"{octet_prefix}.{start}",
                end_address=f"{octet_prefix}.{end}",
            )
            for start, end in exclusion_octet_ranges
        ],
    )
