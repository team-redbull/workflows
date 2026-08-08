"""Derive a cluster's dhcp_values block from its allocated segment.

The ONE policy input is DHCP_EXCLUSION_OCTET_RANGES (validated fail-fast at
worker startup): last-octet ranges excluded from distribution. startRange and
endRange are DERIVED — every host octet of the /24 that is not excluded is
distributable, so the range is simply the first and last of those. One knob
instead of three removes the class of bug where the range and the exclusions
contradict each other, and a mid-range exclusion (say [100, 110]) still gives
the full outer range with a hole punched in it — exactly how DHCP scopes work.

/24 only, ASSERTED rather than assumed: the octet arithmetic and the DHCP
stack's derived gateway (`.254`, valid for a /24 only — dhcp_scope_manager
requires an explicit gateway + subnetMask for anything else) both depend on
it. The first non-/24 segment stops the run with UnsupportedSegmentPrefix
instead of quietly writing a scope that strands half its addresses;
supporting another mask is a deliberate refactor that must also start
emitting subnetMask and gateway.
"""

from __future__ import annotations

import ipaddress

from temporalio.exceptions import ApplicationError

from shared.models.segment_lifecycle import DhcpExclusion, DhcpValues


def build_dhcp_values(
    segment: str, exclusion_octet_ranges: list[tuple[int, int]]
) -> DhcpValues:
    """Turn an allocated /24 CIDR + the exclusion policy into a DhcpValues.

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
            "segments are supported (the derived DHCP range and the stack's "
            "derived .254 gateway assume one). Supporting another mask is a "
            "deliberate refactor, not a config change.",
            type="UnsupportedSegmentPrefix",
            non_retryable=True,
        )

    # "10.20.90.0" -> prefix "10.20.90"; the policy speaks in last octets.
    network_address = str(network.network_address)
    octet_prefix = network_address.rsplit(".", 1)[0]
    excluded = {
        octet
        for start, end in exclusion_octet_ranges
        for octet in range(start, end + 1)
    }
    distributable = [octet for octet in range(1, 255) if octet not in excluded]

    return DhcpValues(
        # The DHCP scope's identity: the network address with the mask STRIPPED.
        network=network_address,
        start_range=f"{octet_prefix}.{distributable[0]}",
        end_range=f"{octet_prefix}.{distributable[-1]}",
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
