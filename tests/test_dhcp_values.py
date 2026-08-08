"""build_dhcp_values: the derived DHCP range and the /24 assertion.

The one policy knob is the exclusion octet ranges; startRange/endRange are
DERIVED (first/last non-excluded host octet). These tests pin the exact
example from the values-repo fixtures and the properties the DHCP stack
depends on (ascending exclusions, the assertion that stops non-/24 segments).
"""

from __future__ import annotations

import pytest
from temporalio.exceptions import ApplicationError

from activities.segment_lifecycle.dhcp_values import build_dhcp_values

STANDARD_RANGES = [(1, 10), (241, 254)]


def test_standard_exclusions_reproduce_the_fixture_block():
    values = build_dhcp_values("10.20.90.0/24", STANDARD_RANGES)
    assert values.network == "10.20.90.0"  # mask stripped — the scope identity
    assert values.start_range == "10.20.90.11"
    assert values.end_range == "10.20.90.240"
    assert [(e.start_address, e.end_address) for e in values.exclusions] == [
        ("10.20.90.1", "10.20.90.10"),
        ("10.20.90.241", "10.20.90.254"),
    ]


def test_ranges_are_derived_not_configured():
    # Shrinking the head exclusion moves startRange with it — no second knob
    # to forget updating.
    values = build_dhcp_values("10.20.90.0/24", [(1, 5), (250, 254)])
    assert values.start_range == "10.20.90.6"
    assert values.end_range == "10.20.90.249"


def test_mid_range_exclusion_keeps_the_outer_range():
    # A hole inside the range stays a hole — the range still spans the first
    # to the last distributable octet, which is exactly how DHCP scopes work.
    values = build_dhcp_values("10.20.90.0/24", [(1, 10), (100, 110), (241, 254)])
    assert values.start_range == "10.20.90.11"
    assert values.end_range == "10.20.90.240"
    assert len(values.exclusions) == 3


def test_exclusions_are_emitted_ascending():
    # The DHCP API returns exclusions sorted; any other order in the values
    # file makes Crossplane diff and PUT on every reconcile poll.
    values = build_dhcp_values("10.20.90.0/24", [(1, 10), (100, 110), (241, 254)])
    starts = [int(e.start_address.rsplit(".", 1)[1]) for e in values.exclusions]
    assert starts == sorted(starts)


def test_non_slash_24_is_rejected_loudly():
    with pytest.raises(ApplicationError) as exc_info:
        build_dhcp_values("10.20.90.0/23", STANDARD_RANGES)
    assert exc_info.value.type == "UnsupportedSegmentPrefix"
    assert exc_info.value.non_retryable
    # The message names the segment and its prefix — the operator's lead.
    assert "10.20.90.0/23" in str(exc_info.value)
    assert "/23" in str(exc_info.value)


def test_host_address_is_rejected_as_invalid_cidr():
    with pytest.raises(ApplicationError) as exc_info:
        build_dhcp_values("10.20.90.1/24", STANDARD_RANGES)
    assert exc_info.value.type == "InvalidSegmentCidr"
    assert exc_info.value.non_retryable
