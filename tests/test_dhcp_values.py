"""build_dhcp_values: the emitted block and the /24 assertion.

The one policy knob is that type's exclusion octet ranges. startRange/endRange
are deliberately NOT emitted — dhcp_scope_manager derives .1-.253 when both are
absent — so these tests pin what IS written: the mask-stripped network, the
exclusions in ascending order, the empty-policy case, and the assertion that
stops a non-/24 segment.
"""

from __future__ import annotations

import pytest
from temporalio.exceptions import ApplicationError

from activities.segment_lifecycle.dhcp_values import build_dhcp_values

STANDARD_RANGES = [(1, 10), (241, 254)]


def test_standard_exclusions_reproduce_the_fixture_block():
    values = build_dhcp_values("10.20.90.0/24", STANDARD_RANGES)
    assert values.network == "10.20.90.0"  # mask stripped — the scope identity
    assert [(e.start_address, e.end_address) for e in values.exclusions] == [
        ("10.20.90.1", "10.20.90.10"),
        ("10.20.90.241", "10.20.90.254"),
    ]


def test_no_distribution_bounds_are_emitted():
    # The DHCP API owns that derivation (.1-.253); duplicating it here would
    # mean two derivations that must stay identical forever.
    values = build_dhcp_values("10.20.90.0/24", STANDARD_RANGES)
    assert not hasattr(values, "start_range")
    assert not hasattr(values, "end_range")
    assert set(values.model_dump()) == {"network", "exclusions"}


def test_an_empty_policy_yields_a_block_with_no_exclusions():
    # A type that excludes nothing is a legitimate configuration: the scope
    # distributes the whole derived .1-.253.
    values = build_dhcp_values("10.20.90.0/24", [])
    assert values.network == "10.20.90.0"
    assert values.exclusions == []


def test_a_mid_range_exclusion_is_emitted_like_any_other():
    # A hole inside the range stays a hole — the DHCP scope holds one
    # contiguous range with the exclusions carved out of it.
    values = build_dhcp_values("10.20.90.0/24", [(1, 10), (100, 110), (241, 254)])
    assert [(e.start_address, e.end_address) for e in values.exclusions] == [
        ("10.20.90.1", "10.20.90.10"),
        ("10.20.90.100", "10.20.90.110"),
        ("10.20.90.241", "10.20.90.254"),
    ]


def test_exclusions_are_emitted_ascending():
    # The DHCP API returns exclusions sorted; any other order in the values
    # file makes Crossplane diff and PUT on every reconcile poll — and the
    # workflow compares the two lists directly when polling for convergence.
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
