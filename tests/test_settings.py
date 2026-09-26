"""Fail-fast parsing in SegmentLifecycleActivitySettings: DHCP_EXCLUSION_OCTET_RANGES
and the DAY1_REPO_URL scheme.

The activity worker's config is fail-fast by design: it is instantiated at
module import, so a bad value crash-loops the pod at startup rather than
mis-rendering a DHCP scope hours into a run. These tests pin that behaviour for
the one knob that still carries structure.

(SITE_NETWORKS and the PORTS_* profiles were the other two, and had tests of
their own here. Both keys are gone with the firewall flow that read them.)
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from shared.models.segment_lifecycle import SegmentType
from shared.settings import SegmentLifecycleActivitySettings


class TestDay1RepoUrl:
    """The push token is injected into https URLs only. Any other scheme would
    drop it silently and every run would retry an auth failure forever, so the
    worker refuses to start instead."""

    def test_https_is_accepted(self, monkeypatch):
        monkeypatch.setenv("DAY1_REPO_URL", "https://gitlab.internal/redbull/day1.git")
        assert SegmentLifecycleActivitySettings().day1_repo_url.startswith("https://")

    @pytest.mark.parametrize(
        "url",
        [
            "http://gitlab.internal/redbull/day1.git",
            "git@gitlab.internal:redbull/day1.git",
            "ssh://git@gitlab.internal/redbull/day1.git",
        ],
    )
    def test_any_other_scheme_is_rejected_at_startup(self, monkeypatch, url):
        monkeypatch.setenv("DAY1_REPO_URL", url)
        with pytest.raises(ValidationError, match="must be an https:// URL"):
            SegmentLifecycleActivitySettings()


class TestDhcpExclusionOctetRanges:
    """The ONE DHCP policy knob, keyed by segment type — a bad edit must
    crash-loop the worker at startup, never mis-render a scope hours later.
    Every per-range rule is enforced for EVERY type in the map, not just HC:
    a type configured ahead of the code that uses it is validated now."""

    def test_parses_the_per_type_configmap_json_object(self, monkeypatch):
        monkeypatch.setenv(
            "DHCP_EXCLUSION_OCTET_RANGES",
            '{"HC": [[1, 10], [100, 110], [241, 254]], "PXE": [[1, 20]]}',
        )
        s = SegmentLifecycleActivitySettings()
        assert s.dhcp_exclusion_octet_ranges == {
            SegmentType.HC: [(1, 10), (100, 110), (241, 254)],
            SegmentType.PXE: [(1, 20)],
        }

    def test_empty_policy_is_rejected(self, monkeypatch):
        # Same env-not-kwarg rule as the empty topology above.
        monkeypatch.setenv("DHCP_EXCLUSION_OCTET_RANGES", "{}")
        with pytest.raises(ValidationError, match="must not be empty"):
            SegmentLifecycleActivitySettings()

    def test_a_policy_without_hc_is_rejected(self, monkeypatch):
        # HC is the only type allocate-segment supports, so a map that omits it
        # configures nothing any run can reach.
        monkeypatch.setenv("DHCP_EXCLUSION_OCTET_RANGES", '{"PXE": [[1, 10]]}')
        with pytest.raises(ValidationError, match="must carry a policy for type 'HC'"):
            SegmentLifecycleActivitySettings()

    def test_an_unknown_type_key_is_rejected(self, monkeypatch):
        # The map is keyed by SegmentType, so a key that is not one is a typo
        # or a type this service does not know — either way, not a silent drop.
        monkeypatch.setenv(
            "DHCP_EXCLUSION_OCTET_RANGES", '{"HC": [[1, 10]], "STORAGE": [[1, 10]]}'
        )
        with pytest.raises(ValidationError):
            SegmentLifecycleActivitySettings()

    def test_a_types_empty_range_list_means_no_exclusions(self, monkeypatch):
        # A type that excludes nothing is a real configuration: its block
        # carries a network and no exclusions, and the scope distributes the
        # DHCP API's whole derived .1-.253.
        monkeypatch.setenv("DHCP_EXCLUSION_OCTET_RANGES", '{"HC": [], "PXE": []}')
        s = SegmentLifecycleActivitySettings()
        assert s.dhcp_exclusion_octet_ranges == {SegmentType.HC: [], SegmentType.PXE: []}

    @pytest.mark.parametrize("ranges", ["[[0, 10]]", "[[1, 255]]", "[[241, 300]]"])
    def test_octets_outside_the_slash_24_host_range_are_rejected(self, monkeypatch, ranges):
        monkeypatch.setenv("DHCP_EXCLUSION_OCTET_RANGES", '{"HC": %s}' % ranges)
        with pytest.raises(ValidationError, match=r"1\.\.254"):
            SegmentLifecycleActivitySettings()

    def test_inverted_pair_is_rejected(self, monkeypatch):
        monkeypatch.setenv("DHCP_EXCLUSION_OCTET_RANGES", '{"HC": [[10, 1]]}')
        with pytest.raises(ValidationError, match="inverted"):
            SegmentLifecycleActivitySettings()

    @pytest.mark.parametrize("ranges", ["[[241, 254], [1, 10]]", "[[1, 10], [5, 20]]"])
    def test_descending_or_overlapping_pairs_are_rejected(self, monkeypatch, ranges):
        monkeypatch.setenv("DHCP_EXCLUSION_OCTET_RANGES", '{"HC": %s}' % ranges)
        with pytest.raises(ValidationError, match="ascending"):
            SegmentLifecycleActivitySettings()

    @pytest.mark.parametrize("ranges", ["[[1, 254]]", "[[1, 253]]"])
    def test_excluding_every_distributable_octet_is_rejected(self, monkeypatch, ranges):
        # .1-.253 is what the DHCP API distributes, so covering all of it
        # leaves nothing to lease — whether or not .254 is named too.
        monkeypatch.setenv("DHCP_EXCLUSION_OCTET_RANGES", '{"HC": %s}' % ranges)
        with pytest.raises(ValidationError, match="nothing left to distribute"):
            SegmentLifecycleActivitySettings()

    def test_a_non_hc_types_ranges_are_validated_too(self, monkeypatch):
        monkeypatch.setenv(
            "DHCP_EXCLUSION_OCTET_RANGES", '{"HC": [[1, 10]], "PXE": [[10, 1]]}'
        )
        with pytest.raises(ValidationError, match="inverted"):
            SegmentLifecycleActivitySettings()
