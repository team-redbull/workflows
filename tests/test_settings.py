"""SITE_NETWORKS parsing in SegmentLifecycleActivitySettings.

SITE_NETWORKS is the shared site topology: the same JSON is rendered into this
service's ConfigMap and the Segments Manager's, from one definition in
redbull-platform. These tests pin the two properties that arrangement depends
on — unknown sub-keys never break us, and a missing/typo'd `bmc` fails at
startup rather than mid-workflow.
"""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from shared.settings import SegmentLifecycleActivitySettings


def build(site_networks: dict) -> SegmentLifecycleActivitySettings:
    """Construct settings with everything but SITE_NETWORKS taken from conftest env."""
    return SegmentLifecycleActivitySettings(site_networks=site_networks)


class TestSharedTopologyContract:

    def test_pool_sub_key_is_ignored(self):
        """`pool` belongs to the Segments Manager. We must parse it and move on."""
        s = build({"site1": {"pool": "192.10.0.0/16", "bmc": "10.50.0.0/16"}})
        assert s.site_networks["site1"].bmc == "10.50.0.0/16"
        assert not hasattr(s.site_networks["site1"], "pool")

    def test_unknown_sub_key_is_ignored(self):
        """A future consumer adding a sub-key must not crash this worker."""
        s = build({"site1": {"bmc": "10.50.0.0/16", "invented_later": {"a": 1}}})
        assert s.site_networks["site1"].bmc == "10.50.0.0/16"

    def test_parses_from_a_json_string(self):
        """This is how the ConfigMap actually delivers it."""
        raw = json.dumps({"site1": {"pool": "192.10.0.0/16", "bmc": "10.50.0.0/16"}})
        s = SegmentLifecycleActivitySettings(site_networks=json.loads(raw))
        assert s.site_networks["site1"].bmc == "10.50.0.0/16"


class TestFailFast:

    def test_missing_bmc_is_rejected(self):
        with pytest.raises(ValidationError, match="bmc"):
            build({"site1": {"pool": "192.10.0.0/16"}})

    def test_typo_in_bmc_key_is_rejected(self):
        """The reason `bmc` is required and not Optional: catch this at startup."""
        with pytest.raises(ValidationError, match="bmc"):
            build({"site1": {"bcm": "10.50.0.0/16"}})

    @pytest.mark.parametrize("bmc", ["not-a-cidr", "10.50.0.1/16", "10.50.0.0/33"])
    def test_invalid_bmc_cidr_is_rejected(self, bmc):
        with pytest.raises(ValidationError):
            build({"site1": {"bmc": bmc}})

    def test_empty_topology_is_rejected(self, monkeypatch):
        # Must go through the environment, not build(): pydantic-settings treats
        # an empty dict kwarg as "not supplied" and silently falls back to the
        # env var, so build({}) would quietly validate conftest's value instead.
        monkeypatch.setenv("SITE_NETWORKS", "{}")
        with pytest.raises(ValidationError, match="must not be empty"):
            SegmentLifecycleActivitySettings()


class TestDhcpExclusionOctetRanges:
    """The ONE DHCP policy knob — a bad edit must crash-loop the worker at
    startup, never mis-render a scope hours later."""

    def test_parses_from_the_configmap_json_string(self, monkeypatch):
        monkeypatch.setenv("DHCP_EXCLUSION_OCTET_RANGES", "[[1, 10], [100, 110], [241, 254]]")
        s = SegmentLifecycleActivitySettings()
        assert s.dhcp_exclusion_octet_ranges == [(1, 10), (100, 110), (241, 254)]

    def test_empty_list_is_rejected(self, monkeypatch):
        # Same env-not-kwarg rule as the empty topology above.
        monkeypatch.setenv("DHCP_EXCLUSION_OCTET_RANGES", "[]")
        with pytest.raises(ValidationError, match="must not be empty"):
            SegmentLifecycleActivitySettings()

    @pytest.mark.parametrize("ranges", ["[[0, 10]]", "[[1, 255]]", "[[241, 300]]"])
    def test_octets_outside_the_slash_24_host_range_are_rejected(self, monkeypatch, ranges):
        monkeypatch.setenv("DHCP_EXCLUSION_OCTET_RANGES", ranges)
        with pytest.raises(ValidationError, match="1..254"):
            SegmentLifecycleActivitySettings()

    def test_inverted_pair_is_rejected(self, monkeypatch):
        monkeypatch.setenv("DHCP_EXCLUSION_OCTET_RANGES", "[[10, 1]]")
        with pytest.raises(ValidationError, match="inverted"):
            SegmentLifecycleActivitySettings()

    @pytest.mark.parametrize("ranges", ["[[241, 254], [1, 10]]", "[[1, 10], [5, 20]]"])
    def test_descending_or_overlapping_pairs_are_rejected(self, monkeypatch, ranges):
        monkeypatch.setenv("DHCP_EXCLUSION_OCTET_RANGES", ranges)
        with pytest.raises(ValidationError, match="ascending"):
            SegmentLifecycleActivitySettings()

    def test_excluding_every_octet_is_rejected(self, monkeypatch):
        monkeypatch.setenv("DHCP_EXCLUSION_OCTET_RANGES", "[[1, 254]]")
        with pytest.raises(ValidationError, match="nothing left to distribute"):
            SegmentLifecycleActivitySettings()


class TestDay1ClustersRoot:

    def test_default_is_sites(self):
        assert SegmentLifecycleActivitySettings().day1_clusters_root == "sites"

    @pytest.mark.parametrize("root", ["/sites", "sites/", " sites"])
    def test_non_bare_paths_are_rejected(self, monkeypatch, root):
        monkeypatch.setenv("DAY1_CLUSTERS_ROOT", root)
        with pytest.raises(ValidationError, match="bare relative path"):
            SegmentLifecycleActivitySettings()
