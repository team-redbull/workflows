"""SITE_NETWORKS parsing in SegmentLifecycleActivitySettings.

SITE_NETWORKS is the shared site topology: the same JSON is rendered into this
service's ConfigMap and the Segments Manager's, from one definition in
redbull-platform. These tests pin the two properties that arrangement depends
on — unknown sub-keys never break us, and a missing/typo'd `dell-bmc` /
`cisco-bmc` fails at startup rather than mid-workflow.

A site carries one BMC network per server hardware vendor it HOSTS: both, or
Dell-only, or Cisco-only. At least one is required — a site with neither is a
config gap, and an MCE there would open no BMC rules at all and look healthy.
A key that only LOOKS like a BMC key is rejected too: with single-vendor sites
legitimate, `dell-bcm` beside a valid `cisco-bmc` would otherwise be
indistinguishable from a real Cisco-only site.
"""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from shared.settings import SegmentLifecycleActivitySettings


# A minimally valid site: both vendor BMC networks, as the ConfigMap spells them.
_SITE = {"dell-bmc": "10.50.0.0/16", "cisco-bmc": "10.60.0.0/16"}


def build(site_networks: dict) -> SegmentLifecycleActivitySettings:
    """Construct settings with everything but SITE_NETWORKS taken from conftest env."""
    return SegmentLifecycleActivitySettings(site_networks=site_networks)


class TestSharedTopologyContract:

    def test_pool_sub_key_is_ignored(self):
        """`pool` belongs to the Segments Manager. We must parse it and move on."""
        s = build({"site1": dict(_SITE, pool="192.10.0.0/16")})
        assert s.site_networks["site1"].dell_bmc == "10.50.0.0/16"
        assert s.site_networks["site1"].cisco_bmc == "10.60.0.0/16"
        assert not hasattr(s.site_networks["site1"], "pool")

    def test_unknown_sub_key_is_ignored(self):
        """A future consumer adding a sub-key must not crash this worker."""
        s = build({"site1": dict(_SITE, invented_later={"a": 1})})
        assert s.site_networks["site1"].dell_bmc == "10.50.0.0/16"

    def test_parses_from_a_json_string(self):
        """This is how the ConfigMap actually delivers it — hyphenated keys and
        all, which is why the model aliases them."""
        raw = json.dumps({"site1": dict(_SITE, pool="192.10.0.0/16")})
        s = SegmentLifecycleActivitySettings(site_networks=json.loads(raw))
        assert s.site_networks["site1"].dell_bmc == "10.50.0.0/16"
        assert s.site_networks["site1"].cisco_bmc == "10.60.0.0/16"

    @pytest.mark.parametrize("vendor", ["dell-bmc", "cisco-bmc"])
    def test_a_single_vendor_site_is_accepted(self, vendor):
        """Sites exist with only Dell or only Cisco hardware. The other
        vendor's field is None, and pairs() will fan out over the one."""
        s = build({"site1": {"pool": "192.10.0.0/16", vendor: "10.50.0.0/16"}})
        site = s.site_networks["site1"]
        present, absent = (
            (site.dell_bmc, site.cisco_bmc)
            if vendor == "dell-bmc"
            else (site.cisco_bmc, site.dell_bmc)
        )
        assert present == "10.50.0.0/16"
        assert absent is None


class TestFailFast:

    def test_missing_both_bmc_keys_is_rejected(self):
        """One vendor is a site shape; none is a config gap."""
        with pytest.raises(ValidationError, match="at least one"):
            build({"site1": {"pool": "192.10.0.0/16"}})

    def test_typo_in_a_bmc_key_is_rejected(self):
        """What replaces both-required as the typo guard. Without it, this
        would parse as a legitimate Cisco-only site and silently halve an
        MCE's BMC connectivity."""
        with pytest.raises(ValidationError, match="dell-bcm"):
            build({"site1": {"dell-bcm": "10.50.0.0/16", "cisco-bmc": "10.60.0.0/16"}})

    @pytest.mark.parametrize("typo", ["dellbmc", "ciscobmc", "DELL-BMC", "bmc-dell"])
    def test_other_bmc_lookalike_keys_are_rejected(self, typo):
        """The guard keys off a BMC/vendor token anywhere in the name, not off
        an exact expected spelling."""
        with pytest.raises(ValidationError, match="unrecognised BMC key"):
            build({"site1": dict(_SITE, **{typo: "10.50.0.0/16"})})

    def test_legacy_single_bmc_key_is_rejected(self):
        """A ConfigMap still on the pre-split shape must crash-loop the worker,
        not start it with no BMC connectivity."""
        with pytest.raises(ValidationError, match="bmc"):
            build({"site1": {"pool": "192.10.0.0/16", "bmc": "10.50.0.0/16"}})

    @pytest.mark.parametrize("key", ["dell-bmc", "cisco-bmc"])
    @pytest.mark.parametrize("bmc", ["not-a-cidr", "10.50.0.1/16", "10.50.0.0/33"])
    def test_invalid_bmc_cidr_is_rejected(self, key, bmc):
        with pytest.raises(ValidationError):
            build({"site1": dict(_SITE, **{key: bmc})})

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
