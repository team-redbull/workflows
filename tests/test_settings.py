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

from shared.models.segment_lifecycle import SegmentType
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

    def test_pool_exceptions_sub_key_is_ignored(self):
        """Pinned by name, not just by the generic unknown-key case above.

        `pool-exceptions` is the Segments Manager's list of out-of-pool CIDRs it
        accepts at a site; it rides in the SAME shared topology, so this worker
        must drop it. Named explicitly because it is a NESTED LIST — the first
        sub-key that is not a string — and because a future tightening of
        _reject_bmc_typos into a broader guard would break the shared ConfigMap
        rather than just this worker.
        """
        s = build({"site1": dict(_SITE, pool="192.10.0.0/16",
                                 **{"pool-exceptions": ["172.20.4.0/22"]})})
        assert s.site_networks["site1"].dell_bmc == "10.50.0.0/16"
        assert not hasattr(s.site_networks["site1"], "pool_exceptions")

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
        monkeypatch.setenv(
            "DHCP_EXCLUSION_OCTET_RANGES", '{"HC": [[1, 10]], "BMC": [[1, 10]]}'
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



class TestSitesWithOpenConnectivity:
    """SITES_WITH_OPEN_CONNECTIVITY: the sites where no firewall stands between
    segments, so initialize-segment never involves the next service.

    Empty is legal and is the default posture; a name SITE_NETWORKS does not
    know is a typo, and a typo is the one failure this knob cannot survive
    quietly — the site would silently fall back to submitting open-rules
    requests and waiting forever for an approval nobody will give.
    """

    def test_empty_list_means_every_site_is_firewalled(self, monkeypatch):
        monkeypatch.setenv("SITES_WITH_OPEN_CONNECTIVITY", "[]")
        assert SegmentLifecycleActivitySettings().sites_with_open_connectivity == []

    def test_listed_sites_are_kept(self, monkeypatch):
        monkeypatch.setenv("SITE_NETWORKS", json.dumps({"site-a": _SITE, "site-b": _SITE}))
        monkeypatch.setenv("SITES_WITH_OPEN_CONNECTIVITY", '["site-b"]')
        assert SegmentLifecycleActivitySettings().sites_with_open_connectivity == ["site-b"]

    def test_a_site_missing_from_site_networks_is_rejected(self, monkeypatch):
        monkeypatch.setenv("SITES_WITH_OPEN_CONNECTIVITY", '["site-typo"]')
        with pytest.raises(ValidationError, match="unknown site"):
            SegmentLifecycleActivitySettings()

    def test_duplicates_are_rejected(self, monkeypatch):
        monkeypatch.setenv("SITES_WITH_OPEN_CONNECTIVITY", '["site-a", "site-a"]')
        with pytest.raises(ValidationError, match="duplicate site"):
            SegmentLifecycleActivitySettings()

    def test_an_empty_site_name_is_rejected(self, monkeypatch):
        monkeypatch.setenv("SITES_WITH_OPEN_CONNECTIVITY", '[""]')
        with pytest.raises(ValidationError, match="empty site name"):
            SegmentLifecycleActivitySettings()

    def test_the_key_is_required(self, monkeypatch):
        # No code default: a missing (or misspelt) ConfigMap key must crash the
        # worker at startup rather than read as "every site is firewalled".
        monkeypatch.delenv("SITES_WITH_OPEN_CONNECTIVITY")
        with pytest.raises(ValidationError, match="sites_with_open_connectivity"):
            SegmentLifecycleActivitySettings()
