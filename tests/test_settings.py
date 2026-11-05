"""Fail-fast parsing in SegmentLifecycleActivitySettings: DHCP_EXCLUSION_OCTET_RANGES,
the DAY1_REPO_URL scheme and the DHCP scope API keys.

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


class TestDhcpApiKeys:
    """release-segment's DHCP scope API: both keys are required, so a chart
    that forgot either crash-loops the worker at startup instead of failing
    every release once a cluster is decommissioned."""

    @pytest.mark.parametrize("key", ["DHCP_API_URL", "DHCP_API_TOKEN"])
    def test_a_missing_key_is_rejected_at_startup(self, monkeypatch, key):
        monkeypatch.delenv(key)
        with pytest.raises(ValidationError, match=key.lower()):
            SegmentLifecycleActivitySettings()


class TestClusterUpgradeActivitySettings:
    """The per-MCE cluster-upgrade limb: its MCE, and where Argo CD keeps the hosted clusters' Applications."""

    def test_the_defaults_are_the_hcappset_s(self):
        from shared.settings import ClusterUpgradeActivitySettings

        settings = ClusterUpgradeActivitySettings()
        assert (settings.mce_cluster, settings.argo_namespace, settings.argo_values_ref) == (
            "ocp4-mce-test", "openshift-gitops", "values")

    def test_both_argo_keys_are_read_from_the_environment(self, monkeypatch):
        from shared.settings import ClusterUpgradeActivitySettings

        monkeypatch.setenv("ARGO_NAMESPACE", "gitops-hc")
        monkeypatch.setenv("ARGO_VALUES_REF", "day1")
        settings = ClusterUpgradeActivitySettings()
        assert (settings.argo_namespace, settings.argo_values_ref) == ("gitops-hc", "day1")

    @pytest.mark.parametrize("key, value", [
        ("ARGO_NAMESPACE", "Openshift-GitOps"), ("ARGO_NAMESPACE", ""), ("ARGO_NAMESPACE", "a" * 64),
        ("ARGO_VALUES_REF", "$values"), ("ARGO_VALUES_REF", ""),
    ])
    def test_a_value_argo_could_not_mean_is_rejected_at_startup(self, monkeypatch, key, value):
        from shared.settings import ClusterUpgradeActivitySettings

        monkeypatch.setenv(key, value)
        with pytest.raises(ValidationError, match=key.lower()):
            ClusterUpgradeActivitySettings()


class TestVmLifecycleSettings:
    """The vm-lifecycle limb's knobs fail at worker start, never on the first run."""

    def _settings(self, monkeypatch, **env):
        from shared.settings import VmLifecycleActivitySettings

        for key, value in env.items():
            monkeypatch.setenv(key, value)
        return VmLifecycleActivitySettings()

    def test_the_url_templates_render_every_placeholder(self, monkeypatch):
        from shared.models.vm_lifecycle import KvClusterRef

        s = self._settings(
            monkeypatch,
            KV_CAPACITY_URL_TEMPLATE="https://kv-capacity.apps.{hosted_cluster}.{site}-{zone}.example/",
            KV_VM_API_URL_TEMPLATE="http://{name}.example",
        )
        ref = KvClusterRef(name="ocp4-kubevirt-s-a1", cluster_name="kubevirt-s-a1", hosted_cluster="openshift-kubevirt-s-a1",
                           site="s", zone="a")
        assert s.kv_capacity_url(ref) == "https://kv-capacity.apps.openshift-kubevirt-s-a1.s-a.example"
        assert s.kv_vm_api_url(ref) == "http://ocp4-kubevirt-s-a1.example"

    @pytest.mark.parametrize(
        "template",
        ["https://kv.{clustr}.example", "https://kv.{}.example", "kv-capacity.apps.{cluster}.example", "https://kv.{cluster.name}.x",
         "https://kv.{0}.example"],
    )
    def test_a_template_with_an_unknown_placeholder_or_no_scheme_is_refused(self, monkeypatch, template):
        with pytest.raises(ValidationError):
            self._settings(monkeypatch, KV_CAPACITY_URL_TEMPLATE=template)

    @pytest.mark.parametrize("value", ["-1", "91", "x"])
    def test_the_buffer_is_a_percentage_up_to_90(self, monkeypatch, value):
        with pytest.raises(ValidationError):
            self._settings(monkeypatch, HEADROOM_BUFFER_PERCENT=value)
        assert self._settings(monkeypatch, HEADROOM_BUFFER_PERCENT="0").headroom_buffer_percent == 0

    @pytest.mark.parametrize("prefix", ["00:a1:b2", "01:a1:b2", "03:a1:b2", "02:a1", "02-a1-b2", "zz:a1:b2", "02:a1:b2:00"])
    def test_the_mac_prefix_is_three_locally_administered_unicast_octets(self, monkeypatch, prefix):
        with pytest.raises(ValidationError):
            self._settings(monkeypatch, VM_MAC_PREFIX=prefix)

    @pytest.mark.parametrize("prefix", ["02:A1:B2", " 0a:00:00 ", "fe:00:01"])
    def test_a_valid_prefix_is_lowercased(self, monkeypatch, prefix):
        assert self._settings(monkeypatch, VM_MAC_PREFIX=prefix).vm_mac_prefix == prefix.strip().lower()

    def test_the_day1_url_must_be_https(self, monkeypatch):
        with pytest.raises(ValidationError, match="https://"):
            self._settings(monkeypatch, DAY1_REPO_URL="git@x:y.git")
