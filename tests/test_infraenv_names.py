"""The naming convention, parsed once at the API edge: InfraEnv names, and the MCE, seed and
KubeVirt-cluster names the site and zone build (shared/infraenv_names.py, shared/cluster_names.py)."""

from __future__ import annotations

import pytest

from shared.cluster_names import (
    MceNameError,
    is_kv_cluster_of,
    kv_cluster_prefix,
    name_token,
    seed_cluster,
    site_of_mce_cluster,
)
from shared.infraenv_names import (
    MEMORY_GIB_PER_CPU,
    InfraEnvKind,
    InfraEnvNameError,
    is_kubevirt,
    parse_infraenv,
    vm_name,
)


class TestParseInfraEnv:
    @pytest.mark.parametrize(
        ("name", "conf", "opt", "zone", "cpu"),
        [
            ("x201-zone-a-8", "x201", None, "a", 8),
            ("x201-r-zone-b-64", "x201", "r", "b", 64),
            ("dell-zone-c1-2", "dell", None, "c1", 2),
            ("x201-zone-a-256", "x201", None, "a", 256),
        ],
    )
    def test_a_kubevirt_infraenv_names_its_zone_and_size(self, name, conf, opt, zone, cpu):
        parsed = parse_infraenv(name)
        assert (parsed.kind, parsed.server_conf, parsed.option, parsed.zone, parsed.cpu) == (
            InfraEnvKind.KUBEVIRT, conf, opt, zone, cpu,
        )
        assert parsed.memory_gib == cpu * MEMORY_GIB_PER_CPU
        assert is_kubevirt(name)

    @pytest.mark.parametrize("name", ["x201-zone-a-metal", "x201-r-zone-b-metal"])
    def test_a_metal_infraenv_has_no_size(self, name):
        parsed = parse_infraenv(name)
        assert parsed.kind is InfraEnvKind.METAL
        assert (parsed.cpu, parsed.memory_gib) == (None, None)
        assert not is_kubevirt(name)

    @pytest.mark.parametrize(
        "name",
        [
            "x201-zone-a",            # no tail
            "x201-zone-a-0",          # zero vCPUs
            "x201-zone-a-08",         # leading zero
            "x201-zone-a-257",        # over the bound
            "x201-zone-a-8x",         # not a number
            "x201-zone--8",           # empty zone
            "x201-rr-zone-a-8",       # the option is one letter
            "X201-zone-a-8",          # uppercase
            "cisco-m6-bat-yam-64c-512gb",  # the old convention
            "x201-zone-a-8/../x",     # path characters
            "",
        ],
    )
    def test_anything_else_is_refused_as_a_value_error(self, name):
        with pytest.raises(InfraEnvNameError) as raised:
            parse_infraenv(name)
        assert isinstance(raised.value, ValueError)


class TestVmName:
    def test_the_vm_is_named_after_the_infraenv_and_the_suffix(self):
        assert vm_name("x201-zone-a-8", "0a1b2c") == "ocp-x201-zone-a-8-0a1b2c"

    @pytest.mark.parametrize("suffix", ["0A1B2C", "0a1b2", "0a1b2cd", "zzzzzz", ""])
    def test_the_suffix_is_six_lowercase_hex_digits(self, suffix):
        with pytest.raises(InfraEnvNameError):
            vm_name("x201-zone-a-8", suffix)

    def test_a_name_too_long_for_kubernetes_is_refused(self):
        with pytest.raises(InfraEnvNameError, match="DNS-1123"):
            vm_name("x" * 50 + "-zone-a-8", "0a1b2c")


class TestMceAndSeedNames:
    @pytest.mark.parametrize(
        ("mce", "site"),
        [("ocp4-prod-mce-bat-yam-a", "bat-yam"), ("ocp4-lab-mce-lab-a", "lab"), ("ocp4-prod-mce-x1-z", "x1")],
    )
    def test_the_site_is_read_from_the_mce_name(self, mce, site):
        assert site_of_mce_cluster(mce) == site

    @pytest.mark.parametrize("mce", ["prod-mce-bat-yam-a", "ocp4-mce-bat-yam-01", "ocp4-prod-mce-bat-yam", "ocp4-prod-mce-bat-yam-A"])
    def test_an_mce_outside_the_convention_has_no_site(self, mce):
        with pytest.raises(MceNameError) as raised:
            site_of_mce_cluster(mce)
        assert isinstance(raised.value, ValueError)

    def test_the_seed_and_the_kv_prefix_are_built_from_site_and_zone(self):
        assert seed_cluster("bat-yam", "a") == "ocp4-seed-bat-yam-a"
        assert kv_cluster_prefix("bat-yam", "a") == "ocp4-kubevirt-bat-yam-a"

    @pytest.mark.parametrize("token", ["../x", "a/b", "a*", "A", "", "-a", "a--b"])
    def test_a_token_that_could_shape_a_path_is_refused(self, token):
        with pytest.raises(MceNameError):
            name_token(token, "site")
        with pytest.raises(MceNameError):
            seed_cluster(token, "a")


class TestKvClusterOfZone:
    @pytest.mark.parametrize(
        ("name", "yes"),
        [
            ("ocp4-kubevirt-bat-yam-a", True),
            ("ocp4-kubevirt-bat-yam-a1", True),
            ("ocp4-kubevirt-bat-yam-a-1", True),
            ("ocp4-kubevirt-bat-yam-ab", False),   # zone `ab`, not `a`
            ("ocp4-kubevirt-bat-yam-b1", False),
            ("ocp4-kubevirt-bat-yama-1", False),   # site `bat-yama`
            ("ocp4-seed-bat-yam-a", False),
            ("ocp4-prod-bat-yam-a1", False),
            ("ocp4-kubevirt-bat-yam-a*", False),
        ],
    )
    def test_the_prefix_match_stops_at_the_zone(self, name, yes):
        assert is_kv_cluster_of(name, "bat-yam", "a") is yes
