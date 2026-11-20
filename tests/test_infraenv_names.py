"""The platform naming convention, parsed once at the API edge: InfraEnv and VM names, and
the MCE, seed, zone and KV-cluster names (shared/infraenv_names.py, shared/cluster_names.py)."""

from __future__ import annotations

import pytest

from shared.cluster_names import (
    ClusterNameError,
    MceNameError,
    name_token,
    parse_cluster_name,
    parse_kv_cluster,
    seed_cluster,
    seed_site,
    seed_zone,
    site_of_mce_cluster,
    zone_number,
)
from shared.infraenv_names import (
    MEMORY_GIB_PER_CPU,
    InfraEnvKind,
    InfraEnvNameError,
    is_kubevirt,
    parse_infraenv,
    vm_name,
    vm_size,
)


class TestParseInfraEnv:
    @pytest.mark.parametrize(
        ("name", "flavor", "site", "zone", "cpu"),
        [
            ("c200-kv32-five-az1", "c200", "five", "az1", 32),
            ("c200n-kv8-five-az2", "c200n", "five", "az2", 8),          # a variant letter is part of the flavor
            ("c200-kv64-bat-yam-az1", "c200", "bat-yam", "az1", 64),    # a site of several fields
            ("x201-kv1-site2-az12", "x201", "site2", "az12", 1),
            ("c200-kv256-five-az1", "c200", "five", "az1", 256),
            ("c200-kv32-kv8-az1", "c200", "kv8", "az1", 32),            # a site that looks like a size is still the site
        ],
    )
    def test_a_kubevirt_infraenv_names_its_flavor_site_zone_and_size(self, name, flavor, site, zone, cpu):
        parsed = parse_infraenv(name)
        assert (parsed.kind, parsed.flavor, parsed.site, parsed.zone, parsed.cpu) == (
            InfraEnvKind.KUBEVIRT, flavor, site, zone, cpu,
        )
        assert parsed.memory_gib == cpu * MEMORY_GIB_PER_CPU
        assert is_kubevirt(name)

    @pytest.mark.parametrize("name", ["c200-mtl-five-az1", "c200n-mtl-bat-yam-az3"])
    def test_a_bare_metal_infraenv_has_no_size(self, name):
        parsed = parse_infraenv(name)
        assert parsed.kind is InfraEnvKind.METAL
        assert (parsed.cpu, parsed.memory_gib) == (None, None)
        assert not is_kubevirt(name)

    @pytest.mark.parametrize(
        "name",
        [
            "c200-kv32-five",             # no zone
            "c200-kv32-five-a",           # the zone is `az<N>`
            "c200-kv32-five-az0",         # zones count from 1
            "c200-kv32-five-az01",        # leading zero
            "c200-kv0-five-az1",          # zero vCPUs
            "c200-kv08-five-az1",         # leading zero
            "c200-kv257-five-az1",        # over the bound
            "c200-kv-five-az1",           # no vCPUs
            "c200-kvm8-five-az1",         # not a number
            "c200-32-five-az1",           # the size is `kv<cpu>` or `mtl`
            "c200-kv32--az1",             # empty site
            "200c-kv32-five-az1",         # a flavor starts with a letter
            "C200-kv32-five-az1",         # uppercase
            "c200-kv32-Five-az1",
            "x201-zone-a-8",              # the convention before 2026-11-20
            "x201-zone-a-metal",
            "cisco-m6-bat-yam-64c-512gb",  # the one before that
            "c200-kv32-five-az1/../x",    # path characters
            "c200-kv32-fi*e-az1",
            "",
        ],
    )
    def test_anything_else_is_refused_as_a_value_error(self, name):
        with pytest.raises(InfraEnvNameError) as raised:
            parse_infraenv(name)
        assert isinstance(raised.value, ValueError)


class TestVmName:
    def test_the_vm_is_named_after_its_flavor_size_site_and_whole_mac(self):
        assert vm_name("c200", 32, "five", "0e:03:f2:a1:b2:c3") == "ocp-c200-kv32-five-0e03f2a1b2c3"
        assert vm_name("c200n", 8, "bat-yam", "0E:00:02:5D:0C:9E") == "ocp-c200n-kv8-bat-yam-0e00025d0c9e"

    def test_the_zone_is_not_in_the_name(self):
        assert "az" not in vm_name("c200", 32, "five", "0e:03:f2:a1:b2:c3")

    def test_the_size_field_is_kv_and_the_vcpus(self):
        assert vm_size(32) == "kv32"

    @pytest.mark.parametrize("mac", ["0e03f2a1b2c3", "0e:03:f2:a1:b2", "0e:03:f2:a1:b2:c3:d4", "zz:03:f2:a1:b2:c3", ""])
    def test_the_mac_is_six_octets_with_colons(self, mac):
        with pytest.raises(InfraEnvNameError):
            vm_name("c200", 32, "five", mac)

    @pytest.mark.parametrize(("flavor", "cpu", "site"), [("C200", 32, "five"), ("c200", 0, "five"), ("c200", 32, "fi/ve"), ("c-200", 32, "five")])
    def test_a_field_outside_the_convention_is_refused(self, flavor, cpu, site):
        with pytest.raises(InfraEnvNameError):
            vm_name(flavor, cpu, site, "0e:03:f2:a1:b2:c3")

    def test_a_name_too_long_for_kubernetes_is_refused(self):
        # 63 characters must also hold the root DataVolume's `<name>-root`.
        with pytest.raises(InfraEnvNameError, match="DNS-1123"):
            vm_name("c" * 30, 32, "s" * 20, "0e:03:f2:a1:b2:c3")
        assert len(vm_name("c" * 20, 32, "s" * 15, "0e:03:f2:a1:b2:c3")) <= 59


class TestMceNames:
    @pytest.mark.parametrize(
        ("mce", "site"),
        [("ocp4-prod-mce-bat-yam-a", "bat-yam"), ("ocp4-lab-mce-lab-a", "lab"), ("ocp4-prod-mce-five-z", "five")],
    )
    def test_the_site_is_read_from_the_mce_name(self, mce, site):
        assert site_of_mce_cluster(mce) == site

    @pytest.mark.parametrize("mce", ["prod-mce-bat-yam-a", "ocp4-mce-bat-yam-01", "ocp4-prod-mce-bat-yam", "ocp4-prod-mce-bat-yam-A"])
    def test_an_mce_outside_the_convention_has_no_site(self, mce):
        with pytest.raises(MceNameError) as raised:
            site_of_mce_cluster(mce)
        assert isinstance(raised.value, ValueError)


class TestClusterNames:
    def test_a_cluster_name_says_its_lifecycle_flavor_site_and_id(self):
        parsed = parse_cluster_name("ocp4-prod-kv-five-a")
        assert (parsed.lifecycle, parsed.flavor, parsed.site, parsed.id) == ("prod", "kv", "five", "a")

    def test_a_site_of_several_fields_is_everything_between_the_flavor_and_the_id(self):
        parsed = parse_kv_cluster("ocp4-prep-kv-bat-yam-c")
        assert (parsed.site, parsed.id) == ("bat-yam", "c")

    @pytest.mark.parametrize(
        "name",
        [
            "ocp4-prod-mce-five-a",        # an MCE is not a KV cluster
            "ocp4-test-kubevirt-site4",    # day1's ordinary hosted cluster that only says kubevirt
            "ocp4-prod-kv-five",           # no id
            "ocp4-prod-kv-five-ab",        # the id is one letter
            "ocp4-prod-kv-five-1",
            "prod-kv-five-a",              # no ocp4-
            "ocp4-kv-five-a",              # no lifecycle: `kv` would be the lifecycle and `five` the flavor
            "ocp4-prod-kv-five-a/../x",
        ],
    )
    def test_anything_else_is_not_a_kv_cluster(self, name):
        with pytest.raises(ClusterNameError) as raised:
            parse_kv_cluster(name)
        assert isinstance(raised.value, ValueError)

    def test_the_id_letters_sort_oldest_first(self):
        names = ["ocp4-prod-kv-five-c", "ocp4-prod-kv-five-a", "ocp4-prod-kv-five-b"]
        assert sorted(names, key=lambda name: parse_kv_cluster(name).id) == [
            "ocp4-prod-kv-five-a", "ocp4-prod-kv-five-b", "ocp4-prod-kv-five-c",
        ]


class TestSeedAndZone:
    def test_the_seed_carries_the_zone_number(self):
        assert seed_cluster("five", "az1") == "ocp4-seed-five-1"
        assert seed_cluster("bat-yam", "az12") == "ocp4-seed-bat-yam-12"

    def test_the_seed_folder_gives_the_zone_and_the_site_back(self):
        assert seed_zone("ocp4-seed-five-1") == "az1"
        assert seed_site("ocp4-seed-five-1") == "five"
        assert seed_zone("ocp4-seed-bat-yam-12") == "az12"
        assert seed_site("ocp4-seed-bat-yam-12") == "bat-yam"

    @pytest.mark.parametrize("site,zone", [("five", "az1"), ("bat-yam", "az3"), ("site2", "az10")])
    def test_the_two_are_inverse(self, site, zone):
        assert seed_zone(seed_cluster(site, zone)) == zone
        assert seed_site(seed_cluster(site, zone)) == site

    @pytest.mark.parametrize(
        "folder", ["ocp4-prod-mce-five-a", "ocp4-mce-site2", "ocp4-seed-five", "ocp4-seed-five-0", "ocp4-seed-five-a", "ocp4-seed-five-01", ""]
    )
    def test_any_other_folder_is_no_seed(self, folder):
        assert seed_zone(folder) is None
        assert seed_site(folder) is None

    @pytest.mark.parametrize("zone", ["a", "az", "az0", "az01", "AZ1", "zone1", "az1/..", ""])
    def test_a_zone_is_az_and_a_number(self, zone):
        with pytest.raises(MceNameError):
            zone_number(zone)
        with pytest.raises(MceNameError):
            seed_cluster("five", zone)

    @pytest.mark.parametrize("token", ["../x", "a/b", "a*", "A", "", "-a", "a--b"])
    def test_a_token_that_could_shape_a_path_is_refused(self, token):
        with pytest.raises(MceNameError):
            name_token(token, "site")
        with pytest.raises(MceNameError):
            seed_cluster(token, "az1")
