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
    parse_mce_cluster,
    parse_seed,
    seed_folder_pattern,
    seed_lifecycle,
    seed_of,
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
        ("name", "flavor", "zone", "cpu"),
        [
            ("c200-kv32-az1", "c200", "az1", 32),
            ("c200n-kv8-az2", "c200n", "az2", 8),          # a variant letter is part of the flavor
            ("x201-kv1-az12", "x201", "az12", 1),
            ("c200-kv256-az1", "c200", "az1", 256),
        ],
    )
    def test_a_kubevirt_infraenv_names_its_flavor_zone_and_size(self, name, flavor, zone, cpu):
        parsed = parse_infraenv(name)
        assert (parsed.kind, parsed.flavor, parsed.zone, parsed.cpu) == (InfraEnvKind.KUBEVIRT, flavor, zone, cpu)
        assert parsed.memory_gib == cpu * MEMORY_GIB_PER_CPU
        assert is_kubevirt(name)

    def test_the_site_is_not_the_infraenvs_but_its_mces(self):
        assert not hasattr(parse_infraenv("c200-kv32-az1"), "site")

    @pytest.mark.parametrize("name", ["c200-mtl-az1", "c200n-mtl-az3"])
    def test_a_bare_metal_infraenv_has_no_size(self, name):
        parsed = parse_infraenv(name)
        assert parsed.kind is InfraEnvKind.METAL
        assert (parsed.cpu, parsed.memory_gib) == (None, None)
        assert not is_kubevirt(name)

    @pytest.mark.parametrize(
        "name",
        [
            "c200-kv32",                  # no zone
            "c200-kv32-a",                # the zone is `az<N>`
            "c200-kv32-az0",              # zones count from 1
            "c200-kv32-az01",             # leading zero
            "c200-kv0-az1",               # zero vCPUs
            "c200-kv08-az1",              # leading zero
            "c200-kv257-az1",             # over the bound
            "c200-kv-az1",                # no vCPUs
            "c200-kvm8-az1",              # not a number
            "c200-32-az1",                # the size is `kv<cpu>` or `mtl`
            "200c-kv32-az1",              # a flavor starts with a letter
            "C200-kv32-az1",              # uppercase
            "c200-kv32-five-az1",         # the convention's draft, with the site (before 2026-11-22)
            "c200-mtl-bat-yam-az1",
            "x201-zone-a-8",              # the convention before 2026-11-20
            "x201-zone-a-metal",
            "cisco-m6-bat-yam-64c-512gb",  # the one before that
            "c200-kv32-az1/../x",         # path characters
            "c200-kv32-a*1",
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


class TestMceLifecycle:
    def test_the_lifecycle_is_read_from_the_mce_name(self):
        parsed = parse_mce_cluster("ocp4-prep-mce-bat-yam-b")
        assert (parsed.lifecycle, parsed.site, parsed.id) == ("prep", "bat-yam", "b")


class TestClusterNames:
    def test_a_tenant_cluster_name_says_its_lifecycle_flavor_site_and_id(self):
        parsed = parse_cluster_name("ocp4-prod-eck-five-a")
        assert (parsed.lifecycle, parsed.flavor, parsed.site, parsed.id, parsed.zone) == ("prod", "eck", "five", "a", None)

    def test_a_kv_cluster_name_also_says_its_zone(self):
        parsed = parse_kv_cluster("ocp4-prod-kv-five-az1-a")
        assert (parsed.lifecycle, parsed.flavor, parsed.site, parsed.zone, parsed.id) == ("prod", "kv", "five", "az1", "a")
        assert parse_cluster_name("ocp4-prod-kv-five-az1-a") == parsed

    def test_a_site_of_several_fields_is_everything_between_the_flavor_and_the_zone(self):
        parsed = parse_kv_cluster("ocp4-prep-kv-bat-yam-az12-c")
        assert (parsed.site, parsed.zone, parsed.id) == ("bat-yam", "az12", "c")

    @pytest.mark.parametrize(
        "name",
        [
            "ocp4-prod-mce-five-a",        # an MCE is not a KV cluster
            "ocp4-prod-seed-five-az1-a",   # nor is a seed
            "ocp4-test-kubevirt-site4",    # day1's ordinary hosted cluster that only says kubevirt
            "ocp4-prod-kv-five-a",         # no zone: the shape before 2026-11-22
            "ocp4-prod-kv-five-az1",       # no id
            "ocp4-prod-kv-five-az1-ab",    # the id is one letter
            "ocp4-prod-kv-five-az0-a",     # zones count from 1
            "ocp4-prod-kv-five-az1-1",
            "prod-kv-five-az1-a",          # no ocp4-
            "ocp4-kv-five-az1-a",          # no lifecycle
            "ocp4-prod-kv-five-az1-a/../x",
        ],
    )
    def test_anything_else_is_not_a_kv_cluster(self, name):
        with pytest.raises(ClusterNameError) as raised:
            parse_kv_cluster(name)
        assert isinstance(raised.value, ValueError)

    @pytest.mark.parametrize("name", ["ocp4-prod-kv-five-a", "ocp4-prod-seed-five-a"])
    def test_a_seed_or_kv_cluster_without_its_zone_is_no_cluster_name(self, name):
        with pytest.raises(ClusterNameError):
            parse_cluster_name(name)

    def test_the_id_letters_sort_oldest_first(self):
        names = ["ocp4-prod-kv-five-az1-c", "ocp4-prod-kv-five-az1-a", "ocp4-prod-kv-five-az1-b"]
        assert sorted(names, key=lambda name: parse_kv_cluster(name).id) == [
            "ocp4-prod-kv-five-az1-a", "ocp4-prod-kv-five-az1-b", "ocp4-prod-kv-five-az1-c",
        ]


class TestSeedAndZone:
    @pytest.mark.parametrize(
        ("folder", "lifecycle", "site", "zone", "id"),
        [
            ("ocp4-prod-seed-five-az1-a", "prod", "five", "az1", "a"),
            ("ocp4-prep-seed-bat-yam-az12-b", "prep", "bat-yam", "az12", "b"),
        ],
    )
    def test_a_seed_folder_says_its_lifecycle_site_zone_and_id(self, folder, lifecycle, site, zone, id):
        seed = seed_of(folder)
        assert seed is not None and (seed.lifecycle, seed.site, seed.zone, seed.id) == (lifecycle, site, zone, id)
        assert (seed_zone(folder), seed_site(folder), seed_lifecycle(folder)) == (zone, site, lifecycle)
        assert parse_seed(folder) == seed

    @pytest.mark.parametrize(
        "folder",
        [
            "ocp4-prod-mce-five-a", "ocp4-mce-site2", "ocp4-prod-kv-five-az1-a",
            "ocp4-seed-five-1",            # the shape before 2026-11-22
            "ocp4-prod-seed-five-a",       # no zone
            "ocp4-prod-seed-five-az1",     # no id
            "ocp4-prod-seed-five-az0-a", "",
        ],
    )
    def test_any_other_folder_is_no_seed(self, folder):
        assert seed_of(folder) is None
        assert (seed_zone(folder), seed_site(folder), seed_lifecycle(folder)) == (None, None, None)

    def test_the_seed_folders_of_a_zone_are_named_by_a_pattern(self):
        assert seed_folder_pattern("prod", "five", "az1") == "ocp4-prod-seed-five-az1-*"

    @pytest.mark.parametrize("zone", ["a", "az", "az0", "az01", "AZ1", "zone1", "az1/..", ""])
    def test_a_zone_is_az_and_a_number(self, zone):
        with pytest.raises(MceNameError):
            zone_number(zone)

    @pytest.mark.parametrize("token", ["../x", "a/b", "a*", "A", "", "-a", "a--b"])
    def test_a_token_that_could_shape_a_path_is_refused(self, token):
        with pytest.raises(MceNameError):
            name_token(token, "site")
        with pytest.raises(MceNameError):
            seed_folder_pattern("prod", token, "az1")
        with pytest.raises(MceNameError):
            seed_folder_pattern(token, "five", "az1")
