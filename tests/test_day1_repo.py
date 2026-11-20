"""The monorepo's day1 reader (activities/day1_repo.py), shared by cluster-lifecycle,
upgrade-gitops and server-lifecycle and owned by none: `Day1`'s questions on a fixture tree
shaped like the real repo, a cluster file read with its value layers the way the
hostedclusters-setup chart renders it, and the one-object guarantee that every limb reads
day1 through it. Pure file reads but for the last class, which opens `main_snapshot()` on
a local bare repo; the snapshot itself is tests/test_git_snapshot.py's."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from activities import day1_repo
from activities.cluster_lifecycle import cluster_values, day1_writer
from activities.day1_repo import Day1
from activities.upgrade_gitops import day1_repo as upgrade_day1_repo
from activities.upgrade_gitops import day1_values
from shared.cluster_names import MceNameError

CLUSTER = "sites/s/mces/ocp4-seed-s-a/hostedClusters/ocp4-kubevirt-s-a-1.yaml"
REVISION = "0123456789abcdef0123456789abcdef01234567"


def _write(root: Path, files: dict[str, str]) -> None:
    for relative, content in files.items():
        (root / relative).parent.mkdir(parents=True, exist_ok=True)
        (root / relative).write_text(content)


def test_every_limb_reads_day1_through_the_one_shared_reader():
    """No limb imports another's domain for day1: each takes the shared module's objects."""
    assert upgrade_day1_repo.fresh_main is day1_repo.fresh_main
    assert upgrade_day1_repo.main_snapshot is day1_repo.main_snapshot
    assert day1_writer.main_snapshot is day1_repo.main_snapshot
    assert day1_values.load_cluster_values is day1_repo.load_cluster_values
    assert day1_values.load_yaml is day1_repo.load_yaml
    assert cluster_values.cluster_env is day1_repo.cluster_env
    for module in (upgrade_day1_repo, day1_values):
        assert "cluster_lifecycle" not in Path(module.__file__).read_text()
    assert "upgrade_gitops" not in Path(day1_writer.__file__).read_text()
    assert "upgrade_gitops" not in Path(day1_repo.__file__).read_text().split('"""', 2)[2]


def test_list_cluster_files_never_globs_with_its_input(tmp_path):
    folder = tmp_path / "sites" / "s" / "mces" / "ocp4-seed-s-a" / "hostedClusters"
    folder.mkdir(parents=True)
    (folder / "ocp4-kubevirt-s-a-1.yaml").write_text("x: 1\n")
    (folder / "ocp4-kubevirt-s-a-2.yml").write_text("x: 1\n")  # .yml is not a cluster file (shared/upgrade_layout.py)
    assert day1_repo.list_cluster_files(tmp_path, "s", "ocp4-seed-s-a") == [
        "sites/s/mces/ocp4-seed-s-a/hostedClusters/ocp4-kubevirt-s-a-1.yaml"
    ]
    assert day1_repo.list_cluster_files(tmp_path, "s", "ocp4-seed-s-*") == []
    assert day1_repo.list_cluster_files(tmp_path, "s", "missing") == []


def test_layers_merge_as_helm_does_and_the_file_name_gives_the_cluster_name(tmp_path):
    _write(tmp_path, {
        "sites/configValues.yaml": "dhcp_values:\n  leaseDurationDays: 8\n  domain: a\nmastertag: 4.17.0-x86_64\n",
        "sites/s/values.yaml": "dhcp_values:\n  domain: b\nextra: [1, 2]\n",
        "sites/s/mces/ocp4-seed-s-a/values.yaml": "mastertag: 4.18.56-x86_64\nextra: [3]\n",
        CLUSTER: "platform: agent\nextra: null\n",
    })
    values = day1_repo.load_cluster_values(tmp_path, CLUSTER)
    assert values.cluster_name == "kubevirt-s-a-1"
    assert values.hosted_cluster == "openshift-kubevirt-s-a-1"
    assert values.hosted_cluster_namespace == "hcp-kubevirt-s-a-1"
    assert values.own == {"platform": "agent", "extra": None}
    # Maps merge deep, a scalar is replaced by the later layer, a list is replaced whole, a null deletes.
    assert values.merged == {
        "dhcp_values": {"leaseDurationDays": 8, "domain": "b"},
        "mastertag": "4.18.56-x86_64",
        "platform": "agent",
    }
    assert values.problems == []


def test_the_files_own_cluster_name_and_config_values_prefix_outrank_the_file_name(tmp_path):
    _write(tmp_path, {"sites/configValues.yaml": "clusterFilePrefix: ocp4-kubevirt-\n", CLUSTER: "platform: agent\n"})
    assert day1_repo.load_cluster_values(tmp_path, CLUSTER).cluster_name == "s-a-1"
    _write(tmp_path, {CLUSTER: "clusterName: named\n"})
    assert day1_repo.load_cluster_values(tmp_path, CLUSTER).cluster_name == "named"
    # An EMPTY clusterName is unset, as Sprig's `default` says.
    _write(tmp_path, {CLUSTER: "clusterName: ''\n"})
    assert day1_repo.load_cluster_values(tmp_path, CLUSTER).cluster_name == "s-a-1"


def test_a_missing_layer_is_skipped_and_what_the_chart_would_refuse_is_a_problem(tmp_path):
    _write(tmp_path, {"sites/s/values.yaml": "- not\n- a map\n", CLUSTER: "platform: agent\nplatform: agent\n"})
    values = day1_repo.load_cluster_values(tmp_path, CLUSTER)
    assert values.cluster_name == "kubevirt-s-a-1" and values.merged == {"platform": "agent"}
    assert [p.split(":")[0] for p in values.problems] == ["sites/s/values.yaml", CLUSTER]
    assert "must be a mapping" in values.problems[0] and "written twice" in values.problems[1]


def test_text_stands_in_for_the_file_and_a_non_cluster_path_is_refused(tmp_path):
    values = day1_repo.load_cluster_values(tmp_path, CLUSTER, text="clusterName: from-text\n")
    assert values.cluster_name == "from-text" and values.own == {"clusterName": "from-text"}
    with pytest.raises(ValueError, match="not a day1 cluster file"):
        day1_repo.layer_paths("sites/s/values.yaml")


# --- the layout: what a writer builds its path with ------------------------------------------


def test_the_path_helpers_spell_the_layout_once():
    folder = day1_repo.mce_dir("site2", "ocp4-prep-mce-site2-a")
    assert folder == "sites/site2/mces/ocp4-prep-mce-site2-a"
    path = day1_repo.cluster_file_path(folder, "ocp4-prep-e2e-site2-a")
    assert path == "sites/site2/mces/ocp4-prep-mce-site2-a/hostedClusters/ocp4-prep-e2e-site2-a.yaml"
    assert day1_repo.CLUSTER_FILE.fullmatch(path).group("name") == "ocp4-prep-e2e-site2-a"
    assert day1_repo.parse_mce_dir(folder) == ("site2", "ocp4-prep-mce-site2-a")
    for not_a_folder in ("sites/site2", "sites/site2/mces/a/b", "sites/../mces/a", "sites/site2/mces/..", "x/site2/mces/a", ""):
        assert day1_repo.parse_mce_dir(not_a_folder) is None


@pytest.mark.parametrize(("merged", "stem", "env"), [
    ({"day2": {"env": "prep"}}, "ocp4-prod-iac-site2-a", "prep"),  # day2.env outranks the name
    ({"day2": {"copy_to_sigs": ["x"]}}, "ocp4-prod-iac-site2-a", "prod"),
    ({}, "ocp4-test-kubevirt-site4", "test"),
    ({"day2": {"env": ""}}, "ocp4-prod-iac-site2-a", "prod"),  # empty is unset
    ({"day2": {"env": 7}}, "ocp4-prod-iac-site2-a", "prod"),  # not a string: provision-cluster reports it, the name answers
    ({"day2": "nonsense"}, "ocp4-prod-iac-site2-a", "prod"),
    ({}, "ocp4-single", None),  # no second field: no environment in the name
])
def test_the_environment_is_day2_env_else_the_names_first_field(merged, stem, env):
    assert day1_repo.cluster_env(merged, stem) == env


# --- Day1: the questions, on a tree shaped like the real repo -----------------------------------


def _nodepools(*zones: str | None) -> str:
    """A cluster file whose NodePools label their nodes with these zones (None: no zone label)."""
    lines = ["nodepool:"]
    for index, zone in enumerate(zones):
        lines.append(f"  - name: pool-{index}")
        lines.append("    nodeLabels:")
        lines.append("      role: worker")
        if zone is not None:
            lines.append(f'      topology.kubernetes.io/zone: "{zone}"')
    return "\n".join(lines) + "\n"


def _hc(site: str, folder: str, name: str) -> str:
    return f"sites/{site}/mces/{folder}/hostedClusters/{name}.yaml"


# Two sites (one with a dash), MCE folders of both conventions, seeds for two zones
# and two sites, and every way a file under a seed can disagree with it.
TREE = {
    "sites/configValues.yaml": "mastertag: 4.18.56\n",
    # --- site five: two MCEs (one legacy), seeds for az1 and az2
    "sites/five/values.yaml": "day2:\n  copy_to_sigs: [net]\n",
    "sites/five/mces/ocp4-prod-mce-five-a/values.yaml": "mceName: ocp4-prod-mce-five-a\n",
    _hc("five", "ocp4-prod-mce-five-a", "ocp4-prod-iac-five-a"): "mastertag: 4.19.1\n",
    _hc("five", "ocp4-prod-mce-five-a", "ocp4-prep-web-five-b"): "day2:\n  env: prod\n",  # env from day2.env
    _hc("five", "ocp4-mce-site2", "ocp4-test-kubevirt-site4"): _nodepools("az1"),  # kubevirt by NAME only: an hc
    _hc("five", "ocp4-mce-site2", "ocp4-legacy"): "clusterName: legacy-named\n",
    "sites/five/mces/ocp4-mce-site2/hostedClusters/notes.txt": "not a cluster file\n",
    "sites/five/mces/ocp4-mce-site2/hostedClusters/ocp4-old.yml": "x: 1\n",
    _hc("five", "ocp4-seed-five-1", "ocp4-prod-kv-five-c"): _nodepools("az1"),
    _hc("five", "ocp4-seed-five-1", "ocp4-prod-kv-five-a"): _nodepools("az1", "az1"),
    _hc("five", "ocp4-seed-five-1", "ocp4-prod-kv-five-b"): _nodepools("az1"),
    _hc("five", "ocp4-seed-five-2", "ocp4-prod-kv-five-d"): _nodepools("az2"),
    _hc("five", "ocp4-seed-five-2", "ocp4-prod-kv-five-e"): _nodepools("az1"),  # another zone
    _hc("five", "ocp4-seed-five-2", "ocp4-prod-kv-five-f"): _nodepools(None),  # no zone
    _hc("five", "ocp4-seed-five-2", "ocp4-prod-kv-five-g"): _nodepools("az2", "az3", None),  # mixed
    _hc("five", "ocp4-seed-five-2", "ocp4-prod-kv-five-h"): "mastertag: 4.19.1\n",  # no NodePool at all
    _hc("five", "ocp4-seed-five-2", "ocp4-kubevirt-five-az2-1"): _nodepools("az2"),  # a name off the convention
    _hc("five", "ocp4-seed-five-2", "ocp4-prod-kv-six-a"): _nodepools("az2"),  # the name says another site
    # --- site bat-yam: a site with a dash, one MCE, one seed, and a seed named for another site
    _hc("bat-yam", "ocp4-prep-mce-bat-yam-a", "ocp4-prep-iac-bat-yam-a"): "mastertag: 4.19.1\n",
    _hc("bat-yam", "ocp4-seed-bat-yam-1", "ocp4-prep-kv-bat-yam-a"): _nodepools("az1"),
    _hc("bat-yam", "ocp4-seed-five-3", "ocp4-prep-kv-bat-yam-b"): _nodepools("az3"),  # the seed's own site differs
    # --- an empty MCE folder, and things that are not folders of clusters
    "sites/bat-yam/mces/ocp4-prep-mce-bat-yam-b/values.yaml": "x: 1\n",
    "sites/bat-yam/not-mces/ocp4-seed-bat-yam-1/hostedClusters/ocp4-prod-kv-bat-yam-z.yaml": "x: 1\n",
    "upgrade/mces/ocp4-prod-mce-five-a.yaml": "mce:\n  version: 2.17.3\n",
}


@pytest.fixture
def day1(tmp_path: Path) -> Day1:
    _write(tmp_path, TREE)
    return Day1(tmp_path, revision=REVISION)


def _names(records) -> list[str]:
    return [record.name for record in records]


class TestKinds:
    def test_a_kv_cluster_is_a_file_under_a_seed_folder_never_a_name(self, day1):
        kubevirt_by_name = day1.cluster("ocp4-test-kubevirt-site4")
        assert (kubevirt_by_name.kind, kubevirt_by_name.zone, kubevirt_by_name.id) == ("hc", None, None)
        assert kubevirt_by_name.folder == "ocp4-mce-site2" and kubevirt_by_name.problems == []
        assert "ocp4-test-kubevirt-site4" not in _names(day1.kv_clusters())
        assert "ocp4-test-kubevirt-site4" in _names(day1.hosted_clusters())

    def test_every_kind_filter(self, day1):
        everything = day1.clusters()
        hc, kv = day1.clusters(kind="hc"), day1.clusters(kind="kv")
        assert len(everything) == 17 and len(hc) == 5 and len(kv) == 12
        assert sorted(_names(everything)) == sorted(_names(hc) + _names(kv))
        assert {record.kind for record in hc} == {"hc"} and {record.kind for record in kv} == {"kv"}
        assert day1.clusters(kind="all") == everything
        assert day1.kv_clusters() == kv
        assert day1.hosted_clusters() == hc and day1.hosted_clusters(include_kv=True) == everything
        assert _names(hc) == [
            "ocp4-prep-iac-bat-yam-a", "ocp4-legacy", "ocp4-test-kubevirt-site4",
            "ocp4-prep-web-five-b", "ocp4-prod-iac-five-a",
        ]

    def test_only_yaml_files_of_a_hosted_clusters_folder_are_clusters(self, day1):
        names = _names(day1.clusters())
        assert "notes" not in names and "ocp4-old" not in names and "ocp4-prod-kv-bat-yam-z" not in names
        assert "ocp4-prod-mce-five-a" not in names  # upgrade/mces/ is not the cluster tree


class TestTheZoneOfAKvCluster:
    def test_it_is_the_seed_folders_and_agreeing_nodepools_leave_no_problem(self, day1):
        records = day1.kv_clusters(site="five", zone="az1")
        assert [(r.name, r.zone, r.id, r.folder, r.problems) for r in records] == [
            ("ocp4-prod-kv-five-a", "az1", "a", "ocp4-seed-five-1", []),
            ("ocp4-prod-kv-five-b", "az1", "b", "ocp4-seed-five-1", []),
            ("ocp4-prod-kv-five-c", "az1", "c", "ocp4-seed-five-1", []),
        ]

    def test_oldest_first_by_the_id_letter_names_without_one_last(self, day1):
        ordered = day1.kv_clusters(site="five", zone="az2")
        assert [(record.id, record.name) for record in ordered] == [
            ("a", "ocp4-prod-kv-six-a"),
            ("d", "ocp4-prod-kv-five-d"),
            ("e", "ocp4-prod-kv-five-e"),
            ("f", "ocp4-prod-kv-five-f"),
            ("g", "ocp4-prod-kv-five-g"),
            ("h", "ocp4-prod-kv-five-h"),
            (None, "ocp4-kubevirt-five-az2-1"),  # alphabetically first, listed last
        ]

    def test_two_clusters_with_one_letter_are_ordered_by_name(self, tmp_path):
        _write(tmp_path, {
            _hc("five", "ocp4-seed-five-1", "ocp4-prod-kv-five-b"): _nodepools("az1"),
            _hc("five", "ocp4-seed-five-1", "ocp4-test-kv-five-a"): _nodepools("az1"),
            _hc("five", "ocp4-seed-five-1", "ocp4-prep-kv-five-a"): _nodepools("az1"),
        })
        assert _names(Day1(tmp_path, revision=REVISION).kv_clusters("five", "az1")) == [
            "ocp4-prep-kv-five-a", "ocp4-test-kv-five-a", "ocp4-prod-kv-five-b",
        ]

    @pytest.mark.parametrize(("name", "said"), [
        ("ocp4-prod-kv-five-e", ["nodepool[0] labels its nodes zone 'az1', but the file is under the seed of zone az2"]),
        ("ocp4-prod-kv-five-f", ["nodepool[0] has no `topology.kubernetes.io/zone` nodeLabel"]),
        ("ocp4-prod-kv-five-g", ["nodepool[1] labels its nodes zone 'az3'", "nodepool[2] has no `topology.kubernetes.io/zone`"]),
        ("ocp4-prod-kv-five-h", ["no NodePool, so no `topology.kubernetes.io/zone` nodeLabel says its nodes are in zone az2"]),
    ])
    def test_nodepools_that_do_not_all_name_the_zone_are_problems_and_the_record_is_still_listed(self, day1, name, said):
        record = next(r for r in day1.kv_clusters(site="five", zone="az2") if r.name == name)
        assert record.zone == "az2" and record.kind == "kv"  # the folder's zone, whatever the NodePools say
        assert len(record.problems) == len(said)
        for problem, expected in zip(record.problems, said):
            assert problem.startswith(record.path + ": ") and expected in problem

    def test_a_name_off_the_convention_has_no_id_and_a_problem(self, day1):
        record = day1.cluster("ocp4-kubevirt-five-az2-1")
        assert (record.kind, record.zone, record.id) == ("kv", "az2", None)
        assert len(record.problems) == 1 and "ocp4-<lifecycle>-kv-<site>-<id>" in record.problems[0]
        assert "its place in the zone's order is unknown" in record.problems[0]

    def test_a_name_of_another_flavor_under_a_seed_is_still_a_kv_cluster_without_an_id(self, tmp_path):
        _write(tmp_path, {_hc("five", "ocp4-seed-five-1", "ocp4-prod-iac-five-a"): _nodepools("az1")})
        (record,) = Day1(tmp_path, revision=REVISION).kv_clusters("five", "az1")
        assert (record.kind, record.id) == ("kv", None) and "'ocp4-prod-iac-five-a' is not" in record.problems[0]

    def test_a_name_or_a_seed_folder_of_another_site_is_a_problem(self, day1):
        by_name = day1.cluster("ocp4-prod-kv-six-a")
        assert (by_name.site, by_name.id) == ("five", "a")
        assert by_name.problems == [f"{by_name.path}: the name says site 'six', but the file is under sites/five"]
        by_folder = day1.cluster("ocp4-prep-kv-bat-yam-b")
        assert (by_folder.site, by_folder.folder, by_folder.zone) == ("bat-yam", "ocp4-seed-five-3", "az3")
        assert by_folder.problems == [
            f"{by_folder.path}: seed folder ocp4-seed-five-3 is named for site 'five', but is under sites/bat-yam"
        ]

    def test_a_zones_order_does_not_depend_on_which_seed_folder_a_file_is_in(self, tmp_path):
        """Two seed folders of one zone under a site (one named for another site): still oldest first."""
        _write(tmp_path, {
            _hc("five", "ocp4-seed-five-1", "ocp4-prod-kv-five-b"): _nodepools("az1"),
            _hc("five", "ocp4-seed-bat-yam-1", "ocp4-prod-kv-five-a"): _nodepools("az1"),
            _hc("five", "ocp4-seed-five-1", "ocp4-prod-kv-five-c"): _nodepools("az1"),
            _hc("five", "ocp4-seed-five-2", "ocp4-prod-kv-five-a"): _nodepools("az2"),
            _hc("five", "ocp4-zzz-mce-five-a", "ocp4-prod-iac-five-a"): "mastertag: 4.19.1\n",
        })
        day1 = Day1(tmp_path, revision=REVISION)
        assert [(r.id, r.folder) for r in day1.kv_clusters("five", "az1")] == [
            ("a", "ocp4-seed-bat-yam-1"), ("b", "ocp4-seed-five-1"), ("c", "ocp4-seed-five-1"),
        ]
        # A site's hosted clusters first, then its KV clusters zone by zone.
        assert [(r.kind, r.zone, r.id) for r in day1.clusters(site="five")] == [
            ("hc", None, None), ("kv", "az1", "a"), ("kv", "az1", "b"), ("kv", "az1", "c"), ("kv", "az2", "a"),
        ]

    def test_a_site_with_dashes(self, day1):
        records = day1.kv_clusters(site="bat-yam", zone="az1")
        assert [(r.name, r.site, r.zone, r.id, r.problems) for r in records] == [
            ("ocp4-prep-kv-bat-yam-a", "bat-yam", "az1", "a", [])
        ]
        assert _names(day1.clusters(site="bat-yam")) == [
            "ocp4-prep-iac-bat-yam-a", "ocp4-prep-kv-bat-yam-a", "ocp4-prep-kv-bat-yam-b",
        ]

    def test_a_zone_or_a_site_with_no_seed_folder_is_an_empty_list(self, day1, tmp_path):
        assert day1.kv_clusters(site="five", zone="az9") == []
        assert day1.kv_clusters(site="bat-yam", zone="az2") == []
        assert day1.kv_clusters(site="nowhere", zone="az1") == []
        assert Day1(tmp_path / "empty", revision=REVISION).clusters() == []

    def test_a_zone_filter_keeps_only_what_is_under_that_zones_seed(self, day1):
        assert _names(day1.clusters(zone="az1")) == [
            "ocp4-prep-kv-bat-yam-a", "ocp4-prod-kv-five-a", "ocp4-prod-kv-five-b", "ocp4-prod-kv-five-c",
        ]
        assert day1.clusters(kind="hc", zone="az1") == []


class TestWhatMakesAKvClusterUnusable:
    """create-vm never places a VM on a cluster day1 does not CONFIRM for its seed's zone —
    and a YAML warning that is every cluster's alike must not take the whole fleet out."""

    def test_a_clean_file_is_confirmed_for_its_seeds_zone(self, day1):
        record = day1.cluster("ocp4-prod-kv-five-a")
        assert (record.zone, record.confirmed_zone, record.blocking, record.problems) == ("az1", "az1", [], [])

    @pytest.mark.parametrize(
        "name",
        ["ocp4-prod-kv-five-e", "ocp4-prod-kv-five-f", "ocp4-prod-kv-five-g", "ocp4-prod-kv-five-h",
         "ocp4-kubevirt-five-az2-1", "ocp4-prod-kv-six-a", "ocp4-prep-kv-bat-yam-b"],
    )
    def test_a_zone_a_name_or_a_site_that_disagrees_blocks_it(self, day1, name):
        record = day1.cluster(name)
        assert record.zone is not None  # still listed under its seed's zone…
        assert record.blocking and record.confirmed_zone is None  # …and never confirmed for it
        assert set(record.blocking) <= set(record.problems)

    def test_a_key_written_twice_in_a_layer_every_cluster_shares_blocks_none_of_them(self, tmp_path):
        _write(tmp_path, {**TREE, "sites/configValues.yaml": "mastertag: 4.18.56\nmastertag: 4.18.57\n"})
        records = Day1(tmp_path, revision=REVISION).kv_clusters(site="five", zone="az1")
        assert _names(records) == ["ocp4-prod-kv-five-a", "ocp4-prod-kv-five-b", "ocp4-prod-kv-five-c"]
        for record in records:
            assert record.problems and "written twice" in record.problems[0]  # reported…
            assert record.blocking == [] and record.confirmed_zone == "az1"   # …and still usable

    def test_a_hosted_cluster_under_an_mce_has_no_zone_to_confirm(self, day1):
        record = day1.cluster("ocp4-test-kubevirt-site4")
        assert (record.kind, record.zone, record.confirmed_zone, record.blocking) == ("hc", None, None, [])

    def test_the_folder_of_a_zones_clusters_is_spelled_in_one_place(self):
        from activities.day1_repo import hosted_clusters_dir

        assert hosted_clusters_dir("five", "ocp4-seed-five-1") == "sites/five/mces/ocp4-seed-five-1/hostedClusters/"


class TestTheRecord:
    def test_what_one_record_says(self, day1):
        record = day1.cluster("ocp4-prod-kv-five-a")
        assert record == day1_repo.ClusterRecord(
            name="ocp4-prod-kv-five-a", site="five", folder="ocp4-seed-five-1", kind="kv", env="prod",
            zone="az1", id="a", cluster_name="prod-kv-five-a", hosted_cluster="openshift-prod-kv-five-a",
            hosted_cluster_namespace="hcp-prod-kv-five-a",
            path="sites/five/mces/ocp4-seed-five-1/hostedClusters/ocp4-prod-kv-five-a.yaml",
            problems=[], revision=REVISION,
        )

    def test_every_record_carries_the_commit_it_was_read_at(self, day1):
        assert {record.revision for record in day1.clusters()} == {REVISION}

    def test_the_environment_is_day2_env_else_the_name_never_the_folder(self, day1):
        by_name = {record.name: record.env for record in day1.clusters()}
        assert by_name["ocp4-prod-iac-five-a"] == "prod"
        assert by_name["ocp4-prep-web-five-b"] == "prod"  # day2.env outranks `prep` in the name
        assert by_name["ocp4-test-kubevirt-site4"] == "test"  # under a legacy folder, which names none
        assert by_name["ocp4-legacy"] is None  # one field: the name says no environment
        assert _names(day1.clusters(env="prod", kind="hc")) == ["ocp4-prep-web-five-b", "ocp4-prod-iac-five-a"]
        assert _names(day1.hosted_clusters(site="five", env="test")) == ["ocp4-test-kubevirt-site4"]
        assert _names(day1.hosted_clusters(env="prep", include_kv=True)) == [
            "ocp4-prep-iac-bat-yam-a", "ocp4-prep-kv-bat-yam-a", "ocp4-prep-kv-bat-yam-b",
        ]

    def test_the_files_own_cluster_name_and_its_yaml_problems_are_carried(self, day1, tmp_path):
        legacy = day1.cluster("ocp4-legacy")
        assert (legacy.cluster_name, legacy.hosted_cluster, legacy.hosted_cluster_namespace) == (
            "legacy-named", "openshift-legacy-named", "hcp-legacy-named")
        _write(tmp_path, {_hc("five", "ocp4-prod-mce-five-a", "ocp4-prod-iac-five-a"): "mastertag: 1\nmastertag: 2\n"})
        assert "written twice" in day1.cluster("ocp4-prod-iac-five-a").problems[0]

    def test_one_unreadable_or_nameless_file_is_a_problem_of_that_record_not_a_failed_listing(self, day1, tmp_path):
        folder = tmp_path / "sites/five/mces/ocp4-prod-mce-five-a/hostedClusters"
        (folder / "ocp4-prod-bin-five-a.yaml").write_bytes(b"mastertag: \xff\xfe\n")
        (folder / "ocp4-.yaml").write_text("mastertag: 4.19.1\n")
        by_name = {record.name: record for record in day1.hosted_clusters(site="five")}
        assert len(by_name) == 6
        assert "not UTF-8 text" in by_name["ocp4-prod-bin-five-a"].problems[0]
        nameless = by_name["ocp4-"]
        assert nameless.cluster_name == "" and "no clusterName" in nameless.problems[0]
        assert by_name["ocp4-prod-iac-five-a"].problems == []

    def test_values_are_the_merged_layers(self, day1):
        record = day1.cluster("ocp4-prod-iac-five-a")
        values = day1.values(record.path)
        assert values.merged == {"mastertag": "4.19.1", "day2": {"copy_to_sigs": ["net"]}, "mceName": "ocp4-prod-mce-five-a"}


class TestFindingByName:
    def test_one_cluster_or_none(self, day1):
        assert day1.cluster("ocp4-nope") is None and day1.cluster_paths("ocp4-nope") == []
        assert day1.cluster_paths("ocp4-prod-iac-five-a") == [_hc("five", "ocp4-prod-mce-five-a", "ocp4-prod-iac-five-a")]

    @pytest.mark.parametrize("name", ["ocp4-*", "*", "ocp4-prod-mce-five-a/hostedClusters/ocp4-prod-iac-five-a", "../x", "", "notes", "ocp4-old"])
    def test_a_name_is_compared_never_globbed_or_joined(self, day1, name):
        assert day1.cluster_paths(name) == [] and day1.cluster(name) is None

    def test_a_name_in_two_folders_is_listed_twice_and_one_cluster_refuses_to_pick(self, day1, tmp_path):
        _write(tmp_path, {_hc("bat-yam", "ocp4-prep-mce-bat-yam-b", "ocp4-prod-iac-five-a"): "x: 1\n"})
        assert day1.cluster_paths("ocp4-prod-iac-five-a") == [
            _hc("bat-yam", "ocp4-prep-mce-bat-yam-b", "ocp4-prod-iac-five-a"),
            _hc("five", "ocp4-prod-mce-five-a", "ocp4-prod-iac-five-a"),
        ]
        with pytest.raises(ValueError, match="2 files named ocp4-prod-iac-five-a.yaml"):
            day1.cluster("ocp4-prod-iac-five-a")

    def test_the_folders_of_a_name_and_the_sites(self, day1, tmp_path):
        assert day1.sites() == ["bat-yam", "five"]
        for not_a_site in ("../five", "five/mces/ocp4-mce-site2", "five/..", "nowhere", "*", ""):
            assert day1.folders(not_a_site) == []
        assert day1.folders("bat-yam") == [
            "ocp4-prep-mce-bat-yam-a", "ocp4-prep-mce-bat-yam-b", "ocp4-seed-bat-yam-1", "ocp4-seed-five-3",
        ]
        assert day1.mce_folders("ocp4-mce-site2") == [("five", "sites/five/mces/ocp4-mce-site2")]
        assert day1.mce_folders("ocp4-prep-mce-bat-yam-b") == [("bat-yam", "sites/bat-yam/mces/ocp4-prep-mce-bat-yam-b")]
        for name in ("ocp4-mce-*", "mces", "five", "ocp4-mce-site2/hostedClusters", "..", ""):
            assert day1.mce_folders(name) == []
        (tmp_path / "sites/bat-yam/mces/ocp4-mce-site2").mkdir()
        assert [site for site, _ in day1.mce_folders("ocp4-mce-site2")] == ["bat-yam", "five"]
        assert day1.has_file(_hc("five", "ocp4-mce-site2", "ocp4-legacy")) and not day1.has_file(_hc("five", "ocp4-mce-site2", "nope"))


class TestPathShapingInputIsRefused:
    @pytest.mark.parametrize("site", ["../five", "five/mces", "*", "Five", "five-", "", "fi ve", "."])
    def test_a_site(self, day1, site):
        with pytest.raises(MceNameError, match="site"):
            day1.clusters(site=site)
        with pytest.raises(ValueError):
            day1.kv_clusters(site=site, zone="az1")

    @pytest.mark.parametrize("zone", ["1", "az", "az0", "az1/..", "*", "AZ1", "az-1", ""])
    def test_a_zone(self, day1, zone):
        with pytest.raises(MceNameError, match="zone"):
            day1.kv_clusters(site="five", zone=zone)

    @pytest.mark.parametrize("env", ["../prod", "*", "PROD", ""])
    def test_an_env(self, day1, env):
        with pytest.raises(ValueError, match="env"):
            day1.clusters(env=env)

    @pytest.mark.parametrize("kind", ["kubevirt", "KV", "", "*"])
    def test_a_kind(self, day1, kind):
        with pytest.raises(ValueError, match="kind"):
            day1.clusters(kind=kind)


# --- main_snapshot: Day1 on the cached snapshot of the remote's head --------------------------------


def _git(*args: str, cwd: Path) -> str:
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout


@pytest.fixture
def origin(tmp_path: Path) -> Path:
    bare = tmp_path / "day1.git"
    _git("init", "-q", "--bare", "--initial-branch=main", str(bare), cwd=tmp_path)
    seed = tmp_path / "seed"
    _git("clone", "-q", str(bare), str(seed), cwd=tmp_path)
    _write(seed, TREE)
    _git("add", "-A", cwd=seed)
    _git("-c", "user.name=seed", "-c", "user.email=seed@test", "commit", "-q", "-m", "seed", cwd=seed)
    _git("push", "-q", "origin", "HEAD:main", cwd=seed)
    return bare


class TestOnTheSnapshot:
    async def test_the_revision_is_mains_head_and_the_blob_is_gits(self, origin, tmp_path):
        async with day1_repo.main_snapshot(repo_url=str(origin), token="", cache_root=tmp_path / "cache") as day1:
            head = _git("rev-parse", "main", cwd=origin).strip()
            assert day1.revision == head
            records = day1.kv_clusters(site="five", zone="az1")
            assert _names(records) == ["ocp4-prod-kv-five-a", "ocp4-prod-kv-five-b", "ocp4-prod-kv-five-c"]
            assert {record.revision for record in records} == {head}
            assert await day1.blob(records[0].path) == _git("rev-parse", f"main:{records[0].path}", cwd=origin).strip()

    async def test_a_plain_directory_has_no_git_to_ask(self, day1):
        with pytest.raises(RuntimeError, match="plain directory"):
            await day1.blob(CLUSTER)
        with pytest.raises(RuntimeError, match="plain directory"):
            day1.scratch(prefix="t-")
