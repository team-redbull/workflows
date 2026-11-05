"""The monorepo's day1 reader (activities/day1_repo.py), shared by vm-lifecycle and
upgrade-gitops and owned by neither: the folder listing, a cluster file read with its value
layers the way the hostedclusters-setup chart renders it, and the one-object guarantee that
both limbs read day1 through it. Pure file reads; the clone is tests/test_git_repo.py's."""

from __future__ import annotations

from pathlib import Path

import pytest

from activities import day1_repo
from activities.upgrade_gitops import day1_repo as upgrade_day1_repo
from activities.upgrade_gitops import day1_values
from activities.vm_lifecycle import day1_repo as vm_day1_repo

CLUSTER = "sites/s/mces/ocp4-seed-s-a/hostedClusters/ocp4-kubevirt-s-a-1.yaml"


def _write(root: Path, files: dict[str, str]) -> None:
    for relative, content in files.items():
        (root / relative).parent.mkdir(parents=True, exist_ok=True)
        (root / relative).write_text(content)


def test_both_limbs_read_day1_through_the_one_shared_reader():
    """Neither limb imports the other's domain for day1: each takes the shared module's objects."""
    assert upgrade_day1_repo.clone_main is day1_repo.clone_main
    assert upgrade_day1_repo.head is day1_repo.head and upgrade_day1_repo.blob is day1_repo.blob
    assert day1_values.load_cluster_values is day1_repo.load_cluster_values
    assert day1_values.load_yaml is day1_repo.load_yaml
    assert vm_day1_repo.load_cluster_values is day1_repo.load_cluster_values
    assert vm_day1_repo.list_cluster_files is day1_repo.list_cluster_files
    for module in (upgrade_day1_repo, day1_values):
        assert "vm_lifecycle" not in Path(module.__file__).read_text()
    assert "upgrade_gitops" not in Path(vm_day1_repo.__file__).read_text()


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
