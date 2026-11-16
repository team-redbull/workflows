"""The hosted-cluster name helpers in shared/cluster_names.py: the stem, its parts, the branch."""

from __future__ import annotations

import pytest

from shared.cluster_names import (
    HostedClusterNameError,
    env_of_hosted_cluster,
    env_of_mce_folder,
    hosted_cluster_name,
    hosted_cluster_stem,
    provision_cluster_branch,
)


@pytest.mark.parametrize("name", ["prep-e2e-site2-a", "ocp4-prep-e2e-site2-a", "  ocp4-prep-e2e-site2-a\n"])
def test_the_stem_is_the_same_with_or_without_the_prefix(name):
    assert hosted_cluster_stem(name) == "ocp4-prep-e2e-site2-a"


def test_the_stem_minus_the_prefix_is_the_charts_cluster_name():
    assert hosted_cluster_name("ocp4-prep-e2e-site2-a") == "prep-e2e-site2-a"
    assert hosted_cluster_name("a1") == "a1"


@pytest.mark.parametrize("name", ["", "ocp4-", "Prep", "a b", "a_b", "-a", "a-", "a.b", "ocp4-ocp4-" + "x", "é"])
def test_a_name_that_is_not_a_dns_label_is_refused(name):
    if name == "ocp4-ocp4-x":
        assert hosted_cluster_stem(name) == "ocp4-ocp4-x"  # the prefix is stripped once; the rest is a label
        return
    with pytest.raises(HostedClusterNameError):
        hosted_cluster_stem(name)


def test_the_name_is_at_most_24_characters_so_the_hcp_namespace_fits():
    assert hosted_cluster_stem("a" * 24) == "ocp4-" + "a" * 24
    with pytest.raises(HostedClusterNameError, match="at most 24"):
        hosted_cluster_stem("a" * 25)


def test_the_error_is_a_value_error_for_the_edge():
    with pytest.raises(ValueError):
        hosted_cluster_stem("Bad")


@pytest.mark.parametrize(("stem", "env"), [
    ("ocp4-prod-iac-site2-a", "prod"), ("ocp4-prep-e2e-site2-a", "prep"), ("ocp4-test-x", "test"),
    ("ocp4-kubevirt-site2-a1", "kubevirt"), ("ocp4-x", None), ("x", None),
])
def test_the_environment_is_the_first_field_of_the_cluster_name(stem, env):
    assert env_of_hosted_cluster(stem) == env


@pytest.mark.parametrize(("mce", "env"), [
    ("ocp4-prod-mce-site2-a", "prod"), ("ocp4-prep-mce-site2-a", "prep"), ("ocp4-test-mce", "test"),
    ("ocp4-mce-site2", None), ("ocp4-mce-site4-prep", None), ("ocp4-seed-site2-a", None), ("mce-site2", None),
])
def test_the_mce_folders_environment_is_read_only_from_the_convention(mce, env):
    assert env_of_mce_folder(mce) == env


def test_the_branch_names_the_workflow_and_the_cluster():
    assert provision_cluster_branch("ocp4-prep-e2e-site2-a") == "provision-cluster/ocp4-prep-e2e-site2-a"
