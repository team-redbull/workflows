"""The cluster-upgrade pieces of shared/: the models module, and the rules both sides read.

shared/upgrade_layout.py is the ONE copy of what a limb reads and the brain
decides on, so these tests replace the ones that used to keep two copies equal.
"""

from __future__ import annotations

import inspect

import pytest
from pydantic import BaseModel

import shared.models.cluster_upgrade as models
from shared import upgrade_layout
from shared.models.cluster_upgrade import ReleaseLayout
from tests.cluster_upgrade_fakes import IMAGE_PATH, RELEASE

PUBLIC = ReleaseLayout(
    image_path="quay.io/openshift-release-dev", repo="{imagePath}/ocp-release", tag="{version}-x86_64",
    mirrored=False,
)


def test_every_model_is_complete_at_import():
    """A forward reference left unresolved is resolved on first use, inside a workflow task."""
    incomplete = [
        name for name, obj in vars(models).items()
        if inspect.isclass(obj) and issubclass(obj, BaseModel) and obj.__module__ == models.__name__
        and not obj.__pydantic_complete__
    ]
    assert incomplete == []


class TestReleaseRepositories:
    def test_templates_expand_to_the_full_mirror(self):
        assert upgrade_layout.release_repositories(RELEASE) == [
            f"{IMAGE_PATH}/openshift/release-images", f"{IMAGE_PATH}/openshift/release",
        ]
        assert upgrade_layout.expand(RELEASE.tag, image_path=IMAGE_PATH, version="4.20.44") == "4.20.44-x86_64"
        # Without a version, `{version}` stays for the caller to fill.
        assert upgrade_layout.expand("{imagePath}/x:{version}", image_path="r") == "r/x:{version}"

    @pytest.mark.parametrize("cpo_repo", [None, ""])
    def test_a_layout_naming_no_cpo_repo_is_pulled_from_the_payload_s_alone(self, cpo_repo):
        # "" and absent mean the same on both sides: the registry pins no operator
        # image, and pre-flight looks for no operator mirror.
        layout = PUBLIC.model_copy(update={"cpo_repo": cpo_repo})
        assert upgrade_layout.cpo_repository(layout) is None
        assert upgrade_layout.release_repositories(layout) == ["quay.io/openshift-release-dev/ocp-release"]


@pytest.mark.parametrize(("path", "parts"), [
    ("sites/site1/mces/ocp4-mce-a/hostedClusters/ocp4-c.yaml", ("site1", "ocp4-mce-a", "ocp4-c")),
    ("sites/s/mces/m/hostedClusters/c.yml", None),  # the hub limb finds `<cluster>.yaml` only
    ("sites/s/mces/m/hostedClusters/sub/c.yaml", None),
    ("sites/s/mces/m/values.yaml", None),
])
def test_the_day1_cluster_file_names_its_site_mce_and_cluster(path, parts):
    match = upgrade_layout.DAY1_CLUSTER_FILE.fullmatch(path)
    assert (match and (match["site"], match["mce"], match["name"])) == parts
