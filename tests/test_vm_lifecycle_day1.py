"""create-vm's day1 read (activities/vm_lifecycle/day1_repo.py): a zone's KubeVirt clusters
listed from the seed folder, against a REAL local bare repo. No network, no Temporal — the
approach of tests/test_upgrade_gitops_day1.py. The shared reader underneath (the clone, the
folder listing, a cluster file with its layers) is tests/test_day1_repo.py's."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from activities.vm_lifecycle import day1_repo
from shared.cluster_names import MceNameError
from shared.exceptions import ValuesRepoGitError
from tests.test_git_repo import SECRET, serve_over_https

SITE, ZONE = "lab", "a"
SEED = f"sites/{SITE}/mces/ocp4-seed-{SITE}-{ZONE}/hostedClusters"
OTHER_SEED = f"sites/{SITE}/mces/ocp4-seed-{SITE}-b/hostedClusters"


def _git(*args: str, cwd: Path) -> str:
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout


def _bare_repo(root: Path, name: str, files: dict[str, str]) -> Path:
    bare = root / name
    _git("init", "-q", "--bare", "--initial-branch=main", str(bare), cwd=root)
    seed = root / f"{name}-seed"
    _git("clone", "-q", str(bare), str(seed), cwd=root)
    for relative, content in files.items():
        (seed / relative).parent.mkdir(parents=True, exist_ok=True)
        (seed / relative).write_text(content)
    _git("add", "-A", cwd=seed)
    _git("-c", "user.name=seed", "-c", "user.email=seed@test", "commit", "-q", "-m", "seed", cwd=seed)
    _git("push", "-q", "origin", "HEAD:main", cwd=seed)
    return bare


FILES = {
    "sites/configValues.yaml": "dhcp_values:\n  leaseDurationDays: 8\n",
    f"sites/{SITE}/mces/ocp4-seed-{SITE}-{ZONE}/values.yaml": "mastertag: 4.18.56-x86_64\n",
    # Its own clusterName.
    f"{SEED}/ocp4-kubevirt-{SITE}-{ZONE}-1.yaml": "clusterName: kv-lab-one\nplatform: agent\nnodepool: []\n",
    # No clusterName: the file name minus the prefix.
    f"{SEED}/ocp4-kubevirt-{SITE}-{ZONE}-2.yaml": "platform: agent\n",
    # A key written twice: a problem, not an error.
    f"{SEED}/ocp4-kubevirt-{SITE}-{ZONE}-3.yaml": "platform: agent\nplatform: agent\n",
    # Not a cluster file: ignored.
    f"{SEED}/README.md": "notes\n",
    # Another zone's seed: never listed for zone a.
    f"{OTHER_SEED}/ocp4-kubevirt-{SITE}-b-1.yaml": "platform: agent\n",
}


@pytest.fixture
def origin(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    serve_over_https(tmp_path, monkeypatch)
    return _bare_repo(tmp_path, "day1.git", FILES)


async def test_the_seed_folder_is_listed_with_each_files_cluster_name(origin):
    listed = await day1_repo.list_kv_clusters(repo_url="https://git.test/day1.git", token=SECRET, site=SITE, zone=ZONE)
    assert [(e.name, e.cluster_name, e.hosted_cluster) for e in listed.clusters] == [
        (f"ocp4-kubevirt-{SITE}-{ZONE}-1", "kv-lab-one", "openshift-kv-lab-one"),
        (f"ocp4-kubevirt-{SITE}-{ZONE}-2", f"kubevirt-{SITE}-{ZONE}-2", f"openshift-kubevirt-{SITE}-{ZONE}-2"),
        (f"ocp4-kubevirt-{SITE}-{ZONE}-3", f"kubevirt-{SITE}-{ZONE}-3", f"openshift-kubevirt-{SITE}-{ZONE}-3"),
    ]
    assert listed.clusters[0].path == f"{SEED}/ocp4-kubevirt-{SITE}-{ZONE}-1.yaml"
    assert listed.clusters[0].problems == [] and listed.clusters[1].problems == []
    assert len(listed.clusters[2].problems) == 1 and "written twice" in listed.clusters[2].problems[0]
    assert listed.revision == _git("rev-parse", "main", cwd=origin).strip()


async def test_a_zone_without_a_seed_folder_is_an_empty_list(origin):
    listed = await day1_repo.list_kv_clusters(repo_url="https://git.test/day1.git", token=SECRET, site=SITE, zone="c")
    assert listed.clusters == [] and listed.revision


async def test_the_other_zones_seed_is_not_this_zones(origin):
    listed = await day1_repo.list_kv_clusters(repo_url="https://git.test/day1.git", token=SECRET, site=SITE, zone="b")
    assert [e.name for e in listed.clusters] == [f"ocp4-kubevirt-{SITE}-b-1"]


@pytest.mark.parametrize(("site", "zone"), [("../lab", "a"), ("lab", "a/../b"), ("lab", "*"), ("LAB", "a")])
async def test_tokens_that_could_shape_a_path_are_refused_before_any_clone(origin, site, zone):
    with pytest.raises(MceNameError):
        await day1_repo.list_kv_clusters(repo_url="https://git.test/day1.git", token=SECRET, site=site, zone=zone)


async def test_a_repo_that_cannot_be_cloned_is_retryable_and_names_no_token(origin):
    with pytest.raises(ValuesRepoGitError) as raised:
        await day1_repo.list_kv_clusters(repo_url="https://git.test/nowhere.git", token=SECRET, site=SITE, zone=ZONE)
    assert SECRET not in str(raised.value)

