"""day1_writer against a REAL local bare repo — fully offline.

A bare repo in tmp_path stands in as the day1 `origin`, shaped like the mirror:
`main` holds the value layers and two MCE folders, and one existing cluster.
inspect reads it; publish creates `provision-cluster/<cluster>` from main by
the push itself, and a re-run converges onto it — with the day1 chain's
appended mcConfigMap commit in between, as production will have.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from activities.cluster_lifecycle import cluster_values, day1_writer
from activities.cluster_lifecycle.dhcp_values import build_dhcp_values
from shared.exceptions import (
    AmbiguousMceFolderError,
    ClusterAlreadyProvisionedError,
    ClusterFileConflictError,
    ClusterValuesRejectedError,
    MceFolderNotFoundError,
    ValuesRepoGitError,
)
from shared.models.cluster_lifecycle import ClusterFilePublishRequest
from shared.models.segment_lifecycle import SegmentType
from tests.test_cluster_values import CONFIG_VALUES, MCE, MCE_VALUES, SITE_VALUES
from workflow_domains.cluster_lifecycle.router import EXAMPLE_CLUSTER_FILE

CLUSTER = "ocp4-prep-e2e-site2-a"
MCE_DIR = f"sites/site2/mces/{MCE}"
PATH = f"{MCE_DIR}/hostedClusters/{CLUSTER}.yaml"
BRANCH = f"provision-cluster/{CLUSTER}"
DHCP = build_dhcp_values("10.20.90.0/24", [(1, 10), (241, 254)])
EXISTING = "ocp4-prep-old-site2-a"

DAY1_FILES = {
    "sites/configValues.yaml": CONFIG_VALUES,
    "sites/site2/values.yaml": SITE_VALUES,
    f"{MCE_DIR}/values.yaml": MCE_VALUES,
    f"{MCE_DIR}/hostedClusters/{EXISTING}.yaml": "mastertag: 4.16.27\n",
    "sites/site4/mces/ocp4-mce-site4/values.yaml": "mceName: ocp4-mce-site4\n",
    # a folder named like an MCE but outside mces/ must never match
    "sites/site4/not-mces/ocp4-prep-mce-site2-a/values.yaml": "x: 1\n",
}


def _git(*args: str, cwd: Path) -> str:
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout


def _commit_all(seed: Path, message: str) -> None:
    _git("add", "-A", cwd=seed)
    _git("-c", "user.name=seed", "-c", "user.email=seed@test", "commit", "-q", "-m", message, cwd=seed)


@pytest.fixture
def origin(tmp_path: Path) -> Path:
    bare = tmp_path / "day1.git"
    _git("init", "-q", "--bare", "--initial-branch=main", str(bare), cwd=tmp_path)
    seed = tmp_path / "seed"
    _git("clone", "-q", str(bare), str(seed), cwd=tmp_path)
    for relative, content in DAY1_FILES.items():
        (seed / relative).parent.mkdir(parents=True, exist_ok=True)
        (seed / relative).write_text(content)
    _commit_all(seed, "seed main")
    _git("push", "-q", "origin", "main", cwd=seed)
    return bare


def _branch_file(origin: Path, tmp_path: Path, name: str, branch: str = BRANCH, path: str = PATH) -> str:
    checkout = tmp_path / name
    _git("clone", "-q", "--branch", branch, str(origin), str(checkout), cwd=tmp_path)
    return (checkout / path).read_text()


def _push_onto_branch(origin: Path, tmp_path: Path, name: str, edit, message: str, branch: str = BRANCH) -> None:
    checkout = tmp_path / name
    _git("clone", "-q", "--branch", branch, str(origin), str(checkout), cwd=tmp_path)
    edit(checkout)
    _commit_all(checkout, message)
    _git("push", "-q", "origin", branch, cwd=checkout)


async def _inspect(origin: Path, **overrides):
    kwargs = dict(repo_url=str(origin), token="", cluster=CLUSTER, mce=MCE, values=EXAMPLE_CLUSTER_FILE)
    kwargs.update(overrides)
    return await day1_writer.inspect(**kwargs)


def _request(**overrides) -> ClusterFilePublishRequest:
    fields = dict(cluster=CLUSTER, mce_dir=MCE_DIR, branch=BRANCH, values=EXAMPLE_CLUSTER_FILE,
                  vlan_id=23, segment="10.20.90.0/24", type=SegmentType.HC)
    fields.update(overrides)
    return ClusterFilePublishRequest(**fields)


async def _publish(origin: Path, **overrides):
    return await day1_writer.publish(repo_url=str(origin), token="", request=_request(**overrides), dhcp_values=DHCP)


# --- inspect ---------------------------------------------------------------------------


class TestInspect:
    async def test_finds_the_mce_folder_by_name_and_derives_the_target(self, origin):
        target = await _inspect(origin)
        assert (target.site, target.mce_dir, target.cluster_name, target.mastertag) == (
            "site2", MCE_DIR, "prep-e2e-site2-a", "4.22.8")
        assert target.config_names[0] == "nm-conf"
        assert target.revision == _git("rev-parse", "main", cwd=origin).strip()

    async def test_an_mce_folder_outside_the_convention_is_found_too(self, origin):
        target = await _inspect(origin, mce="ocp4-mce-site4", cluster="ocp4-prod-x-site4")
        assert (target.site, target.mce_dir) == ("site4", "sites/site4/mces/ocp4-mce-site4")

    @pytest.mark.parametrize("mce", ["ocp4-mce-site9", "ocp4-prep-mce-site2", "mces", "site2"])
    async def test_an_unknown_mce_folder_names_the_sites_searched(self, origin, mce):
        with pytest.raises(MceFolderNotFoundError, match="site2, site4"):
            await _inspect(origin, mce=mce)

    @pytest.mark.parametrize("mce", ["*", "../site2", "ocp4-mce-site4/..", "ocp4-mce-site4/hostedClusters", "-x", "OCP4-mce"])
    async def test_an_mce_that_is_not_a_plain_name_never_reaches_the_filesystem(self, origin, mce):
        with pytest.raises(MceFolderNotFoundError, match="not a plain"):
            await _inspect(origin, mce=mce)

    @pytest.mark.parametrize("cluster", ["../x", "a/b", "*", "-x"])
    async def test_a_cluster_that_is_not_a_plain_name_is_refused(self, origin, cluster):
        with pytest.raises(MceFolderNotFoundError, match="not a plain"):
            await _inspect(origin, cluster=cluster)

    async def test_a_folder_name_under_two_sites_is_ambiguous(self, origin, tmp_path):
        _push_onto_branch(
            origin, tmp_path, "dup",
            lambda c: ((c / "sites/site4/mces" / MCE).mkdir(parents=True), (c / "sites/site4/mces" / MCE / "values.yaml").write_text("x: 1\n")),
            "duplicate mce folder", branch="main",
        )
        with pytest.raises(AmbiguousMceFolderError, match="site2, site4"):
            await _inspect(origin)

    async def test_a_cluster_already_on_main_is_refused(self, origin):
        with pytest.raises(ClusterAlreadyProvisionedError, match=f"{EXISTING}.yaml already exists"):
            await _inspect(origin, cluster=EXISTING)

    async def test_a_cluster_on_main_in_another_mce_is_not_this_folders(self, origin):
        # The file is keyed on its folder: the same stem in another MCE folder
        # is day1's uniqueness problem, not this check's.
        target = await _inspect(origin, cluster=EXISTING, mce="ocp4-mce-site4")
        assert target.mce_dir == "sites/site4/mces/ocp4-mce-site4"

    async def test_a_rejected_file_names_every_reason(self, origin):
        with pytest.raises(ClusterValuesRejectedError) as caught:
            await _inspect(origin, values="nodepool: []\nvlanId: 3\n")
        assert "`mastertag`" in str(caught.value) and "`vlanId`" in str(caught.value)

    async def test_an_unreachable_repo_stays_retryable(self, tmp_path):
        with pytest.raises(ValuesRepoGitError):
            await _inspect(tmp_path / "no-such.git")


# --- publish ---------------------------------------------------------------------------


class TestPublish:
    async def test_creates_the_branch_from_main_with_the_assembled_file(self, origin, tmp_path):
        main_before = _git("rev-parse", "main", cwd=origin).strip()
        published = await _publish(origin)
        assert (published.relative_path, published.branch, published.changed) == (PATH, BRANCH, True)
        assert published.commit_sha == _git("rev-parse", BRANCH, cwd=origin).strip()
        assert published.dhcp_values == DHCP
        # main never moved: the branch is what a person merges.
        assert _git("rev-parse", "main", cwd=origin).strip() == main_before
        assert _git("rev-parse", f"{BRANCH}^", cwd=origin).strip() == main_before
        content = _branch_file(origin, tmp_path, "verify")
        assert content == cluster_values.assemble_cluster_file(EXAMPLE_CLUSTER_FILE, 23, DHCP)
        assert content.startswith(EXAMPLE_CLUSTER_FILE)
        # No `[skip ci]`: the day1 chain must run on the branch.
        log = _git("log", "-1", "--format=%s|%an <%ae>", BRANCH, cwd=origin).strip()
        assert log == (f"feat: provision hosted cluster {CLUSTER} [provision-cluster-workflow]|"
                       "provision-cluster-workflow <provision-cluster-workflow@redbull.local>")
        assert "[skip ci]" not in log

    async def test_an_mce_without_a_hosted_clusters_folder_yet_gets_one(self, origin, tmp_path):
        mce_dir = "sites/site4/mces/ocp4-mce-site4"
        published = await _publish(origin, mce_dir=mce_dir, branch=f"provision-cluster/{CLUSTER}")
        assert published.relative_path == f"{mce_dir}/hostedClusters/{CLUSTER}.yaml"
        assert _branch_file(origin, tmp_path, "v4", path=published.relative_path).startswith(EXAMPLE_CLUSTER_FILE)

    async def test_a_rerun_over_its_own_branch_pushes_nothing(self, origin):
        await _publish(origin)
        before = _git("rev-parse", BRANCH, cwd=origin).strip()
        published = await _publish(origin)
        assert (published.changed, published.commit_sha, published.branch) == (False, None, BRANCH)
        assert _git("rev-parse", BRANCH, cwd=origin).strip() == before

    @pytest.mark.parametrize("header", ["", "# === Added by CI ===\n"], ids=["new-chain", "legacy-ci-header"])
    async def test_a_rerun_after_the_chains_mc_config_map_commit_is_a_no_op(self, origin, tmp_path, header):
        """Production's shape: the day1 CI chain appends mcConfigMap in its own commit."""
        await _publish(origin)

        def inject(checkout: Path) -> None:
            file = checkout / PATH
            text = file.read_text().replace("\n\n", "\n")  # the injector re-spaces too
            file.write_text(text.rstrip("\n") + "\n\n" + header + "mcConfigMap:\n- name: nm-conf\n  config: |\n    kind: MachineConfig\n")

        _push_onto_branch(origin, tmp_path, "ci", inject, "CI: Update mcConfigMap from mc-files [skip ci]")
        before = _git("rev-parse", BRANCH, cwd=origin).strip()
        published = await _publish(origin)
        assert published.changed is False
        assert _git("rev-parse", BRANCH, cwd=origin).strip() == before

    @pytest.mark.parametrize("overrides", [{"vlan_id": 99}, {"values": EXAMPLE_CLUSTER_FILE.replace("replicas: 2", "replicas: 5")}])
    async def test_a_branch_carrying_a_different_file_is_a_conflict(self, origin, overrides):
        await _publish(origin)
        with pytest.raises(ClusterFileConflictError, match="not this run's file"):
            await _publish(origin, **overrides)

    async def test_a_branch_without_the_file_is_a_conflict(self, origin, tmp_path):
        _push_onto_branch(origin, tmp_path, "foreign", lambda c: (c / "README.md").write_text("x\n"), "someone's branch", branch="main")
        _git("--git-dir", str(origin), "branch", BRANCH, "main", cwd=tmp_path)
        with pytest.raises(ClusterFileConflictError, match="has no"):
            await _publish(origin)

    async def test_a_branch_whose_file_does_not_parse_is_a_conflict(self, origin, tmp_path):
        await _publish(origin)
        _push_onto_branch(origin, tmp_path, "mangle", lambda c: (c / PATH).write_text("a: [\n"), "mangle")
        with pytest.raises(ClusterFileConflictError, match="not valid YAML"):
            await _publish(origin)

    async def test_the_cluster_appearing_on_main_in_between_is_refused(self, origin, tmp_path):
        _push_onto_branch(origin, tmp_path, "merged-elsewhere", lambda c: (c / PATH).write_text("mastertag: 4.1.1\n"), "by hand", branch="main")
        with pytest.raises(ClusterAlreadyProvisionedError):
            await _publish(origin)

    async def test_an_mce_folder_gone_from_main_is_refused(self, origin):
        with pytest.raises(MceFolderNotFoundError, match="no longer"):
            await _publish(origin, mce_dir="sites/site2/mces/ocp4-gone")

    @pytest.mark.parametrize("mce_dir", ["sites/site2/mces/../../x", "sites/site2", "/etc", "sites/site2/mces/a/b"])
    async def test_an_mce_dir_that_is_not_a_day1_path_is_refused(self, origin, mce_dir):
        with pytest.raises(MceFolderNotFoundError, match="not a day1 MCE folder path"):
            await _publish(origin, mce_dir=mce_dir)

    async def test_a_push_rejected_as_non_fast_forward_retries_and_converges(self, origin, tmp_path, monkeypatch):
        """Someone pushes to main between our clone and our push of a NEW branch:
        a new branch from a stale main is still a fast-forward push, so it lands;
        but a branch that exists and moves under a re-run is handled by the
        compare, never a push. Pin the first: the branch is created from the main
        the clone saw, and main's later commit is simply not in it."""
        real = day1_writer.git_repo.commit_and_push
        raced = []

        async def racing(*args, **kwargs):
            if not raced:
                raced.append(True)
                _push_onto_branch(origin, tmp_path, "other", lambda c: (c / "README.md").write_text("x\n"), "concurrent", branch="main")
            return await real(*args, **kwargs)

        monkeypatch.setattr(day1_writer.git_repo, "commit_and_push", racing)
        published = await _publish(origin)
        assert published.changed and _git("rev-parse", BRANCH, cwd=origin).strip() == published.commit_sha

    async def test_the_token_never_lands_in_an_error(self, tmp_path):
        secret = "not-a-real-token-abc123"
        with pytest.raises(ValuesRepoGitError) as caught:
            await day1_writer.publish(
                repo_url="https://x-access-token:" + secret + "@git.invalid/day1.git", token=secret,
                request=_request(), dhcp_values=DHCP,
            )
        assert secret not in str(caught.value)
