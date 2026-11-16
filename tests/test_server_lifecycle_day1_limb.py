"""update-nodepool's day1 calls on the server-lifecycle limb: read_day1_nodepools, commit_day1_replicas.

What matters: the day1 settings are OPTIONAL on this limb — a worker without
them serves every other workflow and fails update-nodepool's call at once,
naming the missing key — and its token changes NodePool replicas and nothing
else. The reading and editing themselves are the cluster-upgrade hub limb's
day1_repo, tested in tests/test_upgrade_gitops_day1.py; here only that they
are reached with this limb's settings.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError
from temporalio.testing import ActivityEnvironment

from activities.server_lifecycle import activities
from shared.exceptions import Day1ChangeRejectedError, Day1NotConfiguredError
from shared.models.cluster_upgrade import Day1ChangeKind, Day1ChangeRequest, Day1Cluster, Day1ClusterRequest, Day1Commit
from shared.settings.server_lifecycle import ServerLifecycleActivitySettings

URL = "https://git.example/day1.git"
PATH = "sites/site1/mces/ocp4-mce-a/hostedClusters/ocp4-alpha.yaml"


def _change(kind: Day1ChangeKind = Day1ChangeKind.REPLICAS) -> Day1ChangeRequest:
    return Day1ChangeRequest(
        workflow_id="scale-up-nodepool-ocp4-alpha", mce_cluster="ocp4-mce-a", cluster="ocp4-alpha",
        path=PATH, expected_blob="blob-1", change=kind, nodepool_replicas={"alpha-zone-a": 4},
    )


@pytest.fixture
def day1(monkeypatch):
    """This limb's day1 settings, set; and day1_repo replaced by a recorder."""
    monkeypatch.setattr(activities._settings, "day1_repo_url", URL)
    monkeypatch.setattr(activities._settings, "day1_git_token", "token-1")
    monkeypatch.setattr(activities._settings, "day1_validate_command", "python3 scripts/validate.py")
    calls: list[tuple[str, dict]] = []

    async def read_cluster(**kwargs):
        calls.append(("read_cluster", kwargs))
        return Day1Cluster(path=PATH, revision="rev-1", blob="blob-1", mastertag="4.18.56")

    async def commit_change(**kwargs):
        calls.append(("commit_change", kwargs))
        return Day1Commit(revision="rev-2", blob="blob-2", changed=True)

    monkeypatch.setattr(activities.day1_repo, "read_cluster", read_cluster)
    monkeypatch.setattr(activities.day1_repo, "commit_change", commit_change)
    return calls


class TestWithDay1Configured:
    async def test_the_read_is_day1_repos_with_this_limbs_settings(self, day1):
        cluster = await ActivityEnvironment().run(activities.read_day1_nodepools, Day1ClusterRequest(cluster="ocp4-alpha"))
        assert cluster.path == PATH
        assert day1 == [("read_cluster", {"repo_url": URL, "token": "token-1", "cluster": "ocp4-alpha"})]

    async def test_a_replicas_commit_is_day1_repos_with_the_validator(self, day1):
        commit = await ActivityEnvironment().run(activities.commit_day1_replicas, _change())
        assert commit.revision == "rev-2" and commit.changed
        ((name, kwargs),) = day1
        assert name == "commit_change"
        assert (kwargs["repo_url"], kwargs["token"], kwargs["validate_command"]) == (
            URL, "token-1", "python3 scripts/validate.py",
        )
        assert kwargs["request"].nodepool_replicas == {"alpha-zone-a": 4}

    @pytest.mark.parametrize("kind", [k for k in Day1ChangeKind if k is not Day1ChangeKind.REPLICAS])
    async def test_any_other_change_is_refused_before_day1_is_touched(self, day1, kind):
        with pytest.raises(Day1ChangeRejectedError, match="replicas only"):
            await ActivityEnvironment().run(activities.commit_day1_replicas, _change(kind))
        assert day1 == []


class TestWithoutDay1:
    @pytest.mark.parametrize(("unset", "named"), [("day1_repo_url", "DAY1_REPO_URL"), ("day1_git_token", "DAY1_GIT_TOKEN")])
    async def test_each_call_fails_at_once_naming_the_missing_key(self, day1, monkeypatch, unset, named):
        monkeypatch.setattr(activities._settings, unset, "")
        with pytest.raises(Day1NotConfiguredError, match=named):
            await ActivityEnvironment().run(activities.read_day1_nodepools, Day1ClusterRequest(cluster="ocp4-alpha"))
        with pytest.raises(Day1NotConfiguredError, match=named):
            await ActivityEnvironment().run(activities.commit_day1_replicas, _change())
        assert day1 == []


class TestTheSettings:
    def test_day1_is_optional_on_this_limb(self, monkeypatch):
        for key in ("DAY1_REPO_URL", "DAY1_GIT_TOKEN", "DAY1_VALIDATE_COMMAND"):
            monkeypatch.delenv(key, raising=False)
        settings = ServerLifecycleActivitySettings(_env_file=None)
        assert (settings.day1_repo_url, settings.day1_git_token, settings.day1_validate_command) == ("", "", "")

    def test_a_day1_url_that_is_not_https_fails_at_start(self, monkeypatch):
        monkeypatch.setenv("DAY1_REPO_URL", "git@github.com:team-redbull/day1.git")
        with pytest.raises(ValidationError, match="must be https://"):
            ServerLifecycleActivitySettings(_env_file=None)
