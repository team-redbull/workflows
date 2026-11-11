"""The cluster-lifecycle limb's thin activity surface: what each activity hands its module.

activities/cluster_lifecycle/activities.py is settings + logger + one call into a
module with plain parameters; these tests run the real activities through
ActivityEnvironment with the modules replaced, and pin the wiring the module
tests cannot see: which policy ranges the publish picks per type, how the day2
URL and path templates are filled, which provider the settings choose, and
that the answers come back as the models the workflow expects.
"""

from __future__ import annotations

import pytest
from temporalio.testing import ActivityEnvironment

from activities.cluster_lifecycle import activities as limb
from activities.cluster_lifecycle import day1_writer, day2_sigs, git_provider
from shared.models.cluster_lifecycle import (
    ClusterFilePublishRequest,
    ClusterFileRef,
    ClusterTarget,
    ClusterTargetRequest,
    Day2SigRegistration,
    Day2SigRegistrationRequest,
    MergeRequestRef,
    MergeRequestRequest,
    MergeRequestState,
)
from shared.models.segment_lifecycle import DhcpValues, SegmentType

CLUSTER = "ocp4-prep-e2e-site2-a"
MCE_DIR = "sites/site2/mces/ocp4-prep-mce-site2-a"


@pytest.fixture
def env() -> ActivityEnvironment:
    return ActivityEnvironment()


def _publish_request(**overrides) -> ClusterFilePublishRequest:
    fields = dict(cluster=CLUSTER, mce_dir=MCE_DIR, branch=f"provision-cluster/{CLUSTER}", values="mastertag: 4.22.8\n",
                  vlan_id=23, segment="10.20.90.0/24", type=SegmentType.HC)
    fields.update(overrides)
    return ClusterFilePublishRequest(**fields)


async def test_inspect_hands_the_settings_and_the_request_to_the_writer(env, monkeypatch):
    seen = {}

    async def fake_inspect(**kwargs):
        seen.update(kwargs)
        return ClusterTarget(site="site2", mce_dir=MCE_DIR, cluster_name="prep-e2e-site2-a", mastertag="4.22.8", revision="r")

    monkeypatch.setattr(day1_writer, "inspect", fake_inspect)
    target = await env.run(limb.inspect_cluster_target, ClusterTargetRequest(cluster=CLUSTER, mce="ocp4-prep-mce-site2-a", values="x: 1\n"))
    assert target.site == "site2"
    assert seen == {
        "repo_url": "https://git.test/team/gitops-day1-platform-config.git", "token": "test-git-token",
        "cluster": CLUSTER, "mce": "ocp4-prep-mce-site2-a", "values": "x: 1\n",
    }


async def test_publish_derives_the_hc_policy_block_and_passes_it_on(env, monkeypatch):
    seen = {}

    async def fake_publish(**kwargs):
        seen.update(kwargs)
        return ClusterFileRef(relative_path="p", branch=kwargs["request"].branch, commit_sha="c", changed=True, dhcp_values=kwargs["dhcp_values"])

    monkeypatch.setattr(day1_writer, "publish", fake_publish)
    published = await env.run(limb.publish_cluster_file, _publish_request())
    # conftest's policy: HC -> [[1, 10], [241, 254]]
    assert [(e.start_address, e.end_address) for e in published.dhcp_values.exclusions] == [
        ("10.20.90.1", "10.20.90.10"), ("10.20.90.241", "10.20.90.254"),
    ]
    assert published.dhcp_values.network == "10.20.90.0"
    assert seen["token"] == "test-git-token" and seen["request"].cluster == CLUSTER


async def test_a_type_the_policy_does_not_list_excludes_nothing(env, monkeypatch):
    async def fake_publish(**kwargs):
        return ClusterFileRef(relative_path="p", branch="b", commit_sha=None, changed=False, dhcp_values=kwargs["dhcp_values"])

    monkeypatch.setattr(day1_writer, "publish", fake_publish)
    published = await env.run(limb.publish_cluster_file, _publish_request(type=SegmentType.PXE))
    assert published.dhcp_values == DhcpValues(network="10.20.90.0", exclusions=[])


async def test_a_non_slash_24_from_the_pool_fails_before_any_git(env, monkeypatch):
    from temporalio.exceptions import ApplicationError

    async def forbidden(**_kwargs):
        raise AssertionError("git ran for an unusable segment")

    monkeypatch.setattr(day1_writer, "publish", forbidden)
    with pytest.raises(ApplicationError) as caught:
        await env.run(limb.publish_cluster_file, _publish_request(segment="10.20.90.0/23"))
    assert caught.value.type == "UnsupportedSegmentPrefix" and caught.value.non_retryable


async def test_the_merge_request_goes_through_the_provider_the_settings_name(env, monkeypatch):
    seen = {}

    async def fake_ensure(provider, branch, title, description):
        seen.update(provider=provider, branch=branch, title=title, description=description)
        return MergeRequestRef(url="https://g/mr/1", identifier="1", created=True)

    monkeypatch.setattr(git_provider, "ensure_merge_request", fake_ensure)
    ref = await env.run(limb.ensure_merge_request, MergeRequestRequest(branch="b", title="t", description="d"))
    assert ref.identifier == "1"
    # conftest: DAY1_GIT_PROVIDER=gitlab, bound to the day1 repo URL with the token
    assert isinstance(seen["provider"], git_provider.GitLabProvider)
    assert seen["provider"]._base.startswith("https://git.test/api/v4/projects/team%2Fgitops-day1-platform-config")
    assert seen["provider"]._headers == {"PRIVATE-TOKEN": "test-git-token"}
    assert (seen["branch"], seen["title"], seen["description"]) == ("b", "t", "d")


async def test_the_state_read_uses_the_identifier_only(env, monkeypatch):
    seen = {}

    async def fake_state(self, identifier):
        seen["identifier"] = identifier
        return MergeRequestState(state="merged", merged_sha="m")

    monkeypatch.setattr(git_provider.GitLabProvider, "state", fake_state)
    state = await env.run(limb.get_merge_request_state, MergeRequestRef(url="https://g/mr/7", identifier="7", created=False))
    assert (state.state, state.merged_sha, seen["identifier"]) == ("merged", "m", "7")


async def test_day2_registration_fills_the_sig_into_the_url_and_uses_the_day1_token_by_default(env, monkeypatch):
    seen = {}

    async def fake_register(**kwargs):
        seen.update(kwargs)
        return Day2SigRegistration(sig=kwargs["request"].sig, path="p", created=True, commit_sha="d")

    monkeypatch.setattr(day2_sigs, "register", fake_register)
    request = Day2SigRegistrationRequest(sig="redbull", site="site2", env="prep", mce="ocp4-prep-mce-site2-a", cluster=CLUSTER)
    registration = await env.run(limb.register_cluster_in_day2_sig, request)
    assert registration.created
    # conftest: DAY2_SIG_REPO_URL_TEMPLATE=https://git.test/redbull/gitops-day2-prod/sigs/{sig}.git, path "", no DAY2_GIT_TOKEN
    assert seen["repo_url"] == "https://git.test/redbull/gitops-day2-prod/sigs/redbull.git"
    assert seen["path_prefix"] == ""
    assert seen["token"] == "test-git-token"
    assert seen["request"] == request


async def test_a_day2_token_and_a_path_template_are_honoured(env, monkeypatch):
    from shared.settings import ClusterLifecycleActivitySettings

    monkeypatch.setenv("DAY2_GIT_TOKEN", "sig-token")
    monkeypatch.setenv("DAY2_SIG_REPO_URL_TEMPLATE", "https://github.com/team-redbull/gitops-day2-prod.git")
    monkeypatch.setenv("DAY2_SIG_PATH_TEMPLATE", "/sigs/{sig}/")
    monkeypatch.setattr(limb, "_settings", ClusterLifecycleActivitySettings())
    seen = {}

    async def fake_register(**kwargs):
        seen.update(kwargs)
        return Day2SigRegistration(sig="redbull", path="p", created=False, commit_sha=None)

    monkeypatch.setattr(day2_sigs, "register", fake_register)
    await env.run(limb.register_cluster_in_day2_sig, Day2SigRegistrationRequest(sig="redbull", site="s", env="prep", mce="m", cluster="c"))
    assert seen["repo_url"] == "https://github.com/team-redbull/gitops-day2-prod.git"
    assert seen["path_prefix"] == "sigs/redbull"
    assert seen["token"] == "sig-token"
