"""The cluster-lifecycle limb's thin activity surface: what each activity hands its module.

activities/cluster_lifecycle/activities.py is settings + logger + one call into a
module with plain parameters; these tests run the real activities through
ActivityEnvironment with the modules replaced, and pin the wiring the module
tests cannot see: which policy ranges the publish picks per type, how the day2
URL template is filled, that the merge request goes to GitLab, and
that the answers come back as the models the workflow expects. list_day1_clusters,
the one activity other domains call here, is also run whole against a local bare day1.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
from temporalio.testing import ActivityEnvironment

from activities.cluster_lifecycle import activities as limb
from activities.cluster_lifecycle import day1_clusters, day1_writer, day2_sigs, git_provider
from shared.exceptions import Day1QueryInvalidError
from shared.models.cluster_lifecycle import (
    ClusterFilePublishRequest,
    ClusterFileRef,
    ClusterTarget,
    ClusterTargetRequest,
    Day1ClusterEntry,
    Day1ClusterList,
    Day1ClusterQuery,
    Day2SigRegistration,
    Day2SigRegistrationRequest,
    MergeRequestRef,
    MergeRequestRequest,
    MergeRequestState,
)
from shared.models.segment_lifecycle import DhcpValues, SegmentType
from shared.models.vm_lifecycle import KvClusterList, KvClusterListRequest

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


async def test_the_merge_request_goes_to_gitlab_for_the_day1_repo(env, monkeypatch):
    seen = {}

    async def fake_ensure(provider, branch, title, description):
        seen.update(provider=provider, branch=branch, title=title, description=description)
        return MergeRequestRef(url="https://g/mr/1", identifier="1", created=True)

    monkeypatch.setattr(git_provider, "ensure_merge_request", fake_ensure)
    ref = await env.run(limb.ensure_merge_request, MergeRequestRequest(branch="b", title="t", description="d"))
    assert ref.identifier == "1"
    # GitLab, bound to the day1 repo URL (its path one encoded segment) with the token
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
    # conftest: DAY2_SIG_REPO_URL_TEMPLATE=https://git.test/redbull/gitops-day2-prod/sigs/{sig}.git, no DAY2_GIT_TOKEN
    assert seen["repo_url"] == "https://git.test/redbull/gitops-day2-prod/sigs/redbull.git"
    assert "path_prefix" not in seen
    assert seen["token"] == "test-git-token"
    assert seen["request"] == request


async def test_a_day2_token_is_honoured(env, monkeypatch):
    from shared.settings.cluster_lifecycle import ClusterLifecycleActivitySettings

    monkeypatch.setenv("DAY2_GIT_TOKEN", "sig-token")
    monkeypatch.setattr(limb, "_settings", ClusterLifecycleActivitySettings())
    seen = {}

    async def fake_register(**kwargs):
        seen.update(kwargs)
        return Day2SigRegistration(sig="redbull", path="p", created=False, commit_sha=None)

    monkeypatch.setattr(day2_sigs, "register", fake_register)
    await env.run(limb.register_cluster_in_day2_sig, Day2SigRegistrationRequest(sig="redbull", site="s", env="prep", mce="m", cluster="c"))
    assert seen["repo_url"] == "https://git.test/redbull/gitops-day2-prod/sigs/redbull.git"
    assert seen["token"] == "sig-token"


# --- list_day1_clusters: shared with other domains ----------------------------------------------


async def test_list_day1_clusters_hands_the_settings_and_the_query_to_the_reader(env, monkeypatch):
    seen = {}

    async def fake_list(**kwargs):
        seen.update(kwargs)
        return Day1ClusterList(clusters=[], revision="r")

    monkeypatch.setattr(day1_clusters, "list_clusters", fake_list)
    query = Day1ClusterQuery(site="five", kind="kv", zone="az1")
    listed = await env.run(limb.list_day1_clusters, query)
    assert listed == Day1ClusterList(clusters=[], revision="r")
    assert seen == {
        "repo_url": "https://git.test/team/gitops-day1-platform-config.git", "token": "test-git-token", "query": query,
    }


@pytest.mark.parametrize("query", [
    Day1ClusterQuery(site="../five"),
    Day1ClusterQuery(site="five/mces"),
    Day1ClusterQuery(env="*"),
    Day1ClusterQuery(zone="1"),
    Day1ClusterQuery(zone="az1/.."),
    Day1ClusterQuery(kind="kubevirt"),
])
async def test_a_query_that_could_shape_a_path_is_refused_for_good_before_any_git(env, monkeypatch, query):
    from temporalio.exceptions import ApplicationError

    async def forbidden(**_kwargs):
        raise AssertionError("day1 was read for a refused query")

    monkeypatch.setattr(day1_clusters, "list_clusters", forbidden)
    with pytest.raises(ApplicationError) as caught:
        await env.run(limb.list_day1_clusters, query)
    assert caught.value.type == Day1QueryInvalidError.__name__ and caught.value.non_retryable
    assert caught.value.message.startswith("day1 cluster query refused: ")


def _run_git(*args: str, cwd: Path) -> str:
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout


def _day1_origin(tmp_path: Path, files: dict[str, str]) -> tuple[Path, str]:
    """A local bare day1 whose `main` holds `files`; (its path, main's commit)."""
    bare, seed = tmp_path / "day1.git", tmp_path / "seed"
    _run_git("init", "-q", "--bare", "--initial-branch=main", str(bare), cwd=tmp_path)
    _run_git("clone", "-q", str(bare), str(seed), cwd=tmp_path)
    for relative, content in files.items():
        (seed / relative).parent.mkdir(parents=True, exist_ok=True)
        (seed / relative).write_text(content)
    _run_git("add", "-A", cwd=seed)
    _run_git("-c", "user.name=seed", "-c", "user.email=seed@test", "commit", "-q", "-m", "seed", cwd=seed)
    _run_git("push", "-q", "origin", "HEAD:main", cwd=seed)
    return bare, _run_git("rev-parse", "main", cwd=bare).strip()


_ZONED = 'nodepool:\n  - name: workers\n    nodeLabels:\n      topology.kubernetes.io/zone: "{zone}"\n'


async def test_list_day1_clusters_answers_from_day1_main_with_the_commit_it_read(env, monkeypatch, tmp_path):
    """The real activity over the real reader, against a local bare day1."""
    seed, mce = "sites/five/mces/ocp4-seed-five-1/hostedClusters", "sites/five/mces/ocp4-prod-mce-five-a/hostedClusters"
    origin, head = _day1_origin(tmp_path, {
        f"{seed}/ocp4-prod-kv-five-b.yaml": _ZONED.format(zone="az1"),
        f"{seed}/ocp4-prod-kv-five-a.yaml": _ZONED.format(zone="az2"),
        f"{mce}/ocp4-prod-iac-five-a.yaml": "mastertag: 4.19.1\n",
        "sites/six/mces/ocp4-seed-six-1/hostedClusters/ocp4-prod-kv-six-a.yaml": _ZONED.format(zone="az1"),
    })
    # The settings accept only an https URL; the activity reads these two keys.
    monkeypatch.setattr(limb, "_settings", SimpleNamespace(day1_repo_url=str(origin), redbull_git_token=""))

    listed = await env.run(limb.list_day1_clusters, Day1ClusterQuery(site="five", kind="kv", zone="az1"))
    assert listed.revision == head
    assert [(c.name, c.id, c.zone, c.kind, c.site, c.folder) for c in listed.clusters] == [
        ("ocp4-prod-kv-five-a", "a", "az1", "kv", "five", "ocp4-seed-five-1"),
        ("ocp4-prod-kv-five-b", "b", "az1", "kv", "five", "ocp4-seed-five-1"),
    ]
    first, second = listed.clusters
    assert second.problems == [] and len(first.problems) == 1 and "zone 'az2'" in first.problems[0]  # listed, with its problem
    assert (first.cluster_name, first.hosted_cluster, first.hosted_cluster_namespace, first.path) == (
        "prod-kv-five-a", "openshift-prod-kv-five-a", "hcp-prod-kv-five-a", f"{seed}/ocp4-prod-kv-five-a.yaml")

    everything = await env.run(limb.list_day1_clusters, Day1ClusterQuery())
    assert [c.name for c in everything.clusters] == [
        "ocp4-prod-iac-five-a", "ocp4-prod-kv-five-a", "ocp4-prod-kv-five-b", "ocp4-prod-kv-six-a",
    ]
    by_env = await env.run(limb.list_day1_clusters, Day1ClusterQuery(kind="hc", env="prod"))
    assert [c.name for c in by_env.clusters] == ["ocp4-prod-iac-five-a"]
    assert (await env.run(limb.list_day1_clusters, Day1ClusterQuery(site="nowhere"))).clusters == []
    nothing = await env.run(limb.list_day1_clusters, Day1ClusterQuery(site="five", zone="az7"))
    assert nothing == Day1ClusterList(clusters=[], revision=head)


# --- list_kv_clusters: shared with the vm-lifecycle domain (create-vm) ----------------------------


def _kv_settings(origin: Path, buffer: int = 15) -> SimpleNamespace:
    return SimpleNamespace(day1_repo_url=str(origin), redbull_git_token="", headroom_buffer_percent=buffer)


async def test_list_kv_clusters_answers_a_zones_clusters_oldest_first_with_the_buffer(env, monkeypatch, tmp_path):
    """The real activity over the real reader, against a local bare day1."""
    seed, other = "sites/five/mces/ocp4-seed-five-1/hostedClusters", "sites/five/mces/ocp4-seed-five-2/hostedClusters"
    origin, head = _day1_origin(tmp_path, {
        f"{seed}/ocp4-prod-kv-five-c.yaml": _ZONED.format(zone="az1"),
        f"{seed}/ocp4-prod-kv-five-a.yaml": _ZONED.format(zone="az1"),
        f"{seed}/ocp4-prep-kv-five-b.yaml": _ZONED.format(zone="az1"),     # the id orders, not the lifecycle
        f"{seed}/ocp4-prod-kv-five-d.yaml": _ZONED.format(zone="az2"),     # its NodePools name another zone
        f"{seed}/ocp4-prod-kv-five-e.yaml": "mastertag: 4.19.1\n",         # no NodePool, so no zone at all
        f"{seed}/ocp4-test-kubevirt-five.yaml": _ZONED.format(zone="az1"),  # a name off the convention
        f"{other}/ocp4-prod-kv-five-f.yaml": _ZONED.format(zone="az2"),    # another zone's seed
        "sites/five/mces/ocp4-prod-mce-five-a/hostedClusters/ocp4-test-kubevirt-site4.yaml": _ZONED.format(zone="az1"),
        "sites/six/mces/ocp4-seed-six-1/hostedClusters/ocp4-prod-kv-six-a.yaml": _ZONED.format(zone="az1"),
    })
    monkeypatch.setattr(limb, "_settings", _kv_settings(origin, buffer=22))

    listed = await env.run(limb.list_kv_clusters, KvClusterListRequest(site="five", zone="az1"))
    assert (listed.revision, listed.buffer_percent, listed.folder) == (head, 22, f"{seed}/")
    assert [(c.name, c.id, c.zone) for c in listed.clusters] == [
        ("ocp4-prod-kv-five-a", "a", "az1"),
        ("ocp4-prep-kv-five-b", "b", "az1"),
        ("ocp4-prod-kv-five-c", "c", "az1"),
        ("ocp4-prod-kv-five-d", "d", None),          # listed, never confirmed for this zone
        ("ocp4-prod-kv-five-e", "e", None),
        ("ocp4-test-kubevirt-five", None, None),     # no place in the order, last
    ]
    by_name = {c.name: c for c in listed.clusters}
    assert by_name["ocp4-prod-kv-five-a"].problems == []
    assert "zone 'az2'" in by_name["ocp4-prod-kv-five-d"].problems[0]
    assert "no NodePool" in by_name["ocp4-prod-kv-five-e"].problems[0]
    assert "ocp4-<lifecycle>-kv-<site>-<id>" in by_name["ocp4-test-kubevirt-five"].problems[0]
    first = by_name["ocp4-prod-kv-five-a"]
    assert (first.cluster_name, first.hosted_cluster, first.path) == (
        "prod-kv-five-a", "openshift-prod-kv-five-a", f"{seed}/ocp4-prod-kv-five-a.yaml")
    # What create-vm will use of it: exactly the three confirmed for the zone, oldest first.
    from workflow_domains.vm_lifecycle.create_vm import usable

    assert [c.name for c in listed.clusters if usable(c, "az1")] == [
        "ocp4-prod-kv-five-a", "ocp4-prep-kv-five-b", "ocp4-prod-kv-five-c"]

    az2 = await env.run(limb.list_kv_clusters, KvClusterListRequest(site="five", zone="az2"))
    assert [(c.name, c.zone) for c in az2.clusters] == [("ocp4-prod-kv-five-f", "az2")] and az2.folder == f"{other}/"
    six = await env.run(limb.list_kv_clusters, KvClusterListRequest(site="six", zone="az1"))
    assert [c.name for c in six.clusters] == ["ocp4-prod-kv-six-a"]


async def test_a_zone_without_a_seed_folder_is_an_empty_list_that_still_names_the_folder(env, monkeypatch, tmp_path):
    origin, head = _day1_origin(tmp_path, {"sites/five/values.yaml": "x: 1\n"})
    monkeypatch.setattr(limb, "_settings", _kv_settings(origin))
    listed = await env.run(limb.list_kv_clusters, KvClusterListRequest(site="five", zone="az3"))
    assert listed == KvClusterList(
        clusters=[], buffer_percent=15, revision=head, folder="sites/five/mces/ocp4-seed-five-3/hostedClusters/")
    nowhere = await env.run(limb.list_kv_clusters, KvClusterListRequest(site="nowhere", zone="az1"))
    assert nowhere.clusters == []


async def test_a_duplicate_key_in_a_shared_layer_is_reported_and_blocks_no_cluster(env, monkeypatch, tmp_path):
    seed = "sites/five/mces/ocp4-seed-five-1/hostedClusters"
    origin, _ = _day1_origin(tmp_path, {
        "sites/configValues.yaml": "mastertag: 4.18.1\nmastertag: 4.18.2\n",
        f"{seed}/ocp4-prod-kv-five-a.yaml": _ZONED.format(zone="az1"),
    })
    monkeypatch.setattr(limb, "_settings", _kv_settings(origin))
    listed = await env.run(limb.list_kv_clusters, KvClusterListRequest(site="five", zone="az1"))
    only = listed.clusters[0]
    assert only.zone == "az1" and only.id == "a" and "written twice" in only.problems[0]


async def test_a_new_cluster_file_is_seen_by_the_first_listing_after_its_commit(env, monkeypatch, tmp_path):
    """Git stays the only source of truth: the snapshot is asked for main's head every time."""
    seed = "sites/five/mces/ocp4-seed-five-1/hostedClusters"
    origin, first_head = _day1_origin(tmp_path, {f"{seed}/ocp4-prod-kv-five-a.yaml": _ZONED.format(zone="az1")})
    monkeypatch.setattr(limb, "_settings", _kv_settings(origin))
    request = KvClusterListRequest(site="five", zone="az1")
    assert [c.name for c in (await env.run(limb.list_kv_clusters, request)).clusters] == ["ocp4-prod-kv-five-a"]
    again = await env.run(limb.list_kv_clusters, request)
    assert again.revision == first_head
    work = tmp_path / "work"
    _run_git("clone", "-q", str(origin), str(work), cwd=tmp_path)
    (work / seed / "ocp4-prod-kv-five-b.yaml").write_text(_ZONED.format(zone="az1"))
    _run_git("add", "-A", cwd=work)
    _run_git("-c", "user.name=seed", "-c", "user.email=seed@test", "commit", "-q", "-m", "a standby cluster", cwd=work)
    _run_git("push", "-q", "origin", "HEAD:main", cwd=work)
    after = await env.run(limb.list_kv_clusters, request)
    assert [c.name for c in after.clusters] == ["ocp4-prod-kv-five-a", "ocp4-prod-kv-five-b"] and after.revision != first_head


@pytest.mark.parametrize(("site", "zone"), [("../five", "az1"), ("five", "a"), ("five", "az1/.."), ("fi*e", "az1"), ("five", "az0")])
def test_a_request_that_could_shape_a_path_never_reaches_the_reader(site, zone):
    with pytest.raises(ValueError):
        KvClusterListRequest(site=site, zone=zone)


def test_the_boundary_models_take_what_an_older_or_newer_side_sends():
    """Decoded from history: an unknown field is ignored, an absent one defaults (CLAUDE.md §5)."""
    assert Day1ClusterQuery.model_validate({"future": 1}) == Day1ClusterQuery()
    entry = Day1ClusterEntry.model_validate({
        "name": "n", "site": "s", "folder": "f", "kind": "kv", "cluster_name": "c", "hosted_cluster": "h",
        "hosted_cluster_namespace": "ns", "path": "p", "future": True,
    })
    assert (entry.env, entry.zone, entry.id, entry.problems) == (None, None, None, [])
