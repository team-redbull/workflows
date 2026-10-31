"""Upgrade-gitops activities — the cluster-upgrade domain's HUB limb (DESIGN §2). Thin.

Runs on the hub as `upgrade-gitops-worker`, polling UPGRADE_GITOPS_ACTIVITY_QUEUE,
because it holds what no other limb needs: the day1 git push token, the
release registry's pull credential and the Brain push token. It touches no
Kubernetes API. Each activity reads its settings and hands plain parameters
to one module per dependency:

  * day1_repo.py (+ day1_values.py, upgrade_rules.py) — the day1 values repo:
    the rules, a cluster file, the upgrade's commits to `main`;
  * registry.py (+ oc_release.py) — the release registry: tags, digests, and
    whether a release's payload is complete;
  * day2_repo.py — the day2 repo's version layers (check 4);
  * brain_repo.py — Brain `incidents/` (D10).

Two dependencies are optional, so a lab runs without them: no DAY2_REPO_URL
means check 4 is skipped, saying so; no BRAIN_REPO_URL means an agent verdict
stays in the run's history only. Either is logged and noted in the result,
never an error.
"""

from __future__ import annotations

from temporalio import activity

from activities.upgrade_gitops import brain_repo, day1_repo, day2_repo, registry
from shared.models.cluster_upgrade import (
    Day1ChangeRequest,
    Day1Cluster,
    Day1ClusterRequest,
    Day1Commit,
    Day2Layers,
    Day2LayersRequest,
    IncidentRecord,
    IncidentRequest,
    PinnedReleases,
    PinReleasesRequest,
    ReleaseTags,
    ReleaseTagsRequest,
    UpgradeRules,
    UpgradeRulesRequest,
)
from shared.settings import UpgradeGitopsActivitySettings

_settings = UpgradeGitopsActivitySettings()


def _author() -> day1_repo.GitIdentity:
    return day1_repo.GitIdentity(name=_settings.commit_author_name, email=_settings.commit_author_email)


@activity.defn
async def read_upgrade_rules(request: UpgradeRulesRequest) -> UpgradeRules:
    rules = await day1_repo.read_rules(
        repo_url=_settings.day1_repo_url, token=_settings.day1_git_token, mce_cluster=request.mce_cluster
    )
    activity.logger.info(
        "upgrade rules for %s at %s: %d rungs, release %s (mirrored: %s)",
        request.mce_cluster, rules.revision, len(rules.ladder), rules.release.repo, rules.release.mirrored,
    )
    return rules


@activity.defn
async def read_day1_cluster(request: Day1ClusterRequest) -> Day1Cluster:
    cluster = await day1_repo.read_cluster(
        repo_url=_settings.day1_repo_url, token=_settings.day1_git_token, cluster=request.cluster
    )
    activity.logger.info(
        "day1 %s at %s: HostedCluster %s/%s, %d NodePools, %d problems",
        cluster.path, cluster.revision, cluster.hosted_cluster_namespace, cluster.hosted_cluster,
        len(cluster.nodepools), len(cluster.problems),
    )
    return cluster


@activity.defn
async def list_release_tags(request: ReleaseTagsRequest) -> ReleaseTags:
    tags = await registry.list_tags(
        layout=request.release,
        credentials=registry.load_auth_file(_settings.registry_auth_file),
        verify=_settings.registry_tls_verify,
    )
    activity.logger.info("%d tags in %s", len(tags.tags), registry.release_repository(request.release))
    return tags


@activity.defn
async def pin_release_digests(request: PinReleasesRequest) -> PinnedReleases:
    pinned = await registry.pin_releases(
        versions=request.versions,
        layout=request.release,
        auth_file=_settings.registry_auth_file,
        verify=_settings.registry_tls_verify,
    )
    activity.logger.info(
        "pinned %s; missing %s; %d payload problems",
        sorted(pinned.pinned), pinned.missing, len(pinned.problems),
    )
    return pinned


@activity.defn
async def check_day2_layers(request: Day2LayersRequest) -> Day2Layers:
    if not _settings.day2_repo_url:
        note = "DAY2_REPO_URL is not set: the day2 version layers were not checked"
        activity.logger.warning("%s (%s)", note, request.cluster)
        return Day2Layers(note=note)
    layers = await day2_repo.check_layers(
        repo_url=_settings.day2_repo_url, token=_settings.day2_git_token,
        cluster=request.cluster, versions=request.versions,
    )
    activity.logger.info("day2 layers for %s %s: %d missing", request.cluster, request.versions, len(layers.missing))
    return layers


@activity.defn
async def commit_day1_change(request: Day1ChangeRequest) -> Day1Commit:
    if not _settings.day1_validate_command:
        activity.logger.warning("DAY1_VALIDATE_COMMAND is not set: %s is pushed without day1's validator", request.path)
    commit = await day1_repo.commit_change(
        repo_url=_settings.day1_repo_url,
        token=_settings.day1_git_token,
        request=request,
        validate_command=_settings.day1_validate_command,
        author=_author(),
    )
    activity.logger.info(
        "day1 %s %s for %s: %s at %s",
        request.change.value, "committed" if commit.changed else "already so", request.cluster,
        request.path, commit.revision,
    )
    return commit


@activity.defn
async def write_incident(request: IncidentRequest) -> IncidentRecord:
    if not _settings.brain_repo_url:
        note = "BRAIN_REPO_URL is not set: the agent's verdict is in this run's history only, not in Brain"
        activity.logger.warning("%s (invocation %s)", note, request.invocation_id)
        return IncidentRecord(path="", note=note)
    record = await brain_repo.write_incident(
        repo_url=_settings.brain_repo_url, token=_settings.brain_git_token, request=request, author=_author()
    )
    if record.note:
        activity.logger.warning("incident not written for %s: %s", request.invocation_id, record.note)
    else:
        activity.logger.info("incident for %s: %s", request.invocation_id, record.path)
    return record
