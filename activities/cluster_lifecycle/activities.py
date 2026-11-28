"""Cluster-lifecycle activities — the domain's one hub limb (`cluster-lifecycle-worker`). Thin.

Polls CLUSTER_LIFECYCLE_ACTIVITY_QUEUE on the hub. Holds no Kubernetes
credential: it reaches the day1 values repo with the day1 git token (a clone,
a push, and GitLab's API for the merge request —
day1_writer.py, git_provider.py) and the day2 sig repos with the day2 token
(day2_sigs.py), and the shortcuts service with its own bearer token
(shortcuts_api.py). Each activity reads its settings and hands plain parameters to
one module per dependency; what a cluster file must look like is
cluster_values.py, and the DHCP block it gets is dhcp_values.py.

Two activities are not provision-cluster's, and are here because this limb
holds the day1 token (CLAUDE.md §2), both in day1_clusters.py over
activities/day1_repo.py's `Day1`: list_day1_clusters answers other domains'
questions about day1, and list_kv_clusters gives create-vm a zone's KV
clusters, oldest first, together with the placement buffer
(HEADROOM_BUFFER_PERCENT: ONE value for every KV cluster, this worker's).

Budgets, all under the workflow's timeouts (workflow_domains/cluster_lifecycle/
provision_cluster.py): git under the 180 s start_to_close (git_repo's 150 s
per command), GitLab's HTTP calls at 60 s under the 90 s start_to_close, the
shortcuts service's three calls at 20 s each under its 90 s.
"""

from __future__ import annotations

from temporalio import activity
from temporalio.exceptions import ApplicationError

from activities import day1_repo
from activities.cluster_lifecycle import day1_clusters, day1_writer, day2_sigs, git_provider, mce_folder, shortcuts_api
from activities.cluster_lifecycle.dhcp_values import build_dhcp_values
from shared.exceptions import Day1QueryInvalidError
from shared.models.cluster_lifecycle import (
    ClusterFilePublishRequest,
    ClusterFileRef,
    ClusterShortcut,
    ClusterShortcutRequest,
    ClusterTarget,
    ClusterTargetRequest,
    Day1ClusterList,
    Day1ClusterQuery,
    Day2SigRegistration,
    Day2SigRegistrationRequest,
    MergeRequestRef,
    MergeRequestRequest,
    MergeRequestState,
)
from shared.models.upi_lifecycle import MceFolderPublishRequest, MceFolderRef
from shared.models.vm_lifecycle import KvClusterList, KvClusterListRequest
from shared.settings.cluster_lifecycle import ClusterLifecycleActivitySettings

_settings = ClusterLifecycleActivitySettings()


def _provider() -> git_provider.GitLabProvider:
    return git_provider.GitLabProvider(_settings.day1_repo_url, _settings.redbull_git_token)


@activity.defn
async def inspect_cluster_target(request: ClusterTargetRequest) -> ClusterTarget:
    target = await day1_writer.inspect(
        repo_url=_settings.day1_repo_url, token=_settings.redbull_git_token,
        cluster=request.cluster, mce=request.mce, values=request.values,
    )
    activity.logger.info(
        "day1 %s: %s goes in %s (site %s, clusterName %s, mastertag %s, configs %s, day2 sigs %s)",
        target.revision, request.cluster, target.mce_dir, target.site, target.cluster_name, target.mastertag,
        ", ".join(target.config_names) or "none", ", ".join(target.day2_sigs) or "none",
    )
    return target


@activity.defn
async def publish_cluster_file(request: ClusterFilePublishRequest) -> ClusterFileRef:
    # The exclusion policy is per segment type — pick this allocation's. A type
    # the map does not list excludes NOTHING: an operator lists a type when it
    # reserves part of its /24 and leaves it out otherwise.
    dhcp_values = build_dhcp_values(request.segment, _settings.dhcp_exclusion_octet_ranges.get(request.type, []))
    published = await day1_writer.publish(
        repo_url=_settings.day1_repo_url, token=_settings.redbull_git_token, request=request, dhcp_values=dhcp_values
    )
    if published.changed:
        activity.logger.info(
            "pushed %s to %s (commit %s): vlan %d on %s", published.relative_path, published.branch,
            published.commit_sha, request.vlan_id, dhcp_values.network,
        )
    else:
        activity.logger.info("%s already carries %s as this run would write it — nothing pushed",
                             published.branch, published.relative_path)
    return published


@activity.defn
async def ensure_merge_request(request: MergeRequestRequest) -> MergeRequestRef:
    merge_request = await git_provider.ensure_merge_request(_provider(), request.branch, request.title, request.description)
    activity.logger.info(
        "%s merge request %s from %s: %s", "created" if merge_request.created else "found open",
        merge_request.identifier, request.branch, merge_request.url,
    )
    return merge_request


@activity.defn
async def get_merge_request_state(merge_request: MergeRequestRef) -> MergeRequestState:
    state = await _provider().state(merge_request.identifier)
    activity.logger.info("merge request %s is %s%s", merge_request.identifier, state.state,
                         f" ({state.merged_sha})" if state.merged_sha else "")
    return state


@activity.defn
async def register_cluster_in_day2_sig(request: Day2SigRegistrationRequest) -> Day2SigRegistration:
    registration = await day2_sigs.register(
        repo_url=_settings.day2_sig_repo_url(request.sig), token=_settings.day2_token, request=request,
    )
    activity.logger.info(
        "day2 sig %s: %s %s", request.sig, "created" if registration.created else "already has", registration.path
    )
    return registration


@activity.defn
async def ensure_cluster_shortcut(request: ClusterShortcutRequest) -> ClusterShortcut:
    shortcut = await shortcuts_api.ensure(
        base_url=_settings.shortcuts_api_url, token=_settings.shortcuts_api_token, owner=_settings.shortcut_owner,
        shortcut=request.shortcut, url=_settings.cluster_console_url(request.cluster),
    )
    activity.logger.info(
        "shortcut %s → %s %s (owner %s)", shortcut.shortcut, shortcut.url,
        "created" if shortcut.created else "already there", shortcut.owner,
    )
    return shortcut


# --- shared with other domains ----------------------------------------------------------


@activity.defn
async def list_day1_clusters(query: Day1ClusterQuery) -> Day1ClusterList:
    try:
        day1_repo.check_filters(site=query.site, env=query.env, kind=query.kind, zone=query.zone)
    except ValueError as err:
        # Refused before any git call, and permanently for every caller: the
        # activity is shared, so the classification travels with the error.
        raise ApplicationError(
            f"day1 cluster query refused: {err}", type=Day1QueryInvalidError.__name__, non_retryable=True
        ) from None
    listed = await day1_clusters.list_clusters(
        repo_url=_settings.day1_repo_url, token=_settings.redbull_git_token, query=query
    )
    activity.logger.info(
        "day1 %s: %d clusters for site=%s env=%s kind=%s zone=%s (%d with problems)",
        listed.revision, len(listed.clusters), query.site or "*", query.env or "*", query.kind, query.zone or "*",
        sum(1 for cluster in listed.clusters if cluster.problems),
    )
    return listed


@activity.defn
async def list_kv_clusters(request: KvClusterListRequest) -> KvClusterList:
    if request.lifecycle is None:
        # Only a run started before the lifecycle filter (2026-11-22) asks without one:
        # refused, never guessed — a prod MCE's VM must not land on a test KV cluster.
        raise ApplicationError(
            f"KV clusters of site {request.site} zone {request.zone} asked for without a lifecycle: the run "
            "predates the lifecycle filter (its MCE's lifecycle picks the seeds). Start it again.",
            type=Day1QueryInvalidError.__name__, non_retryable=True,
        )
    listed = await day1_clusters.list_kv_clusters(
        repo_url=_settings.day1_repo_url, token=_settings.redbull_git_token, site=request.site, zone=request.zone,
        lifecycle=request.lifecycle, buffer_percent=_settings.headroom_buffer_percent,
    )
    usable = [entry.name for entry in listed.clusters if entry.zone == request.zone and entry.id]
    activity.logger.info(
        "day1 %s lists %d %s KV clusters for site %s zone %s in %s (buffer %d%%): %s; %d not usable",
        listed.revision, len(listed.clusters), request.lifecycle, request.site, request.zone, listed.folder,
        listed.buffer_percent,
        ", ".join(usable) or "none usable", len(listed.clusters) - len(usable),
    )
    return listed


# --- shared with the upi-lifecycle domain -------------------------------------------------


@activity.defn
async def publish_mce_folder(request: MceFolderPublishRequest) -> MceFolderRef:
    published = await mce_folder.publish(
        repo_url=_settings.day1_repo_url, token=_settings.redbull_git_token, request=request
    )
    if published.on_main:
        activity.logger.info("%s is already at day1 main as this run would write it — nothing pushed", published.folder)
    elif published.changed:
        activity.logger.info("pushed %s to %s (commit %s)", published.folder, published.branch, published.commit_sha)
    else:
        activity.logger.info("%s already carries %s as this run would write it — nothing pushed",
                             published.branch, published.folder)
    return published
