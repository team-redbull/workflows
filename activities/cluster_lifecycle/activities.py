"""Cluster-lifecycle activities — the domain's one hub limb (`cluster-lifecycle-worker`). Thin.

Polls CLUSTER_LIFECYCLE_ACTIVITY_QUEUE on the hub. Holds no Kubernetes
credential: it reaches the day1 values repo with the day1 git token (a clone,
a push, and the repo's git provider API for the merge request —
day1_writer.py, git_provider.py) and the day2 sig repos with the day2 token
(day2_sigs.py). Each activity reads its settings and hands plain parameters to
one module per dependency; what a cluster file must look like is
cluster_values.py, and the DHCP block it gets is dhcp_values.py.

Budgets, all under the workflow's timeouts (workflow_domains/cluster_lifecycle/
provision_cluster.py): git under the 180 s start_to_close (git_repo's 150 s
per command), the provider's HTTP calls at 60 s under the 90 s start_to_close.
"""

from __future__ import annotations

from temporalio import activity

from activities.cluster_lifecycle import day1_writer, day2_sigs, git_provider
from activities.cluster_lifecycle.dhcp_values import build_dhcp_values
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
from shared.settings import ClusterLifecycleActivitySettings

_settings = ClusterLifecycleActivitySettings()


def _provider() -> git_provider.Provider:
    return git_provider.provider_for(_settings.day1_git_provider, _settings.day1_repo_url, _settings.redbull_git_token)


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
        repo_url=_settings.day2_sig_repo_url(request.sig), token=_settings.day2_token,
        path_prefix=_settings.day2_sig_path_prefix(request.sig), request=request,
    )
    activity.logger.info(
        "day2 sig %s: %s %s", request.sig, "created" if registration.created else "already has", registration.path
    )
    return registration
