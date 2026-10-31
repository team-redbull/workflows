"""Upgrade-gitops activity signatures — the cluster-upgrade domain's HUB limb.

Everything here runs on ONE hub deployment, `upgrade-gitops-worker`, on the
queue UPGRADE_GITOPS_ACTIVITY_QUEUE, because it holds the credentials nothing
else needs: the day1 git push token, the mirror registry pull credential and
the Brain push token. None of these activities touches a Kubernetes API.

The cluster-upgrade domain has three limbs, one per credential set, and so one
interface module per limb (see shared/consts.py): this one, cluster_upgrade.py
(per MCE) and upgrade_agent.py (per MCE).
"""

from __future__ import annotations

from temporalio import activity

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


@activity.defn
async def read_upgrade_rules(request: UpgradeRulesRequest) -> UpgradeRules:
    """day1 `upgrade/ladder.yaml` and `upgrade/mces/<mce>.yaml`, validated, at `main`.

    Raises UpgradeRulesInvalidError (permanent) when either is missing or fails
    `scripts/validate_upgrade_rules.py`.
    """
    ...


@activity.defn
async def read_day1_cluster(request: Day1ClusterRequest) -> Day1Cluster:
    """The cluster's day1 file at `main`, its blob id, and what its parser refused.

    Raises ClusterFileNotFoundError / AmbiguousClusterFileError (permanent).
    """
    ...


@activity.defn
async def list_release_tags(request: ReleaseTagsRequest) -> ReleaseTags:
    """Every tag of the mirror's release repository."""
    ...


@activity.defn
async def pin_release_digests(request: PinReleasesRequest) -> PinnedReleases:
    """Resolve each release (and its control-plane-operator image) to a digest.

    Checks the payload is complete: `oc adm release info` on the digest, and a
    manifest HEAD on every image it references. Reports what is missing; never
    raises for it.
    """
    ...


@activity.defn
async def check_day2_layers(request: Day2LayersRequest) -> Day2Layers:
    """For each version, every pinned day2 chart of the cluster has `ocp-versions/<v>/`."""
    ...


@activity.defn
async def commit_day1_change(request: Day1ChangeRequest) -> Day1Commit:
    """Edit exactly the keys of one change, validate, commit and push to day1 `main`.

    Compare-and-set on `expected_blob` (Day1FileChangedError); the day1
    validator refusing the result is Day1ChangeRejectedError. Both permanent.
    The file already saying so is success with `changed` False. A rejected
    push is retried from a fresh clone, never forced.
    """
    ...


@activity.defn
async def write_incident(request: IncidentRequest) -> IncidentRecord:
    """Write one agent verdict to Brain `incidents/`, validated, pushed to `main` (D10).

    Real cluster and MCE names never reach Brain: the record uses `<cluster>`
    and `<mce>`, because Brain's CI refuses real names.
    """
    ...
