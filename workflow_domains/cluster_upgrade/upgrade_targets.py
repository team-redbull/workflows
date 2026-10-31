"""upgrade-targets: what one cluster can be upgraded to, for a person choosing (D22).

A short read, not an upgrade: the "New upgrade" form asks it through
`GET /workflows/cluster-upgrade/upgrade-targets?cluster=` and waits for the
answer. Two reads on the hub limb, both from day1 git: the cluster's file (whose
folder names the hosting MCE, D18, and whose mastertag is the version it runs)
and the upgrade rules for that MCE. Then target_options decides, purely: every
ladder rung newer than the control plane, of a minor the MCE supports, to which
a path resolves. The ladder is what the team mirrored and verified, so a minor
missing from the internal registry is never offered.

Nothing is written, and no cluster is read: planning still checks git against
the cluster and pins every release in the mirror.
"""

from __future__ import annotations

from temporalio import workflow
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from shared.exceptions import UpgradePlanningFailedError
    from shared.interfaces.upgrade_gitops import read_day1_cluster, read_upgrade_rules
    from shared.models.cluster_upgrade import (
        Day1Cluster,
        Day1ClusterRequest,
        UpgradeRules,
        UpgradeRulesRequest,
        UpgradeTargets,
        UpgradeTargetsRunArgs,
    )
    from workflow_domains.cluster_upgrade import gates, upgrade_path
    from workflow_domains.cluster_upgrade.dispatch import on_hub


@workflow.defn
class UpgradeTargetsWorkflow:
    @workflow.run
    async def run(self, run_args: UpgradeTargetsRunArgs) -> UpgradeTargets:
        cluster = run_args.input.cluster
        day1: Day1Cluster = await on_hub(read_day1_cluster, Day1ClusterRequest(cluster=cluster))
        mce_cluster = gates.mce_of_day1_path(day1.path)
        if mce_cluster is None:
            raise ApplicationError(
                f"day1 file {day1.path} is not under sites/<site>/mces/<mce>/hostedClusters/: no MCE",
                type=UpgradePlanningFailedError.__name__,
            )
        rules: UpgradeRules = await on_hub(read_upgrade_rules, UpgradeRulesRequest(mce_cluster=mce_cluster))
        targets = []
        if day1.mastertag:
            targets = upgrade_path.target_options(
                control_plane=day1.mastertag,
                nodepools={np.name: gates.effective_workertag(day1, np.name) or day1.mastertag for np in day1.nodepools},
                ladder=rules.ladder,
                supported_minors=rules.supported_hosted_versions,
                max_worker_skew=rules.max_worker_skew,
            )
        return UpgradeTargets(
            cluster=cluster,
            mce_cluster=mce_cluster,
            mce_version=rules.mce_version,
            control_plane_version=day1.mastertag,
            supported_minors=rules.supported_hosted_versions,
            ladder_revision=rules.revision,
            verified_against_graph=rules.verified_against_graph,
            targets=targets,
        )
