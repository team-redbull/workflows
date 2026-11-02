"""upgrade-targets: what one cluster can be upgraded to, for a person choosing (D22).

A short read, not an upgrade: the "New upgrade" form asks it through
`GET /workflows/cluster-upgrade/upgrade-targets?cluster=` and waits for the
answer. Two reads on the hub limb, both from day1 git: the cluster's file (whose
folder names the hosting MCE, D18, and whose mastertag is the version it runs)
and the upgrade rules for that MCE. Then target_options decides, purely: every
ladder rung newer than the control plane, of a minor the MCE supports, to which
a path resolves. The ladder is what the team mirrored and verified, so a minor
missing from the internal registry is never offered.

It also hands back the file's NodePools and the order a run would roll them in,
for the form's order and maxUnavailable fields (D28).

Nothing is written, and no cluster is read: planning still checks git against
the cluster and pins every release in the mirror. A file the parser refused
(`problems`), a mastertag or a ladder that is no version FAILS the run with
the reason — the route's 409 — never a workflow task retried until the API's
timeout.
"""

from __future__ import annotations

from temporalio import workflow
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from shared.exceptions import UpgradePathRefusedError, UpgradePlanningFailedError
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
    from workflow_domains.cluster_upgrade.dispatch import hosting_mce, on_hub


@workflow.defn
class UpgradeTargetsWorkflow:
    @workflow.run
    async def run(self, run_args: UpgradeTargetsRunArgs) -> UpgradeTargets:
        cluster = run_args.input.cluster
        workflow.logger.info("Listing the upgrade targets of cluster=%s", cluster)
        day1: Day1Cluster = await on_hub(read_day1_cluster, Day1ClusterRequest(cluster=cluster))
        mce_cluster = hosting_mce(day1)
        if day1.problems:
            # Planning's check 3: a file read wrongly would offer wrong targets.
            raise ApplicationError(
                f"day1 file {day1.path} cannot be read as the chart renders it:\n" + "\n".join(day1.problems),
                type=UpgradePlanningFailedError.__name__,
            )
        rules: UpgradeRules = await on_hub(read_upgrade_rules, UpgradeRulesRequest(mce_cluster=mce_cluster))
        targets = []
        if day1.mastertag:
            try:
                targets = upgrade_path.target_options(
                    control_plane=day1.mastertag,
                    nodepools={np.name: gates.effective_workertag(day1, np.name) for np in day1.nodepools},
                    ladder=rules.ladder,
                    supported_minors=rules.supported_hosted_versions,
                    max_worker_skew=rules.max_worker_skew,
                )
            except ValueError as err:
                # A mastertag or a ladder rung that is no release, or a rung
                # listed twice: raised from workflow code, a bare ValueError
                # would fail the workflow TASK and retry it until the timeout.
                raise ApplicationError(
                    f"no upgrade targets for {cluster}: {err}", type=UpgradePathRefusedError.__name__
                ) from None
        names = [np.name for np in day1.nodepools]
        try:
            order = gates.nodepool_order(requested=None, day1=day1, names=names)
        except ValueError:
            # upgrade.nodePoolOrder does not name each NodePool once: planning
            # fails on it unless the request orders them, so offer file order.
            order = names
        workflow.logger.info(
            "cluster=%s on %s at %s: %d upgrade targets (%s)",
            cluster,
            mce_cluster,
            day1.mastertag or "no mastertag",
            len(targets),
            ", ".join(target.version for target in targets) or "none",
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
            nodepools=day1.nodepools,
            nodepool_order=order,
        )
