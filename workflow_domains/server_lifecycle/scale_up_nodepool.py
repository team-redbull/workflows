"""scale-up-nodepool — update-nodepool's child when a kind's total goes up.

The parent has read day1 and the MCE, checked they agree and are balanced, and
planned each NodePool's replicas (nodepool_replicas.plan). This run makes the
change and proves it landed:

  1. committing-day1      — the planned replicas of every NodePool that moves,
                            ONE commit to day1 `main` through the hub limb,
                            compare-and-set on the blob the parent read. day1
                            already saying so (a re-run) is success.
  2. syncing-argo         — the cluster's Argo CD Application synced to exactly
                            that commit; only the moved NodePools may be pending.
  3. verifying-nodepools  — read back on the MCE: every NodePool shows the
                            planned `spec.replicas`, else NodePoolsNotReconciledError.

Done is the NodePools showing the new replicas — not the new nodes Ready
(decided with the operator, 2026-11-03): HyperShift creates the Machines, and
each takes a free Agent from its InfraEnv when one is there. Never: a write to
a NodePool on the MCE (Argo CD applies day1), a wait on the nodes.
"""

from __future__ import annotations

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from shared.models.server_lifecycle import (
        NodePoolProgress,
        NodePoolScaleResult,
        ScaleUpNodePoolRunArgs,
    )
    from workflow_domains.server_lifecycle import nodepool_scaling


@workflow.defn
class ScaleUpNodePoolWorkflow:
    def __init__(self) -> None:
        self._phase = "pending"

    @workflow.query
    def progress(self) -> NodePoolProgress:
        return NodePoolProgress(phase=self._phase, direction="up")

    @workflow.run
    async def run(self, run_args: ScaleUpNodePoolRunArgs) -> NodePoolScaleResult:
        plan = run_args.input
        self._phase = "committing-day1"
        commit = await nodepool_scaling.commit(plan, workflow.info().workflow_id)
        self._phase = "syncing-argo"
        await nodepool_scaling.sync(plan, commit.revision)
        self._phase = "verifying-nodepools"
        live = await nodepool_scaling.verify(plan)
        self._phase = "completed"
        workflow.logger.info(
            "scale-up of %s committed %s at %s: %s",
            plan.cluster, commit.changed, commit.revision,
            ", ".join(f"{np.name} {np.before}→{np.after}" for np in plan.nodepools),
        )
        return NodePoolScaleResult(revision=commit.revision, committed=commit.changed, nodepools=live)
