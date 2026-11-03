"""scale-down-nodepool — update-nodepool's child when a kind's total goes down.

The parent planned each NodePool's replicas. Which SERVERS leave is decided
per NodePool by its own `redbull.io/scale-down-policy` label or annotation,
never by the request (decided with the operator, 2026-11-03):

  * none — nothing is marked: CAPI's MachineSet picks at random, as before;
  * `fewest-pods` | `least-requested` | `least-utilized` — the run ranks the
    NodePool's Machines (scale_down_selection.py) and puts
    `cluster.x-k8s.io/delete-machine` on the ones that leave BEFORE it pushes
    the change. CAPI deletes a marked Machine before any other, under every
    delete policy, so the replicas commit then removes exactly those.

Phases:

  1. selecting-servers    — only when a shrinking NodePool names a policy:
                            read_scale_down_candidates (Machines, their Nodes,
                            Pods, requests and — for `least-utilized` — live
                            use), then the choice. Refused, with nothing
                            written: an unknown policy, a Machine already
                            deleting, another run's (or a person's) mark, a
                            Machine count that is not the replicas.
  2. marking-machines     — the mark on each chosen Machine, read back. An
                            earlier attempt's mark on a Machine NOT chosen now
                            is taken off first.
  3. committing-day1      — as in scale-up. If it is refused (the file changed,
                            the validator), the marks come off again
                            (`unmarking-machines`) before the run fails.
  4. syncing-argo         — as in scale-up. Once the commit has landed the
                            marks are never removed: they are what makes CAPI
                            take the right servers when it applies.
  5. verifying-nodepools  — as in scale-up.
  6. verifying-machines   — only with marks: one heartbeating watch until every
                            marked Machine is deleting or gone. An UNMARKED
                            one deleting is ScaleDownWrongMachineError.

The servers that leave stay in the MCE: CAPI drains each, the Agent provider
unbinds its Agent, and it returns to its InfraEnv as a free Agent. The run
does not wait for that.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from shared.exceptions import (
        AmbiguousClusterFileError,
        ClusterFileNotFoundError,
        Day1ChangeRejectedError,
        Day1FileChangedError,
        NodePoolsNotReconciledError,
        ScaleDownWrongMachineError,
    )
    from shared.interfaces.server_lifecycle import (
        mark_machines_for_deletion,
        read_scale_down_candidates,
        unmark_machines,
        watch_marked_machines,
    )
    from shared.models.server_lifecycle import (
        MachineMarkRequest,
        MarkedMachinesOutcome,
        MarkedMachinesWatch,
        MarkedMachinesWatchRequest,
        NodePoolProgress,
        NodePoolScaleResult,
        ScaleDownCandidates,
        ScaleDownCandidatesRequest,
        ScaleDownChoice,
        ScaleDownNodePoolRef,
        ScaleDownNodePoolRunArgs,
        ScaleDownPolicy,
    )
    from workflow_domains.server_lifecycle import nodepool_scaling, scale_down_selection

# How long CAPI gets to start deleting the marked Machines once the sync has
# applied the new replicas: the MachineSet reconciles within seconds, so this
# is the bound of a machine wait (CLAUDE.md §5), not a drain.
_MARKED_DEADLINE_MINUTES = 10
_WATCH_HEARTBEAT_TIMEOUT = timedelta(minutes=2)
_WATCH_MARGIN = timedelta(minutes=5)

# A commit failing with one of these pushed nothing: the marks come off again.
_NOTHING_PUSHED = frozenset(
    error.__name__
    for error in (Day1FileChangedError, Day1ChangeRejectedError, ClusterFileNotFoundError, AmbiguousClusterFileError)
)


@workflow.defn
class ScaleDownNodePoolWorkflow:
    def __init__(self) -> None:
        self._phase = "pending"

    @workflow.query
    def progress(self) -> NodePoolProgress:
        return NodePoolProgress(phase=self._phase, direction="down")

    @workflow.run
    async def run(self, run_args: ScaleDownNodePoolRunArgs) -> NodePoolScaleResult:
        plan = run_args.input
        mark = workflow.info().workflow_id
        chosen, hcp_namespace = await self._choose(run_args)

        marks = (
            MachineMarkRequest(namespace=hcp_namespace, machines=[c.machine for c in chosen], mark=mark)
            if chosen else None
        )
        if marks is not None:
            self._phase = "marking-machines"
            await nodepool_scaling.on_server_lifecycle(plan.mce_cluster, mark_machines_for_deletion, marks)

        self._phase = "committing-day1"
        try:
            commit = await nodepool_scaling.commit(plan, mark)
        except ActivityError as err:
            if marks is not None and nodepool_scaling.error_type(err) in _NOTHING_PUSHED:
                self._phase = "unmarking-machines"
                await nodepool_scaling.on_server_lifecycle(plan.mce_cluster, unmark_machines, marks)
            raise

        self._phase = "syncing-argo"
        await nodepool_scaling.sync(plan, commit.revision)
        self._phase = "verifying-nodepools"
        live = await nodepool_scaling.verify(plan)

        deleting: list[str] = []
        if chosen:
            self._phase = "verifying-machines"
            deleting = await self._watch(run_args, hcp_namespace, chosen)

        self._phase = "completed"
        workflow.logger.info(
            "scale-down of %s committed %s at %s: %s; chose %s",
            plan.cluster, commit.changed, commit.revision,
            ", ".join(f"{np.name} {np.before}→{np.after}" for np in plan.nodepools),
            [f"{c.nodepool}/{c.machine} ({c.reason})" for c in chosen] or "none (CAPI picks)",
        )
        return NodePoolScaleResult(
            revision=commit.revision, committed=commit.changed, nodepools=live,
            chosen=chosen, deleting=deleting,
        )

    async def _choose(self, run_args: ScaleDownNodePoolRunArgs) -> tuple[list[ScaleDownChoice], str]:
        """The Machines the shrinking NodePools with a policy lose; ([], "") when none has one."""
        plan = run_args.input
        mark = workflow.info().workflow_id
        shrinking = {np.name: np for np in plan.nodepools if np.after < np.before}
        invalid = scale_down_selection.invalid_policies({n: np.scale_down_policy for n, np in shrinking.items()})
        if invalid is not None:
            raise ApplicationError(invalid.message, type=invalid.error.__name__)
        policies: dict[str, ScaleDownPolicy] = {}
        for name, nodepool in sorted(shrinking.items()):
            policy = scale_down_selection.policy_of(nodepool.scale_down_policy)
            if policy is not None:
                policies[name] = policy
        if not policies:
            return [], ""

        self._phase = "selecting-servers"
        candidates: ScaleDownCandidates = await nodepool_scaling.on_server_lifecycle(
            plan.mce_cluster,
            read_scale_down_candidates,
            ScaleDownCandidatesRequest(
                hosted_cluster_namespace=plan.hosted_cluster_namespace,
                hosted_cluster=plan.hosted_cluster,
                nodepools=[ScaleDownNodePoolRef(name=name, policy=policy) for name, policy in policies.items()],
            ),
        )
        shrink = {name: (shrinking[name].before, shrinking[name].before - shrinking[name].after) for name in policies}
        refusal = scale_down_selection.refusal(candidates.machines, shrink, mark)
        if refusal is not None:
            raise ApplicationError(refusal.message, type=refusal.error.__name__)
        chosen = scale_down_selection.choose(candidates.machines, shrink, policies, mark)

        # An earlier attempt's mark on a Machine not chosen now would make CAPI
        # take that one too: off it comes, before anything new is marked.
        picked = {choice.machine for choice in chosen}
        stale = sorted(m.name for m in candidates.machines if m.delete_mark == mark and m.name not in picked)
        if stale:
            await nodepool_scaling.on_server_lifecycle(
                plan.mce_cluster, unmark_machines,
                MachineMarkRequest(namespace=candidates.hcp_namespace, machines=stale, mark=mark),
            )
        return chosen, candidates.hcp_namespace

    async def _watch(
        self, run_args: ScaleDownNodePoolRunArgs, hcp_namespace: str, chosen: list[ScaleDownChoice]
    ) -> list[str]:
        """The marked Machines deleting or gone; a wrong one or the deadline fails the run."""
        plan = run_args.input
        watch: MarkedMachinesWatch = await nodepool_scaling.on_server_lifecycle(
            plan.mce_cluster,
            watch_marked_machines,
            MarkedMachinesWatchRequest(
                hcp_namespace=hcp_namespace,
                hosted_cluster_namespace=plan.hosted_cluster_namespace,
                nodepools=sorted({c.nodepool for c in chosen}),
                marked=[c.machine for c in chosen],
                deadline_minutes=_MARKED_DEADLINE_MINUTES,
            ),
            timeout=timedelta(minutes=_MARKED_DEADLINE_MINUTES) + _WATCH_MARGIN,
            heartbeat_timeout=_WATCH_HEARTBEAT_TIMEOUT,
        )
        if watch.result is MarkedMachinesOutcome.WRONG_MACHINE:
            raise ApplicationError(
                f"CAPI is deleting Machine {', '.join(watch.unexpected)} in {hcp_namespace}, which this "
                f"run did not mark (it marked {', '.join(c.machine for c in chosen)}). It cannot be undone: "
                "its drain has begun",
                type=ScaleDownWrongMachineError.__name__,
            )
        if watch.result is MarkedMachinesOutcome.DEADLINE:
            raise ApplicationError(
                f"{_MARKED_DEADLINE_MINUTES} minutes after the sync, marked Machine "
                f"{', '.join(watch.pending)} in {hcp_namespace} is not deleting. The commit and the marks "
                "stay: CAPI removes the marked Machines first whenever its MachineSet scales down",
                type=NodePoolsNotReconciledError.__name__,
            )
        return watch.deleting
