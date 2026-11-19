"""update-nodepool — set how many workers one NodePool of a hosted cluster runs.

The request names the MCE, the cluster (its day1 file), the NodePool and its
replicas, set EXACTLY as asked whatever its siblings hold: no balance rule
(the operator, 2026-11-08). Only the cluster's OWN NodePools count — never
the rest of the MCE.

For a scale-down it may also name HOW the servers that leave are picked —
at most one of (the operator, 2026-11-18):

  * `scale_down_policy` — ranked by it, ahead of the NodePool's own
    `redbull.io/scale-down-policy`;
  * `server_names` — exactly those, as many as the replicas removed.

A run that scales up or changes nothing ignores both, and lists them in its
result's `ignored` — never a failure.

A run recorded before 2026-11-18 may name an `infra_env_kind` instead of
the NodePool (the API no longer takes one, the operator: "no need infraenv
kind"); it still runs as it began: the replicas are the TOTAL over the
cluster's NodePools of that kind — the InfraEnv its agentLabelSelector names
is `K` or `K-<anything>` — spread so no two are more than one apart.

Replicas are owned by the day1 file and changed only here (decided with the
operator, 2026-11-03): the change is a commit to day1 `main`, which Argo CD
applies. Nothing on the MCE is written directly. Every call goes to the MCE's
server-lifecycle worker (the operator, 2026-11-08; nodepool_scaling.on_mce).

Phases:

  1. reading-day1       — the cluster file: its path (whose MCE folder must be
                          the request's), blob, HostedCluster, and every
                          NodePool's replicas and InfraEnv. A file with
                          `problems` is refused.
  2. reading-nodepools  — the HostedCluster's NodePools on the MCE: the one
                          named (a recorded run: the kind's, by their
                          selector's InfraEnv). Live and git must agree BOTH ways —
                          same NodePools, InfraEnvs and replicas — else
                          NodePoolReplicasDriftError, nothing written. A named
                          NodePool on neither side is NodePoolNotFoundError.
  3. planning           — the NodePool takes the number asked (a recorded run
                          by kind: NodePools more than one replica apart are
                          refused, NodePoolsUnbalancedError, and
                          nodepool_replicas.plan spreads the new total).
                          Already so completes with direction `none`. A
                          scale-down's `server_names` must count the replicas
                          removed (ScaleDownServersInvalidError, nothing
                          written); its `scale_down_policy` goes on the
                          shrinking NodePool's plan.
  4. scaling-up / scaling-down — the child of that direction, awaited, on its
                          own id and queue (ABANDON on close: it writes git and
                          marks Machines in an order that must not be cut).

A re-run RESUMES: when day1 already holds what was asked — the named
NodePool's number, or a balanced plan for a recorded run's total — but the
MCE does not show it yet (a child that committed and never synced), the run
hands that plan to the child instead of failing on the difference: its commit
is then a no-op and it carries on from the sync.
"""

from __future__ import annotations

from temporalio import workflow
from temporalio.exceptions import ApplicationError, ChildWorkflowError, WorkflowAlreadyStartedError
from temporalio.workflow import ParentClosePolicy

with workflow.unsafe.imports_passed_through():
    from shared.consts import SCALE_DOWN_NODEPOOL_WORKFLOW_QUEUE, SCALE_UP_NODEPOOL_WORKFLOW_QUEUE
    from shared.exceptions import (
        ClusterNotOnMceError,
        Day1ClusterProblemsError,
        NodePoolKindNotFoundError,
        NodePoolNotFoundError,
        NodePoolReplicasDriftError,
        NodePoolsUnbalancedError,
        NodePoolTargetInvalidError,
        ScaleDownServersInvalidError,
        ScaleNodePoolAlreadyRunningError,
        ScaleNodePoolFailedError,
    )
    from shared.interfaces.cluster_upgrade import read_hosted_cluster
    from shared.interfaces.server_lifecycle import read_day1_nodepools
    from shared.models.cluster_upgrade import (
        Day1Cluster,
        Day1ClusterRequest,
        Day1NodePool,
        HostedClusterRequest,
        HostedClusterState,
        NodePoolState,
    )
    from shared.models.server_lifecycle import (
        NodePoolProgress,
        NodePoolReplicasChange,
        NodePoolScalePlan,
        NodePoolScaleResult,
        ScaleDownNodePoolRunArgs,
        ScaleUpNodePoolRunArgs,
        UpdateNodePoolInput,
        UpdateNodePoolResult,
        UpdateNodePoolRunArgs,
    )
    from shared.upgrade_layout import DAY1_CLUSTER_FILE
    from shared.workflow_ids import scale_down_nodepool_workflow_id, scale_up_nodepool_workflow_id
    from workflow_domains.server_lifecycle import nodepool_replicas, nodepool_scaling

from workflow_domains.server_lifecycle.scale_down_nodepool import ScaleDownNodePoolWorkflow
from workflow_domains.server_lifecycle.scale_up_nodepool import ScaleUpNodePoolWorkflow


def _fail(message: str, error: type[Exception]) -> ApplicationError:
    return ApplicationError(message, type=error.__name__)


@workflow.defn
class UpdateNodePoolWorkflow:
    def __init__(self) -> None:
        self._phase = "pending"
        self._direction: str | None = None
        self._child: str | None = None

    @workflow.query
    def progress(self) -> NodePoolProgress:
        return NodePoolProgress(phase=self._phase, direction=self._direction, child_workflow_id=self._child)

    @workflow.run
    async def run(self, run_args: UpdateNodePoolRunArgs) -> UpdateNodePoolResult:
        request = run_args.input
        if (request.nodepool is None) == (request.infra_env_kind is None):
            raise _fail(
                "update-nodepool takes exactly one of `nodepool` (one NodePool's replicas) and "
                "`infra_env_kind` (a total over that kind's NodePools); nothing was read",
                NodePoolTargetInvalidError,
            )
        if request.scale_down_policy is not None and request.server_names:
            raise _fail(
                "`scale_down_policy` and `server_names` both say which servers leave a scale-down: send "
                "one of them; nothing was read",
                ScaleDownServersInvalidError,
            )

        self._phase = "reading-day1"
        day1: Day1Cluster = await nodepool_scaling.on_mce(
            request.mce_cluster, read_day1_nodepools, Day1ClusterRequest(cluster=request.cluster),
            timeout=nodepool_scaling.GIT_TIMEOUT,
        )
        self._check_day1(request, day1)

        self._phase = "reading-nodepools"
        state: HostedClusterState = await nodepool_scaling.on_mce(
            request.mce_cluster,
            read_hosted_cluster,
            HostedClusterRequest(
                mce_cluster=request.mce_cluster,
                cluster=request.cluster,
                hosted_cluster=day1.hosted_cluster,
                hosted_cluster_namespace=day1.hosted_cluster_namespace,
            ),
        )
        live, git = self._selected_nodepools(request, day1, state)

        self._phase = "planning"
        changes = self._plan(request, live, git)
        down = any(np.after < np.before for np in changes)
        if down:
            changes = self._how_servers_leave(request, changes)
        result = UpdateNodePoolResult(
            mce_cluster=request.mce_cluster,
            cluster=request.cluster,
            infra_env_kind=request.infra_env_kind,
            nodepool=request.nodepool,
            day1_path=day1.path,
            hosted_cluster=day1.hosted_cluster or "",
            hosted_cluster_namespace=day1.hosted_cluster_namespace or "",
            direction="none",
            total_before=sum(np.before for np in changes),
            total_after=sum(np.after for np in changes),
            nodepools=changes,
            ignored=[] if down else self._ignored(request, changes),
        )
        if all(np.before == np.after for np in changes):
            self._direction = "none"
            self._phase = "completed"
            return result

        self._direction = result.direction = "down" if down else "up"
        plan = NodePoolScalePlan(
            mce_cluster=request.mce_cluster,
            cluster=request.cluster,
            infra_env_kind=request.infra_env_kind,
            path=day1.path,
            expected_blob=day1.blob,
            hosted_cluster=result.hosted_cluster,
            hosted_cluster_namespace=result.hosted_cluster_namespace,
            nodepools=changes,
            server_names=list(request.server_names or []) if down else [],
        )
        result.child_workflow_id = self._child = (
            scale_down_nodepool_workflow_id(request.cluster) if down else scale_up_nodepool_workflow_id(request.cluster)
        )
        self._phase = "scaling-down" if down else "scaling-up"
        result.scale = await self._scale(plan, down, self._child)
        self._phase = "completed"
        return result

    def _check_day1(self, request: UpdateNodePoolInput, day1: Day1Cluster) -> None:
        match = DAY1_CLUSTER_FILE.fullmatch(day1.path)
        if match is None or match.group("mce") != request.mce_cluster:
            raise _fail(
                f"day1 keeps {request.cluster} at {day1.path}, not under sites/<site>/mces/"
                f"{request.mce_cluster}/: it does not run on MCE {request.mce_cluster}. Nothing was read there",
                ClusterNotOnMceError,
            )
        if day1.problems:
            raise _fail(
                f"{day1.path} holds what the chart would refuse, so no edit of it can be verified: "
                + "; ".join(day1.problems),
                Day1ClusterProblemsError,
            )
        if not day1.hosted_cluster or not day1.hosted_cluster_namespace or any(
            "infra_env" not in np.model_fields_set for np in day1.nodepools
        ):
            raise _fail(
                f"the server-lifecycle-worker on MCE {request.mce_cluster} predates update-nodepool: its day1 "
                "read reports no HostedCluster or no InfraEnv per NodePool. Upgrade it, then run again",
                NodePoolKindNotFoundError,
            )

    def _selected_nodepools(
        self, request: UpdateNodePoolInput, day1: Day1Cluster, state: HostedClusterState
    ) -> tuple[dict[str, NodePoolState], dict[str, Day1NodePool]]:
        """The NodePools the run is about, live and in git, by name — refused unless they agree both ways."""
        where = f"HostedCluster {day1.hosted_cluster_namespace}/{day1.hosted_cluster}"
        if not state.found:
            raise _fail(
                f"{where}, which {day1.path} renders, is not on MCE {request.mce_cluster}. Nothing was written",
                NodePoolReplicasDriftError,
            )
        if any(
            "infra_env" not in np.model_fields_set or "scale_down_policy" not in np.model_fields_set
            for np in state.nodepools
        ):
            raise _fail(
                f"the server-lifecycle-worker on MCE {request.mce_cluster} predates update-nodepool: its "
                "NodePool read reports no InfraEnv per NodePool. Upgrade it, then run again",
                NodePoolKindNotFoundError,
            )
        if request.nodepool is not None:
            name = request.nodepool
            live = {np.name: np for np in state.nodepools if np.name == name}
            git = {np.name: np for np in day1.nodepools if np.name == name}
            if not live and not git:
                raise _fail(
                    f"{where} has no NodePool {name}, on the MCE or in {day1.path}; its NodePools are "
                    f"{', '.join(sorted(np.name for np in state.nodepools)) or 'none'}. Nothing was written",
                    NodePoolNotFoundError,
                )
            what = f"NodePool {name}"
        else:
            kind = request.infra_env_kind or ""
            live = {np.name: np for np in state.nodepools if nodepool_replicas.belongs_to_kind(np.infra_env, kind)}
            git = {np.name: np for np in day1.nodepools if nodepool_replicas.belongs_to_kind(np.infra_env, kind)}
            if not live and not git:
                seen = sorted({np.infra_env or "none" for np in state.nodepools})
                raise _fail(
                    f"no NodePool of {where} takes Agents from InfraEnv {kind} or {kind}-*: its NodePools' "
                    f"InfraEnvs are {', '.join(seen) or 'none (it has no NodePool)'}. Nothing was written",
                    NodePoolKindNotFoundError,
                )
            what = f"the NodePools of kind {kind}"
        problems = []
        for name, nodepool in sorted(live.items()):
            entry = next((np for np in day1.nodepools if np.name == name), None)
            if entry is None:
                problems.append(f"{name} is on the MCE but not in {day1.path}")
            elif entry.infra_env != nodepool.infra_env:
                problems.append(f"{name} takes InfraEnv {nodepool.infra_env} on the MCE, {entry.infra_env} in day1")
        for name in sorted(set(git) - set(live)):
            problems.append(f"{name} is in {day1.path} but not on the MCE")
        if problems:
            raise _fail(
                f"{what} on the MCE and in day1 disagree: " + "; ".join(problems) + ". Nothing was written",
                NodePoolReplicasDriftError,
            )
        return live, git

    def _plan(
        self, request: UpdateNodePoolInput, live: dict[str, NodePoolState], git: dict[str, Day1NodePool]
    ) -> list[NodePoolReplicasChange]:
        names = sorted(git)
        git_replicas = {name: git[name].replicas for name in names}
        live_replicas = {name: live[name].replicas for name in names}
        unreadable = sorted(name for name, count in git_replicas.items() if count is None)
        if unreadable:
            raise _fail(
                f"the replicas of NodePool {', '.join(unreadable)} in day1 are not a number",
                Day1ClusterProblemsError,
            )
        counts = {name: int(git_replicas[name] or 0) for name in names}
        drifted = sorted(name for name in names if live_replicas[name] != counts[name])
        by_kind = request.nodepool is None

        def change(name: str, before: int, after: int) -> NodePoolReplicasChange:
            return NodePoolReplicasChange(
                name=name, infra_env=git[name].infra_env or "", before=before, after=after,
                scale_down_policy=live[name].scale_down_policy,
            )

        if drifted:
            # The resume: day1 already holds what was asked — a balanced plan for
            # the total, or the named NodePool's number — and only the MCE has
            # not caught up: an earlier child committed and never synced.
            # Anything else is someone else's difference.
            already_asked = (
                sum(counts.values()) == request.replicas and nodepool_replicas.balanced(list(counts.values()))
                if by_kind else counts[request.nodepool or ""] == request.replicas
            )
            if not already_asked or any(live_replicas[name] is None for name in names):
                raise _fail(
                    "the NodePools on the MCE and in day1 disagree on replicas: "
                    + "; ".join(f"{name} {live_replicas[name]} on the MCE, {counts[name]} in day1" for name in drifted)
                    + ". Someone scaled one by hand, or Argo CD has not synced day1 yet. Nothing was written",
                    NodePoolReplicasDriftError,
                )
            workflow.logger.info(
                "update-nodepool %s resumes: day1 already holds %s, the MCE shows %s",
                request.cluster, counts, live_replicas,
            )
            return [change(name, int(live_replicas[name] or 0), counts[name]) for name in names]

        if not by_kind:
            # One NodePool, set exactly as asked: no balance rule (the operator, 2026-11-08).
            return [change(name, counts[name], request.replicas) for name in names]
        if not nodepool_replicas.balanced(list(counts.values())):
            raise _fail(
                f"the NodePools of kind {request.infra_env_kind} are more than one replica apart: "
                + ", ".join(f"{name} {counts[name]}" for name in names)
                + ". Balance them in day1 first, or set one NodePool by name; nothing was written",
                NodePoolsUnbalancedError,
            )
        planned = nodepool_replicas.plan(
            [(name, git[name].infra_env or "", counts[name]) for name in names], request.replicas
        )
        return [change(name, counts[name], planned[name]) for name in names]

    def _how_servers_leave(
        self, request: UpdateNodePoolInput, changes: list[NodePoolReplicasChange]
    ) -> list[NodePoolReplicasChange]:
        """A scale-down's plan, with what the request says about the servers that leave.

        `server_names` must be as many as the replicas removed — checked here,
        before the child starts; which Machine each one is, the child checks.
        A named scale-down ranks nothing, so the shrinking NodePool carries no
        policy; otherwise the request's `scale_down_policy` goes ahead of the
        NodePool's own. Neither (every recorded run): the plan as it was.
        """
        shrinking = [np for np in changes if np.after < np.before]
        if request.server_names:
            removed = sum(np.before - np.after for np in shrinking)
            if len(request.server_names) != removed:
                raise _fail(
                    f"{', '.join(f'{np.name} goes from {np.before} to {np.after}' for np in shrinking)}, so "
                    f"{removed} server(s) leave; `server_names` names {len(request.server_names)} "
                    f"({', '.join(request.server_names)}). Name exactly the servers that leave; nothing was "
                    "written",
                    ScaleDownServersInvalidError,
                )
            policy = None
        elif request.scale_down_policy is not None:
            policy = request.scale_down_policy.value
        else:
            return changes
        return [
            np.model_copy(update={"scale_down_policy": policy}) if np.after < np.before else np
            for np in changes
        ]

    def _ignored(self, request: UpdateNodePoolInput, changes: list[NodePoolReplicasChange]) -> list[str]:
        """What the request said about a scale-down that is not one: noted, never refused."""
        if all(np.before == np.after for np in changes):
            why = "nothing scales down, the replicas are already " + ", ".join(
                f"{np.name} {np.after}" for np in changes
            )
        else:
            why = "the NodePool scales up"
        ignored = []
        if request.scale_down_policy is not None:
            ignored.append(f"scale_down_policy {request.scale_down_policy.value}: {why}")
        if request.server_names:
            ignored.append(f"server_names {', '.join(request.server_names)}: {why}")
        if ignored:
            workflow.logger.info("update-nodepool %s ignores %s", request.cluster, "; ".join(ignored))
        return ignored

    async def _scale(self, plan: NodePoolScalePlan, down: bool, child_id: str) -> NodePoolScaleResult:
        """The child of this direction, awaited on its own id and queue."""
        try:
            if down:
                return await workflow.execute_child_workflow(
                    ScaleDownNodePoolWorkflow.run,
                    ScaleDownNodePoolRunArgs(input=plan),
                    id=child_id,
                    task_queue=SCALE_DOWN_NODEPOOL_WORKFLOW_QUEUE,
                    parent_close_policy=ParentClosePolicy.ABANDON,
                )
            return await workflow.execute_child_workflow(
                ScaleUpNodePoolWorkflow.run,
                ScaleUpNodePoolRunArgs(input=plan),
                id=child_id,
                task_queue=SCALE_UP_NODEPOOL_WORKFLOW_QUEUE,
                parent_close_policy=ParentClosePolicy.ABANDON,
            )
        except WorkflowAlreadyStartedError:
            raise _fail(
                f"{child_id} is already running for {plan.cluster}: let it finish, then run "
                "update-nodepool again",
                ScaleNodePoolAlreadyRunningError,
            ) from None
        except ChildWorkflowError as err:
            raise _fail(
                f"{child_id} failed — {nodepool_scaling.error_detail(err)}. Its history has the detail; "
                "running update-nodepool again carries on from what it left",
                ScaleNodePoolFailedError,
            ) from err
