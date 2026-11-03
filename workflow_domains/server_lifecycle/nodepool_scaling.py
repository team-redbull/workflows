"""Where update-nodepool and its two children run each activity, and the steps both children share.

Workflow code only: everything below runs in the workflow sandbox. Three limbs,
three routes, and none of them new (CLAUDE.md §2: the deployment follows the
credential set, and these already hold what a replicas change needs):

  * on_hub — UPGRADE_GITOPS_ACTIVITY_QUEUE, the cluster-upgrade hub limb: it
    holds the day1 push token and the day1 reader/editor (read_day1_cluster,
    commit_day1_change);
  * on_mce — cluster_upgrade_activity_queue(mce), the cluster-upgrade per-MCE
    limb: NodePool get/list and the Argo CD sync (read_hosted_cluster,
    sync_argo_application);
  * on_server_lifecycle — server_lifecycle_activity_queue(mce), this domain's
    own per-MCE limb: Machines and the hosted cluster's admin kubeconfig, for
    the scale-down's choice of servers.

The shared steps are a scale's second half — commit, sync, read back — which
scale-up-nodepool and scale-down-nodepool run the same way.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError, ChildWorkflowError

with workflow.unsafe.imports_passed_through():
    from shared.consts import (
        UPGRADE_GITOPS_ACTIVITY_QUEUE,
        cluster_upgrade_activity_queue,
        server_lifecycle_activity_queue,
    )
    from shared.exceptions import (
        AmbiguousClusterFileError,
        AmbiguousHostedClusterError,
        ArgoSyncRefusedError,
        BmhPrerequisiteMissingError,
        BmhRequestInvalidError,
        ClusterFileNotFoundError,
        Day1ChangeRejectedError,
        Day1FileChangedError,
        HostedClusterAccessError,
        MachineMarkConflictError,
        NodeMetricsUnavailableError,
        NodePoolsNotReconciledError,
    )
    from shared.interfaces.cluster_upgrade import read_hosted_cluster, sync_argo_application
    from shared.interfaces.upgrade_gitops import commit_day1_change
    from shared.models.cluster_upgrade import (
        ArgoSyncRequest,
        Day1ChangeKind,
        Day1ChangeRequest,
        Day1Commit,
        HostedClusterRequest,
        HostedClusterState,
    )
    from shared.models.server_lifecycle import NodePoolLiveReplicas, NodePoolScalePlan

# Reads and short writes (CLAUDE.md §5's activity budget).
READ_TIMEOUT = timedelta(seconds=90)
# Every git activity clones fresh and keeps each git command under this.
GIT_TIMEOUT = timedelta(seconds=180)
# One Argo CD sync, polled inside the activity until its operation finishes.
SYNC_TIMEOUT = timedelta(minutes=5)
# "No worker polls this queue" — a limb not deployed, or not on this MCE —
# fails the call after this instead of waiting unseen: these are short
# operator runs, as add-server-to-mce and uninstall-server-from-hc are.
SCHEDULE_TO_START_TIMEOUT = timedelta(minutes=5)

# Activity-raised failures no retry can fix, as CLASSES (a wrong name is an
# ImportError at worker start, not a silent "retry forever"). Unlike
# upgrade-cluster's list, the RBAC and admin-kubeconfig errors are here: a
# replicas change is a short run that should fail loudly, as uninstall does.
_PERMANENT_ACTIVITY_ERRORS: tuple[type[Exception], ...] = (
    ClusterFileNotFoundError,
    AmbiguousClusterFileError,
    Day1FileChangedError,
    Day1ChangeRejectedError,
    ArgoSyncRefusedError,
    AmbiguousHostedClusterError,
    HostedClusterAccessError,
    BmhPrerequisiteMissingError,
    BmhRequestInvalidError,
    MachineMarkConflictError,
    NodeMetricsUnavailableError,
)
RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
    non_retryable_error_types=[error.__name__ for error in _PERMANENT_ACTIVITY_ERRORS],
)


def error_type(error: BaseException) -> str | None:
    """The type name of an activity's (or a child's) ApplicationError, or None for anything else."""
    cause = error.cause if isinstance(error, (ActivityError, ChildWorkflowError)) else error
    return cause.type if isinstance(cause, ApplicationError) else None


def error_detail(error: BaseException) -> str:
    """`<type>: <message>` of the failure at the root, without the wrappers around it."""
    root: BaseException = error
    while isinstance(root, (ActivityError, ChildWorkflowError)) and root.cause is not None:
        root = root.cause
    if isinstance(root, ApplicationError):
        return f"{root.type}: {root.message}"
    return f"{type(root).__name__}: {root}"


async def on_hub(activity: Any, arg: Any, *, timeout: timedelta = GIT_TIMEOUT) -> Any:
    return await workflow.execute_activity(
        activity,
        arg,
        task_queue=UPGRADE_GITOPS_ACTIVITY_QUEUE,
        start_to_close_timeout=timeout,
        schedule_to_start_timeout=SCHEDULE_TO_START_TIMEOUT,
        retry_policy=RETRY_POLICY,
    )


async def on_mce(mce_cluster: str, activity: Any, arg: Any, *, timeout: timedelta = READ_TIMEOUT) -> Any:
    return await workflow.execute_activity(
        activity,
        arg,
        task_queue=cluster_upgrade_activity_queue(mce_cluster),
        start_to_close_timeout=timeout,
        schedule_to_start_timeout=SCHEDULE_TO_START_TIMEOUT,
        retry_policy=RETRY_POLICY,
    )


async def on_server_lifecycle(
    mce_cluster: str,
    activity: Any,
    arg: Any,
    *,
    timeout: timedelta = READ_TIMEOUT,
    heartbeat_timeout: timedelta | None = None,
) -> Any:
    return await workflow.execute_activity(
        activity,
        arg,
        task_queue=server_lifecycle_activity_queue(mce_cluster),
        start_to_close_timeout=timeout,
        schedule_to_start_timeout=SCHEDULE_TO_START_TIMEOUT,
        heartbeat_timeout=heartbeat_timeout,
        retry_policy=RETRY_POLICY,
    )


def hosted_cluster_request(plan: NodePoolScalePlan) -> HostedClusterRequest:
    return HostedClusterRequest(
        mce_cluster=plan.mce_cluster,
        cluster=plan.cluster,
        hosted_cluster=plan.hosted_cluster,
        hosted_cluster_namespace=plan.hosted_cluster_namespace,
    )


def changed(plan: NodePoolScalePlan) -> list[str]:
    """The NodePools the plan moves, by name."""
    return sorted(np.name for np in plan.nodepools if np.before != np.after)


async def commit(plan: NodePoolScalePlan, workflow_id: str) -> Day1Commit:
    """The replicas of every NodePool the plan moves, in ONE day1 commit, compare-and-set on the blob read.

    Already so (a re-run after the commit landed) is `changed` False, never a
    conflict. Day1FileChangedError / Day1ChangeRejectedError propagate as the
    ActivityError: the caller decides what comes before failing.
    """
    return await on_hub(
        commit_day1_change,
        Day1ChangeRequest(
            workflow_id=workflow_id,
            mce_cluster=plan.mce_cluster,
            cluster=plan.cluster,
            path=plan.path,
            expected_blob=plan.expected_blob,
            change=Day1ChangeKind.REPLICAS,
            nodepool_replicas={np.name: np.after for np in plan.nodepools if np.before != np.after},
        ),
    )


async def sync(plan: NodePoolScalePlan, revision: str) -> None:
    """The cluster's Application synced to exactly `revision`; only the moved NodePools may be pending."""
    await on_mce(
        plan.mce_cluster,
        sync_argo_application,
        ArgoSyncRequest(
            mce_cluster=plan.mce_cluster,
            cluster=plan.cluster,
            revision=revision,
            allowed_out_of_sync=[f"NodePool/{name}" for name in changed(plan)],
        ),
        timeout=SYNC_TIMEOUT,
    )


async def verify(plan: NodePoolScalePlan) -> list[NodePoolLiveReplicas]:
    """Read back: every NodePool of the plan shows `spec.replicas` = the plan, else NodePoolsNotReconciledError."""
    state: HostedClusterState = await on_mce(plan.mce_cluster, read_hosted_cluster, hosted_cluster_request(plan))
    live = {np.name: np for np in state.nodepools}
    wrong = []
    for nodepool in plan.nodepools:
        found = live.get(nodepool.name)
        if found is None or found.replicas != nodepool.after:
            wrong.append(
                f"{nodepool.name}: {'not on the MCE' if found is None else f'spec.replicas {found.replicas}'}, "
                f"planned {nodepool.after}"
            )
    if not state.found or wrong:
        raise ApplicationError(
            f"after the commit and the Argo CD sync, HostedCluster {plan.hosted_cluster_namespace}/"
            f"{plan.hosted_cluster} does not show the planned replicas: "
            + ("the HostedCluster is not on the MCE" if not state.found else "; ".join(wrong))
            + ". The commit stays in day1 main",
            type=NodePoolsNotReconciledError.__name__,
        )
    return [
        NodePoolLiveReplicas(name=np.name, replicas=live[np.name].replicas, ready_replicas=live[np.name].ready_replicas)
        for np in plan.nodepools
    ]
