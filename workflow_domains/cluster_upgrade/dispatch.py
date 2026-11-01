"""Where each cluster-upgrade activity runs, and under which budget — shared by both workflows.

The parent run and its control-plane step child call the same activities with
the same options, so the routing, timeouts and retry policies live once, here.
Workflow code only: everything below runs in the workflow sandbox.

Three limbs, three routes (shared/consts.py):
  * on_hub      — UPGRADE_GITOPS_ACTIVITY_QUEUE: day1 git, the mirror, day2, Brain;
  * on_mce      — cluster_upgrade_activity_queue(mce): reads, Argo CD syncs;
  * start_watch — the same queue, for the long heartbeating watches, started
                  as a handle so the parent can cancel one on abort;
  * diagnose    — evidence on the MCE, the agent on upgrade_agent_activity_queue(mce),
                  the incident on the hub.

Plus what decides a route or recurs on one, so both workflows say it once: the
MCE a cluster file names (hosting_mce), the HostedCluster's own name there
(hosted_cluster_name), the re-pin that stops a step when the mirror moved
(mirror_change), and the race of a cancellable activity against a stop
(until_done).
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError, ChildWorkflowError
from temporalio.workflow import ActivityCancellationType

with workflow.unsafe.imports_passed_through():
    from shared.consts import (
        UPGRADE_GITOPS_ACTIVITY_QUEUE,
        cluster_upgrade_activity_queue,
        upgrade_agent_activity_queue,
    )
    from shared.exceptions import (
        AdminGateNotFoundError,
        AmbiguousClusterFileError,
        ArgoSyncRefusedError,
        ClusterFileNotFoundError,
        Day1ChangeRejectedError,
        Day1FileChangedError,
        ProposedActionRefusedError,
        UpgradePlanningFailedError,
        UpgradeRulesInvalidError,
    )
    from shared.interfaces.cluster_upgrade import collect_evidence
    from shared.interfaces.upgrade_agent import invoke_upgrade_agent
    from shared.interfaces.upgrade_gitops import pin_release_digests, write_incident
    from shared.models.cluster_upgrade import (
        AgentBudget,
        AgentRequest,
        AgentVerdict,
        CheckResult,
        ClusterProfile,
        Day1Cluster,
        EvidenceRequest,
        IncidentRecord,
        IncidentRequest,
        PinnedRelease,
        PinnedReleases,
        PinReleasesRequest,
        ReleaseLayout,
        UpgradeStep,
    )
    from shared.ocp_version import release_tag
    from workflow_domains.cluster_upgrade import gates

# Reads and short writes: CLAUDE.md §5's activity budget.
READ_TIMEOUT = timedelta(seconds=90)
# Every git activity clones fresh and keeps each git command under this
# (activities/segment_lifecycle/values_repo.py, CLAUDE.md §5).
GIT_TIMEOUT = timedelta(seconds=180)
# Pinning HEADs every payload image of every release in the plan.
REGISTRY_TIMEOUT = timedelta(minutes=10)
# An Argo CD sync of one Application, until its operation finishes.
SYNC_TIMEOUT = timedelta(minutes=5)
# Only the agent call has one. Temporal never retries a schedule-to-start
# timeout, so on the domain's other calls it would FAIL a weeks-long run the
# moment a limb pod took five minutes to come back; they wait for a worker
# instead, visible as a pending activity. The agent's timeout is caught and
# answered as needs_human (diagnose), so there it is the "no agent" signal.
AGENT_SCHEDULE_TO_START_TIMEOUT = timedelta(minutes=5)
# Watches heartbeat every poll (30 s); two missed minutes means the worker died,
# and the retry resumes from the last heartbeat's snapshot.
WATCH_HEARTBEAT_TIMEOUT = timedelta(minutes=2)
# A watch returns `deadline` on its own clock; this margin past it is only the
# backstop for a watch that cannot even do that.
WATCH_DEADLINE_MARGIN = timedelta(minutes=10)
AGENT_HEARTBEAT_TIMEOUT = timedelta(seconds=60)
AGENT_TIMEOUT_MARGIN = timedelta(seconds=120)

# Activity-raised failures no retry can fix, as CLASSES (a wrong name is an
# ImportError at worker start, not a silent "retry forever").
#
# DELIBERATELY NOT HERE, unlike uninstall-server-from-hc's list: HostedClusterAccessError,
# BmhPrerequisiteMissingError, BmhRequestInvalidError. A run that lives for weeks
# must never FAIL after approval — the HostedCluster may be mid-upgrade, and a
# failed run leaves it with nobody watching. A refused RBAC or a broken admin
# kubeconfig is a person's to fix: the activity stays pending, its error shown
# on it, until they do, and the run goes on from there. (The hosted READS turn
# an unreachable cluster into `reachable: False`, which the run puts to a
# person as needs-human.) Each type listed here is one the workflow catches
# and answers itself, or one planning fails on before anything is committed.
PERMANENT_ACTIVITY_ERRORS: tuple[type[Exception], ...] = (
    UpgradeRulesInvalidError,
    ClusterFileNotFoundError,
    AmbiguousClusterFileError,
    Day1FileChangedError,
    Day1ChangeRejectedError,
    ArgoSyncRefusedError,
    ProposedActionRefusedError,
    AdminGateNotFoundError,
)
RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
    non_retryable_error_types=[error.__name__ for error in PERMANENT_ACTIVITY_ERRORS],
)
# The one bounded retry in the domain, against CLAUDE.md §5's unbounded rule,
# on Temporal's guidance for LLM calls (core/ai-patterns.md): a second attempt
# covers a dead worker; beyond that, an agent that cannot answer is a person's
# problem, and the gate goes to needs-human rather than waiting forever. The
# invocation id makes the retry safe: the tool server keeps the budget and the
# executed calls per id.
AGENT_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    maximum_interval=timedelta(minutes=1),
    maximum_attempts=2,
)

# Gates a stop letting go of the Brain incident write once the agent has
# answered. Runs recorded before it waited for the write, which retries for as
# long as Brain is away, through an abort.
_INCIDENT_YIELDS_PATCH = "upgrade-agent-incident-yields-to-stop"


def error_type(error: BaseException) -> str | None:
    """The type name of an activity's (or a child's) ApplicationError, or None for anything else."""
    cause = error.cause if isinstance(error, (ActivityError, ChildWorkflowError)) else error
    return cause.type if isinstance(cause, ApplicationError) else None


def error_message(error: BaseException) -> str:
    """What the failure at the root said, without the wrappers around it.

    A child's failure wraps its activity's, which wraps the error raised: the
    person needs the last one (as GET /workflows/runs reports it).
    """
    root: BaseException = error
    while isinstance(root, (ActivityError, ChildWorkflowError)) and root.cause is not None:
        root = root.cause
    return root.message if isinstance(root, ApplicationError) else str(root)


async def on_hub(activity: Any, arg: Any, *, timeout: timedelta = GIT_TIMEOUT) -> Any:
    """One call to the hub limb: day1 git, the mirror registry, day2, Brain."""
    return await workflow.execute_activity(
        activity,
        arg,
        task_queue=UPGRADE_GITOPS_ACTIVITY_QUEUE,
        start_to_close_timeout=timeout,
        retry_policy=RETRY_POLICY,
        # A commit is never abandoned half-way: cancelling the run waits for
        # the push to land or fail, so an abort's own commit cannot race it.
        cancellation_type=ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
    )


async def on_mce(
    mce_cluster: str, activity: Any, arg: Any, *, timeout: timedelta = READ_TIMEOUT
) -> Any:
    """One call to the MCE's own cluster limb."""
    return await workflow.execute_activity(
        activity,
        arg,
        task_queue=cluster_upgrade_activity_queue(mce_cluster),
        start_to_close_timeout=timeout,
        retry_policy=RETRY_POLICY,
        cancellation_type=ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
    )


def hosting_mce(day1: Day1Cluster) -> str:
    """The MCE the cluster file's folder names (D18): where every MCE activity of the run goes.

    A file outside `sites/<site>/mces/<mce>/hostedClusters/` names none, and
    nothing can be planned for it: an ApplicationError (UpgradePlanningFailedError).
    """
    mce_cluster = gates.mce_of_day1_path(day1.path)
    if mce_cluster is None:
        raise ApplicationError(
            f"day1 file {day1.path} is not under sites/<site>/mces/<mce>/hostedClusters/: no MCE to run on",
            type=UpgradePlanningFailedError.__name__,
        )
    return mce_cluster


def hosted_cluster_name(hosted_cluster: str | None, cluster: str) -> str:
    """The HostedCluster's metadata.name, as Argo CD lists it among the Application's resources.

    The chart's own name for it when day1 told it (`openshift-<clusterName>`),
    else — a run planned before day1 reported it — the cluster's.
    """
    return hosted_cluster or cluster


async def mirror_change(version: str, approved: PinnedRelease | None, release: ReleaseLayout) -> str | None:
    """Re-resolve one release right before its commit: why the step must stop (D2), or None.

    Anything but the digests the run was approved with stops it, before
    anything is committed — a tag that moved since approval.
    """
    now: PinnedReleases = await on_hub(
        pin_release_digests, PinReleasesRequest(versions=[version], release=release), timeout=REGISTRY_TIMEOUT
    )
    current = now.pinned.get(version)
    if current == approved:
        return None
    return (
        f"the mirror changed under the run: {release_tag(version)} was approved as {_pinned(approved)} "
        f"and now resolves to {_pinned(current)}"
    )


def _pinned(release: PinnedRelease | None) -> str:
    return f"{release.release} (operator {release.control_plane_operator})" if release else "nothing"


def start_watch(
    mce_cluster: str, activity: Any, arg: Any, *, deadline_minutes: int
) -> workflow.ActivityHandle:
    """Start one long watch on the MCE; returns its handle, which a person's abort may cancel.

    The watch polls and heartbeats inside the activity and returns on its own
    deadline, so a three-hour control-plane upgrade is a few history events
    rather than a poll loop's thousands (Temporal's polling pattern). Retries
    stay unbounded: a dead worker shows as a missed heartbeat, and the next
    attempt resumes from the last heartbeat's snapshot with its clocks.
    """
    return workflow.start_activity(
        activity,
        arg,
        task_queue=cluster_upgrade_activity_queue(mce_cluster),
        start_to_close_timeout=timedelta(minutes=deadline_minutes) + WATCH_DEADLINE_MARGIN,
        heartbeat_timeout=WATCH_HEARTBEAT_TIMEOUT,
        retry_policy=RETRY_POLICY,
        cancellation_type=ActivityCancellationType.TRY_CANCEL,
    )


async def until_done(
    handle: workflow.ActivityHandle, stopped: Callable[[], bool], *, wait_for_cancel: bool = True
) -> bool:
    """Await an activity started TRY_CANCEL, unless `stopped()` turns true first — then cancel it.

    True when the activity ended first: `handle.result()` holds its result, or
    raises its failure. False when the stop came first: the activity is
    cancelled and let go. Only for TRY_CANCEL activities (the watches, the
    agent): a WAIT_CANCELLATION_COMPLETED call, or a child, would run on
    regardless — those stop at a checkpoint instead.

    `wait_for_cancel=False` is the agent call's shape, kept for the runs
    recorded with it: the caller goes on before the cancellation resolves, so
    its next command (the abort's commit) leaves in the same workflow task as
    the cancel. Waiting there would reorder the two for every run recorded
    before. The outcome is still taken, once it resolves.
    """
    await workflow.wait_condition(lambda: handle.done() or stopped())
    if handle.done():
        return True
    if wait_for_cancel:
        await _let_go(handle)
    else:
        handle.cancel()
        handle.add_done_callback(_take_outcome)
    return False


def _take_outcome(handle: workflow.ActivityHandle) -> None:
    """Retrieve a let-go activity's outcome, so it is never reported as an exception nobody retrieved."""
    if not handle.cancelled():
        handle.exception()


async def _let_go(handle: workflow.ActivityHandle) -> None:
    """Cancel a TRY_CANCEL activity, and take its outcome so nothing is left unawaited.

    TRY_CANCEL resolves the handle at once, as cancelled — or with the result
    or failure it reached first — and the activity is told on its next heartbeat.
    """
    handle.cancel()
    try:
        await handle
    except (asyncio.CancelledError, ActivityError):
        pass


@dataclass(frozen=True)
class Diagnosis:
    """What one agent invocation came back with, and where Brain recorded it."""

    invocation_id: str
    verdict: AgentVerdict
    incident_path: str | None


async def diagnose(
    *,
    workflow_id: str,
    mce_cluster: str,
    cluster: str,
    profile: ClusterProfile,
    phase: str,
    step: UpgradeStep,
    from_version: str | None,
    nodepool: str | None,
    gate: str,
    failing_checks: list[CheckResult],
    tier1_enabled: bool,
    stopped: Callable[[], bool] = lambda: False,
    hosted_cluster: str | None = None,
    hosted_cluster_namespace: str | None = None,
) -> Diagnosis | None:
    """Ask the upgrade agent about one failed gate; record the answer in Brain.

    Only ever called on a failure, so a run that passes every gate makes no
    LLM call. The verdict is advice: the caller re-runs the SAME gate at most
    once, and only a person moves the run past a gate that still fails.

    An agent that cannot be reached (both attempts failed) is answered here
    as needs_human, so the run waits for a person rather than failing.

    Returns None when `stopped()` turns true first (an abort): it is checked
    before each call and raced against the agent, whose activity is then
    cancelled — cancelling the caller's task would not stop a call in flight.
    A stop once the agent has answered is raced against the incident write
    the same way: the verdict comes back with `incident_path` None, and the
    caller's next act stops at its own checkpoint.

    `hosted_cluster(_namespace)`: the HostedCluster's own name, where the
    evidence is read (HostedClusterRequest); None is the one named `cluster`.
    """
    invocation_id = f"{workflow_id}/{workflow.uuid4()}"
    if stopped():
        return None
    evidence = await on_mce(
        mce_cluster, collect_evidence,
        EvidenceRequest(
            mce_cluster=mce_cluster, cluster=cluster, nodepool=nodepool,
            hosted_cluster=hosted_cluster, hosted_cluster_namespace=hosted_cluster_namespace,
        ),
    )
    if stopped():
        return None
    budget = AgentBudget()
    request = AgentRequest(
        invocation_id=invocation_id,
        workflow_id=workflow_id,
        mce_cluster=mce_cluster,
        cluster=cluster,
        profile=profile,
        phase=phase,
        step=step,
        from_version=from_version,
        nodepool=nodepool,
        gate=gate,
        failing_checks=failing_checks,
        evidence=evidence,
        budget=budget,
        tier1_enabled=tier1_enabled,
        hosted_cluster=hosted_cluster,
        hosted_cluster_namespace=hosted_cluster_namespace,
    )
    handle = workflow.start_activity(
        invoke_upgrade_agent,
        request,
        task_queue=upgrade_agent_activity_queue(mce_cluster),
        start_to_close_timeout=timedelta(seconds=budget.wall_clock_seconds) + AGENT_TIMEOUT_MARGIN,
        heartbeat_timeout=AGENT_HEARTBEAT_TIMEOUT,
        schedule_to_start_timeout=AGENT_SCHEDULE_TO_START_TIMEOUT,
        retry_policy=AGENT_RETRY_POLICY,
        cancellation_type=ActivityCancellationType.TRY_CANCEL,
    )
    if not await until_done(handle, stopped, wait_for_cancel=False):
        return None
    try:
        verdict: AgentVerdict = handle.result()
    except ActivityError as err:
        workflow.logger.warning("upgrade agent unavailable for %s: %s", invocation_id, error_message(err))
        return Diagnosis(
            invocation_id=invocation_id,
            verdict=AgentVerdict(
                diagnosis=f"the upgrade agent could not be reached: {error_message(err)}",
                confidence="hypothesis",
                verdict="needs_human",
            ),
            incident_path=None,
        )
    # The incident is the hub's to write, retried for as long as Brain is away;
    # a stop does not wait for it. The verdict is returned either way.
    incident = workflow.start_activity(
        write_incident,
        IncidentRequest(
            workflow_id=workflow_id,
            invocation_id=invocation_id,
            mce_cluster=mce_cluster,
            cluster=cluster,
            phase=phase,
            step=step,
            nodepool=nodepool,
            verdict=verdict,
        ),
        task_queue=UPGRADE_GITOPS_ACTIVITY_QUEUE,
        start_to_close_timeout=GIT_TIMEOUT,
        retry_policy=RETRY_POLICY,
        cancellation_type=ActivityCancellationType.TRY_CANCEL,
    )
    await workflow.wait_condition(lambda: incident.done() or stopped())
    if not incident.done() and workflow.patched(_INCIDENT_YIELDS_PATCH):
        await _let_go(incident)
        return Diagnosis(invocation_id=invocation_id, verdict=verdict, incident_path=None)
    record: IncidentRecord = await incident
    return Diagnosis(invocation_id=invocation_id, verdict=verdict, incident_path=record.path)


def clears_gate(diagnosis: Diagnosis) -> bool:
    """Whether the run may re-run the gate on its own: the agent says so and proposes nothing.

    A proposal needs a person's approval first, so it always goes to needs-human.
    """
    verdict = diagnosis.verdict
    return verdict.verdict == "retry_gate" and not verdict.actions_proposed
