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
"""

from __future__ import annotations

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
        AmbiguousClusterFileError,
        ArgoSyncRefusedError,
        ClusterFileNotFoundError,
        Day1ChangeRejectedError,
        Day1FileChangedError,
        ProposedActionRefusedError,
        UpgradeRulesInvalidError,
    )
    from shared.interfaces.cluster_upgrade import collect_evidence
    from shared.interfaces.upgrade_agent import invoke_upgrade_agent
    from shared.interfaces.upgrade_gitops import write_incident
    from shared.models.cluster_upgrade import (
        AgentBudget,
        AgentRequest,
        AgentVerdict,
        CheckResult,
        ClusterProfile,
        EvidenceRequest,
        IncidentRequest,
        UpgradeStep,
    )

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
SCHEDULE_TO_START_TIMEOUT = timedelta(minutes=5)
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
PERMANENT_ACTIVITY_ERRORS: tuple[type[Exception], ...] = (
    UpgradeRulesInvalidError,
    ClusterFileNotFoundError,
    AmbiguousClusterFileError,
    Day1FileChangedError,
    Day1ChangeRejectedError,
    ArgoSyncRefusedError,
    ProposedActionRefusedError,
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
    """
    invocation_id = f"{workflow_id}/{workflow.uuid4()}"
    if stopped():
        return None
    evidence = await on_mce(
        mce_cluster, collect_evidence,
        EvidenceRequest(mce_cluster=mce_cluster, cluster=cluster, nodepool=nodepool),
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
    )
    handle = workflow.start_activity(
        invoke_upgrade_agent,
        request,
        task_queue=upgrade_agent_activity_queue(mce_cluster),
        start_to_close_timeout=timedelta(seconds=budget.wall_clock_seconds) + AGENT_TIMEOUT_MARGIN,
        heartbeat_timeout=AGENT_HEARTBEAT_TIMEOUT,
        schedule_to_start_timeout=SCHEDULE_TO_START_TIMEOUT,
        retry_policy=AGENT_RETRY_POLICY,
        cancellation_type=ActivityCancellationType.TRY_CANCEL,
    )
    await workflow.wait_condition(lambda: handle.done() or stopped())
    if not handle.done():
        handle.cancel()
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
    record = await on_hub(
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
    )
    return Diagnosis(invocation_id=invocation_id, verdict=verdict, incident_path=record.path)


def clears_gate(diagnosis: Diagnosis) -> bool:
    """Whether the run may re-run the gate on its own: the agent says so and proposes nothing.

    A proposal needs a person's approval first, so it always goes to needs-human.
    """
    verdict = diagnosis.verdict
    return verdict.verdict == "retry_gate" and not verdict.actions_proposed
