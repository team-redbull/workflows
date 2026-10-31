"""upgrade-cluster — takes ONE HostedCluster from its current release to a target, through day1 git.

Every change is a day1 commit followed by a sync of the cluster's one Argo CD
Application on its hosting MCE (DESIGN D1): the run never patches a
HostedCluster or NodePool, and Argo CD stays their only writer. The control
plane moves one minor at a time along the ladder (upgrade_path.py); NodePools
follow one at a time, at a person's pace, and may wait weeks between two.

Shape of the run (the `progress` query exposes all of it, `waiting_for` says
what a person has to do next):

  1. planning           — rules, the day1 file, the MCE and the HostedCluster
                          read; the path resolved; every release pinned to a
                          digest; checks 1–4 (gates.py). Any failure FAILS the
                          run, listing every failed check; nothing committed.
  2. preflight          — checks 5–11. A failure waits for a person
                          (needs-human), never fails: a PDB's owner can fix it
                          and `retry`.
  3. awaiting-approval  — the one approval, before anything is committed
                          (AGENT.md #3). `abort` here CANCELS: nothing changed.
  4. the steps, in plan order:
       control plane     — pre-flight 5–10; before a MINOR step, the admin
                           acks the hosted cluster asks for (D16: given by the
                           run when no client used the APIs they remove in the
                           last 24 hours, else awaiting-admin-ack); a free slot
                           on the MCE; then the child workflow
                           (upgrade_control_plane.py) commits, syncs and
                           watches. Then awaiting-day2-confirm: the day2 team
                           syncs their layer for the new version and confirms
                           (D14). After EVERY control-plane step.
       workers           — for each NodePool: awaiting-continue (manual mode;
                           auto mode goes on after the soak), pre-flight 5–10,
                           the digest re-checked, the commit and the sync, the
                           roll watched, then the gate (nodes, workloads, the
                           cluster's readiness checks, the soak).
  5. postflight         — the top-level workertag the run added is dropped;
                          the cluster checked at the target.

A failed gate goes to the agent once (dispatch.diagnose); it may clear the gate
for ONE more try. Anything else is needs-human: `retry` re-runs the same gate,
and a tier-2 action the agent proposed runs only after a person approves it.

PEOPLE DRIVE IT THROUGH UPDATES, never signals: approve, approve_admin_ack,
continue_nodepool, confirm_day2, pause, resume, abort, retry, approve_action,
reject_action. Each has a validator that only reads state and refuses a
decision the run is not waiting for, so the API can answer 409 at once instead
of a signal vanishing into a run that ignores it. Handlers only record the decision; this `run`
loop does all the work, in one ordered place.

WAITING IS FREE. Every human wait is a `wait_condition` on a flag a handler
sets — no timer, no polling — so a week between two NodePools adds nothing to
history. The run continues as new after every control-plane step and every
NodePool, which keeps each run short however long the upgrade, and lets new
code take over between units.

ABORT AND PAUSE ARE CHECKED RIGHT BEFORE EVERY ACT: every commit, every sync,
every admin ack, every child start and every agent call (`_checkpoint`), besides
every wait and every watch. An abort stops the control-plane child by signal and waits for it,
then commits `pausedUntil` for the HostedCluster and its NodePools and syncs
(ABORTED); if Argo CD refuses that sync, a person is asked, never the run
failed. Before approval an abort CANCELS, with nothing committed. There is no
downgrade, and no commit is ever reverted.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from collections.abc import Callable
from typing import Any, NoReturn

from temporalio import workflow
from temporalio.exceptions import (
    ActivityError,
    ApplicationError,
    ChildWorkflowError,
    FailureError,
    WorkflowAlreadyStartedError,
)
from temporalio.workflow import ParentClosePolicy

with workflow.unsafe.imports_passed_through():
    from shared.consts import UPGRADE_CONTROL_PLANE_WORKFLOW_QUEUE
    from shared.exceptions import (
        AdminGateNotFoundError,
        ArgoSyncRefusedError,
        Day1ChangeRejectedError,
        Day1FileChangedError,
        ProposedActionRefusedError,
        UpgradePlanningFailedError,
        UpgradeUpdateRefusedError,
    )
    from shared.interfaces.cluster_upgrade import (
        execute_proposed_action,
        give_admin_ack,
        read_admin_acks,
        read_argo_application,
        read_hosted_cluster,
        read_hosted_health,
        read_mce_state,
        sync_argo_application,
        watch_nodepool,
        watch_nodepool_gate,
    )
    from shared.interfaces.upgrade_gitops import (
        check_day2_layers,
        commit_day1_change,
        list_release_tags,
        pin_release_digests,
        read_day1_cluster,
        read_upgrade_rules,
    )
    from shared.models.cluster_upgrade import (
        ActionDecision,
        AdminAckGiven,
        AdminAckRequest,
        AdminAckState,
        AdminGateReview,
        AgentVerdict,
        ArgoSyncRequest,
        CheckFailure,
        CheckResult,
        ClusterProfile,
        ContinueNodePool,
        ControlPlaneStepOutcome,
        Day1ChangeKind,
        Day1ChangeRequest,
        Day1Cluster,
        Day1ClusterRequest,
        Day1Commit,
        Day2LayersRequest,
        GateOutcome,
        GateResult,
        GiveAdminAckRequest,
        HostedClusterRequest,
        HostedClusterState,
        HostedHealthRequest,
        HumanDecision,
        MceRequest,
        MceState,
        NodePoolGateRequest,
        NodePoolMode,
        PendingAction,
        PinReleasesRequest,
        ProposedActionRequest,
        ReleaseTagsRequest,
        StepKind,
        UpgradeClusterInput,
        UpgradeClusterProgress,
        UpgradeClusterResult,
        UpgradeClusterResumeState,
        UpgradeClusterRunArgs,
        UpgradeContext,
        UpgradeControlPlaneInput,
        UpgradeControlPlaneResult,
        UpgradeControlPlaneRunArgs,
        UpgradeLogEntry,
        UpgradeOutcome,
        UpgradePhase,
        UpgradeRules,
        UpgradeRulesRequest,
        UpgradeStep,
        WatchNodePoolRequest,
        WatchOutcome,
        WatchResult,
    )
    from shared.ocp_version import minor_name, minor_of, parse_minor, release_tag
    from shared.workflow_ids import upgrade_control_plane_workflow_id

from workflow_domains.cluster_upgrade import gates
from workflow_domains.cluster_upgrade.dispatch import (
    REGISTRY_TIMEOUT,
    SYNC_TIMEOUT,
    Diagnosis,
    clears_gate,
    diagnose,
    error_message,
    error_type,
    on_hub,
    on_mce,
    start_watch,
)
from workflow_domains.cluster_upgrade.upgrade_control_plane import (
    NEEDS_HUMAN_SIGNAL,
    UpgradeControlPlaneWorkflow,
)
from workflow_domains.cluster_upgrade.upgrade_path import recommended_target, resolve_upgrade_path

# MACHINE convergence gets deadlines (CLAUDE.md §5); human waits never do.
# Changing any of these is a non-deterministic change for in-flight runs.
#
# A NodePool roll with no node finishing in this long is a stall worth a
# diagnosis; its deadline is per batch of nodes (gates.nodepool_roll_deadline_minutes).
NODEPOOL_STALL_MINUTES = 30
# A drain the eviction API refuses (a PDB at disruptionsAllowed 0) never clears
# by itself — nodeDrainTimeout 0 waits for ever — so the watch hands it to the
# agent after this long instead of the full stall window (D17; CONTEXT §8.13).
DRAIN_BLOCKED_MINUTES = 10
# The gate's own deadline, on top of the soak it has to hold for.
GATE_DEADLINE_MINUTES = 60
# A control plane still settling (pre-flight check 7) is re-checked this
# often, for this long, before a person is asked.
_SETTLE_POLL = timedelta(minutes=1)
_SETTLE_DEADLINE = timedelta(minutes=30)
# Every control-plane slot of the MCE busy: look again this often. The other
# runs' steps are bounded by their own deadlines, so this wait is too.
_SLOT_POLL = timedelta(minutes=2)
# An ack given, the CVO not yet past AdminAckRequired: it re-checks as soon as
# admin-acks changes (its ConfigMap informer, pkg/cvo/upgradeable.go), so a
# few minutes is generous before a person is asked.
_ACK_SETTLE_POLL = timedelta(minutes=1)
_ACK_SETTLE_DEADLINE = timedelta(minutes=10)

# The waves whose agent runs tier-1 actions itself, judge-reviewed, within its
# budget: lab and canary (AGENT.md #25). Everywhere else every action is a
# proposal a person approves, until the fault-injection evals are green and the
# same fix has been approved repeatedly — graduating a wave is a reviewed change
# here. Settled at planning, from the cluster file's `upgrade.wave`.
_TIER1_WAVES = (0, 1)

_LOG_LIMIT = 500
_PROGRESS_LOG_TAIL = 20
_TERMINAL = (UpgradePhase.DONE, UpgradePhase.CANCELLED, UpgradePhase.ABORTED)
_OUTCOME_PHASE = {
    UpgradeOutcome.DONE: UpgradePhase.DONE,
    UpgradeOutcome.CANCELLED: UpgradePhase.CANCELLED,
    UpgradeOutcome.ABORTED: UpgradePhase.ABORTED,
}


class _AbortRequested(Exception):
    """A person's abort reached a wait or a watch. Raised and caught inside run() only."""


@workflow.defn
class UpgradeClusterWorkflow:
    # @workflow.init, so an Update that reaches a CONTINUED run before its
    # first line of run() is validated against the restored state, not
    # against a blank one that would refuse it.
    @workflow.init
    def __init__(self, run_args: UpgradeClusterRunArgs) -> None:
        self._input: UpgradeClusterInput = run_args.input
        # Found in day1 during planning; restored from the context on a continued run.
        self._mce = ""
        self._cluster = run_args.input.cluster
        self._context: UpgradeContext | None = None
        self._phase = UpgradePhase.PLANNING
        self._waiting_for: str | None = None
        self._approved_by: str | None = None
        self._next_step = 0
        self._nodepools_done: list[str] = []
        self._step_committed = False
        self._current_nodepool: str | None = None
        self._cp_version: str | None = None
        self._np_versions: dict[str, str] = {}
        self._day1_blob: str | None = None
        self._day1_stale = False
        # The revision committed and not yet synced. Never carried through
        # continue-as-new: that happens only once a unit has synced.
        self._pending_sync: str | None = None
        self._cp_workflow_id: str | None = None
        # The control-plane step child while it runs, and what it last reported.
        self._child: workflow.ChildWorkflowHandle | None = None
        self._child_report: UpgradeControlPlaneResult | None = None
        self._aborting = False
        self._last_verdict: AgentVerdict | None = None
        self._pending_actions: list[PendingAction] = []
        self._log_entries: list[UpgradeLogEntry] = []
        # Decisions recorded by the Update handlers, consumed by run().
        self._approval: HumanDecision | None = None
        self._admin_ack_approval: HumanDecision | None = None
        self._continue: ContinueNodePool | None = None
        self._continue_choices: list[str] = []
        self._day2_confirmed: HumanDecision | None = None
        self._paused: HumanDecision | None = None
        self._abort: HumanDecision | None = None
        self._retry: HumanDecision | None = None

        resume = run_args.resume
        if resume is not None:
            self._context = resume.context
            self._mce = resume.context.mce_cluster or ""
            self._approved_by = resume.approved_by
            self._next_step = resume.next_step
            self._nodepools_done = list(resume.nodepools_done)
            self._step_committed = resume.step_committed
            self._cp_version = resume.control_plane_version
            self._np_versions = dict(resume.nodepool_versions)
            self._day1_blob = resume.day1_blob
            self._paused = resume.paused
            self._log_entries = list(resume.log)
            self._phase = UpgradePhase.PREFLIGHT

    # --- what a person sees ----------------------------------------------------

    @workflow.query
    def progress(self) -> UpgradeClusterProgress:
        context = self._context
        return UpgradeClusterProgress(
            phase=self._phase,
            waiting_for=self._waiting_for,
            mce_cluster=self._mce or None,
            plan=context.plan if context else None,
            profile=context.profile if context else None,
            current_step=self._next_step if context else None,
            current_nodepool=self._current_nodepool,
            nodepools_done=list(self._nodepools_done),
            control_plane_version=self._cp_version,
            nodepool_versions=dict(self._np_versions),
            paused=self._paused is not None,
            control_plane_workflow_id=self._cp_workflow_id,
            pending_actions=list(self._pending_actions),
            last_verdict=self._last_verdict,
            verified_against_graph=context.verified_against_graph if context else None,
            log=self._log_entries[-_PROGRESS_LOG_TAIL:],
        )

    @workflow.signal(name=NEEDS_HUMAN_SIGNAL)
    def control_plane_needs_human(self, report: UpgradeControlPlaneResult) -> None:
        """The control-plane child needs a person; it waits, holding its slot, for retry or stop.

        A signal, not an Update: it is the child talking to its parent, code to
        code, and needs no answer. People's decisions are the Updates below.
        """
        self._child_report = report

    # --- what a person decides: Updates, each refused unless the run waits for it ---

    @workflow.update
    def approve(self, decision: HumanDecision) -> None:
        self._approval = decision
        self._log("approved", decision.by, decision.reason or "")

    @approve.validator
    def _validate_approve(self, decision: HumanDecision) -> None:
        self._require_phase(UpgradePhase.AWAITING_APPROVAL, "approve")

    @workflow.update
    def approve_admin_ack(self, decision: HumanDecision) -> None:
        self._admin_ack_approval = decision
        self._log("admin-ack-approved", decision.by, decision.reason or "")

    @approve_admin_ack.validator
    def _validate_approve_admin_ack(self, decision: HumanDecision) -> None:
        self._require_phase(UpgradePhase.AWAITING_ADMIN_ACK, "approve-admin-ack")
        if not (decision.reason or "").strip():
            self._refuse(
                "approve-admin-ack needs a reason: clients still call the APIs this ack lets the "
                "upgrade remove, and the reason records why that is acceptable"
            )

    @workflow.update
    def continue_nodepool(self, decision: ContinueNodePool) -> None:
        self._continue = decision
        self._log("continue", decision.by, decision.nodepool or "")

    @continue_nodepool.validator
    def _validate_continue(self, decision: ContinueNodePool) -> None:
        self._require_phase(UpgradePhase.AWAITING_CONTINUE, "continue")
        if decision.nodepool is not None and decision.nodepool not in self._continue_choices:
            self._refuse(
                f"NodePool {decision.nodepool} is not waiting to roll; "
                f"the next can be one of {', '.join(self._continue_choices)}"
            )

    @workflow.update
    def confirm_day2(self, decision: HumanDecision) -> None:
        self._day2_confirmed = decision
        self._log("day2-confirmed", decision.by, decision.reason or "")

    @confirm_day2.validator
    def _validate_confirm_day2(self, decision: HumanDecision) -> None:
        self._require_phase(UpgradePhase.AWAITING_DAY2_CONFIRM, "confirm-day2")

    @workflow.update
    def pause(self, decision: HumanDecision) -> None:
        self._paused = decision
        self._log("paused", decision.by, decision.reason or "")

    @pause.validator
    def _validate_pause(self, decision: HumanDecision) -> None:
        self._require_open("pause")
        if self._approved_by is None:
            self._refuse("nothing is committed before approval, so there is nothing to pause: abort cancels")
        if self._paused is not None:
            self._refuse(f"already paused by {self._paused.by}")

    @workflow.update
    def resume(self, decision: HumanDecision) -> None:
        self._paused = None
        self._log("resumed", decision.by, decision.reason or "")

    @resume.validator
    def _validate_resume(self, decision: HumanDecision) -> None:
        self._require_open("resume")
        if self._paused is None:
            self._refuse("the run is not paused")

    @workflow.update
    def abort(self, decision: HumanDecision) -> None:
        self._abort = decision
        self._log("abort-requested", decision.by, decision.reason or "")

    @abort.validator
    def _validate_abort(self, decision: HumanDecision) -> None:
        self._require_open("abort")
        if self._abort is not None:
            self._refuse(f"already aborting, at {self._abort.by}'s request")

    @workflow.update
    def retry(self, decision: HumanDecision) -> None:
        self._retry = decision
        self._log("retry", decision.by, decision.reason or "")

    @retry.validator
    def _validate_retry(self, decision: HumanDecision) -> None:
        # Waiting for an admin ack, retry checks the API callers again.
        if self._phase is not UpgradePhase.AWAITING_ADMIN_ACK:
            self._require_phase(UpgradePhase.NEEDS_HUMAN, "retry")

    @workflow.update
    def approve_action(self, decision: ActionDecision) -> None:
        self._decide_action(decision, "approved")

    @approve_action.validator
    def _validate_approve_action(self, decision: ActionDecision) -> None:
        self._require_pending_action(decision)

    @workflow.update
    def reject_action(self, decision: ActionDecision) -> None:
        self._decide_action(decision, "rejected")

    @reject_action.validator
    def _validate_reject_action(self, decision: ActionDecision) -> None:
        self._require_pending_action(decision)

    def _refuse(self, message: str) -> NoReturn:
        raise ApplicationError(message, type=UpgradeUpdateRefusedError.__name__)

    def _require_open(self, update: str) -> None:
        if self._phase in _TERMINAL or self._phase is UpgradePhase.ABORTING or self._aborting:
            self._refuse(f"{update}: the run is aborting" if self._aborting else f"{update}: the run is {self._phase.value}")

    def _require_phase(self, phase: UpgradePhase, update: str) -> None:
        if self._phase is not phase:
            waiting = f", waiting for {self._waiting_for}" if self._waiting_for else ""
            self._refuse(f"{update} is not waited for: the run is {self._phase.value}{waiting}")

    def _require_pending_action(self, decision: ActionDecision) -> None:
        self._require_phase(UpgradePhase.NEEDS_HUMAN, "an action decision")
        pending = self._pending_action(decision.action_id)
        if pending is None:
            self._refuse(f"no action {decision.action_id} was proposed")
        if pending.decision != "pending":
            self._refuse(f"action {decision.action_id} was already {pending.decision} by {pending.decided_by}")

    def _decide_action(self, decision: ActionDecision, outcome: str) -> None:
        pending = self._pending_action(decision.action_id)
        if pending is not None:
            pending.decision = outcome  # type: ignore[assignment]
            pending.decided_by = decision.by
        self._log(f"action-{outcome}", decision.by, decision.action_id)

    def _pending_action(self, action_id: str) -> PendingAction | None:
        return next((p for p in self._pending_actions if p.action.action_id == action_id), None)

    # --- the run -----------------------------------------------------------------

    @workflow.run
    async def run(self, run_args: UpgradeClusterRunArgs) -> UpgradeClusterResult:
        try:
            if run_args.resume is None:
                nothing_to_do = await self._plan_and_approve()
                if nothing_to_do:
                    return await self._finish(UpgradeOutcome.DONE)
            await self._run_steps()
            await self._postflight()
            return await self._finish(UpgradeOutcome.DONE)
        except _AbortRequested:
            if self._approved_by is None:
                return await self._finish(UpgradeOutcome.CANCELLED)
            self._aborting = True
            self._phase = UpgradePhase.ABORTING
            self._waiting_for = None
            await self._stop_child()
            await self._abort_upgrade()
            return await self._finish(UpgradeOutcome.ABORTED)

    async def _plan_and_approve(self) -> bool:
        """Planning, pre-flight, approval. True when the cluster is already at the target."""
        request = self._input
        self._phase = UpgradePhase.PLANNING
        day1: Day1Cluster = await on_hub(read_day1_cluster, Day1ClusterRequest(cluster=self._cluster))
        mce_cluster = gates.mce_of_day1_path(day1.path)
        if mce_cluster is None:
            self._fail_planning(
                f"day1 file {day1.path} is not under sites/<site>/mces/<mce>/hostedClusters/: no MCE to run on"
            )
        self._mce = mce_cluster
        rules: UpgradeRules = await on_hub(read_upgrade_rules, UpgradeRulesRequest(mce_cluster=self._mce))
        mce: MceState = await on_mce(self._mce, read_mce_state, MceRequest(mce_cluster=self._mce))
        hosted: HostedClusterState = await on_mce(
            self._mce, read_hosted_cluster, HostedClusterRequest(mce_cluster=self._mce, cluster=self._cluster)
        )
        if not hosted.found:
            self._fail_planning(f"HostedCluster {self._cluster} not found on {self._mce}")
        if hosted.control_plane_version is None:
            self._fail_planning(f"HostedCluster {self._cluster} is mid-upgrade: no Completed version to plan from")
        unreported = [nodepool.name for nodepool in hosted.nodepools if nodepool.version is None]
        if unreported:
            self._fail_planning(f"NodePools report no version: {', '.join(unreported)}")

        profile = gates.cluster_profile(day1)
        try:
            order = gates.nodepool_order(
                requested=request.nodepool_order,
                day1=day1,
                names=[nodepool.name for nodepool in hosted.nodepools],
            )
        except ValueError as err:
            self._fail_planning(str(err))
        versions_now = {nodepool.name: nodepool.version for nodepool in hosted.nodepools}
        plan = resolve_upgrade_path(
            control_plane=hosted.control_plane_version,
            nodepools={name: versions_now[name] for name in order},
            target=await self._target(rules),
            ladder=rules.ladder,
            max_worker_skew=rules.max_worker_skew,
            workers_to_target=request.workers_to_target,
        )
        self._cp_version = plan.control_plane
        self._np_versions = dict(plan.nodepools)
        context = UpgradeContext(
            plan=plan,
            profile=profile,
            nodepool_order=order,
            soak_minutes=gates.soak_minutes(profile, day1),
            readiness_checks=day1.upgrade.readiness_checks,
            max_concurrent_control_planes=rules.max_concurrent_control_planes,
            release=rules.release,
            verified_against_graph=rules.verified_against_graph,
            day1_path=day1.path,
            mce_cluster=self._mce,
            tier1_enabled=day1.upgrade.wave in _TIER1_WAVES,
            admin_acks=rules.admin_acks,
            api_usage_ignored_users=rules.api_usage_ignored_users,
        )
        if not plan.steps:
            self._context = context
            self._log("nothing-to-do", detail=f"already at {plan.target}")
            return True

        versions = gates.step_versions(plan)
        pinned = await on_hub(
            pin_release_digests,
            PinReleasesRequest(versions=versions, release=rules.release),
            timeout=REGISTRY_TIMEOUT,
        )
        day2 = await on_hub(check_day2_layers, Day2LayersRequest(cluster=self._cluster, versions=versions))
        checks = gates.planning_checks(
            request=request, rules=rules, mce=mce, day1=day1, hosted=hosted,
            plan=plan, pinned=pinned, day2=day2,
        )
        if gates.failed(checks):
            self._fail_planning(gates.describe(checks))
        plan.pinned = {version: pinned.pinned[version] for version in versions}
        self._context = context
        self._day1_blob = day1.blob
        self._log("planned", detail=self._plan_summary())

        await self._preflight(include_idms=True, nodepools=[])

        if request.auto_approve:
            # An abort that came in during the pre-flight must still CANCEL.
            if self._abort is not None:
                raise _AbortRequested()
            self._approved_by = f"auto-approve, requested by {request.requested_by}"
            self._log("approved", self._approved_by, f"upgrade.wave {day1.upgrade.wave}")
            return False
        self._phase = UpgradePhase.AWAITING_APPROVAL
        verified = context.verified_against_graph or "never"
        acks = gates.admin_acks_on_path(context.plan, context.admin_acks)
        self._waiting_for = (
            f"approve: {self._plan_summary()}. Ladder last checked against the upgrade graph: {verified}"
            + (
                ". Admin acks on the way, given by the run only if no client called the APIs they "
                f"remove in the 24 hours before (else it asks): {'; '.join(acks)}"
                if acks else ""
            )
        )
        await self._wait_for(lambda: self._approval is not None)
        self._approved_by = self._approval.by if self._approval else None
        self._phase = UpgradePhase.PREFLIGHT
        self._waiting_for = None
        return False

    async def _target(self, rules: UpgradeRules) -> str:
        request = self._input
        if request.target_version is not None:
            return request.target_version
        try:
            minor = parse_minor(request.target_minor or "")
        except ValueError as err:
            self._fail_planning(str(err))
        tags = await on_hub(list_release_tags, ReleaseTagsRequest(release=rules.release), timeout=REGISTRY_TIMEOUT)
        target = recommended_target(minor, rules.ladder, tags.tags)
        if target is None:
            name = minor_name(minor)
            self._fail_planning(
                f"no release of {name} is both in the mirror and a ladder rung: mirror a rung of {name} first"
            )
        return target

    def _fail_planning(self, message: str) -> NoReturn:
        raise ApplicationError(message, type=UpgradePlanningFailedError.__name__)

    # --- the steps -----------------------------------------------------------------

    async def _run_steps(self) -> None:
        steps = self._ctx.plan.steps
        while self._next_step < len(steps):
            step = steps[self._next_step]
            if step.kind is StepKind.CONTROL_PLANE:
                await self._control_plane_step(step)
                self._complete_step()
            else:
                remaining = self._remaining(step)
                if remaining:
                    nodepool = await self._choose_nodepool(step, remaining)
                    await self._roll_nodepool(step, nodepool)
                    self._nodepools_done.append(nodepool)
                    self._np_versions[nodepool] = step.version
                    self._step_committed = False
                    self._current_nodepool = None
                if not self._remaining(step):
                    self._complete_step()
            if self._next_step < len(steps):
                await self._continue_as_new()

    def _complete_step(self) -> None:
        self._next_step += 1
        self._nodepools_done = []
        self._step_committed = False

    def _remaining(self, step: UpgradeStep) -> list[str]:
        return [name for name in step.nodepools if name not in self._nodepools_done]

    async def _control_plane_step(self, step: UpgradeStep) -> None:
        from_version = self._cp_version or ""
        while True:
            await self._checkpoint()
            if not self._step_committed:
                await self._preflight(include_idms=False, nodepools=[])
                if minor_of(step.version) > minor_of(from_version):
                    await self._admin_acks(step, from_version)
            await self._take_current_day1_if_stale()
            child = await self._take_slot(step, from_version)
            try:
                result = await self._drive_child(child, step)
            except ChildWorkflowError as err:
                # Terminated by an operator, or failed on an error nothing
                # classified: the step ends without a result, the run does not.
                self._child = None
                self._cp_workflow_id = None
                self._log("control-plane-ended", detail=f"{child.id}: {error_message(err)}")
                await self._needs_human(
                    f"control-plane step to {step.version}: its workflow {child.id} ended without a "
                    f"result ({error_message(err)}). Retry starts the step again"
                )
                continue
            self._child = None
            self._cp_workflow_id = None
            self._absorb(result)
            self._log(f"control-plane-{result.outcome.value}", detail=result.detail or step.version)
            if result.outcome is ControlPlaneStepOutcome.DONE:
                break
            detail = result.detail
            if result.outcome is ControlPlaneStepOutcome.DAY1_CHANGED:
                self._day1_stale = True
                detail = self._day1_changed(detail)
            await self._needs_human(f"control-plane step to {step.version}: {detail}", verdict=result.verdict)
        self._cp_version = step.version

        # The day2 team's part (D14): their layer for the new version is now
        # OutOfSync on their side, and only they sync it.
        self._day2_confirmed = None
        self._phase = UpgradePhase.AWAITING_DAY2_CONFIRM
        self._waiting_for = (
            f"confirm-day2: the control plane runs {step.version}. The day2 Applications labelled "
            f"day2.gitops/cluster={self._cluster} now render for ocp-version {step.version}; "
            "sync them, then confirm"
        )
        await self._wait_for(lambda: self._day2_confirmed is not None)
        self._day2_confirmed = None
        self._phase = UpgradePhase.CONTROL_PLANE_STEP
        self._waiting_for = None

    async def _admin_acks(self, step: UpgradeStep, from_version: str) -> None:
        """Right before a control-plane minor step: the admin acks the hosted cluster asks for (D16).

        Some releases make an administrator acknowledge an API removal before
        the control plane may leave their minor (4.19 before 4.20): until then
        its CVO says Upgradeable=False, AdminAckRequired, and HyperShift refuses
        a minor change of the HostedCluster (isUpgrading). Each pending gate is:
          * listed in the rules file, its APIs unused in the last 24 hours —
            acked by the run, with the workflow identity;
          * listed, its APIs still called — awaiting-admin-ack: a person
            approves (answering for the callers shown) or retries (check
            again, once the callers moved off the API);
          * not listed — needs-human: a person acks it by hand, then retries.
        Done once nothing is pending AND the CVO no longer says
        AdminAckRequired: a commit before that would be refused. An ack given
        is never taken back; it allows a minor step, it does not start one.
        """
        context = self._ctx
        request = AdminAckRequest(
            mce_cluster=self._mce, cluster=self._cluster, apis=gates.admin_ack_apis(context.admin_acks)
        )
        approved_by: dict[str, str] = {}
        settling = timedelta(0)
        while True:
            await self._checkpoint()
            self._phase = UpgradePhase.ADMIN_ACKS
            self._waiting_for = None
            state: AdminAckState = await on_mce(self._mce, read_admin_acks, request)
            if not state.reachable:
                await self._needs_human(f"admin acks before {step.version}: the hosted cluster's API is unreachable")
                continue
            pending = gates.pending_admin_gates(state, from_version)
            if not pending:
                if state.admin_ack_required is None:
                    return
                if settling < _ACK_SETTLE_DEADLINE:
                    self._waiting_for = f"the CVO to take the admin acks: it still says {state.admin_ack_required}"
                    await self._sleep_or_abort(_ACK_SETTLE_POLL)
                    settling += _ACK_SETTLE_POLL
                    continue
                await self._needs_human(
                    f"admin acks before {step.version}: every gate is acked, yet the CVO still says "
                    f"AdminAckRequired: {state.admin_ack_required}"
                )
                settling = timedelta(0)
                continue
            reviews = gates.review_admin_gates(
                pending, context.admin_acks, state.usage, context.api_usage_ignored_users
            )
            unlisted = [review for review in reviews if not review.listed]
            if unlisted:
                await self._needs_human(
                    f"before {step.version} the hosted cluster asks for admin acks the rules file does not list:\n"
                    + "\n".join(f"  {review.gate}: {review.message}" for review in unlisted)
                    + "\nCheck the removal it names, then set the ack by hand in the hosted cluster's "
                    "openshift-config/admin-acks, or list it in upgrade/ladder.yaml adminAcks, and retry"
                )
                continue
            in_use = [review for review in reviews if review.in_use and review.gate not in approved_by]
            if in_use:
                decision = await self._await_admin_ack_approval(step, in_use)
                if decision is None:
                    continue
                approved_by.update({review.gate: decision.by for review in in_use})
            for review in reviews:
                await self._checkpoint()
                try:
                    given: AdminAckGiven = await on_mce(
                        self._mce, give_admin_ack,
                        GiveAdminAckRequest(mce_cluster=self._mce, cluster=self._cluster, gate=review.gate),
                    )
                except ActivityError as err:
                    if error_type(err) != AdminGateNotFoundError.__name__:
                        raise
                    # The gates changed since the read: read them again.
                    self._log("admin-ack-gone", detail=error_message(err))
                    break
                self._log(
                    "admin-ack",
                    approved_by.get(review.gate, "workflow"),
                    review.gate + (
                        f": approved with {len(review.in_use)} callers in the last 24 hours" if review.in_use
                        else ": no client called its APIs in the last 24 hours"
                    ) + ("" if given.changed else " (already acked)"),
                )

    async def _await_admin_ack_approval(
        self, step: UpgradeStep, in_use: list[AdminGateReview]
    ) -> HumanDecision | None:
        """awaiting-admin-ack, with the callers listed. The approval, or None for a retry."""
        self._admin_ack_approval = None
        self._retry = None
        self._phase = UpgradePhase.AWAITING_ADMIN_ACK
        callers = "\n".join(f"{review.gate}:\n{gates.describe_api_usage(review.in_use)}" for review in in_use)
        self._waiting_for = (
            f"approve-admin-ack or retry: before {step.version} the hosted cluster needs admin acks, "
            f"and clients called the APIs they remove in the last 24 hours:\n{callers}\n"
            "Move them to the new API version. Then approve-admin-ack with a reason (you answer for "
            "them), or retry to check again (the count covers 24 hours)"
        )
        self._log("admin-ack-in-use", detail=callers)
        await self._wait_for(lambda: self._admin_ack_approval is not None or self._retry is not None)
        decision, self._admin_ack_approval, self._retry = self._admin_ack_approval, None, None
        self._phase = UpgradePhase.ADMIN_ACKS
        self._waiting_for = None
        return decision

    async def _drive_child(self, child: workflow.ChildWorkflowHandle, step: UpgradeStep) -> UpgradeControlPlaneResult:
        """Await the step's child: pass it a pause, and a person's retry whenever it reports needing one.

        The child never learns of an abort from here — this raises
        _AbortRequested, and run() stops it by signal before anything else.
        A child that FAILED raises ChildWorkflowError, for the caller.
        """
        held = False
        while True:
            await workflow.wait_condition(
                lambda: child.done()
                or self._abort is not None
                or self._child_report is not None
                or (self._paused is not None) != held
            )
            if child.done():
                return child.result()
            if self._abort is not None:
                raise _AbortRequested()
            if (self._paused is not None) != held:
                held = self._paused is not None
                await self._signal_child(child, UpgradeControlPlaneWorkflow.hold, held)
                continue
            report, self._child_report = self._child_report, None
            if report is None or report.step_index != self._next_step:
                continue
            self._absorb(report)
            self._log("control-plane-needs-human", detail=report.detail)
            # The child waits meanwhile, its slot still taken: the HostedCluster is mid-upgrade.
            await self._needs_human(f"control-plane step to {step.version}: {report.detail}", verdict=report.verdict)
            await self._signal_child(child, UpgradeControlPlaneWorkflow.retry)
            self._phase = UpgradePhase.CONTROL_PLANE_STEP

    async def _signal_child(self, child: workflow.ChildWorkflowHandle, signal: Any, *args: Any) -> None:
        try:
            await child.signal(signal, *args)
        except FailureError as err:
            # The child closed in the meantime; its result says how it ended.
            workflow.logger.info("child %s closed before %s reached it: %s", child.id, signal, err)

    async def _stop_child(self) -> None:
        """An abort: tell a running control-plane child to stop, and wait for it to.

        It stops at its next safe point — after the call in flight, before any
        commit or sync it has not made — so the abort's own commit never races it.
        """
        child = self._child
        if child is None or child.done():
            return
        await self._signal_child(child, UpgradeControlPlaneWorkflow.stop)
        try:
            self._absorb(await child)
        except ChildWorkflowError as err:
            self._log("control-plane-ended", detail=f"{child.id}: {error_message(err)}")
        self._child = None
        self._cp_workflow_id = None

    def _absorb(self, result: UpgradeControlPlaneResult) -> None:
        """What a step result tells the parent: a commit landed, the file's new blob, a verdict."""
        if result.committed:
            self._step_committed = True
        if result.day1_blob:
            self._day1_blob = result.day1_blob
        if result.verdict is not None:
            self._last_verdict = result.verdict

    async def _take_slot(self, step: UpgradeStep, from_version: str) -> workflow.ChildWorkflowHandle:
        """Start the step's child on the first free slot of the MCE; wait while all are taken."""
        context = self._ctx
        child_input = UpgradeControlPlaneInput(
            parent_workflow_id=workflow.info().workflow_id,
            mce_cluster=self._mce,
            cluster=self._cluster,
            step_index=self._next_step,
            step=step,
            from_version=from_version,
            pinned=context.plan.pinned[step.version],
            release=context.release,
            nodepool_versions=dict(self._np_versions),
            day1_path=context.day1_path,
            day1_blob=self._day1_blob or "",
            profile=context.profile,
            tier1_enabled=context.tier1_enabled,
        )
        while True:
            # Starting the child starts the commit: a pause or an abort that
            # came in while every slot was taken holds it here.
            await self._checkpoint()
            self._phase = UpgradePhase.WAITING_FOR_SLOT
            for slot in range(1, context.max_concurrent_control_planes + 1):
                child_id = upgrade_control_plane_workflow_id(self._mce, slot)
                try:
                    handle = await workflow.start_child_workflow(
                        UpgradeControlPlaneWorkflow.run,
                        UpgradeControlPlaneRunArgs(input=child_input),
                        id=child_id,
                        task_queue=UPGRADE_CONTROL_PLANE_WORKFLOW_QUEUE,
                        parent_close_policy=ParentClosePolicy.ABANDON,
                    )
                except WorkflowAlreadyStartedError:
                    continue
                self._phase = UpgradePhase.CONTROL_PLANE_STEP
                self._waiting_for = None
                self._child = handle
                self._child_report = None
                self._cp_workflow_id = child_id
                self._log("control-plane-started", detail=f"{child_id} → {step.version}")
                return handle
            self._waiting_for = (
                f"a control-plane slot on {self._mce}: all {context.max_concurrent_control_planes} "
                "are upgrading other clusters"
            )
            if workflow.info().is_continue_as_new_suggested():
                await self._continue_as_new()
            await self._sleep_or_abort(_SLOT_POLL)

    async def _choose_nodepool(self, step: UpgradeStep, remaining: list[str]) -> str:
        if self._input.nodepool_mode is NodePoolMode.AUTO:
            return remaining[0]
        self._continue = None
        self._phase = UpgradePhase.AWAITING_CONTINUE
        self._continue_choices = list(remaining)
        self._waiting_for = (
            f"continue: roll the next NodePool to {step.version}, one of {', '.join(remaining)} "
            f"({remaining[0]} unless named)"
        )
        await self._wait_for(lambda: self._continue is not None)
        choice = (self._continue.nodepool if self._continue else None) or remaining[0]
        self._continue = None
        self._continue_choices = []
        self._phase = UpgradePhase.NODEPOOL_ROLL
        self._waiting_for = None
        return choice

    async def _roll_nodepool(self, step: UpgradeStep, nodepool: str) -> None:
        context = self._ctx
        self._current_nodepool = nodepool
        from_version = self._np_versions.get(nodepool)
        while True:
            await self._checkpoint()
            if not self._step_committed:
                await self._preflight(include_idms=False, nodepools=[nodepool])
                reason = await self._commit(Day1ChangeKind.NODEPOOL, step.version, nodepool)
                if reason is not None:
                    await self._needs_human(reason)
                    continue
                self._step_committed = True
            reason = await self._sync_pending(allowed=[f"NodePool/{nodepool}"])
            if reason is not None:
                await self._needs_human(reason)
                continue

            self._phase = UpgradePhase.NODEPOOL_ROLL
            self._waiting_for = None
            outcome = await self._watch_nodepool(step, nodepool)
            if outcome.result is not WatchResult.DONE:
                check = CheckResult(
                    name="nodepool-converged",
                    passed=False,
                    expected=f"NodePool {nodepool} at {step.version}, every node healthy",
                    observed=f"{outcome.result.value}: {outcome.snapshot.summary}",
                )
                diagnosis = await self._diagnose(
                    step, from_version, nodepool, f"watch-nodepool-{outcome.result.value}", [check]
                )
                if outcome.result is WatchResult.STALLED and clears_gate(diagnosis):
                    self._phase = UpgradePhase.NODEPOOL_ROLL
                    outcome = await self._watch_nodepool(step, nodepool)
                if outcome.result is not WatchResult.DONE:
                    await self._needs_human(
                        f"NodePool {nodepool} did not reach {step.version}: {outcome.snapshot.summary}",
                        verdict=diagnosis.verdict,
                    )
                    continue

            self._phase = UpgradePhase.NODEPOOL_GATE
            gate = await self._watch_gate(step, nodepool)
            if gate.result is not GateResult.PASSED:
                diagnosis = await self._diagnose(step, from_version, nodepool, "nodepool-gate", gate.failing_checks)
                if clears_gate(diagnosis):
                    self._phase = UpgradePhase.NODEPOOL_GATE
                    gate = await self._watch_gate(step, nodepool)
                if gate.result is not GateResult.PASSED:
                    await self._needs_human(
                        f"the gate after NodePool {nodepool} did not pass:\n{gates.describe(gate.failing_checks)}",
                        verdict=diagnosis.verdict,
                    )
                    continue
            self._log("nodepool-rolled", detail=f"{nodepool} → {step.version}")
            return

    async def _watch_nodepool(self, step: UpgradeStep, nodepool: str) -> WatchOutcome:
        hosted: HostedClusterState = await on_mce(
            self._mce, read_hosted_cluster, HostedClusterRequest(mce_cluster=self._mce, cluster=self._cluster)
        )
        state = next((np for np in hosted.nodepools if np.name == nodepool), None)
        deadline = gates.nodepool_roll_deadline_minutes(
            state.replicas if state else None, state.max_unavailable if state else None
        )
        return await self._until_done(
            start_watch(
                self._mce,
                watch_nodepool,
                WatchNodePoolRequest(
                    mce_cluster=self._mce,
                    cluster=self._cluster,
                    nodepool=nodepool,
                    version=step.version,
                    stall_minutes=NODEPOOL_STALL_MINUTES,
                    deadline_minutes=deadline,
                    drain_blocked_minutes=DRAIN_BLOCKED_MINUTES,
                ),
                deadline_minutes=deadline,
            )
        )

    async def _watch_gate(self, step: UpgradeStep, nodepool: str) -> GateOutcome:
        context = self._ctx
        deadline = context.soak_minutes + GATE_DEADLINE_MINUTES
        return await self._until_done(
            start_watch(
                self._mce,
                watch_nodepool_gate,
                NodePoolGateRequest(
                    mce_cluster=self._mce,
                    cluster=self._cluster,
                    nodepool=nodepool,
                    version=step.version,
                    workload_checks=(
                        context.profile is ClusterProfile.ZONED or bool(context.readiness_checks)
                    ),
                    readiness_checks=context.readiness_checks,
                    soak_minutes=context.soak_minutes,
                    deadline_minutes=deadline,
                ),
                deadline_minutes=deadline,
            )
        )

    async def _postflight(self) -> None:
        context = self._ctx
        committed = False
        while True:
            await self._checkpoint()
            self._phase = UpgradePhase.POSTFLIGHT
            self._waiting_for = None
            reason = None
            if not committed:
                reason = await self._commit(Day1ChangeKind.FINISH, None, None)
                committed = reason is None
            if reason is None:
                # Dropping the top-level workertag renders nothing new: every
                # NodePool carries its own tag by now, so nothing may be out of sync.
                reason = await self._sync_pending(allowed=[])
            if reason is None:
                hosted: HostedClusterState = await on_mce(
                    self._mce, read_hosted_cluster,
                    HostedClusterRequest(mce_cluster=self._mce, cluster=self._cluster),
                )
                reason = self._not_at_target(hosted, context)
            if reason is None:
                self._log("done", detail=f"at {context.plan.target}")
                return
            await self._needs_human(f"post-flight: {reason}")

    def _not_at_target(self, hosted: HostedClusterState, context: UpgradeContext) -> str | None:
        target = context.plan.target
        wrong = []
        if hosted.control_plane_version != target:
            wrong.append(f"the control plane runs {hosted.control_plane_version}")
        for nodepool in hosted.nodepools:
            expected = target if self._input.workers_to_target else self._np_versions.get(nodepool.name)
            if nodepool.version != expected:
                wrong.append(f"NodePool {nodepool.name} runs {nodepool.version}, not {expected}")
        return f"expected {target}: " + "; ".join(wrong) if wrong else None

    # --- commits, checks, people ------------------------------------------------------

    async def _commit(self, change: Day1ChangeKind, version: str | None, nodepool: str | None) -> str | None:
        """One commit to the cluster file. Returns why a person is needed, or None.

        The release's digests are re-resolved first (D2): a tag that moved
        since approval stops here, before anything is committed. The commit's
        revision is left for _sync_pending, which a retry repeats on its own
        when the commit landed and the sync was refused.
        """
        context = self._ctx
        await self._checkpoint()
        if version is not None:
            now = await on_hub(
                pin_release_digests,
                PinReleasesRequest(versions=[version], release=context.release),
                timeout=REGISTRY_TIMEOUT,
            )
            if now.pinned.get(version) != context.plan.pinned.get(version):
                return f"the mirror changed under the run: {release_tag(version)} no longer resolves to the approved digests"
        await self._take_current_day1_if_stale()
        await self._checkpoint()
        try:
            commit: Day1Commit = await on_hub(
                commit_day1_change,
                Day1ChangeRequest(
                    workflow_id=workflow.info().workflow_id,
                    mce_cluster=self._mce,
                    cluster=self._cluster,
                    path=context.day1_path,
                    expected_blob=self._day1_blob,
                    change=change,
                    version=version,
                    nodepool=nodepool,
                ),
            )
        except ActivityError as err:
            kind = error_type(err)
            if kind == Day1FileChangedError.__name__:
                self._day1_stale = True
                return self._day1_changed(error_message(err))
            if kind == Day1ChangeRejectedError.__name__:
                return f"the day1 validator refused the change: {error_message(err)}"
            raise
        self._day1_blob = commit.blob
        self._log(f"commit-{change.value}", detail=f"{commit.revision} {nodepool or ''} {version or ''}".strip())
        # A NodePool commit is synced even when the file already said so: the
        # earlier attempt may have committed and never synced. A finish that
        # changed nothing has nothing to apply.
        if commit.changed or change is not Day1ChangeKind.FINISH:
            self._pending_sync = commit.revision
        return None

    async def _sync_pending(self, *, allowed: list[str]) -> str | None:
        """Sync the Application to the run's last commit, if it is not yet. Why a person is needed, or None."""
        if self._pending_sync is None:
            return None
        await self._checkpoint()
        try:
            await on_mce(
                self._mce,
                sync_argo_application,
                ArgoSyncRequest(
                    mce_cluster=self._mce, cluster=self._cluster,
                    revision=self._pending_sync, allowed_out_of_sync=allowed,
                ),
                timeout=SYNC_TIMEOUT,
            )
        except ActivityError as err:
            if error_type(err) == ArgoSyncRefusedError.__name__:
                return f"Argo CD has changes pending that this run did not make: {error_message(err)}"
            raise
        self._pending_sync = None
        return None

    def _day1_changed(self, detail: str) -> str:
        return (
            f"someone else changed {self._ctx.day1_path} ({detail}). "
            "Retry carries on from the file as it is now; the run never commits over a person"
        )

    async def _take_current_day1_if_stale(self) -> None:
        """After a person's retry on a conflict, carry on from the file as they left it."""
        if not self._day1_stale:
            return
        day1: Day1Cluster = await on_hub(read_day1_cluster, Day1ClusterRequest(cluster=self._cluster))
        self._day1_blob = day1.blob
        self._day1_stale = False
        self._log("day1-reread", detail=f"{day1.path} at {day1.revision}")

    async def _preflight(self, *, include_idms: bool, nodepools: list[str]) -> None:
        """Checks 5–10 (and 11) until they pass, waiting for a person on a failure.

        A control plane still settling is re-checked for a while first. Before
        approval a FAILED-kind check fails the run, as planning does; after it,
        the run never fails on a check, it waits.
        """
        settling = timedelta(0)
        while True:
            self._phase = UpgradePhase.PREFLIGHT
            self._waiting_for = None
            checks = await self._preflight_checks(include_idms=include_idms, nodepools=nodepools)
            worst = gates.worst(checks)
            if worst is None:
                return
            if worst is CheckFailure.WAIT and settling < _SETTLE_DEADLINE:
                self._waiting_for = f"the control plane to settle:\n{gates.describe(checks)}"
                await self._sleep_or_abort(_SETTLE_POLL)
                settling += _SETTLE_POLL
                continue
            if worst is CheckFailure.FAILED and self._approved_by is None:
                self._fail_planning(gates.describe(checks))
            owner = "its owner must act first, then retry. " if worst is CheckFailure.NOTIFY_OWNER else ""
            await self._needs_human(f"pre-flight failed: {owner}\n{gates.describe(checks)}")
            settling = timedelta(0)

    async def _preflight_checks(self, *, include_idms: bool, nodepools: list[str]) -> list[CheckResult]:
        cluster = HostedClusterRequest(mce_cluster=self._mce, cluster=self._cluster)
        argo, hosted, health = await asyncio.gather(
            on_mce(self._mce, read_argo_application, cluster),
            on_mce(self._mce, read_hosted_cluster, cluster),
            on_mce(
                self._mce, read_hosted_health,
                HostedHealthRequest(mce_cluster=self._mce, cluster=self._cluster, nodepools=nodepools),
            ),
        )
        mce = (
            await on_mce(self._mce, read_mce_state, MceRequest(mce_cluster=self._mce))
            if include_idms
            else MceState(supported_versions_found=True)
        )
        return gates.preflight_checks(
            argo=argo, hosted=hosted, health=health, mce=mce,
            release=self._ctx.release, include_idms=include_idms,
        )

    async def _diagnose(
        self,
        step: UpgradeStep,
        from_version: str | None,
        nodepool: str | None,
        gate: str,
        failing_checks: list[CheckResult],
    ) -> Diagnosis:
        context = self._ctx
        await self._checkpoint()
        self._phase = UpgradePhase.AGENT_INVOCATION
        diagnosis = await diagnose(
            workflow_id=workflow.info().workflow_id,
            mce_cluster=self._mce,
            cluster=self._cluster,
            profile=context.profile,
            phase=f"{step.kind.value}-step",
            step=step,
            from_version=from_version,
            nodepool=nodepool,
            gate=gate,
            failing_checks=failing_checks,
            tier1_enabled=context.tier1_enabled,
            stopped=lambda: self._abort is not None,
        )
        if diagnosis is None:
            raise _AbortRequested()
        self._last_verdict = diagnosis.verdict
        self._log(
            "agent",
            detail=f"{diagnosis.verdict.verdict}: {diagnosis.verdict.diagnosis}"
            + (f" ({diagnosis.incident_path})" if diagnosis.incident_path else ""),
        )
        return diagnosis

    async def _needs_human(
        self, reason: str, *, verdict: AgentVerdict | None = None, during_abort: bool = False
    ) -> None:
        """Wait for a person: `retry` re-runs what failed; `abort` stops. Approved actions run meanwhile.

        Inside an abort (`during_abort`) only `retry` ends the wait: the abort
        is what is being retried. A `retry` left over from an earlier wait is
        dropped on the way in, so it can never answer this one.
        """
        previous = self._phase
        self._retry = None
        self._phase = UpgradePhase.NEEDS_HUMAN
        self._waiting_for = f"retry or abort: {reason}"
        if verdict is not None:
            self._last_verdict = verdict
            self._pending_actions = [PendingAction(action=action) for action in verdict.actions_proposed]
        self._log("needs-human", detail=reason)
        while True:
            ready = lambda: self._retry is not None or bool(self._approved_unexecuted())  # noqa: E731
            if during_abort:
                await workflow.wait_condition(ready)
            else:
                await self._wait_for(ready)
            for pending in self._approved_unexecuted():
                await self._execute(pending)
            if self._retry is not None:
                self._retry = None
                self._pending_actions = []
                self._phase = previous
                self._waiting_for = None
                return

    def _approved_unexecuted(self) -> list[PendingAction]:
        return [p for p in self._pending_actions if p.decision == "approved" and not p.executed and p.result is None]

    async def _execute(self, pending: PendingAction) -> None:
        """A tier-2 action a person approved, run with the WORKFLOW's identity."""
        try:
            result = await on_mce(
                self._mce,
                execute_proposed_action,
                ProposedActionRequest(
                    mce_cluster=self._mce, cluster=self._cluster,
                    action=pending.action, approved_by=pending.decided_by or "",
                ),
            )
        except ActivityError as err:
            if error_type(err) != ProposedActionRefusedError.__name__:
                raise
            pending.result = f"refused: {error_message(err)}"
            self._log("action-refused", detail=f"{pending.action.action_id}: {pending.result}")
            return
        pending.executed = result.executed
        pending.result = result.detail or ("executed" if result.executed else "not executed")
        self._log("action-executed", detail=f"{pending.action.action_id}: {pending.result}")

    async def _hold_if_paused(self) -> None:
        """A pause holds the run before its next commit; resume lets it go on."""
        if self._paused is None:
            return
        previous = self._phase
        self._phase = UpgradePhase.PAUSED
        paused = self._paused
        self._waiting_for = f"resume: paused by {paused.by}" + (f" ({paused.reason})" if paused.reason else "")
        await self._wait_for(lambda: self._paused is None)
        self._phase = previous
        self._waiting_for = None

    async def _checkpoint(self) -> None:
        """Right before every act — a commit, a sync, an admin ack, a child start, an agent call.

        An abort stops the run here; a pause holds it here until resume.
        """
        if self._abort is not None:
            raise _AbortRequested()
        await self._hold_if_paused()

    async def _abort_upgrade(self) -> None:
        """pausedUntil for the HostedCluster and every NodePool, committed and synced (D6).

        Never fails on what a person can fix: the day1 validator refusing the
        edit, or Argo CD refusing the sync over someone else's pending change.
        The run waits for them and retries, because a half-done abort — paused
        in git, still rolling on the cluster — must not be left with nobody
        watching it.
        """
        context = self._ctx
        self._pending_actions = []
        revision: str | None = None
        while True:
            self._phase = UpgradePhase.ABORTING
            self._waiting_for = None
            if revision is None:
                try:
                    commit: Day1Commit = await on_hub(
                        commit_day1_change,
                        Day1ChangeRequest(
                            workflow_id=workflow.info().workflow_id,
                            mce_cluster=self._mce,
                            cluster=self._cluster,
                            path=context.day1_path,
                            expected_blob=None,
                            change=Day1ChangeKind.PAUSE,
                        ),
                    )
                except ActivityError as err:
                    if error_type(err) != Day1ChangeRejectedError.__name__:
                        raise
                    await self._needs_human(
                        f"abort: the day1 validator refused pausedUntil: {error_message(err)}",
                        during_abort=True,
                    )
                    continue
                revision = commit.revision
                self._log("commit-pause", detail=revision)
            try:
                await on_mce(
                    self._mce,
                    sync_argo_application,
                    ArgoSyncRequest(
                        mce_cluster=self._mce,
                        cluster=self._cluster,
                        revision=revision,
                        allowed_out_of_sync=[
                            f"HostedCluster/{self._cluster}",
                            *(f"NodePool/{name}" for name in context.nodepool_order),
                        ],
                    ),
                    timeout=SYNC_TIMEOUT,
                )
            except ActivityError as err:
                if error_type(err) != ArgoSyncRefusedError.__name__:
                    raise
                await self._needs_human(
                    f"abort: pausedUntil is committed ({revision}) but Argo CD refused the sync: "
                    f"{error_message(err)}. Clear that change or sync by hand, then retry",
                    during_abort=True,
                )
                continue
            return

    # --- waiting --------------------------------------------------------------------

    async def _wait_for(self, condition: Callable[[], bool]) -> None:
        """A human wait: no timer and no deadline, ended by `condition` — or an abort."""
        await workflow.wait_condition(lambda: condition() or self._abort is not None)
        if self._abort is not None:
            raise _AbortRequested()

    async def _sleep_or_abort(self, interval: timedelta) -> None:
        try:
            await workflow.wait_condition(lambda: self._abort is not None, timeout=interval)
        except asyncio.TimeoutError:
            return
        raise _AbortRequested()

    async def _until_done(self, task: Any) -> Any:
        """Await a watch activity (started TRY_CANCEL) — unless a person aborts first, which cancels it.

        Only for those: a cancelled TRY_CANCEL activity is let go at once. A
        child, or anything awaiting a WAIT_CANCELLATION_COMPLETED call, would
        run on regardless — those stop through _checkpoint and signals instead.
        """
        await workflow.wait_condition(lambda: task.done() or self._abort is not None)
        if self._abort is None:
            return task.result()
        if not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, ActivityError):
                pass
        raise _AbortRequested()

    async def _continue_as_new(self) -> None:
        await workflow.wait_condition(workflow.all_handlers_finished)
        # An abort recorded while the handlers drained must not be lost to the next run.
        if self._abort is not None:
            raise _AbortRequested()
        workflow.continue_as_new(
            UpgradeClusterRunArgs(
                input=self._input,
                resume=UpgradeClusterResumeState(
                    context=self._ctx,
                    approved_by=self._approved_by or "",
                    next_step=self._next_step,
                    nodepools_done=list(self._nodepools_done),
                    step_committed=self._step_committed,
                    control_plane_version=self._cp_version or "",
                    nodepool_versions=dict(self._np_versions),
                    day1_blob=self._day1_blob or "",
                    paused=self._paused,
                    log=self._log_entries[-_LOG_LIMIT:],
                ),
            )
        )

    async def _finish(self, outcome: UpgradeOutcome) -> UpgradeClusterResult:
        if outcome is UpgradeOutcome.CANCELLED:
            self._log("cancelled", self._abort.by if self._abort else None, "before approval: nothing committed")
        elif outcome is UpgradeOutcome.ABORTED:
            self._log("aborted", self._abort.by if self._abort else None, "pausedUntil committed and synced")
        self._phase = _OUTCOME_PHASE[outcome]
        self._waiting_for = None
        await workflow.wait_condition(workflow.all_handlers_finished)
        context = self._context
        return UpgradeClusterResult(
            outcome=outcome,
            mce_cluster=self._mce,
            cluster=self._cluster,
            plan=context.plan if context else None,
            steps_completed=self._next_step,
            control_plane_version=self._cp_version,
            nodepool_versions=dict(self._np_versions),
            log=self._log_entries[-_LOG_LIMIT:],
        )

    # --- small things ---------------------------------------------------------------

    @property
    def _ctx(self) -> UpgradeContext:
        assert self._context is not None, "planning sets the context before any step"
        return self._context

    def _plan_summary(self) -> str:
        plan = self._ctx.plan if self._context else None
        if plan is None:
            return ""
        steps = ", ".join(
            f"{'control plane' if s.kind is StepKind.CONTROL_PLANE else 'workers'} → {s.version}"
            for s in plan.steps
        )
        return f"{plan.control_plane} → {plan.target} in {len(plan.steps)} steps ({steps})"

    def _log(self, event: str, by: str | None = None, detail: str = "") -> None:
        self._log_entries.append(UpgradeLogEntry(at=workflow.now(), event=event, by=by, detail=detail))
        del self._log_entries[:-_LOG_LIMIT]
