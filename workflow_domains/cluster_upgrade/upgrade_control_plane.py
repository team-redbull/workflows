"""upgrade-control-plane — ONE control-plane step of an upgrade-cluster run, on an MCE slot.

Started by UpgradeClusterWorkflow as a child whose id is a SLOT,
`upgrade-control-plane-<mce>_<n>` (shared/workflow_ids.py). Holding the slot is
what caps how many hosted control planes upgrade on one MCE at once, so once the
step has synced, the child lives as long as the upgrade it started — through a
stall and a person's retry too — and ends only when the HostedCluster runs the
version or the parent stops it. It is the domain's only child workflow, and it
has a lifecycle of its own: started with ParentClosePolicy.ABANDON, a parent
terminated mid-step leaves the slot taken until this step ends.

Shape of the run (the `progress` query exposes the phase):

  1. checking-mirror — re-resolve the step's tags to digests. Anything but the
                       digests the run was approved with stops here, before
                       anything is committed (DESIGN D2).
  2. committing      — `mastertag: <v>` in the day1 file, every NodePool's
                       release pinned explicitly so no worker moves with it;
                       compare-and-set on the file's blob id.
  3. syncing         — the cluster's one Argo CD Application, to exactly that
                       commit, refused if anything but the HostedCluster is
                       out of sync.
  4. watching        — one heartbeating activity until the HostedCluster runs
                       the version, nothing changes for the stall window, or
                       the deadline.
  5. diagnosing      — on a stall or the deadline only: the upgrade agent.
                       After a stall it may clear the gate, and the watch runs
                       ONCE more. Otherwise:
  6. needs-human     — the child tells the parent (signal NEEDS_HUMAN_SIGNAL),
                       which owns the person's decision, and waits, still
                       holding the slot: `retry` from the parent goes back to
                       4, `stop` ends the step.

The PARENT DRIVES IT WITH SIGNALS, never with cancellation: `stop` (an abort),
`hold` (a pause, forwarded), `retry` (a person's retry). Cancelling a child
cannot stop it reliably — an activity that does not heartbeat runs on and its
result comes back as if nothing happened — so the child checks `stop` and
`hold` itself right before every commit, every sync and every agent call, and
races `stop` against the watch and the agent. Parent and child are both code,
so fire-and-forget signals fit; a person's decisions are Updates, on the parent.

An early end before the sync (mirror changed, someone else's edit, the day1
validator, Argo CD refusing) is a RESULT, never a failure, and frees the slot:
the HostedCluster is not upgrading. A retry starts the child again, and each
part is idempotent for it — the commit sees the file already says so, the sync
sees the revision already synced.
"""

from __future__ import annotations

import asyncio

from temporalio import workflow
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from shared.exceptions import (
        ArgoSyncRefusedError,
        Day1ChangeRejectedError,
        Day1FileChangedError,
    )
    from shared.interfaces.cluster_upgrade import sync_argo_application, watch_control_plane
    from shared.interfaces.upgrade_gitops import commit_day1_change, pin_release_digests
    from shared.models.cluster_upgrade import (
        ArgoSyncRequest,
        CheckFailure,
        CheckResult,
        ControlPlaneStepOutcome,
        Day1ChangeKind,
        Day1ChangeRequest,
        Day1Commit,
        PinReleasesRequest,
        UpgradeControlPlaneInput,
        UpgradeControlPlaneProgress,
        UpgradeControlPlaneResult,
        UpgradeControlPlaneRunArgs,
        WatchControlPlaneRequest,
        WatchOutcome,
        WatchResult,
    )
    from shared.ocp_version import release_tag

from workflow_domains.cluster_upgrade.dispatch import (
    REGISTRY_TIMEOUT,
    SYNC_TIMEOUT,
    clears_gate,
    diagnose,
    error_message,
    error_type,
    on_hub,
    on_mce,
    start_watch,
)

# MACHINE convergence, so real deadlines (CLAUDE.md §5). A hosted control plane
# upgrade is CVO-driven and takes well under an hour on a healthy MCE; no
# change at all in 20 minutes is a stall worth a diagnosis, and three hours is
# the end of waiting (DESIGN §6.2).
CONTROL_PLANE_STALL_MINUTES = 20
CONTROL_PLANE_DEADLINE_MINUTES = 180

# The signal the child sends its parent when the step needs a person. A name,
# not a method reference: the parent's module imports this one.
NEEDS_HUMAN_SIGNAL = "control_plane_needs_human"


class _Stopped(Exception):
    """The parent stopped the step. Raised and caught inside run() only."""


@workflow.defn
class UpgradeControlPlaneWorkflow:
    def __init__(self) -> None:
        self._phase = "pending"
        self._input: UpgradeControlPlaneInput | None = None
        self._stop = False
        self._held = False
        self._retry = False

    @workflow.query
    def progress(self) -> UpgradeControlPlaneProgress:
        request = self._input
        return UpgradeControlPlaneProgress(
            phase=self._phase,
            parent_workflow_id=request.parent_workflow_id if request else "",
            cluster=request.cluster if request else "",
            version=request.step.version if request else "",
        )

    @workflow.signal
    def stop(self) -> None:
        """The parent is aborting: end the step at the next safe point."""
        self._stop = True

    @workflow.signal
    def hold(self, held: bool) -> None:
        """The parent is paused (True) or resumed (False): no commit, sync or agent call while held."""
        self._held = held

    @workflow.signal
    def retry(self) -> None:
        """A person retried: watch the same gate again."""
        self._retry = True

    @workflow.run
    async def run(self, run_args: UpgradeControlPlaneRunArgs) -> UpgradeControlPlaneResult:
        request = run_args.input
        self._input = request
        committed: dict = {}
        try:
            return await self._step(request, committed)
        except _Stopped:
            self._phase = "stopped"
            return UpgradeControlPlaneResult(
                outcome=ControlPlaneStepOutcome.STOPPED, step_index=request.step_index, **committed
            )

    async def _step(self, request: UpgradeControlPlaneInput, committed: dict) -> UpgradeControlPlaneResult:
        version = request.step.version

        # 1 — the release the run was approved with is still the one the tag names.
        await self._checkpoint()
        self._phase = "checking-mirror"
        now = await on_hub(
            pin_release_digests,
            PinReleasesRequest(versions=[version], release=request.release),
            timeout=REGISTRY_TIMEOUT,
        )
        current = now.pinned.get(version)
        if current != request.pinned:
            return self._result(
                request,
                ControlPlaneStepOutcome.MIRROR_CHANGED,
                detail=(
                    f"the mirror changed under the run: {release_tag(version)} was approved as "
                    f"{request.pinned.release} (operator {request.pinned.control_plane_operator}) "
                    f"and now resolves to "
                    + (f"{current.release} (operator {current.control_plane_operator})" if current else "nothing")
                ),
            )

        # 2 — the commit: mastertag, with every worker pinned where it is.
        await self._checkpoint()
        self._phase = "committing"
        try:
            commit: Day1Commit = await on_hub(
                commit_day1_change,
                Day1ChangeRequest(
                    workflow_id=request.parent_workflow_id,
                    mce_cluster=request.mce_cluster,
                    cluster=request.cluster,
                    path=request.day1_path,
                    expected_blob=request.day1_blob,
                    change=Day1ChangeKind.CONTROL_PLANE,
                    version=version,
                    nodepool_versions=request.nodepool_versions,
                ),
            )
        except ActivityError as err:
            outcome = {
                Day1FileChangedError.__name__: ControlPlaneStepOutcome.DAY1_CHANGED,
                Day1ChangeRejectedError.__name__: ControlPlaneStepOutcome.DAY1_REJECTED,
            }.get(error_type(err) or "")
            if outcome is None:
                raise
            return self._result(request, outcome, detail=error_message(err))
        committed.update(committed=True, revision=commit.revision, day1_blob=commit.blob)

        # 3 — Argo CD applies exactly that commit, and nothing of anyone else's.
        await self._checkpoint()
        self._phase = "syncing"
        try:
            await on_mce(
                request.mce_cluster,
                sync_argo_application,
                ArgoSyncRequest(
                    mce_cluster=request.mce_cluster,
                    cluster=request.cluster,
                    revision=commit.revision,
                    allowed_out_of_sync=[f"HostedCluster/{request.cluster}"],
                ),
                timeout=SYNC_TIMEOUT,
            )
        except ActivityError as err:
            if error_type(err) != ArgoSyncRefusedError.__name__:
                raise
            return self._result(
                request, ControlPlaneStepOutcome.SYNC_REFUSED, detail=error_message(err), **committed
            )

        # From here the HostedCluster is upgrading: the step, and its slot,
        # last until it runs the version or the parent stops it.
        while True:
            # 4 — the watch.
            self._phase = "watching"
            outcome = await self._watch(request)
            if outcome.result is WatchResult.DONE:
                self._phase = "done"
                return self._result(request, ControlPlaneStepOutcome.DONE, **committed)

            # 5 — a stall or the deadline: ask the agent, then the same gate at most once more.
            await self._checkpoint()
            self._phase = "diagnosing"
            failing = CheckResult(
                name="control-plane-converged",
                passed=False,
                expected=f"the HostedCluster at {version}, Completed, every ClusterOperator at {version}",
                observed=f"{outcome.result.value}: {outcome.snapshot.summary}",
                on_failure=CheckFailure.NEEDS_HUMAN,
            )
            diagnosis = await diagnose(
                workflow_id=request.parent_workflow_id,
                mce_cluster=request.mce_cluster,
                cluster=request.cluster,
                profile=request.profile,
                phase="control-plane-step",
                step=request.step,
                from_version=request.from_version,
                nodepool=None,
                gate=f"watch-control-plane-{outcome.result.value}",
                failing_checks=[failing],
                tier1_enabled=request.tier1_enabled,
                stopped=lambda: self._stop,
            )
            if diagnosis is None:
                raise _Stopped()
            if outcome.result is WatchResult.STALLED and clears_gate(diagnosis):
                self._phase = "watching"
                outcome = await self._watch(request)
                if outcome.result is WatchResult.DONE:
                    self._phase = "done"
                    return self._result(
                        request, ControlPlaneStepOutcome.DONE,
                        verdict=diagnosis.verdict, incident_path=diagnosis.incident_path, **committed,
                    )
                failing = failing.model_copy(
                    update={"observed": f"{outcome.result.value} again: {outcome.snapshot.summary}"}
                )

            # 6 — a person decides, through the parent; the slot stays taken meanwhile.
            self._phase = "needs-human"
            self._retry = False
            report = self._result(
                request,
                ControlPlaneStepOutcome.NEEDS_HUMAN,
                detail=f"the control plane did not reach {version}: {failing.observed}",
                verdict=diagnosis.verdict,
                incident_path=diagnosis.incident_path,
                failing_checks=[failing],
                **committed,
            )
            await workflow.get_external_workflow_handle(request.parent_workflow_id).signal(
                NEEDS_HUMAN_SIGNAL, report
            )
            await workflow.wait_condition(lambda: self._retry or self._stop)
            if self._stop:
                raise _Stopped()

    async def _checkpoint(self) -> None:
        """Right before a commit, a sync or the agent: a stop ends the step, a hold waits."""
        if self._held and not self._stop:
            previous, self._phase = self._phase, "held"
            await workflow.wait_condition(lambda: self._stop or not self._held)
            self._phase = previous
        if self._stop:
            raise _Stopped()

    async def _watch(self, request: UpgradeControlPlaneInput) -> WatchOutcome:
        """The heartbeating watch, unless the parent stops the step first (which cancels it)."""
        handle = start_watch(
            request.mce_cluster,
            watch_control_plane,
            WatchControlPlaneRequest(
                mce_cluster=request.mce_cluster,
                cluster=request.cluster,
                version=request.step.version,
                stall_minutes=CONTROL_PLANE_STALL_MINUTES,
                deadline_minutes=CONTROL_PLANE_DEADLINE_MINUTES,
            ),
            deadline_minutes=CONTROL_PLANE_DEADLINE_MINUTES,
        )
        await workflow.wait_condition(lambda: handle.done() or self._stop)
        if not handle.done():
            handle.cancel()
            try:
                await handle
            except (asyncio.CancelledError, ActivityError):
                pass
            raise _Stopped()
        return handle.result()

    def _result(
        self, request: UpgradeControlPlaneInput, outcome: ControlPlaneStepOutcome, **fields
    ) -> UpgradeControlPlaneResult:
        return UpgradeControlPlaneResult(outcome=outcome, step_index=request.step_index, **fields)
