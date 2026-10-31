"""upgrade-cluster: the real UpgradeClusterWorkflow and its control-plane child, on a fake fleet.

The time-skipping environment makes the week between two NodePools instant
(`env.sleep`), and every human decision is a real Temporal Update sent while
the run waits for it. The fleet behind the mocks (tests/cluster_upgrade_fakes.py)
is consistent: a sync applies the day1 file to the cluster, so a watch only
reports done for a version Argo CD really applied.

The default fleet: control plane and both NodePools at 4.18.56, target
4.20.44, ladder rungs 4.19.53 and 4.20.44 — so the plan is control plane
→ 4.19.53, control plane → 4.20.44, workers → 4.20.44.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
from temporalio.client import WorkflowExecutionStatus, WorkflowFailureError, WorkflowUpdateFailedError
from temporalio.exceptions import ApplicationError

from shared.exceptions import (
    ClusterFileNotFoundError,
    UpgradePlanningFailedError,
    UpgradeUpdateRefusedError,
)
from shared.interfaces import cluster_upgrade as cluster_interface
from shared.interfaces import upgrade_agent as agent_interface
from shared.interfaces import upgrade_gitops as gitops_interface
from shared.models.cluster_upgrade import (
    ActionDecision,
    ApiUsage,
    CheckFailure,
    CheckResult,
    ClusterUpgradeSettings,
    ContinueNodePool,
    ControlPlaneStepOutcome,
    Day1ChangeKind,
    GateOutcome,
    GateResult,
    HostedHealth,
    HumanDecision,
    NodePoolMode,
    ProposedAction,
    ToolCallRecord,
    UpgradeOutcome,
    UpgradePhase,
    WatchResult,
)
from shared.workflow_ids import upgrade_control_plane_workflow_id
from tests.cluster_upgrade_fakes import (
    ADMIN_GATE,
    AGENT_QUEUE,
    CLUSTER,
    HUB_QUEUE,
    MCE,
    MIRRORS,
    MCE_QUEUE,
    SEVEN_DAYS,
    WORKFLOW_ID,
    Fleet,
    Harness,
    digest,
    request,
    verdict,
)
from workflow_domains.cluster_upgrade.upgrade_cluster import UpgradeClusterWorkflow as Run
from workflow_domains.cluster_upgrade.upgrade_control_plane import UpgradeControlPlaneWorkflow

ALICE = HumanDecision(by="alice")
DAY2 = HumanDecision(by="day2-team")
KYVERNO = ApiUsage(
    api="validatingwebhookconfigurations.v1beta1.admissionregistration.k8s.io",
    username="system:serviceaccount:kyverno:kyverno-admission-controller", user_agent="kyverno/v1.10.0",
    verbs=["get", "update"], requests=12,
)


async def _approve_through_control_plane(h: Harness, steps: int = 2) -> None:
    """Approve, then confirm day2 after each of the plan's control-plane steps."""
    await h.until(UpgradePhase.AWAITING_APPROVAL)
    await h.decide(Run.approve, ALICE)
    for _ in range(steps):
        await h.until(UpgradePhase.AWAITING_DAY2_CONFIRM)
        await h.decide(Run.confirm_day2, DAY2)


async def _refused(h: Harness, update, arg) -> str:
    with pytest.raises(WorkflowUpdateFailedError) as excinfo:
        await h.decide(update, arg)
    cause = excinfo.value.cause
    assert isinstance(cause, ApplicationError) and cause.type == UpgradeUpdateRefusedError.__name__
    return cause.message


async def _fails(h: Harness) -> ApplicationError:
    with pytest.raises(WorkflowFailureError) as excinfo:
        await h.result()
    cause = excinfo.value.cause
    assert isinstance(cause, ApplicationError)
    return cause


class TestTheHappyPath:
    async def test_it_climbs_every_rung_then_rolls_the_nodepools_a_week_apart(self):
        fleet = Fleet()
        async with Harness(fleet) as h:
            await h.start(request())
            approval = await h.until(UpgradePhase.AWAITING_APPROVAL)
            # Nothing is committed before a person approves the plan they can read.
            assert fleet.commits == []
            assert [(s.kind.value, s.version) for s in approval.plan.steps] == [
                ("control-plane", "4.19.53"), ("control-plane", "4.20.44"), ("workers", "4.20.44"),
            ]
            assert approval.plan.pinned["4.19.53"] == digest("4.19.53")
            assert "2026-10-31" in approval.waiting_for

            await _approve_through_control_plane(h)
            await h.until(UpgradePhase.AWAITING_CONTINUE)
            await h.decide(Run.continue_nodepool, ContinueNodePool(by="alice"))
            waiting = await h.until(UpgradePhase.AWAITING_CONTINUE)
            assert waiting.nodepools_done == ["np-a"]
            # The week a person takes between two NodePools costs the run nothing.
            await h.env.sleep(SEVEN_DAYS)
            await h.decide(Run.continue_nodepool, ContinueNodePool(by="bob"))
            result = await h.result()

        assert result.outcome is UpgradeOutcome.DONE
        assert result.control_plane_version == "4.20.44"
        assert result.nodepool_versions == {"np-a": "4.20.44", "np-b": "4.20.44"}
        assert fleet.commit_summary() == [
            "control-plane::4.19.53", "control-plane::4.20.44",
            "nodepool:np-a:4.20.44", "nodepool:np-b:4.20.44", "finish",
        ]
        # Every control-plane commit pins the workers where they are, so
        # moving mastertag never moves a NodePool.
        assert fleet.commits[0].nodepool_versions == {"np-a": "4.18.56", "np-b": "4.18.56"}
        assert [s.allowed_out_of_sync for s in fleet.syncs] == [
            [f"HostedCluster/{CLUSTER}"], [f"HostedCluster/{CLUSTER}"], ["NodePool/np-a"], ["NodePool/np-b"],
        ]
        # One run per unit of work: each control-plane step, each NodePool.
        assert len(set(fleet.commit_runs)) == 4
        continued = [e.at for e in result.log if e.event == "continue"]
        assert continued[1] - continued[0] >= SEVEN_DAYS
        # The happy path is boring: no agent, no incident.
        assert fleet.agent_requests == [] and fleet.incidents == []

    async def test_day2_must_confirm_after_every_control_plane_step(self):
        fleet = Fleet()
        async with Harness(fleet) as h:
            await h.start(request())
            await h.until(UpgradePhase.AWAITING_APPROVAL)
            await h.decide(Run.approve, ALICE)
            first = await h.until(UpgradePhase.AWAITING_DAY2_CONFIRM)
            assert "day2.gitops/cluster=" + CLUSTER in first.waiting_for
            assert "ocp-version 4.19.53" in first.waiting_for
            # The next control-plane step waits for the day2 team, however long.
            await h.env.sleep(timedelta(days=2))
            assert fleet.commit_summary() == ["control-plane::4.19.53"]
            await h.decide(Run.confirm_day2, DAY2)
            second = await h.until(UpgradePhase.AWAITING_DAY2_CONFIRM)
            assert "ocp-version 4.20.44" in second.waiting_for
            await h.decide(Run.abort, HumanDecision(by="alice", reason="end of test"))
            await h.result()
        assert [e.by for e in second.log if e.event == "day2-confirmed"] == ["day2-team"]

    async def test_auto_mode_rolls_by_zone_and_soaks_in_the_gate(self):
        fleet = Fleet(
            control_plane="4.20.44",
            nodepools={"zone-c": "4.19.53", "zone-a": "4.19.53", "zone-b": "4.19.53"},
            zones={"zone-a": "a", "zone-b": "b", "zone-c": "c"},
        )
        async with Harness(fleet) as h:
            await h.start(request(nodepool_mode=NodePoolMode.AUTO))
            approval = await h.until(UpgradePhase.AWAITING_APPROVAL)
            assert approval.profile.value == "zoned"
            await h.decide(Run.approve, ALICE)
            result = await h.result()

        assert result.outcome is UpgradeOutcome.DONE
        assert [g.nodepool for g in fleet.gate_requests] == ["zone-a", "zone-b", "zone-c"]
        # Zoned: workload checks on, and a 30-minute soak inside every gate.
        assert {(g.soak_minutes, g.workload_checks) for g in fleet.gate_requests} == {(30, True)}
        assert fleet.commit_summary()[:3] == [
            "nodepool:zone-a:4.20.44", "nodepool:zone-b:4.20.44", "nodepool:zone-c:4.20.44",
        ]

    async def test_a_cluster_already_at_the_target_completes_with_nothing_done(self):
        fleet = Fleet(control_plane="4.20.44")
        async with Harness(fleet) as h:
            await h.start(request())
            result = await h.result()
        assert result.outcome is UpgradeOutcome.DONE
        assert result.plan.steps == [] and fleet.commits == []

    async def test_a_target_minor_takes_its_newest_mirrored_rung_and_wave_zero_auto_approves(self):
        fleet = Fleet(control_plane="4.19.53", upgrade=ClusterUpgradeSettings(wave=0))
        async with Harness(fleet) as h:
            await h.start(request(target_version=None, target_minor="4.20", auto_approve=True,
                                  workers_to_target=False))
            await h.until(UpgradePhase.AWAITING_DAY2_CONFIRM)
            await h.decide(Run.confirm_day2, DAY2)
            result = await h.result()
        assert result.plan.target == "4.20.44"
        assert [e.by for e in result.log if e.event == "approved"] == ["auto-approve, requested by ops@example"]


class TestPlanningRefusals:
    async def test_the_mce_comes_from_the_day1_folder_and_the_day1_read_comes_first(self):
        fleet = Fleet()
        async with Harness(fleet) as h:
            await h.start(request())
            progress = await h.until(UpgradePhase.AWAITING_APPROVAL)
        # Every MCE activity reached the fake MCE's queue, named by that folder.
        assert progress.mce_cluster == MCE
        assert fleet.calls[:2] == ["read_day1_cluster", "read_upgrade_rules"]

    async def test_a_cluster_file_outside_the_day1_layout_fails_with_nothing_read_on_an_mce(self):
        fleet = Fleet()
        fleet.day1_path = f"clusters/{CLUSTER}.yaml"
        async with Harness(fleet) as h:
            await h.start(request())
            failure = await _fails(h)
        assert failure.type == UpgradePlanningFailedError.__name__
        assert "not under sites/<site>/mces/<mce>/hostedClusters/" in failure.message
        assert fleet.calls == ["read_day1_cluster"]

    async def test_a_release_missing_from_the_mirror_fails_naming_what_to_mirror(self):
        fleet = Fleet(mirror=["4.20.44"])
        async with Harness(fleet) as h:
            await h.start(request())
            failure = await _fails(h)
        assert failure.type == UpgradePlanningFailedError.__name__
        assert "mirror 4.19.53-x86_64 first" in failure.message
        assert "kind: ImageSetConfiguration" in failure.message and "minVersion: 4.19.53" in failure.message
        assert fleet.commits == []

    async def test_auto_approve_needs_a_wave_zero_or_one_cluster(self):
        fleet = Fleet(upgrade=ClusterUpgradeSettings(wave=3))
        async with Harness(fleet) as h:
            await h.start(request(auto_approve=True))
            failure = await _fails(h)
        assert "upgrade.wave is 3" in failure.message
        assert fleet.commits == []

    async def test_a_target_minor_with_no_mirrored_rung_fails(self):
        fleet = Fleet(mirror=["4.19.53"])
        async with Harness(fleet) as h:
            await h.start(request(target_version=None, target_minor="4.20"))
            failure = await _fails(h)
        assert "mirror a rung of 4.20 first" in failure.message

    async def test_a_nodepool_order_that_leaves_one_out_fails(self):
        fleet = Fleet()
        async with Harness(fleet) as h:
            await h.start(request(nodepool_order=["np-b"]))
            failure = await _fails(h)
        assert "name each one exactly once" in failure.message


class TestPreflight:
    async def test_a_pdb_at_zero_waits_for_its_owner_and_retry_checks_again(self):
        fleet = Fleet()
        fleet.health_script = [HostedHealth(
            reachable=True, cluster_version_upgradeable=True, idms_mirrors=MIRRORS,
            blocking_pdbs=["db/pg-pdb"],
        )]
        async with Harness(fleet) as h:
            await h.start(request())
            waiting = await h.until(UpgradePhase.NEEDS_HUMAN)
            assert "its owner must act first" in waiting.waiting_for and "db/pg-pdb" in waiting.waiting_for
            await h.decide(Run.retry, HumanDecision(by="db-owner", reason="pdb relaxed"))
            await h.until(UpgradePhase.AWAITING_APPROVAL)
            # Still before approval: aborting now cancels, with nothing committed.
            await h.decide(Run.abort, HumanDecision(by="alice", reason="end of test"))
            result = await h.result()
        assert result.outcome is UpgradeOutcome.CANCELLED and fleet.commits == []

    async def test_a_control_plane_still_settling_is_waited_for_not_escalated(self):
        fleet = Fleet()
        fleet.settling = 3  # planning's read, then two pre-flight reads
        async with Harness(fleet) as h:
            await h.start(request())
            await h.until(UpgradePhase.PREFLIGHT)
            await h.env.sleep(timedelta(minutes=5))
            waiting = await h.until(UpgradePhase.AWAITING_APPROVAL)
            await h.decide(Run.abort, HumanDecision(by="alice", reason="end of test"))
            await h.result()
        assert not any(e.event == "needs-human" for e in waiting.log)


class TestHumanDecisions:
    async def test_a_decision_the_run_is_not_waiting_for_is_refused_with_what_it_waits_for(self):
        fleet = Fleet()
        async with Harness(fleet) as h:
            await h.start(request())
            await h.until(UpgradePhase.AWAITING_APPROVAL)
            message = await _refused(h, Run.continue_nodepool, ContinueNodePool(by="alice"))
            assert "awaiting-approval" in message and "approve:" in message
            await _refused(h, Run.confirm_day2, DAY2)
            await _refused(h, Run.retry, ALICE)
            assert "nothing is committed before approval" in await _refused(h, Run.pause, ALICE)
            assert "not paused" in await _refused(h, Run.resume, ALICE)
            await h.decide(Run.approve, ALICE)
            await h.until(UpgradePhase.AWAITING_DAY2_CONFIRM)
            await _refused(h, Run.approve, ALICE)
            await h.decide(Run.confirm_day2, DAY2)
            await h.until(UpgradePhase.AWAITING_DAY2_CONFIRM)
            await h.decide(Run.confirm_day2, DAY2)
            await h.until(UpgradePhase.AWAITING_CONTINUE)
            message = await _refused(h, Run.continue_nodepool, ContinueNodePool(by="alice", nodepool="np-z"))
            assert "np-z is not waiting to roll" in message
            # Naming the NodePool picks it, out of the default order.
            await h.decide(Run.continue_nodepool, ContinueNodePool(by="alice", nodepool="np-b"))
            await h.until(UpgradePhase.AWAITING_CONTINUE)
            await h.decide(Run.continue_nodepool, ContinueNodePool(by="alice"))
            await h.result()
        assert fleet.commit_summary()[2:4] == ["nodepool:np-b:4.20.44", "nodepool:np-a:4.20.44"]

    async def test_abort_before_approval_cancels_with_nothing_committed(self):
        fleet = Fleet()
        async with Harness(fleet) as h:
            await h.start(request())
            await h.until(UpgradePhase.AWAITING_APPROVAL)
            await h.decide(Run.abort, HumanDecision(by="alice", reason="wrong cluster"))
            result = await h.result()
        assert result.outcome is UpgradeOutcome.CANCELLED
        assert fleet.commits == [] and fleet.syncs == []

    async def test_pause_holds_the_next_commit_and_survives_continue_as_new(self):
        fleet = Fleet()
        async with Harness(fleet) as h:
            await h.start(request())
            await h.until(UpgradePhase.AWAITING_APPROVAL)
            await h.decide(Run.approve, ALICE)
            await h.until(UpgradePhase.AWAITING_DAY2_CONFIRM)
            # Paused in the run that is about to continue as new; the next run
            # must still hold before its commit.
            await h.decide(Run.pause, HumanDecision(by="alice", reason="change freeze"))
            await h.decide(Run.confirm_day2, DAY2)
            paused = await h.until(UpgradePhase.PAUSED)
            assert paused.paused and "change freeze" in paused.waiting_for
            await h.env.sleep(timedelta(days=1))
            assert fleet.commit_summary() == ["control-plane::4.19.53"]
            assert len(set(fleet.commit_runs)) == 1
            await h.decide(Run.resume, ALICE)
            await h.until(UpgradePhase.AWAITING_DAY2_CONFIRM)
            assert fleet.commit_summary() == ["control-plane::4.19.53", "control-plane::4.20.44"]
            assert len(set(fleet.commit_runs)) == 2
            await h.decide(Run.abort, HumanDecision(by="alice", reason="end of test"))
            await h.result()

    async def test_abort_mid_roll_commits_paused_until_and_syncs_everything(self):
        fleet = Fleet()
        fleet.block_nodepool_watch = True
        async with Harness(fleet) as h:
            await h.start(request())
            await _approve_through_control_plane(h)
            await h.until(UpgradePhase.AWAITING_CONTINUE)
            await h.decide(Run.continue_nodepool, ContinueNodePool(by="alice"))
            await h.until(UpgradePhase.NODEPOOL_ROLL)
            await h.decide(Run.abort, HumanDecision(by="alice", reason="customer escalation"))
            result = await h.result()

        assert result.outcome is UpgradeOutcome.ABORTED
        assert fleet.commit_summary()[-2:] == ["nodepool:np-a:4.20.44", "pause"]
        assert fleet.commits[-1].expected_blob is None  # an abort lands whatever the file holds
        assert fleet.syncs[-1].allowed_out_of_sync == [
            f"HostedCluster/{CLUSTER}", "NodePool/np-a", "NodePool/np-b",
        ]
        assert fleet.paused_until
        assert [e.by for e in result.log if e.event == "abort-requested"] == ["alice"]


class TestAbortDuringAControlPlaneStep:
    async def test_the_child_is_stopped_its_slot_freed_and_paused_until_committed(self):
        fleet = Fleet(max_concurrent_control_planes=1)
        fleet.block_control_plane_watch = True
        async with Harness(fleet) as h:
            await h.start(request())
            await h.until(UpgradePhase.AWAITING_APPROVAL)
            await h.decide(Run.approve, ALICE)
            await h.until(UpgradePhase.CONTROL_PLANE_STEP)
            await h.called("watch-cp:4.19.53")
            await h.decide(Run.abort, HumanDecision(by="alice", reason="api latency"))
            result = await h.result()
            child = h.client.get_workflow_handle_for(
                UpgradeControlPlaneWorkflow.run, upgrade_control_plane_workflow_id(MCE, 1)
            )
            stopped = await child.result()

        assert result.outcome is UpgradeOutcome.ABORTED
        # The step's own commit stays: nothing is ever reverted, the run pauses instead.
        assert fleet.commit_summary() == ["control-plane::4.19.53", "pause"]
        # Stopped by signal — not cancelled — and closed, so the slot is free.
        assert stopped.outcome is ControlPlaneStepOutcome.STOPPED and stopped.committed

    async def test_an_abort_while_the_child_pins_stops_it_before_its_commit(self):
        """A non-heartbeating call in flight: the child must stop after it, not commit."""
        fleet = Fleet()
        fleet.block_step_pin = True
        async with Harness(fleet) as h:
            await h.start(request())
            await h.until(UpgradePhase.AWAITING_APPROVAL)
            await h.decide(Run.approve, ALICE)
            await h.called("pin_release_digests", times=2)  # planning's, then the step's
            await h.decide(Run.abort, HumanDecision(by="alice", reason="wrong window"))
            await asyncio.sleep(0.2)
            fleet.block_step_pin = False
            result = await h.result()
        assert result.outcome is UpgradeOutcome.ABORTED
        assert fleet.commit_summary() == ["pause"]
        assert not any(c.startswith("sync:") and c != f"sync:rev-2" for c in fleet.calls[:-1])

    async def test_a_child_that_fails_is_a_person_s_call_and_retry_starts_it_again(self):
        """A child ending with no result — failed here; terminated by an operator in production.

        (Termination itself is not exercised: the time-skipping test server
        never tells a parent its child was terminated. The parent takes the
        same ChildWorkflowError path for both.)
        """
        fleet = Fleet()
        fleet.commit_errors = [ClusterFileNotFoundError(f"no file for {CLUSTER}")]
        async with Harness(fleet) as h:
            await h.start(request(workers_to_target=False))
            await h.until(UpgradePhase.AWAITING_APPROVAL)
            await h.decide(Run.approve, ALICE)
            waiting = await h.until(UpgradePhase.NEEDS_HUMAN)
            assert "ended without a result" in waiting.waiting_for
            assert f"no file for {CLUSTER}" in waiting.waiting_for
            await h.decide(Run.retry, ALICE)
            await h.until(UpgradePhase.AWAITING_DAY2_CONFIRM)
            await h.decide(Run.abort, HumanDecision(by="alice", reason="end of test"))
            await h.result()
        assert fleet.commit_summary() == ["control-plane::4.19.53", "pause"]


class TestPauseAndAbortReachEveryAct:
    async def test_a_pause_while_every_slot_is_taken_holds_the_step_once_one_frees(self):
        fleet = Fleet(max_concurrent_control_planes=1)
        async with Harness(fleet) as h:
            (blocker,) = await h.block_slots(1)
            await h.start(request(workers_to_target=False))
            await h.until(UpgradePhase.AWAITING_APPROVAL)
            await h.decide(Run.approve, ALICE)
            await h.until(UpgradePhase.WAITING_FOR_SLOT)
            await h.decide(Run.pause, HumanDecision(by="alice", reason="freeze"))
            await blocker.terminate()
            await h.env.sleep(timedelta(minutes=3))
            await h.until(UpgradePhase.PAUSED)
            assert fleet.commits == []
            await h.decide(Run.resume, ALICE)
            await h.until(UpgradePhase.AWAITING_DAY2_CONFIRM)
            await h.decide(Run.abort, HumanDecision(by="alice", reason="end of test"))
            await h.result()
        assert fleet.commit_summary() == ["control-plane::4.19.53", "pause"]

    async def test_an_abort_during_a_nodepool_s_pin_lands_no_roll(self):
        fleet = Fleet(control_plane="4.20.44", nodepools={"np-a": "4.19.53"})
        async with Harness(fleet) as h:
            await h.start(request(nodepool_mode=NodePoolMode.AUTO))
            await h.until(UpgradePhase.AWAITING_APPROVAL)
            fleet.block_step_pin = True  # planning pinned already; this holds the roll's re-pin
            await h.decide(Run.approve, ALICE)
            await h.called("pin_release_digests", times=2)
            await h.decide(Run.abort, HumanDecision(by="alice", reason="change of plan"))
            await asyncio.sleep(0.2)
            fleet.block_step_pin = False
            result = await h.result()
        assert result.outcome is UpgradeOutcome.ABORTED
        assert fleet.commit_summary() == ["pause"]

    async def test_an_abort_while_evidence_is_collected_never_asks_the_agent(self):
        fleet = Fleet(control_plane="4.20.44", nodepools={"np-a": "4.19.53"})
        fleet.np_watch_script = [WatchResult.STALLED]
        fleet.block_evidence = True
        async with Harness(fleet) as h:
            await h.start(request(nodepool_mode=NodePoolMode.AUTO))
            await h.until(UpgradePhase.AWAITING_APPROVAL)
            await h.decide(Run.approve, ALICE)
            await h.called("collect_evidence")
            await h.decide(Run.abort, HumanDecision(by="alice", reason="change of plan"))
            await asyncio.sleep(0.2)
            fleet.block_evidence = False
            result = await h.result()
        assert result.outcome is UpgradeOutcome.ABORTED
        assert fleet.agent_requests == [] and fleet.incidents == []

    async def test_an_abort_whose_sync_argo_refuses_waits_for_a_person_not_a_failure(self):
        fleet = Fleet()
        async with Harness(fleet) as h:
            await h.start(request())
            await h.until(UpgradePhase.AWAITING_APPROVAL)
            await h.decide(Run.approve, ALICE)
            await h.until(UpgradePhase.AWAITING_DAY2_CONFIRM)
            fleet.sync_errors = [fleet.sync_refused("Secret/extra is out of sync")]
            await h.decide(Run.abort, HumanDecision(by="alice", reason="escalation"))
            waiting = await h.until(UpgradePhase.NEEDS_HUMAN)
            assert "pausedUntil is committed" in waiting.waiting_for
            assert "Secret/extra" in waiting.waiting_for
            # Inside an abort there is nothing to pause, and nothing more to abort.
            assert "aborting" in await _refused(h, Run.pause, ALICE)
            assert "aborting" in await _refused(h, Run.abort, HumanDecision(by="bob", reason="again"))
            await h.decide(Run.retry, HumanDecision(by="alice", reason="cleared"))
            result = await h.result()
        assert result.outcome is UpgradeOutcome.ABORTED
        # One pause commit; its sync tried twice.
        assert fleet.commit_summary() == ["control-plane::4.19.53", "pause"]
        assert fleet.paused_until and fleet.syncs[-1].revision == f"rev-{fleet.revision}"

    async def test_a_second_refusal_waits_again_however_many_retries_came_before(self):
        fleet = Fleet(control_plane="4.20.44", nodepools={"np-a": "4.19.53"})
        fleet.sync_errors = [fleet.sync_refused(), fleet.sync_refused()]
        async with Harness(fleet) as h:
            await h.start(request(nodepool_mode=NodePoolMode.AUTO))
            await h.until(UpgradePhase.AWAITING_APPROVAL)
            await h.decide(Run.approve, ALICE)
            await h.until(UpgradePhase.NEEDS_HUMAN)
            await h.decide(Run.retry, ALICE)
            # A retry the run is no longer waiting for is refused, never saved up.
            try:
                await h.decide(Run.retry, HumanDecision(by="alice", reason="double click"))
            except WorkflowUpdateFailedError:
                pass
            await h.called("sync:rev-2", times=2)
            second = await h.until(UpgradePhase.NEEDS_HUMAN)
            await h.env.sleep(timedelta(minutes=5))
            still = await h.progress()
            assert still.phase is UpgradePhase.NEEDS_HUMAN
            assert sum(e.event == "needs-human" for e in second.log) == 2
            await h.decide(Run.retry, ALICE)
            result = await h.result()
        assert result.outcome is UpgradeOutcome.DONE
        assert fleet.calls.count("sync:rev-2") == 3


class TestGatesAndTheAgent:
    async def test_a_stall_the_agent_clears_is_watched_once_more_and_passes(self):
        fleet = Fleet(control_plane="4.20.44", nodepools={"np-a": "4.19.53"})
        fleet.np_watch_script = [WatchResult.STALLED]
        fleet.verdicts = [verdict("retry_gate")]
        async with Harness(fleet) as h:
            await h.start(request(nodepool_mode=NodePoolMode.AUTO))
            await h.until(UpgradePhase.AWAITING_APPROVAL)
            await h.decide(Run.approve, ALICE)
            result = await h.result()

        assert result.outcome is UpgradeOutcome.DONE
        assert [c for c in fleet.calls if c.startswith(("watch-np", "agent"))] == [
            "watch-np:np-a:4.20.44", "agent:watch-nodepool-stalled", "watch-np:np-a:4.20.44",
        ]
        (asked,) = fleet.agent_requests
        assert asked.invocation_id.startswith(f"{WORKFLOW_ID}/")
        assert asked.nodepool == "np-a" and asked.from_version == "4.19.53"
        assert asked.tier1_enabled is False  # everything is a proposal in production (AGENT.md #25)
        assert asked.evidence.sections
        assert len(fleet.incidents) == 1 and fleet.incidents[0].verdict.verdict == "retry_gate"

    async def test_a_second_failure_waits_for_a_person_and_retry_re_runs_the_same_gate(self):
        fleet = Fleet(control_plane="4.20.44", nodepools={"np-a": "4.19.53"})
        fleet.np_watch_script = [WatchResult.STALLED, WatchResult.STALLED]
        fleet.verdicts = [verdict("retry_gate")]
        async with Harness(fleet) as h:
            await h.start(request(nodepool_mode=NodePoolMode.AUTO))
            await h.until(UpgradePhase.AWAITING_APPROVAL)
            await h.decide(Run.approve, ALICE)
            waiting = await h.until(UpgradePhase.NEEDS_HUMAN)
            assert "np-a did not reach 4.20.44" in waiting.waiting_for
            assert waiting.last_verdict.verdict == "retry_gate"
            await h.decide(Run.retry, ALICE)
            result = await h.result()

        assert result.outcome is UpgradeOutcome.DONE
        # The retry re-ran the watch; it did not commit or pre-flight again.
        assert fleet.commit_summary().count("nodepool:np-a:4.20.44") == 1
        assert len(fleet.agent_requests) == 1

    async def test_an_agent_proposal_runs_only_once_a_person_approves_it(self):
        fleet = Fleet(control_plane="4.20.44", nodepools={"np-a": "4.19.53"})
        failing = CheckResult(name="pdbs-back", passed=False, expected="disruptionsAllowed >= 1",
                              observed="db/pdb at 0", on_failure=CheckFailure.NEEDS_HUMAN)
        fleet.gate_script = [GateOutcome(result=GateResult.DEADLINE, failing_checks=[failing])]
        action = ProposedAction(action_id="act-1", kind="delete_pod", target="openshift-monitoring/prom-0",
                                rationale="stuck terminating")
        fleet.verdicts = [verdict("retry_gate", actions_proposed=[action])]
        async with Harness(fleet) as h:
            await h.start(request(nodepool_mode=NodePoolMode.AUTO))
            await h.until(UpgradePhase.AWAITING_APPROVAL)
            await h.decide(Run.approve, ALICE)
            waiting = await h.until(UpgradePhase.NEEDS_HUMAN)
            assert [p.action.action_id for p in waiting.pending_actions] == ["act-1"]
            assert "db/pdb at 0" in waiting.waiting_for
            # A proposal is never executed on the agent's say-so.
            assert fleet.actions == []
            with pytest.raises(WorkflowUpdateFailedError):
                await h.decide(Run.approve_action, ActionDecision(by="alice", action_id="act-404"))
            await h.decide(Run.approve_action, ActionDecision(by="alice", action_id="act-1"))
            with pytest.raises(WorkflowUpdateFailedError):
                await h.decide(Run.reject_action, ActionDecision(by="bob", action_id="act-1"))
            await h.decide(Run.retry, ALICE)
            result = await h.result()

        assert result.outcome is UpgradeOutcome.DONE
        (executed,) = fleet.actions
        assert executed.approved_by == "alice" and executed.action.target == "openshift-monitoring/prom-0"
        assert [c for c in fleet.calls if c.startswith("gate:")] == ["gate:np-a", "gate:np-a"]

    async def test_a_drain_a_pdb_blocks_goes_to_the_agent_early_and_lab_waves_let_it_act(self):
        # Seen live (CONTEXT §8.13): an OpenShift PDB whose pods crash-loop
        # blocks a node's drain for ever. The watch hands it over after
        # DRAIN_BLOCKED_MINUTES; in a lab wave the agent deletes those pods
        # itself (tier 1, AGENT.md #25) and the run watches once more (D17).
        fleet = Fleet(control_plane="4.20.44", nodepools={"np-a": "4.19.53"},
                      upgrade=ClusterUpgradeSettings(wave=0))
        fleet.np_watch_script = [WatchResult.STALLED]
        fleet.verdicts = [verdict(
            "retry_gate",
            diagnosis="the drain of w-2 waits on PDB openshift-console/downloads: both its pods are in CrashLoopBackOff",
            actions_taken=[ToolCallRecord(tool="delete_pod", tier=1, judged="approved",
                                          args={"namespace": "openshift-console", "name": "downloads-7f9c"},
                                          result="deleted", at="2026-10-31T06:06:16Z")],
        )]
        async with Harness(fleet) as h:
            await h.start(request(nodepool_mode=NodePoolMode.AUTO))
            await h.until(UpgradePhase.AWAITING_APPROVAL)
            await h.decide(Run.approve, ALICE)
            result = await h.result()

        assert result.outcome is UpgradeOutcome.DONE
        assert {r.drain_blocked_minutes for r in fleet.np_watch_requests} == {10}
        (asked,) = fleet.agent_requests
        assert asked.tier1_enabled is True
        # What the agent did is in the incident, and nobody was asked.
        assert fleet.incidents[0].verdict.actions_taken[0].tool == "delete_pod"
        assert not any(e.event == "needs-human" for e in result.log)

    async def test_a_control_plane_deadline_is_never_retried_by_the_run_itself(self):
        fleet = Fleet()
        fleet.cp_watch_script = [WatchResult.DEADLINE]
        fleet.verdicts = [verdict("retry_gate")]
        async with Harness(fleet) as h:
            await h.start(request(workers_to_target=False))
            await h.until(UpgradePhase.AWAITING_APPROVAL)
            await h.decide(Run.approve, ALICE)
            waiting = await h.until(UpgradePhase.NEEDS_HUMAN)
            assert "did not reach 4.19.53" in waiting.waiting_for
            assert [c for c in fleet.calls if c.startswith("watch-cp")] == ["watch-cp:4.19.53"]
            # The HostedCluster is still upgrading, so its slot stays taken
            # while a person decides: the child waits, it has not returned.
            slot = await h.client.get_workflow_handle(upgrade_control_plane_workflow_id(MCE, 1)).describe()
            assert slot.status is WorkflowExecutionStatus.RUNNING
            await h.decide(Run.retry, ALICE)
            await h.until(UpgradePhase.AWAITING_DAY2_CONFIRM)
            await h.decide(Run.abort, HumanDecision(by="alice", reason="end of test"))
            await h.result()
        # The retry re-ran the same child's watch: no second commit, no second agent call.
        assert fleet.commit_summary() == ["control-plane::4.19.53", "pause"]
        assert [c for c in fleet.calls if c.startswith("watch-cp")] == ["watch-cp:4.19.53"] * 2
        assert len(fleet.agent_requests) == 1

    async def test_an_agent_that_cannot_be_reached_is_a_person_s_call_not_a_failure(self):
        fleet = Fleet(control_plane="4.20.44", nodepools={"np-a": "4.19.53"})
        fleet.np_watch_script = [WatchResult.STALLED]
        fleet.agent_down = True
        async with Harness(fleet) as h:
            await h.start(request(nodepool_mode=NodePoolMode.AUTO))
            await h.until(UpgradePhase.AWAITING_APPROVAL)
            await h.decide(Run.approve, ALICE)
            await h.until(UpgradePhase.AGENT_INVOCATION)
            await h.env.sleep(timedelta(minutes=1))  # past the one retry's backoff
            waiting = await h.until(UpgradePhase.NEEDS_HUMAN)
            await h.decide(Run.abort, HumanDecision(by="alice", reason="end of test"))
            await h.result()
        # Two attempts, then a person: the run neither fails nor waits forever.
        assert [c for c in fleet.calls if c.startswith("agent:")] == ["agent:watch-nodepool-stalled"] * 2
        assert waiting.last_verdict.verdict == "needs_human"
        assert "could not be reached" in waiting.last_verdict.diagnosis
        assert fleet.incidents == []


class TestAdminAcks:
    """D16: before a control-plane minor step, the admin acks the hosted cluster asks for.

    The default fleet's 4.19 control plane holds ack-4.19-admissionregistration-
    v1beta1-api-removals-in-4.20, which the rules file lists, so every run in
    this file that goes 4.19 -> 4.20 passes through this step.
    """

    async def test_with_no_caller_of_the_removed_apis_the_run_gives_the_ack_itself(self):
        fleet = Fleet()
        async with Harness(fleet) as h:
            await h.start(request(nodepool_mode=NodePoolMode.AUTO))
            approval = await h.until(UpgradePhase.AWAITING_APPROVAL)
            assert f"{ADMIN_GATE} before 4.20.44" in approval.waiting_for
            await _approve_through_control_plane(h)
            result = await h.result()

        assert result.outcome is UpgradeOutcome.DONE
        assert fleet.acks_given == [ADMIN_GATE]
        # Given at 4.19, after the step's pre-flight, before its commit.
        assert fleet.calls.index(f"ack:{ADMIN_GATE}") < fleet.calls.index("commit:control-plane::4.20.44")
        assert fleet.calls.index(f"ack:{ADMIN_GATE}") > fleet.calls.index("commit:control-plane::4.19.53")
        (logged,) = [e for e in result.log if e.event == "admin-ack"]
        assert logged.by == "workflow" and "no client called its APIs" in logged.detail

    async def test_a_gate_gone_by_the_time_of_the_ack_is_read_again_not_written(self):
        fleet = Fleet()
        fleet.ack_gate_vanishes = True
        async with Harness(fleet) as h:
            await h.start(request(nodepool_mode=NodePoolMode.AUTO))
            await h.until(UpgradePhase.AWAITING_APPROVAL)
            await _approve_through_control_plane(h)
            result = await h.result()

        assert result.outcome is UpgradeOutcome.DONE
        assert fleet.acks_given == [] and not [e for e in result.log if e.event == "admin-ack"]
        (gone,) = [e for e in result.log if e.event == "admin-ack-gone"]
        assert ADMIN_GATE in gone.detail and "nothing was written" in gone.detail
        # The refusal sends the run back to read the gates, not on to the commit.
        refused_at = fleet.calls.index(f"ack:{ADMIN_GATE}")
        assert fleet.calls.index("read_admin_acks", refused_at) < fleet.calls.index("commit:control-plane::4.20.44")

    async def test_a_client_still_calling_a_removed_api_waits_for_a_person(self):
        fleet = Fleet()
        fleet.api_usage = [KYVERNO]
        async with Harness(fleet) as h:
            await h.start(request(nodepool_mode=NodePoolMode.AUTO))
            await h.until(UpgradePhase.AWAITING_APPROVAL)
            await h.decide(Run.approve, ALICE)
            await h.until(UpgradePhase.AWAITING_DAY2_CONFIRM)
            await h.decide(Run.confirm_day2, DAY2)
            waiting = await h.until(UpgradePhase.AWAITING_ADMIN_ACK)
            assert "kyverno-admission-controller (kyverno/v1.10.0), 12 requests (get, update)" in waiting.waiting_for
            assert fleet.acks_given == [] and "control-plane::4.20.44" not in fleet.commit_summary()
            # An approval answers for those callers, so it carries a reason.
            refusal = await _refused(h, Run.approve_admin_ack, HumanDecision(by="carol"))
            assert "needs a reason" in refusal
            await h.decide(Run.approve_admin_ack, HumanDecision(by="carol", reason="kyverno 1.10 is gone next week"))
            await h.until(UpgradePhase.AWAITING_DAY2_CONFIRM)
            await h.decide(Run.confirm_day2, DAY2)
            result = await h.result()

        assert result.outcome is UpgradeOutcome.DONE
        assert fleet.acks_given == [ADMIN_GATE]
        (logged,) = [e for e in result.log if e.event == "admin-ack"]
        assert logged.by == "carol" and "approved with 1 callers" in logged.detail

    async def test_retry_checks_the_callers_again(self):
        fleet = Fleet()
        fleet.api_usage = [KYVERNO]
        async with Harness(fleet) as h:
            await h.start(request(nodepool_mode=NodePoolMode.AUTO))
            await h.until(UpgradePhase.AWAITING_APPROVAL)
            await h.decide(Run.approve, ALICE)
            await h.until(UpgradePhase.AWAITING_DAY2_CONFIRM)
            await h.decide(Run.confirm_day2, DAY2)
            await h.until(UpgradePhase.AWAITING_ADMIN_ACK)
            fleet.api_usage = []  # the client moved to admissionregistration.k8s.io/v1
            await h.decide(Run.retry, ALICE)
            await h.until(UpgradePhase.AWAITING_DAY2_CONFIRM)
            await h.decide(Run.confirm_day2, DAY2)
            result = await h.result()
        (logged,) = [e for e in result.log if e.event == "admin-ack"]
        assert logged.by == "workflow"

    async def test_callers_that_walk_every_api_do_not_count(self):
        fleet = Fleet()
        fleet.api_usage = [ApiUsage(api=KYVERNO.api, username="system:kube-controller-manager",
                                    verbs=["list", "watch"], requests=300)]
        async with Harness(fleet) as h:
            await h.start(request(nodepool_mode=NodePoolMode.AUTO))
            await _approve_through_control_plane(h)
            result = await h.result()
        assert result.outcome is UpgradeOutcome.DONE and fleet.acks_given == [ADMIN_GATE]

    async def test_a_gate_the_rules_do_not_list_is_acked_by_a_person_never_by_the_run(self):
        fleet = Fleet()
        fleet.admin_gates["ack-4.19-kube-1.33-api-removals-in-4.20"] = "Kubernetes 1.33 removes an API."
        async with Harness(fleet) as h:
            await h.start(request(nodepool_mode=NodePoolMode.AUTO))
            await h.until(UpgradePhase.AWAITING_APPROVAL)
            await h.decide(Run.approve, ALICE)
            await h.until(UpgradePhase.AWAITING_DAY2_CONFIRM)
            await h.decide(Run.confirm_day2, DAY2)
            waiting = await h.until(UpgradePhase.NEEDS_HUMAN)
            assert "ack-4.19-kube-1.33-api-removals-in-4.20: Kubernetes 1.33 removes an API." in waiting.waiting_for
            assert fleet.acks_given == []
            fleet.admin_acks["ack-4.19-kube-1.33-api-removals-in-4.20"] = "true"  # a person, by hand
            await h.decide(Run.retry, ALICE)
            await h.until(UpgradePhase.AWAITING_DAY2_CONFIRM)
            await h.decide(Run.confirm_day2, DAY2)
            result = await h.result()
        assert result.outcome is UpgradeOutcome.DONE
        assert fleet.acks_given == [ADMIN_GATE]

    async def _until_the_cvo_lags(self, h: Harness) -> None:
        await h.until(UpgradePhase.AWAITING_APPROVAL)
        await h.decide(Run.approve, ALICE)
        await h.until(UpgradePhase.AWAITING_DAY2_CONFIRM)
        await h.decide(Run.confirm_day2, DAY2)
        while not ((await h.progress()).waiting_for or "").startswith("the CVO to take the admin acks"):
            await asyncio.sleep(0.02)

    async def test_the_commit_waits_for_the_cvo_to_take_the_ack(self):
        fleet = Fleet()
        fleet.cvo_ack_lag = 3
        async with Harness(fleet) as h:
            await h.start(request(nodepool_mode=NodePoolMode.AUTO))
            await self._until_the_cvo_lags(h)
            assert "control-plane::4.20.44" not in fleet.commit_summary()
            await h.env.sleep(timedelta(minutes=5))
            await h.until(UpgradePhase.AWAITING_DAY2_CONFIRM)
            await h.decide(Run.confirm_day2, DAY2)
            result = await h.result()
        assert result.outcome is UpgradeOutcome.DONE
        # Before 4.19: one read, nothing pending. Before 4.20: pending, then
        # three reads where the CVO still says AdminAckRequired, then clear.
        assert fleet.calls.count("read_admin_acks") == 1 + 5

    async def test_a_cvo_that_never_takes_the_ack_is_a_person_s_call(self):
        fleet = Fleet()
        fleet.cvo_ack_lag = 100
        async with Harness(fleet) as h:
            await h.start(request(nodepool_mode=NodePoolMode.AUTO))
            await self._until_the_cvo_lags(h)
            await h.env.sleep(timedelta(minutes=15))
            waiting = await h.until(UpgradePhase.NEEDS_HUMAN)
            assert "yet the CVO still says AdminAckRequired" in waiting.waiting_for
            assert "control-plane::4.20.44" not in fleet.commit_summary()
            await h.decide(Run.abort, HumanDecision(by="alice", reason="end of test"))
            await h.result()

    async def test_a_nodepool_roll_is_not_held_by_a_missing_ack(self):
        # Workers two minors behind a 4.19 control plane roll before it may go
        # to 4.20 (D4) — with the gate still un-acked, as HyperShift allows
        # (CONTEXT §8.13). The ack comes after, right before the 4.20 commit.
        fleet = Fleet(control_plane="4.19.53", nodepools={"np-a": "4.17.58"})
        async with Harness(fleet) as h:
            await h.start(request(nodepool_mode=NodePoolMode.AUTO))
            await h.until(UpgradePhase.AWAITING_APPROVAL)
            await h.decide(Run.approve, ALICE)
            await h.until(UpgradePhase.AWAITING_DAY2_CONFIRM)
            await h.decide(Run.confirm_day2, DAY2)
            result = await h.result()
        assert result.outcome is UpgradeOutcome.DONE
        assert not any(e.event == "needs-human" for e in result.log)
        roll = next(c for c in fleet.calls if c.startswith("commit:nodepool:np-a:"))
        assert fleet.calls.index(roll) < fleet.calls.index(f"ack:{ADMIN_GATE}")
        assert fleet.calls.index(f"ack:{ADMIN_GATE}") < fleet.calls.index("commit:control-plane::4.20.44")

    async def test_approve_admin_ack_is_refused_unless_the_run_waits_for_it(self):
        fleet = Fleet()
        async with Harness(fleet) as h:
            await h.start(request())
            await h.until(UpgradePhase.AWAITING_APPROVAL)
            refusal = await _refused(h, Run.approve_admin_ack, HumanDecision(by="carol", reason="r"))
            assert "approve-admin-ack is not waited for" in refusal
            await h.decide(Run.abort, HumanDecision(by="alice", reason="end of test"))
            await h.result()


class TestTheMirrorAndGit:
    async def test_a_digest_that_moves_after_approval_stops_the_step_before_any_commit(self):
        fleet = Fleet()
        fleet.pin_script = [{}, {"4.19.53": digest("4.19.53", "-moved")}]
        async with Harness(fleet) as h:
            await h.start(request())
            await h.until(UpgradePhase.AWAITING_APPROVAL)
            await h.decide(Run.approve, ALICE)
            waiting = await h.until(UpgradePhase.NEEDS_HUMAN)
            assert "the mirror changed under the run" in waiting.waiting_for
            assert "sha256:release-4.19.53-moved" in waiting.waiting_for
            assert fleet.commits == []
            await h.decide(Run.abort, HumanDecision(by="alice", reason="re-plan"))
            result = await h.result()
        assert result.outcome is UpgradeOutcome.ABORTED
        assert fleet.commit_summary() == ["pause"]

    async def test_someone_else_s_edit_waits_for_a_person_and_retry_carries_on_from_their_file(self):
        fleet = Fleet()
        async with Harness(fleet) as h:
            await h.start(request(workers_to_target=False))
            await h.until(UpgradePhase.AWAITING_APPROVAL)
            fleet.edit_by_hand()
            await h.decide(Run.approve, ALICE)
            waiting = await h.until(UpgradePhase.NEEDS_HUMAN)
            assert "someone else changed" in waiting.waiting_for
            assert fleet.commits == []
            await h.decide(Run.retry, ALICE)
            await h.until(UpgradePhase.AWAITING_DAY2_CONFIRM)
            await h.decide(Run.abort, HumanDecision(by="alice", reason="end of test"))
            await h.result()
        # Planning read the file, the retry read it again, and the commit
        # went in on top of the person's edit, never over it.
        assert fleet.calls.count("read_day1_cluster") == 2
        assert fleet.commits[0].expected_blob == "blob-2"
        assert fleet.commit_summary()[0] == "control-plane::4.19.53"

    async def test_a_refused_sync_after_the_commit_retries_only_the_sync(self):
        fleet = Fleet(control_plane="4.20.44", nodepools={"np-a": "4.19.53"})
        fleet.sync_errors = [fleet.sync_refused()]
        async with Harness(fleet) as h:
            await h.start(request(nodepool_mode=NodePoolMode.AUTO))
            await h.until(UpgradePhase.AWAITING_APPROVAL)
            await h.decide(Run.approve, ALICE)
            waiting = await h.until(UpgradePhase.NEEDS_HUMAN)
            assert "ConfigMap/extra is out of sync" in waiting.waiting_for
            preflights = fleet.calls.count("read_argo_application")
            await h.decide(Run.retry, ALICE)
            result = await h.result()
        assert result.outcome is UpgradeOutcome.DONE
        assert fleet.commit_summary() == ["nodepool:np-a:4.20.44", "finish"]
        # No second pre-flight: the run's own commit is pending in Argo CD and
        # would fail it; only the sync is repeated.
        assert fleet.calls.count("read_argo_application") == preflights
        assert [c.change for c in fleet.commits].count(Day1ChangeKind.NODEPOOL) == 1


class TestTheControlPlaneSlots:
    async def test_a_run_waits_while_every_slot_of_its_mce_is_taken(self):
        fleet = Fleet(max_concurrent_control_planes=1)
        async with Harness(fleet) as h:
            (blocker,) = await h.block_slots(1)
            await h.start(request(workers_to_target=False))
            await h.until(UpgradePhase.AWAITING_APPROVAL)
            await h.decide(Run.approve, ALICE)
            waiting = await h.until(UpgradePhase.WAITING_FOR_SLOT)
            assert f"a control-plane slot on {MCE}" in waiting.waiting_for
            assert fleet.commits == []
            await blocker.terminate()
            await h.env.sleep(timedelta(minutes=3))
            started = await h.until(UpgradePhase.AWAITING_DAY2_CONFIRM)
            await h.decide(Run.abort, HumanDecision(by="alice", reason="end of test"))
            await h.result()
        assert any(upgrade_control_plane_workflow_id(MCE, 1) in e.detail for e in started.log)

    async def test_a_free_second_slot_is_taken_at_once(self):
        fleet = Fleet(max_concurrent_control_planes=2)
        async with Harness(fleet) as h:
            await h.block_slots(1)
            await h.start(request(workers_to_target=False))
            await h.until(UpgradePhase.AWAITING_APPROVAL)
            await h.decide(Run.approve, ALICE)
            started = await h.until(UpgradePhase.AWAITING_DAY2_CONFIRM)
            await h.decide(Run.abort, HumanDecision(by="alice", reason="end of test"))
            await h.result()
        assert any(upgrade_control_plane_workflow_id(MCE, 2) in e.detail for e in started.log)


def test_every_interface_is_mocked_on_its_own_queue():
    """The harness's premise: each limb's whole interface, and nothing else, on that limb's queue."""
    def names(module) -> set[str]:
        return {
            defn.name
            for value in vars(module).values()
            if (defn := getattr(value, "__temporal_activity_definition", None)) is not None
        }

    mocked = {
        queue: {fn.__temporal_activity_definition.name for fn in fns}
        for queue, fns in Fleet().mocks().items()
    }
    assert mocked == {
        HUB_QUEUE: names(gitops_interface),
        MCE_QUEUE: names(cluster_interface),
        AGENT_QUEUE: names(agent_interface),
    }
