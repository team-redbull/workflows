"""Replay: every history the cluster-upgrade workflows record must replay on the current code.

An upgrade run lives for weeks, through deploys, so a code change that alters
the commands a recorded run issued — an activity added before an existing one,
a timer removed, a branch reordered — fails that run on its next replay. Temporal
calls this a non-determinism error, and the only cure is `workflow.patched()`.
Two sources of histories catch it:

  1. histories recorded HERE, from three scenarios on the fake fleet, replayed at
     once: they catch non-determinism inside the code (a clock, a random, a set's
     iteration order);
  2. GOLDEN histories in tests/histories/cluster_upgrade/, recorded by earlier
     code and committed: they catch a change that would break a run in flight.
     When this fails, the change needs `workflow.patched()` — do NOT re-record.
     Re-record only for a change made before any real run exists, or once every
     run recorded with the old shape has finished:

         UPDATE_REPLAY_HISTORIES=1 .venv/bin/pytest tests/test_upgrade_cluster_replay.py

Both workflow types replay: the parent's runs, following continue-as-new, and
every control-plane step child they started.
"""

from __future__ import annotations

import asyncio
import json
import os
import pathlib
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from temporalio.client import WorkflowFailureError, WorkflowHistory
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Replayer

from shared.exceptions import AmbiguousClusterFileError, ClusterFileNotFoundError, Day1ChangeRejectedError
from shared.models.cluster_upgrade import (
    ActionDecision,
    ApiUsage,
    CheckFailure,
    CheckResult,
    ClusterUpgradeSettings,
    ContinueNodePool,
    GateOutcome,
    GateResult,
    HostedHealth,
    HumanDecision,
    NodePoolMode,
    ProposedAction,
    UpgradeClusterProgress,
    UpgradePhase,
    WatchResult,
)
from tests.cluster_upgrade_fakes import CLUSTER, MIRRORS, SEVEN_DAYS, Fleet, Harness, request, verdict
from tests.test_add_server_to_mce_replay import _SCRUBBED, _scrubbed
from workflow_domains.cluster_upgrade.upgrade_cluster import UpgradeClusterWorkflow as Run
from workflow_domains.cluster_upgrade.upgrade_control_plane import UpgradeControlPlaneWorkflow

GOLDEN = pathlib.Path(__file__).parent / "histories" / "cluster_upgrade"
_RECORD = os.environ.get("UPDATE_REPLAY_HISTORIES") == "1"

ALICE = HumanDecision(by="alice")


async def _happy_path(h: Harness) -> None:
    await h.start(request())
    await h.until(UpgradePhase.AWAITING_APPROVAL)
    await h.decide(Run.approve, ALICE)
    for _ in range(2):
        await h.until(UpgradePhase.AWAITING_DAY2_CONFIRM)
        await h.decide(Run.confirm_day2, HumanDecision(by="day2-team"))
    for _ in range(2):
        await h.until(UpgradePhase.AWAITING_CONTINUE)
        await h.env.sleep(SEVEN_DAYS)
        await h.decide(Run.continue_nodepool, ContinueNodePool(by="alice"))
    await h.result()


async def _abort_mid_roll(h: Harness) -> None:
    h.fleet.block_nodepool_watch = True
    await h.start(request(workers_to_target=True))
    await h.until(UpgradePhase.AWAITING_APPROVAL)
    await h.decide(Run.approve, ALICE)
    for _ in range(2):
        await h.until(UpgradePhase.AWAITING_DAY2_CONFIRM)
        await h.decide(Run.confirm_day2, HumanDecision(by="day2-team"))
    await h.until(UpgradePhase.AWAITING_CONTINUE)
    await h.decide(Run.pause, HumanDecision(by="alice", reason="freeze"))
    await h.decide(Run.continue_nodepool, ContinueNodePool(by="alice"))
    await h.until(UpgradePhase.PAUSED)
    await h.decide(Run.resume, ALICE)
    await h.until(UpgradePhase.NODEPOOL_ROLL)
    await h.decide(Run.abort, HumanDecision(by="alice", reason="customer escalation"))
    await h.result()


async def _agent_then_person(h: Harness) -> None:
    h.fleet.np_watch_script = [WatchResult.STALLED, WatchResult.STALLED]
    h.fleet.verdicts = [verdict("retry_gate")]
    await h.start(request(nodepool_mode=NodePoolMode.AUTO))
    await h.until(UpgradePhase.AWAITING_APPROVAL)
    await h.decide(Run.approve, ALICE)
    await h.until(UpgradePhase.NEEDS_HUMAN)
    await h.decide(Run.retry, ALICE)
    await h.result()


async def _control_plane_stall(h: Harness) -> None:
    h.fleet.cp_watch_script = [WatchResult.DEADLINE]
    h.fleet.verdicts = [verdict("retry_gate")]
    await h.start(request(workers_to_target=False))
    await h.until(UpgradePhase.AWAITING_APPROVAL)
    await h.decide(Run.approve, ALICE)
    await h.until(UpgradePhase.NEEDS_HUMAN)  # the child waits, holding its slot
    await h.decide(Run.pause, HumanDecision(by="alice", reason="look first"))
    await h.decide(Run.retry, ALICE)
    await h.until(UpgradePhase.AWAITING_DAY2_CONFIRM)
    await h.decide(Run.abort, HumanDecision(by="alice", reason="end of scenario"))
    await h.result()


async def _admin_ack_in_use(h: Harness) -> None:
    # A client still calls a removed API: the run waits for a person, who
    # retries once, then approves (D16).
    h.fleet.api_usage = [ApiUsage(
        api="validatingwebhookconfigurations.v1beta1.admissionregistration.k8s.io",
        username="system:serviceaccount:kyverno:kyverno", verbs=["get"], requests=4,
    )]
    await h.start(request(nodepool_mode=NodePoolMode.AUTO, workers_to_target=False))
    await h.until(UpgradePhase.AWAITING_APPROVAL)
    await h.decide(Run.approve, ALICE)
    await h.until(UpgradePhase.AWAITING_DAY2_CONFIRM)
    await h.decide(Run.confirm_day2, HumanDecision(by="day2-team"))
    await h.until(UpgradePhase.AWAITING_ADMIN_ACK)
    await h.decide(Run.retry, ALICE)
    await h.until(UpgradePhase.AWAITING_ADMIN_ACK)
    await h.decide(Run.approve_admin_ack, HumanDecision(by="alice", reason="kyverno is being replaced"))
    await h.until(UpgradePhase.AWAITING_DAY2_CONFIRM)
    await h.decide(Run.confirm_day2, HumanDecision(by="day2-team"))
    await h.result()


async def _image_pruner_degraded(h: Harness) -> None:
    # Pre-flight finds image-registry Degraded by a failed pruner Job; the agent proposes
    # deleting it, a person approves, retries, and the run goes on (D21).
    h.fleet.health_script = [HostedHealth(
        reachable=True, cluster_version_upgradeable=True, idms_mirrors=MIRRORS,
        operators_unhealthy=["image-registry (Degraded=True: ImagePrunerJobFailed)"],
    )]
    h.fleet.verdicts = [verdict("needs_human", diagnosis="a failed image-pruner Job", actions_proposed=[ProposedAction(
        action_id="act-pruner", kind="delete_failed_job", target="openshift-image-registry/image-pruner-29365920",
    )])]
    await h.start(request(workers_to_target=False))
    await h.until(UpgradePhase.NEEDS_HUMAN)
    await h.decide(Run.approve_action, ActionDecision(by="alice", action_id="act-pruner"))
    await h.decide(Run.retry, ALICE)
    await h.until(UpgradePhase.AWAITING_APPROVAL)
    await h.decide(Run.approve, ALICE)
    for _ in range(2):
        await h.until(UpgradePhase.AWAITING_DAY2_CONFIRM)
        await h.decide(Run.confirm_day2, HumanDecision(by="day2-team"))
    await h.result()


# --- recorded on the code BEFORE a fix, so a run of the old shape stays replayable -------
#
# Each driver works on both shapes: where the old code waited twice, failed or
# ran on, the new one waits once, waits for a person or holds. `next_stop`
# returns None once the run has closed, whatever it closed with.

_REFUSED = "the validator refused the edit: workertag must be pinned"


async def _left(h: Harness, stop: UpgradeClusterProgress) -> None:
    """Wait until the run has left the wait a decision was just sent to, or has closed."""
    while (now := await h.next_stop(*UpgradePhase)) is not None and (now.phase, now.waiting_for) == (
        stop.phase, stop.waiting_for
    ):
        await asyncio.sleep(0.02)


async def _retry_until_closed(h: Harness, *, fix_refusals: bool = False, end_at_day2: bool = False) -> None:
    """Retry every needs-human; confirm day2 (or abort there); until the run closes, failed or not."""
    while (stop := await h.next_stop(UpgradePhase.NEEDS_HUMAN, UpgradePhase.AWAITING_DAY2_CONFIRM)) is not None:
        if stop.phase is UpgradePhase.AWAITING_DAY2_CONFIRM:
            if end_at_day2:
                await h.decide(Run.abort, HumanDecision(by="alice", reason="end of scenario"))
            else:
                await h.decide(Run.confirm_day2, HumanDecision(by="day2-team"))
        else:
            if fix_refusals and _REFUSED in (stop.waiting_for or ""):
                h.fleet.edit_by_hand()  # a person fixes the file the validator refused
            await h.decide(Run.retry, ALICE)
        await _left(h, stop)
    try:
        await h.result()
    except WorkflowFailureError:
        pass  # the old shape of some of these failed the run


async def _day1_rejected(h: Harness) -> None:
    # The validator refuses the control plane's commit, then the NodePool's; a
    # person fixes the file each time and retries.
    h.fleet.commit_errors = [Day1ChangeRejectedError(_REFUSED)]
    await h.start(request(nodepool_mode=NodePoolMode.AUTO))
    await h.until(UpgradePhase.AWAITING_APPROVAL)
    await h.decide(Run.approve, ALICE)
    while (stop := await h.next_stop(UpgradePhase.NEEDS_HUMAN, UpgradePhase.AWAITING_DAY2_CONFIRM)) is not None:
        if stop.phase is UpgradePhase.AWAITING_DAY2_CONFIRM:
            break
        if _REFUSED in (stop.waiting_for or ""):
            h.fleet.edit_by_hand()
        await h.decide(Run.retry, ALICE)
        await _left(h, stop)
    h.fleet.commit_errors = [Day1ChangeRejectedError(_REFUSED)]  # the NodePool's commit, next
    await _retry_until_closed(h, fix_refusals=True)


async def _cluster_file_gone_mid_roll(h: Harness) -> None:
    await h.start(request(nodepool_mode=NodePoolMode.AUTO))
    await h.until(UpgradePhase.AWAITING_APPROVAL)
    h.fleet.commit_errors = [ClusterFileNotFoundError(f"no {CLUSTER}.yaml under the clusters root")]
    await h.decide(Run.approve, ALICE)
    await _retry_until_closed(h)


async def _cluster_file_gone_on_abort(h: Harness) -> None:
    await h.start(request())
    await h.until(UpgradePhase.AWAITING_APPROVAL)
    await h.decide(Run.approve, ALICE)
    await h.until(UpgradePhase.AWAITING_DAY2_CONFIRM)
    h.fleet.commit_errors = [AmbiguousClusterFileError(f"two {CLUSTER}.yaml files under the clusters root")]
    await h.decide(Run.abort, HumanDecision(by="alice", reason="customer escalation"))
    await _retry_until_closed(h)


async def _cluster_file_gone_on_reread(h: Harness) -> None:
    # Someone else's edit sends the run to re-read the file, which is gone by then.
    await h.start(request(workers_to_target=False))
    await h.until(UpgradePhase.AWAITING_APPROVAL)
    h.fleet.edit_by_hand()
    await h.decide(Run.approve, ALICE)
    await h.until(UpgradePhase.NEEDS_HUMAN)
    h.fleet.day1_read_errors = [ClusterFileNotFoundError(f"no {CLUSTER}.yaml under the clusters root")]
    await _retry_until_closed(h, end_at_day2=True)


async def _action_while_paused(h: Harness) -> None:
    # A person pauses a run waiting on an agent proposal, approves the proposal, then resumes.
    h.fleet.gate_script = [GateOutcome(result=GateResult.DEADLINE, failing_checks=[CheckResult(
        name="pdbs-back", passed=False, expected="disruptionsAllowed >= 1", observed="db/pdb at 0",
        on_failure=CheckFailure.NEEDS_HUMAN,
    )])]
    h.fleet.verdicts = [verdict("needs_human", actions_proposed=[ProposedAction(
        action_id="act-1", kind="delete_pod", target="openshift-monitoring/prom-0", rationale="stuck terminating",
    )])]
    await h.start(request(nodepool_mode=NodePoolMode.AUTO))
    await h.until(UpgradePhase.AWAITING_APPROVAL)
    await h.decide(Run.approve, ALICE)
    await h.until(UpgradePhase.NEEDS_HUMAN)
    await h.decide(Run.pause, HumanDecision(by="alice", reason="look first"))
    await h.decide(Run.approve_action, ActionDecision(by="alice", action_id="act-1"))
    await asyncio.sleep(0.3)
    await h.decide(Run.resume, ALICE)
    await h.called("action:act-1")
    await h.until(UpgradePhase.NEEDS_HUMAN)
    await h.decide(Run.retry, ALICE)
    await h.result()


async def _abort_during_agent(h: Harness) -> None:
    h.fleet.np_watch_script = [WatchResult.STALLED]
    h.fleet.block_agent = True
    await h.start(request(nodepool_mode=NodePoolMode.AUTO))
    await h.until(UpgradePhase.AWAITING_APPROVAL)
    await h.decide(Run.approve, ALICE)
    await h.called("agent:watch-nodepool-stalled")
    await h.decide(Run.abort, HumanDecision(by="alice", reason="change of plan"))
    await h.result()


async def _abort_during_incident(h: Harness) -> None:
    h.fleet.np_watch_script = [WatchResult.STALLED]
    h.fleet.block_incident = True
    await h.start(request(nodepool_mode=NodePoolMode.AUTO))
    await h.until(UpgradePhase.AWAITING_APPROVAL)
    await h.decide(Run.approve, ALICE)
    await h.called("write_incident")
    await h.decide(Run.abort, HumanDecision(by="alice", reason="change of plan"))
    await asyncio.sleep(0.3)
    h.fleet.block_incident = False  # Brain answers at last
    await h.result()


_ONE_NODEPOOL_BEHIND = {"control_plane": "4.20.44", "nodepools": {"np-a": "4.19.53"}}


SCENARIOS: dict[str, tuple[Callable[[], Fleet], Callable[[Harness], Awaitable[None]]]] = {
    "happy-path": (Fleet, _happy_path),
    "abort-mid-roll": (Fleet, _abort_mid_roll),
    "control-plane-stall": (Fleet, _control_plane_stall),
    "admin-ack-in-use": (Fleet, _admin_ack_in_use),
    "image-pruner-degraded": (lambda: Fleet(upgrade=ClusterUpgradeSettings(wave=2)), _image_pruner_degraded),
    "agent-then-person": (
        lambda: Fleet(control_plane="4.20.44", nodepools={"np-a": "4.19.53"}),
        _agent_then_person,
    ),
    "day1-rejected": (lambda: Fleet(control_plane="4.19.53", nodepools={"np-a": "4.19.53"}), _day1_rejected),
    "cluster-file-gone-mid-roll": (lambda: Fleet(**_ONE_NODEPOOL_BEHIND), _cluster_file_gone_mid_roll),
    "cluster-file-gone-on-abort": (Fleet, _cluster_file_gone_on_abort),
    "cluster-file-gone-on-reread": (Fleet, _cluster_file_gone_on_reread),
    "action-while-paused": (lambda: Fleet(**_ONE_NODEPOOL_BEHIND), _action_while_paused),
    "abort-during-agent": (lambda: Fleet(**_ONE_NODEPOOL_BEHIND), _abort_during_agent),
    "abort-during-incident": (lambda: Fleet(**_ONE_NODEPOOL_BEHIND), _abort_during_incident),
}


def _replayer() -> Replayer:
    return Replayer(
        workflows=[Run, UpgradeControlPlaneWorkflow],
        data_converter=pydantic_data_converter,
    )


async def _children(h: Harness, parent: WorkflowHistory) -> list[WorkflowHistory]:
    """The histories of every control-plane step child a parent run started."""
    children = []
    for event in parent.events:
        if event.HasField("child_workflow_execution_started_event_attributes"):
            execution = event.child_workflow_execution_started_event_attributes.workflow_execution
            handle = h.client.get_workflow_handle(execution.workflow_id, run_id=execution.run_id)
            children.append(await handle.fetch_history())
    return children


async def _record(scenario: str) -> list[tuple[str, WorkflowHistory]]:
    make_fleet, drive = SCENARIOS[scenario]
    async with Harness(make_fleet()) as h:
        await drive(h)
        named = []
        for index, run in enumerate(await h.histories(), start=1):
            named.append((f"{index:02d}-upgrade-cluster", run))
            for child_index, child in enumerate(await _children(h, run), start=1):
                named.append((f"{index:02d}-{child_index}-upgrade-control-plane", child))
    return named


@pytest.mark.parametrize("scenario", sorted(SCENARIOS))
async def test_every_run_a_scenario_records_replays(scenario):
    named = await _record(scenario)
    assert named
    for name, history in named:
        await _replayer().replay_workflow(history)
    if _RECORD:
        folder = GOLDEN / scenario
        folder.mkdir(parents=True, exist_ok=True)
        for stale in folder.glob("*.json"):
            stale.unlink()
        for name, history in named:
            (folder / f"{name}.json").write_text(
                json.dumps(
                    {"workflow_id": history.workflow_id, "history": _scrubbed(json.loads(history.to_json()))},
                    separators=(",", ":"), sort_keys=True,
                )
                + "\n"
            )


# The placeholder every worker identity is scrubbed to — add-server-to-mce's
# scrubber, which every replay test shares — and the one this file's own copy
# of it wrote into the histories recorded before.
_PLACEHOLDER_IDENTITIES = {_SCRUBBED["identity"], "upgrade-cluster-replay"}


def _golden_files() -> list[pathlib.Path]:
    return sorted(GOLDEN.glob("*/*.json"))


def test_every_scenario_has_golden_histories():
    """A scenario without committed histories protects nothing in flight."""
    assert {path.parent.name for path in _golden_files()} == set(SCENARIOS)


@pytest.mark.parametrize("path", _golden_files(), ids=lambda p: f"{p.parent.name}/{p.stem}")
async def test_golden_histories_replay_on_the_current_code(path):
    recorded = json.loads(path.read_text())
    history = WorkflowHistory.from_json(recorded["workflow_id"], recorded["history"])
    await _replayer().replay_workflow(history)


def test_no_golden_history_names_the_machine_that_recorded_it():
    """This repo is public: a worker identity is `<pid>@<hostname>`."""
    for path in _golden_files():
        for value in _identities(json.loads(path.read_text())):
            assert value in _PLACEHOLDER_IDENTITIES, f"{path}: identity {value!r}"


def _identities(node: Any):
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "identity":
                yield value
            else:
                yield from _identities(value)
    elif isinstance(node, list):
        for item in node:
            yield from _identities(item)

