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

import json
import os
import pathlib
from collections.abc import Awaitable, Callable

import pytest
from temporalio.client import WorkflowHistory
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Replayer

from shared.models.cluster_upgrade import (
    ApiUsage,
    ContinueNodePool,
    HumanDecision,
    NodePoolMode,
    UpgradePhase,
    WatchResult,
)
from tests.cluster_upgrade_fakes import Fleet, Harness, request, verdict, SEVEN_DAYS
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


SCENARIOS: dict[str, tuple[Callable[[], Fleet], Callable[[Harness], Awaitable[None]]]] = {
    "happy-path": (Fleet, _happy_path),
    "abort-mid-roll": (Fleet, _abort_mid_roll),
    "control-plane-stall": (Fleet, _control_plane_stall),
    "admin-ack-in-use": (Fleet, _admin_ack_in_use),
    "agent-then-person": (
        lambda: Fleet(control_plane="4.20.44", nodepools={"np-a": "4.19.53"}),
        _agent_then_person,
    ),
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
                    {"workflow_id": history.workflow_id, "history": json.loads(history.to_json())},
                    separators=(",", ":"), sort_keys=True,
                )
                + "\n"
            )


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
