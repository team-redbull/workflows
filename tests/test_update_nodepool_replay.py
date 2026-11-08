"""Replay: every history update-nodepool and its two children record must replay on the current code.

A scale-down that marks servers, or a sync waiting on Argo CD, can be in
flight when a brain is deployed, so a change to the commands a recorded run
issued fails that run on its next replay with a non-determinism error. The
cure is `workflow.patched()`. As for the other server-lifecycle workflows
(tests/test_remove_server_from_mce_replay.py), two sources catch it:

  1. histories recorded HERE, from scenarios on the fake fleet of
     tests/test_update_nodepool_workflow.py — the parent's and its child's —
     replayed at once;
  2. GOLDEN histories under tests/histories/{update_nodepool,scale_up_nodepool,
     scale_down_nodepool}/, recorded by earlier code and committed. When one
     fails, the change needs `workflow.patched()` — do NOT re-record.
     Recording writes ONLY the scenarios with no golden file yet:

         UPDATE_REPLAY_HISTORIES=1 .venv/bin/pytest tests/test_update_nodepool_replay.py
"""

from __future__ import annotations

import asyncio
import json
import os
import pathlib
from collections.abc import Callable
from dataclasses import dataclass

import pytest
from temporalio.client import Client, WorkflowFailureError, WorkflowHistory
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Replayer

from shared.consts import UPDATE_NODEPOOL_WORKFLOW_QUEUE
from shared.exceptions import ArgoSyncRefusedError, Day1FileChangedError
from shared.workflow_ids import scale_down_nodepool_workflow_id, scale_up_nodepool_workflow_id
from tests.test_add_server_to_mce_replay import _scrubbed
from tests.test_update_nodepool_workflow import CLUSTER, _Fleet, _Harness, _policy_fleet, run_args
from workflow_domains.server_lifecycle.scale_down_nodepool import ScaleDownNodePoolWorkflow
from workflow_domains.server_lifecycle.scale_up_nodepool import ScaleUpNodePoolWorkflow
from workflow_domains.server_lifecycle.update_nodepool import UpdateNodePoolWorkflow

HISTORIES = pathlib.Path(__file__).parent / "histories"
_DIRECTORIES = {"update": "update_nodepool", "up": "scale_up_nodepool", "down": "scale_down_nodepool"}
_RECORD = os.environ.get("UPDATE_REPLAY_HISTORIES") == "1"


def _failing_commit() -> _Fleet:
    fleet = _policy_fleet()
    fleet.commit_error = Day1FileChangedError("someone else changed it")
    return fleet


def _refused_sync() -> _Fleet:
    fleet = _Fleet((3, 3, 2))
    fleet.sync_error = ArgoSyncRefusedError("someone else's change is pending")
    return fleet


@dataclass
class Scenario:
    fleet: Callable[[], _Fleet]
    replicas: int
    # Which child the run starts: "up", "down" or None.
    child: str | None
    fails: bool = False
    # One NodePool by name instead of the kind (since 2026-11-08).
    nodepool: str | None = None


SCENARIOS: dict[str, Scenario] = {
    "scale-up": Scenario(lambda: _Fleet((3, 3, 2)), 10, "up"),
    "scale-down-capi-picks": Scenario(lambda: _Fleet((3, 3, 2)), 6, "down"),
    "scale-down-by-policy": Scenario(_policy_fleet, 8, "down"),
    "already-the-total": Scenario(lambda: _Fleet((3, 3, 2)), 8, None),
    "resumes-a-committed-plan": Scenario(lambda: _Fleet((4, 3, 3), live=(3, 3, 2)), 10, "up"),
    "drift": Scenario(lambda: _Fleet((3, 3, 2), live=(4, 3, 2)), 10, None, fails=True),
    "commit-refused-marks-come-off": Scenario(_failing_commit, 8, "down", fails=True),
    "sync-refused": Scenario(_refused_sync, 10, "up", fails=True),
    "one-nodepool-up": Scenario(lambda: _Fleet((3, 3, 2)), 5, "up", nodepool="alpha-zone-c"),
    "one-nodepool-down-by-policy": Scenario(_policy_fleet, 1, "down", nodepool="alpha-zone-c"),
    "one-nodepool-not-found": Scenario(lambda: _Fleet((3, 3, 2)), 4, None, fails=True, nodepool="alpha-zone-x"),
}


def _replayer() -> Replayer:
    return Replayer(
        workflows=[UpdateNodePoolWorkflow, ScaleUpNodePoolWorkflow, ScaleDownNodePoolWorkflow],
        data_converter=pydantic_data_converter,
    )


async def _record(name: str) -> dict[str, WorkflowHistory]:
    scenario = SCENARIOS[name]
    async with _Harness(scenario.fleet()) as client:
        client: Client
        handle = await client.start_workflow(
            UpdateNodePoolWorkflow.run, run_args(scenario.replicas, nodepool=scenario.nodepool),
            id=f"update-nodepool-replay-{name}", task_queue=UPDATE_NODEPOOL_WORKFLOW_QUEUE,
        )
        if scenario.fails:
            with pytest.raises(WorkflowFailureError):
                await asyncio.wait_for(handle.result(), timeout=60)
        else:
            await asyncio.wait_for(handle.result(), timeout=60)
        histories = {"update": await handle.fetch_history()}
        if scenario.child is not None:
            child = (scale_up_nodepool_workflow_id if scenario.child == "up" else scale_down_nodepool_workflow_id)(CLUSTER)
            histories[scenario.child] = await client.get_workflow_handle(child).fetch_history()
        return histories


@pytest.mark.parametrize("scenario", sorted(SCENARIOS))
async def test_every_run_a_scenario_records_replays(scenario):
    for kind, history in (await _record(scenario)).items():
        await _replayer().replay_workflow(history)
        golden = HISTORIES / _DIRECTORIES[kind] / f"{scenario}.json"
        if _RECORD and not golden.exists():
            golden.parent.mkdir(parents=True, exist_ok=True)
            golden.write_text(
                json.dumps(
                    {"workflow_id": history.workflow_id, "history": _scrubbed(json.loads(history.to_json()))},
                    separators=(",", ":"),
                    sort_keys=True,
                )
                + "\n"
            )


def _golden_files() -> list[pathlib.Path]:
    return sorted(path for directory in _DIRECTORIES.values() for path in (HISTORIES / directory).glob("*.json"))


def test_there_are_golden_histories_for_all_three():
    """No committed history protects nothing in flight."""
    for directory in _DIRECTORIES.values():
        assert sorted((HISTORIES / directory).glob("*.json")), directory


@pytest.mark.parametrize("path", _golden_files(), ids=lambda p: f"{p.parent.name}/{p.stem}")
async def test_golden_histories_replay_on_the_current_code(path):
    recorded = json.loads(path.read_text())
    history = WorkflowHistory.from_json(recorded["workflow_id"], recorded["history"])
    await _replayer().replay_workflow(history)
