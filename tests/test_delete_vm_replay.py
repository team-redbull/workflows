"""Replay tests for delete-vm: what a change to the workflow would do to runs IN FLIGHT.

  1. Every scenario of tests/test_delete_vm_workflow.py (`SCENARIOS`) is run on the fake
     fleet with the PRODUCTION constants, and its history replays at once.
  2. GOLDEN histories in tests/histories/delete_vm/, recorded by earlier code, replay on
     the current code.

A golden is written only with UPDATE_REPLAY_HISTORIES=1 and only when its file does not
exist: a golden that fails to replay means the change needs `workflow.patched()`, never a
re-record (CLAUDE.md §5).
"""

from __future__ import annotations

import json
import os
import pathlib

import pytest
from temporalio.client import WorkflowHistory
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Replayer

from tests.test_add_server_to_mce_replay import _scrubbed
from tests.test_delete_vm_workflow import SCENARIOS, record
from workflow_domains.vm_lifecycle.delete_vm import DeleteVmWorkflow

GOLDEN = pathlib.Path(__file__).parent / "histories" / "delete_vm"
_RECORD = os.environ.get("UPDATE_REPLAY_HISTORIES") == "1"


def _replayer() -> Replayer:
    return Replayer(workflows=[DeleteVmWorkflow], data_converter=pydantic_data_converter)


@pytest.mark.parametrize("scenario", sorted(SCENARIOS))
async def test_every_scenario_records_and_replays(scenario):
    history = await record(scenario)
    await _replayer().replay_workflow(history)
    golden = GOLDEN / f"{scenario}.json"
    if _RECORD and not golden.exists():
        GOLDEN.mkdir(parents=True, exist_ok=True)
        golden.write_text(
            json.dumps(
                {"workflow_id": history.workflow_id, "history": _scrubbed(json.loads(history.to_json()))},
                separators=(",", ":"), sort_keys=True,
            )
            + "\n"
        )


def test_there_are_golden_histories():
    """No committed history protects nothing in flight; every scenario has its golden."""
    assert {path.stem for path in GOLDEN.glob("*.json")} >= set(SCENARIOS)


@pytest.mark.parametrize("path", sorted(GOLDEN.glob("*.json")), ids=lambda p: p.stem)
async def test_golden_histories_replay_on_the_current_code(path):
    recorded = json.loads(path.read_text())
    history = WorkflowHistory.from_json(recorded["workflow_id"], recorded["history"])
    await _replayer().replay_workflow(history)
