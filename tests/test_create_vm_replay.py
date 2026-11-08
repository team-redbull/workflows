"""create-vm's runs in flight are guarded by REPLAY: a workflow-code change that would make a
recorded history non-deterministic fails here, before it ships (CLAUDE.md §5).

The only cure for such a change is `workflow.patched()`. Two sources of histories:

  1. histories recorded HERE, from scenarios on the mock limb of
     tests/test_create_vm_workflow.py, replayed at once — a change that breaks
     determinism within one commit;
  2. GOLDEN histories in tests/histories/create_vm/, recorded by earlier
     versions of the code (the first is the lab's first real run on
     cluster-64spr, 2026-11-05, scrubbed of worker identities and stack traces) and
     replayed on the current code — the runs a deploy would find in flight.

A golden is written only with UPDATE_REPLAY_HISTORIES=1 and only when its
file does not exist: never re-recorded to make a change pass.
"""

from __future__ import annotations

import asyncio
import json
import os
import pathlib
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import pytest
from temporalio.client import WorkflowFailureError, WorkflowHistory
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Replayer

from shared.consts import CREATE_VM_WORKFLOW_QUEUE
from shared.exceptions import (
    KvAgentAuthError,
    NoKvClusterFitsError,
    VmIdentityConflictError,
    VmNameExhaustedError,
    VmRootDiskNotBoundError,
)
from shared.models.vm_lifecycle import CreateVmRunArgs, VmReadyOutcome
from tests.test_create_vm_workflow import INPUT, _error, _Harness, _placement, make_mocks
from workflow_domains.vm_lifecycle.create_vm import CreateVmWorkflow

GOLDEN = pathlib.Path(__file__).parent / "histories" / "create_vm"
_RECORD = os.environ.get("UPDATE_REPLAY_HISTORIES") == "1"


@dataclass
class Scenario:
    """One run: the mock limb's behaviour, and how it must end."""

    mocks: dict[str, Any] = field(default_factory=dict)
    # The ApplicationError type the run fails with; None = it completes.
    fails_with: str | None = None


SCENARIOS: dict[str, Callable[[], Scenario]] = {
    # List, rank, one PUT, the wait, done.
    "create": lambda: Scenario(),
    # Nothing fits: fails on the table, nothing created.
    "nothing-fits": lambda: Scenario(mocks={"placement": _placement(chosen=None)}, fails_with=NoKvClusterFitsError.__name__),
    # Every cluster refused the token: the rank activity's permanent error.
    "token-refused": lambda: Scenario(mocks={"rank_error": KvAgentAuthError("every cluster refused")}, fails_with=KvAgentAuthError.__name__),
    # A name born in between: a 409 on the first PUT, a redraw, then the create.
    "redraw": lambda: Scenario(mocks={"create_errors": [VmIdentityConflictError("taken")]}),
    # Three 409s: the draws are spent.
    "draws-exhausted": lambda: Scenario(
        mocks={"create_errors": [VmIdentityConflictError("taken")] * 3}, fails_with=VmNameExhaustedError.__name__
    ),
    # The disk never binds: the VM is deleted again, then the run fails.
    "rollback": lambda: Scenario(
        mocks={"ready": VmReadyOutcome(result="deadline", root_disk_phase="Pending", detail="root disk Pending")},
        fails_with=VmRootDiskNotBoundError.__name__,
    ),
}

_SCRUBBED = {"identity": "create-vm-replay", "stackTrace": ""}


def _scrubbed(node: Any) -> Any:
    """The history with every worker `identity` and failure `stackTrace` neutralised."""
    if isinstance(node, dict):
        return {key: (_SCRUBBED[key] if key in _SCRUBBED else _scrubbed(value)) for key, value in node.items()}
    if isinstance(node, list):
        return [_scrubbed(item) for item in node]
    return node


def _replayer() -> Replayer:
    return Replayer(workflows=[CreateVmWorkflow], data_converter=pydantic_data_converter)


async def _record(name: str) -> WorkflowHistory:
    scenario = SCENARIOS[name]()
    _, acts = make_mocks(**scenario.mocks)
    async with _Harness(acts) as client:
        handle = await client.start_workflow(
            CreateVmWorkflow.run, CreateVmRunArgs(input=INPUT), id=f"create-vm-replay-{name}",
            task_queue=CREATE_VM_WORKFLOW_QUEUE,
        )
        if scenario.fails_with is None:
            await asyncio.wait_for(handle.result(), timeout=60)
        else:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await asyncio.wait_for(handle.result(), timeout=60)
            assert _error(excinfo.value).type == scenario.fails_with
        return await handle.fetch_history()


@pytest.mark.parametrize("scenario", sorted(SCENARIOS))
async def test_every_run_a_scenario_records_replays(scenario):
    history = await _record(scenario)
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
    """No committed history protects nothing in flight."""
    assert sorted(GOLDEN.glob("*.json"))


@pytest.mark.parametrize("path", sorted(GOLDEN.glob("*.json")), ids=lambda p: p.stem)
async def test_golden_histories_replay_on_the_current_code(path):
    recorded = json.loads(path.read_text())
    history = WorkflowHistory.from_json(recorded["workflow_id"], recorded["history"])
    await _replayer().replay_workflow(history)
