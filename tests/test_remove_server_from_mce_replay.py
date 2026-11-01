"""Replay: every history remove-server-from-mce records must replay on the current code.

remove-server-from-mce runs in flight whenever a brain is deployed — a power-off and the
final check take minutes, and as uninstall-server-from-hc's child it starts hours
after its parent — so a change to the commands a recorded run issued fails
that run on its next replay with a non-determinism error. The cure is
`workflow.patched()`. Two sources of histories catch it, as for add-server-to-mce
(tests/test_add_server_to_mce_replay.py, whose recorder this follows):

  1. histories recorded HERE, from scenarios on the mock limbs of
     tests/test_remove_server_from_mce_workflow.py, replayed at once;
  2. GOLDEN histories in tests/histories/remove_server_from_mce/, recorded by earlier
     code and committed. When one fails, the change needs `workflow.patched()`
     — do NOT re-record. Recording writes ONLY the scenarios with no golden
     file yet:

         UPDATE_REPLAY_HISTORIES=1 .venv/bin/pytest tests/test_remove_server_from_mce_replay.py

The first five goldens were recorded on the workflow as it stood before it
released server-scan's install lock (2026-10-31), so
`remove-server-release-own-mce-lock` is proven against the shape production
runs have; the lock scenarios were recorded with it, so the next change is
proven against those too.

The workflow was remove-server until 2026-11-01. Those goldens stay in
tests/histories/remove_server/ (LEGACY_GOLDEN), as runs of type
`RemoveServerWorkflow`, replayed by the legacy subclass; the goldens recorded
since, under the new type, are in tests/histories/remove_server_from_mce/.
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

from shared.consts import REMOVE_SERVER_FROM_MCE_WORKFLOW_QUEUE
from shared.exceptions import PxeMapAuthError
from shared.models.server_lifecycle import (
    InventoryHostState,
    RemoveServerFromMceInput,
    RemoveServerFromMceRunArgs,
    ServerLockRecord,
)
from tests.test_add_server_to_mce_replay import _scrubbed
from tests.test_remove_server_from_mce_workflow import (
    AGENT_ONLY,
    INPUT,
    INSTALL_LOCK,
    MCE,
    WITH_MACS,
    _error,
    _Harness,
    make_mocks,
)
from workflow_domains.server_lifecycle.remove_server_from_mce import (
    RemoveServerFromMceWorkflow,
    RemoveServerWorkflow,
)

GOLDEN = pathlib.Path(__file__).parent / "histories" / "remove_server_from_mce"
# Recorded as remove-server, before the rename: never re-recorded, only replayed.
LEGACY_GOLDEN = pathlib.Path(__file__).parent / "histories" / "remove_server"
_RECORD = os.environ.get("UPDATE_REPLAY_HISTORIES") == "1"


@dataclass
class Scenario:
    """One run: the mock limbs' behaviour, the input, and how it must end."""

    mocks: dict[str, Any] = field(default_factory=dict)
    input: RemoveServerFromMceInput = field(default_factory=lambda: INPUT)
    # The ApplicationError type the run fails with; None = it completes.
    fails_with: str | None = None


SCENARIOS: dict[str, Callable[[], Scenario]] = {
    # An inventory host: power off, BMH + NMStateConfig, Agent, PXE map, check.
    "inventory-host": lambda: Scenario(mocks={"state": WITH_MACS}),
    # An Agent whose BMH is gone: nothing to power off through.
    "agent-only": lambda: Scenario(mocks={"state": AGENT_ONLY}),
    # Nothing of the server on the MCE: a no-op.
    "nothing-on-the-mce": lambda: Scenario(mocks={"state": InventoryHostState()}),
    # The PXE VM refuses the token: reported, and the removal completes.
    "pxe-failed": lambda: Scenario(
        mocks={"state": WITH_MACS, "pxe_error": PxeMapAuthError("check PXE_MAP_TOKEN")}
    ),
    # A re-registered Agent: the final check fails the run.
    "still-present": lambda: Scenario(
        mocks={"state": AGENT_ONLY, "after": InventoryHostState(bmh_found=True)},
        fails_with="ServerStillPresentError",
    ),
    # Since 2026-10-31: add-server-to-mce's lock for THIS MCE released after the check ...
    "releases-install-lock": lambda: Scenario(mocks={"state": WITH_MACS, "lock": INSTALL_LOCK}),
    # ... another MCE's left alone ...
    "another-mces-lock": lambda: Scenario(
        mocks={"lock": INSTALL_LOCK.model_copy(update={"mce_cluster": "ocp4-mce-other"})}
    ),
    # ... uninstall-server-from-hc's (this run as its child) left to the parent ...
    "parents-lock-left": lambda: Scenario(
        mocks={"lock": ServerLockRecord(live=True, holder="uninstall-server-from-hc",
                                        workflow_id="uninstall-server-from-hc-x", mce_cluster=MCE)}
    ),
    # ... and nothing read when the PXE cleanup failed.
    "pxe-failed-lock-kept": lambda: Scenario(
        mocks={"state": WITH_MACS, "lock": INSTALL_LOCK,
               "pxe_error": PxeMapAuthError("check PXE_MAP_TOKEN")}
    ),
}


def _replayer() -> Replayer:
    # Each history picks its class by the workflow type it recorded.
    return Replayer(
        workflows=[RemoveServerFromMceWorkflow, RemoveServerWorkflow],
        data_converter=pydantic_data_converter,
    )


async def _record(name: str) -> WorkflowHistory:
    scenario = SCENARIOS[name]()
    _, acts = make_mocks(**scenario.mocks)
    async with _Harness(acts) as client:
        handle = await client.start_workflow(
            RemoveServerFromMceWorkflow.run,
            RemoveServerFromMceRunArgs(input=scenario.input),
            id=f"remove-server-from-mce-replay-{name}",
            task_queue=REMOVE_SERVER_FROM_MCE_WORKFLOW_QUEUE,
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
                {
                    "workflow_id": history.workflow_id,
                    "history": _scrubbed(json.loads(history.to_json())),
                },
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        )


def _golden_files() -> list[pathlib.Path]:
    return sorted([*GOLDEN.glob("*.json"), *LEGACY_GOLDEN.glob("*.json")])


def test_there_are_golden_histories():
    """No committed history protects nothing in flight — under either name."""
    assert sorted(GOLDEN.glob("*.json")) and sorted(LEGACY_GOLDEN.glob("*.json"))


@pytest.mark.parametrize("path", _golden_files(), ids=lambda p: f"{p.parent.name}/{p.stem}")
async def test_golden_histories_replay_on_the_current_code(path):
    recorded = json.loads(path.read_text())
    history = WorkflowHistory.from_json(recorded["workflow_id"], recorded["history"])
    await _replayer().replay_workflow(history)
