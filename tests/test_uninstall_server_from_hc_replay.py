"""Replay: every history uninstall-server-from-hc records must replay on the current code.

An uninstall waits up to 20 min for a drain and an hour for the host to reboot
into discovery, so runs are in flight whenever a brain is deployed; a change
to the commands a recorded run issued fails that run on its next replay. The
cure is `workflow.patched()`. Two sources of histories catch it, as for
add-server-to-mce (tests/test_add_server_to_mce_replay.py, whose recorder this
follows):

  1. histories recorded HERE, from scenarios on the mock limb of
     tests/test_uninstall_server_from_hc_workflow.py (the REAL remove-server-from-mce child
     included), replayed at once — the parent's history; remove-server-from-mce's own
     is tests/test_remove_server_from_mce_replay.py's;
  2. GOLDEN histories in tests/histories/uninstall_server_from_hc/, recorded by
     earlier code and committed. When one fails, the change needs
     `workflow.patched()` — do NOT re-record. Recording writes ONLY the
     scenarios with no golden file yet:

         UPDATE_REPLAY_HISTORIES=1 .venv/bin/pytest tests/test_uninstall_server_from_hc_replay.py

The first five goldens were recorded on the workflow as it stood before it
released its own install lock after the removal (2026-10-31), so
`uninstall-server-release-after-removal` is proven against the shape
production runs have; the release scenarios were recorded with it, so the
next change is proven against those too.

The workflow was uninstall-server until 2026-11-01. Those goldens stay in
tests/histories/uninstall_server/ (LEGACY_GOLDEN), as runs of type
`UninstallServerWorkflow` whose child is `RemoveServerWorkflow` on its old id
and queue: replayed by the legacy subclass, they prove such a run still
starts the very child it recorded. The goldens recorded since, under the new
type, are in tests/histories/uninstall_server_from_hc/.
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
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.worker import Replayer

from shared.consts import UNINSTALL_SERVER_FROM_HC_WORKFLOW_QUEUE
from shared.exceptions import PxeMapAuthError
from shared.models.server_lifecycle import (
    AgentBinding,
    BoundHostState,
    InventoryHostState,
    UninstallServerFromHcInput,
    UninstallServerFromHcRunArgs,
)
from tests.test_add_server_to_mce_replay import _scrubbed
from tests.test_uninstall_server_from_hc_workflow import (
    IN_INVENTORY,
    INPUT,
    INSTALLED,
    INVENTORY_HOST,
    KEEP,
    ON_THE_MCE,
    _Harness,
    make_mocks,
)
from workflow_domains.server_lifecycle.remove_server_from_mce import (
    RemoveServerFromMceWorkflow,
    RemoveServerWorkflow,
)
from workflow_domains.server_lifecycle.uninstall_server_from_hc import (
    UninstallServerFromHcWorkflow,
    UninstallServerWorkflow,
)

GOLDEN = pathlib.Path(__file__).parent / "histories" / "uninstall_server_from_hc"
# Recorded as uninstall-server, before the rename: never re-recorded, only replayed.
LEGACY_GOLDEN = pathlib.Path(__file__).parent / "histories" / "uninstall_server"
_RECORD = os.environ.get("UPDATE_REPLAY_HISTORIES") == "1"

_AVAILABLE = INSTALLED.model_copy(
    update={"lifecycle_state": "AVAILABLE", "cluster_name": None, "mce_name": None}
)


@dataclass
class Scenario:
    """One run: the mock limb's behaviour, the input, and how it must end."""

    mocks: dict[str, Any] = field(default_factory=dict)
    input: UninstallServerFromHcInput = field(default_factory=lambda: INPUT)
    # The ApplicationError type the run fails with; None = it completes.
    fails_with: str | None = None


SCENARIOS: dict[str, Callable[[], Scenario]] = {
    # A hosted cluster's node: lock, Machine delete, drain + unbind, verify,
    # the day-long hold, remove-server-from-mce.
    "hosted-node": lambda: Scenario(),
    # An inventory host: lock, the fresh look, the hold, remove-server-from-mce.
    "inventory-host": lambda: Scenario(
        mocks={"membership": IN_INVENTORY, "host": INVENTORY_HOST}
    ),
    # keep_inventory: out of the hosted cluster, held a day, no remove-server-from-mce.
    "keep-inventory": lambda: Scenario(input=KEEP),
    # Nothing on the MCE: completes through the child's found=false.
    "nothing-on-the-mce": lambda: Scenario(
        mocks={
            "membership": _AVAILABLE,
            "host": BoundHostState(),
            "inventory": InventoryHostState(),
        }
    ),
    # remove-server-from-mce refuses an Agent bound since the inspection.
    "child-failed": lambda: Scenario(
        mocks={
            "membership": IN_INVENTORY,
            "host": INVENTORY_HOST,
            "inventory": ON_THE_MCE.model_copy(update={
                "agents": [AgentBinding(name="agent-1", cluster_deployment="clusters-hc-02/hc-02")]
            }),
        },
        fails_with="RemoveServerFromMceFailedError",
    ),
    # Since 2026-10-31: the run's own lock released once remove-server-from-mce completed ...
    "released-after-removal": lambda: Scenario(),
    "inventory-host-released": lambda: Scenario(
        mocks={"membership": IN_INVENTORY, "host": INVENTORY_HOST}
    ),
    # ... and kept when remove-server-from-mce's PXE cleanup failed.
    "pxe-failed-hold-kept": lambda: Scenario(
        mocks={"pxe_error": PxeMapAuthError("check PXE_MAP_TOKEN")}
    ),
}


def _replayer() -> Replayer:
    # Each history picks its class by the workflow type it recorded.
    return Replayer(
        workflows=[
            UninstallServerFromHcWorkflow,
            RemoveServerFromMceWorkflow,
            UninstallServerWorkflow,
            RemoveServerWorkflow,
        ],
        data_converter=pydantic_data_converter,
    )


def _failure_type(exc: WorkflowFailureError) -> str | None:
    cause = exc.cause
    if isinstance(cause, ActivityError):
        cause = cause.cause
    return cause.type if isinstance(cause, ApplicationError) else None


async def _record(name: str) -> WorkflowHistory:
    scenario = SCENARIOS[name]()
    _, acts = make_mocks(**scenario.mocks)
    async with _Harness(acts) as client:
        handle = await client.start_workflow(
            UninstallServerFromHcWorkflow.run,
            UninstallServerFromHcRunArgs(input=scenario.input),
            id=f"uninstall-server-from-hc-replay-{name}",
            task_queue=UNINSTALL_SERVER_FROM_HC_WORKFLOW_QUEUE,
        )
        if scenario.fails_with is None:
            await asyncio.wait_for(handle.result(), timeout=60)
        else:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await asyncio.wait_for(handle.result(), timeout=60)
            assert _failure_type(excinfo.value) == scenario.fails_with
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
