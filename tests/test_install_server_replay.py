"""Replay: every history install-server records must replay on the current code.

Production install-server runs are in flight whenever a brain is deployed — an
Agent wait alone is an hour per candidate — so a change to the commands a
recorded run issued (an activity added before an existing one, a branch taken
differently) fails that run on its next replay with a non-determinism error.
The only cure is `workflow.patched()`. Two sources of histories catch it:

  1. histories recorded HERE, from scenarios on the mock limbs of
     tests/test_install_server_workflow.py, replayed at once: they catch
     non-determinism inside the code (a clock, a random, a set's order);
  2. GOLDEN histories in tests/histories/install_server/, recorded by earlier
     code and committed: they catch a change that would break a run in flight.
     When this fails, the change needs `workflow.patched()` — do NOT re-record.
     Re-record only once no run of the old shape can still exist:

         UPDATE_REPLAY_HISTORIES=1 .venv/bin/pytest tests/test_install_server_replay.py

The golden set was first recorded on the workflow as it stood before
`allow_unhealthy`, the NMStateConfig replacement and the own-lock re-run, so
each of those patches is proven against the shape production runs have.
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

from shared.consts import INSTALL_SERVER_WORKFLOW_QUEUE
from shared.exceptions import BmhConflictError, ServerNotAvailableError
from shared.models.server_lifecycle import InstallServerInput, InstallServerRunArgs
from tests.test_install_server_workflow import (
    INPUT,
    SERVER_NAME,
    _application_error,
    _Harness,
    bondable_server,
    make_mock_activities,
    oneview_server,
    ucs_server,
)
from workflow_domains.server_lifecycle.install_server import InstallServerWorkflow

GOLDEN = pathlib.Path(__file__).parent / "histories" / "install_server"
_RECORD = os.environ.get("UPDATE_REPLAY_HISTORIES") == "1"

_BLADE = "ocp-cisco-m6-bat-yam-64c-512gb-FCH0001"
_CONFLICT = BmhConflictError(
    "NMStateConfig nmstate-config-x already exists and does not match this request: "
    "VLAN id: want ['24'], found ['77']"
)


def _named(server_name: str) -> InstallServerInput:
    return INPUT.model_copy(update={"server_name": server_name})


@dataclass
class Scenario:
    """One run: the mock limbs' behaviour, the input, and how it must end."""

    mocks: dict[str, Any] = field(default_factory=dict)
    input: InstallServerInput = field(default_factory=lambda: INPUT)
    # The ApplicationError type the run fails with; None = it completes.
    fails_with: str | None = None


SCENARIOS: dict[str, Callable[[], Scenario]] = {
    # A Dell from the pool: lock, Secret -> BMH -> NMStateConfig, Agent, hold.
    "pool-install": lambda: Scenario(),
    # One named machine.
    "named-install": lambda: Scenario(input=_named(SERVER_NAME)),
    # An IPMI blade: PXE map, NMStateConfig FIRST, boot entry, Secret, BMH.
    "ipmi-blade": lambda: Scenario(
        mocks={"candidates": [ucs_server()], "image_after_polls": 2},
    ),
    # The incident: the NMStateConfig already exists with other content, after
    # this run created the BMH (Redfish order) ...
    "nmstate-conflict-redfish": lambda: Scenario(
        mocks={"nmstate_error": _CONFLICT},
        input=_named(SERVER_NAME),
        fails_with=BmhConflictError.__name__,
    ),
    # ... and before it (IPMI order).
    "nmstate-conflict-ipmi": lambda: Scenario(
        mocks={"candidates": [ucs_server()], "nmstate_error": _CONFLICT},
        input=_named(_BLADE),
        fails_with=BmhConflictError.__name__,
    ),
    # A named machine server-scan will not hand out (its own leftover lock, a
    # health tier, a claim — the run cannot tell which from the 404).
    "named-not-available": lambda: Scenario(
        mocks={
            "acquire_error": ServerNotAvailableError(
                f"'{SERVER_NAME}' is no longer available: health=HEALTHY"
            )
        },
        input=_named(SERVER_NAME),
        fails_with=ServerNotAvailableError.__name__,
    ),
    # Skips before the install: no bond, another run's lock, then the next.
    "skips-then-install": lambda: Scenario(
        mocks={
            "candidates": [
                oneview_server(),
                bondable_server("srv_held", name="ocp-held", mac_prefix="11:22:33:44:55"),
                bondable_server(),
            ],
            "reserved_elsewhere": {"ocp-held"},
        },
    ),
}


_SCRUBBED = {"identity": "install-server-replay", "stackTrace": ""}


def _scrubbed(node: Any) -> Any:
    """The history with every worker `identity` and failure `stackTrace` neutralised.

    An identity is `<pid>@<hostname>` of whoever recorded it, a stack trace
    carries their local paths, and this repo is public. The replayer reads
    neither, so nothing is lost.
    """
    if isinstance(node, dict):
        return {
            key: (_SCRUBBED[key] if key in _SCRUBBED else _scrubbed(value))
            for key, value in node.items()
        }
    if isinstance(node, list):
        return [_scrubbed(item) for item in node]
    return node


def _replayer() -> Replayer:
    return Replayer(workflows=[InstallServerWorkflow], data_converter=pydantic_data_converter)


async def _record(name: str) -> WorkflowHistory:
    scenario = SCENARIOS[name]()
    _, segment_acts, server_acts = make_mock_activities(**scenario.mocks)
    async with _Harness(segment_acts, server_acts) as client:
        handle = await client.start_workflow(
            InstallServerWorkflow.run,
            InstallServerRunArgs(input=scenario.input),
            id=f"install-server-replay-{name}",
            task_queue=INSTALL_SERVER_WORKFLOW_QUEUE,
        )
        if scenario.fails_with is None:
            await asyncio.wait_for(handle.result(), timeout=60)
        else:
            with pytest.raises(WorkflowFailureError) as excinfo:
                await asyncio.wait_for(handle.result(), timeout=60)
            assert _application_error(excinfo.value).type == scenario.fails_with
        return await handle.fetch_history()


@pytest.mark.parametrize("scenario", sorted(SCENARIOS))
async def test_every_run_a_scenario_records_replays(scenario):
    history = await _record(scenario)
    await _replayer().replay_workflow(history)
    if _RECORD:
        GOLDEN.mkdir(parents=True, exist_ok=True)
        (GOLDEN / f"{scenario}.json").write_text(
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
    return sorted(GOLDEN.glob("*.json"))


def test_there_are_golden_histories():
    """No committed history protects nothing in flight."""
    assert _golden_files()


@pytest.mark.parametrize("path", _golden_files(), ids=lambda p: p.stem)
async def test_golden_histories_replay_on_the_current_code(path):
    recorded = json.loads(path.read_text())
    history = WorkflowHistory.from_json(recorded["workflow_id"], recorded["history"])
    await _replayer().replay_workflow(history)
