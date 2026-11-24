"""Replay: every history add-server-to-mce records must replay on the current code.

Production add-server-to-mce runs are in flight whenever a brain is deployed — an
Agent wait alone is an hour per candidate — so a change to the commands a
recorded run issued (an activity added before an existing one, a branch taken
differently) fails that run on its next replay with a non-determinism error.
The only cure is `workflow.patched()`. Two sources of histories catch it:

  1. histories recorded HERE, from scenarios on the mock limbs of
     tests/test_add_server_to_mce_workflow.py, replayed at once: they catch
     non-determinism inside the code (a clock, a random, a set's order);
  2. GOLDEN histories in tests/histories/add_server_to_mce/, recorded by earlier
     code and committed: they catch a change that would break a run in flight.
     When this fails, the change needs `workflow.patched()` — do NOT re-record.
     Recording writes ONLY the scenarios that have no golden file yet, so a new
     scenario gets one and an old shape is never overwritten by accident:

         UPDATE_REPLAY_HISTORIES=1 .venv/bin/pytest tests/test_add_server_to_mce_replay.py

     Re-record an existing one (delete its file first) only once no run of
     that shape can still exist.

The golden set was first recorded on the workflow as it stood before
`allow_unhealthy`, the NMStateConfig replacement and the own-lock re-run, so
each of those patches is proven against the shape production runs have.

Every golden but the `allocated-*` ones was recorded before the run was
allocated ONE server by server-scan (`add-server-allocates-one-server`,
2026-11-17): they draw a candidate list with GET /servers/available and lock
the one chosen, and prove a run in flight then still finishes that way. The
scenarios of the same names now RECORD the allocated shape live; the
`allocated-*` goldens keep it for the next change. Two of them were
re-recorded the same day, before the allocation was ever deployed (no run of
their first shape can exist): `allocated-server-refused` once an allocated
server's NICs and MACs were no longer judged by the run, and
`allocated-no-agent` once such a server went into maintenance.

Every golden before `rollback-waits-for-metal3` and `rollback-bmh-still-deleting`
that rolls a host back does it through teardown_bmh_resources, which is what
proves `add-server-rollback-keeps-finalizer` (2026-11-24) replays them unchanged.

The workflow was install-server until 2026-11-01. Those goldens stay where they
were recorded, in tests/histories/install_server/ (LEGACY_GOLDEN), as runs of
type `InstallServerWorkflow`: replayed by the legacy subclass, they prove a run
started before the rename finishes on the renamed code. The goldens recorded
since, under the new type, are in tests/histories/add_server_to_mce/.
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

from shared.consts import ADD_SERVER_TO_MCE_WORKFLOW_QUEUE
from shared.exceptions import (
    AgentNeverAppearedError,
    BmhConflictError,
    BmhDeletionTimeoutError,
    InfraEnvAgentLabelsError,
    ServerLooksInstalledError,
    ServerNotAvailableError,
    ServerScanAuthError,
    UnknownBmcVendorError,
)
from shared.models.server_lifecycle import (
    AddServerToMceInput,
    AddServerToMceRunArgs,
    ServerLockRecord,
)
from tests.test_add_server_to_mce_workflow import (
    INPUT,
    SERVER_NAME,
    _application_error,
    _Harness,
    _refused,
    _FINE,
    _unhealthy,
    bondable_server,
    make_mock_activities,
    one_nic_up_server,
    scan_record,
    standalone_server,
    ucs_server,
)
from workflow_domains.server_lifecycle.add_server_to_mce import (
    AddServerToMceWorkflow,
    InstallServerWorkflow,
)

GOLDEN = pathlib.Path(__file__).parent / "histories" / "add_server_to_mce"
# Recorded as install-server, before the rename: never re-recorded, only replayed.
LEGACY_GOLDEN = pathlib.Path(__file__).parent / "histories" / "install_server"
_RECORD = os.environ.get("UPDATE_REPLAY_HISTORIES") == "1"

_BLADE = "ocp-cisco-m6-bat-yam-64c-512gb-FCH0001"
_CONFLICT = BmhConflictError(
    "NMStateConfig nmstate-config-x already exists and does not match this request: "
    "VLAN id: want ['24'], found ['77']"
)


def _named(server_name: str) -> AddServerToMceInput:
    return INPUT.model_copy(update={"server_name": server_name})


@dataclass
class Scenario:
    """One run: the mock limbs' behaviour, the input, and how it must end."""

    mocks: dict[str, Any] = field(default_factory=dict)
    input: AddServerToMceInput = field(default_factory=lambda: INPUT)
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
    # allow_unhealthy: /servers/available refuses the named one-NIC server,
    # its stored record is read, the health refusals waived.
    "allow-unhealthy-one-nic": lambda: Scenario(
        mocks={"acquire_error": _refused(), "record": scan_record(one_nic_up_server())},
        input=_unhealthy(),
    ),
    # A stale NMStateConfig of the server's name: replaced before the first
    # create, and the install goes on.
    "nmstate-replaced": lambda: Scenario(
        mocks={"nmstate_differs": {SERVER_NAME: ["VLAN id: want ['24'], found ['77']"]}},
        input=_named(SERVER_NAME),
    ),
    # An OLDER limb's answer (before 2026-10-31's revision) when a
    # BareMetalHost of it was there: refused, the lock given back, nothing
    # written. The current limb never refuses; the workflow still handles it.
    "nmstate-in-use": lambda: Scenario(
        mocks={"in_use": {SERVER_NAME: f"BareMetalHost {SERVER_NAME.lower()} exists"}},
        input=_named(SERVER_NAME),
        fails_with=ServerLooksInstalledError.__name__,
    ),
    # A re-run of a named install after a failure: server-scan hides the
    # machine behind the run's OWN leftover lock; the record shows it is ours.
    "named-own-lock-rerun": lambda: Scenario(
        mocks={
            "acquire_error": _refused(),
            "record": scan_record(
                bondable_server(),
                health=_FINE,
                lock=ServerLockRecord(
                    live=True,
                    holder="add-server-to-mce",
                    workflow_id="add-server-to-mce-replay-named-own-lock-rerun",
                ),
            ),
        },
        input=_named(SERVER_NAME),
    ),
    # (`skips-then-install` — no bond, another run's lock, then the next — is a
    # candidate loop no run started since the allocation has: its golden stays,
    # replayed, and it is recorded live no more.)
    #
    # Since the allocation (2026-11-17): ONE server, locked by server-scan.
    # Each of these got a golden of the allocated shape.
    "allocated-pool-install": lambda: Scenario(),
    "allocated-named-install": lambda: Scenario(input=_named(SERVER_NAME)),
    # Refused by the run's own checks (no BMC driver; its NICs and MACs are
    # server-scan's word): its lock given back, the run failed.
    "allocated-server-refused": lambda: Scenario(
        mocks={"candidates": [standalone_server(), bondable_server()]},
        fails_with=UnknownBmcVendorError.__name__,
    ),
    # No Agent within the hour: rolled back, put in maintenance, given back, failed.
    "allocated-no-agent": lambda: Scenario(
        mocks={"agent_never_for": {SERVER_NAME}},
        fails_with=AgentNeverAppearedError.__name__,
    ),
    # A named re-run meeting its own leftover lock, which the allocation
    # refuses even to its holder: recognised, re-taken, installed.
    "allocated-named-own-lock-rerun": lambda: Scenario(
        mocks={
            "acquire_error": ServerNotAvailableError(
                f"server-scan could not allocate a server (name={SERVER_NAME!r}): RESERVED"
            ),
            "record": scan_record(
                bondable_server(),
                health=_FINE,
                lock=ServerLockRecord(
                    live=True,
                    holder="add-server-to-mce",
                    workflow_id="add-server-to-mce-replay-allocated-named-own-lock-rerun",
                ),
            ),
        },
        input=_named(SERVER_NAME),
    ),
    # After the Agent (2026-11-09): server-scan read every 5 min until it shows
    # the server installed, then the lock released ...
    "release-once-server-scan-sees-it": lambda: Scenario(
        mocks={"scan_states": ["AVAILABLE", "AVAILABLE", "INSTALLED_TO_INVENTORY"]},
    ),
    # ... or, server-scan unreadable, the lock kept and the run completed.
    "server-scan-unreadable-after-install": lambda: Scenario(
        mocks={"scan_error": ServerScanAuthError("server-scan rejected our credentials (401)")},
    ),
    # Since 2026-11-22 the pool is `ocp-<flavor>-<size>-<site>-`, from the
    # InfraEnv's agentLabels (every allocated-* golden above predates the key
    # and draws by the name's first three words, as an older limb's run does).
    "labelled-pool-install": lambda: Scenario(),
    # ... and an InfraEnv whose labels cannot name a server fails before the draw.
    "infraenv-without-agent-labels": lambda: Scenario(
        mocks={"agent_labels": {"topology.kubernetes.io/zone": "az1"}},
        fails_with=InfraEnvAgentLabelsError.__name__,
    ),
    # Since 2026-11-24 (`add-server-rollback-keeps-finalizer`) a rollback deletes
    # with no detach and waits for metal3 to let the host go (every golden above
    # tears down through teardown_bmh_resources) ...
    "rollback-waits-for-metal3": lambda: Scenario(
        mocks={"agent_never_for": {SERVER_NAME}},
        fails_with=AgentNeverAppearedError.__name__,
    ),
    # ... or fails at 15 min, metal3's finalizer left in place, nothing after it.
    "rollback-bmh-still-deleting": lambda: Scenario(
        mocks={"agent_never_for": {SERVER_NAME}, "bmh_stays_deleting": {SERVER_NAME}},
        fails_with=BmhDeletionTimeoutError.__name__,
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
    # Each history picks its class by the workflow type it recorded.
    return Replayer(
        workflows=[AddServerToMceWorkflow, InstallServerWorkflow],
        data_converter=pydantic_data_converter,
    )


async def _record(name: str) -> WorkflowHistory:
    scenario = SCENARIOS[name]()
    _, segment_acts, server_acts = make_mock_activities(**scenario.mocks)
    async with _Harness(segment_acts, server_acts) as client:
        handle = await client.start_workflow(
            AddServerToMceWorkflow.run,
            AddServerToMceRunArgs(input=scenario.input),
            id=f"add-server-to-mce-replay-{name}",
            task_queue=ADD_SERVER_TO_MCE_WORKFLOW_QUEUE,
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
