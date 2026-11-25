"""Replay: every history a KubeVirt InfraEnv's add-server-to-mce run records must replay.

The same guard as tests/test_add_server_to_mce_replay.py, for the branch that
has a VM made (`add-server-creates-vm`, 2026-11-25): each scenario is recorded
on the mock limbs and the stand-in create-vm child of
tests/test_add_server_to_mce_vm_workflow.py, replayed at once, and kept as a
GOLDEN history in tests/histories/add_server_to_mce/ (`vm-*.json`), which
test_add_server_to_mce_replay.py replays with every other golden on every run.

A golden failing to replay means the change needs `workflow.patched()` — do NOT
re-record. Recording writes only the scenarios that have no file yet:

    UPDATE_REPLAY_HISTORIES=1 .venv/bin/pytest tests/test_add_server_to_mce_vm_replay.py

Recorded with the production constants (the time-skipping server passes the
Agent's hour and the rollback's waits): a history recorded under shrunk ones
does not replay on the real ones.
"""

from __future__ import annotations

import asyncio
import json
import os
import pathlib
from dataclasses import dataclass, field
from typing import Any

import pytest
from temporalio.client import WorkflowFailureError, WorkflowHistory
from temporalio.exceptions import is_cancelled_exception

from shared.consts import ADD_SERVER_TO_MCE_WORKFLOW_QUEUE
from shared.exceptions import (
    AgentNeverAppearedError,
    BmcCredentialsMissingError,
    BmhConflictError,
    CreateVmFailedError,
    InfraEnvAgentLabelsError,
    KubevirtBmcNotConfiguredError,
    NamedServerOnKubevirtInfraEnvError,
    VmNameClashError,
)
from shared.models.server_lifecycle import AddServerToMceInput, AddServerToMceRunArgs
from tests.test_add_server_to_mce_replay import GOLDEN, _replayer, _scrubbed
from tests.test_add_server_to_mce_vm_workflow import (
    CHILD,
    INPUT,
    KV_LABELS,
    VM,
    _Harness,
    _failure,
    _until_phase,
    make_vm_mocks,
)
from workflow_domains.server_lifecycle.add_server_to_mce import AddServerToMceWorkflow

_RECORD = os.environ.get("UPDATE_REPLAY_HISTORIES") == "1"


@dataclass
class Scenario:
    """One run: the mock limbs' and the child's behaviour, the input, and how it must end."""

    mocks: dict[str, Any] = field(default_factory=dict)
    input: AddServerToMceInput = field(default_factory=lambda: INPUT)
    # The stand-in create-vm's script (ok | fail | hold).
    child: str = "ok"
    # The ApplicationError type the run fails with; None = it completes (or is cancelled).
    fails_with: str | None = None
    # Cancel the run once it reports this phase: it must then end CANCELED.
    cancel_at: str | None = None


SCENARIOS: dict[str, Scenario] = {
    # A VM made, then NMStateConfig -> Secret -> BareMetalHost, its Agent: no server-scan call.
    "vm-install": Scenario(),
    # No Agent in the hour: the host out of the MCE (metal3 waited for), then the VM deleted.
    "vm-no-agent": Scenario(mocks={"agent_never_for": {VM}}, fails_with=AgentNeverAppearedError.__name__),
    # ... and a BareMetalHost metal3 will not let go: the VM is kept.
    "vm-bmh-still-deleting": Scenario(
        mocks={"agent_never_for": {VM}, "bmh_stays_deleting": {VM}},
        fails_with=AgentNeverAppearedError.__name__,
    ),
    # create-vm made no VM: nothing on the MCE, nothing to undo.
    "vm-create-failed": Scenario(child="fail", fails_with=CreateVmFailedError.__name__),
    # A host of the VM's name already on the MCE: nothing written, the VM deleted.
    "vm-name-clash": Scenario(mocks={"already_installed": {VM}}, fails_with=VmNameClashError.__name__),
    # A conflict at the first create: nothing on the MCE touched, the VM deleted.
    "vm-nmstate-conflict": Scenario(
        mocks={"nmstate_error": BmhConflictError(f"NMStateConfig {VM} already exists and does not match this request")},
        fails_with=BmhConflictError.__name__,
    ),
    # The BareMetalHost refused: NMStateConfig and Secret out, then the VM.
    "vm-bmh-refused": Scenario(
        mocks={"bmh_error": BmcCredentialsMissingError("No BMC credentials configured for vendor KUBEVIRT")},
        fails_with=BmcCredentialsMissingError.__name__,
    ),
    # Refused before any VM: a worker not set up for VMs, labels that cannot say which VM, a named server.
    "vm-worker-not-configured": Scenario(
        mocks={"configured": False}, fails_with=KubevirtBmcNotConfiguredError.__name__
    ),
    "vm-labels-refused": Scenario(
        mocks={"labels": {**KV_LABELS, "redbull.io/size": "kv7"}}, fails_with=InfraEnvAgentLabelsError.__name__
    ),
    "vm-named-server-refused": Scenario(
        input=INPUT.model_copy(update={"server_name": VM}), fails_with=NamedServerOnKubevirtInfraEnvError.__name__
    ),
    # Cancelled while the child runs: the child stopped, which removes its own VM.
    "vm-cancelled-during-create": Scenario(child="hold", cancel_at="creating-vm"),
    # Cancelled while waiting for the Agent: remove-server-from-mce, then the VM deleted.
    "vm-cancelled-awaiting-agent": Scenario(mocks={"agent_never_for": {VM}}, cancel_at="awaiting-agent"),
}


async def _record(name: str) -> WorkflowHistory:
    scenario = SCENARIOS[name]
    async with _Harness(make_vm_mocks(**scenario.mocks)) as client:
        CHILD["mode"] = scenario.child
        handle = await client.start_workflow(
            AddServerToMceWorkflow.run,
            AddServerToMceRunArgs(input=scenario.input),
            id=f"add-server-to-mce-replay-{name}",
            task_queue=ADD_SERVER_TO_MCE_WORKFLOW_QUEUE,
        )
        if scenario.cancel_at is not None:
            await _until_phase(handle, scenario.cancel_at)
            if scenario.child == "hold":
                for _ in range(200):  # the child must be there to be stopped
                    if CHILD.get("run_ids"):
                        break
                    await asyncio.sleep(0.05)
            await handle.cancel()
            with pytest.raises(WorkflowFailureError) as excinfo:
                await asyncio.wait_for(handle.result(), timeout=60)
            assert is_cancelled_exception(excinfo.value.cause)
        elif scenario.fails_with is None:
            await asyncio.wait_for(handle.result(), timeout=60)
        else:
            assert (await _failure(handle)).type == scenario.fails_with
        return await handle.fetch_history()


@pytest.mark.parametrize("scenario", sorted(SCENARIOS))
async def test_every_run_a_vm_scenario_records_replays(scenario):
    history = await _record(scenario)
    await _replayer().replay_workflow(history)
    golden = GOLDEN / f"{scenario}.json"
    if _RECORD and not golden.exists():
        golden.write_text(
            json.dumps(
                {"workflow_id": history.workflow_id, "history": _scrubbed(json.loads(history.to_json()))},
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        )


def test_every_vm_scenario_has_its_golden():
    """A scenario with no committed history protects no run in flight."""
    missing = [name for name in SCENARIOS if not pathlib.Path(GOLDEN / f"{name}.json").exists()]
    assert not missing, f"record them: UPDATE_REPLAY_HISTORIES=1 pytest {pathlib.Path(__file__).name} ({missing})"
