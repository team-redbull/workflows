"""create-vm's runs in flight are guarded by REPLAY: a workflow-code change that would make a
recorded history non-deterministic fails here, before it ships (CLAUDE.md §5).

The only cure for such a change is `workflow.patched()`. Two sources of histories:

  1. histories recorded HERE, from scenarios on the fleet of fake KV clusters of
     tests/test_create_vm_workflow.py (the real workflows, the real limb code),
     replayed at once — a change that breaks determinism within one commit;
  2. GOLDEN histories in tests/histories/create_vm/, recorded by earlier
     versions of the code and replayed on the current code — the runs a deploy
     would find in flight.

A golden is written only with UPDATE_REPLAY_HISTORIES=1 and only when its
file does not exist: never re-recorded to make a change pass.

The goldens of the design before this one (every run ranking the zone's
clusters through HTTP agents, 2026-11-05) were deleted with it on 2026-11-20:
that workflow could only run against agents that were never deployed, the
operator terminates any such run before the brain is replaced
(docs/design/create-vm-placement-plan.md §0), and the rewrite is not patched
to replay them.
"""

from __future__ import annotations

import asyncio
import json
import os
import pathlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta

import pytest
from temporalio.client import WorkflowFailureError, WorkflowHandle, WorkflowHistory
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import CancelledError
from temporalio.worker import Replayer

from activities.vm_lifecycle.kubevirt_api import MacRefused, NameTaken
from shared.exceptions import (
    KvZoneLeaseExpiredError,
    KvZoneLockTimeoutError,
    NoKvClusterError,
    NoKvClusterFitsError,
    VmLeaseExpiredError,
    VmMacExhaustedError,
    VmRootDiskNotBoundError,
)
from tests.kv_fleet import KvFleet
from tests.test_add_server_to_mce_replay import _scrubbed
from tests.test_create_vm_workflow import A, B, Harness, cause, dead_and_alive, fleet_of
from workflow_domains.vm_lifecycle.create_vm import CreateVmWorkflow

GOLDEN = pathlib.Path(__file__).parent / "histories" / "create_vm"
_RECORD = os.environ.get("UPDATE_REPLAY_HISTORIES") == "1"


async def _just_start(h: Harness) -> WorkflowHandle:
    return await h.start(workflow_id=None)


@dataclass
class Scenario:
    """One run: the fleet it meets, how the test drives it, and how it must end."""

    fleet: Callable[[], KvFleet]
    # The ApplicationError type the run fails with; None = it completes; "cancelled" = it ends CANCELED.
    ends: str | None = None
    down: Callable[[KvFleet], set[str]] = lambda fleet: set()
    lock_worker: bool = True
    drive: Callable[[Harness], Awaitable[WorkflowHandle]] = _just_start
    # Tasks nobody serves while the run waits (a figures read on a dead cluster's queue, the
    # lock's own workflow task): the clock is jumped past that many (tests/manual_time.py),
    # in steps of `step`. EVERY scenario records with the production waits.
    outstanding: int = 0
    step: timedelta = timedelta(seconds=1)


def _full() -> KvFleet:
    fleet = fleet_of(A, B)
    for name in (A, B):
        fleet.clusters[name].add_pod(f"{name}-n1", 60_000, 0)
    return fleet


def _first_full() -> KvFleet:
    fleet = fleet_of(A, B)
    fleet.clusters[A].add_pod(f"{A}-n1", 60_000, 0)
    return fleet


def _redraw() -> KvFleet:
    fleet = fleet_of(A)
    fleet.clusters[A].fail("create_vm", NameTaken("exists"))
    return fleet


def _exhausted() -> KvFleet:
    fleet = fleet_of(A)
    fleet.clusters[A].fail("create_vm", NameTaken("exists"), MacRefused("failed to allocate requested mac address"), NameTaken("exists"))
    return fleet


def _late() -> KvFleet:
    fleet = fleet_of(A)
    fleet.skew = timedelta(hours=1)
    return fleet


async def _behind_another(h: Harness) -> WorkflowHandle:
    """Started while another run holds the zone: it waits for the lock, then is served."""
    h.fleet.clusters[A].delays["create_vm"] = 0.6
    first = await h.start()
    await h.until_phase(first, "creating-vm")
    second = await h.start()
    await h.until_phase(second, "awaiting-zone-lock")
    await h.result(first)
    return second


async def _cancel_in_the_disk_wait(h: Harness) -> WorkflowHandle:
    h.fleet.poll_seconds = 0.05
    handle = await h.start()
    await h.until_phase(handle, "awaiting-root-disk")
    await handle.cancel()
    return handle


async def _cancel_in_the_line(h: Harness) -> WorkflowHandle:
    h.fleet.clusters[A].delays["create_vm"] = 1.0
    first = await h.start()
    await h.until_phase(first, "creating-vm")
    second = await h.start()
    await h.until_phase(second, "awaiting-zone-lock")
    await h.until_lock(lambda p: p.waiting == 1)
    await second.cancel()
    return second


SCENARIOS: dict[str, Scenario] = {
    # List, lock, one cluster's figures, the create, release, the disk wait, done.
    "create": Scenario(fleet=lambda: fleet_of(A)),
    # The oldest cluster is over its buffer: the second one gets the VM.
    "second-cluster": Scenario(fleet=_first_full),
    # Nothing fits: fails on the table after the release, nothing created.
    "no-capacity": Scenario(fleet=_full, ends=NoKvClusterFitsError.__name__),
    # day1 lists no cluster for the zone: fails before the lock is even asked for.
    "no-cluster": Scenario(fleet=lambda: KvFleet(), ends=NoKvClusterError.__name__),
    # The oldest cluster has no worker: a `no-worker` row, then the next cluster.
    "no-kv-worker": Scenario(fleet=lambda: fleet_of(A, B), down=lambda fleet: {A}, outstanding=1, step=timedelta(seconds=10)),
    # A name taken on the first draw: the WORKER draws again; the run sees one create.
    "mac-redraw": Scenario(fleet=_redraw),
    # Three draws taken: the worker's permanent error, after the release.
    "draws-exhausted": Scenario(fleet=_exhausted, ends=VmMacExhaustedError.__name__),
    # The create reached the worker after its lease: nothing created.
    "lease-expired-at-the-worker": Scenario(fleet=_late, ends=VmLeaseExpiredError.__name__),
    # The disk never binds: the VM is deleted again, then the run fails.
    "rollback": Scenario(fleet=lambda: fleet_of(A, storage="wffc"), ends=VmRootDiskNotBoundError.__name__),
    # Another run holds the zone: wait for the grant, then the same path as `create`.
    "waited-for-the-lock": Scenario(fleet=lambda: fleet_of(A), drive=_behind_another),
    # Dead workers outlast the first lease: a second lease, the scan carried on.
    # (Eleven clusters at 30 s each against a 5-minute lease; the twelfth has a worker.)
    "second-lease": Scenario(
        fleet=lambda: dead_and_alive(11)[0], down=lambda fleet: set(dead_and_alive(11)[1]), outstanding=1,
        step=timedelta(seconds=10),
    ),
    # Dead workers outlast every lease: thirty-one of them against three leases.
    "leases-exhausted": Scenario(
        fleet=lambda: dead_and_alive(31)[0], down=lambda fleet: set(dead_and_alive(31)[1]), outstanding=1,
        step=timedelta(seconds=15), ends=KvZoneLeaseExpiredError.__name__,
    ),
    # The lock is never granted (nobody serves its workflow): the run asks again every two
    # minutes, then withdraws and fails at its thirty-minute bound.
    "lock-timeout": Scenario(
        fleet=lambda: fleet_of(A), lock_worker=False, outstanding=1, step=timedelta(seconds=60),
        ends=KvZoneLockTimeoutError.__name__,
    ),
    # Cancelled while its VM's disk is awaited: the VM is removed, the run ends CANCELED.
    "cancel-removes-the-vm": Scenario(fleet=lambda: fleet_of(A, storage="wffc"), drive=_cancel_in_the_disk_wait, ends="cancelled"),
    # Cancelled while waiting for the lock: it leaves the line, the run ends CANCELED.
    "cancel-in-the-line": Scenario(fleet=lambda: fleet_of(A), drive=_cancel_in_the_line, ends="cancelled"),
}


def _replayer() -> Replayer:
    return Replayer(workflows=[CreateVmWorkflow], data_converter=pydantic_data_converter)


async def _record(name: str) -> WorkflowHistory:
    scenario = SCENARIOS[name]
    fleet = scenario.fleet()
    async with Harness(fleet, down=scenario.down(fleet), lock_worker=scenario.lock_worker) as h:
        handle = await scenario.drive(h)
        waits = {"limit": 180, "step": scenario.step, "outstanding": scenario.outstanding}
        if scenario.ends is None:
            await h.result(handle, **waits)
        else:
            failed = await h.failure(handle, **waits)
            if scenario.ends == "cancelled":
                assert isinstance(failed.cause, CancelledError), failed.cause
            else:
                assert cause(failed).type == scenario.ends
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
    """No committed history protects nothing in flight; every scenario has its golden."""
    assert {path.stem for path in GOLDEN.glob("*.json")} >= set(SCENARIOS)


@pytest.mark.parametrize("path", sorted(GOLDEN.glob("*.json")), ids=lambda p: p.stem)
async def test_golden_histories_replay_on_the_current_code(path):
    recorded = json.loads(path.read_text())
    history = WorkflowHistory.from_json(recorded["workflow_id"], recorded["history"])
    await _replayer().replay_workflow(history)
