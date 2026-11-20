"""kv-zone-lock's runs in flight are guarded by REPLAY, like create-vm's (tests/test_create_vm_replay.py).

A zone's lock is ALWAYS in flight — it never completes — so every brain deploy
finds one per zone and replays it. A change to the commands it issued (a
grant's signal, the lease's timer, the continue-as-new) fails that replay,
and with it every create-vm run of the zone until someone terminates the lock.
The cure is `workflow.patched()`. Two sources of histories:

  1. histories recorded HERE, from scenarios driven by the stand-in runs of
     tests/test_kv_zone_lock_workflow.py, replayed at once;
  2. GOLDEN histories in tests/histories/kv_zone_lock/, recorded by earlier
     versions of the code and replayed on the current one. Written only with
     UPDATE_REPLAY_HISTORIES=1 and only when the file does not exist: never
     re-recorded to make a change pass.

Every scenario records with the production lease and the production
continue-as-new threshold: a history recorded under other constants would not
be one a deploy can meet.
"""

from __future__ import annotations

import json
import os
import pathlib
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, timedelta

import pytest
from temporalio.client import WorkflowHistory
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Replayer

from shared.consts import KV_ZONE_LOCK_WORKFLOW_QUEUE
from shared.models.vm_lifecycle import KvZoneLease, KvZoneLockRelease, KvZoneLockRequest, KvZoneLockRunArgs
from tests.test_add_server_to_mce_replay import _scrubbed
from tests.test_kv_zone_lock_workflow import LEASE, Harness, Waiter
from workflow_domains.vm_lifecycle import kv_zone_lock
from workflow_domains.vm_lifecycle.kv_zone_lock import KvZoneLockWorkflow

GOLDEN = pathlib.Path(__file__).parent / "histories" / "kv_zone_lock"
_RECORD = os.environ.get("UPDATE_REPLAY_HISTORIES") == "1"


async def _two_runs(h: Harness) -> str | None:
    """One holds, one waits; a release passes the lock on."""
    first = await h.waiter(hold_seconds=None)
    await h.holding(first)
    second = await h.waiter(hold_seconds=1)
    await h.queued(1)
    await first.signal(Waiter.proceed)
    await h.results(first, second)
    await h.until(lambda p: p.holder is None and p.grants == 2)
    return None


async def _lease_expiry(h: Harness) -> str | None:
    """The holder never releases in time: its lease ends, the next run is served, the late release is ignored."""
    hog = await h.waiter(hold_seconds=None)
    await h.holding(hog)
    waiting = await h.waiter(hold_seconds=None)
    await h.queued(1)
    await h.advance(LEASE + timedelta(seconds=5))
    await h.holding(waiting)
    await hog.signal(Waiter.proceed)
    await h.result(hog)
    await waiting.signal(Waiter.proceed)
    await h.result(waiting)
    await h.until(lambda p: p.holder is None and p.expired == 1)
    return None


async def _dead_waiter(h: Harness) -> str | None:
    """A waiter terminated in the line is skipped in the same task as the next grant."""
    holder = await h.waiter(hold_seconds=None)
    await h.holding(holder)
    dead = await h.waiter()
    await h.queued(1)
    alive = await h.waiter(hold_seconds=1)
    await h.queued(2)
    await dead.terminate()
    await holder.signal(Waiter.proceed)
    await h.results(holder, alive)
    await h.until(lambda p: p.holder is None and p.skipped == 1 and p.grants == 2)
    return None


async def _withdraw(h: Harness) -> str | None:
    """A waiter that gives up leaves the line; a withdraw from the holder frees the zone."""
    holder = await h.waiter(hold_seconds=None, release=False)
    held = await h.holding(holder)
    impatient = await h.waiter(give_up_after_seconds=60)
    await h.queued(1)
    patient = await h.waiter(hold_seconds=1)
    await h.queued(2)
    await h.advance(timedelta(seconds=61))
    await h.result(impatient)
    await h.queued(1)
    await h.client.get_workflow_handle(h.lock_id).signal(KvZoneLockWorkflow.withdraw, held.holder)
    await h.result(patient)
    await holder.terminate()
    await h.until(lambda p: p.holder is None and p.grants == 2)
    return None


async def _continue_as_new(h: Harness) -> str | None:
    """Enough grants for the lock to hand its history on; the FIRST run's history ends in the continue."""
    first_run: str | None = None
    for batch in range(kv_zone_lock._GRANTS_PER_RUN // 25 + 1):
        waiters = [await h.waiter(hold_seconds=0) for _ in range(25)]
        if first_run is None:
            await h.until(lambda p: p.grants >= 1)
            first_run = (await h.client.get_workflow_handle(h.lock_id).describe()).run_id
        await h.results(*waiters, limit=120)
    await h.until(lambda p: p.holder is None and p.waiting == 0 and p.grants > kv_zone_lock._GRANTS_PER_RUN)
    assert (await h.client.get_workflow_handle(h.lock_id).describe()).run_id != first_run
    return first_run


async def _continued(h: Harness) -> str | None:
    """A run started from carried state, as a continued one is: a lease in force and a line."""
    ghost = KvZoneLockRequest(workflow_id="a-run-that-holds", run_id=str(uuid.uuid4()))
    carried = await h.client.start_workflow(
        Waiter.run, {"site": f"{h.site}-elsewhere", "hold_seconds": None}, id=f"w-{uuid.uuid4()}",
        task_queue="test-kv-zone-lock-waiters",
    )
    carried_run = (await carried.describe()).run_id
    await h.client.start_workflow(
        KvZoneLockWorkflow.run,
        KvZoneLockRunArgs(
            site=h.site, zone="az1", grants=250, skipped=1, expired=1,
            line=[KvZoneLockRequest(workflow_id=carried.id, run_id=carried_run)],
            lease=KvZoneLease(lease_id="carried-lease", holder=ghost, ends_at=(await h.now()).astimezone(UTC) + timedelta(minutes=3)),
        ),
        id=h.lock_id, task_queue=KV_ZONE_LOCK_WORKFLOW_QUEUE,
    )
    await h.until(lambda p: p.lease_id == "carried-lease")
    await h.client.get_workflow_handle(h.lock_id).signal(KvZoneLockWorkflow.release, KvZoneLockRelease(lease_id="carried-lease"))
    await h.until(lambda p: p.grants == 251)
    await carried.terminate()
    await h.advance(LEASE + timedelta(seconds=5))
    await h.until(lambda p: p.holder is None and p.expired == 2)
    return None


SCENARIOS: dict[str, Callable[[Harness], Awaitable[str | None]]] = {
    "two-runs": _two_runs,
    "lease-expiry": _lease_expiry,
    "dead-waiter-skipped": _dead_waiter,
    "withdraw": _withdraw,
    "continue-as-new": _continue_as_new,
    "continued": _continued,
}


def _replayer() -> Replayer:
    return Replayer(workflows=[KvZoneLockWorkflow], data_converter=pydantic_data_converter)


async def _record(name: str) -> WorkflowHistory:
    async with Harness() as h:
        run_id = await SCENARIOS[name](h)
        # The lock never completes: its history is fetched as it stands (or, of a run that
        # continued as new, the run that ended in the continue).
        handle = h.client.get_workflow_handle(h.lock_id, run_id=run_id)
        return await handle.fetch_history()


@pytest.mark.parametrize("scenario", sorted(SCENARIOS))
async def test_every_lock_history_a_scenario_records_replays(scenario):
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
