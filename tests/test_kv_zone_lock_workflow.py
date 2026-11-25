"""kv-zone-lock: the REAL KvZoneLockWorkflow, with small test runs standing in for create-vm.

What is pinned is what the zone's safety rests on (docs/design/create-vm-placement.md §4):
one holder at a time, in arrival order; a release passes the lock on at once; a lease that
ends passes it on and a late release is ignored; a waiter that is gone costs nothing; a
run is known by its run id, so a reused workflow id never receives a grant it did not ask
for; continue-as-new loses neither the line, nor the lease, nor a request.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import BaseModel
from temporalio import workflow
from temporalio.client import WorkflowExecutionStatus, WorkflowFailureError, WorkflowHandle
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError
from temporalio.worker import UnsandboxedWorkflowRunner, Worker

from shared.consts import KV_ZONE_LOCK_WORKFLOW_QUEUE
from shared.models.vm_lifecycle import (
    KvZoneLease,
    KvZoneLockGrant,
    KvZoneLockProgress,
    KvZoneLockRelease,
    KvZoneLockRequest,
    KvZoneLockRunArgs,
)
from shared.workflow_ids import kv_zone_lock_workflow_id
from tests.manual_time import ManualTime
from workflow_domains.vm_lifecycle import kv_zone_lock
from workflow_domains.vm_lifecycle.kv_zone_lock import LOCK_GRANTED_SIGNAL, KvZoneLockWorkflow

WAITER_QUEUE = "test-kv-zone-lock-waiters"
LEASE = kv_zone_lock._LEASE


class WaiterArgs(BaseModel):
    site: str = "five"
    zone: str = "az1"
    # How long the run holds the zone once granted; None: until it is signalled `proceed`.
    hold_seconds: int | None = 1
    release: bool = True
    give_up_after_seconds: int | None = None
    ask_twice: bool = False


class WaiterResult(BaseModel):
    granted: bool
    lease_id: str | None = None
    granted_at: datetime | None = None
    lease_ends_at: datetime | None = None
    released_at: datetime | None = None
    grants_received: int = 0


@workflow.defn
class Waiter:
    """A stand-in for a create-vm run: ask, wait for the grant, hold, release."""

    def __init__(self) -> None:
        self.grants: list[KvZoneLockGrant] = []
        self.go = False

    @workflow.signal(name=LOCK_GRANTED_SIGNAL)
    def lock_granted(self, grant: KvZoneLockGrant) -> None:
        self.grants.append(grant)

    @workflow.signal
    def proceed(self) -> None:
        self.go = True

    @workflow.query
    def grants_received(self) -> int:
        return len(self.grants)

    async def _ask(self, lock_id: str, args: WaiterArgs, me: KvZoneLockRequest) -> None:
        try:
            await workflow.start_child_workflow(
                KvZoneLockWorkflow.run, KvZoneLockRunArgs(site=args.site, zone=args.zone, line=[me]), id=lock_id,
                task_queue=KV_ZONE_LOCK_WORKFLOW_QUEUE, parent_close_policy=workflow.ParentClosePolicy.ABANDON,
            )
        except WorkflowAlreadyStartedError:
            await workflow.get_external_workflow_handle(lock_id).signal(KvZoneLockWorkflow.request_lock, me)

    @workflow.run
    async def run(self, args: WaiterArgs) -> WaiterResult:
        info = workflow.info()
        me = KvZoneLockRequest(workflow_id=info.workflow_id, run_id=info.run_id)
        lock_id = kv_zone_lock_workflow_id(args.site, args.zone)
        lock = workflow.get_external_workflow_handle(lock_id)
        await self._ask(lock_id, args, me)
        if args.ask_twice:
            await self._ask(lock_id, args, me)
        try:
            await workflow.wait_condition(
                lambda: bool(self.grants),
                timeout=timedelta(seconds=args.give_up_after_seconds) if args.give_up_after_seconds else None,
            )
        except asyncio.TimeoutError:
            await lock.signal(KvZoneLockWorkflow.withdraw, me)
            return WaiterResult(granted=False)
        grant = self.grants[0]
        granted_at = workflow.now()
        try:
            await workflow.wait_condition(
                lambda: self.go, timeout=timedelta(seconds=args.hold_seconds) if args.hold_seconds is not None else None
            )
        except asyncio.TimeoutError:
            pass
        if args.release:
            await lock.signal(KvZoneLockWorkflow.release, KvZoneLockRelease(lease_id=grant.lease_id))
        return WaiterResult(
            granted=True, lease_id=grant.lease_id, granted_at=granted_at, lease_ends_at=grant.lease_ends_at,
            released_at=workflow.now() if args.release else None, grants_received=len(self.grants),
        )


class Harness(ManualTime):
    """The real lock workflow, the stand-in runs, and a clock that moves only when told (tests/manual_time.py)."""

    def __init__(self, *, unsandboxed_lock: bool = False) -> None:
        self._unsandboxed_lock = unsandboxed_lock
        self.site = f"s{uuid.uuid4().hex[:8]}"  # a zone of its own per test: lock ids never collide

    async def __aenter__(self) -> Harness:
        await super().__aenter__()
        lock_runner = {"workflow_runner": UnsandboxedWorkflowRunner()} if self._unsandboxed_lock else {}
        self._lock_worker = Worker(self.client, task_queue=KV_ZONE_LOCK_WORKFLOW_QUEUE, workflows=[KvZoneLockWorkflow], **lock_runner)
        self._waiters = Worker(self.client, task_queue=WAITER_QUEUE, workflows=[Waiter], workflow_runner=UnsandboxedWorkflowRunner())
        await self._lock_worker.__aenter__()
        await self._waiters.__aenter__()
        return self

    async def __aexit__(self, *exc) -> None:
        await self._waiters.__aexit__(*exc)
        await self._lock_worker.__aexit__(*exc)
        await super().__aexit__(*exc)

    @property
    def lock_id(self) -> str:
        return kv_zone_lock_workflow_id(self.site, "az1")

    async def waiter(self, name: str | None = None, **args) -> WorkflowHandle:
        return await self.client.start_workflow(
            Waiter.run, WaiterArgs(site=self.site, **args), id=name or f"waiter-{uuid.uuid4()}", task_queue=WAITER_QUEUE,
        )

    async def lock_progress(self, zone: str = "az1") -> KvZoneLockProgress:
        return await self.client.get_workflow_handle(kv_zone_lock_workflow_id(self.site, zone)).query(KvZoneLockWorkflow.progress)

    async def until(self, condition, *, tries: int = 200) -> KvZoneLockProgress:
        """Poll the lock's progress until `condition(progress)`; a real-time wait, bounded."""
        progress = None
        for _ in range(tries):
            try:
                progress = await self.lock_progress()
            except Exception:  # noqa: BLE001 — the lock may not be started yet, or is between two runs
                progress = None
            if progress is not None and condition(progress):
                return progress
            await asyncio.sleep(0.05)
        raise AssertionError(f"the lock never reached the state waited for; last: {progress}")

    async def holding(self, handle: WorkflowHandle) -> KvZoneLockProgress:
        return await self.until(lambda p: p.holder is not None and p.holder.workflow_id == handle.id)

    async def queued(self, count: int) -> KvZoneLockProgress:
        return await self.until(lambda p: p.waiting == count)


# "At once": far inside a lease, with room for a slow machine nudging the clock a few extra seconds.
AT_ONCE = timedelta(seconds=60)


async def test_the_first_run_starts_the_lock_and_is_granted_at_once():
    async with Harness() as h:
        granted = await h.result(await h.waiter(hold_seconds=1))
        assert granted.granted and granted.lease_ends_at - granted.granted_at <= LEASE
        assert granted.lease_ends_at - granted.granted_at > LEASE - AT_ONCE
        progress = await h.until(lambda p: p.holder is None)
        assert (progress.grants, progress.waiting, progress.phase, progress.site, progress.zone) == (1, 0, "idle", h.site, "az1")


async def test_the_lock_outlives_the_run_that_started_it():
    async with Harness() as h:
        await h.result(await h.waiter())
        description = await h.client.get_workflow_handle(h.lock_id).describe()
        assert description.status == WorkflowExecutionStatus.RUNNING
        # …and a second run meets it running: it signals, and is served by the SAME lock.
        await h.result(await h.waiter())
        assert (await h.lock_progress()).grants == 2
        assert (await h.client.get_workflow_handle(h.lock_id).describe()).run_id == description.run_id


async def test_an_idle_lock_just_waits():
    async with Harness() as h:
        await h.result(await h.waiter())
        await h.advance(timedelta(days=30))
        assert (await h.client.get_workflow_handle(h.lock_id).describe()).status == WorkflowExecutionStatus.RUNNING
        assert (await h.result(await h.waiter())).granted


async def test_runs_are_granted_one_at_a_time_in_arrival_order():
    async with Harness() as h:
        first = await h.waiter(hold_seconds=None)  # holds until told
        await h.holding(first)
        others = []
        for _ in range(4):
            others.append(await h.waiter(hold_seconds=2))
            await h.queued(len(others))  # each is in the line before the next asks
        progress = await h.lock_progress()
        assert [waiting.workflow_id for waiting in progress.line] == [w.id for w in others]
        await h.advance(timedelta(seconds=10))
        await first.signal(Waiter.proceed)
        outcomes = await h.results(first, *others)
        for earlier, later in zip(outcomes, outcomes[1:], strict=False):
            assert later.granted_at >= earlier.released_at, "two runs held the zone at once"
            assert later.granted_at > earlier.granted_at
        assert len({o.lease_id for o in outcomes}) == 5
        assert all(o.grants_received == 1 for o in outcomes)
        assert (await h.lock_progress()).grants == 5


async def test_a_release_passes_the_lock_on_at_once():
    async with Harness() as h:
        first = await h.waiter(hold_seconds=10)
        await h.holding(first)
        second = await h.waiter(hold_seconds=1)
        a, b = await h.results(first, second)
        waited = b.granted_at - a.released_at
        assert timedelta(0) <= waited < AT_ONCE, f"the next run waited {waited} after the release"


async def test_a_lease_that_ends_passes_the_lock_on_and_a_late_release_is_ignored():
    async with Harness() as h:
        hog = await h.waiter(hold_seconds=None)  # holds past its lease, and releases only when told
        granted = await h.holding(hog)
        waiting = await h.waiter(hold_seconds=None)
        await h.queued(1)
        # Just short of the lease's end the zone is still the hog's…
        await h.advance(LEASE - timedelta(seconds=30))
        assert (await h.lock_progress()).holder.workflow_id == hog.id
        # …and at its end the lock passes on, without any release.
        await h.advance(timedelta(seconds=40))
        progress = await h.holding(waiting)
        assert (progress.expired, progress.grants) == (1, 2) and progress.lease_id != granted.lease_id
        # The hog's release, minutes late, carries the OLD lease id: ignored.
        await h.advance(timedelta(minutes=2))
        await hog.signal(Waiter.proceed)
        late = await h.result(hog)
        assert late.released_at > late.lease_ends_at and late.lease_id == granted.lease_id
        still = await h.lock_progress()
        assert still.holder.workflow_id == waiting.id and still.lease_id == progress.lease_id
        await waiting.signal(Waiter.proceed)
        second = await h.result(waiting)
        assert second.granted_at >= late.lease_ends_at
        assert (await h.until(lambda p: p.holder is None)).expired == 1


async def test_a_waiter_that_was_terminated_is_skipped_at_once():
    async with Harness() as h:
        holder = await h.waiter(hold_seconds=None)
        await h.holding(holder)
        dead = await h.waiter()
        await h.queued(1)
        alive = await h.waiter(hold_seconds=1)
        await h.queued(2)
        await dead.terminate()
        await holder.signal(Waiter.proceed)
        released, served = await h.results(holder, alive)
        # No five-minute gap for the dead waiter's turn: the live one is granted straight after the release.
        assert served.granted_at - released.released_at < AT_ONCE
        progress = await h.lock_progress()
        assert (progress.skipped, progress.grants, progress.expired) == (1, 2, 0)


async def test_several_dead_waiters_in_a_row_cost_nothing():
    async with Harness() as h:
        holder = await h.waiter(hold_seconds=None)
        await h.holding(holder)
        dead = []
        for count in range(1, 6):
            dead.append(await h.waiter())
            await h.queued(count)
        alive = await h.waiter(hold_seconds=1)
        await h.queued(6)
        for waiter in dead:
            await waiter.terminate()
        await holder.signal(Waiter.proceed)
        released, served = await h.results(holder, alive)
        assert served.granted_at - released.released_at < AT_ONCE
        assert (await h.lock_progress()).skipped == 5


async def test_a_run_that_gives_up_takes_itself_out_of_the_line():
    async with Harness() as h:
        holder = await h.waiter(hold_seconds=None)
        await h.holding(holder)
        impatient = await h.waiter(give_up_after_seconds=60)
        await h.queued(1)
        patient = await h.waiter(hold_seconds=1)
        await h.queued(2)
        await h.advance(timedelta(seconds=61))
        gave_up = await h.result(impatient)
        assert gave_up.granted is False
        progress = await h.queued(1)
        assert [waiting.workflow_id for waiting in progress.line] == [patient.id]
        await holder.signal(Waiter.proceed)
        _, served = await h.results(holder, patient)
        assert served.granted
        final = await h.lock_progress()
        assert (final.grants, final.skipped) == (2, 0)  # the one who left was never even tried
        assert await impatient.query(Waiter.grants_received) == 0


async def test_a_withdraw_from_the_holder_frees_the_zone():
    async with Harness() as h:
        holder = await h.waiter(hold_seconds=None, release=False)
        progress = await h.holding(holder)
        waiting = await h.waiter(hold_seconds=1)
        await h.queued(1)
        await h.client.get_workflow_handle(h.lock_id).signal(KvZoneLockWorkflow.withdraw, progress.holder)
        served = await h.result(waiting)
        assert served.granted and (await h.lock_progress()).expired == 0
        await holder.terminate()


async def test_asking_again_while_waiting_or_holding_adds_no_second_place():
    async with Harness() as h:
        holder = await h.waiter(hold_seconds=None, ask_twice=True)
        progress = await h.holding(holder)
        await asyncio.sleep(0.3)
        assert (await h.lock_progress()).waiting == 0  # the holder's second ask did not put it back in the line
        waiting = await h.waiter(hold_seconds=1, ask_twice=True)
        progress = await h.until(lambda p: p.waiting >= 1)
        await asyncio.sleep(0.3)
        assert (await h.lock_progress()).waiting == 1
        # A third ask, from outside, for the one already waiting.
        await h.client.get_workflow_handle(h.lock_id).signal(KvZoneLockWorkflow.request_lock, progress.line[0])
        assert (await h.lock_progress()).waiting == 1
        await holder.signal(Waiter.proceed)
        first, second = await h.results(holder, waiting)
        assert (first.grants_received, second.grants_received) == (1, 1)
        assert (await h.lock_progress()).grants == 2


async def test_a_reused_workflow_id_never_receives_the_grant_of_the_run_before_it():
    """A slot's id is reused: the grant is signalled to the RUN that asked, and to no other."""
    async with Harness() as h:
        holder = await h.waiter(hold_seconds=None)
        await h.holding(holder)
        slot = f"create-vm-slot-{uuid.uuid4()}"
        first_run = await h.waiter(slot)
        await h.queued(1)
        await first_run.terminate()
        second_run = await h.waiter(slot, hold_seconds=1)  # the same id, a NEW run, asks for itself
        progress = await h.queued(2)
        assert [w.workflow_id for w in progress.line] == [slot, slot]
        assert progress.line[0].run_id != progress.line[1].run_id
        await holder.signal(Waiter.proceed)
        _, served = await h.results(holder, second_run)
        assert served.grants_received == 1  # its own, not also the dead run's
        final = await h.lock_progress()
        assert (final.skipped, final.grants) == (1, 2)


async def test_two_zones_never_wait_for_each_other():
    async with Harness() as h:
        hog = await h.waiter(hold_seconds=None)
        await h.holding(hog)
        other_zone = await h.client.start_workflow(
            Waiter.run, WaiterArgs(site=h.site, zone="az2", hold_seconds=1), id=f"waiter-{uuid.uuid4()}", task_queue=WAITER_QUEUE,
        )
        other_site = await h.client.start_workflow(
            Waiter.run, WaiterArgs(site=f"{h.site}x", zone="az1", hold_seconds=1), id=f"waiter-{uuid.uuid4()}", task_queue=WAITER_QUEUE,
        )
        in_zone, in_site = await h.results(other_zone, other_site)
        assert in_zone.granted and in_site.granted
        assert (await h.lock_progress("az2")).grants == 1
        still = await h.lock_progress()
        assert still.holder is not None and still.holder.workflow_id == hog.id
        await hog.terminate()


async def test_the_lock_is_not_cancelled_with_the_run_that_started_it():
    async with Harness() as h:
        starter = await h.waiter(hold_seconds=None)
        await h.holding(starter)
        await starter.cancel()
        with pytest.raises(WorkflowFailureError):
            await h.result(starter)
        description = await h.client.get_workflow_handle(h.lock_id).describe()
        assert description.status == WorkflowExecutionStatus.RUNNING
        # A stand-in releases nothing when cancelled: its lease ends on its own, then the next run is served.
        waiting = await h.waiter(hold_seconds=1)
        await h.queued(1)
        await h.advance(LEASE + timedelta(seconds=5))
        served = await h.result(waiting)
        assert served.granted and (await h.lock_progress()).expired == 1


async def test_a_terminated_lock_is_started_afresh_by_the_next_run():
    async with Harness() as h:
        await h.result(await h.waiter())
        first = await h.client.get_workflow_handle(h.lock_id).describe()
        await h.client.get_workflow_handle(h.lock_id).terminate()
        served = await h.result(await h.waiter())
        assert served.granted
        again = await h.client.get_workflow_handle(h.lock_id).describe()
        assert again.run_id != first.run_id and again.status == WorkflowExecutionStatus.RUNNING
        assert (await h.lock_progress()).grants == 1  # a new lock: it remembers nothing, and needs nothing


class TestContinueAsNew:
    async def test_the_history_is_handed_on_and_nothing_is_lost(self, monkeypatch):
        monkeypatch.setattr(kv_zone_lock, "_GRANTS_PER_RUN", 3)
        async with Harness(unsandboxed_lock=True) as h:
            first = await h.waiter(hold_seconds=None)
            await h.holding(first)
            first_run_id = (await h.client.get_workflow_handle(h.lock_id).describe()).run_id
            waiters = []
            for count in range(1, 11):
                waiters.append(await h.waiter(hold_seconds=1))
                await h.queued(count)
            await first.signal(Waiter.proceed)
            outcomes = await h.results(first, *waiters)
            for earlier, later in zip(outcomes, outcomes[1:], strict=False):
                assert later.granted_at >= earlier.released_at
            assert all(o.granted and o.grants_received == 1 for o in outcomes)
            progress = await h.until(lambda p: p.holder is None and p.waiting == 0)
            assert (progress.grants, progress.skipped, progress.expired) == (11, 0, 0)  # counted across the runs
            last = await h.client.get_workflow_handle(h.lock_id).describe()
            assert last.run_id != first_run_id and last.status == WorkflowExecutionStatus.RUNNING

    async def test_requests_arriving_while_the_lock_continues_are_all_served(self, monkeypatch):
        monkeypatch.setattr(kv_zone_lock, "_GRANTS_PER_RUN", 2)
        async with Harness(unsandboxed_lock=True) as h:
            handles = await asyncio.gather(*(h.waiter(hold_seconds=1) for _ in range(24)))
            outcomes = await h.results(*handles, limit=120)
            assert all(o.granted and o.grants_received == 1 for o in outcomes)
            spans = sorted((o.granted_at, o.released_at) for o in outcomes)
            for (_, earlier_released), (later_granted, _) in zip(spans, spans[1:], strict=False):
                assert later_granted >= earlier_released, "two runs held the zone at once"
            progress = await h.until(lambda p: p.holder is None and p.waiting == 0)
            assert (progress.grants, progress.expired) == (24, 0)

    async def test_a_continued_lock_carries_the_line_and_the_lease_in_force(self):
        """Started straight from carried state, exactly as a continued run is."""
        async with Harness() as h:
            ghost = KvZoneLockRequest(workflow_id="a-run-that-holds", run_id=str(uuid.uuid4()))
            # A real run whose place in the line is carried over (it has not asked this lock run itself).
            queued = await h.client.start_workflow(
                Waiter.run, WaiterArgs(site=f"{h.site}-elsewhere", hold_seconds=None), id=f"w-{uuid.uuid4()}", task_queue=WAITER_QUEUE)
            queued_run = (await queued.describe()).run_id
            lease_end = (await h.now()).astimezone(UTC) + timedelta(minutes=3)
            await h.client.start_workflow(
                KvZoneLockWorkflow.run,
                KvZoneLockRunArgs(
                    site=h.site, zone="az1", grants=41, skipped=2, expired=1,
                    line=[KvZoneLockRequest(workflow_id=queued.id, run_id=queued_run)],
                    lease=KvZoneLease(lease_id="carried-lease", holder=ghost, ends_at=lease_end),
                ),
                id=h.lock_id, task_queue=KV_ZONE_LOCK_WORKFLOW_QUEUE,
            )
            progress = await h.until(lambda p: p.lease_id == "carried-lease")
            assert (progress.grants, progress.skipped, progress.expired, progress.waiting) == (41, 2, 1, 1)
            assert progress.holder == ghost and progress.phase == "held"
            # The carried lease still holds the zone: the carried waiter is not granted yet…
            await h.advance(timedelta(minutes=1))
            assert (await h.lock_progress()).lease_id == "carried-lease"
            # …and its release is honoured by the continued run, which then serves the carried line.
            await h.client.get_workflow_handle(h.lock_id).signal(KvZoneLockWorkflow.release, KvZoneLockRelease(lease_id="carried-lease"))
            after = await h.until(lambda p: p.holder is not None and p.holder.workflow_id == queued.id and p.grants == 42)
            assert (after.expired, after.waiting) == (1, 0)
            assert await queued.query(Waiter.grants_received) == 2  # its own lock's, and this carried place's
            await queued.terminate()

    async def test_a_carried_lease_ends_when_it_was_due_not_five_minutes_after_the_continue(self):
        async with Harness() as h:
            ghost = KvZoneLockRequest(workflow_id="a-run-that-held", run_id=str(uuid.uuid4()))
            lease_end = (await h.now()).astimezone(UTC) + timedelta(seconds=90)
            await h.client.start_workflow(
                KvZoneLockWorkflow.run,
                KvZoneLockRunArgs(site=h.site, zone="az1", lease=KvZoneLease(lease_id="carried", holder=ghost, ends_at=lease_end)),
                id=h.lock_id, task_queue=KV_ZONE_LOCK_WORKFLOW_QUEUE,
            )
            await h.until(lambda p: p.lease_id == "carried")
            await h.advance(timedelta(seconds=60))
            assert (await h.lock_progress()).lease_id == "carried"
            await h.advance(timedelta(seconds=40))
            assert (await h.until(lambda p: p.holder is None)).expired == 1

    async def test_a_carried_lease_that_already_ended_is_dropped_at_once(self):
        async with Harness() as h:
            ghost = KvZoneLockRequest(workflow_id="a-run-that-held", run_id=str(uuid.uuid4()))
            await h.client.start_workflow(
                KvZoneLockWorkflow.run,
                KvZoneLockRunArgs(site=h.site, zone="az1", lease=KvZoneLease(
                    lease_id="stale", holder=ghost, ends_at=(await h.now()).astimezone(UTC) - timedelta(minutes=1))),
                id=h.lock_id, task_queue=KV_ZONE_LOCK_WORKFLOW_QUEUE,
            )
            progress = await h.until(lambda p: p.holder is None)
            assert progress.expired == 1
            assert (await h.result(await h.waiter(hold_seconds=1))).granted


# --- signals nobody should have sent ---------------------------------------------------------------


class TestStraySignals:
    """What reaches the lock late, twice, or from a run it does not know must never move the zone
    from the run that holds it: a release frees only ITS lease, a withdraw only ITS run."""

    async def test_a_release_or_withdraw_from_a_stranger_frees_nothing(self):
        async with Harness() as h:
            holder = await h.waiter(hold_seconds=None)
            held = await h.holding(holder)
            lock = h.client.get_workflow_handle(h.lock_id)
            stranger = KvZoneLockRequest(workflow_id=f"never-asked-{uuid.uuid4()}", run_id=str(uuid.uuid4()))
            await lock.signal(KvZoneLockWorkflow.release, KvZoneLockRelease(lease_id=str(uuid.uuid4())))
            await lock.signal(KvZoneLockWorkflow.withdraw, stranger)
            # The holder's own workflow id under ANOTHER run id is a stranger too (a slot's earlier run).
            await lock.signal(KvZoneLockWorkflow.withdraw, KvZoneLockRequest(workflow_id=holder.id, run_id=str(uuid.uuid4())))
            await h.advance(timedelta(seconds=30))
            still = await h.lock_progress()
            assert still.holder == held.holder and still.lease_id == held.lease_id and still.waiting == 0
            await holder.signal(Waiter.proceed)
            assert (await h.result(holder)).granted
            assert (await h.until(lambda p: p.holder is None)).expired == 0

    async def test_a_release_sent_twice_never_frees_the_next_holders_lease(self):
        async with Harness() as h:
            first = await h.waiter(hold_seconds=None)
            first_lease = (await h.holding(first)).lease_id
            second = await h.waiter(hold_seconds=None)
            await h.queued(1)
            await first.signal(Waiter.proceed)
            await h.result(first)
            held = await h.holding(second)
            assert held.lease_id != first_lease
            lock = h.client.get_workflow_handle(h.lock_id)
            # The first run's release again (a retried signal), and its withdraw, minutes later.
            await lock.signal(KvZoneLockWorkflow.release, KvZoneLockRelease(lease_id=first_lease))
            await h.advance(timedelta(minutes=2))
            await lock.signal(KvZoneLockWorkflow.release, KvZoneLockRelease(lease_id=first_lease))
            still = await h.lock_progress()
            assert still.holder.workflow_id == second.id and still.lease_id == held.lease_id
            await second.signal(Waiter.proceed)
            assert (await h.result(second)).granted
            assert (await h.until(lambda p: p.holder is None)).grants == 2

    async def test_a_late_withdraw_from_the_run_before_does_not_free_the_run_that_holds_now(self):
        async with Harness() as h:
            first = await h.waiter(hold_seconds=None)
            first_request = (await h.holding(first)).holder
            second = await h.waiter(hold_seconds=None)
            await h.queued(1)
            await first.signal(Waiter.proceed)
            await h.result(first)
            held = await h.holding(second)
            await h.client.get_workflow_handle(h.lock_id).signal(KvZoneLockWorkflow.withdraw, first_request)
            await h.advance(timedelta(seconds=30))
            still = await h.lock_progress()
            assert still.holder.workflow_id == second.id and still.lease_id == held.lease_id
            await second.terminate()

    async def test_a_run_that_asks_again_after_its_release_goes_to_the_back_of_the_line(self):
        """A create-vm run whose lease ran out mid-scan asks for a second one: it gets no place
        ahead of the runs that were already waiting."""
        async with Harness() as h:
            first = await h.waiter(hold_seconds=None)
            first_request = (await h.holding(first)).holder
            second = await h.waiter(hold_seconds=None)
            await h.queued(1)
            third = await h.waiter(hold_seconds=None)
            await h.queued(2)
            lock = h.client.get_workflow_handle(h.lock_id)
            # Released, then asked again — while it holds, the same ask is ignored.
            await lock.signal(KvZoneLockWorkflow.request_lock, first_request)
            assert (await h.lock_progress()).waiting == 2
            await lock.signal(KvZoneLockWorkflow.release, KvZoneLockRelease(lease_id=(await h.lock_progress()).lease_id))
            await lock.signal(KvZoneLockWorkflow.request_lock, first_request)
            progress = await h.until(lambda p: p.holder is not None and p.holder.workflow_id == second.id and p.waiting == 2)
            assert [waiting.workflow_id for waiting in progress.line] == [third.id, first.id]
            for handle in (first, second, third):
                await handle.terminate()

    async def test_a_request_delivered_with_the_locks_first_task_is_not_lost(self):
        """signal-with-start, as a client would do it: the signal is handled BEFORE run() starts,
        and must find the line the lock was started with."""
        async with Harness() as h:
            starter = KvZoneLockRequest(workflow_id=f"gone-{uuid.uuid4()}", run_id=str(uuid.uuid4()))
            waiter = await h.waiter(hold_seconds=None)  # starts the lock itself, or signals it
            await h.holding(waiter)
            await waiter.terminate()
            await h.client.get_workflow_handle(h.lock_id).terminate()
            other = await h.client.start_workflow(Waiter.run, WaiterArgs(site=f"{h.site}y", hold_seconds=None),
                                                  id=f"waiter-{uuid.uuid4()}", task_queue=WAITER_QUEUE)
            other_lock = kv_zone_lock_workflow_id(f"{h.site}y", "az1")
            for _ in range(100):
                try:
                    held = await h.client.get_workflow_handle(other_lock).query(KvZoneLockWorkflow.progress)
                    if held.holder is not None:
                        break
                except Exception:  # noqa: BLE001 — not started yet
                    pass
                await asyncio.sleep(0.05)
            # A lock started WITH a line and signalled in the same breath: both requests are kept, in order.
            await h.client.start_workflow(
                KvZoneLockWorkflow.run, KvZoneLockRunArgs(site=h.site, zone="az1", line=[starter]), id=h.lock_id,
                task_queue=KV_ZONE_LOCK_WORKFLOW_QUEUE, start_signal="request_lock",
                start_signal_args=[KvZoneLockRequest(workflow_id=other.id, run_id=other.first_execution_run_id)],
            )
            # `starter` never existed: skipped. `other` exists and is granted — although it never asked this zone.
            progress = await h.until(lambda p: p.skipped == 1 and p.grants == 1)
            assert progress.holder.workflow_id == other.id
            await other.terminate()


# --- many runs, every way a run can behave ---------------------------------------------------------


class TestAStormOfRuns:
    async def test_forty_runs_asking_in_the_same_instant_are_each_served_once(self):
        """Forty runs race to START the lock; one wins and thirty-nine signal it."""
        async with Harness() as h:
            handles = await asyncio.gather(*(h.waiter(hold_seconds=0) for _ in range(40)))
            outcomes = await h.results(*handles, step=timedelta(seconds=2), limit=120)
            assert all(o.granted and o.grants_received == 1 for o in outcomes)
            assert len({o.lease_id for o in outcomes}) == 40
            ordered = sorted(outcomes, key=lambda o: o.granted_at)
            for earlier, later in zip(ordered, ordered[1:], strict=False):
                assert later.granted_at >= earlier.released_at, "two runs held the zone at once"
            final = await h.until(lambda p: p.holder is None and p.waiting == 0)
            assert (final.grants, final.skipped, final.expired) == (40, 0, 0)

    @pytest.mark.parametrize("seed", [1, 7, 20261124])
    async def test_whatever_the_runs_do_only_one_holds_the_zone_at_a_time_and_every_patient_run_is_served(self, seed):
        """Runs that release, runs that hold past their lease and never release, runs that give
        up, runs terminated in the line or while holding, runs that ask twice — all at once.
        Whatever the mix: no two leases overlap, nobody is granted twice, every run that waits
        is served, and the lock ends idle with an empty line."""
        import random

        rng = random.Random(seed)
        async with Harness() as h:
            kinds: list[str] = []
            handles: list[WorkflowHandle] = []
            for _ in range(24):
                kind = rng.choice(["release", "release", "release", "hog", "give-up", "terminated", "twice"])
                kinds.append(kind)
                if kind == "hog":
                    args = {"hold_seconds": 400, "release": False}
                elif kind == "give-up":
                    args = {"hold_seconds": rng.randrange(0, 4), "give_up_after_seconds": rng.randrange(1, 240)}
                elif kind == "twice":
                    args = {"hold_seconds": rng.randrange(0, 4), "ask_twice": True}
                else:
                    args = {"hold_seconds": rng.randrange(0, 20)}
                handles.append(await h.waiter(**args))
                if rng.random() < 0.3:
                    await h.advance(timedelta(seconds=rng.randrange(1, 30)))
            for kind, handle in zip(kinds, handles, strict=True):
                if kind == "terminated":
                    try:
                        await handle.terminate()
                    except RPCError:
                        pass  # it was served and done before anyone got to terminating it
                    if rng.random() < 0.5:
                        await h.advance(timedelta(seconds=rng.randrange(1, 90)))
            alive = [(kind, handle) for kind, handle in zip(kinds, handles, strict=True) if kind != "terminated"]
            outcomes = await h.results(*(handle for _, handle in alive), step=timedelta(seconds=20), limit=240)
            held = []
            for (kind, _), outcome in zip(alive, outcomes, strict=True):
                if kind != "give-up":
                    assert outcome.granted, f"a {kind} run was never served"
                if outcome.granted:
                    assert outcome.grants_received == 1
                    # A run that did not release held the zone to its lease's end, and no longer.
                    held.append((outcome.granted_at, outcome.released_at or outcome.lease_ends_at, kind))
            held.sort()
            for (_, until, kind), (granted_at, _, next_kind) in zip(held, held[1:], strict=False):
                assert granted_at >= until, f"a {next_kind} run was granted the zone while a {kind} run still held it"
            assert len({outcome.lease_id for outcome in outcomes if outcome.granted}) == len(held)
            final = await h.until(lambda p: p.holder is None and p.waiting == 0)
            hogs = sum(1 for kind, _ in alive if kind == "hog")
            assert final.expired >= hogs  # each hog's lease ended on its own (a terminated holder's too)
            assert final.grants >= len(held)
