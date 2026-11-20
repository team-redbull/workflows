"""kv-zone-lock — ONE long-lived run per (site, zone) that hands the zone to one create-vm run at a time.

WHY A LOCK. A create-vm run reads the zone's KV clusters' figures and picks
one. Ten runs reading at the same moment all see the same empty cluster and
all pick it, even if it has room for two (seen in the lab, 2026-11-05). So
only the run holding its zone's lock may read figures and choose, and it
keeps the lock until its VM OBJECT EXISTS on the chosen cluster. The next
run's figures then already count that VM — a stopped VM, and stopped VMs are
counted — so NOTHING IS REMEMBERED BETWEEN RUNS: this workflow holds only the
line of runs waiting and the lease in force. It never reads a cluster and
never chooses (docs/design/create-vm-placement.md §4).

No route starts it: a create-vm run starts it as an ABANDONed child on
`kv_zone_lock_workflow_id(site, zone)` with its own request as the first of
the line, or, when it already runs, signals it (create_vm.py). It never
completes on its own.

  request_lock  run -> lock. Appended to the line, once: a run already waiting
                or already holding is not added again, so a run may ask again
                while it waits (create-vm does, which is what brings a waiter
                back if a person ever terminates this workflow).
  lock_granted  lock -> run, to the head of the line, in arrival order, with a
                lease id and the lease's end. A run is known by its workflow
                id AND its run id — a slot's workflow id is reused by later
                runs — and the grant is signalled to exactly that run.
  release       run -> lock, with the lease id. The lock passes to the next
                run. A release carrying another lease's id is ignored.
  withdraw      run -> lock: a run that gives up leaves the line; from the
                holder it frees the lease.

THE LEASE. Each holder has the zone for at most _LEASE (5 minutes, a durable
timer). A holder that died never releases; when its lease ends the lock
passes on, and its late release is then ignored. The holder passes the
lease's end with its VM create, and the cluster's worker creates nothing
after it — so a create that was held up cannot land on a cluster the next
run has already measured without it.

A WAITER THAT IS GONE COSTS NOTHING. A waiting run can be terminated, or give
up. Signalling a closed run fails (an ApplicationError,
`ExternalWorkflowExecutionNotFound`, and the same for a run id whose workflow
id a later run now uses): the grant is skipped and the next run granted in
the same workflow task, so a dead waiter never costs the zone a lease.

HISTORY stays bounded: `continue_as_new` every _GRANTS_PER_RUN grants, or
when the server suggests it, carrying the line and the lease in force, after
`all_handlers_finished`. A signal arriving during the continue-as-new task
makes the server refuse that completion, so none is lost; every signal is
sent to the workflow ID, never a run id, so it reaches the continued run.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from shared.models.vm_lifecycle import (
        KvZoneLease,
        KvZoneLockGrant,
        KvZoneLockProgress,
        KvZoneLockRelease,
        KvZoneLockRequest,
        KvZoneLockRunArgs,
    )

# The signal a create-vm run receives its grant by. Named here, not through
# CreateVmWorkflow's method: create_vm.py imports this module, never the reverse.
LOCK_GRANTED_SIGNAL = "lock_granted"
# How long one holder has the zone. Changing it is a non-deterministic change
# for a lock workflow in flight (a durable timer's length).
_LEASE = timedelta(minutes=5)
# Grants before the history is handed to a fresh run.
_GRANTS_PER_RUN = 250


@workflow.defn
class KvZoneLockWorkflow:
    @workflow.init
    def __init__(self, run_args: KvZoneLockRunArgs) -> None:
        # State is set here, not in run(): a signal delivered with the first
        # workflow task is handled BEFORE run() starts, and must find the line.
        self._site = run_args.site
        self._zone = run_args.zone
        self._line: list[KvZoneLockRequest] = list(run_args.line)
        self._lease: KvZoneLease | None = run_args.lease
        self._grants = run_args.grants
        self._skipped = run_args.skipped
        self._expired = run_args.expired
        self._grants_this_run = 0
        self._phase = "idle"

    # --- signals ---------------------------------------------------------------------

    def _holds(self, request: KvZoneLockRequest) -> bool:
        return self._lease is not None and self._lease.holder == request

    @workflow.signal
    def request_lock(self, request: KvZoneLockRequest) -> None:
        if self._holds(request) or request in self._line:
            return
        self._line.append(request)

    @workflow.signal
    def release(self, release: KvZoneLockRelease) -> None:
        if self._lease is not None and self._lease.lease_id == release.lease_id:
            self._lease = None

    @workflow.signal
    def withdraw(self, request: KvZoneLockRequest) -> None:
        self._line = [waiting for waiting in self._line if waiting != request]
        if self._holds(request):
            self._lease = None

    @workflow.query
    def progress(self) -> KvZoneLockProgress:
        return KvZoneLockProgress(
            phase=self._phase, site=self._site, zone=self._zone, waiting=len(self._line), line=list(self._line),
            holder=self._lease.holder if self._lease else None,
            lease_id=self._lease.lease_id if self._lease else None,
            lease_ends_at=self._lease.ends_at if self._lease else None,
            grants=self._grants, skipped=self._skipped, expired=self._expired,
        )

    # --- the loop --------------------------------------------------------------------

    @workflow.run
    async def run(self, run_args: KvZoneLockRunArgs) -> None:
        while True:
            if self._grants_this_run >= _GRANTS_PER_RUN or workflow.info().is_continue_as_new_suggested():
                await workflow.wait_condition(workflow.all_handlers_finished)
                workflow.continue_as_new(
                    KvZoneLockRunArgs(
                        site=self._site, zone=self._zone, line=list(self._line), lease=self._lease,
                        grants=self._grants, skipped=self._skipped, expired=self._expired,
                    )
                )
            if self._lease is not None:
                await self._until_released_or_expired(self._lease)
                continue
            if not self._line:
                self._phase = "idle"
                await workflow.wait_condition(lambda: bool(self._line))
                continue
            await self._grant(self._line.pop(0))

    async def _until_released_or_expired(self, lease: KvZoneLease) -> None:
        """Wait for the holder's release, or the lease's end — whichever comes first."""
        self._phase = "held"
        remaining = lease.ends_at - workflow.now()
        if remaining > timedelta(0):
            try:
                await workflow.wait_condition(lambda: self._lease is not lease, timeout=remaining)
                return
            except asyncio.TimeoutError:
                pass
        if self._lease is lease:  # not released: the holder died, hung, or is slow — the zone moves on
            self._expired += 1
            self._lease = None
            workflow.logger.warning(
                "zone %s/%s: the lease of %s (run %s) ended unreleased; the lock passes on",
                self._site, self._zone, lease.holder.workflow_id, lease.holder.run_id,
            )

    async def _grant(self, request: KvZoneLockRequest) -> None:
        """Hand the zone to the head of the line; a waiter that is gone is skipped at once."""
        self._phase = "granting"
        lease = KvZoneLease(lease_id=str(workflow.uuid4()), holder=request, ends_at=workflow.now() + _LEASE)
        # In force BEFORE the signal is sent: the holder's release may be the
        # very next thing this workflow hears.
        self._lease = lease
        try:
            await workflow.get_external_workflow_handle(request.workflow_id, run_id=request.run_id).signal(
                LOCK_GRANTED_SIGNAL, KvZoneLockGrant(lease_id=lease.lease_id, lease_ends_at=lease.ends_at)
            )
        except Exception as err:  # noqa: BLE001 — a grant that cannot be delivered must not hold the zone
            self._skipped += 1
            if self._lease is lease:
                self._lease = None
            workflow.logger.info(
                "zone %s/%s: %s (run %s) is gone, its turn is skipped (%s)",
                self._site, self._zone, request.workflow_id, request.run_id, err,
            )
            return
        self._grants += 1
        self._grants_this_run += 1
