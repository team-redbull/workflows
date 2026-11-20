"""create-vm — ONE Halted VirtualMachine for a KubeVirt InfraEnv, on the OLDEST KV cluster of
its zone that has room, with a fixed MAC, ready for add-server-to-mce.

An InfraEnv `<flavor>-kv<cpu>-<site>-<zone>` holds VMs of `<cpu>` vCPUs and
`<cpu> × 8` GiB. A bare-metal InfraEnv has servers to draw; a KubeVirt one
has nothing until a VM exists. This run creates it: once it is Halted and
labelled `redfish-enabled: "true"`, kubevirt-redfish exposes it as a Redfish
system and server-scan lists it under its name, `ocp-<flavor>-kv<cpu>-<site>-<MAC>`.

THE ZONE'S CLUSTERS COME FROM DAY1, THEIR NUMBERS FROM THE CLUSTERS THEMSELVES,
ONLY ONE RUN AT A TIME CHOOSES IN A ZONE, AND THE CHOSEN CLUSTER BUILDS THE VM
(docs/design/create-vm-placement.md). Shape of the run:

  1. listing-kv-clusters   the zone's KV clusters, oldest first, and the
                           placement buffer — from day1 `main`, read by the
                           cluster-lifecycle limb (it holds the day1 token),
                           BEFORE the lock so the locked part stays short. A
                           file day1 lists with a problem is never read. None
                           usable: NoKvClusterError.
  2. awaiting-zone-lock    ask the zone's lock (kv_zone_lock.py: start it, or
                           signal the one running) and wait for `lock_granted`,
                           asking again every _LOCK_REASK, at most _LOCK_WAIT
                           (30 min) — then `withdraw` and KvZoneLockTimeoutError.
  3. choosing-kv-cluster   while holding the lock: each cluster's figures from
                           the worker INSIDE it, on that cluster's own queue,
                           one at a time, oldest first; the fit rule
                           (placement.py); STOP AT THE FIRST THAT FITS — the
                           clusters after it are not read. No worker on a
                           queue within 30 s is a `no-worker` row, never a
                           failure. None fits: release, NoKvClusterFitsError
                           with the table summarised per reason.
  4. creating-vm           what the VM IS (flavor, vCPUs, memory, site, zone,
                           MCE, namespace, this run's id, the lease's end) to
                           the chosen cluster's worker, which draws the MAC,
                           derives the name, builds the VirtualMachine and
                           creates it Halted, drawing again by itself when a
                           MAC is taken (3 draws). It creates nothing after
                           the lease's end.
  5. releasing-zone-lock   as soon as the VM OBJECT exists — and on every
                           other path out of 3 and 4. The next run's figures
                           already count this VM, as a stopped VM: nothing is
                           remembered between runs.
  6. awaiting-root-disk    ONE heartbeating activity until the root disk's
                           DataVolume is Succeeded and bound and the VM
                           admitted, bounded (10 min, its own clock) — after
                           the lock, in parallel with every other run.
  7. removing-vm           ONLY when the run will not complete with its VM:
                           the deadline (then VmRootDiskNotBoundError), a
                           permanent failure of the wait, or a CANCEL. The run
                           deletes ITS OWN VM, found by the run-id label it
                           was created with. A re-run of the slot is a NEW
                           execution with a NEW MAC, so an orphan would never
                           be converged on, only multiplied — and a stopped
                           orphan is counted against its cluster for ever.
  8. completed             name, MAC, namespace, size, cluster, the table.

THE LEASE. The lock is held for at most five minutes (the lock workflow's
timer). The run never acts on figures once its lease is spent: before every
read, and before the create, it checks the clock; a spent lease is given up
and the lock asked for again, the scan carrying on from the cluster it had
reached (and reading that one afresh) — at most _MAX_LEASES leases, then
KvZoneLeaseExpiredError. Only a zone where many clusters have no worker gets
there: each costs 30 s. The create itself is sent with `create_before`, the
lease's end less _LEASE_GUARD (45 s: one API call in flight, and the clock
skew between the hub and a KV cluster), and the cluster's worker enforces it.

A RUN IS KNOWN BY ITS RUN ID: the slot's workflow id is reused by later runs,
so the VM's label and the lock's line both carry the run id.

A CANCEL ROLLS BACK: waiting, it leaves the line; holding, it releases; after
the create was sent, it removes the VM (after _CANCEL_SETTLE when the
create's answer was cut off, since that call may still land). Then the run
ends CANCELED. TERMINATE runs no code: the lock passes on at the lease's end,
and a VM it left is found by its `redbull.io/created-by` label.

The namespace is add-server-to-mce's `namespace`, verbatim: a kubevirt-redfish
chassis namespace the platform created. Nothing is created on the MCE.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import (
    ActivityError,
    ApplicationError,
    TimeoutError as TemporalTimeoutError,
    TimeoutType,
    WorkflowAlreadyStartedError,
    is_cancelled_exception,
)

with workflow.unsafe.imports_passed_through():
    from shared.cluster_names import seed_cluster
    from shared.consts import (
        CLUSTER_LIFECYCLE_ACTIVITY_QUEUE,
        KV_ZONE_LOCK_WORKFLOW_QUEUE,
        vm_lifecycle_activity_queue,
    )
    from shared.exceptions import (
        KvClusterAccessError,
        KvClusterMismatchError,
        KvZoneLeaseExpiredError,
        KvZoneLockTimeoutError,
        NoKvClusterError,
        NoKvClusterFitsError,
        VmDisappearedError,
        VmIdentityConflictError,
        VmLeaseExpiredError,
        VmMacExhaustedError,
        VmNamespaceNotFoundError,
        VmRequestRejectedError,
        VmRootDiskNotBoundError,
    )
    from shared.interfaces.cluster_lifecycle import list_kv_clusters
    from shared.interfaces.vm_lifecycle import (
        await_vm_ready,
        create_vm_on_cluster,
        delete_vm_on_cluster,
        read_kv_cluster_figures,
    )
    from shared.models.vm_lifecycle import (
        CreateVmInput,
        CreateVmProgress,
        CreateVmResult,
        CreateVmRunArgs,
        KvClusterEntry,
        KvClusterFigures,
        KvClusterList,
        KvClusterListRequest,
        KvClusterRow,
        KvFiguresRequest,
        KvZoneLockGrant,
        KvZoneLockRelease,
        KvZoneLockRequest,
        KvZoneLockRunArgs,
        VmCreated,
        VmCreateRequest,
        VmDeleted,
        VmDeleteRequest,
        VmReadyOutcome,
        VmReadyRequest,
    )
    from shared.vm_requests import GIB, pod_cpu_millicores, pod_memory_bytes
    from shared.workflow_ids import kv_zone_lock_workflow_id

from workflow_domains.vm_lifecycle import placement
from workflow_domains.vm_lifecycle.kv_zone_lock import LOCK_GRANTED_SIGNAL, KvZoneLockWorkflow

# A git read on the cluster-lifecycle limb, as every git activity in the repo.
_LIST_TIMEOUT = timedelta(seconds=180)
# The short-run "no worker polls this queue" signal, on the day1 read ONLY:
# nothing is held and nothing written yet. Temporal never retries this timeout.
_LIST_SCHEDULE_TO_START_TIMEOUT = timedelta(minutes=5)
# The figures read (design §6.3.2): 30 s for the cluster's worker to pick it
# up — none: a `no-worker` row — and 60 s to answer (the worker bounds its own
# reads at 20 s). Best effort while the lock is held, so it is also bounded as
# a whole: whatever keeps it from answering, the cluster is a row, never a hang.
_FIGURES_SCHEDULE_TO_START_TIMEOUT = timedelta(seconds=30)
_FIGURES_TIMEOUT = timedelta(seconds=60)
_FIGURES_SCHEDULE_TO_CLOSE_TIMEOUT = timedelta(seconds=120)
# The create: a list, a namespace read, at most three creates and a read-back on
# the cluster's API. The worker ends an attempt by itself well inside this
# (vm_operations.CREATE_BUDGET_SECONDS + one call), so Temporal never starts a
# second attempt beside a first. NO schedule_to_start: a worker that restarts
# must not fail the run — the request's `create_before` bounds it instead.
_CREATE_TIMEOUT = timedelta(seconds=90)
_DELETE_TIMEOUT = timedelta(seconds=90)
# The ready wait is ONE heartbeating activity on its own clock (CLAUDE.md §5):
# `deadline` after this, with start_to_close only a margin past it.
_READY_DEADLINE_MINUTES = 10
_READY_TIMEOUT = timedelta(minutes=12)
_READY_HEARTBEAT_TIMEOUT = timedelta(minutes=2)
# The lock: how long a run waits for its zone in all, how often it asks again
# while it waits (a lock workflow someone terminated is started afresh by the
# next ask), and how many leases one run may take.
_LOCK_WAIT = timedelta(minutes=30)
_LOCK_REASK = timedelta(minutes=2)
_MAX_LEASES = 3
# The cluster's worker stops creating this long BEFORE the lock moves on: one
# call to its API may still be in flight when it checks (25 s at most), and its
# clock is another machine's — what is left of the guard, 20 s, is the clock
# skew between the hub and a KV cluster that is tolerated.
_LEASE_GUARD = timedelta(seconds=45)
# A create is not sent with less than this left before `create_before`.
_CREATE_MARGIN = timedelta(seconds=10)
# After a cancel cut the create's answer off: the attempt in flight may still
# land (its own budget, plus one API call's tail).
_CANCEL_SETTLE = _CREATE_TIMEOUT + timedelta(seconds=30)
# The cancel rollback's delete is best effort: it never keeps a cancelled run open for ever.
_ROLLBACK_DELETE_DEADLINE = timedelta(minutes=10)

# Activity-raised failures no retry can fix, as CLASSES (CLAUDE.md §5).
_PERMANENT_ACTIVITY_ERRORS: tuple[type[Exception], ...] = (
    KvClusterAccessError,
    KvClusterMismatchError,
    VmIdentityConflictError,
    VmNamespaceNotFoundError,
    VmRequestRejectedError,
    VmDisappearedError,
    VmMacExhaustedError,
    VmLeaseExpiredError,
)
_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
    non_retryable_error_types=[error.__name__ for error in _PERMANENT_ACTIVITY_ERRORS],
)


class _LeaseSpent(Exception):
    """Module-local: the lease in hand is over (or too close to it); ask for the lock again."""


def usable(entry: KvClusterEntry, zone: str) -> bool:
    """Whether day1 lists the cluster as one this zone's VMs may go on: its file gives it
    this zone and its name carries its place in the creation order."""
    return entry.zone == zone and entry.id is not None


def _activity_failure(err: ActivityError) -> str:
    """Why an activity call failed, in a few words, for a table row."""
    cause = err.cause
    if isinstance(cause, TemporalTimeoutError):
        return f"timed out ({cause.type.name.lower().replace('_', '-') if cause.type else 'timeout'})"
    if isinstance(cause, ApplicationError):
        return f"{cause.type or 'error'}: {str(cause.message)[:300]}"
    return str(cause or err)[:300]


@workflow.defn
class CreateVmWorkflow:
    @workflow.init
    def __init__(self, run_args: CreateVmRunArgs) -> None:
        self._phase = "pending"
        self._candidates: list[str] = []
        self._buffer_percent: int | None = None
        self._revision: str | None = None
        self._table: list[KvClusterRow] = []
        self._not_read: list[str] = []
        self._lock_id = kv_zone_lock_workflow_id(run_args.input.site, run_args.input.zone)
        self._namespace = run_args.input.namespace
        # How far down the zone's list the run has judged: a new lease carries on from here.
        self._reached = 0
        self._awaiting_lock = False
        self._grant: KvZoneLockGrant | None = None
        self._lease: KvZoneLockGrant | None = None
        self._leases = 0
        self._kv_cluster: str | None = None
        self._create_sent = False
        self._created: VmCreated | None = None
        self._removed = False
        self._cancel_rollback: str | None = None

    @workflow.query
    def progress(self) -> CreateVmProgress:
        return CreateVmProgress(
            phase=self._phase, kv_candidates=list(self._candidates), buffer_percent=self._buffer_percent,
            day1_revision=self._revision, table=list(self._table), not_read=list(self._not_read),
            zone_lock=self._lock_id, lease_ends_at=self._lease.lease_ends_at if self._lease else None,
            leases=self._leases, kv_cluster=self._kv_cluster,
            vm_name=self._created.name if self._created else None, mac=self._created.mac if self._created else None,
            cancel_rollback=self._cancel_rollback,
        )

    @workflow.signal(name=LOCK_GRANTED_SIGNAL)
    async def lock_granted(self, grant: KvZoneLockGrant) -> None:
        """The zone's lock, from its lock workflow. A grant this run is not waiting for (it
        gave up, or already holds one) is given straight back, so it never costs the zone a lease."""
        if self._awaiting_lock and self._grant is None:
            self._grant = grant
            return
        await self._tell_lock(KvZoneLockWorkflow.release, KvZoneLockRelease(lease_id=grant.lease_id))

    # --- the run -----------------------------------------------------------------------

    @workflow.run
    async def run(self, run_args: CreateVmRunArgs) -> CreateVmResult:
        """The create; on a CANCEL its rollback first (_roll_back_after_cancel), then CANCELED."""
        try:
            result = await self._run(run_args.input)
        except BaseException as err:
            # Only a cancel a PERSON asked for rolls back. A worker dropping this run from its
            # cache cancels its tasks too, and must run no workflow code on the way out.
            cancel_requested = workflow.in_workflow() and workflow.cancellation_reason() is not None
            if cancel_requested and is_cancelled_exception(err):
                await self._roll_back_after_cancel(run_args.input)
            if cancel_requested or isinstance(err, Exception):
                await self._settle_handlers()
            raise
        await self._settle_handlers()
        return result

    @staticmethod
    async def _settle_handlers() -> None:
        """A grant given straight back (lock_granted) must be on its way before the run closes."""
        await workflow.wait_condition(workflow.all_handlers_finished)

    async def _run(self, request: CreateVmInput) -> CreateVmResult:
        info = workflow.info()
        me = KvZoneLockRequest(workflow_id=info.workflow_id, run_id=info.run_id)

        # Step 1 — the zone's KV clusters and the buffer, from day1.
        self._phase = "listing-kv-clusters"
        try:
            zone_request = KvClusterListRequest(site=request.site, zone=request.zone)
        except ValueError as err:
            # Only a run started around the API can carry these: the edge parses both from
            # the InfraEnv's name. Failed, not hung (a plain error here fails the TASK for ever, §5).
            raise ApplicationError(
                f"site {request.site!r} zone {request.zone!r} are not ones the naming convention knows "
                f"({str(err)[:300]}), so day1 has no seed folder for them. Nothing was created.",
                type=NoKvClusterError.__name__,
            ) from None
        listed: KvClusterList = await workflow.execute_activity(
            list_kv_clusters,
            zone_request,
            task_queue=CLUSTER_LIFECYCLE_ACTIVITY_QUEUE,
            start_to_close_timeout=_LIST_TIMEOUT,
            schedule_to_start_timeout=_LIST_SCHEDULE_TO_START_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )
        self._buffer_percent, self._revision = listed.buffer_percent, listed.revision
        clusters = [entry for entry in listed.clusters if usable(entry, request.zone)]
        self._candidates = [entry.name for entry in clusters]
        for entry in listed.clusters:
            if not usable(entry, request.zone):
                why = "; ".join(entry.problems) or "its day1 file does not give it this zone"
                self._table.append(KvClusterRow(name=entry.name, skip_reason=placement.DAY1_PROBLEM, skip_detail=why[:500]))
                workflow.logger.warning("day1 %s: %s is not usable for zone %s: %s", listed.revision, entry.path,
                                        request.zone, why)
        if not clusters:
            folder = listed.folder or f"the day1 folder of seed {seed_cluster(request.site, request.zone)}"  # both validated above
            listed_with_problems = (
                f"; listed with a problem and never used: {placement.summarise_skips(self._table)}" if self._table else ""
            )
            raise ApplicationError(
                f"day1 {listed.revision} lists no usable KV cluster for site {request.site} zone {request.zone} "
                f"in {folder}{listed_with_problems}. Nothing was created.",
                type=NoKvClusterError.__name__,
            )

        # Steps 2-5 — under the zone's lock: choose the oldest cluster that fits, create the VM there.
        vm_cpu, vm_memory = pod_cpu_millicores(request.cpu), pod_memory_bytes(request.memory_gib)
        created: VmCreated | None = None
        while created is None:
            if self._leases >= _MAX_LEASES:
                raise ApplicationError(
                    f"{_MAX_LEASES} zone leases ran out while this run was still reading the KV clusters of site "
                    f"{request.site} zone {request.zone} (reached {self._reached} of {len(clusters)}): "
                    f"{placement.summarise_skips(self._table)}. Nothing was created; bring those clusters' "
                    "workers back, then ask again",
                    type=KvZoneLeaseExpiredError.__name__,
                )
            lease = await self._acquire(me, request)
            try:
                created = await self._choose_and_create(
                    request, me, clusters, lease, vm_cpu, vm_memory, listed.buffer_percent
                )
            except _LeaseSpent:
                workflow.logger.info(
                    "the zone lease ended at cluster %d of %d; asking for the lock again",
                    self._reached + 1, len(clusters),
                )
            finally:
                await self._release(lease)

        # Step 6 — its root disk, bounded, with the zone free again.
        self._phase = "awaiting-root-disk"
        queue = vm_lifecycle_activity_queue(created.kv_cluster)
        try:
            ready: VmReadyOutcome = await workflow.execute_activity(
                await_vm_ready,
                VmReadyRequest(
                    kv_cluster=created.kv_cluster, namespace=created.namespace, name=created.name,
                    deadline_minutes=_READY_DEADLINE_MINUTES,
                ),
                task_queue=queue,
                start_to_close_timeout=_READY_TIMEOUT,
                heartbeat_timeout=_READY_HEARTBEAT_TIMEOUT,
                retry_policy=_RETRY_POLICY,
            )
        except ActivityError as err:
            cause = err.cause
            gone = isinstance(cause, ApplicationError) and cause.type == VmDisappearedError.__name__
            if not gone and not is_cancelled_exception(err):
                # A wait that failed for good leaves a VM nobody will be told about.
                await self._remove_best_effort(me, created, "the wait for its root disk failed")
            raise
        if ready.result != "done":
            # Step 7 — nothing survives on a cluster that did not take the VM.
            self._phase = "removing-vm"
            await workflow.execute_activity(
                delete_vm_on_cluster,
                VmDeleteRequest(
                    kv_cluster=created.kv_cluster, namespace=created.namespace, request_id=me.run_id,
                    name=created.name, mac=created.mac,
                ),
                task_queue=queue,
                start_to_close_timeout=_DELETE_TIMEOUT,
                retry_policy=_RETRY_POLICY,
            )
            self._removed = True
            raise ApplicationError(
                f"VM {created.namespace}/{created.name} on {created.kv_cluster} showed no bound root disk within "
                f"{_READY_DEADLINE_MINUTES} min ({ready.detail}). The VM was deleted again: a re-run draws a "
                "new MAC and could never resume this one. A WaitForFirstConsumer storage class never binds "
                "a Halted VM's disk — the cluster's VM storage class must bind Immediately and allow a "
                "ReadWriteMany Filesystem volume; otherwise check the DataVolume and the CDI log on that cluster",
                type=VmRootDiskNotBoundError.__name__,
            )

        self._phase = "completed"
        workflow.logger.info(
            "VM %s/%s on %s: mac %s, %d vCPU / %d GiB, root disk %s (%d draws, %d leases)",
            created.namespace, created.name, created.kv_cluster, created.mac, request.cpu, request.memory_gib,
            ready.root_disk_phase, created.draws, self._leases,
        )
        return CreateVmResult(
            vm_name=created.name, mac=created.mac, namespace=created.namespace, flavor=request.flavor,
            cpu=request.cpu, memory_gib=request.memory_gib, kv_cluster=created.kv_cluster,
            table=list(self._table), not_read=list(self._not_read), buffer_percent=listed.buffer_percent,
            day1_revision=listed.revision, root_disk_phase=ready.root_disk_phase, created=created.created,
            draws=created.draws, leases=self._leases, infra_env=request.infra_env,
            mce_cluster=request.mce_cluster, site=request.site, zone=request.zone,
        )

    # --- the zone's lock ---------------------------------------------------------------

    async def _tell_lock(self, signal, payload) -> bool:  # noqa: ANN001 — a signal method of KvZoneLockWorkflow
        """Signal the zone's lock workflow (by id: the signal follows a continued run). Best
        effort — a lock that is not there has nothing to be told; True when it was delivered."""
        try:
            await workflow.get_external_workflow_handle(self._lock_id).signal(signal, payload)
            return True
        except Exception as err:  # noqa: BLE001 — never a reason to fail the run
            workflow.logger.info("zone lock %s could not be signalled (%s)", self._lock_id, err)
            return False

    async def _ask_for_lock(self, me: KvZoneLockRequest, request: CreateVmInput) -> None:
        """Join the line: start the zone's lock with this run as its first waiter, or — it
        already runs — signal it. The lock keeps a run in its line once, however often it asks."""
        try:
            await workflow.start_child_workflow(
                KvZoneLockWorkflow.run,
                KvZoneLockRunArgs(site=request.site, zone=request.zone, line=[me]),
                id=self._lock_id,
                task_queue=KV_ZONE_LOCK_WORKFLOW_QUEUE,
                # The lock outlives every run that started it.
                parent_close_policy=workflow.ParentClosePolicy.ABANDON,
            )
        except WorkflowAlreadyStartedError:
            await self._tell_lock(KvZoneLockWorkflow.request_lock, me)

    async def _acquire(self, me: KvZoneLockRequest, request: CreateVmInput) -> KvZoneLockGrant:
        """Wait for the zone's lock: `lock_granted`, at most _LOCK_WAIT, asking again every _LOCK_REASK."""
        self._phase = "awaiting-zone-lock"
        deadline = workflow.now() + _LOCK_WAIT
        self._grant = None
        self._awaiting_lock = True
        try:
            while True:
                remaining = deadline - workflow.now()
                if remaining <= timedelta(0):
                    break
                await self._ask_for_lock(me, request)
                try:
                    await workflow.wait_condition(lambda: self._grant is not None, timeout=min(remaining, _LOCK_REASK))
                except asyncio.TimeoutError:
                    continue
                grant, self._grant = self._grant, None
                if grant is None:
                    continue
                self._lease = grant
                self._leases += 1
                return grant
        except BaseException:
            # A cancel while waiting: leave the line before going. (Not for a worker dropping
            # the run from its cache: the run is still waiting, on another worker.)
            self._awaiting_lock = False
            if workflow.in_workflow() and workflow.cancellation_reason() is not None:
                await self._tell_lock(KvZoneLockWorkflow.withdraw, me)
            raise
        finally:
            self._awaiting_lock = False
        # Gave up: out of the line first, so the lock does not grant this run later
        # (a grant already on its way is released by lock_granted, or by this withdraw).
        await self._tell_lock(KvZoneLockWorkflow.withdraw, me)
        raise ApplicationError(
            f"the lock of site {request.site} zone {request.zone} ({self._lock_id}) was not granted within "
            f"{int(_LOCK_WAIT.total_seconds() // 60)} min: other create-vm runs of the zone hold it one after "
            "another, or its lock workflow is not running. This run left the line; nothing was created",
            type=KvZoneLockTimeoutError.__name__,
        )

    async def _release(self, lease: KvZoneLockGrant) -> None:
        """Give the lock back — on every path. A lease that already ended is ignored by the lock."""
        if self._lease is not lease or not workflow.in_workflow():
            return
        previous, self._phase = self._phase, "releasing-zone-lock"
        self._lease = None
        await self._tell_lock(KvZoneLockWorkflow.release, KvZoneLockRelease(lease_id=lease.lease_id))
        self._phase = previous

    @staticmethod
    def _spent(lease: KvZoneLockGrant, margin: timedelta = timedelta(0)) -> bool:
        return workflow.now() + margin >= lease.lease_ends_at

    # --- choosing and creating, under the lock ---------------------------------------------

    def _record(self, row: KvClusterRow) -> None:
        """A judged cluster's row; a cluster read again under a new lease replaces its row."""
        self._table = [existing for existing in self._table if existing.name != row.name] + [row]

    async def _figures(self, name: str) -> KvClusterRow | KvClusterFigures:
        """One cluster's figures from the worker inside it; a row instead when it has none to give."""
        queue = vm_lifecycle_activity_queue(name)
        try:
            figures: KvClusterFigures = await workflow.execute_activity(
                read_kv_cluster_figures,
                KvFiguresRequest(kv_cluster=name),
                task_queue=queue,
                start_to_close_timeout=_FIGURES_TIMEOUT,
                schedule_to_start_timeout=_FIGURES_SCHEDULE_TO_START_TIMEOUT,
                schedule_to_close_timeout=_FIGURES_SCHEDULE_TO_CLOSE_TIMEOUT,
                retry_policy=_RETRY_POLICY,
            )
        except ActivityError as err:
            if is_cancelled_exception(err):
                raise
            cause = err.cause
            if isinstance(cause, TemporalTimeoutError) and cause.type == TimeoutType.SCHEDULE_TO_START:
                return KvClusterRow(
                    name=name, skip_reason=placement.NO_WORKER,
                    skip_detail=f"nothing polled {queue} within {int(_FIGURES_SCHEDULE_TO_START_TIMEOUT.total_seconds())} s",
                )
            return KvClusterRow(
                name=name, skip_reason=placement.UNREACHABLE,
                skip_detail=f"its worker gave no figures: {_activity_failure(err)}",
            )
        if figures.kv_cluster != name:
            return KvClusterRow(
                name=name, skip_reason=placement.UNREACHABLE, figures=figures,
                skip_detail=f"the worker on {queue} answered for {figures.kv_cluster}",
            )
        return figures

    async def _choose_and_create(
        self,
        request: CreateVmInput,
        me: KvZoneLockRequest,
        clusters: list[KvClusterEntry],
        lease: KvZoneLockGrant,
        vm_cpu: int,
        vm_memory: int,
        buffer_percent: int,
    ) -> VmCreated:
        """From cluster `self._reached` on, oldest first: the first that fits gets the VM.

        Raises _LeaseSpent, with nothing created, when the lease ends first —
        the caller asks for the lock again and comes back at the same cluster,
        which is then read afresh. Raises NoKvClusterFitsError past the last.
        """
        self._phase = "choosing-kv-cluster"
        while self._reached < len(clusters):
            if self._spent(lease):
                raise _LeaseSpent
            entry = clusters[self._reached]
            answer = await self._figures(entry.name)
            if isinstance(answer, KvClusterRow):
                self._record(answer)
                self._reached += 1
                continue
            verdict = placement.judge(answer, vm_cpu, vm_memory, buffer_percent)
            self._record(placement.row(entry.name, answer, verdict))
            if not verdict.fits:
                self._reached += 1
                continue
            # The first that fits. Its figures are only good under THIS lease.
            if self._spent(lease, _LEASE_GUARD + _CREATE_MARGIN):
                raise _LeaseSpent
            self._not_read = [later.name for later in clusters[self._reached + 1:]]
            self._phase = "creating-vm"
            self._kv_cluster = entry.name
            queue = vm_lifecycle_activity_queue(entry.name)
            # Recorded BEFORE the call: a cancel stops waiting for it, not the call itself.
            self._create_sent = True
            created: VmCreated = await workflow.execute_activity(
                create_vm_on_cluster,
                VmCreateRequest(
                    kv_cluster=entry.name, namespace=request.namespace, flavor=request.flavor, cpu=request.cpu,
                    memory_gib=request.memory_gib, site=request.site, zone=request.zone, mce=request.mce_cluster,
                    request_id=me.run_id, workflow_id=me.workflow_id,
                    create_before=lease.lease_ends_at - _LEASE_GUARD,
                ),
                task_queue=queue,
                start_to_close_timeout=_CREATE_TIMEOUT,
                retry_policy=_RETRY_POLICY,
            )
            self._created = created
            return created
        raise ApplicationError(
            f"no KV cluster of site {request.site} zone {request.zone} takes a VM of {request.cpu} vCPU / "
            f"{request.memory_gib} GiB (pod request {vm_cpu} m / {vm_memory // GIB} GiB) under the "
            f"{buffer_percent}% buffer: {placement.summarise_skips(self._table)}. Nothing was created.",
            type=NoKvClusterFitsError.__name__,
        )

    # --- removing the run's own VM -----------------------------------------------------------

    async def _remove_best_effort(self, me: KvZoneLockRequest, created: VmCreated | None, why: str) -> str:
        """Delete this run's VM on the chosen cluster, bounded, never raising; what happened, in words."""
        if self._kv_cluster is None:
            return "nothing was sent to any cluster"
        previous, self._phase = self._phase, "removing-vm"
        queue = vm_lifecycle_activity_queue(self._kv_cluster)
        try:
            deleted: VmDeleted = await workflow.execute_activity(
                delete_vm_on_cluster,
                VmDeleteRequest(
                    kv_cluster=self._kv_cluster, namespace=created.namespace if created else self._namespace,
                    request_id=me.run_id, name=created.name if created else None,
                    mac=created.mac if created else None,
                ),
                task_queue=queue,
                start_to_close_timeout=_DELETE_TIMEOUT,
                schedule_to_close_timeout=_ROLLBACK_DELETE_DEADLINE,
                retry_policy=_RETRY_POLICY,
                cancellation_type=workflow.ActivityCancellationType.ABANDON,
            )
        except ActivityError as err:
            said = (
                f"{why}; its VM on {self._kv_cluster} could NOT be removed ({_activity_failure(err)}): find it by "
                f"label redbull.io/created-by={me.run_id} and delete it"
            )
            workflow.logger.warning(said)
            return said
        finally:
            self._phase = previous
        self._removed = True
        return f"{why}; removed {', '.join(deleted.names) if deleted.deleted else 'nothing (no VM of this run was there)'} on {self._kv_cluster}"

    async def _roll_back_after_cancel(self, request: CreateVmInput) -> None:
        """What a cancel must undo; never raises. The line and the lease are already given
        back where the cancel met them (_acquire, _release)."""
        info = workflow.info()
        me = KvZoneLockRequest(workflow_id=info.workflow_id, run_id=info.run_id)
        try:
            if not self._create_sent:
                self._cancel_rollback = "cancelled before anything was sent to a cluster: nothing to undo"
                return
            if self._removed:
                self._cancel_rollback = "cancelled after its VM was removed: nothing to undo"
                return
            if self._created is None:
                # The create's answer was cut off: the attempt in flight may still land.
                self._phase = "removing-vm"
                await workflow.sleep(_CANCEL_SETTLE)
            self._cancel_rollback = await self._remove_best_effort(me, self._created, "cancelled")
        except BaseException as err:  # noqa: BLE001 — the rollback never raises; the cancel still ends the run
            self._cancel_rollback = f"cancelled; the rollback itself was cut off ({type(err).__name__}): a VM labelled redbull.io/created-by={me.run_id} may remain on {self._kv_cluster}"
            workflow.logger.warning(self._cancel_rollback)
