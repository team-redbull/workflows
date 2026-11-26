"""delete-vm — delete ONE VM by name from its KV cluster, and end only when nothing of it is left.

A VM that add-server-to-mce had create-vm make stays on its KV cluster after
its host leaves the MCE: Halted, counted against that cluster's capacity for
ever, its disks in the storage array. This workflow removes it, and COMPLETES
ONLY WHEN NOTHING OF IT IS LEFT IN KUBERNETES, ITS PERSISTENTVOLUMES INCLUDED
— "the PersistentVolume object is gone" is how it knows the volume left the
array, with no call to any storage system (CLAUDE.md §3, §4).

It has NO ROUTE (the operator, 2026-11-26): it holds no MCE credential, so it
cannot check that the VM's BareMetalHost is gone, and metal3 needs the VM's
Redfish to release a host. remove-server-from-mce stays the one public way to
remove a VM and is to start this as its child; until then it is started by
tests and the Temporal CLI only. Every call goes to the KV cluster's OWN
worker, on `vm_lifecycle_activity_queue(kv_cluster)`.

  inspecting        inspect_vm_for_deletion. The VirtualMachine must be one
                    create-vm made for the caller's MCE (its labels; the run id
                    and the MAC too when the caller names them), or the run
                    fails VmIdentityConflictError with nothing touched. Records
                    the VM, its DataVolumes, its claims, the PersistentVolumes
                    behind them (reclaim policy, CSI driver, volume handle),
                    VolumeSnapshots of them and the pods mounting them. Nothing
                    of the VM anywhere: COMPLETED, `found` false. The only call
                    with a schedule-to-start limit (5 min): nothing is written
                    yet, so "no worker on that cluster" may fail the run —
                    KvClusterWorkerAbsentError, naming the queue.
  deleting          delete_vm_and_volumes. Reads and checks everything again,
                    then deletes the VirtualMachine (by UID), its DataVolumes,
                    a boot ISO's VolumeImportSource and every claim of the VM.
                    A RUNNING VM is deleted too, and the result says so. NEVER
                    a PersistentVolume, a VolumeSnapshot or a pod.
  awaiting-release  await_vm_gone, ONE heartbeating activity, 8 s polls, 15 min
                    on its own clock: the VirtualMachine, the DataVolumes, the
                    claims and the PersistentVolumes this run recorded. `done`,
                    `left` (only `Retain` volumes remain) or `deadline`.
  completed         nothing is left and no snapshot stands.

DELETE, THEN FAIL (the operator, 2026-11-26). When the array cannot be proven
clean the run still deletes everything it may and then FAILS, never a success
with a note:
  VmVolumesLeftInStorageError  a PersistentVolume is still there (`Retain`
                    keeps it by design; another did not go within the deadline),
                    a VolumeSnapshot of one of the VM's disks stands (the
                    backend keeps the volume under it), or a claim only LOOKS
                    like the VM's boot ISO (a cut name) and was left alone.
  VmDeletionTimeoutError  the VirtualMachine, a DataVolume or a claim was still
                    there at the deadline: something holds it.
Both name every object and what a person does.

Nothing is undone and nothing runs on an exit path: a delete has no rollback.
A re-run of the same id RESUMES — the inspection finds whatever is left, by
the same rule — and after a "delete, then fail" it fails the same way until a
person has removed the snapshot or the Released volume, then completes
`found` false. No zone lock: freeing room only makes a create-vm run's
figures err on the full side.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError, TimeoutError as TemporalTimeoutError, TimeoutType

with workflow.unsafe.imports_passed_through():
    from shared.consts import vm_lifecycle_activity_queue
    from shared.exceptions import (
        KvClusterAccessError,
        KvClusterMismatchError,
        KvClusterWorkerAbsentError,
        VmDeletionTimeoutError,
        VmIdentityConflictError,
        VmRequestRejectedError,
        VmVolumesLeftInStorageError,
    )
    from shared.interfaces.vm_lifecycle import await_vm_gone, delete_vm_and_volumes, inspect_vm_for_deletion
    from shared.models.vm_lifecycle import (
        DeleteVmInput,
        DeleteVmProgress,
        DeleteVmResult,
        DeleteVmRunArgs,
        VmClaim,
        VmDeletionState,
        VmGoneOutcome,
        VmRemovalRequest,
        VmSnapshot,
        VmVolume,
        VmVolumesDeleted,
    )

# One call to the cluster's API is at most 25 s (kubevirt_api.REQUEST_TIMEOUT); the
# inspection makes a handful at once, the delete a handful in a row.
_INSPECT_TIMEOUT = timedelta(minutes=3)
# Nothing is written before the inspection answers, so a KV cluster with no worker may
# fail the run here. NOT on the delete or the wait: once a delete was sent, a worker pod
# away for five minutes must not fail a run that only has to wait for it.
_INSPECT_SCHEDULE_TO_START_TIMEOUT = timedelta(minutes=5)
_DELETE_TIMEOUT = timedelta(minutes=3)
# The wait's own clock, and Temporal's margin past it.
_GONE_DEADLINE_MINUTES = 15
_GONE_TIMEOUT = timedelta(minutes=17)
_GONE_HEARTBEAT_TIMEOUT = timedelta(minutes=2)
# What no retry fixes, as CLASSES (CLAUDE.md §5). Everything else is the cluster's
# outage, out-waited. A 403 is the chart's ClusterRole predating delete-vm.
_PERMANENT_ACTIVITY_ERRORS: tuple[type[Exception], ...] = (
    KvClusterAccessError,
    KvClusterMismatchError,
    VmIdentityConflictError,
    VmRequestRejectedError,
)
_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
    non_retryable_error_types=[error.__name__ for error in _PERMANENT_ACTIVITY_ERRORS],
)
# A failure message lists objects; it stays a message.
_MAX_MESSAGE_CHARS = 3000


def _merged(*groups: list) -> list:
    """Rows of several reads as one list, by name, the LATER read's row winning."""
    rows: dict[str, object] = {}
    for group in groups:
        for row in group:
            rows[row.name] = row
    return [rows[name] for name in sorted(rows)]


def _volume_line(volume: VmVolume) -> str:
    return (
        f"{volume.name} (claim {volume.claim}, {volume.phase}, reclaim policy {volume.reclaim_policy}, storage class "
        f"{volume.storage_class}, CSI driver {volume.driver}, volume handle {volume.volume_handle})"
    )


def _claim_line(claim: VmClaim) -> str:
    held = f", mounted by pod(s) {', '.join(claim.used_by)}" if claim.used_by else ""
    return f"{claim.name} ({'Terminating' if claim.deleting else claim.phase}, finalizers {claim.finalizers}{held})"


def _cut(message: str) -> str:
    if len(message) <= _MAX_MESSAGE_CHARS:
        return message
    return f"{message[:_MAX_MESSAGE_CHARS]}… ({len(message) - _MAX_MESSAGE_CHARS} more characters)"


@workflow.defn
class DeleteVmWorkflow:
    @workflow.init
    def __init__(self, run_args: DeleteVmRunArgs) -> None:
        request = run_args.input
        self._phase = "pending"
        self._progress = DeleteVmProgress(
            phase=self._phase, kv_cluster=request.kv_cluster, namespace=request.namespace, vm_name=request.vm_name
        )

    @workflow.query
    def progress(self) -> DeleteVmProgress:
        return self._progress.model_copy(update={"phase": self._phase})

    def _saw(self, state: VmDeletionState) -> None:
        progress = self._progress
        progress.found = state.found
        progress.was_running = progress.was_running or state.was_running
        progress.datavolumes = sorted({*progress.datavolumes, *state.datavolumes})
        progress.claims = sorted({*progress.claims, *(claim.name for claim in state.claims)})
        progress.volumes = sorted({*progress.volumes, *(volume.name for volume in state.volumes)})
        progress.snapshots = sorted({*progress.snapshots, *(snapshot.name for snapshot in state.snapshots)})

    @workflow.run
    async def run(self, run_args: DeleteVmRunArgs) -> DeleteVmResult:
        wf_input: DeleteVmInput = run_args.input
        queue = vm_lifecycle_activity_queue(wf_input.kv_cluster)
        where = f"{wf_input.namespace}/{wf_input.vm_name} on KV cluster {wf_input.kv_cluster}"
        request = VmRemovalRequest(
            kv_cluster=wf_input.kv_cluster, namespace=wf_input.namespace, name=wf_input.vm_name,
            mce=wf_input.mce_cluster, created_by=wf_input.created_by, mac=wf_input.mac,
        )
        workflow.logger.info("deleting VM %s (MCE %s): %s", where, wf_input.mce_cluster, wf_input.why)

        # 1 — what is there, and is it the VM asked for. Nothing is written.
        self._phase = "inspecting"
        try:
            inspected: VmDeletionState = await workflow.execute_activity(
                inspect_vm_for_deletion,
                request,
                task_queue=queue,
                start_to_close_timeout=_INSPECT_TIMEOUT,
                schedule_to_start_timeout=_INSPECT_SCHEDULE_TO_START_TIMEOUT,
                retry_policy=_RETRY_POLICY,
                cancellation_type=workflow.ActivityCancellationType.ABANDON,
            )
        except ActivityError as err:
            # Not an exit path that acts: one timeout Temporal never retries is given its name.
            if not (isinstance(err.cause, TemporalTimeoutError) and err.cause.type == TimeoutType.SCHEDULE_TO_START):
                raise
            self._phase = "failed"
            raise ApplicationError(
                f"no vm-lifecycle worker polled {queue} within "
                f"{int(_INSPECT_SCHEDULE_TO_START_TIMEOUT.total_seconds() // 60)} min: the worker inside KV cluster "
                f"{wf_input.kv_cluster} is not running, cannot reach Temporal, or is deployed under another "
                f"`kvCluster` than this day1 file name. VM {where} was not read and nothing was deleted. Run again "
                "once the worker polls",
                type=KvClusterWorkerAbsentError.__name__,
            ) from None
        self._saw(inspected)
        if not inspected.found:
            self._phase = "completed"
            workflow.logger.info("nothing of VM %s is there: already deleted", where)
            return DeleteVmResult(
                kv_cluster=wf_input.kv_cluster, namespace=wf_input.namespace, vm_name=wf_input.vm_name, found=False
            )

        # 2 — delete what a worker may: the VM, its DataVolumes, its claims. Re-read, re-checked.
        self._phase = "deleting"
        deleted: VmVolumesDeleted = await workflow.execute_activity(
            delete_vm_and_volumes,
            request,
            task_queue=queue,
            start_to_close_timeout=_DELETE_TIMEOUT,
            retry_policy=_RETRY_POLICY,
            cancellation_type=workflow.ActivityCancellationType.ABANDON,
        )
        self._saw(deleted.seen)
        self._progress.deleted = [
            *([f"VirtualMachine {wf_input.vm_name}"] if deleted.vm_deleted else []),
            *(f"DataVolume {name}" for name in deleted.datavolumes_deleted),
            *(f"claim {name}" for name in deleted.claims_deleted),
            *(f"VolumeImportSource {name}" for name in deleted.import_sources_deleted),
        ]
        # The union of both reads, kept in THIS run's history: a volume is found through
        # its claim, so the wait is told what to watch and never has to find it again.
        claims: list[VmClaim] = _merged(inspected.claims, deleted.seen.claims)
        volumes: list[VmVolume] = _merged(inspected.volumes, deleted.seen.volumes)
        snapshots: list[VmSnapshot] = _merged(inspected.snapshots, deleted.seen.snapshots)
        look_alikes: list[VmClaim] = _merged(inspected.look_alikes, deleted.seen.look_alikes)
        datavolumes = sorted({*inspected.datavolumes, *deleted.seen.datavolumes})
        was_running = inspected.was_running or deleted.seen.was_running
        if was_running:
            workflow.logger.warning("VM %s was RUNNING when it was deleted: its guest is shut down by the delete", where)

        # 3 — until nothing is left, PersistentVolumes included. One heartbeating activity.
        self._phase = "awaiting-release"
        gone: VmGoneOutcome = await workflow.execute_activity(
            await_vm_gone,
            request.model_copy(update={
                "vm_uid": deleted.seen.vm_uid or inspected.vm_uid,
                "datavolumes": datavolumes,
                "claims": [claim.name for claim in claims],
                "volumes": [volume.name for volume in volumes],
                "deadline_minutes": _GONE_DEADLINE_MINUTES,
            }),
            task_queue=queue,
            start_to_close_timeout=_GONE_TIMEOUT,
            heartbeat_timeout=_GONE_HEARTBEAT_TIMEOUT,
            retry_policy=_RETRY_POLICY,
            cancellation_type=workflow.ActivityCancellationType.TRY_CANCEL,
        )
        self._progress.remaining = gone.detail

        # 4 — the verdict. Every delete was sent; what is left decides.
        if gone.vm or gone.datavolumes or gone.claims:
            self._phase = "failed"
            left = [
                *([f"VirtualMachine {wf_input.vm_name}"] if gone.vm else []),
                *(f"DataVolume {name}" for name in gone.datavolumes),
                *(f"claim {_claim_line(claim)}" for claim in gone.claims),
                *(f"PersistentVolume {_volume_line(volume)}" for volume in gone.volumes),
            ]
            raise ApplicationError(
                _cut(
                    f"VM {where} was deleted but is not gone after {_GONE_DEADLINE_MINUTES} min: {'; '.join(left)}. "
                    "Every delete was sent, so something holds these: a guest that does not shut down, a pod still "
                    "mounting a claim, a finalizer nobody removes. Fix that on the KV cluster and run again: the "
                    "run resumes and waits for the same objects"
                ),
                type=VmDeletionTimeoutError.__name__,
            )
        if gone.volumes or snapshots or look_alikes:
            self._phase = "failed"
            problems: list[str] = []
            if gone.volumes:
                kept = [volume for volume in gone.volumes if volume.reclaim_policy == "Retain"]
                late = [volume for volume in gone.volumes if volume.reclaim_policy != "Retain"]
                if kept:
                    problems.append(
                        "PersistentVolume(s) with reclaim policy Retain, which Kubernetes never removes — delete each "
                        "volume in the storage array by its volume handle, then its PersistentVolume: "
                        + "; ".join(_volume_line(volume) for volume in kept)
                    )
                if late:
                    problems.append(
                        f"PersistentVolume(s) still there {_GONE_DEADLINE_MINUTES} min after their claims went — the "
                        "CSI driver has not deleted the volume; see its provisioner's log: "
                        + "; ".join(_volume_line(volume) for volume in late)
                    )
            if snapshots:
                problems.append(
                    "VolumeSnapshot(s) of the VM's disks, under which the storage keeps the volume even after its "
                    "PersistentVolume is gone — delete them if nobody needs them: "
                    + "; ".join(f"{snapshot.name} (of claim {snapshot.claim}, content {snapshot.content})" for snapshot in snapshots)
                )
            if look_alikes:
                problems.append(
                    "claim(s) that look like this VM's boot ISO by a name kubevirt-redfish cut short, which no longer "
                    "says whose they are — left alone; delete each by hand if it is this VM's: "
                    + "; ".join(_claim_line(claim) for claim in look_alikes)
                )
            raise ApplicationError(
                _cut(
                    f"VM {where} is deleted — the VirtualMachine, its DataVolumes and its claims are gone — but its "
                    f"storage is not provably clean: {' | '.join(problems)}. This run deletes no PersistentVolume and "
                    "no snapshot. Run again afterwards: it completes once nothing is left"
                ),
                type=VmVolumesLeftInStorageError.__name__,
            )

        self._phase = "completed"
        workflow.logger.info(
            "VM %s is gone: %d DataVolume(s), %d claim(s), %d PersistentVolume(s) with it, %d polls",
            where, len(datavolumes), len(claims), len(volumes), gone.polls,
        )
        return DeleteVmResult(
            kv_cluster=wf_input.kv_cluster,
            namespace=wf_input.namespace,
            vm_name=wf_input.vm_name,
            found=True,
            # What was there and is verified gone — not one attempt's own count, which a
            # retry after a lost answer would report short.
            vm_deleted=inspected.vm_found or deleted.seen.vm_found,
            was_running=was_running,
            datavolumes_deleted=datavolumes,
            claims_deleted=[claim.name for claim in claims],
            import_sources_deleted=sorted({claim.import_source for claim in claims if claim.import_source}),
            volumes_released=volumes,
            polls=gone.polls,
        )
