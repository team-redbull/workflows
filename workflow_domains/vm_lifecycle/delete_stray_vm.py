"""delete-stray-vm — remove a create-vm run's VM from a KV cluster the run moved on from.

A create-vm run that cannot create its VM on a KV cluster moves on to the
next cluster of its zone (create_vm.py). Before it does, it makes sure the
cluster holds no VM of the run: once the create's deadline has passed the
cluster's worker creates nothing more for that run, and the run asks the
cluster for any VM labelled with its run id. When the cluster cannot even
be asked — its API or its worker is down — nobody can know whether the
create landed. The run then goes on anyway (the operator, 2026-11-24:
"move on, clean up later") and starts THIS workflow, which asks that
cluster's worker to delete every VM labelled with the run's id, and waits
for as long as it takes the cluster to answer.

So a run never KEEPS two VMs. For the length of the outage a VM of the run
MAY exist on the cluster that failed: Halted, used by nobody (the run's
result names the other one), and counted against that cluster's capacity
like any stopped VM — then it is deleted.

  deleting     delete_vm_on_cluster on the cluster's own queue, by the run-id
               label, in every namespace. Retried without bound: an outage is
               out-waited, and there is no schedule-to-start limit, so a
               cluster whose worker is gone keeps this run waiting for it —
               visible as a pending activity. A refusal no retry can fix (the
               worker's role lacks a verb, a worker deployed for another
               cluster) FAILS the run, naming the label to delete by hand.
  completed    `deleted` False: there was nothing — the create had never landed.

No route starts it: create-vm does, as an ABANDONed child on
`delete_stray_vm_workflow_id(cluster, run id)`, after the cluster's create
deadline, so nothing of the run can land there after this run's delete.
Nothing in it can delete another run's VM: the label is one run's id, and the
run's own VM is on another cluster's API.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from shared.consts import vm_lifecycle_activity_queue
    from shared.exceptions import KvClusterAccessError, KvClusterMismatchError, VmRequestRejectedError
    from shared.interfaces.vm_lifecycle import delete_vm_on_cluster
    from shared.models.vm_lifecycle import (
        DeleteStrayVmProgress,
        DeleteStrayVmResult,
        DeleteStrayVmRunArgs,
        VmDeleted,
        VmDeleteRequest,
    )

_DELETE_TIMEOUT = timedelta(seconds=90)
# What no retry fixes, as CLASSES (CLAUDE.md §5). Everything else is the cluster's
# outage, out-waited.
_PERMANENT_ACTIVITY_ERRORS: tuple[type[Exception], ...] = (
    KvClusterAccessError,
    KvClusterMismatchError,
    VmRequestRejectedError,
)
_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=5),
    non_retryable_error_types=[error.__name__ for error in _PERMANENT_ACTIVITY_ERRORS],
)


@workflow.defn
class DeleteStrayVmWorkflow:
    @workflow.init
    def __init__(self, run_args: DeleteStrayVmRunArgs) -> None:
        self._phase = "pending"
        self._kv_cluster = run_args.input.kv_cluster
        self._request_id = run_args.input.request_id

    @workflow.query
    def progress(self) -> DeleteStrayVmProgress:
        return DeleteStrayVmProgress(phase=self._phase, kv_cluster=self._kv_cluster, request_id=self._request_id)

    @workflow.run
    async def run(self, run_args: DeleteStrayVmRunArgs) -> DeleteStrayVmResult:
        request = run_args.input
        self._phase = "deleting"
        workflow.logger.info(
            "removing any VM of create-vm run %s (%s) from %s, which failed its create: %s",
            request.request_id, request.workflow_id, request.kv_cluster, request.why,
        )
        deleted: VmDeleted = await workflow.execute_activity(
            delete_vm_on_cluster,
            VmDeleteRequest(kv_cluster=request.kv_cluster, namespace=request.namespace, request_id=request.request_id),
            task_queue=vm_lifecycle_activity_queue(request.kv_cluster),
            start_to_close_timeout=_DELETE_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )
        self._phase = "completed"
        workflow.logger.info(
            "%s: %s", request.kv_cluster,
            f"deleted {', '.join(deleted.names)}" if deleted.deleted else "no VM of the run was there",
        )
        return DeleteStrayVmResult(
            kv_cluster=request.kv_cluster, request_id=request.request_id, deleted=deleted.deleted, names=deleted.names,
        )
