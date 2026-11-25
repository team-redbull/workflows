"""VM-lifecycle activity signatures — the worker that runs INSIDE each KV cluster, `vm-lifecycle-worker`.

One deployment per KubeVirt (KV) cluster, polling
`vm_lifecycle_activity_queue(<its cluster>)` (shared/consts.py) and acting on
its own API server as its own ServiceAccount; it dials out to the hub's
Temporal and exposes nothing. So the queue a call is sent on IS the cluster
it acts on, and every request also names that cluster so a worker deployed
under the wrong name refuses instead of acting.

The zone's cluster list is NOT here: it is day1's, read by the
cluster-lifecycle limb (shared/interfaces/cluster_lifecycle.py, list_kv_clusters).
"""

from __future__ import annotations

from temporalio import activity

from shared.models.vm_lifecycle import (
    KvClusterFigures,
    KvFiguresRequest,
    VmCreated,
    VmCreateRequest,
    VmDeleted,
    VmDeleteRequest,
    VmReadyOutcome,
    VmReadyRequest,
)


@activity.defn
async def read_kv_cluster_figures(request: KvFiguresRequest) -> KvClusterFigures:
    """This KV cluster's capacity figures, measured now: allocatable over the usable nodes,
    what pods request, what the stopped `redfish-enabled` VMs will request, each usable
    node's headroom as a CPU and memory pair.

    Observe-only: four list calls at once (nodes, unfinished pods, labelled
    VirtualMachines, VirtualMachineInstances), each bounded at 20 s. NEVER
    raises for the cluster's sake: an API it could not read, or a request
    naming another cluster than the worker's own, is answered `readable:
    false` with a bounded `detail` — the caller holds the zone's lock and must
    neither retry nor wait.
    """
    ...


@activity.defn
async def create_vm_on_cluster(request: VmCreateRequest) -> VmCreated:
    """ONE Halted VirtualMachine for the request, its serial drawn and its name derived HERE.

    Idempotent by the request id: a VM already labelled with it (an earlier
    attempt whose answer was lost) is returned, `created` False, whatever the
    time. Otherwise nothing is created after `create_before`
    (VmLeaseExpiredError). Then up to three draws of an 8-character serial,
    the name `ocp-<flavor>-kv<cpu>-<site>-<serial>` (the serial also the VM's
    SMBIOS serial): a name any VM of the cluster has, or an AlreadyExists,
    draws again. The VM is created with NO MAC: KubeMacPool assigns one,
    unique on the cluster, and the VM read back must carry it — one without is
    deleted (VmMacNotAssignedError). Raises VmSerialExhaustedError (three
    draws taken), VmMacNotAssignedError, VmLeaseExpiredError, VmNamespaceNotFoundError, VmRequestRejectedError
    (400/422), KvClusterMismatchError (another cluster's or site's request),
    KvClusterAccessError (403) — all permanent — and the retryable
    KvClusterApiError (5xx, 401, a timeout).
    """
    ...


@activity.defn
async def await_vm_ready(request: VmReadyRequest) -> VmReadyOutcome:
    """Until the VM's root disk is bound and the VM admitted, or the deadline on its OWN clock.

    ONE heartbeating activity (CLAUDE.md §5): a poll every 8 s, a heartbeat
    per poll, the clock kept in the heartbeat details so a retry on a
    restarted worker resumes it. Returns `done` or `deadline` (with the last
    state seen) — the deadline is a result, never an exception. Raises
    VmDisappearedError (permanent) when the VM is gone, KvClusterMismatchError
    and KvClusterAccessError (permanent), the retryable KvClusterApiError.
    """
    ...


@activity.defn
async def delete_vm_on_cluster(request: VmDeleteRequest) -> VmDeleted:
    """Delete the run's own VM and its root DataVolume: the VM labelled with the request id.

    Idempotent: nothing there is `deleted` False, success. With `name` and
    `mac`, a VM of that name carrying another MAC or another request id is
    someone else's and is left alone (VmIdentityConflictError, permanent).
    Raises KvClusterMismatchError and KvClusterAccessError (permanent), the
    retryable KvClusterApiError.
    """
    ...
