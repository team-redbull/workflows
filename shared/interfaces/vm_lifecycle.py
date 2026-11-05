"""VM-lifecycle activity signatures — the domain's ONE hub limb, `vm-lifecycle-worker`.

Everything here runs on one hub deployment polling VM_LIFECYCLE_ACTIVITY_QUEUE.
It holds no Kubernetes credential: each KubeVirt cluster is reached through
its two agents behind Routes (kv-capacity, read-only facts; kv-vm-api, the
idempotent VM create/read/delete), one fleet bearer token each, and the
zone's cluster list comes from the day1 repo with the day1 git token
(activities/vm_lifecycle/day1_repo.py on activities/day1_repo.py, the monorepo's
shared day1 reader).
"""

from __future__ import annotations

from temporalio import activity

from shared.models.vm_lifecycle import (
    KvClusterList,
    KvClusterListRequest,
    KvPlacement,
    KvPlacementRequest,
    VmCreated,
    VmCreateRequest,
    VmDeleted,
    VmDeleteRequest,
    VmReadyOutcome,
    VmReadyRequest,
)


@activity.defn
async def list_kv_clusters(request: KvClusterListRequest) -> KvClusterList:
    """The KubeVirt clusters of (site, zone), as day1 `main` lists them.

    One fresh clone per attempt; the seed folder
    `sites/<site>/mces/ocp4-seed-<site>-<zone>/hostedClusters/` is built from
    the validated tokens, and each file there is one entry with the chart's
    `clusterName` (its own key, else the file name minus the prefix). A
    missing folder is an EMPTY list, never an error. Observe-only. Raises the
    retryable ValuesRepoGitError / ValuesBranchNotFoundError (unreachable or
    no `main`: waited out, as cluster-upgrade does).
    """
    ...


@activity.defn
async def rank_kv_clusters(request: KvPlacementRequest) -> KvPlacement:
    """Every candidate's kv-capacity report, ranked for a VM of (cpu, memory_gib).

    Fans out to every candidate's agent at once; a cluster that cannot be
    read is a row with its skip reason (unreachable, no-agent, refused-token,
    malformed), never an exception, and `known_macs` unions every MAC the
    reachable reports list. Observe-only. Raises KvCapacityUnavailableError
    (retried) when NO cluster could be read, KvAgentAuthError (permanent)
    when every reachable one refused the token.
    """
    ...


@activity.defn
async def create_vm_on_cluster(request: VmCreateRequest) -> VmCreated:
    """PUT the VM on the chosen cluster's kv-vm-api: Halted, labelled for kubevirt-redfish.

    Idempotent by NAME and identity: the agent answers 201 on create and 200
    when a VM of this name with the same namespace, MAC, size and labels
    already exists (`created` False — a retry finding its own earlier PUT).
    The MAC is `VM_MAC_PREFIX:<suffix_hex as xx:xx:xx>`. Raises, permanent:
    VmIdentityConflictError (409, a different VM under the name),
    VmNamespaceNotFoundError (404), VmRequestRejectedError (422),
    KvAgentAuthError (401/403); transient: KvVmApiUnavailableError.
    """
    ...


@activity.defn
async def await_vm_ready(request: VmReadyRequest) -> VmReadyOutcome:
    """ONE heartbeating wait until the VM's root disk is bound and the VM admitted, or the deadline.

    Polls kv-vm-api every few seconds, heartbeats each poll with its start
    time so a retry on another worker keeps the clock, and returns `done` or
    `deadline` with the last root-disk phase — never raises for "not yet".
    Raises VmDisappearedError (404, permanent) and KvVmApiUnavailableError
    (transient; the retry resumes the clock).
    """
    ...


@activity.defn
async def delete_vm_on_cluster(request: VmDeleteRequest) -> VmDeleted:
    """DELETE the run's own VM (and its DataVolume) by name AND MAC on the chosen cluster.

    Idempotent: 204 is `deleted` True, 404 (already gone) `deleted` False —
    both success. The MAC guards it: the agent deletes only a VM carrying the
    caller's MAC, and answers 409 for any other, raised as
    VmIdentityConflictError (permanent — someone replaced the VM, leave it).
    KvAgentAuthError (401/403) permanent; KvVmApiUnavailableError transient.
    """
    ...
