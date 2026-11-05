"""create-vm — ONE VirtualMachine for a KubeVirt InfraEnv, on the best-fitting KubeVirt
cluster of the InfraEnv's zone, Halted, with a fixed MAC, ready for add-server-to-mce.

Under the naming convention an InfraEnv `<conf>[-<opt>]-zone-<zone>-<cpu>` holds
VMs of `<cpu>` vCPUs and `<cpu> × 8` GiB. A physical InfraEnv has servers to
draw; a KubeVirt one has nothing until a VM exists. This run creates it: once
it is Halted and labelled `redfish-enabled: "true"`, kubevirt-redfish exposes
it as a Redfish system and server-scan lists it as `ocp-<infraenv>-<suffix>`,
the name add-server-to-mce installs through Ironic.

Shape of the run:

  1. listing-kv-clusters  — the zone's KubeVirt clusters, from the day1 repo's
                            seed folder (`sites/<site>/mces/ocp4-seed-<site>-<zone>/
                            hostedClusters/`). None: NoKvClusterError.
  2. ranking-kv-clusters  — every cluster's kv-capacity agent read at once;
                            best-fit bin packing under the headroom buffer
                            (activities/vm_lifecycle/placement.py — fit per
                            node, buffer per cluster, Halted reservations
                            counted). Nothing fits: NoKvClusterFitsError
                            with the table summarised per reason.
  3. creating-vm          — a six-hex suffix drawn from the run's own random
                            (workflow.uuid4), skipped while its MAC tail is
                            any VM's in the zone, then ONE PUT on the chosen
                            cluster's kv-vm-api: `ocp-<infraenv>-<suffix>`,
                            MAC `<prefix>:xx:xx:xx`. A 409 (born in between)
                            redraws; at most _MAX_DRAWS draws, then
                            VmNameExhaustedError. The create is out-waited on
                            the chosen cluster and NEVER moved to another:
                            Halted, the scheduler is not consulted, and a
                            move after an ambiguous failure could leave two.
  4. awaiting-root-disk   — ONE heartbeating activity until the root disk's
                            DataVolume is Succeeded and bound and the VM
                            admitted, bounded (10 min, its own clock).
  5. removing-vm          — ONLY on that deadline: the run deletes ITS OWN
                            VM (by name and MAC), then fails with
                            VmRootDiskNotBoundError. A re-run of the slot is a
                            NEW execution with a NEW suffix, so an orphan would
                            never be converged on, only multiplied.
  6. completed            — name, MAC, namespace, size, cluster, the table.

The namespace is add-server-to-mce's `namespace`, verbatim: a kubevirt-redfish
chassis namespace the platform created. Nothing is created on the MCE.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from shared.cluster_names import is_kv_cluster_of, kv_cluster_prefix, seed_cluster
    from shared.consts import VM_LIFECYCLE_ACTIVITY_QUEUE
    from shared.exceptions import (
        KvAgentAuthError,
        NoKvClusterError,
        NoKvClusterFitsError,
        VmDisappearedError,
        VmIdentityConflictError,
        VmNameExhaustedError,
        VmNamespaceNotFoundError,
        VmRequestRejectedError,
        VmRootDiskNotBoundError,
    )
    from shared.infraenv_names import vm_name
    from shared.interfaces.vm_lifecycle import (
        await_vm_ready,
        create_vm_on_cluster,
        delete_vm_on_cluster,
        list_kv_clusters,
        rank_kv_clusters,
    )
    from shared.models.vm_lifecycle import (
        CreateVmProgress,
        CreateVmResult,
        CreateVmRunArgs,
        KvClusterList,
        KvClusterListRequest,
        KvClusterRef,
        KvPlacement,
        KvPlacementRequest,
        VmCreated,
        VmCreateRequest,
        VmDeleteRequest,
        VmReadyOutcome,
        VmReadyRequest,
    )

# One HTTP call to an agent (30 s in the limb) fits well inside this.
_ACTIVITY_TIMEOUT = timedelta(seconds=90)
# The capacity fan-out: 45 s in the limb for the whole zone.
_RANK_TIMEOUT = timedelta(seconds=60)
# A git clone, as every git activity in the repo.
_LIST_TIMEOUT = timedelta(seconds=180)
# The ready wait is ONE heartbeating activity on its own clock (CLAUDE.md §5):
# `deadline` after this, with start_to_close only a margin past it.
_READY_DEADLINE_MINUTES = 10
_READY_TIMEOUT = timedelta(minutes=12)
_READY_HEARTBEAT_TIMEOUT = timedelta(minutes=2)
# The short-run "no worker polls this queue" signal — on the two READS before
# anything is written ONLY. Temporal never retries this timeout, so after the
# PUT a limb pod away for five minutes must not fail the run, or its rollback,
# and orphan the VM: the create, the wait and the delete wait for a worker.
_SCHEDULE_TO_START_TIMEOUT = timedelta(minutes=5)
# Suffix draws per run: a drawn MAC already in the zone and a 409 each spend one.
_MAX_DRAWS = 3

# Activity-raised failures no retry can fix, as CLASSES (CLAUDE.md §5).
_PERMANENT_ACTIVITY_ERRORS: tuple[type[Exception], ...] = (
    KvAgentAuthError,
    VmIdentityConflictError,
    VmNamespaceNotFoundError,
    VmRequestRejectedError,
    VmDisappearedError,
)
_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
    non_retryable_error_types=[error.__name__ for error in _PERMANENT_ACTIVITY_ERRORS],
)

# Operator wording for each skip reason the table records — the mirror of
# activities/vm_lifecycle/placement.SKIP_REASONS (the workflow cannot import the
# limb; a test keeps the two equal, the _REJECTION_SUMMARY precedent).
_SKIP_SUMMARY: dict[str, str] = {
    "unreachable": "could not be read: the cluster, its ingress or its kv-capacity agent did not answer",
    "no-agent": "have no kv-capacity agent behind their route yet (deploy the agents there first)",
    "refused-token": "refused the kv-capacity token (KV_CAPACITY_TOKEN is not theirs)",
    "malformed": "answered something that is not a capacity report (an agent/contract mismatch)",
    "no-nodes": "have no schedulable node (none Ready and uncordoned)",
    "fit-node": "have no schedulable node with the headroom for this VM's pod request (CPU or memory)",
    "fit-buffer": "would go past the headroom buffer on CPU or memory with this VM on them",
}


def mac_tail(suffix_hex: str) -> str:
    """`0a1b2c` -> `0a:1b:2c`: the last three octets a suffix becomes in the VM's MAC."""
    return f"{suffix_hex[0:2]}:{suffix_hex[2:4]}:{suffix_hex[4:6]}"


def suffix_is_taken(suffix_hex: str, known_macs: list[str]) -> bool:
    """Whether a drawn suffix's MAC tail is any VM's in the zone.

    Compared on the LAST THREE OCTETS only: the workflow never sees
    VM_MAC_PREFIX (a limb setting), and every MAC we make carries it, so a
    tail already in use under any prefix is refused — stricter, never looser,
    than comparing whole MACs.
    """
    tail = mac_tail(suffix_hex)
    return any(mac.lower()[-8:] == tail for mac in known_macs)


def summarise_skips(placement: KvPlacement) -> str:
    """One line per skip reason naming the clusters, for NoKvClusterFitsError."""
    by_reason: dict[str, list[str]] = {}
    for row in placement.ranked:
        if not row.fits:
            by_reason.setdefault(row.skip_reason or "unknown", []).append(row.name)
    return "; ".join(
        f"{', '.join(names)} {_SKIP_SUMMARY.get(reason, reason)}" for reason, names in by_reason.items()
    )


@workflow.defn
class CreateVmWorkflow:
    def __init__(self) -> None:
        self._phase = "pending"
        self._candidates: list[str] = []
        self._placement: KvPlacement | None = None
        self._kv_cluster: str | None = None
        self._vm_name: str | None = None
        self._mac: str | None = None
        self._redraws = 0

    @workflow.query
    def progress(self) -> CreateVmProgress:
        return CreateVmProgress(
            phase=self._phase, kv_candidates=list(self._candidates), placement=self._placement,
            kv_cluster=self._kv_cluster, vm_name=self._vm_name, mac=self._mac, redraws=self._redraws,
        )

    @workflow.run
    async def run(self, run_args: CreateVmRunArgs) -> CreateVmResult:
        request = run_args.input

        # Step 1 — the zone's KubeVirt clusters, from day1.
        self._phase = "listing-kv-clusters"
        listed: KvClusterList = await workflow.execute_activity(
            list_kv_clusters,
            KvClusterListRequest(site=request.site, zone=request.zone),
            task_queue=VM_LIFECYCLE_ACTIVITY_QUEUE,
            start_to_close_timeout=_LIST_TIMEOUT,
            schedule_to_start_timeout=_SCHEDULE_TO_START_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )
        # Defensive: the seed folder should hold only KubeVirt clusters, and a
        # file there named otherwise is not one the agents are deployed on.
        candidates = [
            KvClusterRef(
                name=entry.name, cluster_name=entry.cluster_name, hosted_cluster=entry.hosted_cluster,
                site=request.site, zone=request.zone,
            )
            for entry in listed.clusters
            if is_kv_cluster_of(entry.name, request.site, request.zone)
        ]
        self._candidates = [ref.name for ref in candidates]
        for entry in listed.clusters:
            for problem in entry.problems:
                workflow.logger.warning("day1 %s: %s", entry.path, problem)
        if not candidates:
            folder = f"sites/{request.site}/mces/{seed_cluster(request.site, request.zone)}/hostedClusters/"
            raise ApplicationError(
                f"day1 {listed.revision} lists no KubeVirt cluster for site {request.site} zone "
                f"{request.zone}: nothing named {kv_cluster_prefix(request.site, request.zone)}… in "
                f"{folder} ({len(listed.clusters)} files there). Nothing was created.",
                type=NoKvClusterError.__name__,
            )

        # Step 2 — read every cluster, rank, choose.
        self._phase = "ranking-kv-clusters"
        placement: KvPlacement = await workflow.execute_activity(
            rank_kv_clusters,
            KvPlacementRequest(candidates=candidates, cpu=request.cpu, memory_gib=request.memory_gib),
            task_queue=VM_LIFECYCLE_ACTIVITY_QUEUE,
            start_to_close_timeout=_RANK_TIMEOUT,
            schedule_to_start_timeout=_SCHEDULE_TO_START_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )
        self._placement = placement
        if placement.chosen is None:
            raise ApplicationError(
                f"no KubeVirt cluster of site {request.site} zone {request.zone} takes a VM of "
                f"{request.cpu} vCPU / {request.memory_gib} GiB (pod request {placement.pod_cpu_millicores} m / "
                f"{placement.pod_memory_bytes // 1024**3} GiB) under the {placement.buffer_percent}% buffer: "
                f"{summarise_skips(placement)}. Nothing was created.",
                type=NoKvClusterFitsError.__name__,
            )
        chosen_ref = next((ref for ref in candidates if ref.name == placement.chosen), None)
        chosen_row = next((row for row in placement.ranked if row.name == placement.chosen), None)
        if chosen_ref is None or chosen_row is None:
            # A limb answering outside its request. Failed, not hung: a bare
            # StopIteration here would fail the workflow TASK for ever (§5).
            raise ApplicationError(
                f"placement chose {placement.chosen!r}, which is not one of the zone's candidates "
                f"({', '.join(self._candidates)}) — a vm-lifecycle limb bug. Nothing was created.",
                type=NoKvClusterFitsError.__name__,
            )
        chosen = chosen_ref.model_copy(update={"agent_host": chosen_row.agent_host})
        self._kv_cluster = chosen.name

        # Step 3 — the VM, under a suffix nothing in the zone uses.
        self._phase = "creating-vm"
        created = await self._create(request, chosen, placement.known_macs)
        self._vm_name, self._mac = created.name, created.mac

        # Step 4 — its root disk, bounded.
        self._phase = "awaiting-root-disk"
        ready: VmReadyOutcome = await workflow.execute_activity(
            await_vm_ready,
            VmReadyRequest(
                kv_cluster=chosen, namespace=request.namespace, name=created.name,
                deadline_minutes=_READY_DEADLINE_MINUTES,
            ),
            task_queue=VM_LIFECYCLE_ACTIVITY_QUEUE,
            start_to_close_timeout=_READY_TIMEOUT,
            heartbeat_timeout=_READY_HEARTBEAT_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )
        if ready.result != "done":
            # Step 5 — nothing survives on a cluster that did not take the VM.
            self._phase = "removing-vm"
            await workflow.execute_activity(
                delete_vm_on_cluster,
                VmDeleteRequest(kv_cluster=chosen, namespace=request.namespace, name=created.name, mac=created.mac),
                task_queue=VM_LIFECYCLE_ACTIVITY_QUEUE,
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
                retry_policy=_RETRY_POLICY,
            )
            raise ApplicationError(
                f"VM {request.namespace}/{created.name} on {chosen.name} showed no bound root disk within "
                f"{_READY_DEADLINE_MINUTES} min ({ready.detail}). The VM was deleted again: a re-run draws a "
                "new name and could never resume this one. A WaitForFirstConsumer storage class never binds "
                "a Halted VM's disk — the VM's storage class must bind Immediately; otherwise check the "
                "DataVolume and the CDI log on that cluster",
                type=VmRootDiskNotBoundError.__name__,
            )

        self._phase = "completed"
        workflow.logger.info(
            "VM %s/%s on %s: mac %s, %d vCPU / %d GiB, root disk %s (%d redraws)",
            request.namespace, created.name, chosen.name, created.mac, request.cpu, request.memory_gib,
            ready.root_disk_phase, self._redraws,
        )
        return CreateVmResult(
            vm_name=created.name, mac=created.mac, namespace=request.namespace, cpu=request.cpu,
            memory_gib=request.memory_gib, kv_cluster=chosen, placement=placement, redraws=self._redraws,
            root_disk_phase=ready.root_disk_phase, created=created.created, infra_env=request.infra_env,
            mce_cluster=request.mce_cluster, site=request.site, zone=request.zone,
        )

    async def _create(self, request, chosen: KvClusterRef, known_macs: list[str]) -> VmCreated:
        """Up to _MAX_DRAWS suffixes; each PUT is out-waited on the chosen cluster, never moved."""
        for _ in range(_MAX_DRAWS):
            suffix = workflow.uuid4().hex[-6:]
            if suffix_is_taken(suffix, known_macs):
                self._redraws += 1
                workflow.logger.info("suffix %s is a MAC already in the zone; redrawing", suffix)
                continue
            name = vm_name(request.infra_env, suffix)
            self._vm_name = name
            try:
                return await workflow.execute_activity(
                    create_vm_on_cluster,
                    VmCreateRequest(
                        kv_cluster=chosen, namespace=request.namespace, name=name, cpu=request.cpu,
                        memory_gib=request.memory_gib, suffix_hex=suffix,
                    ),
                    task_queue=VM_LIFECYCLE_ACTIVITY_QUEUE,
                    start_to_close_timeout=_ACTIVITY_TIMEOUT,
                    retry_policy=_RETRY_POLICY,
                )
            except ActivityError as err:
                cause = err.cause
                if not (isinstance(cause, ApplicationError) and cause.type == VmIdentityConflictError.__name__):
                    raise
                # The race backstop: a VM born under this name or MAC between
                # the capacity read and the PUT. Not ours — draw again.
                self._redraws += 1
                workflow.logger.info("VM %s is held by another identity on %s (%s); redrawing", name, chosen.name,
                                     cause.message)
        raise ApplicationError(
            f"every one of {_MAX_DRAWS} drawn suffixes for VM ocp-{request.infra_env}-… was taken on "
            f"{chosen.name} or in the zone's MAC set ({self._redraws} redraws). Nothing of this run survives "
            "on the cluster; run it again for a fresh draw",
            type=VmNameExhaustedError.__name__,
        )
