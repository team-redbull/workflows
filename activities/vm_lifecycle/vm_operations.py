"""What the worker inside a KV cluster does for create-vm: measure, create, wait, delete.

Each function here is one activity's whole body, over an `api` object
(kubevirt_api.KubevirtApi, or anything with its async surface) and the
cluster's own values as plain parameters — no settings, no Temporal
(CLAUDE.md §1). activities.py only binds them to this worker's settings and
to Temporal's heartbeat, so the same code is run by the unit tests and by the
fleet simulator (tests/kv_fleet.py), which stands dozens of fake KV clusters
side by side, one `api` and one ClusterValues each.

docs/design/create-vm-placement.md is the design: §6.3.3 for the figures,
§7.5 for the create.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from activities.vm_lifecycle import cluster_figures, vm_spec
from activities.vm_lifecycle.kubevirt_api import MacRefused, NameTaken, NotFound, bounded
from shared.exceptions import (
    KvClusterApiError,
    KvClusterMismatchError,
    VmDisappearedError,
    VmIdentityConflictError,
    VmLeaseExpiredError,
    VmMacNotAssignedError,
    VmNamespaceNotFoundError,
    VmRequestRejectedError,
    VmSerialExhaustedError,
)
from shared.infraenv_names import VM_SERIAL_LENGTH, InfraEnvNameError, vm_name
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

# Serials drawn per create attempt: a name some VM of the cluster has, or an AlreadyExists, spends one.
MAX_DRAWS = 3
# One create ATTEMPT starts no call to the cluster's API after this many seconds.
# With one call's own bound on top (kubevirt_api.REQUEST_TIMEOUT) the attempt is
# over before the workflow's start_to_close (90 s) lets Temporal start the next
# one — two attempts of one create never run side by side — and before the
# cancel rollback's settle wait ends. What is left undone is the retry's.
CREATE_BUDGET_SECONDS = 55.0
# The four list calls of a figures read are cut off here, together: the bound
# the hub's 60 s start_to_close and its zone lease are sized against.
FIGURES_TIMEOUT_SECONDS = 20.0
READY_POLL_SECONDS = 8.0
# The root disk is `done` in this DataVolume phase with its PVC bound.
ROOT_DISK_READY_PHASE = "Succeeded"
PVC_BOUND = "Bound"


@dataclass(frozen=True)
class ClusterValues:
    """What a KV cluster's worker knows about its own cluster (its ConfigMap, as plain values)."""

    kv_cluster: str
    site: str
    storage_class_name: str
    # The zone the cluster's name carries (`ocp4-prod-kv-five-az1-a`); None only where a
    # caller builds values without one, and then no request's zone is checked against it.
    zone: str | None = None
    redfish_label: tuple[str, str] = ("redfish-enabled", "true")
    network_name: str = "openshift-cnv/vm-external-network"
    node_selector: dict[str, str] = field(default_factory=dict)
    root_disk_size: str = "120Gi"

    @property
    def redfish_selector(self) -> str:
        return f"{self.redfish_label[0]}={self.redfish_label[1]}"


def _require_own_cluster(cluster: ClusterValues, kv_cluster: str, doing: str) -> None:
    if kv_cluster != cluster.kv_cluster:
        raise KvClusterMismatchError(
            f"this worker is deployed for KV cluster {cluster.kv_cluster} and was asked to {doing} for "
            f"{kv_cluster}: its `kvCluster` value is not the day1 file name its queue was built from"
        )


# --- read_kv_cluster_figures -----------------------------------------------------------------


async def read_figures(api: Any, cluster: ClusterValues, request: KvFiguresRequest) -> KvClusterFigures:
    """This cluster's figures, or `readable: false` with why. Never raises for the cluster's sake."""
    if request.kv_cluster != cluster.kv_cluster:
        return KvClusterFigures(
            kv_cluster=request.kv_cluster,
            readable=False,
            detail=bounded(
                f"the worker on this queue is deployed for KV cluster {cluster.kv_cluster}, not "
                f"{request.kv_cluster}: its `kvCluster` value is not the day1 file name"
            ),
        )
    reads = asyncio.gather(
        api.list_nodes(),
        api.list_unfinished_pods(cluster_figures.pod_usage),
        api.list_vms(cluster.redfish_selector),
        api.list_vmis(),
    )
    try:
        nodes, pods, vms, vmis = await asyncio.wait_for(reads, timeout=FIGURES_TIMEOUT_SECONDS)
        return cluster_figures.compute(
            kv_cluster=cluster.kv_cluster, nodes=nodes, pods=pods, vms=vms, vmis=vmis,
            redfish_label=cluster.redfish_label, node_selector=cluster.node_selector,
        )
    except asyncio.CancelledError:
        raise
    except asyncio.TimeoutError:
        why = f"its API did not answer the four list calls within {FIGURES_TIMEOUT_SECONDS:g}s"
    except Exception as err:  # noqa: BLE001 — unreadable is a VALUE here, whatever the cause
        why = f"{type(err).__name__}: {err}"
    return KvClusterFigures(kv_cluster=cluster.kv_cluster, readable=False, detail=bounded(why))


# --- create_vm_on_cluster ----------------------------------------------------------------------


def _created(cluster: ClusterValues, vm: dict, *, created: bool, draws: int) -> VmCreated:
    return VmCreated(
        name=vm_spec.name_of(vm), namespace=vm_spec.namespace_of(vm), mac=vm_spec.mac_of(vm) or "",
        cpu=vm_spec.cores(vm), memory_gib=vm_spec.memory_gib(vm), created=created,
        kv_cluster=cluster.kv_cluster, draws=draws,
    )


def _oldest_first(vm: dict) -> tuple[str, str, str]:
    return (
        str(cluster_figures.get(vm, "metadata", "creationTimestamp", default="")),
        vm_spec.namespace_of(vm), vm_spec.name_of(vm),
    )


async def _the_one_vm_of(
    api: Any, cluster: ClusterValues, request_id: str, found: list[dict], spend: Callable[[str], None],
    log: Callable[[str], None],
) -> dict | None:
    """The run's ONE VM among `found` (every VM labelled with its request id): the oldest.

    More than one can only come from a create the API server finished after
    its caller had given up on it — the caller's retry then saw no VM of the
    run and made another. Whichever attempt meets them converges on the
    oldest and deletes the rest (each by the UID read), so two attempts that
    met the same pair return the same VM, and no stopped VM of this run is
    left to weigh on the cluster for ever.
    """
    mine = sorted((vm for vm in found if vm_spec.created_by(vm) == request_id), key=_oldest_first)
    if not mine:
        return None
    for extra in mine[1:]:
        namespace, name = vm_spec.namespace_of(extra), vm_spec.name_of(extra)
        log(f"request {request_id} has {len(mine)} VMs on {cluster.kv_cluster}; keeping {vm_spec.name_of(mine[0])}, deleting {namespace}/{name}")
        spend(f"deleting the surplus VM {namespace}/{name}")
        await api.delete_vm(namespace, name, vm_spec.uid_of(extra))
        root = vm_spec.root_disk_name(extra)
        if root:
            spend(f"deleting the surplus VM's root disk {namespace}/{root}")
            await api.delete_datavolume(namespace, root)
    return mine[0]


async def create_vm(
    api: Any,
    cluster: ClusterValues,
    request: VmCreateRequest,
    *,
    now: Callable[[], datetime],
    random_bytes: Callable[[int], bytes],
    log: Callable[[str], None] = lambda _message: None,
    monotonic: Callable[[], float] = time.monotonic,
) -> VmCreated:
    """ONE Halted VM for the request: found (a retry), or drawn, named, built and created."""
    _require_own_cluster(cluster, request.kv_cluster, "create a VM")
    if request.site != cluster.site:
        raise KvClusterMismatchError(
            f"KV cluster {cluster.kv_cluster} is at site {cluster.site} and was asked for a VM of site "
            f"{request.site}: a VM boots on its own site's inventory network"
        )
    if cluster.zone is not None and request.zone != cluster.zone:
        raise KvClusterMismatchError(
            f"KV cluster {cluster.kv_cluster} is in zone {cluster.zone} and was asked for a VM of zone "
            f"{request.zone}: a VM goes into its InfraEnv's zone"
        )
    try:  # every field the name is built from, before anything is asked of the cluster
        vm_name(request.flavor, request.cpu, request.site, "0" * VM_SERIAL_LENGTH)
    except InfraEnvNameError as err:
        raise VmRequestRejectedError(f"the request cannot name a VM: {err}") from None

    started = monotonic()

    def spend(doing: str) -> None:
        """Before EVERY call to the cluster's API: this attempt starts none past its budget."""
        if monotonic() - started >= CREATE_BUDGET_SECONDS:
            raise KvClusterApiError(
                f"this create attempt on {cluster.kv_cluster} has run {CREATE_BUDGET_SECONDS:g}s and stops before "
                f"{doing}: the cluster's API is slow. The next attempt first looks for the VM this one may have made"
            )

    def require_lease() -> None:
        # The zone's lease: after it the lock has moved on and the next run has
        # measured this cluster without this VM, so nothing is created.
        if now() >= request.create_before:
            raise VmLeaseExpiredError(
                f"the zone lease this create was sent under ended at {request.create_before.isoformat()} "
                f"before the VM existed on {cluster.kv_cluster}: nothing was created. Ask again"
            )

    # 1 — has this run already created its VM? An earlier attempt may have, its
    # answer lost. Checked BEFORE the lease, so a retry finds it whatever the time.
    spend("listing the cluster's VMs")
    everything = await api.list_vms()
    existing = await _the_one_vm_of(api, cluster, request.request_id, everything, spend, log)
    if existing is not None:
        log(f"VM {vm_spec.namespace_of(existing)}/{vm_spec.name_of(existing)} already carries request {request.request_id}")
        await _require_mac(api, cluster, existing, spend)
        return _created(cluster, existing, created=False, draws=0)

    # 2 — the lease, then the namespace.
    require_lease()
    spend("reading the VM namespace")
    if not await api.namespace_exists(request.namespace):
        raise VmNamespaceNotFoundError(
            f"namespace {request.namespace} does not exist on KV cluster {cluster.kv_cluster}: the platform "
            "creates a VM namespace (a kubevirt-redfish chassis namespace), this run never does"
        )

    # 3-4 — draw a serial, name the VM after it, create it Halted with NO MAC: KubeMacPool
    # assigns one from its pool, unique on the cluster (the operator, 2026-11-25). A name any
    # VM of the cluster has, in any namespace (Kubernetes refuses a duplicate only within
    # one), or an AlreadyExists, is one more draw. Nothing races the check: the run holds
    # the zone's lock.
    taken = {vm_spec.name_of(vm) for vm in everything}
    spent: list[str] = []
    for draw in range(1, MAX_DRAWS + 1):
        serial = vm_spec.draw_serial(random_bytes)
        body = vm_spec.build(
            namespace=request.namespace, flavor=request.flavor, cpu=request.cpu, memory_gib=request.memory_gib,
            site=request.site, zone=request.zone, mce=request.mce, serial=serial, request_id=request.request_id,
            workflow_id=request.workflow_id, redfish_label=cluster.redfish_label,
            storage_class_name=cluster.storage_class_name, network_name=cluster.network_name,
            node_selector=cluster.node_selector, root_disk_size=cluster.root_disk_size,
        )
        name = vm_spec.name_of(body)
        if name in taken:
            spent.append(f"{name} (a VM of the cluster has it)")
            log(f"draw {draw}: {name} is taken on {cluster.kv_cluster}; drawing again")
            continue
        spend("creating the VM")
        require_lease()
        try:
            created = await api.create_vm(request.namespace, body)
        except NameTaken as err:
            taken.add(name)
            spent.append(f"{name} (it exists)")
            log(f"draw {draw}: {name} already exists ({err}); drawing again")
            continue
        except MacRefused as err:
            raise VmMacNotAssignedError(
                f"KubeMacPool on KV cluster {cluster.kv_cluster} refused to give {name} a MAC: {err}. "
                "Nothing was created"
            ) from None
        except NotFound as err:
            if not await api.namespace_exists(request.namespace):
                raise VmNamespaceNotFoundError(
                    f"namespace {request.namespace} vanished from KV cluster {cluster.kv_cluster} before the create"
                ) from None
            raise KvClusterApiError(
                f"KV cluster {cluster.kv_cluster} does not serve VirtualMachines (404: {err}): "
                "OpenShift Virtualization is not installed or not ready there"
            ) from None
        # 5 — read back (CLAUDE.md §6): the run must hold exactly ONE VM here, WITH the MAC
        # KubeMacPool gave it, before its name is recorded. A create this run's EARLIER
        # attempt was cut off from may have landed since the lookup above; then the older
        # of the two is the run's.
        spend("reading the run's VMs back")
        labelled = await api.list_vms(f"{vm_spec.CREATED_BY_LABEL}={request.request_id}")
        kept = await _the_one_vm_of(api, cluster, request.request_id, labelled, spend, log) or created
        await _require_mac(api, cluster, kept, spend)
        return _created(cluster, kept, created=vm_spec.name_of(kept) == name, draws=draw)
    raise VmSerialExhaustedError(
        f"no free VM name on KV cluster {cluster.kv_cluster} after {MAX_DRAWS} draws: {'; '.join(spent)}. "
        "Three taken names in a row out of 2.8 trillion serials means something else names VMs this way. "
        "Nothing was created"
    )


async def _require_mac(api: Any, cluster: ClusterValues, vm: dict, spend: Callable[[str], None]) -> None:
    """The VM carries the MAC KubeMacPool gave it; without one it is deleted and the create fails.

    A BareMetalHost needs a fixed boot MAC, and KubeVirt draws a new one on every start when
    none is set: a VM without one is KubeMacPool missing (not installed, or the namespace
    labelled `mutatevirtualmachines.kubemacpool.io=ignore`). Its VM and root disk are removed
    here, so a retry finds nothing of this run and the run leaves the cluster clean."""
    if vm_spec.mac_of(vm):
        return
    namespace, name = vm_spec.namespace_of(vm), vm_spec.name_of(vm)
    spend(f"deleting {namespace}/{name}, which came back without a MAC")
    await api.delete_vm(namespace, name, vm_spec.uid_of(vm))
    root = vm_spec.root_disk_name(vm)
    if root:
        await api.delete_datavolume(namespace, root)
    raise VmMacNotAssignedError(
        f"VM {namespace}/{name} on KV cluster {cluster.kv_cluster} came back with no MAC: KubeMacPool did not "
        f"assign one (it is not installed, or namespace {namespace} is labelled "
        "mutatevirtualmachines.kubemacpool.io=ignore). The VM was deleted; nothing of this run is left"
    )


# --- await_vm_ready ----------------------------------------------------------------------------


@dataclass(frozen=True)
class VmState:
    """The VM as one poll of the ready wait sees it."""

    mac: str | None
    admitted: bool
    root_disk: str | None
    root_disk_phase: str | None
    root_disk_bound: bool

    @property
    def ready(self) -> bool:
        return self.admitted and self.root_disk_bound and self.root_disk_phase == ROOT_DISK_READY_PHASE

    @property
    def detail(self) -> str:
        return (
            f"root disk {self.root_disk or '(none)'} phase {self.root_disk_phase}, "
            f"bound {self.root_disk_bound}, admitted {self.admitted}"
        )


async def vm_state(api: Any, namespace: str, name: str) -> VmState | None:
    """The VM and its root disk now; None when the VM is gone."""
    vm = await api.get_vm(namespace, name)
    if vm is None:
        return None
    root = vm_spec.root_disk_name(vm)
    phase: str | None = None
    bound = False
    if root is not None:
        datavolume = await api.get_datavolume(namespace, root)
        phase = cluster_figures.get(datavolume, "status", "phase") if datavolume else None
        bound = await api.pvc_phase(namespace, root) == PVC_BOUND
    return VmState(
        mac=vm_spec.mac_of(vm), admitted=vm_spec.admitted(vm), root_disk=root, root_disk_phase=phase, root_disk_bound=bound
    )


async def wait_ready(
    api: Any,
    cluster: ClusterValues,
    request: VmReadyRequest,
    *,
    started_at: datetime,
    polls: int,
    now: Callable[[], datetime],
    heartbeat: Callable[[datetime, int], None],
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    log: Callable[[str], None] = lambda _message: None,
) -> VmReadyOutcome:
    """Poll until the root disk is bound and the VM admitted, or `deadline_minutes` since `started_at`.

    `started_at` and `polls` come from the last heartbeat when this attempt
    resumes an earlier one's wait; `heartbeat` records them after every poll.
    """
    _require_own_cluster(cluster, request.kv_cluster, "wait for a VM")
    while True:
        state = await vm_state(api, request.namespace, request.name)
        polls += 1
        heartbeat(started_at, polls)
        if state is None:
            raise VmDisappearedError(
                f"VM {request.namespace}/{request.name} is gone from {cluster.kv_cluster} before its root disk bound"
            )
        outcome = VmReadyOutcome(
            result="done", root_disk_phase=state.root_disk_phase, root_disk_bound=state.root_disk_bound,
            admitted=state.admitted, mac=state.mac, detail=state.detail, polls=polls,
        )
        if state.ready:
            log(f"VM {request.namespace}/{request.name} ready on {cluster.kv_cluster} after {polls} polls: {state.detail}")
            return outcome
        elapsed = now() - started_at
        if elapsed.total_seconds() >= request.deadline_minutes * 60:
            log(f"VM {request.namespace}/{request.name} not ready on {cluster.kv_cluster} after {polls} polls ({elapsed}): {state.detail}")
            return outcome.model_copy(update={"result": "deadline"})
        await sleep(READY_POLL_SECONDS)


# --- delete_vm_on_cluster ----------------------------------------------------------------------


async def delete_vm(
    api: Any, cluster: ClusterValues, request: VmDeleteRequest, *, log: Callable[[str], None] = lambda _message: None
) -> VmDeleted:
    """Delete the run's own VM(s) — the ones labelled with its request id — and their root disks."""
    _require_own_cluster(cluster, request.kv_cluster, "delete a VM")
    if request.name:
        named = await api.get_vm(request.namespace, request.name)
        if named is not None:
            owner, mac = vm_spec.created_by(named), vm_spec.mac_of(named)
            if owner != request.request_id or (request.mac and mac != request.mac.lower()):
                raise VmIdentityConflictError(
                    f"VM {request.namespace}/{request.name} on {cluster.kv_cluster} carries request "
                    f"{owner or '(none)'} and MAC {mac or '(none)'}, not this run's {request.request_id} / "
                    f"{(request.mac or '(any)').lower()}: it is not this run's VM and was left alone"
                )
    mine = await api.list_vms(f"{vm_spec.CREATED_BY_LABEL}={request.request_id}")
    deleted: list[str] = []
    for vm in sorted(mine, key=vm_spec.name_of):
        if vm_spec.created_by(vm) != request.request_id:  # a selector the API did not honour
            continue
        namespace, name = vm_spec.namespace_of(vm), vm_spec.name_of(vm)
        root = vm_spec.root_disk_name(vm)
        gone = await api.delete_vm(namespace, name, vm_spec.uid_of(vm))
        if root:
            # Owned by the VM and garbage-collected anyway; explicit, idempotent.
            await api.delete_datavolume(namespace, root)
        if gone:
            deleted.append(f"{namespace}/{name}")
        log(f"VM {namespace}/{name} on {cluster.kv_cluster}: {'deleted' if gone else 'already gone'}")
    if not deleted:
        log(f"no VM of request {request.request_id} on {cluster.kv_cluster}: nothing to delete")
    return VmDeleted(deleted=bool(deleted), names=deleted)
