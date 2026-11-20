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
    VmMacExhaustedError,
    VmNamespaceNotFoundError,
    VmRequestRejectedError,
)
from shared.infraenv_names import InfraEnvNameError, vm_name
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

# MACs drawn per create attempt: a used MAC, an AlreadyExists and a KubeMacPool
# refusal each spend one.
MAX_DRAWS = 3
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
    mac_prefix: str
    storage_class_name: str
    redfish_label: tuple[str, str] = ("redfish-enabled", "true")
    network: dict[str, Any] = field(default_factory=dict)
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


async def create_vm(
    api: Any,
    cluster: ClusterValues,
    request: VmCreateRequest,
    *,
    now: Callable[[], datetime],
    random_bytes: Callable[[int], bytes],
    log: Callable[[str], None] = lambda _message: None,
) -> VmCreated:
    """ONE Halted VM for the request: found (a retry), or drawn, named, built and created."""
    _require_own_cluster(cluster, request.kv_cluster, "create a VM")
    if request.site != cluster.site:
        raise KvClusterMismatchError(
            f"KV cluster {cluster.kv_cluster} is at site {cluster.site} and was asked for a VM of site "
            f"{request.site}: a VM boots on its own site's inventory network"
        )
    try:  # every field the name is built from, before anything is asked of the cluster
        vm_name(request.flavor, request.cpu, request.site, vm_spec.draw_mac(cluster.mac_prefix, bytes(vm_spec.RANDOM_OCTETS)))
    except InfraEnvNameError as err:
        raise VmRequestRejectedError(f"the request cannot name a VM: {err}") from None

    # 1 — has this run already created its VM? An earlier attempt may have, its
    # answer lost. Checked BEFORE the lease, so a retry finds it whatever the time.
    everything = await api.list_vms()
    mine = sorted((vm for vm in everything if vm_spec.created_by(vm) == request.request_id), key=vm_spec.name_of)
    if mine:
        log(f"VM {vm_spec.namespace_of(mine[0])}/{vm_spec.name_of(mine[0])} already carries request {request.request_id}")
        return _created(cluster, mine[0], created=False, draws=0)

    def require_lease() -> None:
        # 2 — the zone's lease: after it the lock has moved on and the next run
        # has measured this cluster without this VM, so nothing is created.
        if now() >= request.create_before:
            raise VmLeaseExpiredError(
                f"the zone lease this create was sent under ended at {request.create_before.isoformat()} "
                f"before the VM existed on {cluster.kv_cluster}: nothing was created. Ask again"
            )

    require_lease()
    if not await api.namespace_exists(request.namespace):
        raise VmNamespaceNotFoundError(
            f"namespace {request.namespace} does not exist on KV cluster {cluster.kv_cluster}: the platform "
            "creates a VM namespace (a kubevirt-redfish chassis namespace), this run never does"
        )

    # 3-5 — draw, check the MAC against every VM of the cluster (any namespace:
    # Kubernetes refuses a duplicate NAME only within one), create Halted.
    taken = {mac for vm in everything for mac in vm_spec.macs(vm)}
    spent: list[str] = []
    for draw in range(1, MAX_DRAWS + 1):
        mac = vm_spec.draw_mac(cluster.mac_prefix, random_bytes(vm_spec.RANDOM_OCTETS))
        if mac in taken:
            spent.append(f"{mac} (a VM of the cluster has it)")
            log(f"draw {draw}: MAC {mac} is a VM's on {cluster.kv_cluster}; drawing again")
            continue
        body = vm_spec.build(
            namespace=request.namespace, flavor=request.flavor, cpu=request.cpu, memory_gib=request.memory_gib,
            site=request.site, zone=request.zone, mce=request.mce, mac=mac, request_id=request.request_id,
            workflow_id=request.workflow_id, redfish_label=cluster.redfish_label,
            storage_class_name=cluster.storage_class_name, network=cluster.network,
            node_selector=cluster.node_selector, root_disk_size=cluster.root_disk_size,
        )
        require_lease()
        try:
            created = await api.create_vm(request.namespace, body)
        except NameTaken as err:
            taken.add(mac)
            spent.append(f"{mac} (its name exists)")
            log(f"draw {draw}: {vm_spec.name_of(body)} already exists ({err}); drawing again")
            continue
        except MacRefused as err:
            taken.add(mac)
            spent.append(f"{mac} (KubeMacPool refused it)")
            log(f"draw {draw}: MAC {mac} was refused ({err}); drawing again")
            continue
        except NotFound as err:
            if not await api.namespace_exists(request.namespace):
                raise VmNamespaceNotFoundError(
                    f"namespace {request.namespace} vanished from KV cluster {cluster.kv_cluster} before the create"
                ) from None
            raise KvClusterApiError(
                f"KV cluster {cluster.kv_cluster} does not serve VirtualMachines (404: {err}): "
                "OpenShift Virtualization is not installed or not ready there"
            ) from None
        # The API echoes the object; fall back to what was sent if it echoed less.
        return _created(cluster, created if vm_spec.mac_of(created) else body, created=True, draws=draw)
    raise VmMacExhaustedError(
        f"no free MAC behind {cluster.mac_prefix} on KV cluster {cluster.kv_cluster} after {MAX_DRAWS} draws: "
        f"{'; '.join(spent)}. Three in a row out of 16.7 million means the prefix is wrong or shared. "
        "Nothing was created"
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
