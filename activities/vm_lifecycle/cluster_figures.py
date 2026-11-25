"""One KV cluster's capacity figures, from what its own API lists — pure arithmetic.

docs/design/create-vm-placement.md §6.2-6.3 is the design; this is its one
implementation. No settings, no Temporal, no I/O: vm_operations.py hands it
the lists kubevirt_api.py read (plain JSON objects, as the API server sends
them), and the unit tests hand it fake ones.

  1. USABLE NODES: Ready, not cordoned, carrying no NoSchedule/NoExecute taint,
     and matching the VM template's nodeSelector when the cluster has one —
     that is, the nodes a VM's launcher pod can be scheduled to. Nothing else
     counts, on either side of the fraction.
  2. PODS: each unfinished pod's request, added to the node it is bound to,
     for usable nodes only, counted the way the kube-scheduler counts it:
     per resource, the larger of (its containers plus its sidecar init
     containers) and its largest ordinary init container (with the sidecars
     started before it), plus the pod's overhead; a pod-level request, where
     the pod states one, replaces the containers' sum.
  3. STOPPED VMs: every VM carrying the redfish label, in any namespace and
     with any run strategy, whose instance is not Scheduled, Running or
     Unknown — those three have a launcher pod on a node, already counted in
     2. A stopped VM has no pod yet, but kubevirt-redfish will start it, so
     its room is taken: it is counted at what its pod WILL request
     (shared/vm_requests.py: vCPUs × 15/16, at least a core; guest memory +
     KubeVirt's overhead for a VM of its vCPUs and memory). THIS IS HOW A VM
     CREATED A MOMENT AGO IS COUNTED, and why the zone needs no memory of its
     own between runs.
  4. THE FIGURES: allocatable summed over usable nodes; pods summed; stopped
     VMs counted and summed; each usable node's headroom (allocatable minus
     its pods) as a CPU and memory PAIR.

Every quantity is parsed with the client's own `parse_quantity` and turned
into integer millicores and bytes right here; nothing leaves as a string.
"""

from __future__ import annotations

from decimal import ROUND_CEILING, Decimal
from typing import Any, NamedTuple

from kubernetes.utils import parse_quantity

from shared.models.vm_lifecycle import KvClusterFigures, KvNodeHeadroom
from shared.vm_requests import pod_cpu_millicores, pod_memory_bytes_of_guest

# VMI phases with a launcher pod bound to a node — its request is in the pod
# sum already. Pending and Scheduling have none bound yet, Succeeded and Failed
# none left: those VMs are counted as stopped.
_PHASES_WITH_A_POD_ON_A_NODE = frozenset({"Scheduled", "Running", "Unknown"})
_FINISHED_POD_PHASES = frozenset({"Succeeded", "Failed"})
# Taint effects that keep a pod without a toleration off a node. The VM
# template carries no toleration, so a node with either is not capacity.
_REPELLING_EFFECTS = frozenset({"NoSchedule", "NoExecute"})


class PodUsage(NamedTuple):
    """One pod reduced to what the figures need: where it is and what it requests."""

    node: str | None
    cpu_millicores: int
    memory_bytes: int


def get(obj: Any, *path: str, default: Any = None) -> Any:
    """`get(vm, "spec", "template", "spec", "domain")` over nested dicts, None-safe."""
    for key in path:
        if not isinstance(obj, dict) or key not in obj:
            return default
        obj = obj[key]
    return obj if obj is not None else default


def millicores(quantity: Any) -> int:
    """`"7"` -> 7000, `"500m"` -> 500, `"0.5"` -> 500, `"1.5"` -> 1500, None -> 0 (rounded up, as Kubernetes does)."""
    if quantity is None or quantity == "":
        return 0
    return int((parse_quantity(str(quantity)) * 1000).to_integral_value(rounding=ROUND_CEILING))


def to_bytes(quantity: Any) -> int:
    """`"128Gi"` -> 137438953472, `"1000M"` -> 1000000000, None -> 0 (rounded up)."""
    if quantity is None or quantity == "":
        return 0
    value: Decimal = parse_quantity(str(quantity))
    return int(value.to_integral_value(rounding=ROUND_CEILING))


# --- nodes ----------------------------------------------------------------------------------


def _ready(node: dict) -> bool:
    for condition in get(node, "status", "conditions", default=[]) or []:
        if isinstance(condition, dict) and condition.get("type") == "Ready":
            return condition.get("status") == "True"
    return False


def _repelled(node: dict) -> bool:
    return any(
        isinstance(taint, dict) and taint.get("effect") in _REPELLING_EFFECTS
        for taint in get(node, "spec", "taints", default=[]) or []
    )


def usable(node: dict, node_selector: dict[str, str]) -> bool:
    """Whether a VM's launcher pod could be scheduled to this node at all."""
    if not _ready(node) or bool(get(node, "spec", "unschedulable", default=False)) or _repelled(node):
        return False
    labels = get(node, "metadata", "labels", default={}) or {}
    return all(labels.get(key) == value for key, value in node_selector.items())


# --- pods -----------------------------------------------------------------------------------


def _requests(container: dict) -> tuple[int, int]:
    requests = get(container, "resources", "requests", default={}) or {}
    return millicores(requests.get("cpu")), to_bytes(requests.get("memory"))


def pod_request(pod: dict) -> tuple[int, int]:
    """(cpu millicores, memory bytes) the kube-scheduler counts for this pod.

    Kubernetes' own rule (`resourcehelper.PodRequests`): init containers run
    one at a time, so an ordinary one needs its own request plus the sidecars
    (init containers with `restartPolicy: Always`) started before it; the
    running pod needs every container plus every sidecar; the pod needs the
    larger of the two, plus its RuntimeClass overhead. A pod-level request
    (`spec.resources.requests`) replaces the containers' side for the
    resource it names.
    """
    total = [0, 0]
    for container in get(pod, "spec", "containers", default=[]) or []:
        cpu, memory = _requests(container)
        total[0] += cpu
        total[1] += memory
    sidecars = [0, 0]
    init_peak = [0, 0]
    for container in get(pod, "spec", "initContainers", default=[]) or []:
        cpu, memory = _requests(container)
        if container.get("restartPolicy") == "Always":
            sidecars[0] += cpu
            sidecars[1] += memory
            # A sidecar starting is itself a moment the pod must fit.
            init_peak = [max(init_peak[0], sidecars[0]), max(init_peak[1], sidecars[1])]
        else:
            init_peak = [max(init_peak[0], cpu + sidecars[0]), max(init_peak[1], memory + sidecars[1])]
    result = [max(total[0] + sidecars[0], init_peak[0]), max(total[1] + sidecars[1], init_peak[1])]
    pod_level = get(pod, "spec", "resources", "requests", default={}) or {}
    if pod_level.get("cpu") is not None:
        result[0] = millicores(pod_level.get("cpu"))
    if pod_level.get("memory") is not None:
        result[1] = to_bytes(pod_level.get("memory"))
    overhead = get(pod, "spec", "overhead", default={}) or {}
    return result[0] + millicores(overhead.get("cpu")), result[1] + to_bytes(overhead.get("memory"))


def pod_usage(pod: dict) -> PodUsage | None:
    """The pod as the figures need it; None for a finished pod (the API's field selector
    leaves those out already — checked again so a list without the selector is still right)."""
    if get(pod, "status", "phase") in _FINISHED_POD_PHASES:
        return None
    cpu, memory = pod_request(pod)
    return PodUsage(node=get(pod, "spec", "nodeName"), cpu_millicores=cpu, memory_bytes=memory)


# --- VMs ------------------------------------------------------------------------------------


def vcpus(vm: dict) -> int:
    """cores × sockets × threads, each 1 when the VM does not say."""
    cpu = get(vm, "spec", "template", "spec", "domain", "cpu", default={}) or {}
    return max(1, int(cpu.get("cores") or 1)) * max(1, int(cpu.get("sockets") or 1)) * max(1, int(cpu.get("threads") or 1))


def guest_memory_bytes(vm: dict) -> int:
    """`memory.guest`, else the memory request, in bytes; 0 when the VM states neither."""
    domain = get(vm, "spec", "template", "spec", "domain", default={}) or {}
    guest = get(domain, "memory", "guest")
    if guest is not None:
        return to_bytes(guest)
    return to_bytes(get(domain, "resources", "requests", "memory"))


def stopped_vm_request(vm: dict) -> tuple[int, int]:
    """(cpu millicores, memory bytes) this VM's launcher pod will request when it starts."""
    cpu = vcpus(vm)
    return pod_cpu_millicores(cpu), pod_memory_bytes_of_guest(cpu, guest_memory_bytes(vm))


def _key(obj: dict) -> tuple[str, str]:
    return str(get(obj, "metadata", "namespace", default="")), str(get(obj, "metadata", "name", default=""))


def has_label(obj: dict, label: tuple[str, str]) -> bool:
    return (get(obj, "metadata", "labels", default={}) or {}).get(label[0]) == label[1]


def stopped_vms(vms: list[dict], vmis: list[dict], redfish_label: tuple[str, str]) -> list[dict]:
    """Every labelled VM with no launcher pod on a node (checked again for the label, so a
    list read without the selector is still right)."""
    placed = {_key(vmi) for vmi in vmis if get(vmi, "status", "phase") in _PHASES_WITH_A_POD_ON_A_NODE}
    return [vm for vm in vms if has_label(vm, redfish_label) and _key(vm) not in placed]


# --- the figures ----------------------------------------------------------------------------


def compute(
    *,
    kv_cluster: str,
    nodes: list[dict],
    pods: list[PodUsage],
    vms: list[dict],
    vmis: list[dict],
    redfish_label: tuple[str, str],
    node_selector: dict[str, str] | None = None,
) -> KvClusterFigures:
    """The four lists as the figures the hub decides on (design §6.3)."""
    selector = node_selector or {}
    headroom: dict[str, list[int]] = {}
    allocatable_cpu = allocatable_memory = 0
    for node in nodes:
        if not usable(node, selector):
            continue
        name = str(get(node, "metadata", "name", default=""))
        allocatable = get(node, "status", "allocatable", default={}) or {}
        cpu, memory = millicores(allocatable.get("cpu")), to_bytes(allocatable.get("memory"))
        headroom[name] = [cpu, memory]
        allocatable_cpu += cpu
        allocatable_memory += memory
    pods_cpu = pods_memory = 0
    for pod in pods:
        if pod.node not in headroom:
            continue
        headroom[pod.node][0] -= pod.cpu_millicores
        headroom[pod.node][1] -= pod.memory_bytes
        pods_cpu += pod.cpu_millicores
        pods_memory += pod.memory_bytes
    stopped = stopped_vms(vms, vmis, redfish_label)
    stopped_cpu = stopped_memory = 0
    for vm in stopped:
        cpu, memory = stopped_vm_request(vm)
        stopped_cpu += cpu
        stopped_memory += memory
    return KvClusterFigures(
        kv_cluster=kv_cluster,
        readable=True,
        nodes=len(nodes),
        usable_nodes=len(headroom),
        allocatable_cpu_millicores=allocatable_cpu,
        allocatable_memory_bytes=allocatable_memory,
        pods_cpu_millicores=pods_cpu,
        pods_memory_bytes=pods_memory,
        stopped_vms=len(stopped),
        stopped_vms_cpu_millicores=stopped_cpu,
        stopped_vms_memory_bytes=stopped_memory,
        node_headroom=[
            KvNodeHeadroom(name=name, cpu_millicores=free[0], memory_bytes=free[1])
            for name, free in sorted(headroom.items())
        ],
    )
