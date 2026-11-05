"""What scale-down-nodepool reads off a NodePool's Machines and their Nodes — pure functions only.

The sibling of hosted_resources.py for the NodePool side: plain dicts in, as
the API servers return them, typed values out; no client, no settings, no
Temporal. activities.py fetches, this module reads, and the brain decides
(workflow_domains/server_lifecycle/scale_down_selection.py) — nothing here
picks a server.

The shapes, from the code that writes them:

  * HyperShift's NodePool controller annotates the MachineSet / MachineDeployment
    it makes for a NodePool, and their Machine template, with
    `hypershift.openshift.io/nodePool: <nodepool namespace>/<nodepool name>`
    (`nodePoolAnnotation`, `client.ObjectKeyFromObject(nodePool).String()`),
    in the HostedCluster's control-plane namespace. That annotation is how a
    Machine is found to be the NodePool's.
  * CAPI's MachineSet ranks the Machines it removes on a scale-down and takes
    the top ones; ties go by Machine NAME. HyperShift sets no delete policy,
    so it is Random: a Machine annotated `cluster.x-k8s.io/delete-machine`
    (any value) ranks above every healthy one in every CAPI version, and above
    an UNHEALTHY one from CAPI 1.9 on (kubernetes-sigs/cluster-api#11431;
    openshift/cluster-api release-4.19 and later). In 4.18's CAPI a marked and
    an unhealthy Machine rank the SAME, so the brain takes CAPI-unhealthy
    Machines first, in name order — `capi_healthy` here is CAPI's own
    `isMachineHealthy`, read off the Machine, so the two always agree.
    (machineset_delete_policy.go, machineset_deletion_order.go from 1.12.)
  * HyperShift backs an InPlace NodePool with ONE MachineSet and a Replace one
    with a MachineDeployment (`hypershift-operator/controllers/nodepool/capi.go`).
    A MachineDeployment mid-rollout has two MachineSets and may scale down the
    one the marks are not in (the CAPI book: "the label only affects MachineSet
    scale-down"), so each Machine's owning MachineSet is reported.
  * A Machine's Node is `status.nodeRef.name`.
  * The Pods a drain moves are hosted_resources.pod_to_evict's (CAPI's drain
    rules); one that has finished (Succeeded / Failed) holds nothing.
  * A Pod's request is what the scheduler counts: the larger of its containers'
    sum and its largest init container, plus its overhead.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from kubernetes.utils.quantity import parse_quantity

from activities.server_lifecycle.hosted_resources import node_ready, pod_to_evict
from shared.models.server_lifecycle import ScaleDownMachine

NODEPOOL_ANNOTATION = "hypershift.openshift.io/nodePool"
DELETE_MACHINE_ANNOTATION = "cluster.x-k8s.io/delete-machine"

_RESOURCES = ("cpu", "memory")
_FINISHED = ("Succeeded", "Failed")


def _metadata(obj: dict[str, Any]) -> dict[str, Any]:
    return obj.get("metadata") or {}


def machine_nodepool(machine: dict[str, Any]) -> str | None:
    """`<namespace>/<name>` of the NodePool this Machine is, by HyperShift's annotation."""
    return (_metadata(machine).get("annotations") or {}).get(NODEPOOL_ANNOTATION) or None


def delete_mark(machine: dict[str, Any] | None) -> str | None:
    """The value of the Machine's `cluster.x-k8s.io/delete-machine`; None when unmarked.

    An empty value still marks it for CAPI, so it reads as "" (not None).
    """
    if machine is None:
        return None
    annotations = _metadata(machine).get("annotations") or {}
    return str(annotations[DELETE_MACHINE_ANNOTATION]) if DELETE_MACHINE_ANNOTATION in annotations else None


def machine_deleting(machine: dict[str, Any]) -> bool:
    return bool(_metadata(machine).get("deletionTimestamp"))


def machine_node(machine: dict[str, Any]) -> str | None:
    return (((machine.get("status") or {}).get("nodeRef")) or {}).get("name") or None


# The v1beta1 conditions CAPI's isMachineHealthy reads (a v1beta1 read of a
# v1beta2-stored Machine converts them back to these).
_UNHEALTHY_WHEN_FALSE = ("NodeHealthy", "HealthCheckSucceeded")


def capi_healthy(machine: dict[str, Any]) -> bool:
    """CAPI's own `isMachineHealthy`, so the brain ranks unhealthy Machines exactly as CAPI does.

    Unhealthy: no nodeRef, a failureReason or failureMessage, or NodeHealthy /
    HealthCheckSucceeded False. Unknown is not unhealthy, as in CAPI.
    """
    status = machine.get("status") or {}
    if not (status.get("nodeRef") or {}).get("name"):
        return False
    if status.get("failureReason") or status.get("failureMessage"):
        return False
    for condition in status.get("conditions") or []:
        if isinstance(condition, dict) and condition.get("type") in _UNHEALTHY_WHEN_FALSE:
            if condition.get("status") == "False":
                return False
    return True


def machine_set(machine: dict[str, Any]) -> str | None:
    """The MachineSet that owns this Machine (its controller ownerReference), by name."""
    for owner in _metadata(machine).get("ownerReferences") or []:
        if isinstance(owner, dict) and owner.get("kind") == "MachineSet":
            return owner.get("name") or None
    return None


def machines_of(machines: list[dict[str, Any]], nodepool_key: str) -> list[dict[str, Any]]:
    """The Machines whose NodePool annotation is exactly `<namespace>/<name>`, by name."""
    return sorted(
        (m for m in machines if machine_nodepool(m) == nodepool_key),
        key=lambda m: _metadata(m).get("name") or "",
    )


def _quantity(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return parse_quantity(value)
    except (ValueError, InvalidOperation, TypeError):
        return None


def _requests(resources: Any) -> dict[str, Decimal]:
    requests = ((resources or {}).get("requests") or {}) if isinstance(resources, dict) else {}
    found: dict[str, Decimal] = {}
    for name in _RESOURCES:
        amount = _quantity(requests.get(name))
        if amount is not None:
            found[name] = amount
    return found


def pod_requests(pod: dict[str, Any]) -> dict[str, Decimal]:
    """What the scheduler counts for one Pod: max(sum of containers, largest init container) + overhead."""
    spec = pod.get("spec") or {}
    total = {name: Decimal(0) for name in _RESOURCES}
    for container in spec.get("containers") or []:
        for name, amount in _requests((container or {}).get("resources")).items():
            total[name] += amount
    for container in spec.get("initContainers") or []:
        for name, amount in _requests((container or {}).get("resources")).items():
            total[name] = max(total[name], amount)
    for name in _RESOURCES:
        overhead = _quantity((spec.get("overhead") or {}).get(name))
        if overhead is not None:
            total[name] += overhead
    return total


def pod_running(pod: dict[str, Any]) -> bool:
    """Not finished: a Succeeded or Failed Pod holds no capacity and moves nothing."""
    return (pod.get("status") or {}).get("phase") not in _FINISHED


def pods_on(pods: list[dict[str, Any]], node_name: str) -> list[dict[str, Any]]:
    return [p for p in pods if (p.get("spec") or {}).get("nodeName") == node_name and pod_running(p)]


def workload_pods(pods_on_node: list[dict[str, Any]]) -> int:
    """How many of a Node's running Pods a drain would move (not DaemonSet, not mirror)."""
    return sum(1 for pod in pods_on_node if pod_to_evict(pod))


def allocatable(node: dict[str, Any]) -> dict[str, Decimal]:
    found: dict[str, Decimal] = {}
    for name in _RESOURCES:
        amount = _quantity(((node.get("status") or {}).get("allocatable") or {}).get(name))
        if amount is not None and amount > 0:
            found[name] = amount
    return found


def share_permille(amounts: dict[str, Decimal], capacity: dict[str, Decimal]) -> int | None:
    """The larger of CPU and memory, per thousand of the Node's allocatable; None with no capacity."""
    shares = [amounts.get(name, Decimal(0)) / capacity[name] for name in _RESOURCES if name in capacity]
    if not shares:
        return None
    return int((max(shares) * 1000).to_integral_value())


def node_usage(node_metrics: dict[str, Any]) -> dict[str, Decimal]:
    """A `metrics.k8s.io` NodeMetrics' `usage` (cpu, memory)."""
    usage = node_metrics.get("usage") or {}
    found: dict[str, Decimal] = {}
    for name in _RESOURCES:
        amount = _quantity(usage.get(name))
        if amount is not None:
            found[name] = amount
    return found


def candidate(
    machine: dict[str, Any],
    nodepool: str,
    nodes: dict[str, dict[str, Any]],
    pods: list[dict[str, Any]],
    metrics: dict[str, dict[str, Any]] | None,
) -> ScaleDownMachine:
    """One Machine as a scale-down candidate: what every policy ranks by.

    `nodes` and `metrics` are keyed by Node name; `metrics` None when the
    NodePool's policy does not use them. A Node in `nodes` with no entry in
    `metrics` leaves `used_permille` None — the activity refuses that case
    before it gets here.
    """
    metadata = _metadata(machine)
    node_name = machine_node(machine)
    node = nodes.get(node_name) if node_name else None
    state = ScaleDownMachine(
        nodepool=nodepool,
        namespace=metadata.get("namespace") or "",
        name=metadata.get("name") or "",
        created=metadata.get("creationTimestamp"),
        deleting=machine_deleting(machine),
        delete_mark=delete_mark(machine),
        capi_healthy=capi_healthy(machine),
        machine_set=machine_set(machine),
        node=node_name,
        node_found=node is not None,
    )
    if node is None:
        return state
    running = pods_on(pods, node_name or "")
    capacity = allocatable(node)
    requested = {name: Decimal(0) for name in _RESOURCES}
    for pod in running:
        for name, amount in pod_requests(pod).items():
            requested[name] += amount
    state.ready = node_ready(node)
    state.unschedulable = bool((node.get("spec") or {}).get("unschedulable"))
    state.workload_pods = workload_pods(running)
    state.requested_permille = share_permille(requested, capacity)
    if metrics is not None and node_name in metrics:
        state.used_permille = share_permille(node_usage(metrics[node_name]), capacity)
    return state


def mark_patch(resource_version: str | None, mark: str | None) -> dict[str, Any]:
    """The merge patch that sets (`mark`) or removes (None) the delete mark, at the version read.

    The resourceVersion makes it a compare-and-set: a Machine changed since the
    read answers 409, and the activity's retry reads it again.
    """
    metadata: dict[str, Any] = {"annotations": {DELETE_MACHINE_ANNOTATION: mark}}
    if resource_version:
        metadata["resourceVersion"] = resource_version
    return {"metadata": metadata}
