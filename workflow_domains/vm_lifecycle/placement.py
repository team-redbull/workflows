"""Whether a VM fits on a KV cluster: the fit rule and the buffer — pure, sandbox-safe.

docs/design/create-vm-placement.md §6.4 is the design; this is its one
implementation (server_lifecycle/bond_selection.py is the precedent for a
policy module beside its workflow). No I/O, no clock, no settings: the
create-vm run hands it one cluster's figures, as that cluster's worker
measured them, and the buffer the zone's cluster list came with.

THE SPLIT IS ON PURPOSE. Each worker only knows its own cluster and only
MEASURES; "the oldest cluster that fits" can only be decided by whoever sees
them all, the run, while it holds the zone's lock. And the buffer is one
value for every cluster, so it is applied in one place — here.

A VM fits when ALL of these hold; the FIRST that does not is the skip reason:

  unreachable  the worker answered but could not read its cluster
  no-nodes     the cluster has no usable node
  fit-node     no single node has room for the VM's CPU AND memory — the
               largest free CPU and the largest free memory can be on two
               different nodes, and then neither holds the VM
  fit-buffer   requested + the VM would pass (1 − buffer) × allocatable, on
               CPU, then on memory. The buffer is the whole CLUSTER's, never
               one node's: nodes may fill completely; the free share is the
               room live migration needs when a node is drained

"Requested" is what pods request plus what STOPPED VMs will request: a VM
created a moment ago by the run before this one is a stopped VM, which is the
whole reason nothing has to be remembered between runs.

The run never chooses a node: the VM is created Halted, and when it is
started the Kubernetes scheduler chooses. `fit-node` only makes sure such a
node exists.

All arithmetic is in integers (millicores, bytes, percent), so the decision
is exact and replays identically: `(requested + vm) × 100 <= allocatable × (100 − buffer)`.
"""

from __future__ import annotations

from dataclasses import dataclass

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from shared.models.vm_lifecycle import KvClusterFigures, KvClusterRow
    from shared.vm_requests import GIB

# Every reason a cluster is skipped, keyed as the run's table records it, worded
# for the operator who reads NoKvClusterFitsError. The first two are the run's
# own (a cluster never read); the rest are skip_reason()'s.
SKIP_REASONS: dict[str, str] = {
    "day1-problem": "are listed in day1 with a problem and were never read (their file does not give them this zone)",
    "no-worker": "have no vm-lifecycle worker answering on their queue (not deployed, down, or deployed under another name)",
    "unreachable": "have a worker that could not read its own cluster's API in time",
    "no-nodes": "have no usable node (none Ready, uncordoned and untainted)",
    "fit-node": "have no single node with the room for this VM's CPU and memory",
    "fit-buffer": "would go past the placement buffer on CPU or memory with this VM on them",
}
DAY1_PROBLEM = "day1-problem"
NO_WORKER = "no-worker"
UNREACHABLE = "unreachable"
NO_NODES = "no-nodes"
FIT_NODE = "fit-node"
FIT_BUFFER = "fit-buffer"


@dataclass(frozen=True)
class Verdict:
    """What the fit rule says about one cluster: `reason` None means the VM fits."""

    reason: str | None
    detail: str | None = None
    cpu_fill_after: float | None = None
    memory_fill_after: float | None = None

    @property
    def fits(self) -> bool:
        return self.reason is None


def _within_buffer(requested: int, vm: int, allocatable: int, buffer_percent: int) -> bool:
    return (requested + vm) * 100 <= allocatable * (100 - buffer_percent)


def _ratio(numerator: int, denominator: int) -> float | None:
    return None if denominator <= 0 else numerator / denominator


def judge(figures: KvClusterFigures, vm_cpu_millicores: int, vm_memory_bytes: int, buffer_percent: int) -> Verdict:
    """The fit rule on one cluster's figures, with the numbers behind the decision."""
    if not figures.readable:
        return Verdict(UNREACHABLE, figures.detail or "its worker could not read its cluster")
    if figures.usable_nodes <= 0 or not figures.node_headroom:
        return Verdict(NO_NODES, f"{figures.nodes} nodes, none usable")
    requested_cpu = figures.pods_cpu_millicores + figures.stopped_vms_cpu_millicores
    requested_memory = figures.pods_memory_bytes + figures.stopped_vms_memory_bytes
    cpu_after = _ratio(requested_cpu + vm_cpu_millicores, figures.allocatable_cpu_millicores)
    memory_after = _ratio(requested_memory + vm_memory_bytes, figures.allocatable_memory_bytes)
    if not any(
        node.cpu_millicores >= vm_cpu_millicores and node.memory_bytes >= vm_memory_bytes
        for node in figures.node_headroom
    ):
        roomiest = max(figures.node_headroom, key=lambda node: (node.cpu_millicores, node.memory_bytes, node.name))
        return Verdict(
            FIT_NODE,
            f"no node holds {vm_cpu_millicores} m and {vm_memory_bytes // GIB} GiB together; the roomiest, "
            f"{roomiest.name}, has {roomiest.cpu_millicores} m / {roomiest.memory_bytes // GIB} GiB free",
            cpu_after, memory_after,
        )
    ceiling = 100 - buffer_percent
    if not _within_buffer(requested_cpu, vm_cpu_millicores, figures.allocatable_cpu_millicores, buffer_percent):
        return Verdict(
            FIT_BUFFER,
            f"CPU after placing: {requested_cpu + vm_cpu_millicores} m of {figures.allocatable_cpu_millicores} m "
            f"allocatable; the {buffer_percent}% buffer allows {ceiling}%",
            cpu_after, memory_after,
        )
    if not _within_buffer(requested_memory, vm_memory_bytes, figures.allocatable_memory_bytes, buffer_percent):
        return Verdict(
            FIT_BUFFER,
            f"memory after placing: {(requested_memory + vm_memory_bytes) // GIB} GiB of "
            f"{figures.allocatable_memory_bytes // GIB} GiB allocatable; the {buffer_percent}% buffer allows {ceiling}%",
            cpu_after, memory_after,
        )
    return Verdict(None, None, cpu_after, memory_after)


def skip_reason(
    figures: KvClusterFigures, vm_cpu_millicores: int, vm_memory_bytes: int, buffer_percent: int
) -> str | None:
    """The first reason the VM does not fit on the cluster, or None when it fits."""
    return judge(figures, vm_cpu_millicores, vm_memory_bytes, buffer_percent).reason


def row(name: str, figures: KvClusterFigures, verdict: Verdict) -> KvClusterRow:
    """One judged cluster as a row of the run's table."""
    return KvClusterRow(
        name=name, fits=verdict.fits, skip_reason=verdict.reason, skip_detail=verdict.detail, figures=figures,
        cpu_fill_after=verdict.cpu_fill_after, memory_fill_after=verdict.memory_fill_after,
    )


def summarise_skips(table: list[KvClusterRow]) -> str:
    """One clause per skip reason naming its clusters, in the table's order, for NoKvClusterFitsError."""
    by_reason: dict[str, list[str]] = {}
    for entry in table:
        if not entry.fits:
            by_reason.setdefault(entry.skip_reason or "unknown", []).append(entry.name)
    return "; ".join(f"{', '.join(names)} {SKIP_REASONS.get(reason, reason)}" for reason, names in by_reason.items())
