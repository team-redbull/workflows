"""Where a VM goes: the fit test, the buffer and the best-fit ranking — pure.

docs/design/kubevirt-vm-placement.md is the design; this is its one
implementation. No settings, no Temporal, no I/O: the activity hands it the
agents' reports and the policy, and unit tests hand it node tables.

  * The VM's size in the scheduler's terms is what vm.yaml.j2 makes the
    launcher pod request: CPU `cpu × 15 / 16` cores (integer division — the
    template's `div (mul cpu 15) 16`), floored at one core, memory the guest's
    plus 1 GiB for the launcher's overhead. The two formulas live HERE and
    nowhere else.
  * FIT IS PER NODE: one schedulable node must have the headroom (allocatable
    minus its Pending|Running pods' requests) for the pod. A cluster with 200
    free cores over ten nodes holds no 64-vCPU VM. Halted reservations are
    NOT in the per-node test: they sit on no node yet.
  * THE BUFFER IS PER CLUSTER: after placing the VM, the fill — active
    requests + our Halted, launcher-less VMs' reservations + this VM, over the
    schedulable allocatable — must stay at or under `1 − buffer` on CPU AND
    on memory. Halted VMs COUNT here (decided 2026-11-04, reversing the design
    doc's first draft): kubevirt-redfish will start them, and two creates in
    a row must not both see an empty cluster.
  * BEST-FIT among the fitting: the cluster FULLEST after placement on its
    DOMINANT resource (the larger of its CPU and memory fill-after), ties by
    name. First-fit is what falls out of stopping at the first `true`; it is
    not what the fleet wants.
"""

from __future__ import annotations

from activities.vm_lifecycle.kv_capacity_api import KvProbeFailure
from shared.models.vm_lifecycle import KvCapacityReport, KvClusterRank, KvClusterRef, KvPlacement

GIB = 1024**3
# vm.yaml: `cpu: "{{ div (mul (int .spec.cpu) 15) 16 }}"`.
_CPU_REQUEST_NUMERATOR, _CPU_REQUEST_DENOMINATOR = 15, 16
# The launcher's memory overhead on top of the guest's memory (design §1; to be
# confirmed against kubevirt_vmi_launcher_memory_overhead_bytes on the first 64/256 VM).
LAUNCHER_OVERHEAD_GIB = 1

# Every reason a cluster is skipped, keyed as the table records it, worded for
# the operator who reads NoKvClusterFitsError. The workflow's _SKIP_SUMMARY
# mirrors these keys (a test keeps them equal — the _REJECTION_SUMMARY precedent).
SKIP_REASONS: dict[str, str] = {
    "unreachable": "could not be read: the cluster, its ingress or its kv-capacity agent did not answer",
    "no-agent": "have no kv-capacity agent behind their route yet (deploy the agents there first)",
    "refused-token": "refused the kv-capacity token (KV_CAPACITY_TOKEN is not theirs)",
    "malformed": "answered something that is not a capacity report (an agent/contract mismatch)",
    "no-nodes": "have no schedulable node (none Ready and uncordoned)",
    "fit-node": "have no schedulable node with the headroom for this VM's pod request (CPU or memory)",
    "fit-buffer": "would go past the headroom buffer on CPU or memory with this VM on them",
}


def pod_cpu_millicores(cpu: int) -> int:
    """What vm.yaml.j2 makes the launcher pod request for `cpu` vCPUs, in millicores.

    Floored at ONE core (the operator, 2026-11-05): the bare `cpu × 15 / 16`
    rounds a 1-vCPU VM to a ZERO request, which the fit test could never see
    and the scheduler would never account for. The template floors the same way.
    """
    return max(1, cpu * _CPU_REQUEST_NUMERATOR // _CPU_REQUEST_DENOMINATOR) * 1000


def pod_memory_bytes(memory_gib: int) -> int:
    """The guest's memory plus the launcher's overhead, in bytes."""
    return (memory_gib + LAUNCHER_OVERHEAD_GIB) * GIB


def _fill(numerator: int, denominator: int) -> float | None:
    return None if denominator <= 0 else numerator / denominator


def _rank_one(
    ref: KvClusterRef,
    outcome: KvCapacityReport | KvProbeFailure,
    *,
    cpu_millicores: int,
    memory_bytes: int,
    buffer_percent: int,
) -> KvClusterRank:
    row = KvClusterRank(name=ref.name, cluster_name=ref.cluster_name, reachable=False, fits=False, agent_host=ref.agent_host)
    if isinstance(outcome, KvProbeFailure):
        return row.model_copy(update={"skip_reason": outcome.reason, "skip_detail": outcome.detail})
    row.reachable = True
    nodes = [node for node in outcome.nodes if node.schedulable]
    if not nodes:
        return row.model_copy(update={"skip_reason": "no-nodes", "skip_detail": f"{len(outcome.nodes)} nodes, none schedulable"})
    alloc_cpu = sum(node.allocatable_cpu_millicores for node in nodes)
    alloc_mem = sum(node.allocatable_memory_bytes for node in nodes)
    active_cpu = sum(node.requested_cpu_millicores for node in nodes)
    active_mem = sum(node.requested_memory_bytes for node in nodes)
    halted_cpu = sum(vm.cpu_millicores for vm in outcome.halted_reservations)
    halted_mem = sum(vm.memory_bytes for vm in outcome.halted_reservations)
    row.max_node_cpu_headroom_millicores = max(node.allocatable_cpu_millicores - node.requested_cpu_millicores for node in nodes)
    row.max_node_memory_headroom_bytes = max(node.allocatable_memory_bytes - node.requested_memory_bytes for node in nodes)
    row.halted_reservations = len(outcome.halted_reservations)
    row.cpu_fill_before = _fill(active_cpu + halted_cpu, alloc_cpu)
    row.memory_fill_before = _fill(active_mem + halted_mem, alloc_mem)
    row.cpu_fill_after = _fill(active_cpu + halted_cpu + cpu_millicores, alloc_cpu)
    row.memory_fill_after = _fill(active_mem + halted_mem + memory_bytes, alloc_mem)
    if row.max_node_cpu_headroom_millicores < cpu_millicores or row.max_node_memory_headroom_bytes < memory_bytes:
        row.skip_reason = "fit-node"
        row.skip_detail = (
            f"largest node headroom {row.max_node_cpu_headroom_millicores} m / "
            f"{row.max_node_memory_headroom_bytes // GIB} GiB, the pod needs {cpu_millicores} m / {memory_bytes // GIB} GiB"
        )
        return row
    ceiling = 1 - buffer_percent / 100
    cpu_after = row.cpu_fill_after if row.cpu_fill_after is not None else 1.0
    mem_after = row.memory_fill_after if row.memory_fill_after is not None else 1.0
    if cpu_after > ceiling or mem_after > ceiling:
        row.skip_reason = "fit-buffer"
        row.skip_detail = (
            f"fill after placing: cpu {cpu_after:.0%}, memory {mem_after:.0%}; the buffer of "
            f"{buffer_percent}% allows {ceiling:.0%}"
        )
        return row
    row.fits = True
    return row


def _score(row: KvClusterRank) -> float:
    """Fill after placement on the dominant resource: the best-fit key, highest first."""
    return max(row.cpu_fill_after or 0.0, row.memory_fill_after or 0.0)


def known_macs(outcomes: list[KvCapacityReport | KvProbeFailure]) -> list[str]:
    """Every VM MAC the reachable reports list, lowercased, sorted, once."""
    macs = {
        vm.mac.lower()
        for outcome in outcomes
        if isinstance(outcome, KvCapacityReport)
        for vm in outcome.vms
        if vm.mac
    }
    return sorted(macs)


def rank(
    candidates: list[KvClusterRef],
    outcomes: list[KvCapacityReport | KvProbeFailure],
    *,
    cpu: int,
    memory_gib: int,
    buffer_percent: int,
) -> KvPlacement:
    """The table: chosen first, the other fitting clusters by best-fit, then the skipped in name order."""
    cpu_millicores = pod_cpu_millicores(cpu)
    memory_bytes = pod_memory_bytes(memory_gib)
    rows = [
        _rank_one(ref, outcome, cpu_millicores=cpu_millicores, memory_bytes=memory_bytes, buffer_percent=buffer_percent)
        for ref, outcome in zip(candidates, outcomes, strict=True)
    ]
    fitting = sorted((row for row in rows if row.fits), key=lambda row: (-_score(row), row.name))
    skipped = sorted((row for row in rows if not row.fits), key=lambda row: row.name)
    return KvPlacement(
        ranked=fitting + skipped,
        chosen=fitting[0].name if fitting else None,
        buffer_percent=buffer_percent,
        pod_cpu_millicores=cpu_millicores,
        pod_memory_bytes=memory_bytes,
        known_macs=known_macs(outcomes),
    )
