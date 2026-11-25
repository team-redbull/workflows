"""How big a VM is in the Kubernetes scheduler's terms — pure, stdlib only, read on both sides.

Capacity is measured by what the VM's launcher pod REQUESTS, never in vCPUs.
Both sides need the two formulas: the worker inside a KV cluster counts every
stopped VM at what its pod will request when it starts
(activities/vm_lifecycle/cluster_figures.py), and the create-vm run sizes the
new VM the same way before it asks whether it fits
(workflow_domains/vm_lifecycle/placement.py). Two copies of one rule drift
apart, so there is one, here (shared/upgrade_layout.py is the precedent).

  * CPU request   = vCPUs × 15 / 16, rounded down, at least ONE core — what the
    VirtualMachine the worker builds asks for (activities/vm_lifecycle/vm_spec.py,
    the platform's vm.yaml: `div (mul cpu 15) 16`, which alone would make a
    1-vCPU VM request 0; floored, the operator 2026-11-05).
  * memory request = the guest's memory + KubeVirt's overhead, counted as
    300 MiB + 8 MiB × vCPUs + guest memory ÷ 512. KubeVirt adds that overhead
    to the launcher pod's request itself (page tables at 1/512 of the guest,
    per-vCPU state, its own processes), and the scheduler also counts the
    pod's 35 MB console-log container. Measured on four VMs built by
    vm_spec.py (cluster-qcl8n, OpenShift Virtualization 4.22, 2026-11-22) as
    260 MiB + 8 MiB × vCPUs + guest ÷ 512, + 35 MB, each to the MiB; 300 MiB
    rounds the fixed part up, so the formula is never under the pod's request
    and at most 10 MiB over it (tests/test_vm_lifecycle_placement.py pins the
    four). Until 2026-11-24 it was a flat 1 GiB — too little from 32 vCPUs up
    (a 64-vCPU VM's overhead is about 1.8 GiB), so placement could approve a
    VM whose pod then had no node to run on.

    VM (vCPU / memory)   CPU request   memory request
    8  / 60 GiB          7 cores       60 GiB + 484 MiB
    16 / 120 GiB         15 cores      120 GiB + 668 MiB
    32 / 240 GiB         30 cores      240 GiB + 1,036 MiB
    64 / 480 GiB         60 cores      480 GiB + 1,772 MiB

Integers only, in millicores and bytes: nothing here or downstream parses a
Kubernetes quantity string.
"""

from __future__ import annotations

MIB = 1024**2
GIB = 1024**3
MILLICORES_PER_CORE = 1000
_CPU_REQUEST_NUMERATOR, _CPU_REQUEST_DENOMINATOR = 15, 16
# KubeVirt's launcher overhead on top of the guest's memory, per VM: a fixed
# part, a part per vCPU, and the guest's page tables (one 512th of the guest).
_OVERHEAD_FIXED_BYTES = 300 * MIB
_OVERHEAD_BYTES_PER_VCPU = 8 * MIB
_OVERHEAD_GUEST_DIVISOR = 512


def request_cores(cpu: int) -> int:
    """The whole cores a VM of `cpu` vCPUs requests: `cpu × 15 // 16`, at least one."""
    return max(1, cpu * _CPU_REQUEST_NUMERATOR // _CPU_REQUEST_DENOMINATOR)


def pod_cpu_millicores(cpu: int) -> int:
    """What the launcher pod of a `cpu`-vCPU VM requests, in millicores."""
    return request_cores(cpu) * MILLICORES_PER_CORE


def launcher_overhead_bytes(cpu: int, guest_bytes: int) -> int:
    """What the launcher pod requests ON TOP of the guest's memory, in bytes."""
    return _OVERHEAD_FIXED_BYTES + _OVERHEAD_BYTES_PER_VCPU * cpu + guest_bytes // _OVERHEAD_GUEST_DIVISOR


def pod_memory_bytes_of_guest(cpu: int, guest_bytes: int) -> int:
    """The guest's memory plus the launcher's overhead, in bytes, for a guest whose memory
    is known in bytes (a VM read from a cluster)."""
    return guest_bytes + launcher_overhead_bytes(cpu, guest_bytes)


def pod_memory_bytes(cpu: int, memory_gib: int) -> int:
    """The same, for a `cpu`-vCPU VM of `memory_gib` GiB (the VM a run is about to create)."""
    return pod_memory_bytes_of_guest(cpu, memory_gib * GIB)


def _gib(hundredths: int) -> str:
    sign, hundredths = ("-" if hundredths < 0 else ""), abs(hundredths)
    return f"{sign}{hundredths // 100}.{hundredths % 100:02d} GiB"


def gib_needed(memory_bytes: int) -> str:
    """Bytes as GiB for a message, two decimals, rounded UP: what something NEEDS."""
    return _gib(-(-memory_bytes * 100 // GIB))


def gib_free(memory_bytes: int) -> str:
    """Bytes as GiB for a message, two decimals, rounded DOWN: what something HAS. With
    `gib_needed`, a need that does not fit never reads as smaller than the room it was refused."""
    return _gib(memory_bytes * 100 // GIB)
