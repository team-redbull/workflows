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
  * memory request = the guest's memory + 1 GiB for KubeVirt's own overhead.
    The lab measured about 0.3 GiB on a 16 GiB guest, so 1 GiB errs on the
    safe side.

    VM (vCPU / memory)   CPU request   memory request
    8  / 64 GiB          7 cores       65 GiB
    16 / 128 GiB         15 cores      129 GiB
    32 / 256 GiB         30 cores      257 GiB
    64 / 512 GiB         60 cores      513 GiB

Integers only, in millicores and bytes: nothing here or downstream parses a
Kubernetes quantity string.
"""

from __future__ import annotations

GIB = 1024**3
MILLICORES_PER_CORE = 1000
_CPU_REQUEST_NUMERATOR, _CPU_REQUEST_DENOMINATOR = 15, 16
# KubeVirt's launcher overhead on top of the guest's memory, counted per VM.
LAUNCHER_OVERHEAD_GIB = 1


def request_cores(cpu: int) -> int:
    """The whole cores a VM of `cpu` vCPUs requests: `cpu × 15 // 16`, at least one."""
    return max(1, cpu * _CPU_REQUEST_NUMERATOR // _CPU_REQUEST_DENOMINATOR)


def pod_cpu_millicores(cpu: int) -> int:
    """What the launcher pod of a `cpu`-vCPU VM requests, in millicores."""
    return request_cores(cpu) * MILLICORES_PER_CORE


def pod_memory_bytes(memory_gib: int) -> int:
    """The guest's memory plus the launcher's overhead, in bytes."""
    return (memory_gib + LAUNCHER_OVERHEAD_GIB) * GIB


def pod_memory_bytes_of_guest(guest_bytes: int) -> int:
    """The same, for a guest whose memory is known in bytes (a VM read from a cluster)."""
    return guest_bytes + LAUNCHER_OVERHEAD_GIB * GIB
