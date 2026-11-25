# KV VM memory overcommit: rejected (2026-11-22)

Status: **rejected, 2026-11-22.** Overcommit (a pod request below the guest's memory, backed by node
swap) will not be implemented. The problem it addressed is solved instead by sizing the guest to fit
the node: a KubeVirt InfraEnv's VMs get **vCPUs × 7.75 GiB** (62 / 124 / 248 / 496 GiB for
kv8 / kv16 / kv32 / kv64; vCPUs a multiple of 4), the pod request staying guest + 1 GiB. The
analysis below is kept as the record of why.

## Background facts

- **VM sizes.** VMs are 8 GiB per vCPU, from the InfraEnv `<flavor>-kv<cpu>-<site>-<zone>`. Sizes are
  kv8 (8 vCPU / 64 GiB, **the smallest**), kv16 (128 GiB), kv32 (256 GiB) and kv64 (512 GiB).
- **How placement counts a VM today.** The definition is in `shared/vm_requests.py`; the scheduler
  sees the virt-launcher pod's request.
  - CPU: vCPUs × 15 // 16, at least 1 core.
  - Memory: guest + 1 GiB pad. The 2026-11-05 lab measured ~308 MiB of real overhead on a 16 GiB
    guest, so the pad is conservative.
- **The VM body is the platform's `vm.yaml`.** `tests/test_vm_spec.py` pins it field by field, so any
  change to its memory fields needs the vm.yaml owner's agreement.
- **The count must match the pod.** Placement only counts what the pod will request. Lowering our
  count without changing the VM spec would admit VMs whose pods then sit Pending.
- **KV clusters will run upstream KubeVirt, not OpenShift Virtualization** (decided 2026-11-20).
  Nothing may depend on CNV-only features.

## The node

- **Hardware.** `dell-r660-site4-128c-1024gb` (from day1 `platform-config`).
- **day1's kubelet reserve.** `systemReserved` is cpu 4000m and memory 15000Mi (~14.6 GiB). The hard
  eviction threshold is 100Mi.
- **Allocatable.** About **124 cores and ~1,009 GiB**, assuming the OS sees the full 1,024 GiB. This
  is not verified.

## The ask and the problem

- **The ask.** Make memory work like CPU: a 64 GiB VM requests 63 (limit 64, or 65 for overhead), so
  memory stops blocking VMs the CPU would allow.
- **The real cause.** The VM ratio (8 GiB/vCPU) equals the node's (1,024 GiB / 128 cores). The node
  reserve plus the 1 GiB pad therefore always pushes the last VM out, while CPU has slack from its
  1/16 discount.

| VM | Request (CPU, memory) | VMs the CPU allows | VMs the memory allows | Unused per node |
|---|---|---|---|---|
| kv8 | 7 cores, 65 GiB | 17 | **15** | 2 VMs: 19 cores, 34 GiB |
| kv16 | 15 cores, 129 GiB | 8 | **7** | 19 cores, 106 GiB |
| kv32 | 30 cores, 257 GiB | 4 | **3** | 34 cores, 238 GiB |
| kv64 | 60 cores, 513 GiB | 2 | **1** | **64 cores, 496 GiB (half the node)**; 2 × 513 = 1,026 vs 1,009 |

## Why not just request less (63 GiB)

- **Memory is incompressible.** When the node runs short, the kubelet evicts pods, starting with those
  over their request, or the kernel OOM-kills QEMU. Either way a tenant cluster's node VM is
  hard-killed. CPU only throttles.
- **Guests fill up.** OpenShift guests eventually use all their memory through page cache, so a
  request below the guest's memory is a promise the node can't keep. Example: kv8 at 63 GiB fits 16
  VMs on paper, but they need ~1,029 GiB of real memory against 1,009.
- **Never set a memory limit.** KubeVirt sets none by default and advises against one. QEMU's
  overhead is only an estimate and grows during migration and hotplug, so a limit near it gets QEMU
  killed.

## The direction: overcommit backed by node swap

- **What overcommit means here.** The guest still sees its full memory; only the pod's request is
  lower (guest ÷ overcommit %). When the VMs together really exceed RAM, the kernel moves pages to the
  node's swap disk. VMs slow down instead of being killed.
- **The 1:8 ratio doesn't change.** Changing the ratio is a separate alternative (below).
- **Swap is finite.** It must be sized for the worst case, or eviction or OOM comes back.
- **Swap use will be steady, not rare.** Guests keep their page cache full, so some swap is always in
  use. Swapped page cache is usually cold, so the cost is often small but not zero. Live migration of
  a VM with swapped pages is slower.

### Upstream implementation (not wasp-agent)

wasp-agent is OpenShift Virtualization's "higher workload density"; the upstream equivalent is:

- **Node swap.** Each KV worker node gets a swap device plus kubelet
  `memorySwap.swapBehavior: LimitedSwap`. Only Burstable pods use swap; a virt-launcher pod with no
  limit is Burstable.
- **Per-VM overcommit** in the spec the KV `vm-lifecycle-worker` builds: `domain.memory.guest` = the
  full memory, `resources.requests.memory` = guest × 100 / %. The alternative is the cluster-wide
  `memoryOvercommit` in the KubeVirt CR; per-VM keeps it under our control.
- **A per-cluster setting.** `VM_MEMORY_OVERCOMMIT_PERCENT` in the worker's ConfigMap. The worker
  reports it with its figures, so placement counts exactly that cluster's pod request. Both sides use
  one function in `shared/vm_requests.py`.
- **Still no memory limit.**
- **Swap becomes a requirement of the KV cluster platform**, to add to the design's §9.

## The official reserve formula

OpenShift's `autoSizingReserved: true` in the KubeletConfig uses the same scale GKE publishes.

| Memory share of the node's RAM | Reserved |
|---|---|
| first 4 GiB | 25% |
| next 4 GiB | 20% |
| next 8 GiB | 10% |
| next 112 GiB | 6% |
| above 128 GiB | 2% |

| CPU share of the logical CPUs | Reserved |
|---|---|
| first CPU | 6% |
| second CPU | 1% |
| next 2 | 0.5% |
| above 4 | 0.25% |

- **This node by the formula:** memory 1 + 0.8 + 0.8 + 6.72 + 2% × 896 ≈ **27.2 GiB**; CPU ≈ **0.4
  cores** with 128 logical CPUs, ~0.7 with 256 (if "128c" means physical cores with hyperthreading).
- **day1 today:** about **10× the formula's CPU** and about **half its memory**.
- **What the reserve covers:** only what isn't a pod (OS, kubelet, CRI-O). DaemonSet pods (OVN,
  multus, virt-handler, node-exporter) have their own requests, which the worker's figures already
  subtract. QEMU's overhead is in each virt-launcher pod's request.

## Numbers with the formula's reserve (allocatable ≈ 997 GiB)

| VM | Overcommit for the CPU's count | Worst-case swap per node |
|---|---|---|
| kv8 (17/node) | ~111% | ~96 GiB (17 × 64.3 − 997) |
| kv16 (8) | ~104% | ~30 GiB |
| kv32 (4) | ~103% | ~28 GiB |
| kv64 (2) | ~103% | ~28 GiB |

With day1's current reserve (1,009 GiB) the needs are kv8 ~110% / ~85 GiB swap and kv16–64
~102–103% / ~15–17 GiB.

**Recommendation: ~112% overcommit with ~100–110 GiB of swap per node.** That makes CPU the limit at
every size.

## Other suggestions

- Use `autoSizingReserved: true` instead of hand-written reserves. Check that it's honoured through a
  HyperShift NodePool's KubeletConfig ConfigMap first.
- The 4-core CPU reserve is generous. Cutting it to 1–2 cores gives back 2–3 cores per node, close to
  half a kv8 VM. Don't cut to 0.4 if the OS runs anything heavy outside pods.

## To verify before choosing the percentage

1. The real `oc get node <n> -o jsonpath='{.status.allocatable}'` on a KV node.
2. Physical cores vs logical CPUs.
3. The sum of the DaemonSet pod requests on a KV node.
4. Agreement from vm.yaml's owner to change its memory fields.
5. Agreement from the KV platform owner on swap (disks, kubelet `LimitedSwap`).

## Alternatives still on the table

1. **Accept memory as the limit** (status quo). The pad could drop to ~0.5 GiB, but that gains
   nothing at these sizes.
2. **Overcommit + swap**, as above (the current direction).
3. **Change the ratio:** less than 8 GiB per vCPU, or nodes with more memory per core.
