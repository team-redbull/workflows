# How create-vm places a VM: one zone at a time, a worker in every KubeVirt cluster

> **Names, 2026-11-24.** Every name on this page is the platform convention's
> (`Idan-Maimon/kubevirt-workflows` `docs/naming-convention.md`, fetched by the `naming-convention`
> skill): seed `ocp4-<lifecycle>-seed-<site>-<zone>-<id>`, KV cluster
> `ocp4-<lifecycle>-kv-<site>-<zone>-<id>`, InfraEnv `<flavor>-kv<cpu>-<zone>` (no site: it is the
> MCE's), size label `kv<cpu>`. Section 3 was rewritten for them on 2026-11-24. Until the
> convention left draft (2026-11-22) this page used the seed `ocp4-seed-<site>-<N>`, the KV cluster
> without its zone, the InfraEnv with its site and the size label `<cpu>c`; none of those shapes
> ran in production.

> **Memory, 2026-11-24.** A VM's memory is **vCPUs × 7.5 GiB**, no longer × 8, and placement
> counts KubeVirt's real overhead per VM instead of a flat 1 GiB (section 6.1). Measured on the
> operator's R660 KV nodes: at 8 GiB per vCPU every size was one VM short of what the CPU allows
> per node. Memory overcommit was weighed and rejected (`kv-memory-overcommit.md`). In the code
> since 2026-11-24 ("As implemented").

This page explains, end to end, how a create-vm run decides which KubeVirt cluster (KV cluster)
a new VM goes on, how the VM gets its MAC address and its name, and who talks to what. It replaces
`kubevirt-vm-placement.md` (every run ranks the clusters on its own) and `kv-agents-api.md` (two
HTTP agents on every KV cluster). It also replaces `kv-placement-server-scan.md`: **server-scan
takes no part in placing or creating a VM.**

Names follow the platform's naming convention (`Idan-Maimon/kubevirt-workflows`
`docs/naming-convention.md`): clusters `ocp4-<lifecycle>-<flavor>-<site>-<id>`, InfraEnvs
`<flavor>-kv<cpu>-<zone>` (no site: it is the MCE's), VMs `ocp-<flavor>-kv<cpu>-<site>-<full MAC>`.
The seed and KV cluster names are the older ones (the note at the top).

---

## 1. The idea in one paragraph

**The zone's clusters come from day1, their numbers come from the clusters themselves, only one
run at a time chooses a cluster in a zone, and the chosen cluster builds the VM.** A create-vm run
reads the zone's KV clusters from the day1 repo. It then takes the **zone's lock**. While it holds
the lock, it goes through the clusters oldest first, asks each one's `vm-lifecycle-worker` for its
current figures, and picks the **first** cluster where the VM fits. It sends that cluster's worker
what the VM is (flavor, vCPUs, memory, site, zone, MCE); the worker draws the MAC from its own
cluster's prefix, builds the name and the VirtualMachine, and creates it, drawing again by itself
if the MAC is taken. As soon as the VM object exists, the run releases the lock, so the next run
sees that VM in the cluster's own figures. The long part, waiting up to 10 minutes for the VM's
disk, happens after the lock is released, in parallel with every other run.

---

## 2. Who runs where

| piece | where it runs | what it does | what it holds |
| --- | --- | --- | --- |
| create-vm workflow | hub, in the brain | one per VM: lists the clusters, takes the lock, chooses, asks for the VM, releases, waits | nothing |
| zone lock workflow | hub, in the brain | **one per (site, zone)**: hands the lock to one run at a time, in arrival order | only the line of runs waiting |
| `cluster-lifecycle-worker` | hub (already deployed) | lists the zone's KV clusters from day1, and hands the run the placement buffer (`HEADROOM_BUFFER_PERCENT`) | its day1 git token, the same one provision-cluster reads day1 with; create-vm only needs it to **read** day1 `main` |
| `vm-lifecycle-worker` | **inside each KV cluster** | reports its cluster's figures; draws the MAC, builds and creates the VM; reads and deletes VMs | its own ServiceAccount, and its ConfigMap: the cluster's MAC prefix and the VM template's values |

The worker inside a KV cluster only makes **outbound** connections. It connects to the hub's
Temporal over gRPC with TLS through a Route, exactly as the `server-lifecycle-worker` does from
each MCE. The KV cluster exposes nothing, and nothing on the hub holds a credential for it, or
its MAC prefix.

**How a run reaches a given cluster:** by Temporal queue name. Each worker polls
`vm-lifecycle-activity-<its cluster's name>`. The name is the day1 file's name, so the list read
from day1 is also the list of queues to send work to.

---

## 3. Where the list of KV clusters comes from

**What the request names.** The InfraEnv's name says everything about the VM, and is parsed once,
when the request arrives:

```
c200-kv32-az1
 │    │    └─ zone, az<N>
 │    └────── kv + vCPU count: 32 vCPUs
 └─────────── flavor, with its optional variant letter (c200, c200n, c200d…)
```

A `-mtl-` InfraEnv is bare metal, not a VM pool, and is refused. The InfraEnv has no site: it
belongs to one MCE, so the site comes from the MCE's name, `ocp4-<lifecycle>-mce-<site>-<id>`,
whose lifecycle also picks the seeds (only KV clusters of the same lifecycle). Memory is not in
the name: it is **vCPUs × 7.5 GiB** (section 6.1).

**Where the clusters are.** A KV cluster is a hosted cluster whose control plane runs on a
**seed** cluster of its zone (AD17). The naming convention names both after their site and zone,
and gives each an `id`, a letter in creation order:

| | name | example |
| --- | --- | --- |
| seed | `ocp4-<lifecycle>-seed-<site>-<zone>-<id>` | `ocp4-prod-seed-five-az1-a` |
| KV cluster | `ocp4-<lifecycle>-kv-<site>-<zone>-<id>` | `ocp4-prod-kv-five-az1-a` |

A zone may have **several seeds** (`-a`, `-b`). In the day1 repo the seed folders sit beside the
MCE folders, and a seed's KV clusters are the files of its folder. The `cluster-lifecycle-worker`
on the hub reads them from day1's `main` (`list_kv_clusters`, through the `Day1` query library's
`kv_clusters(site, zone, lifecycle)`):

```
sites/<site>/mces/ocp4-<lifecycle>-seed-<site>-<zone>-<id>/hostedClusters/ocp4-<lifecycle>-kv-<site>-<zone>-<id>.yaml

sites/five/mces/ocp4-prod-seed-five-az1-a/hostedClusters/ocp4-prod-kv-five-az1-a.yaml
sites/five/mces/ocp4-prod-seed-five-az1-a/hostedClusters/ocp4-prod-kv-five-az1-b.yaml
sites/five/mces/ocp4-prod-seed-five-az1-b/hostedClusters/ocp4-prod-kv-five-az1-c.yaml
sites/five/mces/ocp4-test-seed-five-az1-a/hostedClusters/ocp4-test-kv-five-az1-a.yaml   (test: not for a prod MCE)
sites/five/mces/ocp4-prod-mce-five-a/hostedClusters/…                                    (an MCE: never KV clusters)
```

- **The zone's clusters are the files of every seed folder of that zone and the MCE's
  lifecycle.** A folder is a seed when its name parses as one; its site, zone and lifecycle are
  read from that name. A prod MCE's VMs go on prod KV clusters only (the operator, 2026-11-22),
  so the run asks for its MCE's lifecycle and the other lifecycles' seeds are not listed. The
  zone's lock is still one per (site, zone): runs of two lifecycles choose among different
  clusters, and only wait for each other.
- **What is `kv` is decided by FOLDER, never by name or platform.** Only a file under a seed
  folder is a KV cluster. A file under any other folder, an MCE's, is an ordinary hosted
  cluster, even one whose name says `kubevirt` (day1 has `ocp4-test-kubevirt-site4` under an
  MCE) or whose platform is KubeVirt.
- **A cluster is used only when day1 confirms it for the zone.** Three places must agree, as the
  convention's §4.b requires: the KV cluster's NAME (`ocp4-<lifecycle>-kv-<site>-<zone>-<id>`,
  with the seed folder's site, zone and lifecycle), the SEED folder (of this site), and EVERY
  NodePool, which labels its nodes `topology.kubernetes.io/zone` with that zone
  (`nodeLabels`). A file where any of them disagrees, or that has no NodePool, is a
  `day1-problem` row of the run's table and is never read: its VMs would carry the wrong zone.
  A plain YAML warning (a key written twice in a value layer) is reported with the file and
  blocks nothing; judging day1's values is not create-vm's job.
- **The file's name (without `.yaml`) is the cluster's name everywhere.** It is the queue name and
  the `kvCluster` value the worker is deployed with. Both must be the same string.
- **Oldest first, across every seed of the zone.** The clusters are sorted by their `id` letter,
  whichever seed holds them, ties by name. Letters are given in creation order, so the oldest
  cluster is tried first, which is how AD28 fills the active cluster before the standby one.
- No usable cluster in the zone: the run fails at once (`NoKvClusterError`), naming the
  folders it read and every file it listed with a problem.
- Each run reads this list **before** taking the lock, so the locked section stays short.
- **The same read is not repeated for nothing.** The worker remembers day1 for one commit only,
  the latest one it read. Each run first asks git which commit `main` is on, which is one small
  call. If it is the remembered commit, the list is the same by definition and no clone is
  needed. If `main` moved (for example, someone added a cluster file), the worker clones once and
  replaces what it remembered. This is the repo-wide git read cache (the plan's §1.4a), not
  something of create-vm's own. Thirty runs at the same moment cause one clone. Git stays the
  only source of truth: a new cluster is seen by the first run after its commit lands. Every
  list carries the commit it was read at (`day1_revision` in the result).

Today there is one KV cluster per zone. Nothing depends on that: with several, `-a` is tried
before `-b`, whichever seed each is under.

---

## 4. Why the zone is locked, and for how long

**The problem.** Each run reads the clusters' figures and picks one. If ten runs read at the
same moment, they all see the same empty cluster and all pick it, even if it only has room for
two.

**The fix.** Only the run holding the zone's lock may read the figures and choose. It keeps the
lock **until its VM object exists on the chosen cluster**, then releases it. The next run reads
the figures after that, and the cluster's own figures already count the new VM: it is a stopped
VM, and stopped VMs are counted (section 6.3). Nothing has to be remembered between runs.

**How long the lock is held:** the figures of the clusters tried, plus one VM create. That is a
few seconds when every worker answers. The 10-minute wait for the disk happens after the lock
is released, so it does not hold up the zone.

**A lock that is never released.** Each holder gets the lock for at most **5 minutes**. If the
run dies or hangs, the lock passes to the next run after that time. A late release from the old
holder is then ignored.

**A create can never land after the lease.** The lock tells its holder when the lease ends, and
the run passes that time with its VM create. The cluster's worker creates nothing after it: the
run fails instead, with nothing created. So a create that was held up (the worker restarting, for
example) cannot add a VM to a cluster that the next run has already measured without it.

**A waiting run that is gone is skipped at once.** A run can be terminated while it waits, or
give up after 30 minutes (section 8). When the lock comes to it, the lock notices the run is
closed and hands the lock to the next run straight away, so a dead waiter costs the zone no time.
A run that gives up also takes itself out of the line on its way out.

**The cost.** Runs of the same zone choose and create one after another. With several requests
at once, a run waits behind the ones ahead of it, each a few seconds. Runs in different zones
never wait for each other.

---

## 5. The flow, end to end

```mermaid
sequenceDiagram
    participant U as caller
    participant WF as create-vm run (hub)
    participant G as cluster-lifecycle-worker (hub)
    participant L as zone lock (hub)
    participant A as worker in kv cluster -a
    participant B as worker in kv cluster -b

    U->>WF: POST /workflows/vm-lifecycle/create-vm (MCE ocp4-prod-mce-five-a, InfraEnv c200-kv32-az1)
    WF->>G: list the KV clusters of five / az1 (day1)
    G-->>WF: [-a, -b], buffer 15%
    WF->>L: I want the lock
    L-->>WF: it is yours (when the runs ahead have released it)
    WF->>A: your figures?
    A-->>WF: figures
    WF->>WF: -a + this VM: over the buffer, skip
    WF->>B: your figures?
    B-->>WF: figures
    WF->>WF: -b fits
    WF->>B: create a VM: c200, 32 vCPU, 240 GiB, five, az1, MCE
    B->>B: MAC = its prefix + 3 random bytes; name = ocp-c200-kv32-five-<MAC>
    B->>B: MAC taken? draw again (at most 3 draws)
    B-->>WF: created: name, MAC
    WF->>L: release
    Note over WF: from here, in parallel with other runs
    WF->>B: wait for the root disk (up to 10 min)
    B-->>WF: ready
```

| phase | what happens | sent to |
| --- | --- | --- |
| `listing-kv-clusters` | Read the zone's clusters from day1. If there are none, the run fails. | `cluster-lifecycle-worker` |
| `awaiting-zone-lock` | Wait for the zone's lock, at most 30 minutes. | the zone lock |
| `choosing-kv-cluster` | Read each cluster's figures, oldest first, and stop at the first that fits. If none fits, release the lock and fail, listing every cluster and why it was skipped. | each cluster's worker, one after another |
| `creating-vm` | Send what the VM is: flavor, vCPUs, memory, site, zone, MCE, namespace, the run's id, and the lease's end. The worker draws the MAC, builds the name and the VM, and creates it Halted; a taken MAC is drawn again **by the worker**, at most 3 draws, with no round trip to the hub. After the lease's end it creates nothing. | the chosen cluster's worker |
| `releasing-zone-lock` | Give the lock back. This happens on every path, success or failure. | the zone lock |
| `awaiting-root-disk` | Wait until the root disk is ready, up to 10 minutes. | the chosen cluster's worker |
| `removing-vm` | Only if the disk never became ready: delete the VM, then fail. | the chosen cluster's worker |
| `completed` | Return the VM's name, MAC, cluster and namespace. | |

---

## 6. The capacity calculation, in full

**Who does what.** The worker inside each KV cluster **measures** its own cluster; the create-vm
run on the hub **decides**.

| step | who | where |
| --- | --- | --- |
| **read** nodes, pods, VMs and VMIs from the cluster's API (6.2) | the `vm-lifecycle-worker` | inside the KV cluster, as its own ServiceAccount |
| **compute** the figures: allocatable, requested, each node's headroom (6.3) | the same worker | inside the KV cluster; only these few numbers go back to the hub, never the lists of pods and VMs |
| **decide** whether the VM fits, with the buffer, and stop at the first cluster that fits (6.4) | the create-vm run | the hub, while holding the zone's lock |

The split is on purpose. Each worker only knows its own cluster, while "the oldest cluster that
fits" can only be decided by whoever sees them all, the run. And the buffer is one value for every
cluster, so it is applied in one place, not in every worker.

### 6.1 How big a VM is, in the scheduler's terms

The InfraEnv's name gives the vCPUs: `c200-kv32-az1` means 32 vCPUs. Memory is
`vCPUs × 7.5` GiB, so 240 GiB, computed in integers as `cpu × 15 // 2`. The vCPU count must
therefore be **even**, so the memory is a whole number of GiB; every size in use (8 to 64) is.
Capacity is measured by what the VM's pod **requests** from the Kubernetes scheduler:

- **CPU request** = vCPU × 15 / 16, rounded down, at least 1 core
- **memory request** = guest memory + KubeVirt's overhead, counted as
  **300 MiB + 8 MiB × vCPUs + guest memory ÷ 512**. KubeVirt adds this overhead to the launcher
  pod's request itself (page tables, its own processes, per-vCPU state), plus a 35 MB
  console-log container the scheduler also counts. Measured on cluster-qcl8n (OpenShift
  Virtualization 4.22) on four VMs built by `vm_spec.py`, as 260 MiB + 8 MiB × vCPUs +
  guest ÷ 512, + 35 MB: every one matched to the MiB. 300 MiB rounds the fixed part up. The
  earlier flat 1 GiB pad was too small from 32 vCPUs up (a 64-vCPU VM's overhead is about
  1.8 GiB), so placement could approve a VM whose pod then had no node to run on.

| VM (vCPU / memory) | InfraEnv size | CPU request | memory request |
| --- | --- | --- | --- |
| 8 / 60 GiB | `kv8` | 7 cores | 60 GiB + 484 MiB |
| 16 / 120 GiB | `kv16` | 15 cores | 120 GiB + 668 MiB |
| 32 / 240 GiB | `kv32` | 30 cores | 240 GiB + 1,036 MiB |
| 64 / 480 GiB | `kv64` | 60 cores | 480 GiB + 1,772 MiB |

Every number below is in these request terms, never in vCPUs.

#### 6.1.1 Why 7.5 GiB per vCPU

Every KV node has **8 GiB of RAM per core** (the operator, 2026-11-24). At 8 GiB per vCPU the VM
had exactly the node's ratio, and a node gives VMs less than its RAM, so the last VM never fit.
Measured on the operator's KV nodes (`dell-r660-…-128c-1024gb`, 2026-11-24):

| | 128c-1024gb |
| --- | --- |
| RAM installed | 1,024 GiB |
| what the kernel makes usable (`capacity.memory`): 1.62% goes to its own page tables and reserve | 1,007.4 GiB |
| allocatable (day1's kubelet reserve, 15,100 Mi) | 992.6 GiB |
| left for VMs after the node's own pods (OVN, virt-handler, CSI…, 6.6 GiB) | **986.1 GiB** |
| CPUs, allocatable | 128, 124 |

VMs per node, the CPU's limit after the slash:

| GiB per vCPU | 128c-1024gb: kv8 / kv16 / kv32 / kv64 | 192c-1536gb (computed) |
| --- | --- | --- |
| 8 (before) | 15/17, 7/8, 3/4, 1/2 | 23/26, 11/12, 5/6, 2/3 |
| 7.875 or 7.75 | 15/17, 7/8, 3/4, 1/2: no change | 23/26, 11/12, 5/6, 2/3: no change |
| **7.5** | 16/17, **8/8, 4/4, 2/2**, ~18 GiB left | 24/26, **12/12, 6/6, 3/3**, ~38 GiB left |

A ratio works on a node when the memory it leaves unclaimed covers 1.62% of the RAM + the
kubelet reserve + the node's own pods + each VM's overhead: about 4.2% of the RAM on a 128-core
node, 3.6% on a 192-core one. 7.5 leaves 6.25%. Modelled across every 8-GiB-per-core node size
in steps of 8 CPUs (4 cores reserved, the same kubelet reserve and node pods), 7.5 packs as many
kv16, kv32 and kv64 as the CPU allows on most sizes up to 208 CPUs, both node types in use
included, but not on all: it stays one VM short at **64 CPUs** (kv16, by 0.2 GiB), **184** (all
three sizes) and **200** (kv16), and above 208 at 216 and 232 and most sizes from 336 up — where
the CPU's 15/16 discount lets more vCPUs onto a node than 7.5 GiB each can feed. (Corrected
2026-11-24 when the code was written: this paragraph said "every node from 64 to 208";
`tests/test_vm_lifecycle_placement.py::TestTheDesignsNodeTable` runs the fit rule over the sweep
and pins the list.) **Any node type other than 128c-1024gb and 192c-1536gb needs this table
recomputed before it joins**; 7.375 (`cpu × 59 // 8`, vCPUs a multiple of 8) fits every size
from 64 to 512 CPUs. On a 128-core node kv8 stays one VM under the CPU's limit at
any ratio above about 7.1, and is accepted so. Memory overcommit (a request below the guest, backed by node swap) was the other way
to close the gap, and was rejected: `kv-memory-overcommit.md`.

### 6.2 What the worker reads on its cluster when asked

With its own read-only permissions, at the moment of the request:

1. **Nodes.** A node is *usable* when it is Ready and not cordoned. Only usable nodes count.
2. **Pods**, in every namespace, except finished ones (Succeeded or Failed). For each pod: the
   sum of its containers' requests, and the node it runs on.
3. **VirtualMachines** and **VirtualMachineInstances**, in every namespace.

### 6.3 What it reports

| figure | how |
| --- | --- |
| **allocatable** | the sum of `allocatable` CPU and memory over the usable nodes |
| **requested by pods** | the sum of the requests of the pods on usable nodes |
| **requested by stopped VMs** | for every VM labelled `redfish-enabled=true` with **no running instance**, in any namespace and with any run strategy: the request its pod will make when it starts (section 6.1, including KubeVirt's overhead). These VMs have no pod yet, but kubevirt-redfish will start them, so their room is already taken. **This is how a VM created a moment ago is counted.** |
| **requested** | requested by pods + requested by stopped VMs |
| **node headroom** | for each usable node, its allocatable minus the requests of the pods on it, CPU and memory **as a pair**. Kept per node, because the largest free CPU and the largest free memory can be on two different nodes, and then neither node holds the VM. It is one pair of numbers per node, never a pod list. |

#### 6.3.1 The contract

One activity, `read_kv_cluster_figures`, sent to `vm-lifecycle-activity-<kv cluster>`.

**The request** carries one field, the KV cluster's name. The queue already picks the cluster; the
name is there so the worker can check it against the cluster it was deployed for, and a worker on
the wrong queue says so instead of reporting another cluster's numbers.

**The answer:**

| field | meaning |
| --- | --- |
| `kv_cluster` | the cluster the figures are for |
| `readable` | false when the worker answered but could not read its cluster's API in time; the run records it as `unreachable`. It is a value, never an exception, so the run neither retries it nor holds the zone lock while it does |
| `detail` | why it is not readable, bounded to a few hundred characters |
| `allocatable_cpu_millicores`, `allocatable_memory_bytes` | allocatable, over the usable nodes |
| `pods_cpu_millicores`, `pods_memory_bytes` | requested by pods |
| `stopped_vms`, `stopped_vms_cpu_millicores`, `stopped_vms_memory_bytes` | how many stopped VMs were counted, and what they will request |
| `node_headroom` | one entry per usable node: its name, free CPU and free memory |

Every number is an integer, in millicores and bytes, never a Kubernetes quantity string, so the
run never parses `"64Gi"` or `"500m"`. "Requested" is pods plus stopped VMs, computed by whoever
reads the answer.

The VM's size in request terms (6.1) is needed on both sides: the worker uses it for the stopped
VMs, the run for the new VM. So the two formulas live in one sandbox-safe module the brain and
the worker both import.

#### 6.3.2 The call, from the run: `workflow_domains/vm_lifecycle/create_vm.py`

While the run holds the zone's lock, it asks one cluster at a time, oldest first, and stops at
the first that fits. The request goes to **that cluster's queue**: Temporal delivers it to the
worker running inside that cluster.

For each cluster in the list, which is already oldest first (section 3):

1. **Ask for its figures** on `vm-lifecycle-activity-<cluster>`, with two time limits:
   - **30 seconds for a worker to pick the request up.** If none does, the cluster is a
     `no-worker` row and the run moves to the next cluster. Temporal does not retry this
     timeout, so it is a skip, never a failure.
   - **60 seconds for the worker to answer** once it has picked the request up. The worker
     bounds its own reads of its cluster at 20 seconds.
2. **Apply the fit rule** (6.4) to the figures: the VM's request in scheduler terms (6.1) and the
   buffer the cluster list came with.
3. **Record the cluster as a row** of the run's progress and result, with its figures and its skip
   reason, if any.
4. **Stop at the first cluster that fits.** The clusters after it are not read.

The fit rule is a pure function next to the workflow
(`workflow_domains/vm_lifecycle/placement.py`, as `server_lifecycle/bond_selection.py` is). It
returns the first reason that applies, in this order, or none when the VM fits:

| check | reason |
| --- | --- |
| the worker could not read its cluster | `unreachable` |
| the cluster has no usable node | `no-nodes` |
| no single node has room for the VM's CPU **and** memory | `fit-node` |
| requested CPU + the VM's CPU is over `(1 − buffer)` × allocatable CPU | `fit-buffer` |
| the same, for memory | `fit-buffer` |

One attempt takes at most 30 s waiting for a worker plus 60 s running. If the worker dies
mid-read, the retry waits for a worker again (the schedule-to-start limit applies to each
attempt), so a cluster that has lost its worker ends as `no-worker`. It never holds the zone
past its 5-minute lease.

#### 6.3.3 The worker: how it measures

The activity stays thin (`activities/vm_lifecycle/activities.py`). The reading and arithmetic are
in a plain module with no Temporal and no settings
(`activities/vm_lifecycle/cluster_figures.py`), tested with fake API objects.

**The activity, `read_kv_cluster_figures`:**

- **A request for another cluster** than the one the worker was deployed for (`KV_CLUSTER`) is
  answered `readable: false`, naming the cluster the worker is for. A worker on the wrong queue
  never reports its own cluster's numbers under another name.
- **A cluster API it cannot read** is also answered `readable: false`, with the reason cut to a
  few hundred characters and logged. It is never an exception, so Temporal does not retry it.
- Otherwise it answers the figures computed below.

**The reads.** Four list calls to the cluster's own API, as the worker's ServiceAccount, all at
the same time, each cut off at 20 seconds. The Kubernetes client is synchronous, so each call
runs in a thread, as the server-lifecycle worker's calls do.

| call | filter |
| --- | --- |
| nodes | none |
| pods, in every namespace | finished pods (`Succeeded`, `Failed`) left out by the API server |
| VirtualMachines, in every namespace | only those labelled `redfish-enabled=true` |
| VirtualMachineInstances, in every namespace | none: the worker checks their phase itself |

**The arithmetic**, a pure function from those four lists to the figures:

1. **Usable nodes:** Ready and not cordoned. Nothing else counts.
2. **Pods:** each pod's request, added to the node it runs on, for usable nodes only. A pod's
   request is counted the way the scheduler counts it: the larger of (its containers plus its
   sidecar init containers) and its largest ordinary init container, plus the pod's overhead.
   6.2 calls this "the sum of its containers' requests"; it is that sum, with the cases where the
   scheduler counts more.
3. **Stopped VMs:** every labelled VM whose VMI is not `Scheduled`, `Running` or `Unknown`. Those
   three phases have a launcher pod on a node, already counted in step 2. A stopped VM has no pod
   yet, so its request is computed from the VM itself, with the formulas of 6.1: vCPUs = cores ×
   sockets × threads, guest memory from `memory.guest`, else the memory request.
4. **The figures:** allocatable summed over usable nodes; pods summed; stopped VMs counted and
   summed; and for each usable node, its allocatable minus its pods' requests, as a CPU and
   memory pair.

Quantities are parsed with the client's own `kubernetes.utils.parse_quantity` (`"500m"`, `"64Gi"`,
`"1.5"`), converted to integer millicores and bytes right there, and never sent on as strings.

**What the worker's ServiceAccount needs:** `list` on `nodes`, `pods`,
`virtualmachines.kubevirt.io` and `virtualmachineinstances.kubevirt.io`, cluster-wide, all
read-only. Its create, read and delete of a VM (sections 5 and 8) are separate grants.

### 6.4 The fit rule

A VM fits on a cluster when ALL of these hold:

1. **CPU stays under the buffer:** `(requested CPU + the VM's CPU) / allocatable CPU <= 1 − buffer`
2. **Memory stays under the buffer:** the same, for memory
3. **One node can hold it:** at least one node's headroom covers the VM's request, on CPU **and** on memory on that same node

- **The buffer is ONE value for every KV cluster, 15% by default.** It is an environment
  setting of the `cluster-lifecycle-worker` on the hub (`HEADROOM_BUFFER_PERCENT`, from 0 to 90),
  not a setting of each cluster. That worker hands it to the run together with the zone's
  cluster list, so each run records the buffer it used: a changed value applies to the runs
  started after it, never to a run already going. With 15%, a cluster of 100 cores takes at most
  85 cores of requests. The buffer applies to the **whole cluster**, never to one node. Nodes
  may fill completely; the free share is the room live migration needs when a node is drained.
- **Memory is checked as well as CPU**, although it follows CPU. At 7.5 GiB per vCPU a VM asks
  for about 8 GiB per requested core, close to what a node gives VMs per core, so either can run
  out first. At the earlier 8 GiB per vCPU memory always did: on a 128-core, 1,024 GiB node
  (986 GiB left for VMs), a second 64-vCPU VM fit on CPU (120 of 123 cores) but not on memory
  (1,026 GiB); at 7.5 GiB the two need 963.5 GiB and fit (section 6.1.1).
- The run **never chooses a node**. The VM is created Halted; when it is started, the Kubernetes
  scheduler chooses the node. Check 3 only makes sure such a node exists.
- The **first** cluster, oldest first, that fits is chosen. The clusters after it are not even
  read.

The reasons a cluster is skipped, as the run reports them:

| reason | meaning |
| --- | --- |
| `no-worker` | No worker answered on the cluster's queue within 30 seconds. |
| `unreachable` | The worker answered but could not read its cluster in time. |
| `no-nodes` | The cluster has no usable node. |
| `fit-node` | No single node has room for the VM. |
| `fit-buffer` | The VM would push the cluster past the buffer, on CPU or memory. |

A cluster whose day1 file does not give it one zone is never read at all; it is reported with
the listing's problem (section 3).

### 6.5 Example: ten 32-vCPU VMs requested at the same moment

Two clusters, `-a` and `-b`, each with 100 cores allocatable and nothing on them. With the
15% buffer, each can take 85 cores. The ten runs take the lock one after another, and each one
sees the VMs the runs before it created:

| run | `-a` before this run | result | `-a` after | `-b` after |
| --- | --- | --- | --- | --- |
| 1 | 0 + 30 = 30 ≤ 85 | **`-a`** | 30 | 0 |
| 2 | 30 + 30 = 60 ≤ 85 | **`-a`** | 60 | 0 |
| 3 | 60 + 30 = 90 > 85, skip; `-b`: 0 + 30 = 30 | **`-b`** | 60 | 30 |
| 4 | `-a` full; `-b`: 30 + 30 = 60 | **`-b`** | 60 | 60 |
| 5–10 | both would be 90 > 85 | **no capacity** | 60 | 60 |

---

## 7. The VM: its MAC, its name, and who builds it

**The chosen cluster's worker builds the VM.** The run sends it only what the VM *is*; the worker
draws the MAC, derives the name, assembles the VirtualMachine and creates it. A MAC that is
already taken is therefore drawn again inside the cluster, without asking the hub.

### 7.1 What the run sends

| field | example | where the run got it |
| --- | --- | --- |
| `flavor` | `c200` (variant included: `c200n`) | the InfraEnv's name |
| `cpu` | `32` | the InfraEnv's `kv<cpu>` |
| `memory_gib` | `240` | `cpu × 15 // 2` (7.5 GiB per vCPU) |
| `site` | `five` | the MCE's name (the InfraEnv has no site) |
| `zone` | `az1` | the InfraEnv's name |
| `mce` | `ocp4-prod-mce-five-a` | the request |
| `namespace` | `inventory` | the request, defaulted at the API (section 11) |
| `request_id` | the run's workflow id | the run |
| `create_before` | the lease's end | the zone lock, when it granted the lock |

Everything else is the cluster's: KubeMacPool's range, read from the cluster (7.2), and the VM
template's values (storage class, network, node selector), in the worker's ConfigMap. The worker refuses a request whose site
or zone is not its own cluster's (both read from its name, `ocp4-<lifecycle>-kv-<site>-<zone>-<id>`).

### 7.2 The MAC: KubeMacPool's, assigned

**Changed 2026-11-25 (the operator):** the worker chart must carry no per-cluster value, so the
per-cluster prefix below (`0e` + the cluster's VLAN ID) went, and with it the worker's own check
of the MAC against every VM. Now:

- **The VM is created with no MAC, and KubeMacPool assigns one** at admission, from its pool,
  unique on the cluster and written into the VM's spec, so it stays the same across restarts.
  The worker reads it back and returns it; the run needs it for the BareMetalHost.
- **Never a requested MAC.** On cluster-qcl8n (2026-11-25) KubeMacPool admitted three Halted VMs
  requesting the same MAC inside its own range, two in one namespace: it keeps unique only the
  MACs it assigns. (A few hours that day the worker drew a MAC inside KubeMacPool's range and
  requested it; that test removed it.)
- **So every KV cluster must run KubeMacPool**, and the VM namespace must not be labelled
  `mutatevirtualmachines.kubemacpool.io=ignore`. A VM that comes back without a MAC is deleted
  and the create fails, `VmMacNotAssignedError`: a BareMetalHost needs a fixed boot MAC.
- **Across clusters it is not solved yet, on purpose.** Each cluster keeps the random
  `02:xx:xx` /24 KubeMacPool picked, and two clusters can share one. The range is to be set per
  cluster in the HyperConverged object (`spec.networking.kubeMacPoolConfiguration`), later — for
  instance from a hash of the cluster's name, with a check over every KV cluster name in git that
  no two share a range.

What it replaced (2026-11-20 to 2026-11-25):

```
byte 0   bytes 1-2              bytes 3-5
0e     : the cluster's VLAN ID : 3 random bytes
VLAN 1010 = 0x3F2  ->  0e:03:f2:a1:b2:c3
```

A per-cluster `VM_MAC_PREFIX` in the worker's ConfigMap, unique within a site because the
Segments Manager never gives one VLAN to two clusters of a site, with the worker's own check
against every VM of its cluster as the guard, and KubeMacPool's refusal only one more draw.

### 7.3 The name: a serial

```
ocp-<flavor>-kv<cpu>-<site>-<serial, 8 lowercase letters and digits>
ocp-c200-kv32-five-k7q2m9xd
```

- **The serial** is drawn by the worker, uniform over `a-z0-9`: 36^8 = 2.8 trillion values. It
  is also the VM's SMBIOS serial (`firmware.serial`), as a bare-metal node's name carries its
  serial (the convention's `<serial|mac>` field). The Agent's inventory reports it; kubevirt-redfish
  reports no SerialNumber today, so server-scan still matches a VM's Agent by its hostname.
- **Unique on its cluster by a check**: a name any VM of the cluster has, in any namespace, is
  drawn again (Kubernetes refuses a duplicate only within one namespace). **Across a site's
  clusters, by chance**: two of 10,000 VMs of one flavor and size at one site share a serial
  about once in 55,000; such a clash fails add-server-to-mce on the existing BareMetalHost,
  loudly, never silently.
- Until 2026-11-25 the name carried the full MAC (`ocp-c200-kv32-five-0e03f2a1b2c3`), as the
  convention says; with KubeMacPool assigning the MAC at creation, the name could no longer be
  built from it. The convention still says the full MAC: a divergence to raise with its owner.
- **The zone is not in the name** (convention §2): it is on the VM's label, and server-scan
  records it from the KV cluster.
- The name is about 27 characters (`ocp-c200-kv32-five-k7q2m9xd`); Kubernetes allows 63.

### 7.4 The labels

| label | value |
| --- | --- |
| `redbull.io/flavor` | `c200` |
| `redbull.io/size` | `kv32` |
| `redbull.io/site` | `five` |
| `topology.kubernetes.io/zone` | `az1` |
| `redbull.io/role` | `worker` (a tenant warm-pool VM) |
| `redbull.io/mce` | the MCE whose InfraEnv the VM serves |
| `redfish-enabled` | `"true"`, so kubevirt-redfish exposes it and the capacity figures count it |
| `redbull.io/created-by` | the run's id, so a retried create finds its own VM (7.5) |

### 7.5 How the worker creates it

All inside one activity on the chosen cluster, while the run holds the zone's lock:

1. **Has this run already created its VM?** The worker looks for a VM labelled with the run's
   id. If one exists, an earlier attempt of the same activity created it and its answer was lost
   (a pod restart, a timed-out response); the worker returns that VM and draws nothing. Without
   this step a retry would draw a new serial, hence a new name, and create a second VM.
2. **Is the lease over?** If `create_before` has passed, the worker creates nothing and the
   activity fails ("the zone's lease ended before the create"). Step 1 comes first, so a retry
   still finds the VM it already created, whatever the time.
3. **Draw** a serial, build the name. A name some VM of the cluster has, in any namespace, is
   drawn again.
4. **Create** the VM Halted, with no MAC. If the API answers `AlreadyExists` (a VM of that name
   appeared), draw again (back to 3).
5. **At most 3 draws**, then the activity fails with "no free VM name after 3 draws"
   (`VmSerialExhaustedError`), and the run leaves the cluster.
6. **Read back** the run's VMs: exactly one, carrying the MAC KubeMacPool gave it. Without a
   MAC it is deleted and the activity fails (`VmMacNotAssignedError`).

The worker answers with the VM's name, MAC and namespace; the run needs the MAC for the
BareMetalHost.

---

## 8. When things go wrong

| what happens | what the design does |
| --- | --- |
| The zone has no cluster in day1 | The run fails at once with "no KV cluster in this zone". |
| A cluster's day1 file names no zone, or two | It is listed with that problem and never chosen. |
| No cluster fits | The run releases the lock and fails, listing every cluster and its reason. Nothing was created. |
| A cluster's worker is down | That cluster is skipped as `no-worker` after 30 seconds, and the next one is tried. Every run in the zone pays those 30 seconds until the worker is back. |
| The name is already used | The worker draws another serial by itself, on the same cluster, at most 3 draws in total. The hub is not involved. |
| KubeMacPool gives the VM no MAC | The VM is deleted, `VmMacNotAssignedError`, and the run moves to the next cluster. |
| The cluster refuses the VM for another reason (namespace missing, invalid request) | The run releases the lock and fails. |
| The run dies while holding the lock | The lock passes to the next run after 5 minutes. |
| The create is held up past the lease (the worker restarting) | The worker creates nothing after the lease's end, and the run fails with nothing created. The caller asks again. |
| A run waits more than 30 minutes for the lock | It gives up, takes itself out of the line, and fails. |
| A waiting run was terminated | When its turn comes, the lock goes straight to the next run. |
| The root disk is not ready in 10 minutes | The VM is deleted, and the run fails. |
| The worker's pod restarts during the create | Temporal retries. The worker finds the VM labelled with the run's id and returns it, and the disk wait resumes its own clock. |
| The zone lock workflow is restarted | Its line of waiting runs is part of its Temporal state; it carries on where it was. |

---

## 9. Deploying a KV cluster into placement

All of this happens **when the KV cluster is created**; nothing is set on the hub.

1. **Its day1 file**, in the folder of the seed that hosts it
   (`ocp4-<lifecycle>-seed-<site>-<zone>-<id>`), named `ocp4-<lifecycle>-kv-<site>-<zone>-<id>`
   with the seed's lifecycle, site and zone and the next free `id` letter of that zone. Its name
   is the cluster's name from then on. Its NodePools set the same zone as
   `topology.kubernetes.io/zone` in their `nodeLabels`; the name, the seed folder and the
   NodePools must agree, or create-vm never uses it (section 3). Its `vlanId` is the VLAN the Segments
   Manager allocated to it.
2. **The `vm-lifecycle-worker` chart** on the cluster, with its ConfigMap holding:
   - `kvCluster`: exactly the day1 file's name. The worker reads its site and zone from it.
   - no MAC value (since 2026-11-25): KubeMacPool on the cluster assigns every VM's (7.2)
   - the VM template's values: storage class, network, node selector
   - and, beside it, the hub Temporal address and its CA
3. **Its permissions**, all on its own cluster:
   - read nodes, pods, namespaces, VirtualMachineInstances and PersistentVolumeClaims
   - get, list, create and delete VirtualMachines
   - get and delete DataVolumes

A cluster
that is in day1 but has no running worker is skipped as `no-worker` (section 6.4).

The storage class must bind immediately and allow a `ReadWriteMany` + `Filesystem` volume. The
lab found that otherwise every root disk stays unbound and every run ends in the rollback.

---

## 10. What changes compared to today

| | today | this design |
| --- | --- | --- |
| Which clusters exist | the day1 seed folder, read by the hub vm-lifecycle worker | the zone's seed folders in day1 (`ocp4-<lifecycle>-seed-<site>-<zone>-<id>`, the MCE's lifecycle), read by the `cluster-lifecycle-worker` with provision-cluster's token, through the shared day1 reader |
| Where the figures come from | an HTTP agent (`kv-capacity`) on each cluster | the worker inside each cluster |
| Order | best fit across all clusters | oldest first (the cluster's `id` letter), first that fits |
| Two runs at once | can pick the same cluster and overbook it | cannot: one run per zone chooses and creates at a time |
| How the VM is created | an HTTP agent (`kv-vm-api`) behind a Route, with a fleet token | the worker inside the cluster, with its own ServiceAccount |
| Who builds the VM spec | the agent, from what the hub sent (name, MAC, size) | the worker, from what the hub sent (flavor, vCPUs, memory, site, zone, MCE) |
| The MAC | a fixed fleet prefix + 3 random bytes, drawn by the run, checked against every MAC the zone's agents reported | the cluster's own prefix (`0e` + its VLAN) + 3 random bytes, drawn by the cluster's worker, which redraws by itself; unique across clusters by construction |
| The VM's name | `ocp-<infraenv>-<last 6 hex digits of the MAC>` | `ocp-<flavor>-kv<cpu>-<site>-<full MAC>` |
| What a KV cluster exposes | two Routes | nothing |
| Credentials for KV clusters on the hub | two fleet tokens, with TLS not verified by default | none |
| The buffer | a setting of the hub vm-lifecycle worker, default 30% | one setting of the hub `cluster-lifecycle-worker`, `HEADROOM_BUFFER_PERCENT`, default 15% |

---

## 11. Known limits and open points

1. **The listing depends on the naming convention** (`Idan-Maimon/kubevirt-workflows`
   `docs/naming-convention.md`, out of draft since 2026-11-22): seeds and KV clusters named
   with their lifecycle, site and zone, and the zone label on every KV NodePool (section 3). A
   seed or KV cluster named any other way is not listed, or is listed with a problem and never
   used. The interim seed name `ocp4-seed-<site>-<zone number>` (2026-11-20 to 2026-11-22) is
   gone and must not come back.
2. **Memory is `vCPUs × 7.5` GiB, computed for the 128c-1024gb and 192c-1536gb node types**
   (section 6.1.1). The InfraEnv name carries the vCPUs only. Any other node type, or another
   RAM-per-core ratio, needs the table recomputed first: 64, 184 and 200 CPUs already leave a
   size one VM short (7.375 fits every 8-GiB-per-core size). If a flavor ever has another ratio, the ratio must come from the flavor, not from this
   rule. The platform naming convention (`kubevirt-workflows` `docs/naming-convention.md`)
   names the size by vCPUs only (`kv<cpu>`) and defines no memory, so it needs no change; the
   ratio is this repo's (`shared/infraenv_names.py`). The overhead formula was measured on OpenShift
   Virtualization 4.22; upstream KubeVirt computes it the same way, but its constants are worth
   one check (a running VM's launcher pod request) on the first upstream KV cluster.
3. **A VLAN freed by a deleted KV cluster can be given to a new one**, which then gets the old
   prefix. That is safe only once the old cluster's VMs are gone, including their BareMetalHost
   and Agent records on the MCE.
4. **Existing KV clusters need their ConfigMap written once** (the same prefix, from their
   VLAN). Until then their worker does not start, and they are skipped as `no-worker`.
5. **A create held up past the lease fails the run.** Closing the overbooking gap (section 4)
   means a run whose create is delayed more than 5 minutes fails, with nothing created, and the
   caller must ask again. Under AD29 that caller is add-server-to-mce, which retries the pool.
6. **Halted VMs are on no node**, so the "one node can hold it" check cannot see them. Ten nodes
   of 64 cores can really run ten 32-vCPU VMs, but a 15% buffer admits more than that. The extra
   VMs would be created and then fail to start.
7. **A worker that is down slows the whole zone.** Every run in the zone waits 30 seconds for it
   before moving on. Under AD28 a standby cluster is always being built, so this can be routine.
8. **Which namespace the VMs go in is not settled.** create-vm's default is `inventory`, but the
   kubevirt-redfish facade on the platform exposes `server-scan-vms`. Capacity is right either
   way, because the worker counts labelled VMs in every namespace.
9. **The day1 seed folders do not exist yet.** Until a zone has one, every run for it fails with
   "no KV cluster in this zone".
10. **Making the new VM installable is not part of this design.** Under AD29, add-server-to-mce
    runs create-vm as a child for a VM InfraEnv and then installs exactly that VM; that branch
    is add-server-to-mce's, not this page's.

## As implemented (2026-11-20)

Implemented on branch `feat/vm-lifecycle-per-kv-cluster`. The sections above are the design as
reviewed; the code differs in these points. The full list, with what was added beyond the
design, ends `create-vm-placement-plan.md`; the current description is
`docs/site/vm-lifecycle/create-vm.html`.

| Section | The design says | The code does |
|---|---|---|
| 7.1, 7.4, 7.5 | `request_id` is the run's workflow id; `redbull.io/created-by` carries it | Both carry the RUN id. A create-vm workflow id is a slot the next run reuses, so a VM keyed on it would be taken by the slot's next run as its own. The workflow id is the VM's annotation `redbull.io/workflow-id`. |
| 7.2, 7.3 | The `0e` prefix keeps clear of KubeMacPool's range; the name is the MAC | Until 2026-11-25 as designed, with uniqueness the worker's own check, never KubeMacPool's. Since 2026-11-25 (the operator) as 7.2 and 7.3 now read: KubeMacPool assigns the MAC (it does not guard a requested one), the name carries an 8-character serial; `VM_MAC_PREFIX` and the worker's MAC check are gone; the range per cluster is the HyperConverged object's, later. |
| 7.5 | A retried create finds its own VM | And a create that finds TWO VMs of its request keeps the oldest and deletes the other. An attempt ends inside its timeout, and one reaching the worker after `create_before` is refused. |
| 8 | A failed run leaves no VM | A CANCELLED run leaves none either: it deletes the VM carrying its run id, then leaves the line or releases the lock. A terminated run runs no code and can leave one, found by its label. |
| 4 | The lock's lease | A run takes at most three leases for one placement, then fails `KvZoneLeaseExpiredError`. |
| 6.1 | 7.5 GiB per vCPU (even vCPUs), KubeVirt's overhead formula (2026-11-24) | As designed, since 2026-11-24: `MEMORY_GIB_PER_CPU = (15, 2)` and `vm_memory_gib` in `shared/infraenv_names.py`, `launcher_overhead_bytes` in `shared/vm_requests.py`. Only the InfraEnv parser at the API edge refuses an odd size; a VM's name and the figures read any size, so VMs created before the rule are still counted. No `workflow.patched()`: every golden replays unchanged. |
| 4, 8 | A run releases the lock on every path, and a cancelled run rolls back | Only inside its OWN run's workflow task, on a failure or a cancel asked of that run — never in a `finally` (2026-11-24). A brain that shuts down drops the runs it had in memory, and Python throws their coroutines away inside another run's task: a release or a rollback sent from there was recorded in THAT run's history, which then never replayed. `create_vm.py` `_own_task` / `_cancel_requested`; CLAUDE.md §5; checker WF017. A dropped run does nothing on the way out and is replayed by the next brain. |
| 5 | The create is out-waited on the chosen cluster and never moved | A cluster that fails the create (retried to its own 90-s deadline, or refused) is LEFT: fenced, checked for a VM of the run (kept if found), then the next cluster; one that cannot be checked gets a `delete-stray-vm` child (the operator, 2026-11-24). CLAUDE.md §4. |
