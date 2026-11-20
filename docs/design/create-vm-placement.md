# How create-vm places a VM: one zone at a time, a worker in every KubeVirt cluster

This page explains, end to end, how a create-vm run decides which KubeVirt cluster (KV cluster)
a new VM goes on, how the VM gets its MAC address and its name, and who talks to what. It replaces
`kubevirt-vm-placement.md` (every run ranks the clusters on its own) and `kv-agents-api.md` (two
HTTP agents on every KV cluster). It also replaces `kv-placement-server-scan.md`: **server-scan
takes no part in placing or creating a VM.**

Every name on this page follows the platform's naming convention
(`kubevirt-workflows/docs/review/naming-convention-draft.md`): clusters
`ocp4-<lifecycle>-<flavor>-<site>-<id>`, InfraEnvs `<flavor>-kv<cpu>-<site>-<zone>`, VMs
`ocp-<flavor>-kv<cpu>-<site>-<full MAC>`.

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
c200-kv32-five-az1
 │    │    │    └─ zone
 │    │    └────── site
 │    └─────────── kv + vCPU count: 32 vCPUs
 └──────────────── flavor, with its optional variant letter (c200, c200n, c200d…)
```

A `-mtl-` InfraEnv is bare metal, not a VM pool, and is refused. The MCE is a cluster name,
`ocp4-<lifecycle>-<flavor>-<site>-<id>`; its site must be the InfraEnv's site, or the request is
refused. Memory is not in the name: it is **vCPUs × 8 GiB** (section 6.1).

**Where the clusters are.** A KV cluster is a hosted cluster whose control plane runs on its
zone's **seed** cluster (one seed per zone, AD17). The convention reserves two cluster flavors for
them: a seed is `ocp4-<lifecycle>-seed-<site>-<id>`, a KV cluster `ocp4-<lifecycle>-kv-<site>-<id>`.
So the zone's KV clusters are the files of the site's seed folders in the day1 repo's `main`,
read by the `cluster-lifecycle-worker` on the hub:

```
sites/<site>/mces/<seed cluster>/hostedClusters/<kv cluster>.yaml
sites/five/mces/ocp4-prod-seed-five-a/hostedClusters/ocp4-prod-kv-five-a.yaml
```

- **A seed's folder is told apart by its flavor**, `seed`; an MCE's folder holds tenant clusters,
  never KV clusters.
- **The zone is read from the file, not from a name.** Under the naming convention no cluster
  name carries a zone. A KV cluster's zone is the `topology.kubernetes.io/zone` its NodePools set
  on their nodes (`nodeLabels`), which the convention requires on every KV NodePool and which is
  in its day1 file. A KV cluster is single-zone (AD8), so every NodePool of the file must name the
  same zone; a file whose NodePools disagree, or name none, is listed with a problem and never
  chosen.
- **The file's name (without `.yaml`) is the cluster's name everywhere.** It is the queue name and
  the `kvCluster` value the worker is deployed with. Both must be the same string.
- **Oldest first.** The clusters are sorted by their `id`, the sequence letter the convention ends
  every cluster name with (`…-a`, `…-b`). Letters are given in creation order, so the oldest
  cluster is tried first, which is how AD28 fills the active cluster before the standby one.
- No cluster in the zone: the run fails at once with "no KV cluster in this zone".
- Each run reads this list **before** taking the lock, so the locked section stays short.
- **The same read is not repeated for nothing.** The worker remembers the list for one commit
  only, the latest one it read. Each run first asks git which commit `main` is on, which is one
  small call. If it is the remembered commit, the list is the same by definition and no clone is
  needed. If `main` moved (for example, someone added a cluster file), the worker clones once,
  lists every site's seed folders, and replaces what it remembered. Thirty runs at the same moment
  cause one clone. Git stays the only source of truth: a new cluster is seen by the first run
  after its commit lands.

Today there is one KV cluster per zone. Nothing depends on that: with several, `-a` is tried
before `-b`.

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

    U->>WF: POST /workflows/vm-lifecycle/create-vm (MCE, InfraEnv c200-kv32-five-az1)
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
    WF->>B: create a VM: c200, 32 vCPU, 256 GiB, five, az1, MCE
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

The InfraEnv's name gives the vCPUs: `c200-kv32-five-az1` means 32 vCPUs. Memory is
`vCPUs × 8` GiB, so 256 GiB. Capacity is measured by what the VM's pod **requests** from the
Kubernetes scheduler:

- **CPU request** = vCPU × 15 / 16, rounded down, at least 1 core
- **memory request** = guest memory + 1 GiB, for KubeVirt's own overhead. The lab measured about
  0.3 GiB, so 1 GiB errs on the safe side.

| VM (vCPU / memory) | InfraEnv size | CPU request | memory request |
| --- | --- | --- | --- |
| 8 / 64 GiB | `kv8` | 7 cores | 65 GiB |
| 16 / 128 GiB | `kv16` | 15 cores | 129 GiB |
| 32 / 256 GiB | `kv32` | 30 cores | 257 GiB |
| 64 / 512 GiB | `kv64` | 60 cores | 513 GiB |

Every number below is in these request terms, never in vCPUs.

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
| **requested by stopped VMs** | for every VM labelled `redfish-enabled=true` with **no running instance**, in any namespace and with any run strategy: the request its pod will make when it starts (section 6.1, including the 1 GiB). These VMs have no pod yet, but kubevirt-redfish will start them, so their room is already taken. **This is how a VM created a moment ago is counted.** |
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

```python
_FIGURES_TIMEOUT = timedelta(seconds=60)      # one attempt; the worker bounds its own reads at 20 s
_NO_WORKER_TIMEOUT = timedelta(seconds=30)    # nobody polls the queue -> `no-worker`

# phase: choosing-kv-cluster (zone lock held)
vm_cpu = pod_cpu_millicores(request.cpu)
vm_memory = pod_memory_bytes(request.memory_gib * GIB)
chosen: str | None = None
for cluster in listed.clusters:                   # already oldest first (section 3)
    try:
        figures: KvClusterFigures = await workflow.execute_activity(
            read_kv_cluster_figures,
            KvFiguresRequest(kv_cluster=cluster.name),
            task_queue=vm_lifecycle_activity_queue(cluster.name),
            start_to_close_timeout=_FIGURES_TIMEOUT,
            schedule_to_start_timeout=_NO_WORKER_TIMEOUT,
            retry_policy=_RETRY_POLICY,
        )
    except ActivityError as err:
        # Temporal does not retry a schedule-to-start timeout: it means no worker
        # polled this cluster's queue for 30 s. That is a skip, not a failure.
        if isinstance(err.cause, TimeoutError) and err.cause.type == TimeoutType.SCHEDULE_TO_START:
            self._skip(cluster.name, "no-worker")
            continue
        raise
    reason = skip_reason(figures, vm_cpu, vm_memory, listed.buffer_percent)
    self._record(cluster.name, figures, reason)   # every cluster tried is a row in progress/result
    if reason is None:
        chosen = cluster.name
        break                                     # the clusters after it are not read (6.4)
```

`skip_reason` is the fit rule of 6.4, pure and next to the workflow
(`workflow_domains/vm_lifecycle/placement.py`, as `server_lifecycle/bond_selection.py` is):

```python
def skip_reason(f: KvClusterFigures, vm_cpu: int, vm_memory: int, buffer_percent: int) -> str | None:
    if not f.readable:
        return "unreachable"
    if not f.node_headroom:
        return "no-nodes"
    if not any(n.cpu_millicores >= vm_cpu and n.memory_bytes >= vm_memory for n in f.node_headroom):
        return "fit-node"
    limit = (100 - buffer_percent) / 100
    if (f.requested_cpu_millicores + vm_cpu) > limit * f.allocatable_cpu_millicores:
        return "fit-buffer"
    if (f.requested_memory_bytes + vm_memory) > limit * f.allocatable_memory_bytes:
        return "fit-buffer"
    return None
```

One attempt takes at most 30 s waiting for a worker plus 60 s running. If the worker dies
mid-read, the retry waits for a worker again (the schedule-to-start limit applies to each
attempt), so a cluster that has lost its worker ends as `no-worker`. It never holds the zone
past its 5-minute lease.

#### 6.3.3 The worker: how it measures

The activity stays thin (`activities/vm_lifecycle/activities.py`). The reading and arithmetic are
in a plain module with no Temporal and no settings
(`activities/vm_lifecycle/cluster_figures.py`), tested with fake API objects.

```python
# activities/vm_lifecycle/activities.py
@activity.defn
async def read_kv_cluster_figures(request: KvFiguresRequest) -> KvClusterFigures:
    settings = _settings()                        # KV_CLUSTER: the cluster this worker is deployed in
    if request.kv_cluster != settings.kv_cluster:
        return KvClusterFigures(
            kv_cluster=request.kv_cluster, readable=False,
            detail=f"the worker on this queue is deployed for {settings.kv_cluster}",
        )
    try:
        return await cluster_figures.read(settings.kv_cluster, timeout_seconds=20)
    except cluster_figures.ClusterReadError as err:
        activity.logger.warning("cannot read %s: %s", settings.kv_cluster, err)
        return KvClusterFigures(kv_cluster=settings.kv_cluster, readable=False, detail=str(err)[:500])
```

```python
# activities/vm_lifecycle/cluster_figures.py
REDFISH_LABEL = "redfish-enabled=true"
_POD_ON_A_NODE = {"Scheduled", "Running", "Unknown"}   # VMI phases whose launcher pod is already counted


async def read(kv_cluster: str, *, timeout_seconds: int) -> KvClusterFigures:
    """Four list calls, each bounded; the kubernetes client is synchronous, so
    each runs in a thread (the server-lifecycle precedent)."""
    core, custom = _clients()                     # in-cluster config, this worker's ServiceAccount
    t = {"_request_timeout": timeout_seconds}
    try:
        nodes, pods, vms, vmis = await asyncio.gather(
            asyncio.to_thread(core.list_node, **t),
            asyncio.to_thread(core.list_pod_for_all_namespaces,
                              field_selector="status.phase!=Succeeded,status.phase!=Failed", **t),
            asyncio.to_thread(custom.list_cluster_custom_object, "kubevirt.io", "v1",
                              "virtualmachines", label_selector=REDFISH_LABEL, **t),
            asyncio.to_thread(custom.list_cluster_custom_object, "kubevirt.io", "v1",
                              "virtualmachineinstances", **t),
        )
    except (ApiException, urllib3.exceptions.HTTPError) as err:
        raise ClusterReadError(_bounded(err)) from err
    return figures(kv_cluster, nodes.items, pods.items, vms["items"], vmis["items"])


def figures(kv_cluster, nodes, pods, vms, vmis) -> KvClusterFigures:
    """Pure: the API objects in, the figures out."""
    usable = {n.metadata.name: n for n in nodes if _ready(n) and not n.spec.unschedulable}

    # Pods: what each one requests, summed per node, on usable nodes only.
    on_node: dict[str, tuple[int, int]] = {name: (0, 0) for name in usable}
    for pod in pods:
        if pod.spec.node_name in on_node:
            cpu, mem = pod_request(pod.spec)
            c, m = on_node[pod.spec.node_name]
            on_node[pod.spec.node_name] = (c + cpu, m + mem)

    # Stopped VMs: labelled for kubevirt-redfish, with no launcher pod on a node.
    # Their pod does not exist yet, so the request is computed from the VM itself.
    running = {(i["metadata"]["namespace"], i["metadata"]["name"])
               for i in vmis if i.get("status", {}).get("phase") in _POD_ON_A_NODE}
    stopped = [vm for vm in vms
               if (vm["metadata"]["namespace"], vm["metadata"]["name"]) not in running]
    stopped_cpu = sum(pod_cpu_millicores(_vcpu(vm)) for vm in stopped)
    stopped_mem = sum(pod_memory_bytes(_guest_memory_bytes(vm)) for vm in stopped)

    alloc = {name: (_millicores(n.status.allocatable["cpu"]), _bytes(n.status.allocatable["memory"]))
             for name, n in usable.items()}
    return KvClusterFigures(
        kv_cluster=kv_cluster,
        readable=True,
        allocatable_cpu_millicores=sum(c for c, _ in alloc.values()),
        allocatable_memory_bytes=sum(m for _, m in alloc.values()),
        pods_cpu_millicores=sum(c for c, _ in on_node.values()),
        pods_memory_bytes=sum(m for _, m in on_node.values()),
        stopped_vms=len(stopped),
        stopped_vms_cpu_millicores=stopped_cpu,
        stopped_vms_memory_bytes=stopped_mem,
        node_headroom=[
            KvNodeHeadroom(node=name, cpu_millicores=alloc[name][0] - on_node[name][0],
                           memory_bytes=alloc[name][1] - on_node[name][1])
            for name in sorted(usable)
        ],
    )


def pod_request(spec) -> tuple[int, int]:
    """A pod's request the way the scheduler counts it: the larger of (its
    containers + sidecar init containers) and its largest ordinary init container,
    plus the pod's overhead. 6.2 says "sum of its containers' requests"; this
    is that sum, with the cases where the scheduler counts more."""
    app = _sum(spec.containers) + _sum(c for c in spec.init_containers or [] if c.restart_policy == "Always")
    init = max((_req(c) for c in spec.init_containers or [] if c.restart_policy != "Always"),
               default=(0, 0), key=lambda r: r[0])
    overhead = _from_quantities(spec.overhead or {})
    return (max(app[0], init[0]) + overhead[0], max(app[1], init[1]) + overhead[1])


def _vcpu(vm: dict) -> int:
    cpu = vm["spec"]["template"]["spec"]["domain"].get("cpu", {})
    return cpu.get("cores", 1) * cpu.get("sockets", 1) * cpu.get("threads", 1)


def _guest_memory_bytes(vm: dict) -> int:
    domain = vm["spec"]["template"]["spec"]["domain"]
    guest = domain.get("memory", {}).get("guest") or domain["resources"]["requests"]["memory"]
    return _bytes(guest)
```

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
- **Memory is checked as well as CPU**, although it follows CPU. A VM asks for about 8.5 GiB per
  requested core, so on a node with 8 GiB per core memory runs out first. On a 64-core, 512 GiB
  node, a second 32-vCPU VM fits on CPU (60 of 64 cores) but not on memory (514 of 512 GiB).
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
| `memory_gib` | `256` | `cpu × 8` |
| `site` | `five` | the InfraEnv's name |
| `zone` | `az1` | the InfraEnv's name |
| `mce` | `ocp4-prod-mce-five-a` | the request |
| `namespace` | `inventory` | the request, defaulted at the API (section 11) |
| `request_id` | the run's workflow id | the run |
| `create_before` | the lease's end | the zone lock, when it granted the lock |

Everything else is the cluster's: its MAC prefix, and the VM template's values (storage class,
network, node selector), all in the worker's ConfigMap. The worker refuses a request whose site
is not its own cluster's (read from its name, `ocp4-<lifecycle>-kv-<site>-<id>`).

### 7.2 The MAC: half the cluster's, half random

```
byte 0   bytes 1-2              bytes 3-5
0e     : the cluster's VLAN ID : 3 random bytes
        (12 bits)                drawn by the worker
VLAN 1010 = 0x3F2  ->  0e:03:f2:a1:b2:c3
VLAN 2    = 0x002  ->  0e:00:02:5d:0c:9e
```

- **The first half is the KV cluster's own prefix**, `VM_MAC_PREFIX` in the worker's ConfigMap,
  written **when the KV cluster is created** (section 9) and never on the hub. It is `0e`
  followed by the VLAN ID the Segments Manager allocated to that cluster.
- **Why it can never repeat across clusters:** the Segments Manager never gives one VLAN to two
  clusters of a site, and every VM of an MCE boots on that site's inventory network. So two KV
  clusters that could put VMs on the same network always have different prefixes, without either
  one knowing the other.
- **Why `0e`:** it marks the address as locally administered (never a real vendor's NIC, such as
  a bare-metal server on the same network) and unicast. It is deliberately not `02`, the range
  OpenShift Virtualization's KubeMacPool gives out, nor `0a`, which OVN-Kubernetes gives pods, so
  no other software on the network draws from the same space.
- **The second half is 3 random bytes**, drawn by the worker: 16.7 million MACs per cluster.

### 7.3 The name: the MAC in full

```
ocp-<flavor>-kv<cpu>-<site>-<MAC, 12 hex digits without colons>
ocp-c200-kv32-five-0e03f2a1b2c3
```

- It follows the naming convention for a VM node. The full MAC makes the name unique wherever the
  MAC is, so two VMs of the same flavor and size on two clusters of one site never share a name.
- **The zone is not in the name** (convention §2): it is on the VM's label, and server-scan
  records it from the KV cluster.
- The name is at most about 34 characters; Kubernetes allows 63.

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
   this step a retry would draw a new MAC, hence a new name, and create a second VM.
2. **Is the lease over?** If `create_before` has passed, the worker creates nothing and the
   activity fails ("the zone's lease ended before the create"). Step 1 comes first, so a retry
   still finds the VM it already created, whatever the time.
3. **Draw** 3 random bytes, build the MAC and the name.
4. **Is the MAC taken?** The worker checks it against every VM on its cluster, in every
   namespace. The name alone is not enough: Kubernetes refuses a duplicate name only within one
   namespace. A taken MAC is drawn again (back to 3).
5. **Create** the VM Halted. If the API answers `AlreadyExists` (a VM of that name appeared),
   draw again (back to 3).
6. **At most 3 draws**, then the activity fails with "no free MAC after 3 draws", which fails the
   run after the lock is released.

Because the zone's lock is held, no other run of ours creates a VM on that cluster at the same
moment, so "check, then create" cannot race. The worker answers with the VM's name, MAC and
namespace; the run needs the MAC for the BareMetalHost.

---

## 8. When things go wrong

| what happens | what the design does |
| --- | --- |
| The zone has no cluster in day1 | The run fails at once with "no KV cluster in this zone". |
| A cluster's day1 file names no zone, or two | It is listed with that problem and never chosen. |
| No cluster fits | The run releases the lock and fails, listing every cluster and its reason. Nothing was created. |
| A cluster's worker is down | That cluster is skipped as `no-worker` after 30 seconds, and the next one is tried. Every run in the zone pays those 30 seconds until the worker is back. |
| The MAC or name is already used | The worker draws again by itself, on the same cluster, at most 3 draws in total. The hub is not involved. |
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

1. **Its day1 file**, in its zone's seed folder, named `ocp4-<lifecycle>-kv-<site>-<id>`. Its
   name is the cluster's name from then on. Its NodePools set `topology.kubernetes.io/zone` in
   their `nodeLabels`: that is how create-vm knows its zone (section 3). Its `vlanId` is the
   VLAN the Segments Manager allocated to it.
2. **The `vm-lifecycle-worker` chart** on the cluster, with its ConfigMap holding:
   - `kvCluster`: exactly the day1 file's name. The worker reads its site from it.
   - `VM_MAC_PREFIX`: `0e` + the cluster's VLAN ID (section 7.2), computed from the same
     allocation, never typed in by hand. It is the only place the prefix is set; the VLAN ID
     itself is not stored beside it.
   - the VM template's values: storage class, network, node selector
   - and, beside it, the hub Temporal address and its CA
3. **Its permissions**, all on its own cluster:
   - read nodes, pods, namespaces, VirtualMachineInstances and PersistentVolumeClaims
   - get, list, create and delete VirtualMachines
   - get and delete DataVolumes

The worker refuses to start without a valid `VM_MAC_PREFIX` (3 octets, `0e` first). A cluster
that is in day1 but has no running worker is skipped as `no-worker` (section 6.4).

The storage class must bind immediately and allow a `ReadWriteMany` + `Filesystem` volume. The
lab found that otherwise every root disk stays unbound and every run ends in the rollback.

---

## 10. What changes compared to today

| | today | this design |
| --- | --- | --- |
| Which clusters exist | the day1 seed folder, read by the hub vm-lifecycle worker | the site's seed folders in day1, filtered by the zone each cluster's NodePools label, read by the `cluster-lifecycle-worker` with provision-cluster's token |
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

1. **The naming convention is still a draft.** Since 2026-11-19 it reserves the `seed` and `kv`
   cluster flavors and requires the zone label on every KV NodePool, which is all the listing
   (section 3) relies on. A change to either would change the listing.
2. **Memory is `vCPUs × 8` GiB.** The InfraEnv name carries the vCPUs only. If a flavor ever has
   another ratio, the ratio must come from the flavor, not from this rule.
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
