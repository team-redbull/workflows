# KubeVirt cluster placement for create-vm — the metric and the choice rule

Status: **implemented, 2026-11-05** — `activities/vm_lifecycle/placement.py`
is this document's one implementation, called by create-vm's `rank_kv_clusters`
activity. First written 2026-11-04, before the workflow was designed; the
sections below were amended where the design settled differently (each
amendment is marked). Written against the production KubeVirt layout (per site,
zones in the data centre; per zone, one seed cluster hosting several KubeVirt
hosted clusters) and the VM template in `redbull-platform/vm.yaml`.

## The ask

create-vm will have to pick ONE KubeVirt cluster among those of the requested
(site, zone). The clusters bin-pack (the goal is each cluster as full as it can
be), and a configured share of each cluster's CPU must stay unallocated — the
BUFFER (30% for example). Wanted: a per-cluster metric that says whether a VM
of a given size fits with the buffer intact, and a rule that picks the cluster
that ends up FULLEST, not the first one that fits.

Multi-cluster scheduling proper is later research; this is the interim design.

## Three things settled up front

- **A metric cannot take the VM's size and answer true.** A metric is a gauge.
  What exists per cluster is a HEADROOM series; the fit test with the buffer is
  a predicate evaluated at decision time with the VM's size as a parameter.
- **The descheduler is on the wrong axis.** It evicts inside ONE cluster after
  placement and can never move a VM between clusters. It matters for keeping a
  cluster packed (last section), not for choosing one.
- **First-fit is wrong for the goal; the rule is best-fit:** among the
  clusters where the VM fits with the buffer intact, the one that is fullest
  AFTER placement.

## 1. What "CPU" means here — the scheduler view (decided by `vm.yaml`)

KubeVirt's virt-launcher pod does not request one core per vCPU by default:
with the default `cpuAllocationRatio` of 10 a 64-vCPU VM requests 6.4 CPU, so
the scheduler's view undercounts vCPUs ten times over and "30% buffer" means
two different things in pod-request terms and in vCPU terms.

**That trap does not apply to us.** `vm.yaml` sets explicit requests, and
KubeVirt applies the allocation ratio only to a VM with no CPU request of its
own:

```yaml
domain:
  cpu:
    cores: {{ .spec.cpu }}
  memory:
    guest: {{ .spec.memory }}
  resources:
    requests:
      cpu: "{{ div (mul (int .spec.cpu) 15) 16 }}"   # integer division
      memory: {{ .spec.memory }}
    limits:
      cpu: "{{ .spec.cpu }}"
      memory: {{ .spec.memory }}
```

| VM (vCPU / guest memory) | pod CPU request | pod CPU limit | pod memory request |
| --- | --- | --- | --- |
| 32 / 128Gi | 30 | 32 | 128Gi + launcher overhead |
| 64 / 256Gi | 60 | 64 | 256Gi + launcher overhead |

Consequences:

- Consumption is **active pod requests**, capacity is **node allocatable** —
  what the kube scheduler itself compares. No vCPU branch, no
  `kubevirt_vm_resource_requests` join, no overcommit factor in the capacity.
- The VM's CPU size in the predicate is the POD REQUEST the template produces,
  `C = max(1, floor(vCPU × 15 / 16))`, never the vCPU count. That formula lives ONCE,
  in the plain-parameter placement module next to the fit test, so the
  workflow and the template cannot disagree; if the template's ratio changes,
  that constant follows it.
- The buffer is in request terms, which here is roughly physical cores: 30%
  of allocatable is close to 30% of real cores.
- **Memory is the likelier binding constraint** for 256Gi VMs and is checked
  the same way. The pod requests the guest memory PLUS KubeVirt's launcher
  overhead (a few hundred MiB, growing with vCPUs, disks, interfaces), so
  `M = guest + overhead`. Pad by a fixed 1Gi and confirm it against
  `kubevirt_vmi_launcher_memory_overhead_bytes` once the first 64/256 VM runs. The
  cluster's memory fill from pod requests already includes the real overhead
  of the VMs that exist.
- **The 15/16 ratio leaves node fragments.** A 64-core-allocatable node holds
  two 32-vCPU VMs at 60 requested and 4 cores nothing of ours can use. The
  per-node check below handles it; the cluster fill ratio never reaches 100%
  on such a fleet, so the buffer and the alert threshold are set with that
  ceiling in mind.
- **`runStrategy: Halted` moves the moment of truth to START.** A VM created
  Halted consumes nothing on the scheduler and the active-pod view does not
  count it. The fit test and the choice are made right before the START, and
  the verify-after-mutate loop (§3) wraps the start, not the create. Keeping
  Halted VMs as reserved capacity is NOT the design; it would need the stopped
  VMs' `kubevirt_vm_resource_requests{source="requests"}` (emitted for stopped
  VMs too) added to consumption.
  **AMENDED 2026-11-04, decided with the operator: Halted VMs of ours with no
  launcher pod DO count, toward the CLUSTER fill (never toward per-node
  headroom — they sit on no node yet).** create-vm's end state is Halted and
  kubevirt-redfish starts the VM later, so two creates in a row must not both
  see an empty cluster. The kv-capacity agent reports them as
  `halted_reservations`, sized by the template's requests (else as KubeVirt
  would size the launcher), and the verify-after-mutate wrap is around the
  ROOT DISK, not a start.

There are no VMs yet; this is the architecture they will be created into. The
rule to carry forward is that **every VM on a KV cluster comes from this
template**, or from one that sets explicit CPU requests the same way: a VM
without a request of its own is sized by the allocation ratio, its fill is
understated ten times over, and the ranking favours that cluster wrongly.
create-vm itself only ever creates from the template, so the rule holds by
construction unless something else creates VMs there.

## 2. The metric: per-node headroom, per-cluster fill

Best practice: kube-state-metrics (shipped by OpenShift cluster monitoring on
every cluster) plus recording rules in a `PrometheusRule` in
`openshift-monitoring`. **Fit is PER NODE, the buffer is PER CLUSTER.** A
cluster can have 200 free cores over ten nodes and hold no 64-vCPU VM; and a
per-node buffer would be the opposite of bin packing. Built on the
kubernetes-mixin rule
`cluster:namespace:pod_cpu:active:kube_pod_container_resource_requests`, which
counts only Pending and Running pods and carries the `node` label.

```yaml
groups:
- name: kv-placement.rules
  rules:
  # schedulable, Ready nodes only
  - record: kv:node_schedulable:bool
    expr: |
      (kube_node_spec_unschedulable == 0)
        * on (node) group_left() (kube_node_status_condition{condition="Ready",status="true"} == 1)
  # active (Pending|Running) requests per node
  - record: kv:node_cpu_requests:sum
    expr: sum by (node) (cluster:namespace:pod_cpu:active:kube_pod_container_resource_requests)
  - record: kv:node_memory_requests:sum
    expr: sum by (node) (cluster:namespace:pod_memory:active:kube_pod_container_resource_requests)
  # headroom per node — what the fit test uses ("or 0 * …" keeps an empty node at full headroom)
  - record: kv:node_cpu_headroom_cores
    expr: |
      (kube_node_status_allocatable{resource="cpu"}
        - on (node) (kv:node_cpu_requests:sum or 0 * kube_node_status_allocatable{resource="cpu"}))
        * on (node) kv:node_schedulable:bool
  - record: kv:node_memory_headroom_bytes
    expr: |
      (kube_node_status_allocatable{resource="memory"}
        - on (node) (kv:node_memory_requests:sum or 0 * kube_node_status_allocatable{resource="memory"}))
        * on (node) kv:node_schedulable:bool
  # cluster fill ratio — what the buffer and the ranking use
  - record: kv:cluster_cpu_fill:ratio
    expr: |
      sum(kv:node_cpu_requests:sum * on (node) kv:node_schedulable:bool)
        / sum(kube_node_status_allocatable{resource="cpu"} * on (node) kv:node_schedulable:bool)
  - record: kv:cluster_memory_fill:ratio
    expr: |
      sum(kv:node_memory_requests:sum * on (node) kv:node_schedulable:bool)
        / sum(kube_node_status_allocatable{resource="memory"} * on (node) kv:node_schedulable:bool)
```

The predicate for a VM of `C` cores (the pod request) and `M` bytes (guest +
overhead) against buffer `B`:

```
fits(cluster) =  max(kv:node_cpu_headroom_cores)      >= C
             and max(kv:node_memory_headroom_bytes)   >= M
             and kv:cluster_cpu_fill:ratio    + C / sum(allocatable cpu)    <= 1 - B
             and kv:cluster_memory_fill:ratio + M / sum(allocatable memory) <= 1 - B
```

Add ONE alert per cluster on either fill ratio crossing `1 - B`, so a human
sees a zone running out before create-vm starts skipping clusters.

### Where the decision reads from

**AMENDED 2026-11-04: neither row below is the read path.** The decision reads
each cluster's **kv-capacity agent** (`GET /v1/capacity`, behind a Route, one
fleet bearer token; `docs/design/kv-agents-api.md`), which computes the node
table from the live API inside the cluster — the scheduler's exact view, no
kubeconfig held by any limb, no monitoring pipeline in the path. The agent is
DUMB: it reports nodes, requests, our Halted reservations and every VM's MAC;
the fit test, the buffer and the ranking stay in
`activities/vm_lifecycle/placement.py`. The recording rules above remain the
dashboard/alert shape for operators.

| option | what it gives | what it costs |
| --- | --- | --- |
| **Hub Thanos (ACM Observability)**, one query over `cluster=~"<the zone's clusters>"` | dashboards, the alert, one place for humans | needs ACM (not just MCE); scrape + forward lag of a minute or more; the default allowlist ships `kube_node_status_allocatable`, `kube_node_spec_unschedulable`, `kube_pod_container_resource_requests`, `kube_pod_info`, `kubevirt_vm_resource_requests` but NOT `kube_pod_status_phase`, so the active-pod join cannot be computed on the hub — the `kv:*` recorded series go on a custom allowlist |
| **Direct Kubernetes API read of each KV cluster at decision time** (recommended for the decision) | the scheduler's exact current view, no monitoring pipeline in the path | a handful of list calls (nodes, pods) per cluster per run |

(The kubeconfig-per-zone-MCE limb this section first proposed was rejected
with the agents' design: one hub limb, `vm-lifecycle-worker`, holds no
Kubernetes credential at all.) The formula, the `C`/`M` sizing and the ranking
live in a plain-parameter module (`activities/vm_lifecycle/placement.py`: no
settings, no Temporal, §1 of CLAUDE.md), unit-tested against hand-built node
tables (`tests/test_vm_lifecycle_placement.py`).

## 3. Choosing the cluster: best-fit, the scheduler stays the authority

1. Filter the zone's clusters to those where `fits` holds.
2. Rank the survivors by fill AFTER placement, highest first, on the DOMINANT
   resource (the larger of CPU fill and memory fill), so a memory-heavy VM does
   not pack a CPU-full cluster into a memory wall.
3. Tie-break deterministically by cluster name.

That is best-fit bin packing: the cluster the VM fills up most. First-fit is
what falls out of stopping at the first `true`.

**AMENDED 2026-11-04: NO candidate loop.** The VM is created Halted, so the
scheduler is not consulted at create time and there is no `Unschedulable`
event to react to; and a move after an ambiguous failure could leave two VMs.
create-vm makes ONE create on the chosen cluster, out-waited, then a BOUNDED
wait (10 min) for the ROOT DISK to bind (CLAUDE.md §5: a machine wait gets a
deadline; §6: verify after mutate). A disk that never binds is rolled back —
the VM deleted by name and MAC — and the run fails; nothing survives on a
cluster that did not take the VM, because a re-run draws a new suffix and could
never converge on an orphan.

The concurrency hole stays closed by the buffer and the reservations: two
create-vm runs in one zone reading the same snapshot may pick the same
cluster; both VMs land as Halted reservations, the buffer absorbs the double
booking, and the next run sees both. NO placement lock (§5: narrow inputs or
verify, rather than add detection).

Progress and result carry the full ranked table — every cluster with its fill
before and after and its skip reason — the way add-server-to-mce reports its
candidates (`_REJECTION_SUMMARY`), so an operator sees why B was chosen over A.

## 4. The buffer setting

ONE knob, `HEADROOM_BUFFER_PERCENT`, applied to CPU and to memory alike
(decided with the implementation): no service prefix, it is OUR policy (§3);
read in the ACTIVITY only, never from workflow code (§8); value in the
vm-lifecycle-worker chart's `values.yaml` (`config.headroomBufferPercent`), the
ConfigMap template only renders it; validated fail-fast at worker start (0..90).

- It applies to the CLUSTER fill, never per node.
- A flat percentage may be the wrong shape. The real reason for headroom is
  live migration: upgrade-cluster rolls NodePool nodes and every VM on a
  draining node must fit elsewhere in the SAME cluster (`evictionStrategy:
  LiveMigrate` in `vm.yaml`). The honest definition is "the largest node's
  requests fit in the rest", roughly N+1. Keep the percentage as the knob; the
  alert tells us if 30% is below one node's worth on any cluster.

## 5. The descheduler, and keeping each cluster packed

Within a cluster, bin packing is TWO settings that must agree: the
`HighNodeUtilization` scheduler profile in the `Scheduler` CR (as many pods on
as few nodes as possible) and the Kube Descheduler Operator's `CompactAndScale`
profile (evicts from nodes below 20% so they land on fuller ones). "Bin
packing enabled" may mean only the first; check whether the operator is
installed and which profiles its CRD enum offers in our version. For VMs,
eviction means live migration. The operator's VM-aware profile,
`KubeVirtRelieveAndMigrate`, BALANCES load across nodes on PSI metrics — the
opposite of compaction — and conflicts with `CompactAndScale`: one goal per
cluster. None of this chooses between clusters.

For the later multi-cluster research: the ACM-native shape is Open Cluster
Management `Placement` with `AddOnPlacementScore` — a small controller
publishes a score per managed cluster (the fill ratio above) and `Placement`
ranks on it. The formula here is reusable there unchanged.

## 6. Still open

- Will anything other than create-vm create VMs on the KV clusters? If so,
  it must set explicit CPU requests like `vm.yaml` (§1); the kv-capacity agent
  sizes a Halted VM without requests as KubeVirt would (the allocation ratio),
  which understates it.
- The launcher memory overhead of a 64/256 VM, to confirm the 1Gi pad: read
  `kubevirt_vmi_launcher_memory_overhead_bytes` from the first one created.
- Whether the Kube Descheduler Operator is on the KV clusters, and its profile.


Settled since the first draft: cluster → (site, zone) comes from the day1
seed folder (CLAUDE.md §4), not a label; the hub Thanos option is moot (the
agents are the read path); the CPU request is floored at one core on both
sides (2026-11-05), since `cpu × 15 / 16` makes a 1-vCPU VM request nothing.

## Sources

- kubernetes-mixin recording rules (`apps.libsonnet`): the active-pod request
  rules, Pending|Running only.
- ACM observability default allowlist
  (`stolostron/multicluster-observability-operator`,
  `metrics_allowlist.yaml`): which series reach the hub.
- KubeVirt metrics reference (kubevirt.io/monitoring/metrics.html) and the
  virt-controller `vmstats_collector.go`: `kubevirt_vm_resource_requests`
  labels (`source` = domain | requests | guest_effective | default), emitted
  for stopped VMs too; `kubevirt_vmi_launcher_memory_overhead_bytes`.
- OpenShift docs, `modules/nodes-descheduler-profiles.adoc`: profile list and
  conflicts; scheduler profiles `LowNodeUtilization` / `HighNodeUtilization` /
  `NoScoring`.
- KubeVirt `cpuAllocationRatio` (default 10, pod request = vCPU / ratio when
  the VM sets no request): Red Hat OpenShift Virtualization overcommit lab,
  FOSDEM 2025 KubeVirt scheduling talk.
