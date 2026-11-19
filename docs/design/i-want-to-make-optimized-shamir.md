# create-vm: a worker inside every KV cluster, one placement at a time per zone

The design, in plain language, is `docs/design/create-vm-placement.md` (written 2026-11-17).
This file is the implementation plan for it. **Nothing is implemented until the operator approves.**

## Context

create-vm today runs ONE hub limb that reaches each KubeVirt (KV) cluster through two HTTP
agents behind Routes (`kv-capacity`, `kv-vm-api`), reads the zone's clusters from day1, and
ranks them itself. Two problems: every KV cluster needs two extra services, Routes, fleet tokens
and the repo's only unverified-TLS exception (`KV_AGENT_CA`); and nothing coordinates two runs,
so parallel creates all pick the same cluster (seen in the lab).

Decided with the operator, 2026-11-17 (reversing CLAUDE.md §4's "none to reopen" block):

1. **One `vm-lifecycle-worker` inside each KV cluster**, polling
   `vm-lifecycle-activity-<kv cluster>`, acting on its own API server as its own ServiceAccount,
   dialling out to the hub's Temporal over gRPC/TLS (the per-MCE `server-lifecycle-worker`
   pattern). It reports its cluster's capacity AND creates/reads/deletes VMs. The agents, their
   Routes and tokens, `KV_AGENT_CA`, the vendored agent contract and the hub vm-lifecycle limb go.
2. **server-scan takes no part** in placement or creation (the 2026-11-08 ledger proposal and
   this plan's earlier server-scan phase are dropped). No server-scan change at all.
3. **The zone's clusters come from day1**: the hosted clusters of the site's seed folders
   (`sites/<site>/mces/<seed>/hostedClusters/`), kept when their NodePools' `nodeLabels` name the
   requested `topology.kubernetes.io/zone`, sorted by the cluster name's `id` letter (oldest
   first). Read on the **cluster-lifecycle limb** with `redbull-git-token`, the token
   provision-cluster uses.
3b. **Names follow the platform naming convention**
   (`kubevirt-workflows/docs/review/naming-convention-draft.md`): clusters
   `ocp4-<lifecycle>-<flavor>-<site>-<id>`, InfraEnvs `<flavor>-kv<cpu>-<site>-<zone>`
   (`c200-kv32-five-az1`), VMs `ocp-<flavor>-kv<cpu>-<site>-<full MAC, 12 hex>`
   (`ocp-c200-kv32-five-0e03f2a1b2c3`), VM labels per its §4.b.
4. **Placement is serialized per (site, zone)**: alphabetical scan, each cluster's figures read
   live from its worker, the FIRST that fits wins, and the scan stops. A **pending list** (chosen,
   not yet created) is added to what each cluster reports, because creation is parallel.
5. **The chosen cluster's worker builds the VM.** The hub sends what the VM is (flavor, cpu,
   memory, site, zone, MCE, namespace, the run's id); the worker draws the MAC from its own
   `VM_MAC_PREFIX` (ConfigMap, `0e` + the cluster's VLAN ID, written when the KV cluster is
   created, never on the hub) + 3 random bytes, derives the name, assembles the VirtualMachine
   and creates it. A taken MAC or name is redrawn **by the worker**, 3 draws in total, with no
   round trip to the hub; then the activity fails. Prefixes differ across a site's clusters
   because their VLANs do, so cross-cluster MAC collisions cannot happen.
6. Today there is one KV cluster per zone; nothing in the code assumes it.
7. **Nothing is pushed** in any repo.

---

## 0. Preconditions

- workflows: branch `feat/vm-lifecycle-per-kv-cluster` off `main`; follow `/redbull-workflows`.
- redbull-platform: the local checkout is a dirty `feat/cluster-lifecycle-worker` touching the
  files this plan replaces — work in a separate worktree off fresh `origin/main`.
- Operator: terminate any old-shape run (`WorkflowType='CreateVmWorkflow' AND
  ExecutionStatus='Running'`) — the hub limb is deployed (v1.0.7) but no agent is, so none can succeed.

---

## 1. The workflows

### 1.1 Zone placer — new `workflow_domains/vm_lifecycle/kv_placer.py`

`KvPlacerWorkflow`, one per zone: id `kv-placer-<site>-<zone>` (builder in
`shared/workflow_ids.py`), own queue `KV_PLACER_WORKFLOW_QUEUE = "kv-placer-workflow"`, registered
in `main_worker_init.py`'s `_WORKER_SPECS`. Long-lived; never completes on its own.

- **How requests reach it** (workflow code can only signal or start another workflow — verified
  in the installed SDK: `ExternalWorkflowHandle` has `signal`/`cancel` only; workflow-side
  `signal_with_start_workflow` exists in 1.32 but is marked experimental and the pin is
  `>=1.28`, so it is NOT used): the requester `start_child_workflow(KvPlacerWorkflow.run, …,
  id=…, parent_close_policy=ABANDON)` with its request as the first input; on
  `WorkflowAlreadyStartedError` it signals `request_placement` to the running placer instead.
- **How the answer comes back**: the placer signals the requester's workflow id
  (`placement_decided`); the requester `wait_condition`s on it, bounded (30 min →
  `KvPlacementUnansweredError`, WORKFLOW-RAISED). Later the requester signals the placer
  `placement_settled(request_id, created | failed)`.
- **The loop**: FIFO queue of requests. For each: walk its candidate list (from day1, sorted),
  `read_kv_capacity` on `vm_lifecycle_activity_queue(name)`, add that cluster's pending entries,
  apply the fit rule; first fit → add a pending entry, signal the requester, next request. No fit
  → signal "not placed" with the per-cluster table.
- **Nothing inside the loop may wait forever** (one stuck read would stall the zone):
  `read_kv_capacity` carries `schedule_to_start_timeout=30s` (→ `no-worker`) and
  `schedule_to_close_timeout≈60s` (→ `unreachable`), inside the best-effort `try` the checker
  allows (WF005). A skip row, never a failure of the placer.
- **Pending entries** `{request_id, requester workflow id, cluster, cpu_m, mem_b, added_at}` leave
  on `created`, on `failed`, or 30 min after `added_at` (`workflow.now()`).
- **History bound**: `continue_as_new` every N requests or when suggested, carrying the queue and
  the pending list, after `wait_condition(workflow.all_handlers_finished)`. A signal arriving
  during the CAN task makes the server refuse the completion, so none is lost.
- **Fit policy**: `workflow_domains/vm_lifecycle/placement.py`, pure and sandbox-safe —
  `activities/vm_lifecycle/placement.py` moved, its `KvProbeFailure` import cut, best-fit
  replaced by first-fit, a `pending` term added. `pod_cpu_millicores` / `pod_memory_bytes`,
  the fit-per-node and buffer-per-cluster rules, `SKIP_REASONS` are kept.
  The buffer comes back IN the capacity report (each cluster's worker setting): workflow code
  cannot read settings.
- `progress` query: the queue length, the pending list, the request in hand.

### 1.2 create-vm — `workflow_domains/vm_lifecycle/create_vm.py` (rewritten, no `workflow.patched`)

```
listing-kv-clusters   list_kv_clusters on CLUSTER_LIFECYCLE_ACTIVITY_QUEUE (outside the lock,
                      in parallel across runs); empty → NoKvClusterError
awaiting-placement    ask the zone placer (1.1); not placed → NoKvClusterFitsError (_SKIP_SUMMARY)
creating-vm           kv_queue = vm_lifecycle_activity_queue(chosen) as a LOCAL name;
                      create_vm_on_cluster(VmCreateRequest: flavor, cpu, memory_gib, site, zone,
                      mce, namespace, request_id = the workflow id) (NO schedule_to_start); the
                      WORKER draws, names, redraws (3 draws) and answers name + MAC;
                      VmMacExhaustedError from it fails the run; then signal the placer
                      created | failed
awaiting-root-disk    await_vm_ready, unchanged (heartbeating, 8 s polls, 10 min own deadline)
removing-vm           only on the deadline: delete_vm_on_cluster (name AND MAC), then
                      VmRootDiskNotBoundError
completed             name, MAC, namespace, size, cluster, the placement table
```

- `known_macs`, the MAC-tail check, the run's suffix draw and `VmNameExhaustedError` go: the run
  never sees a MAC until the worker answers with it.
- New WORKFLOW-RAISED `KvPlacementUnansweredError`.
- Queues are LOCAL names assigned from the builders (`tests/test_worker_registration.py::_resolve_queue`
  follows only that — the `add_server_to_mce.py:649` shape).

### 1.3 Contract

- `shared/consts.py`: `vm_lifecycle_activity_queue(kv_cluster)`, `KV_PLACER_WORKFLOW_QUEUE`;
  `VM_LIFECYCLE_ACTIVITY_QUEUE` stays as the prefix.
- `shared/models/vm_lifecycle.py`: keep `CreateVmInput` (`+ flavor`; `cpu`, `memory_gib`, `site`,
  `zone` as today, all parsed at the edge), `KvClusterListRequest/Entry/List` (an entry gains
  `zone` and `id`), `KvCapacityReport` / `KvNodeCapacity` / `KvHaltedReservation` (no longer a
  vendored contract; `+ buffer_percent`), `VmCreated`, `VmReady*`, `VmDelete*`. `VmCreateRequest`
  becomes `flavor`, `cpu`, `memory_gib`, `site`, `zone`, `mce`, `namespace`, `request_id` (no
  name, no `suffix_hex`). New: the placer's run args, `KvPlacementRequest` (requester id,
  request id, size, candidates), `KvPlacementDecision` (chosen or not, the per-cluster table),
  `KvPlacementSettled`, the pending entry. Drop `KvVmCreateRequest/Response`, `KvVmConflict`,
  `KvRootDisk`, `KvVmState`, `KvApiError`, `known_macs`.
- `shared/infraenv_names.py`: the convention's `<flavor>-kv<cpu>-<site>-<zone>` (flavor
  `[a-z][0-9a-z]*` with its optional variant letter, `mtl` refused by create-vm), memory
  `cpu × MEMORY_GIB_PER_CPU` (8) as today; the old `<conf>[-opt]-zone-<zone>-<cpu>` form goes.
  The route also refuses an MCE whose site (`ocp4-<lifecycle>-<flavor>-<site>-<id>`) is not the
  InfraEnv's.
- `shared/interfaces/cluster_lifecycle.py`: `list_kv_clusters` under a "shared with the
  vm-lifecycle domain" header (the `get_inventory_segment` precedent).
  `shared/interfaces/vm_lifecycle.py`: `read_kv_capacity`, `create_vm_on_cluster`,
  `await_vm_ready`, `delete_vm_on_cluster`.
- `shared/exceptions.py`: − `KvAgentAuthError`, `KvCapacityUnavailableError`,
  `KvVmApiUnavailableError`, `VmNameExhaustedError`; + `KvClusterApiError` (transient),
  `KvClusterAccessError` (403, permanent), `VmMacExhaustedError` (the worker's 3 draws all taken,
  permanent), the WORKFLOW-RAISED one above.
- `shared/settings/vm_lifecycle.py`: `KV_CLUSTER` (required, DNS label, NEVER canonicalised — it
  must equal the day1 file stem), `HEADROOM_BUFFER_PERCENT` (0..90), `VM_MAC_PREFIX` (REQUIRED,
  exactly 3 octets, the first `0e`; no default, so a cluster without it does not start),
  `VM_STORAGE_CLASS_NAME`, `VM_NETWORK` (JSON), `VM_NODE_SELECTOR` (JSON), `VM_REDFISH_LABEL`.
  − day1, `KV_*` URL templates, tokens, CA.
- `shared/cluster_names.py`: `seed_cluster`, `kv_cluster_prefix`, `is_kv_cluster_of` go (the
  convention puts no zone in a cluster name); + a parser for `ocp4-<lifecycle>-<flavor>-<site>-<id>`
  (site and `id` letter).

### 1.4 Limbs

- **cluster-lifecycle**: `activities/cluster_lifecycle/kv_clusters.py` (the walk of the site's
  seed folders over the shared `activities/day1_repo.py`, replacing
  `activities/vm_lifecycle/day1_repo.py`: each file's zone from its NodePools' `nodeLabels`
  `topology.kubernetes.io/zone`, none or two zones = a `problems` line and never a candidate,
  sorted by the name's `id` letter);
  a thin `list_kv_clusters` in its `activities.py`; registered in its `worker_init.py`. No new
  config. `tests/test_day1_repo.py` and CLAUDE.md §4 updated (the walk moves limbs).
- **vm-lifecycle** (`activities/vm_lifecycle/`, rewritten, runs inside a KV cluster):
  - `kubevirt_api.py`: in-cluster config, the sync client in `to_thread`, a request timeout on
    every call (the PATTERN of `activities/server_lifecycle/cluster_api.py`, own classification:
    5xx/401/transport → `KvClusterApiError`, 403 → `KvClusterAccessError`, 400/422 →
    `VmRequestRejectedError`; KubeMacPool's "failed to allocate requested mac address" →
    `VmIdentityConflictError` at ANY status, matched first); paged pod lists; bounded error text.
  - `capacity.py` (pure): nodes/pods/VMs/VMIs → `KvCapacityReport` — the port of kv-capacity
    with three corrections: labelled VMs counted in **any namespace**, with **any runStrategy**,
    and **+1 GiB** launcher pad (so a pending entry and the VM it becomes count the same).
  - `vm_spec.py` (pure): `draw_mac(prefix, random_bytes)`, `vm_name(flavor, cpu, site, mac)` =
    `ocp-<flavor>-kv<cpu>-<site>-<mac, 12 hex>`, the labels (`redbull.io/flavor`,
    `redbull.io/size` = `kv<cpu>`, `redbull.io/site`, `topology.kubernetes.io/zone`,
    `redbull.io/role: worker`, `redbull.io/mce`, `redfish-enabled: "true"`,
    `redbull.io/created-by` = the request id), and the VirtualMachine
    `kv-agents/templates/vm.yaml.j2` renders; root-disk facts.
  - `activities.py` (thin): `read_kv_capacity`; `create_vm_on_cluster`: (1) a VM labelled with
    the request id exists → return it (a retried attempt whose answer was lost); else up to 3
    draws of (2) 3 random bytes behind `VM_MAC_PREFIX`, (3) the MAC used by any VM of the
    cluster, any namespace → draw again, (4) create Halted, AlreadyExists (or KubeMacPool's
    "failed to allocate requested mac address") → draw again; 3 draws taken →
    `VmMacExhaustedError`; namespace missing → `VmNamespaceNotFoundError`;
    `await_vm_ready`; `delete_vm_on_cluster` (by name AND MAC, VM then DataVolume).
  - `worker_init.py` on `vm_lifecycle_activity_queue(KV_CLUSTER)`; requirements + kubernetes
    − httpx/PyYAML; Dockerfile without git.
  - Deleted: `kv_capacity_api.py`, `kv_vm_api.py`, `day1_repo.py`, `placement.py` (moved, 1.1).

### 1.5 Chart and CI

- new `deploy/charts/vm-lifecycle-worker/` on the `cluster-upgrade-worker` model: `kvCluster`
  and `config.vmMacPrefix` (render fails without either; the prefix is written when the KV
  cluster is created, from its VLAN, and rendered into the worker's ConfigMap),
  `temporal.{host,namespace,tls}`, `config.headroomBufferPercent`, `vm.*`, no Secret; ClusterRole per call (nodes, pods, namespaces,
  virtualmachineinstances, persistentvolumeclaims: get/list; virtualmachines:
  get/list/create/delete; datavolumes: get/delete); README with config, RBAC, network tables and
  "`kvCluster` = the day1 file name".
- `scripts/sync_platform_charts.sh` + the chart, **hardened** so a destination of the old hub
  shape (no `.image.repository`) is not "owned" (today it would write `null` as repository and
  tag); `tests/test_sync_platform_charts.py`.
- `.github/workflows/build.yml`: the `charts` filter, the render step, `git add`,
  `build-vm-lifecycle-worker`'s `helm-image-path: image`.

### 1.6 Tests

- new `tests/test_kv_placer_workflow.py`: FIFO; ten parallel requests over two 70-core clusters →
  2 + 2 then "no capacity", never past the ceiling; pending added to the reported figures;
  `created` / `failed` / 30-min expiry free the entry; a cluster with no worker is a `no-worker`
  row after 30 s and the next one is tried; the scan stops at the first fit (later clusters not
  read); continue-as-new keeps queue and pending; a request arriving during CAN is not lost.
- rewritten `tests/test_create_vm_workflow.py`: one worker per queue, each omittable; the request
  carries flavor/cpu/memory/site/zone/MCE and no MAC; the worker's `VmMacExhaustedError` fails
  the run; created/failed
  signalled to the placer; rollback; the placer's "not placed" → `NoKvClusterFitsError`; empty
  day1 → `NoKvClusterError`; an unanswered placement → `KvPlacementUnansweredError`.
- `tests/test_create_vm_replay.py` + new `tests/test_kv_placer_replay.py`: the 7 old goldens
  deleted and new ones recorded in the same step (`create`, `no-capacity`, `no-cluster`,
  `no-kv-worker`, `mac-redraw`, `draws-exhausted`, `rollback`; placer: `two-requests`,
  `continue-as-new`).
- `tests/test_vm_lifecycle_placement.py` reshaped (first-fit, pending, `SKIP_REASONS` mirror);
  new `tests/test_vm_capacity.py` (cordoned/NotReady, terminal pods, label in any namespace, any
  runStrategy, VMI not double-counted, the 1 GiB, paging), `tests/test_vm_spec.py` (the name and
  labels per the convention, the MAC behind the prefix); rewritten
  `tests/test_vm_lifecycle_limb.py` (fake kube client: a taken MAC in another namespace redrawn,
  AlreadyExists redrawn, three taken → `VmMacExhaustedError`, a retry returning the VM its
  request id labels without drawing); `tests/test_vm_lifecycle_day1.py` moved to the
  cluster-lifecycle limb (zone from `nodeLabels`, none/two zones, `id` order);
  `tests/test_infraenv_names.py` for the new InfraEnv form.
- edits: `test_worker_registration.py` (`_WORKERS`; create-vm reaches exactly
  `CLUSTER_LIFECYCLE_ACTIVITY_QUEUE` + `vm_lifecycle_activity_queue`; the placer only the
  latter), `test_contract.py`, `test_error_classification.py`, `test_image_changes.py`,
  `conftest.py`, `test_settings.py`.
- deleted: `kv_agents_simulator.py`, `test_create_vm_e2e.py`, `test_kv_agents_contract.py` + its fixture.

### 1.7 Docs (same change)

`docs/site/vm-lifecycle/create-vm.html` (every section, plus the placer), `nav.js`, README
(layout, charts, create-vm, API, config), `.env.example`, CLAUDE.md §1/§2/§4/§5/§8/§9 (the
create-vm block rewritten with the dated reversals; §5 gains the per-zone placer as the
serialization precedent), `.claude/rules/delivery.md` + the skill's synced-charts rows,
`docs/design/create-vm-placement.md` (corrected for anything that settled differently),
`kubevirt-vm-placement.md`, `kv-agents-api.md`, `kv-placement-server-scan.md` → superseded by it
(the latter rejected 2026-11-17), `docs/lab/create-vm/` → stale. Memory, after approval:
`create-vm-architecture-decisions.md`, `kubevirt-placement-read-path.md`,
`kv-placement-server-scan-proposal.md` (rejected), `MEMORY.md`.

**Gate**: `python3 scripts/check_conventions.py` (0 errors); `UPDATE_REPLAY_HISTORIES=1 …` once
for the two replay files; `.venv/bin/python -m pytest -q`; `helm lint` + `helm template` of the new chart.

---

## 2. redbull-platform (fresh worktree, not pushed)

Delete `gitops/services/vm-lifecycle-worker/app.yaml`, `gitops/charts/kv-capacity/`,
`gitops/charts/kv-vm-api/`; LEAVE `gitops/charts/vm-lifecycle-worker/` (CI's hardened sync
replaces it; removing it first breaks the next tag bump). Update its CLAUDE.md/README. No
`services/` entry for the new chart: each KV cluster gets it through day2.

---

## 3. Verification

1. The gate above, all green.
2. **Lab pass** (one real KV cluster, the worker from source, the brain local): a local day1 with
   a seed folder listing the real cluster plus a second name with no worker; ten separate
   create-vm POSTs → the no-worker cluster skipped in 30 s, the real one filled up to its buffer,
   then "no capacity"; each VM carries the MAC its name says; a forced duplicate MAC redrawn;
   record the HTTP status KubeMacPool answers for a duplicate; record one real history as a golden.

---

## 4. Deploy order (the operator's; nothing here is pushed)

1. Terminate old-shape runs (step 0).
2. Push the redbull-platform cleanup (the hub limb is pruned).
3. Push workflows `main` (CI: brain → cluster-lifecycle limb → chart sync → vm-lifecycle image).
4. Per zone: the seed folder in day1. Per KV cluster, at its creation: its day1 file (NodePools
   labelling the zone, its `vlanId`), then through day2 the `vm-lifecycle-worker` chart
   (`kvCluster` = the day1 file name, `vmMacPrefix` = `0e` + its VLAN ID, Temporal host + CA,
   storage class — Immediate binding, RWX Filesystem — network). Existing KV clusters get the
   same prefix written once.

## Open points (also in the design page §11)

- **The naming convention is a draft** and names no flavor for seed and KV clusters: the listing
  assumes a seed folder can be told apart in day1, and that every KV cluster's NodePools label
  their nodes' zone.
- **Memory = cpu × 8 GiB**: the InfraEnv carries vCPUs only.
- **A released VLAN reused by a new KV cluster** inherits the old prefix: safe only once the old
  cluster's VMs, BareMetalHosts and Agents are gone.
- **Simpler variant offered**: hold the zone lock until the VM object exists (seconds), no
  pending list; creates of one zone then run one after another.
- The VMs' namespace (`inventory` vs the facade's `server-scan-vms`); the day1 seed folders don't
  exist yet; making the VM installable belongs to add-server-to-mce's KubeVirt branch.
