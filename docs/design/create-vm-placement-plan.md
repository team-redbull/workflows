# create-vm: a worker inside every KV cluster, one zone at a time (implementation plan)

The design is `docs/design/create-vm-placement.md`, next to this file. **The design is
authoritative.** This file is the implementation plan for it. It was first written as
`i-want-to-make-optimized-shamir.md`, corrected in review (2026-11-19) wherever it still described
the earlier design, then amended (2026-11-20). Both rounds are listed at the end.
**Implemented on branch `feat/vm-lifecycle-per-kv-cluster` on 2026-11-20**; where the code
differs from or adds to this text is listed in the last section, "As implemented".

## Context

create-vm today runs ONE hub limb that reaches each KubeVirt (KV) cluster through two HTTP
agents behind Routes (`kv-capacity`, `kv-vm-api`), reads the zone's clusters from day1, and
ranks them itself. Two problems: every KV cluster needs two extra services, Routes, fleet tokens
and the repo's only unverified-TLS exception (`KV_AGENT_CA`); and nothing coordinates two runs,
so parallel creates all pick the same cluster (seen in the lab).

Decided with the operator (reversing CLAUDE.md §4's "none to reopen" block):

1. **One `vm-lifecycle-worker` inside each KV cluster**, polling
   `vm-lifecycle-activity-<kv cluster>`, acting on its own API server as its own ServiceAccount,
   dialling out to the hub's Temporal over gRPC/TLS (the per-MCE `server-lifecycle-worker`
   pattern). It reports its cluster's figures AND creates/reads/deletes VMs. The agents, their
   Routes and tokens, `KV_AGENT_CA`, the vendored agent contract and the hub vm-lifecycle limb go.
2. **server-scan takes no part** in placement or creation. No server-scan change at all.
3. **The zone's clusters come from day1**: the hosted clusters of the zone's ONE seed folder,
   `sites/<site>/mces/ocp4-seed-<site>-<zone number>/hostedClusters/` (InfraEnv zone `az<N>` = seed
   `ocp4-seed-<site>-<N>`, decided 2026-11-20), each file's NodePools' `nodeLabels` zone checked
   against it, sorted by the cluster name's `id` letter (oldest first). Read on the
   **cluster-lifecycle limb** with `redbull-git-token`, through the shared day1 query library (1.4b)
   on the commit-keyed snapshot (1.4a).
4. **Names follow the platform naming convention**
   (`kubevirt-workflows/docs/review/naming-convention-draft.md`): clusters
   `ocp4-<lifecycle>-<flavor>-<site>-<id>` (`kv` for a KV cluster), a seed
   `ocp4-seed-<site>-<zone number>` (where the draft still differs, `ocp4-<lifecycle>-seed-<site>-<id>`;
   the draft is to be updated), InfraEnvs
   `<flavor>-kv<cpu>-<site>-<zone>` (`c200-kv32-five-az1`), VMs
   `ocp-<flavor>-kv<cpu>-<site>-<full MAC, 12 hex>` (`ocp-c200-kv32-five-0e03f2a1b2c3`), VM labels
   per its §4.b.
5. **One run at a time chooses and creates in a zone.** The run takes the **zone's lock**, reads
   each cluster's figures live from its worker, oldest first, chooses the FIRST that fits, has
   that cluster's worker create the VM, and **releases the lock as soon as the VM object exists**.
   The next run's figures then already count that VM, as a stopped VM. **Nothing is remembered
   between runs: there is no pending list.** Each holder gets the lock for at most 5 minutes, and
   its VM create carries the lease's end: the worker creates nothing after it.
6. **The chosen cluster's worker builds the VM.** The hub sends what the VM is (flavor, cpu,
   memory, site, zone, MCE, namespace, the run's id); the worker draws the MAC from its own
   `VM_MAC_PREFIX` (`0e` + the cluster's VLAN ID, in its ConfigMap, written when the KV cluster is
   created, never on the hub) + 3 random bytes, derives the name, assembles the VirtualMachine
   and creates it. A taken MAC or name is redrawn **by the worker**, 3 draws in total, with no
   round trip to the hub; then the activity fails.
7. **The buffer is ONE value for every KV cluster, 15% by default**: `HEADROOM_BUFFER_PERCENT` on
   the hub `cluster-lifecycle-worker`, handed to the run with the zone's cluster list. Not a
   setting of each cluster.
8. Today there is one KV cluster per zone; nothing in the code assumes it.
9. **Nothing is pushed** in any repo.

---

## 0. Preconditions

- workflows: branch `feat/vm-lifecycle-per-kv-cluster` off `main`; follow `/redbull-workflows`.
- redbull-platform: the local checkout is a dirty `feat/cluster-lifecycle-worker` touching the
  files this plan replaces — work in a separate worktree off fresh `origin/main`.
- Operator: terminate any old-shape run (`WorkflowType='CreateVmWorkflow' AND
  ExecutionStatus='Running'`) — the hub limb is deployed (v1.0.7) but no agent is, so none can succeed.

---

## 1. The workflows

### 1.1 Zone lock — new `workflow_domains/vm_lifecycle/kv_zone_lock.py`

`KvZoneLockWorkflow`, one per (site, zone): id `kv-zone-lock-<site>-<zone>` (builder in
`shared/workflow_ids.py`), own queue `KV_ZONE_LOCK_WORKFLOW_QUEUE = "kv-zone-lock-workflow"`,
registered in `main_worker_init.py`'s `_WORKER_SPECS`. Long-lived; never completes on its own.
**It holds only the line of runs waiting.** It never reads a cluster and never chooses.

- **How a run asks for the lock** (workflow code can only signal or start another workflow;
  workflow-side `signal_with_start_workflow` is experimental and not used): the run
  `start_child_workflow(KvZoneLockWorkflow.run, …, id=…, parent_close_policy=ABANDON)` with its
  request as the first input; on `WorkflowAlreadyStartedError` it signals `request_lock` to the
  running lock workflow instead.
- **How the lock is granted**: arrival order. The lock workflow signals the run at the head of the
  line `lock_granted(lease_id, lease_ends_at)`; the run `wait_condition`s on it (phase
  `awaiting-zone-lock`).
- **A run that is already gone is skipped at once.** A waiting run can be terminated, or give up
  (its 30-minute bound, 1.2). Signalling a closed workflow fails: the lock workflow catches that
  failure (inside the best-effort `try` the checker allows, WF005) and grants the next run in the
  same task, so a dead waiter never costs the zone a lease. A run that gives up also signals
  `withdraw` on its way out, which takes it out of the line.
- **How it is released**: the run signals `release(lease_id)` on every path, success or failure
  (phase `releasing-zone-lock`). The lock passes to the next run.
- **The lease**: each holder has the lock for at most **5 minutes** (a durable timer). When it
  expires, the lock passes to the next run, and a late `release` carrying the old `lease_id` is
  ignored. `lease_ends_at` is `workflow.now()` + 5 minutes at the grant; the holder passes it to
  its VM create (1.2), so a create cannot land after the lock has moved on.
- **History bound**: `continue_as_new` every N grants or when suggested, carrying the line and the
  current lease, after `wait_condition(workflow.all_handlers_finished)`. A signal arriving during
  the continue-as-new task makes the server refuse the completion, so none is lost.
- `progress` query: the line's length, the current holder and its lease expiry.

### 1.2 create-vm — `workflow_domains/vm_lifecycle/create_vm.py` (rewritten, no `workflow.patched`)

```
listing-kv-clusters   list_kv_clusters on CLUSTER_LIFECYCLE_ACTIVITY_QUEUE, outside the lock;
                      answers the zone's clusters (oldest first) and the buffer;
                      empty → NoKvClusterError
awaiting-zone-lock    ask the zone lock (1.1), wait for lock_granted
choosing-kv-cluster   for each cluster, oldest first: read_kv_cluster_figures on
                      vm_lifecycle_activity_queue(name), skip_reason(...); stop at the first fit.
                      None fits → release, then NoKvClusterFitsError with the per-cluster table
creating-vm           create_vm_on_cluster on the chosen cluster's queue (VmCreateRequest: flavor,
                      cpu, memory_gib, site, zone, mce, namespace, request_id = the workflow id,
                      create_before = the lease's end; NO schedule_to_start); the WORKER draws,
                      names, redraws (3 draws) and answers name + MAC; VmMacExhaustedError or
                      VmLeaseExpiredError fails the run after the release
releasing-zone-lock   signal release(lease_id), on every path
awaiting-root-disk    await_vm_ready, unchanged (heartbeating, 8 s polls, 10 min own deadline)
removing-vm           only on the deadline: delete_vm_on_cluster (name AND MAC), then
                      VmRootDiskNotBoundError
completed             name, MAC, namespace, size, cluster, the per-cluster table
```

- **The figures call** (design §6.3.2): `read_kv_cluster_figures(KvFiguresRequest(kv_cluster=name))`,
  `start_to_close_timeout=60 s` (the worker bounds its own reads at 20 s),
  `schedule_to_start_timeout=30 s`. A schedule-to-start timeout is a `no-worker` skip row, not a
  failure; Temporal does not retry it. A cluster that has lost its worker never holds the zone
  past the 5-minute lease.
- **The fit rule** (design §6.4): `skip_reason(figures, vm_cpu, vm_memory, buffer_percent)` in
  `workflow_domains/vm_lifecycle/placement.py`, pure and sandbox-safe: `unreachable`, `no-nodes`,
  `fit-node` (one node's headroom covers the VM on CPU **and** memory on that same node),
  `fit-buffer` (CPU or memory past `1 − buffer`). First fit wins; later clusters are not read.
- **A bounded wait for the lock**: `awaiting-zone-lock` gives up after 30 minutes with the
  WORKFLOW-RAISED `KvZoneLockTimeoutError`, so a run never waits forever. It signals `withdraw`
  first (best effort), so the lock does not grant it later.
- **The create is bounded by the lease.** The create call has no schedule-to-start limit: a
  worker that restarts must not fail the run. So the request carries `create_before`, the lease's
  end, and the worker refuses to create once it has passed (`VmLeaseExpiredError`, permanent),
  checked right before the API create, after the "already created by this run" lookup. The run
  then fails with nothing created, and a cluster can never end up over its buffer because a
  create landed after the lock moved on. A retry that finds the VM it already created still
  returns it, whatever the time.
- `known_macs`, the MAC-tail check, the run's suffix draw and `VmNameExhaustedError` go: the run
  never sees a MAC until the worker answers with it.
- Queues are LOCAL names assigned from the builders (`tests/test_worker_registration.py::_resolve_queue`
  follows only that — the `add_server_to_mce.py:649` shape).

### 1.3 Contract

- `shared/consts.py`: `vm_lifecycle_activity_queue(kv_cluster)`, `KV_ZONE_LOCK_WORKFLOW_QUEUE`;
  `VM_LIFECYCLE_ACTIVITY_QUEUE` stays as the prefix.
- `shared/models/vm_lifecycle.py`:
  - keep `CreateVmInput` (`+ flavor`; `cpu`, `memory_gib`, `site`, `zone` as today, all parsed at
    the edge), `VmCreated`, `VmReady*`, `VmDelete*`;
  - `KvClusterListRequest/Entry/List`: an entry gains `zone` and `id`; the list gains
    `buffer_percent`;
  - `VmCreateRequest` becomes `flavor`, `cpu`, `memory_gib`, `site`, `zone`, `mce`, `namespace`,
    `request_id`, `create_before` (the lease's end; no name, no `suffix_hex`);
  - new: `KvFiguresRequest` (`kv_cluster`), `KvClusterFigures` and `KvNodeHeadroom` (design
    §6.3.1: `readable`, `detail`, allocatable, pods, stopped VMs, `node_headroom` per node; every
    number an integer in millicores and bytes), the zone lock's run args and its signal payloads
    (`lock_granted` with `lease_id` and `lease_ends_at`, `release` with `lease_id`, `withdraw`);
  - drop `KvVmCreateRequest/Response`, `KvVmConflict`, `KvRootDisk`, `KvVmState`, `KvApiError`,
    `KvCapacityReport`, `KvNodeCapacity`, `KvHaltedReservation`, `known_macs`.
- The VM's size in request terms, `pod_cpu_millicores` and `pod_memory_bytes`, in one
  sandbox-safe module that the brain and the worker both import (design §6.3.1).
- `shared/infraenv_names.py`: the convention's `<flavor>-kv<cpu>-<site>-<zone>` (flavor
  `[a-z][0-9a-z]*` with its optional variant letter, `mtl` refused by create-vm), memory
  `cpu × MEMORY_GIB_PER_CPU` (8) as today; the old `<conf>[-opt]-zone-<zone>-<cpu>` form goes.
  The route also refuses an MCE whose site (`ocp4-<lifecycle>-<flavor>-<site>-<id>`) is not the
  InfraEnv's.
- `shared/interfaces/cluster_lifecycle.py`: `list_kv_clusters` under a "shared with the
  vm-lifecycle domain" header (the `get_inventory_segment` precedent).
  `shared/interfaces/vm_lifecycle.py`: `read_kv_cluster_figures`, `create_vm_on_cluster`,
  `await_vm_ready`, `delete_vm_on_cluster`.
- `shared/exceptions.py`: − `KvAgentAuthError`, `KvCapacityUnavailableError`,
  `KvVmApiUnavailableError`, `VmNameExhaustedError`; + `KvClusterApiError` (transient),
  `KvClusterAccessError` (403, permanent), `VmMacExhaustedError` (the worker's 3 draws all taken,
  permanent), `VmLeaseExpiredError` (the create arrived after the lease's end, permanent),
  `KvZoneLockTimeoutError` (WORKFLOW-RAISED).
- `shared/settings/cluster_lifecycle.py`: `+ HEADROOM_BUFFER_PERCENT` (0..90, default 15).
- `shared/settings/vm_lifecycle.py`: `KV_CLUSTER` (required, NEVER canonicalised — it must equal
  the day1 file stem — and parsed at startup as `ocp4-<lifecycle>-kv-<site>-<id>`: the site comes
  from it, so there is no separate `SITE`), `VM_MAC_PREFIX` (REQUIRED, exactly 3 octets, the first
  `0e`; no default, so a cluster without it does not start; the ONE place the prefix is set, so
  there is no `VLAN_ID` to keep in step with it), `VM_STORAGE_CLASS_NAME`, `VM_NETWORK` (JSON),
  `VM_NODE_SELECTOR` (JSON), `VM_REDFISH_LABEL`. − day1, `KV_*` URL templates, tokens, CA,
  `HEADROOM_BUFFER_PERCENT`.
- `shared/cluster_names.py`: `seed_cluster(site, zone)` stays, now `ocp4-seed-<site>-<N>` for zone
  `az<N>`, with its inverse `seed_zone(folder)` (the zone of a seed folder, None for any other
  folder); `kv_cluster_prefix`, `is_kv_cluster_of` go (a KV cluster's name carries no zone); + a
  parser for `ocp4-<lifecycle>-<flavor>-<site>-<id>` (site, `id` letter). Pure, so the API edge,
  the workflow and the day1 library (1.4b) share one definition.

### 1.4 Limbs

- **cluster-lifecycle**: a thin `list_kv_clusters` in its `activities.py`, registered in its
  `worker_init.py`: `Day1.kv_clusters(site, zone)` from the day1 query library (1.4b) plus the
  limb's `HEADROOM_BUFFER_PERCENT`, as `KvClusterList`. The library reads the one seed folder; a
  file whose NodePools' `nodeLabels` zone does not match (or is absent) is a `problems` line and
  never a candidate; sorted by the name's `id` letter. Through the cached snapshot of 1.4a, so runs
  at the same moment cause one clone and a run on an unchanged `main` causes none. No KV-specific
  module: the reading is the library's, the buffer is the limb's setting, and the activity only
  joins them. `activities/vm_lifecycle/day1_repo.py` is deleted; CLAUDE.md §4 updated.
- **vm-lifecycle** (`activities/vm_lifecycle/`, rewritten, runs inside a KV cluster):
  - `kubevirt_api.py`: in-cluster config, the sync client in `to_thread`, a request timeout on
    every call (the PATTERN of `activities/server_lifecycle/cluster_api.py`, own classification:
    5xx/401/transport → `KvClusterApiError`, 403 → `KvClusterAccessError`, 400/422 →
    `VmRequestRejectedError`; KubeMacPool's "failed to allocate requested mac address" →
    `VmIdentityConflictError` at ANY status, matched first); paged pod lists; bounded error text.
  - `cluster_figures.py` (pure, design §6.3.3): nodes, pods, VMs and VMIs → `KvClusterFigures`:
    usable nodes (Ready, not cordoned), pod requests as the scheduler counts them (init and sidecar
    containers, overhead), stopped VMs labelled `redfish-enabled=true` in **any namespace**, with
    **any runStrategy**, counted at their pod's request **+1 GiB**, per-node headroom as CPU and
    memory pairs; quantities parsed with `kubernetes.utils.parse_quantity` into integers.
  - `vm_spec.py` (pure): `draw_mac(prefix, random_bytes)`, `vm_name(flavor, cpu, site, mac)` =
    `ocp-<flavor>-kv<cpu>-<site>-<mac, 12 hex>`, the labels (`redbull.io/flavor`,
    `redbull.io/size` = `<cpu>c`, `redbull.io/site`, `topology.kubernetes.io/zone`,
    `redbull.io/role: worker`, `redbull.io/mce`, `redfish-enabled: "true"`,
    `redbull.io/created-by` = the request id), and the VirtualMachine
    `kv-agents/templates/vm.yaml.j2` renders; root-disk facts.
  - `activities.py` (thin):
    - `read_kv_cluster_figures`: a request naming another cluster than `KV_CLUSTER` answers
      `readable: false` with the reason; an unreadable API answers `readable: false`, never an
      exception;
    - `create_vm_on_cluster`: a request whose `site` is not the site in `KV_CLUSTER` is refused;
      (1) a VM labelled with the request id exists → return it (a retried attempt whose answer
      was lost); else (2) `create_before` passed → `VmLeaseExpiredError`, nothing created; else up
      to 3 draws of (3) 3 random bytes behind `VM_MAC_PREFIX`, (4) the MAC used by any VM of the
      cluster, any namespace → draw again, (5) create Halted, AlreadyExists (or KubeMacPool's
      "failed to allocate requested mac address") → draw again; 3 draws taken →
      `VmMacExhaustedError`; namespace missing → `VmNamespaceNotFoundError`;
    - `await_vm_ready`; `delete_vm_on_cluster` (by name AND MAC, VM then DataVolume).
  - `worker_init.py` on `vm_lifecycle_activity_queue(KV_CLUSTER)`; requirements + kubernetes
    − httpx/PyYAML; Dockerfile without git.
  - Deleted: `kv_capacity_api.py`, `kv_vm_api.py`, `day1_repo.py`, `placement.py` (moved, 1.2).

### 1.4a Git reads: a snapshot cached by commit (every limb that reads a git repo)

Built for create-vm's listing, but as THE way every limb reads any git repo, not as a create-vm
special case. Today every read is a full clone per activity attempt; a run on an unchanged repo
pays the whole clone again.

- **`activities/git_repo.py`** (the one git client, AC008) gains `snapshot(repo_url, token,
  branch, repo, prefix)`, an async context manager yielding a READ-ONLY tree:
  1. `git ls-remote --exit-code --heads <url> refs/heads/<branch>` (the call every clone already
     makes first): the remote head's SHA. Exit 2 is still `ValuesBranchNotFoundError`, any other
     failure still `ValuesRepoGitError`. **The remote is asked every time**: a cache never answers
     for the remote, so an unreachable repo fails exactly as today and is never served stale.
  2. The SHA equals the snapshot cached for (repo URL without token, branch) → that snapshot,
     no clone. Git is content-addressed: the same SHA is the same tree, so the answer is
     identical to a fresh clone's.
  3. Otherwise ONE shallow clone of exactly that SHA into a new directory under the pod's cache
     root, `origin` reset to the URL without the token (as `clone()` does today), then swapped in
     as the cached snapshot.
- **Concurrency**: one `asyncio.Lock` per (repo, branch): thirty runs arriving together cause
  one clone; the others wait for it and read it.
- **Lifetime**: one snapshot per (repo, branch). The one it replaces is deleted only once no
  read still holds it (a reference count), so a read never loses its files mid-way. The cache is
  per pod, in an `emptyDir`, and empty after a restart: the first read clones.
- **Reads only.** A WRITE keeps `fresh_clone`, one per attempt, in a `TemporaryDirectory`:
  retries must start clean, and the "already done" check must share the push's clone (CLAUDE.md
  §5). A read that decides a later write in ANOTHER activity is fine on a snapshot: that write
  re-checks in its own fresh clone, as it does today (the blob compare-and-set).
- **Every read result carries the SHA it read** (`revision`, as `KvClusterList` already does), so
  a run's history says which commit it decided on.
- **`activities/day1_repo.py`** gains `main_snapshot()` beside `fresh_main()`. Migrated to it, the
  reads only:

  | caller | today | then |
  | --- | --- | --- |
  | cluster-lifecycle `list_kv_clusters` (new) | — | `main_snapshot()` |
  | cluster-lifecycle `day1_writer.inspect` | `fresh_main` | `main_snapshot()` |
  | upgrade-gitops `read_rules`, `read_cluster` (`upgrade_gitops/day1_repo.py`) | `fresh_main` | `main_snapshot()` |
  | upgrade-gitops day2 layers (`upgrade_gitops/day2_repo.py`) | `fresh_clone` | `git_repo.snapshot()` |
  | `day1_writer.publish`, the branch push, `day2_sigs`, upgrade-gitops commits, `brain_repo` | `fresh_clone` / `fresh_main` | unchanged: they write |
- **Enforced by the checker, not by memory**: AC008 already sends every git call through
  `git_repo.py`; it gains a rule that a `fresh_clone`/`fresh_main` block must reach
  `commit_and_push` (a write), so a new read can only be written with `snapshot`.
- Each limb chart that reads git gets the `emptyDir` volume for the cache root (cluster-lifecycle,
  upgrade-gitops), with a size limit.

### 1.4b The day1 query library (every workflow that reads day1)

day1 is where every OpenShift cluster is described, so many workflows will ask it questions. They
ask through ONE library, not each its own walk of the folders. Built here for create-vm, written
for all.

**The repo it reads** (inspected 2026-11-20, `gitops-day1/platform-config`): every cluster file is
`sites/<site>/mces/<folder>/hostedClusters/<cluster>.yaml`, rendered with the layers
`sites/configValues.yaml` → `sites/<site>/values.yaml` → `sites/<site>/mces/<folder>/values.yaml`
→ the file. A `<folder>` is an MCE (`ocp4-<env>-mce-<site>-<letter>`, or a legacy name such as
`ocp4-mce-site2`) or a seed (`ocp4-seed-<site>-<zone number>`). There is no env folder.

- **`activities/day1_repo.py`** (already the shared reader, in no domain) grows a `Day1` class:
  one read-only view of day1 at one commit, opened with `async with day1_repo.main_snapshot(...) as
  day1:` (1.4a). Pure over a directory, no settings, no Temporal: tested on a fixture tree.

  ```python
  day1.clusters(site=None, env=None, kind="all" | "hc" | "kv")  # every query is this one, filtered
  day1.kv_clusters(site=None, zone=None)                         # kind="kv"
  day1.hosted_clusters(site=None, env=None, include_kv=False)    # kind="hc", or "all"
  day1.cluster(name)                                             # one cluster, its values merged
  ```

  Each returns `Day1Cluster` records: `name` (the file stem), `site`, `folder`, `kind`, `env`,
  `zone` (KV only), `cluster_name`, `hosted_cluster` and its namespace, `problems`, and `revision`
  (the commit). Values are merged only when a query needs them (`load_cluster_values`, unchanged).
- **The definitions, each in one place:**

  | | rule |
  | --- | --- |
  | `kv` | a hosted cluster under a SEED folder. Never by name: day1 has `ocp4-test-kubevirt-site4` under an MCE, and it is an `hc` |
  | `hc` | a hosted cluster under an MCE folder (KV clusters are hosted clusters too, so `hc` means "not under a seed", stated in the docstring) |
  | `zone` | the seed folder's zone (`seed_zone`, 1.3), checked against the NodePools' `nodeLabels`; a mismatch is a `problems` line |
  | `env` | provision-cluster's existing rule, moved here so it has one home: `day2.env` from the merged values, else the cluster name's first field after `ocp4-`. Never the MCE folder's name: legacy folders carry none |
  | `site` | the `sites/<site>` folder |

- **The activity, for workflows whose own limb has no day1 token**: `list_day1_clusters(
  Day1ClusterQuery) -> Day1ClusterList` on the cluster-lifecycle limb (it holds the token, §2's
  rule), models and signature under a "shared with other domains" header in
  `shared/models/cluster_lifecycle.py` / `shared/interfaces/cluster_lifecycle.py` (the
  `get_inventory_segment` precedent). The query carries the filters (`site`, `env`, `kind`,
  `zone`); the list carries `revision`. A new question is a new filter or method, not a new
  activity. It is not a "generic git activity" (CLAUDE.md §5): no clone crosses it and it writes
  nothing; it answers typed facts about one commit.
- **A limb that already holds the day1 token** and reads inside its own step (upgrade-gitops before
  an edit, provision-cluster's inspect) uses `Day1` directly, no cross-limb call. Their own walks
  (`find_mce_dir`, `find_cluster_file`) move onto `Day1` where they are reads.
- **Enforced**: a checker rule (AC009) refuses day1 path literals (`hostedClusters`, `sites/`,
  `mces/`) outside `activities/day1_repo.py`, so a domain module cannot grow its own walk.
- **Tests**: `tests/test_day1_repo.py` extended on a fixture tree shaped like the real repo: MCE
  and seed folders, a legacy MCE folder, a `kubevirt`-named `hc`, a KV file whose NodePools name
  another zone, `env` from `day2.env` and from the name, `kind` filters, `revision` carried.
- **Skill**: `.claude/skills/redbull-workflows/references/day1-reads.md`, linked from the skill's
  SKILL.md — the procedure when a workflow needs day1 data: (1) find the `Day1` query that answers
  it; (2) none does → add a `Day1` method (pure, a fixture test) and, if a workflow must reach it
  across limbs, a `Day1ClusterQuery` filter; (3) call `list_day1_clusters`, or `Day1` directly
  in a limb that holds the token; (4) never parse a day1 path or YAML in a domain module (AC009);
  (5) carry `revision` in the result. Plus one line in `.claude/rules/activities.md`: "day1 is
  read only through `Day1`".

### 1.5 Chart and CI

- new `deploy/charts/vm-lifecycle-worker/` on the `cluster-upgrade-worker` model. Its ConfigMap
  holds the cluster's information, rendered when the KV cluster is created and never typed in by
  hand: `kvCluster` (= the day1 file name; the site is read from it), `config.vmMacPrefix`
  (`0e` + the VLAN ID the Segments Manager allocated, computed by the creation process), the VM
  template's values (`vm.*`: storage class, network, node selector). Render fails without
  `kvCluster` or the prefix. Also
  `temporal.{host,namespace,tls}`; no Secret; no buffer. ClusterRole per call (nodes, pods,
  namespaces, virtualmachineinstances, persistentvolumeclaims: get/list; virtualmachines:
  get/list/create/delete; datavolumes: get/delete); README with config, RBAC, network tables and
  "`kvCluster` = the day1 file name".
- `cluster-lifecycle-worker` chart: `config.headroomBufferPercent` (default 15) into
  `HEADROOM_BUFFER_PERCENT`; the git snapshot cache's `emptyDir` (1.4a). The same volume in the
  `upgrade-gitops-worker` chart.
- `scripts/sync_platform_charts.sh` + the chart, **hardened** so a destination of the old hub
  shape (no `.image.repository`) is not "owned" (today it would write `null` as repository and
  tag); `tests/test_sync_platform_charts.py`.
- `.github/workflows/build.yml`: the `charts` filter, the render step, `git add`,
  `build-vm-lifecycle-worker`'s `helm-image-path: image`.

### 1.6 Tests

- new `tests/test_kv_zone_lock_workflow.py`: arrival order; one holder at a time; release on
  success and on failure passes the lock on; a lease that expires passes the lock on and a late
  release with the old lease is ignored; a waiter that was terminated is skipped at once (no
  5-minute gap); a `withdraw` takes a waiter out of the line; continue-as-new keeps the line and
  the lease; a request arriving during continue-as-new is not lost.
- rewritten `tests/test_create_vm_workflow.py`: one worker per queue, each omittable; **ten runs
  at once over two 100-core clusters with the 15% buffer → `-a`, `-a`, `-b`, `-b`, then "no
  capacity"** (design §6.5), never past the buffer; the scan stops at the first fit (later clusters
  not read); a cluster with no worker is a `no-worker` row after 30 s and the next one is tried;
  the lock is released on every path; the request carries flavor/cpu/memory/site/zone/MCE and no
  MAC; the worker's `VmMacExhaustedError` fails the run; rollback; empty day1 →
  `NoKvClusterError`; a lock never granted → `KvZoneLockTimeoutError`.
- `tests/test_create_vm_replay.py` + new `tests/test_kv_zone_lock_replay.py`: the 7 old goldens
  deleted and new ones recorded in the same step (`create`, `no-capacity`, `no-cluster`,
  `no-kv-worker`, `mac-redraw`, `draws-exhausted`, `rollback`; lock: `two-runs`, `lease-expiry`,
  `continue-as-new`).
- `tests/test_vm_lifecycle_placement.py` reshaped (first fit oldest first, the same-node CPU and
  memory check, `SKIP_REASONS` mirror); new `tests/test_vm_cluster_figures.py` (cordoned/NotReady,
  terminal pods, init/sidecar/overhead requests, label in any namespace, any runStrategy, VMI not
  double-counted, the 1 GiB, paging, integers not quantity strings), `tests/test_vm_spec.py` (the
  name and labels per the convention, the MAC behind the prefix); rewritten
  `tests/test_vm_lifecycle_limb.py` (fake kube client: a taken MAC in another namespace redrawn,
  AlreadyExists redrawn, three taken → `VmMacExhaustedError`, a retry returning the VM its request
  id labels without drawing, a request for another cluster or site refused, a create after
  `create_before` → `VmLeaseExpiredError` with nothing created, while a retry after it still
  returns the VM it created);
  `tests/test_vm_lifecycle_day1.py` deleted: its cases (the zone's ONE seed folder, a NodePool
  zone that disagrees or is absent, `id` order) move to `tests/test_day1_repo.py` (1.4b), and
  `list_kv_clusters` gets a limb test (the library's list + the buffer); `tests/test_cluster_names.py`
  for `seed_cluster` / `seed_zone` (`az1` ↔ `ocp4-seed-five-1`); `tests/test_infraenv_names.py` for the new
  InfraEnv form; settings tests for `VM_MAC_PREFIX` (3 octets, `0e` first) and the site parsed
  from `KV_CLUSTER`.
- new `tests/test_git_snapshot.py`, on a local bare repo: an unchanged head clones once and
  serves the snapshot after (`ls-remote` still called each time); a new commit clones again and
  replaces it; concurrent reads of one head cause one clone; a replaced snapshot survives while a
  read holds it, then is deleted; a missing branch / unreachable remote raise exactly as
  `fresh_clone` does and serve nothing cached; no file of a snapshot contains the token; the
  result's `revision` is the head read. The migrated readers' tests unchanged in what they
  assert. `tests/test_conventions.py`: AC008's new read-through-`fresh_clone` case.
- edits: `test_worker_registration.py` (`_WORKERS`; create-vm reaches exactly
  `CLUSTER_LIFECYCLE_ACTIVITY_QUEUE` + `vm_lifecycle_activity_queue`; the zone lock reaches no
  activity queue), `test_contract.py`, `test_error_classification.py`, `test_image_changes.py`,
  `conftest.py`, `test_settings.py`.
- deleted: `kv_agents_simulator.py`, `test_create_vm_e2e.py`, `test_kv_agents_contract.py` + its fixture.

### 1.7 Docs (same change)

`docs/site/vm-lifecycle/create-vm.html` (every section, plus the zone lock), `nav.js`, README
(layout, charts, create-vm, API, config), `.env.example`, CLAUDE.md §1/§2/§4/§5/§8/§9 (the
create-vm block rewritten with the dated reversals; §5 gains the per-zone lock as the
serialization precedent), `.claude/rules/delivery.md` + the skill's synced-charts rows,
`kubevirt-vm-placement.md`, `kv-agents-api.md`, `kv-placement-server-scan.md` → superseded by
`create-vm-placement.md` (the latter rejected 2026-11-17), `docs/lab/create-vm/` → stale.

The git snapshot rule (1.4a) is a repo-wide invariant, so it goes in the three committed layers
together, per CLAUDE.md §0, and nowhere else:
- **CLAUDE.md §5**, the existing "Git-subprocess activities" bullet: a WRITER opens one
  `fresh_clone` per attempt; a READER takes `git_repo.snapshot`, which asks the remote for its
  head every time and clones only when the head moved. A few lines inside the existing bullet,
  no new section.
- **`.claude/rules/activities.md`**, the "Git only through `activities/git_repo.py`" line: the same
  rule in one line, with its checker id. It loads whenever an activity is edited, which is
  exactly when a new git read is written.
- **`scripts/check_conventions.py`**: AC008 extended (1.4a).
Not the skill: the skill is the procedure for a change, and this is an invariant every change
must keep.

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
   create-vm POSTs → the no-worker cluster skipped in 30 s, the real one filled up to the 15%
   buffer, then "no capacity"; each VM carries the MAC its name says; a forced duplicate MAC
   redrawn by the worker; record the HTTP status KubeMacPool answers for a duplicate; record one
   real history as a golden.

---

## 4. Deploy order (the operator's; nothing here is pushed)

1. Terminate old-shape runs (step 0).
2. Push the redbull-platform cleanup (the hub limb is pruned).
3. Push workflows `main` (CI: brain → cluster-lifecycle limb → chart sync → vm-lifecycle image).
4. Per zone: the seed folder in day1. Per KV cluster, at its creation: its day1 file (NodePools
   labelling the zone, its `vlanId`), then through day2 the `vm-lifecycle-worker` chart
   (`kvCluster` = the day1 file name, `vmMacPrefix` = `0e` + its VLAN ID,
   Temporal host + CA, storage class — Immediate binding, RWX Filesystem — network). Existing KV
   clusters get the same ConfigMap written once.

---

## Open points (the design's §11)

- **The seed's name**: `ocp4-seed-<site>-<zone number>` (decided 2026-11-20), the number matching
  the InfraEnv's `az<N>`. The naming draft still says `ocp4-<lifecycle>-seed-<site>-<id>` and must
  be updated to match. The `kv` flavor and the zone label on every KV NodePool stand as the draft
  has them.
- **Memory = cpu × 8 GiB**: the InfraEnv carries vCPUs only.
- **A released VLAN reused by a new KV cluster** inherits the old prefix: safe only once the old
  cluster's VMs, BareMetalHosts and Agents are gone.
- **Existing KV clusters** need their ConfigMap written once; until then their worker does not
  start and they are skipped as `no-worker`.
- **A lease that expires before the VM exists** (a worker restart during the create): closed by
  `create_before`. The run fails with nothing created instead of creating after the lock moved
  on; the caller asks again.
- **Halted VMs are on no node**, so the one-node check cannot see them; a buffer can admit VMs
  that then fail to start.
- **A worker that is down slows the whole zone** by 30 seconds per run; under AD28 a standby
  cluster is always being built, so this can be routine.
- **The VMs' namespace** (`inventory` vs the facade's `server-scan-vms`).
- **The day1 seed folders do not exist yet.**
- **Making the VM installable** belongs to add-server-to-mce's KubeVirt branch (AD29).

---

## What was corrected from the team's plan

| Section | The team's plan said | Corrected to (the design) |
|---|---|---|
| Context 4, §1.1 | A `KvPlacerWorkflow` per zone with a **pending list** (chosen, not yet created), signals both ways, a 30-minute expiry | A `KvZoneLockWorkflow` per zone holding only the line of waiting runs; the run holds the lock until its VM object exists; nothing remembered between runs; a 5-minute lease (design §4) |
| Context 4 | "Alphabetical scan" | Oldest first, by the cluster name's `id` letter (design §3) |
| §1.2 phases | `awaiting-placement`, with the placer choosing | `awaiting-zone-lock`, `choosing-kv-cluster` in the run, `releasing-zone-lock` on every path (design §5) |
| §1.1, §1.3, §1.5 | The buffer as **each KV worker's setting**, returned in the capacity report; `config.headroomBufferPercent` in the vm-lifecycle chart | One value on the hub `cluster-lifecycle-worker`, 15% by default, handed to the run with the cluster list (design §6.4); removed from the vm-lifecycle settings and chart |
| §1.3, §1.4 | `read_kv_capacity`, `KvCapacityReport`, `KvNodeCapacity`, `KvHaltedReservation`, `capacity.py` | `read_kv_cluster_figures`, `KvFiguresRequest`, `KvClusterFigures`, `KvNodeHeadroom`, `cluster_figures.py` (design §6.3) |
| §1.2, §1.3 | `KvPlacementRequest`, `KvPlacementDecision`, `KvPlacementSettled`, the pending entry, `KvPlacementUnansweredError` | The lock's run args and signals (`lock_granted`, `release`, with a lease id); `KvZoneLockTimeoutError` |
| §1.4 | The capacity check with a `pending` term | Per-node headroom as CPU and memory pairs, checked on the same node; pod requests as the scheduler counts them (design §6.3) |
| §1.4 | The day1 list read on every run | Remembered for the latest day1 commit only (design §3) |
| §1.5 | The ConfigMap with `kvCluster` and the prefix | Also `site` and `vlanId`; the worker checks the prefix is `0e` + the VLAN ID and refuses a request for another site |
| §1.6 | Ten requests over two 70-core clusters (a 30% buffer) | Two 100-core clusters with the 15% buffer, as the design's example (§6.5) |
| Open points | "Simpler variant offered" | Adopted by the design; open points replaced by the design's §11 |

## Amended after the review (2026-11-20)

| Section | The corrected plan said | Amended to |
|---|---|---|
| §1.1 | The lock signals the head of the line and waits for its release or the lease | A grant to a run that is already closed fails and is caught: the next run is granted in the same task, so a dead waiter never costs the zone a 5-minute lease. A run that gives up signals `withdraw` |
| §1.1, §1.2, §1.3 | The create has no schedule-to-start limit, so it can land after the lease expired and the lock moved on (an open point: one VM over the buffer) | The grant carries `lease_ends_at`; the create carries it as `create_before`, and the worker refuses to create after it (`VmLeaseExpiredError`, nothing created). The open point is closed |
| §1.3, §1.5 | `SITE`, `VLAN_ID` and `VM_MAC_PREFIX` in the worker's settings and ConfigMap, the prefix checked against the VLAN | `VM_MAC_PREFIX` only, the one place the prefix is set; the site is parsed from `KV_CLUSTER` (`ocp4-<lifecycle>-kv-<site>-<id>`, now that the convention reserves `kv`) |
| File | `i-want-to-make-optimized-shamir.md` | `create-vm-placement-plan.md`, next to the design |
| §1.4a (new), §1.5, §1.6, §1.7 | create-vm's listing remembered the day1 list for one commit, as its own cache | One cache for every git READ in the repo: `git_repo.snapshot` asks the remote's head every time and clones only when it moved; writes keep `fresh_clone`. The existing day1 and day2 readers migrate; AC008 enforces it; CLAUDE.md §5 and `.claude/rules/activities.md` state it |
| Context 3, §1.3, §1.4, §1.4b (new), §1.6 | KV clusters found by walking every seed folder of the site, the zone read from each file's NodePools; a create-vm module for the walk | The seed is `ocp4-seed-<site>-<zone number>` (2026-11-20), so a zone's clusters are ONE folder and the NodePools' zone only cross-checks it. Every day1 read goes through one `Day1` query library (`clusters`, `kv_clusters`, `hosted_clusters`, `cluster`; kinds `hc` / `kv`), a `list_day1_clusters` activity on the cluster-lifecycle limb for other domains, AC009 refusing a day1 walk elsewhere, and a skill page for adding a query |

## As implemented (2026-11-20)

Branch `feat/vm-lifecycle-per-kv-cluster`. The text above is the plan as reviewed; this section
lists where the code differs from it or adds to it. The current description of the workflow is
`docs/site/vm-lifecycle/create-vm.html` and CLAUDE.md §4.

### Differs from the plan

| Section | The plan said | The code does |
|---|---|---|
| §1.2, §1.3 | `request_id` = the workflow id | `request_id` = the RUN id. A create-vm id is a slot, reused by the next run once this one closed; keyed on the workflow id, the next run of the slot would find the last run's VM as "its own" and create nothing. The run id is one execution's. The workflow id is on the VM as the annotation `redbull.io/workflow-id`. |
| §1.4 | KubeMacPool's refusal of a duplicate MAC is one of the answers that draws again | Still handled when it comes, and NEVER relied on (the operator, 2026-11-20: the KV clusters will run upstream KubeVirt, with no KubeMacPool). The worker lists every VM of the cluster before it draws and compares MACs itself; that list is the uniqueness check. |
| §1.4a | `snapshot(repo_url, token, branch, repo, prefix)`; "ONE shallow clone of exactly that SHA" | `snapshot(repo_url, token, repo, branch, missing_message, cache_root)`: no `prefix`. It clones `--depth 1 --branch <branch> --single-branch` and keys the snapshot by the commit the clone actually holds (`rev-parse HEAD`): a branch that moved since the `ls-remote` gives the newer head, and the next read finds it already held. |
| §1.4b | `Day1Cluster` records | `ClusterRecord` in `activities/day1_repo.py` (`Day1Cluster` is already cluster-upgrade's model), with `blocking` (problems that take the file out of a listing) apart from warnings, and `confirmed_zone`. On the wire: `Day1ClusterEntry` in a `Day1ClusterList`. |
| §1.4a | Reads take the snapshot | So does `read_rules`, whose validator is one of day1's own scripts: it runs in a `scratch` copy of the snapshot, never in the tree other readers hold. |
| §1.6 | Workflow tests in the time-skipping environment | create-vm's and the lock's move the clock by hand (`tests/manual_time.py`): left alone the test server jumps an idle lock to its run timeout, and skips no time under an outstanding task. Goldens are recorded with the production constants. |

### Added beyond the plan (awaiting the operator's confirmation, CLAUDE.md §4)

- **`flavor` on `CreateVmInput`**, parsed at the edge with the rest of the InfraEnv's name, and
  REQUIRED on the model, against CLAUDE.md §5's "a field added to a history-decoded model
  defaults to None": a run of the earlier shape cannot be replayed by the rewritten workflow
  anyway. Also added: the 45 s lease guard, usable nodes without repelling taints and under
  `VM_NODE_SELECTOR`, the figures read's 120 s bound, `VM_ROOT_DISK_SIZE`,
  `KvClusterMismatchError`, and the brain's `temporalio>=1.32` floor.
- **A cancel rolls back.** A cancelled run waits for its create to settle, deletes the VM that
  carries its run id on the chosen cluster, withdraws from the line or releases the lock, and
  ends CANCELED. Terminate runs no code.
- **A run holds ONE VM.** A create that finds two VMs of its request keeps the oldest and
  deletes the other, before and after its own write.
- **A create attempt ends inside its timeout** (`CREATE_BUDGET_SECONDS`, 55 s of the 90 s
  `start_to_close_timeout`), and the worker refuses a create that reaches it after
  `create_before` (`VmLeaseExpiredError`).
- **The lease bounds the scan.** A run takes at most three leases (`_MAX_LEASES`), then
  `KvZoneLeaseExpiredError`; a cluster with no worker costs one 30 s schedule-to-start.
- **Checker rule AC009**: day1's folder layout is spelled in `activities/day1_repo.py` only.
  Three marked exceptions, none of them day1's tree: `upgrade_gitops/day2_repo.py`,
  `cluster_lifecycle/day2_sigs.py` (day2's tree) and `upgrade_gitops/upgrade_rules.py` (the
  rules' own file).
- **The chart sync** (`scripts/sync_platform_charts.sh`) copies `vm-lifecycle-worker` too, and
  replaces a destination chart of another shape (the hub chart of the earlier design).

### Replay goldens as recorded

`tests/histories/create_vm/`: create, second-cluster, no-capacity, no-cluster, no-kv-worker,
mac-redraw, draws-exhausted, lease-expired-at-the-worker, rollback, waited-for-the-lock,
second-lease, leases-exhausted, lock-timeout, cancel-removes-the-vm, cancel-in-the-line.
`tests/histories/kv_zone_lock/`: two-runs, lease-expiry, dead-waiter-skipped, withdraw,
continue-as-new, continued.
