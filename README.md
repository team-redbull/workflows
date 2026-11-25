# Cluster Orchestrator

An OpenShift cluster lifecycle orchestrator built on Temporal. Four domains today:

- **segment-lifecycle** owns the segments a hosted cluster runs on: **creating**
  them in the team's **Segments Manager**, **allocating** one to a cluster
  and writing its DHCP block into the day1 values repo, and **releasing** it
  back to the pool once the cluster is decommissioned.
- **server-lifecycle** owns putting physical servers into an MCE inventory:
  **add-server-to-mce** takes a healthy, unclaimed server from the **server-scan**
  inventory platform and creates the `BareMetalHost` + BMC `Secret` +
  `NMStateConfig` an InfraEnv needs. It replaces `bmh-generator-operator`, a
  Kopf operator that queried four vendor managers itself.
- **server-provisioning** owns getting a racked server INTO that inventory:
  **provision-dell-server** takes a Dell whose iDRAC has an IP and nothing else,
  and discovers it in OpenManage Enterprise, deploys its template (which sets
  root's password), builds the RAID 1 on the BOSS, sets the PERC drives
  Non-RAID, has the naming service rename it, and completes when server-scan
  lists it — ready for add-server-to-mce. It replaces the DC team's manual OME and
  BIOS work; Cisco, HPE and Intersight get their own workflows later.
- **cluster-upgrade** (in progress) upgrades a HyperShift HostedCluster through
  day1 git and Argo CD, never by patching it: the control plane one minor at a
  time along a verified ladder, then the NodePools one at a time, at a person's
  pace. Before a minor step it gives the admin ack the release asks for (4.19's
  before 4.20) only when no client called the removed APIs in the last 24 hours;
  otherwise it lists the callers and waits for a person. **upgrade-cluster**
  and its control-plane step child are built and tested on a fake fleet; their
  three limbs, and the routes that start a run, come next — until then nothing
  can start one.

**One entry point, one Temporal run.** The Segments Manager used to own creation and
then fire a best-effort HTTP trigger at this service — which put creation outside
Temporal (invisible in the UI, silently skipped whenever that call failed). The
direction is reversed: callers POST the definition to
`POST /workflows/segment-lifecycle/initialize-segment`, and the workflow calls the
Segments Manager itself. The Segments Manager is a dependency, never a trigger.

**A segment has no type until it is allocated.** The type (`HC`, `MCE`,
`INVENTORY`, `PXE`) is allocation state, like the cluster: initialize-segment
creates a typeless segment into one shared Available pool per site, allocate-segment
passes the type as a parameter and the Segments Manager stamps it onto whichever
segment it reserves, and a release clears it again. So there is no per-type
inventory to rebalance, and the `convert-segment` workflow that used to re-type
spare segments between pools has been removed.

> **Firewall rules are gone.** initialize-segment used to open firewall rules
> against every same-site peer via the black-box **next** connectivity service,
> mirror the pending request ids into the Segments Manager UI, poll for a HUMAN
> approval indefinitely, and only then flip the segment `Locked -> Available`.
> Every firewall between segments is open now, so the whole flow — and the
> `Locked` status it existed to clear — has been removed. A segment is born
> Available.

## Layout

```
shared/                           Contract layer (temporalio + pydantic only)
  models/<domain>.py              Typed state across the workflow/activity boundary
  interfaces/<domain>.py          Activity signatures (no bodies)
  bmc_address.py                  Pure naming/address logic BOTH sides need
  infraenv_names.py               The naming convention: an InfraEnv's flavor, VM size,
                                    site and zone; a VM's name
  cluster_names.py                Canonical MCE names; cluster, seed, zone and KV-cluster names
  vm_requests.py                  A VM's size as the scheduler counts it (read on both sides)
  settings/<limb>.py              One settings module per worker (+ temporal.py); never re-exported
  exceptions.py / consts.py / workflow_ids.py / logging_config.py
workflow_domains/                 The brain — one folder per domain, plus main_worker_init.py + api.py
  segment_lifecycle/              initialize_segment.py, allocate_segment.py, router.py
  server_lifecycle/               add_server_to_mce.py (the SHAPE of the run),
                                    bond_selection.py (the pure rule it applies),
                                    router.py
  server_provisioning/            provision_dell_server.py, storage_plan.py and
                                    server_name.py (its pure rules), regions.py
                                    (iDRAC prefix -> region), router.py
  cluster_upgrade/                upgrade_cluster.py, upgrade_control_plane.py (the
                                    per-MCE slot child), upgrade_path.py and gates.py
                                    (its pure rules), dispatch.py, router.py (not
                                    mounted yet)
  vm_lifecycle/                   create_vm.py, kv_zone_lock.py (the per-zone lock it
                                    takes), placement.py (the pure fit rule), router.py
  cluster_lifecycle/              provision_cluster.py, router.py (the body is the
                                    hosted-cluster YAML itself)
  routers/                        What no domain owns: deps, shared models, runs.py
                                    (status), slots.py (the free-slot walk)
activities/<domain>/              The limbs
  activities.py                   The @activity.defn surface ONLY — thin
  <technology>.py                 One module per dependency, plain parameters, no
                                    settings and no Temporal, so each is testable
                                    with no worker: cluster_lifecycle/day1_writer.py
                                    (the cluster file on its branch), git_provider.py
                                    (GitLab merge requests), day2_sigs.py (the
                                    sig repos), shortcuts_api.py (the console
                                    go-link), cluster_values.py (what a file must
                                    look like), dhcp_api.py (the DHCP scope API),
                                    server_scan.py (the inventory API),
                                    cluster_api.py (the Kubernetes API, its
                                    idempotency rule and error classification),
                                    bmh_resources.py (the resource bodies),
                                    idrac.py (Redfish), ome.py (OpenManage),
                                    server_namer.py (the naming service),
                                    cluster_lifecycle/day1_clusters.py (day1's
                                    clusters, a zone's KV clusters), and, for the
                                    worker inside a KV cluster,
                                    vm_lifecycle/kubevirt_api.py (its cluster's
                                    API), cluster_figures.py (the capacity
                                    arithmetic), vm_spec.py (the VirtualMachine it
                                    builds), vm_operations.py (each activity's body)
  worker_init.py                  Registers this domain's activities, polls its queue
docs/                             The static documentation site (its own image)
```

The prod charts are vendored into the Argo CD repo, not this one:

| chart | what it deploys |
| --- | --- |
| `redbull-platform/gitops/charts/workflows-orchestrator/` | the brain — ONE release, shared by every domain |
| `redbull-platform/gitops/charts/segment-lifecycle-worker/` | the `segment-lifecycle-worker` limb |
| `redbull-platform/gitops/charts/server-lifecycle-worker/` | the `server-lifecycle-worker` limb |
| `redbull-platform/gitops/charts/server-provisioning-worker/` | the `server-provisioning-worker` limb — no `gitops/services/` entry yet, so nothing deploys it |
| `redbull-platform/gitops/charts/cluster-lifecycle-worker/` | the `cluster-lifecycle-worker` limb (provision-cluster; create-vm's list of a zone's KV clusters and the placement buffer) — ONE release on the hub; reads the shared `redbull-git-token` (day1 push + its provider's merge-request API, the day2 sig repos) and names the `shortcuts-api-token` Secret |
| `redbull-platform/gitops/charts/cluster-upgrade-worker/`, `.../upgrade-agent-worker/` | cluster-upgrade's per-MCE limbs, copied from `deploy/charts/` here by CI (`sync-upgrade-charts`) on every `main` push that changes them — no `gitops/services/` entry yet |
| `redbull-platform/gitops/charts/vm-lifecycle-worker/` | the `vm-lifecycle-worker` limb (create-vm) — one release INSIDE each KV cluster, no Secret; copied from `deploy/charts/vm-lifecycle-worker/` here by the same CI job. No `gitops/services/` entry: each KV cluster gets it through day2. (Until 2026-11-20 this was a hub chart reaching each cluster through two HTTP agents) |
| `redbull-platform/gitops/charts/upgrade-gitops-worker/` | cluster-upgrade's hub limb (its only copy) — no `gitops/services/` entry yet |

Every image built on `main` is tagged `vX.Y.Z` by the shared build workflow, which writes the
tag into that chart's `values.yaml`; a branch build is tagged `<branch-slug>-<short sha>`.
CI (`.github/workflows/build.yml`) builds an image only when a file it loads changed
(`scripts/image_changes.py`): the brain first, then every other changed image at the same
time, one job each (`.github/image-builds.json` holds every image's build inputs), so one
image's failure stops no other. A change that only decides how CI runs (`build.yml`, the
scripts, their tests) rebuilds nothing, and a change to `.github/image-builds.json`
rebuilds only the images whose row changed. Each `main` run that changes an image
publishes a GitHub Release of this repo, `release-<date>-<short sha>`
(`scripts/release_notes.py`): every image rebuilt since the previous release, with its new
version, the one it replaces, its digest and the commits that changed it, and every image
that was not, at the version it stays at. The first release is a baseline: every image at
its current version. A branch run shows the same page as its run summary.

Worker-file naming convention: the workflow (brain) worker is
`workflow_domains/main_worker_init.py`; each activity domain's worker is
`activities/<domain>/worker_init.py`.

## The workflows

Each has its own workflow queue and its own deterministic id. The three
segment-lifecycle workflows share the one `segment-lifecycle-activity` queue and
therefore the one limb deployment; `add-server-to-mce` runs on
`server-lifecycle-activity` and reaches back to the segment-lifecycle queue for
one activity (below); `provision-cluster` runs on `cluster-lifecycle-activity`
and starts `allocate-segment` as an awaited child; `create-vm` reads its zone's
KV clusters on `cluster-lifecycle-activity`, takes its zone's lock (`kv-zone-lock`,
an internal workflow on its own queue, one run per site and zone) and asks
everything else of the worker inside the chosen KV cluster, on
`vm-lifecycle-activity-<kv cluster>`.

**The server-lifecycle workflows were renamed on 2026-11-01**, so each name says
what it does to what:

| Before | Now |
| --- | --- |
| `install-server` | `add-server-to-mce` |
| `remove-server` | `remove-server-from-mce` |
| `uninstall-server` | `uninstall-server-from-hc` |
| `publish-infraenv-boot` | `publish-infraenv-to-pxe-server` |

The route, ids, queue, workflow type and server-scan lock holder all follow the
new name. An old route answers **410** naming the new one and starts nothing;
send the same body to the new route. Runs started before the rename finish as
they began: their old workflow type on their old queue, their old ids and lock
holder, and, for an uninstall, its old `remove-server-…` child. The result
fields `remove_server` and `remove_server_workflow_id` keep their names.

### `initialize-segment` — create a segment

One step. `create_segment(definition)` — `POST /api/segments` — and the segment is
Available, with no type. The definition is site, VLAN, EPG, CIDR and DHCP flag; a
`type` in the body is rejected (422), since the type is only given at allocation.
The Segments Manager is the validator of record (site configured, CIDR
matches the site prefix, no overlap, VLAN free), so a bad definition fails there,
as a FAILED run on the status endpoint rather than a 4xx on the trigger.

Idempotent: a create rejected because the CIDR already exists is looked up and
accepted when the stored segment matches — which covers both a Temporal retry of
an accepted-but-unacknowledged POST and an operator re-submitting the same
definition. A stored segment that DISAGREES is a non-retryable
`SegmentConflictError`.

Still a Temporal workflow rather than a bare HTTP call, for three reasons: durable
unbounded retries (a Segments Manager outage is out-waited, and the request
survives an orchestrator restart), ONE place owning the dedup id, and a pollable
per-segment status — which is what lets the bulk route start N of them.

### `add-server-to-mce` — put a server into an MCE inventory

`POST /workflows/server-lifecycle/add-server-to-mce` with the **InfraEnv** to fill
and its **MCE cluster**.

#### In plain terms — read this first

Say you ask for a server for InfraEnv `x` on **MCE-A**. The run asks server-scan
to **allocate** it ONE (`POST /servers/available`, server-scan's ADR-0063):
server-scan picks a candidate, **locks it for this run** for 2 h ("being
installed into MCE-A"), live-checks it against its vendor manager while locked
and hands it over — or releases it and tries its next, up to five. Call it
**S1**: no other run can be handed S1 now.

1. **Check S1** (seconds, nothing written): usable name, known BMC vendor, a
   BMC address, no BareMetalHost for it already in MCE-A. Its NICs and MACs are
   not checked again — server-scan did, while it held the lock — but the run
   picks which two form the NMStateConfig's bond. Fails one → S1's lock goes
   back and the run **fails**, saying which.
2. **Create** S1's Secret, BareMetalHost and NMStateConfig in MCE-A.
3. **Wait up to 1 h for an Agent** — the machine phoning home from the
   discovery ISO, proof that the BMC, bond, VLAN and DHCP all worked. The lock
   is renewed for 2 h as the wait starts.
4. **Agent** → extend S1's lock to **24 h** (installed), then read server-scan
   **every 5 min** until it shows S1 `INSTALLED_TO_INVENTORY` or `INSTALLED`,
   **release** the lock and finish — usually 5–15 min. Not seen within the
   24 h, the lock is kept to expire and the run still **completes**.
   **No Agent after 1 h** → delete S1's resources, put S1 **in maintenance** in
   server-scan, **release** its lock, and **fail**, saying where the host's
   state points. No run is given S1 again until someone ends its maintenance.

**One server per run** (the operator, 2026-11-17). Until then a run drew several
candidates and tried the next whenever one was unusable — an hour-deep missing
Agent included. Now a failed run has given its server back, and asking again
starts a new run with a new allocation. A run started before 2026-11-17 still
finishes the old way.

**Why the lock, and what the 24 h hold is.** server-scan learns a machine is
taken only when a *cluster* reports it (its membership jobs, every 5 min where
enabled); until then it reads `AVAILABLE`. The "already has a BareMetalHost?"
check sees only *this* MCE, so without the lock a run for MCE-B could draw S1
minutes after MCE-A installed it. server-scan never allocates a locked machine,
and locks the one it allocates in the same call. The 24 h hold after success writes nothing to any cluster — it only
keeps S1 out of other draws until server-scan can see it in a cluster itself —
and the run waits for exactly that, then releases it (`awaiting-server-scan`,
`releasing-lock`; the operator, 2026-11-09): server-scan hides an
`INSTALLED_TO_INVENTORY` / `INSTALLED` server from every draw on its own.
remove-server-from-mce does not give that lock back (it did from 2026-10-31 until
the operator removed it on 2026-11-17): a lock is its holder's, so a server taken
out of the MCE before server-scan reported it stays locked until this run sees the
report, or the lock expires.

**Who checks what.** server-scan, in the allocation — live, against the vendor
manager, while the server is locked: unclaimed, not in maintenance, BMC
reachable, health tier, the **network category HEALTHY — at least two links
observed UP** (since 2026-09-26; Dell NPAR already reduced to one entry per
physical port), at least two NIC MACs read (`min_nic_macs: 2`), and not locked
by anyone else. The workflow: the name, BMC driver and address, and no
BareMetalHost in this MCE; it no longer re-checks the NICs or the MACs (the
operator, 2026-11-17) — server-scan says the machine *can* be bonded, and the
workflow only picks *which* two NICs, because the bond and the boot MAC are
written from them. The limb refuses a MAC it cannot use (`InvalidMacError`)
before the BareMetalHost or the NMStateConfig is written.

| What happens when… | The run | The lock |
| --- | --- | --- |
| server-scan has nothing to allocate (nothing healthy, all claimed or locked, every candidate failing its live check) | fails at once, `ServerNotAvailableError` with server-scan's reasons | never taken |
| the allocated server fails a check | gives it back and fails, saying which | released at once |
| the Agent appears | installed; waits ≤ 24 h for server-scan to show it installed, then completes | held 24 h, **released** once server-scan shows it (`lock_released: true`) |
| server-scan never shows it in 24 h, is unreadable, or names another MCE | **completes** all the same | kept, expires (`lock_released: false`, `lock_detail` says why) |
| no Agent within 1 h | deletes the server's resources, puts it **in maintenance** in server-scan, then fails (`AgentNeverAppearedError`) | released once in maintenance; **kept** to expire (2 h) if server-scan refuses the maintenance |
| the lock lapsed and another run took the machine | deletes its *own* resources, then fails (`ServerReservedError`) | theirs, untouched |
| cancelled while server-scan is still allocating | ends CANCELED, never learning which server server-scan may still lock for it | that one expires (2 h) |
| the allocation's answer is lost and it is retried | the retry is allocated *another* server — server-scan ties a lock to the request that took it | the first expires (2 h) |
| crash/failure while creating resources | stops, resources **left** on the MCE | expires after 2 h; a re-run of the same named install extends it |
| operator **cancels** before the Agent | **rolls back** (since 2026-11-09): after 2 min for calls in flight, **remove-server-from-mce** takes the server out of the MCE (power off, delete, PXE, verify) — or, nothing written, its PXE mapping is removed here; ends CANCELED, `progress.cancel_rollback` says what was done | released here, after the child (which releases no lock since 2026-11-17); kept to expire if its PXE cleanup failed |
| operator cancels after the Agent | stops the wait for server-scan, ends CANCELED; the server **stays installed** | expires |
| operator **terminates** | stops at once — no rollback, resources left (use Cancel to roll back) | expires |
| no worker in that MCE | waits; `progress` names the queue | — |
| server-scan token not admin | fails at the allocation, which locks (`ServerScanAuthError`) | never taken |

**Precondition.** The 24 h hold relies on server-scan's `nodes-status` `agents`
job, which runs on every MCE add-server-to-mce targets (each has an InfraEnv and an
`AgentServiceConfig`): every 5 min it marks a server with an unbound Agent
`INSTALLED_TO_INVENTORY`, so it is never drawn again. An MCE without that job
would read the machine `AVAILABLE` again after the 24 h (server-scan's ceiling).

**Known gap.** A crash
mid-create leaves the BareMetalHost on the MCE while the lock expires after
2 h; clean up by hand, or re-run the same named install (`server_names` set to
that machine, so the same workflow id). server-scan's allocation refuses a
locked machine even to the run holding the lock, so a named run it refuses
reads the machine's record (`lookup_install_candidate`) and goes on
when the only reason is its OWN workflow id's lock, which it then extends. Any
other refusal of a named server — another run's lock, a cluster's claim,
maintenance, its health — fails saying which.

#### Phase by phase

1. **resolving-vlan** — the MCE's `INVENTORY` segment, read from the Segments
   Manager by **cluster name**. Before the allocation, because no server could
   make a missing allocation usable (and one allocated first would sit locked).
   Runs on the *segment-lifecycle* queue, where that credential already lives.
2. **acquiring-server** — `POST /servers/available`: server-scan **allocates**
   ONE server, locked for this run (holder `add-server-to-mce`, the run's
   workflow id, 2 h) and live-checked while locked (its ADR-0063). The
   pool is the InfraEnv name's **first three `-`-separated words** behind an
   `ocp-` prefix (since 2026-11-19): InfraEnv `c200-mtl-haifa-az1` draws from
   `^ocp-c200-mtl-haifa-`. Always three, whatever the name — a longer name like
   `cisco-m6-bat-yam-64c-512gb` draws from `^ocp-cisco-m6-bat-`, any size; a name
   of fewer words is used whole. The trailing `-` ends the third word, so
   `c200-mtl-haifa` never draws `c200-mtl-haifa2-…`; `max_health: HEALTHY`,
   `min_nic_macs: 2`. Nothing reservable is server-scan's 503
   `NO_AVAILABLE_SERVER` with its reasons → `ServerNotAvailableError`, the run
   fails. Seconds as a rule, ~22 min at server-scan's worst (five live checks):
   the activity heartbeats and allows 30. **Not idempotent**: a retry after a
   lost answer is allocated another server, the first freeing itself at its TTL.
   A run started before 2026-11-17 asks `GET /servers/available` (stored data,
   unlocked, `max_health` since server-scan's ADR-0063) for several candidates.
3. **selecting-server** — can THIS server be installed? The bond members are
   PICKED, not judged: two NICs on two **distinct physical ports**, link-UP
   first, taken from `interfaces[]` and never `nic_macs` — server-scan reduces
   NPAR partitions to one entry per port in the former and leaves the latter
   whole, so indexing MACs can bond two partitions of one wire. server-scan
   allocated the server with two links read UP and its MACs normalized, so the
   run does not refuse on link state or MAC syntax any more (the operator,
   2026-11-17; a run started before still requires `UP` strictly). An unusable
   name, fewer than two ports with a MAC, an unknown `bmc_vendor`, no BMC host
   or a BareMetalHost it already has: its lock goes back and the run **fails**
   with that reason's type, naming the server (a run started before 2026-11-17
   skipped it for its next candidate).

   **reserving-server** — only for a NAMED server the allocation refused that
   the run takes anyway: its own leftover lock (a re-run), or `allow_unhealthy`.
   The lock is then taken here (`POST /servers/{id}/reservation`, server-scan's
   ADR-0035) before anything is written.
4. **creating-secret / -baremetalhost / -nmstateconfig** — all idempotent; an
   existing resource is success **once its BMC address, boot MAC, InfraEnv and
   VLAN match**, and never an overwrite. A resource that differs is a
   `BmhConflictError` for a human, because reporting success would describe an
   installation that is not the one on the cluster. NIC names in the
   NMStateConfig are logical placeholders (`nic1`, `nic2`) bonded 802.3ad with
   the VLAN riding the bond.
5. **awaiting-agent** — a bounded wait, **one hour**, for an **Agent** to
   register for the host. This is the only observation that proves the install
   worked: an Agent exists because the machine booted the discovery ISO and
   reached assisted-service, so the BMC accepted virtual media, the bond formed,
   the VLAN was right and DHCP answered. An hour because bare metal POSTs for
   longer than a VM takes to boot. The Agent is matched by **bond MAC** — BMAC
   names an Agent after the host's inventory UUID and its spec holds no
   reference back to the BareMetalHost. The lock is renewed as the wait begins,
   so it covers the whole hour however long the creates took; if it had lapsed
   and another run took the machine, this run's resources are torn down and the
   run fails instead.
   On success (**holding-server**) the lock is extended to a day — the machine
   is taken, and server-scan keeps listing it `AVAILABLE` until a membership
   job sees it in a cluster. Then (**awaiting-server-scan**) server-scan is read
   every 5 min, and once it shows the machine `INSTALLED_TO_INVENTORY` or
   `INSTALLED` the lock is given back (**releasing-lock**). Not seen within the
   day — or server-scan unreadable, or naming another MCE — the lock is kept to
   expire and the run completes anyway.
6. **rolling-back** — no Agent by the deadline: the NMStateConfig and
   BareMetalHost are removed (the Secret cascades off the host's
   ownerReference), the server is put **in maintenance** in server-scan
   (**setting-maintenance**, `PUT /servers/{id}/maintenance` with a reason
   naming the run, the InfraEnv and where the host's state points; the
   operator, 2026-11-17), then the lock is released (**releasing-server**), and
   the run fails as `AgentNeverAppearedError`. server-scan allocates no server
   in maintenance, so the machine stays out of every draw until someone ends
   its maintenance there; refused (a viewer token), the lock is kept to expire
   instead. A run started before 2026-11-17 releases with no maintenance and
   tries its next candidate from step 3.

   Teardown order is load-bearing — metal3 must be told to detach the host
   *before* the delete, or its finalizer blocks forever trying to deprovision
   through the BMC that just failed to answer.

   This replaced a 10-minute Ironic-registration deadline, which was the wrong
   signal both ways: it passed hosts whose bond or VLAN was wrong (they register
   fine, then never boot), and on an unreachable BMC metal3 reports
   `registering` with operationalStatus OK and no errorType for as long as you
   watch — 14 hours, measured — so only the deadline itself distinguished a dead
   host from a slow one.

Its id is `add-server-to-mce-<mce>_<infraEnv>_<slot>` — the hub, the
**pool** and a slot. The MCE is in the id so each hub finds only its own
runs; the pool itself (`^ocp-<first 3 words of the infraEnv>-`) has no MCE in it, so
two hubs filling InfraEnvs that start with the same three words draw from the same machines, and the install
lock is what keeps them apart. Installs drawing from one pool run in parallel,
one per slot (see
[Several servers at once](#several-servers-at-once--count-or-a-list-of-server_names));
a named machine's is `add-server-to-mce-<mce>_<server>`. The workflow, its queue
and its route are `add-server-to-mce` too; the MCE is followed by `_` (a Kubernetes
name cannot contain one), so a hub's runs all sit under `add-server-to-mce-<mce>_`.
Runs started earlier keep the ids they started with: `install-server-<mce>_…`
before the rename (2026-11-01), and `install-server-<infraEnv>_<slot>` /
`install-server-name-<server>` before 2026-10-25.

**The install lock is the guard across MCEs.** server-scan lists a machine
`AVAILABLE` until a cluster reports it, and the BareMetalHost probe in step 3
only sees the run's *own* MCE — so without a lock, a server installed into
MCE-A is drawn again by a run for MCE-B and both clusters drive one BMC. server-scan
takes the lock as it allocates the server, keyed on the run's holder and workflow
id (the run's renewals extend it, never lose a race to it), it expires on its own
(a dead run costs one TTL, not the machine), and is released only after a teardown proved nothing on the cluster
still points at the machine; a failure anywhere else leaves it to expire,
because half-created resources may still name it. It needs server-scan's
**admin** role. Runs started before it existed replay without it
(`workflow.patched`). The lock is what lets a pool be drawn from in
parallel: the id no longer has to keep one run per pool.

**The inventory VLAN is the MCE's, and is read before anything is drawn.** An
MCE owns one inventory network, allocated in the Segments Manager as
`INVENTORY` and found by its **cluster name** — so the VLAN is a property of the
cluster this run targets and of nothing else. It is resolved as step one,
because no server could make a missing allocation usable, and the lookup runs on the *segment-lifecycle* queue where that token already
lives. There is deliberately no `vlan_id` input.

`bmc_vendor` still decides the Ironic driver — it no longer decides a network:

| server-scan `bmc_vendor` | managed by | Ironic driver |
| --- | --- | --- |
| `HP` | OneView | `redfish-virtualmedia` |
| `DELL` | OpenManage | `idrac-virtualmedia` |
| `INTERSIGHT` | Intersight | `redfish-virtualmedia` |
| `CISCO` | UCS Central | `ipmi` |
| `null` | standalone | *refused* |

> The VLAN was briefly split per BMC protocol class — `INVENTORY_REDFISH` for
> HP/Dell/Intersight and `INVENTORY_IPMI` for a UCS blade, on the reasoning that
> Ironic reaches the two over different networks — which made the segment a
> property of the chosen machine and moved the lookup inside candidate
> selection. Reverted on both sides on 2026-09-27: the team keeps one inventory
> scope per cluster rather than two per MCE to allocate, track and keep in step.

**One worker per MCE, routed by queue.** The brain runs on the hub; the
resources belong on the MCE that owns the InfraEnv, which is a different API
server. `mce_cluster` names both the inventory segment AND the activity queue
(`server-lifecycle-activity-<mce_cluster>`), and a `server-lifecycle-worker`
inside each MCE polls only its own queue, authenticating to its own API server
as its own ServiceAccount.

That is deliberately routing rather than credentials: no cross-cluster
kubeconfig exists anywhere, and because workers dial OUT to Temporal, the hub
never needs inbound access to an MCE's API server. It also makes the VLAN's
cluster and the cluster written to the same string, so they cannot disagree.

A run whose MCE has no worker waits instead of acting; `GET
/workflows/runs/{workflow_id}` reports `activity_queue`, which says which MCE
to go and look at.

**Two ways to pick the server.** `server_names` is optional, and leaving it out
is the normal mode. Either way a run installs ONE server; several servers are
several runs (next sections).

| | `server_names` omitted — *"fill this InfraEnv"* | `server_names` given — *"install this machine"* |
| --- | --- | --- |
| server-scan allocation | `POST /servers/available {"pattern": "^ocp-<first 3 words of infraEnv>-", …}` | `POST /servers/available {"name": "<server>", …}` |
| servers per run | one — server-scan picks it, locks it for the run, live-checks it | one — that one |
| already has a BareMetalHost | its lock **given back**, the run fails | **converged** — that is the point |
| workflow id | `add-server-to-mce-<mce>_<infraEnv>_<slot>`, the first free slot | `add-server-to-mce-<mce>_<server>` |
| use it for | routine capacity: take whatever is free and healthy | re-running a specific host, or repairing one |

Both modes apply every other rule identically: server-scan allocates only a
`HEALTHY` server with at least two NIC MACs and two links read UP, and the run
needs a `bmc_vendor` this orchestrator has a driver for. So naming a server does
not force an unusable one through — it only narrows the pool to one and turns
the already-installed check off.

A failed run names its server and why it could not be installed — the host's
own state for a missing Agent (and that it is now in maintenance) — with the
error type of that one reason.

### Request body — `AddServerToMceRequest`

| field | type | rules | what it is for |
| --- | --- | --- | --- |
| `infra_env` | str | non-empty | The InfraEnv to fill. Labels both resources, **and** its first three `-`-separated words are the server query (`c200-mtl-haifa-az1` → `^ocp-c200-mtl-haifa-`). Must already exist, in `namespace`. |
| `mce_cluster` | str | non-empty | Which MCE. Names the `INVENTORY` segment the VLAN comes from **and** the activity queue the cluster writes go to. |
| `namespace` | str | non-empty | Where the three resources go. **Must be the InfraEnv's own namespace** — BMAC looks for the InfraEnv beside the BareMetalHost. |
| `server_names` | str \| list \| null | default `null`; a list is 1–20 names, each once | Install these machines instead of letting the pool pick (see above): one name, or a list — one run per name. |
| `count` | int \| null | 1–20, default `null` (the pool picks one) | Let the pool pick this many and install them in parallel, one run each, each allocated its own server (see below). Sending it asks for the per-run report. Not with `server_names`. |
| `labels` | map | default `{}` | Extra labels for the BareMetalHost. Cannot override the InfraEnv label — that would split the host from its NMStateConfig. |

`namespace` and `labels` are **not shown in Swagger** (the operator,
2026-11-09): expert knobs. They are accepted, validated and used as above, and
unsent keep their defaults. `candidate_count`, which sized a run's candidate
draw, was **removed** on 2026-11-17 when server-scan began allocating each run
one server: sent, it is a 422 saying so.

There is deliberately **no `vlan_id`**: it belongs to the MCE's segment, and a
supplied one could contradict it. The request body forbids unknown fields, so
sending `vlan_id` is a 422 rather than a silently ignored value. So is the old
`server_name` field, with a message pointing at `server_names`.

The request is not what a run is started with: each run gets an
`AddServerToMceInput` for ONE server — `server_name` set from a name, or null
for a pool draw — and never `count`. That model is decoded from Temporal
history on every replay, so it keeps its shape.

### Several servers at once — `count`, or a list of `server_names`

Every server is its own workflow run, with its own id, status and failure —
never one run for a batch. What the body says decides how many, and the reply
follows what was sent:

| body | runs | reply |
| --- | --- | --- |
| neither | 1, the pool picks | `{workflow_id, run_id}`; sent again while it runs, a second install |
| `"count": N` | N, the pool picks, in parallel | per-run report |
| `"server_names": "ocp-…"` | 1, that machine | `{workflow_id, run_id}`; 409 while it installs |
| `"server_names": ["ocp-…", …]` | 1 per name, in parallel | per-run report (even for a list of one) |

The per-run report is 202 with `started` / `already_running` / `failed` and one
`results` item per run (`workflow_id`, `status`, `run_id`, plus `slot` for a
pool run or `server_name` for a named one). `count` sizes a draw from the pool,
so it is a 422 with `server_names`.

**From the pool: slots.** Every run the pool picks for is on a slot,
`add-server-to-mce-<mce>_<infraEnv>_<slot>`. The route walks the slots from `_1` up,
steps over every slot whose run is still going and starts one in each free slot
— one without `count`, N with `count: N`. So a second install while the first
still runs takes `_2` instead of a 409, and a second batch of 5 while the first
5 install takes `_6`–`_10`. A finished slot is free again (Temporal reuses a
closed run's id). The only 409 left from the pool is all 100 slots running.

**By name: a list of `server_names`.** One run per name, on the same id a single
named install uses (`add-server-to-mce-<mce>_<server>`), so a machine is one run
however it was asked for: a name already installing reports `already_running`
rather than being installed twice. One refused start does not stop the others.

**No two runs take one machine**, however they were asked for — two batches,
or 5 from the pool and then 5 by name. server-scan allocates each run its own
server and locks it in the same call, by compare-and-set on the document
revision, and never allocates a locked one. So no run sizes anything for its
siblings: every pool run's input is the same. A run server-scan has nothing left
for fails with `ServerNotAvailableError` and its reasons (`RESERVED` when its
siblings took the rest) — the pool was smaller than the installs asked for. A
named machine another run took first fails its own run.

**Why one route.** Decided with the operator 2026-10-23. Someone asked for a
second healthy server from one InfraEnv and got the 409. The parallel draw
existed as `install-server/bulk` with `count`, but "bulk" reads as a list of
named servers, and nobody found it. With one route, the body is the only thing
to learn.

- **One workflow per server.** Each has its own id, status and failure, and
  Temporal refuses a second run for the same machine.
- **No 409 from the pool** (revised 2026-10-24). A single install sent while
  another runs used to be refused until the first finished — up to an hour.
  Asking the pool again is asking for one more server, so it takes the next
  slot. The 409 stays for a named machine.
- **`count` next to names is refused, not ignored** (and `candidate_count` is
  refused anywhere since 2026-11-17, saying it was removed).

`install-server/bulk` is gone (404). A request to the pool, with or without
`count`, starts more installs each time it is sent, so it is not retry-safe: a
double-click installs two. That was accepted because its callers are people,
not controllers. The options weighed and the collision cases are in
[`docs/design/add-server-to-mce-api.md`](docs/design/add-server-to-mce-api.md).

### `uninstall-server-from-hc` — a server out of its hosted cluster, left a free Agent, whatever its Agent's state

`POST /workflows/server-lifecycle/uninstall-server-from-hc` with `server_names` (one
name, or a list of 1–20 — one run each, id `uninstall-server-from-hc-<server>`),
`mce_cluster` (with or without `ocp4-`) and `namespace` (default `inventory`).
Nothing else: no `count`, NodePool or HostedCluster field (422) — server-scan
and the Agent's own binding say where the server is. A string is answered
`{workflow_id, run_id}` (409 while that server uninstalls), a list with the
same per-run report as add-server-to-mce's.

The path is picked by the **binding** seen at inspection, never by the Agent's
state string:

- **A hosted cluster's node** — the Agent is bound and an AgentMachine is
  behind it, *or* an AgentMachine's `status.agentRef` still names it. The
  second shape is a Machine deletion already under way: the Agent provider
  clears the Agent's binding *before* the Machine finishes (it holds the
  Machine on its `pre-terminate.delete.hook…/agentmachine` hook until the host
  is unbound), so a re-run must recognise it and resume. The run deletes the
  CAPI Machine (a no-op when it is already deleting or gone), waits for CAPI to
  drain the Node (`draining`, 20 min) and for the provider to release the
  Agent and the host to reboot into discovery (`unbinding`, 1 h, until
  `known-unbound`), then verifies — re-derived fresh, never assumed — that no
  Machine or AgentMachine is linked to the Agent, no Node of the hosted
  cluster is the server, and the Agent is a clean inventory host.
- **What a broken HyperShift deletion left of one** is that shape too, and
  cleared. Each object is normally cleaned up by a finalizer or hook — CAPI
  drains and deletes the Node through the Machine, the Agent provider unbinds
  the Agent only from the Machine's pre-terminate hook — so when the chain is
  broken nothing finishes it. The Machine is found by any link (owner,
  infrastructureRef, providerID `agent://<agent>`) and the Node by providerID,
  nodeRef or hostname, and where the owner cannot act the run makes its write:
  an AgentMachine with no Machine → the provider's unbind of the Agent, the
  AgentMachine deleted (its stuck finalizer dropped); a Machine with no
  AgentMachine → the unbind at its pre-terminate hook, then the provider's hook
  removed; a binding with both gone → the unbind; a Node with neither → cordon
  and evict (20 min, `NodeDrainTimeoutError`) before the unbind, and the Node
  deleted once the host left it and it is NotReady (`releasing` is the
  sub-phase). The result lists them in `leftovers_at_start` and `cleaned_up`.
  Decided with the operator 2026-10-24.
- **An inventory host** — nothing links it to a hosted cluster: its Agent is
  already free, so the run completes at once (a stale Node in the cluster
  server-scan names is deleted first).

**The run ends there: the server stays in the MCE as a free Agent**
(`kept_in_inventory: true`) — its BareMetalHost, NMStateConfig, Agent and PXE
mapping as they are, and no server-scan lock taken at any point (the
operator, 2026-11-19: the server is never `AVAILABLE` in server-scan while it
moves). To take it out of the MCE too, call `remove-server-from-mce`
afterwards. Every skipped
step is named in `skipped`. `keep_inventory` is a 422 (removed 2026-11-02).

Runs started before 2026-11-19 finish the way they began: an install lock
(holder `uninstall-server-from-hc`), a fresh look at an inventory host, the lock
held a day, **remove-server-from-mce as an awaited child**
(`remove-server-from-mce-<server>`), then the lock released (`lock_released`)
unless its PXE cleanup failed — and one started before 2026-11-02 with
`keep_inventory` stays in the inventory.

> **Nothing has to be done first: the run deletes the node's Machine itself.**
> NodePool replicas are never changed here (they are the day1 file's, changed
> only by `update-nodepool`), so the NodePool backfills a new Machine from
> another free Agent it matches — the node is REPLACED, the cluster keeps its
> size. To shrink the cluster instead, use `update-nodepool` — its scale-down
> takes exactly the server you name in `server_names`. If this server is the only
> free Agent, the replacement takes it back: the run stops with
> `AgentRetakenError` and writes nothing more. A Machine someone else already
> started deleting is waited on, not deleted again.

**The run labels no Agent.** From 2026-11-12 it labelled the server's Agents
`redbull.io/uninstalling: "true"` right after its lock (`marking-agent`), so a
NodePool excluding the label would not take the server back; the operator
removed that step on 2026-11-17. Only a run started in between still labels
(`agents_marked` in its result; `oc label agent -n <ns> <agent>
redbull.io/uninstalling-` takes it off).

Before anything is written it **refuses** on a contradiction: a name
server-scan does not know (`ServerNotInServerScanError`); server-scan saying
another MCE, another cluster than the Agent's, `INSTALLED_TO_INVENTORY` while
the Agent is bound, or `contested_with` (`ClusterMembershipMismatchError`);
two bound Agents (`MultipleBoundAgentsError`); an Agent bound into no
HostedCluster's control plane with nothing behind it — an early-bound ACM/ZTP
cluster (`NotHostedClusterNodeError`); two Machines, two Nodes, or links into
two control planes (`ConflictingHostedObjectsError`); a Ready Node nothing on
the MCE links any more (`HostedNodeStillPresentError`). `AVAILABLE` is not a
contradiction. A lock another run holds in server-scan does not stop it: a
new run takes and checks no lock.

After the Machine delete nothing is undone (it cannot be) and a re-run of the
same id resumes: `MachineDrainTimeoutError` names the Machine's deletion
conditions and pending delete hooks; `AgentUnbindTimeoutError` the Agent's
state; `ServerStillPresentError` an AgentMachine or Machine still linked;
`HostedNodeStillPresentError` a Node still Ready after the host left it (a
NotReady one is deleted); `AgentRetakenError` the NodePool's replacement
AgentMachine that took this same server back (free another Agent the NodePool
matches, or lower its replicas, then re-run).

The Node is read through the HostedCluster's admin kubeconfig
(`status.kubeconfig`), with the server replaced by the control plane's
in-cluster Service, `https://kube-apiserver.<hc-ns>-<hc>.svc:6443`, and TLS
verified against the kubeconfig's CA; nothing from it reaches history or a
log. An Agent's `clusterDeploymentName.namespace` is that control-plane
namespace, not the HostedCluster's (HyperShift creates the ClusterDeployment
there), so the HostedCluster is found by HyperShift's namespace rule. This
needs a **ClusterRole** on the limb — Machines and AgentMachines
get/list/delete/patch, HostedClusters get/list, Secrets get cluster-wide — and
`patch` on Agents in the InfraEnv Role; see
[the chart README](deploy/charts/server-lifecycle-worker/README.md).

### `update-nodepool` — how many workers one NodePool of a hosted cluster runs

`POST /workflows/server-lifecycle/update-nodepool` with `mce_cluster`, `cluster`
(the day1 file's name), `nodepool` and `replicas` (that NodePool's own, set
exactly, 0 allowed; the cluster's other NodePools are left alone). One run per
cluster at a time (`update-nodepool-<cluster>`, 409), and none while an
`upgrade-cluster` run of the cluster is running (409): both commit to the same
day1 file. A name the cluster does not have is `NodePoolNotFoundError`.
`infra_env_kind` (a total spread over one InfraEnv kind's NodePools) was
removed on 2026-11-18 — a 422 now; runs recorded with it finish as they began.

**The day1 file owns replicas** (the operator, 2026-11-03), and this is the one
place that changes them:

1. It reads the cluster file and the HostedCluster's NodePool. They must
   agree — same NodePool, InfraEnv and replicas — else
   `NodePoolReplicasDriftError`, nothing written.
2. A child, **scale-up-nodepool** or **scale-down-nodepool**, commits the new
   replicas to day1 `main` (compare-and-set on the file's blob, so a person's
   edit is never committed over), syncs the cluster's Argo CD Application to
   exactly that commit, and reads the NodePool back. Done is the NodePool
   showing the new replicas — not new nodes Ready.

**On a scale-down, which servers leave** (the operator, 2026-11-18), the first
that applies — sending both request fields is a 422:

> `scale_down_policy` and `server_names` are features for **OpenShift clusters
> from version 4.19 on** only (the operator, 2026-11-18). This is documented,
> not enforced: the run does not check the cluster's version.

- `server_names` — exactly those (server-scan names, one or a list), as many as
  the replicas removed, each a Machine of the NodePool: the worker follows each
  Machine to its Agent's BareMetalHost (else hostname, else Node). Refused,
  nothing written (`ScaleDownServersInvalidError`): a wrong count, a server no
  Machine is, or an unnamed CAPI-unhealthy Machine that 4.18's CAPI would take
  in place of a named one;
- `scale_down_policy` — `fewest-pods`, `least-requested` (CPU/memory requested
  out of allocatable) or `least-utilized` (live use, from `metrics.k8s.io`);
- the NodePool's own `redbull.io/scale-down-policy` annotation, written under
  the day1 entry's `annotations:` (a label of that key, set by hand, is read
  too), same values;
- none: nothing is marked and CAPI picks at random.

A scale-up (or no change) **ignores** both fields and lists them in the
result's `ignored` — never a failure. Under every policy the Machines CAPI
counts unhealthy go first, by name — CAPI's own order, which is what makes the
mark decide in 4.18's CAPI as in later ones — then a broken or cordoned Node.
A NodePool mid-rollout (Machines in two MachineSets) is refused. The run marks
the chosen Machines `cluster.x-k8s.io/delete-machine` **before it pushes**, so
CAPI removes exactly those, then watches that they are the ones deleting
(`ScaleDownWrongMachineError` otherwise). A refused commit takes the marks off
again. The servers that leave stay in the MCE's inventory as free Agents.

**Everything runs on the MCE's `server-lifecycle-worker`** (the operator,
2026-11-08) — no cluster-upgrade worker needs to run. It reads and commits day1
with its own **optional** `DAY1_REPO_URL` / `DAY1_GIT_TOKEN` (unset: every other
workflow is unaffected, update-nodepool fails with `Day1NotConfiguredError`), and
reads NodePools and syncs Argo CD with cluster-upgrade's own two activities,
registered on it too. Its chart grants NodePools get/list and Applications
get/patch in `argo.namespace`, beside the `rbac.hostedClusters` grant the
Machines and the hosted cluster already had. See
[the page](docs/site/server-lifecycle/update-nodepool.html).

### `publish-infraenv-to-pxe-server` — an InfraEnv's current iPXE script onto its PXE VM

`POST /workflows/server-lifecycle/publish-infraenv-to-pxe-server` with
`{"mce_cluster": "mce-bat-yam-01", "infra_env": "cisco-m6-bat-yam-64c-512gb"}`
(`namespace` defaults to `inventory`; the MCE is accepted with or without
`ocp4-`). It writes the InfraEnv's **boot entry**,
`/var/www/pxe-map/<mce_cluster>/<infra_env>.ipxe`, on the PXE VM of the MCE's
inventory-segment site — the file every IPMI host add-server-to-mce mapped to that
InfraEnv chains to. Use it to seed the entry before the first install, to repair
one lost on the VM, or to point it at a regenerated image without installing
anything; add-server-to-mce does the same publish itself, per host.

Phases: `resolving-site` (Segments Manager) → `validating-infraenv` (missing
fails the run) → `publishing`, which waits — bounded, 10 min, else
`InfraEnvImageNotUpdatedError` — for the InfraEnv to show a ready image
(`ImageCreated` True, an `ipxeScript`, a `createdTime`) and writes it with that
`createdTime` as its version. A VM already holding a newer image keeps it and
the run completes with `superseded: true`. Nothing is written on the MCE; the
URL (it carries an `api_key`) never enters workflow history. Id:
`publish-infraenv-to-pxe-server-<mce>-<infra_env>`, so a second trigger for the same
entry while one runs is a 409.

### `provision-dell-server` — from an iDRAC IP to a server add-server-to-mce can use

`POST /workflows/server-provisioning/provision-dell-server` with `{"idrac_ip": "134.1.2.1"}`
(or `/bulk` with `{"servers": [{"idrac_ip": ...}, ...]}` — one run per machine).
The **region** is resolved from the IP's prefix (`workflow_domains/server_provisioning/regions.py`)
before the run starts; an address under no prefix is a 422 (a `rejected` item in
bulk). An explicit `region` wins. The only manual step left is the iDRAC IP.

1. **probing-idrac** — which root password does the iDRAC accept: the target
   (`IDRAC_ROOT_PASSWORD`) first, then each of `IDRAC_FACTORY_PASSWORDS`. One
   request each, so a round can trip iDRAC9's 3-failure IP block only when every
   candidate is wrong — and the next round then waits 11 min for the block to
   lift. Three rounds, then `IdracCredentialsRejectedError`. An iDRAC that
   answers nothing gets 30 min (`IdracUnreachableError`).
2. **reading-identity**, **checking-server-scan** — service tag, model, iDRAC
   firmware; must be a Dell PowerEdge; and server-scan must not list that service
   tag as claimed by a cluster (`ServerAlreadyInstalledError`) — everything after
   this reboots the machine, and a mistyped IP must not take a node down.
3. **discovering-in-ome** — skipped when OME already has the service tag.
4. **deploying-template** — `DELL_TEMPLATES[model][iDRAC firmware]`. The template
   creates the profile and **enforces root's password**; it must touch root
   (user 2) only, never the OME account on the iDRAC. A device already carrying a
   profile from another template is `ProfileConflictError`, never overwritten.
5. **verifying-root-password** — root must accept the target password now; if the
   machine came in on a factory one, OME is re-discovered with the new one.
6. **configuring-storage / applying-storage / verifying-storage** — RAID 1 over
   the BOSS's two drives, every PERC drive Non-RAID, staged together and applied
   by ONE reboot, then read back. It **never deletes**: an existing volume or a
   drive in use is `StorageLayoutUnsupportedError`.
7. **naming-server / verifying-name** — the naming service renames the OME
   profile; the name is read back and must be
   `ocp-dell-<model>-<region>-<N>c-<N>gb-<N>tb-<service tag>` with THIS region
   and service tag. `server_namer.py` is where the service's real request
   contract goes — today it POSTs the request model to `SERVER_NAMER_URL`.

The run ends there: the machine is configured. It does **not** wait for
server-scan to list it — server-scan's own Dell collector (every 6 h) picks the
server up afterwards, and waiting would add hours to every run to watch
something the run cannot influence.

Workflow id `provision-dell-server-<idrac ip>`. Every wait is the workflow's, on
durable timers with a deadline; no password is ever in Temporal history (a run
names a credential by its position in the limb's list).

Every OME and iDRAC request was checked against Dell's own client code (the
`dellemc.openmanage` collection); the facts, their sources and what is still
unverified on real hardware are in `docs/design/dell-ome-idrac-api.md`.
`tests/test_provision_dell_server_e2e.py` runs the real workflow and activities
against a Dell simulator built from them (`tests/dell_simulator.py`).

> **The templates in `DELL_TEMPLATES` must not carry storage components.** A
> template captured from a reference server can include `RAIDresetConfig=True`
> (Dell's own sample does), which wipes the controller when it is deployed —
> and the storage layout is this workflow's job, done after the template.

### `create-vm` — one VirtualMachine for a KubeVirt InfraEnv

Under the platform naming convention an InfraEnv `<flavor>-kv<cpu>-<zone>`
(`c200-kv32-az1`) holds VMs of `<cpu>` vCPUs (an even number) and `<cpu> × 7.5` GiB on one of the
zone's KV clusters (the hosted clusters whose nodes run the warm-pool VMs); a
`<flavor>-mtl-<zone>` InfraEnv holds physical servers. A KubeVirt InfraEnv has
nothing to install until a VM exists, and `create-vm` is that step. Full page:
`docs/site/vm-lifecycle/create-vm.html`; the design: `docs/design/create-vm-placement.md`.

**The zone's clusters come from day1, their numbers from the clusters themselves, only
one run at a time chooses in a zone, and the chosen cluster builds the VM.**

Input: `mce_cluster` (named `ocp4-<lifecycle>-mce-<site>-<id>`: the **site** and the
**lifecycle** come from it), `infra_env` (the **flavor**, the **size** and the **zone** come from it),
`namespace` (default `inventory` — the VM's namespace on the KV cluster, verbatim, a
kubevirt-redfish chassis namespace the platform created) and an optional `count`. The
API derives flavor, site, lifecycle, zone, cpu and memory once; the run carries them.

1. **listing-kv-clusters** — the zone's KV clusters, oldest first, from the day1 folders
   of the zone's seeds OF THE MCE's LIFECYCLE, `sites/<site>/mces/ocp4-<lifecycle>-seed-<site>-<zone>-<id>/hostedClusters/`,
   with the placement buffer — read at `main` by the **cluster-lifecycle worker** (it
   holds the day1 token) on the snapshot cached by commit. A file whose NodePools do not
   all label the zone, whose name is off the convention or names another zone or lifecycle
   than its seed's, is listed and never used.
   None usable: `NoKvClusterError`.
2. **awaiting-zone-lock** — the run asks the zone's lock (`kv-zone-lock`, one long-lived
   workflow per (site, zone), no route) and waits for its grant, at most 30 min
   (`KvZoneLockTimeoutError`). One holder at a time, in arrival order, a 5-minute lease.
3. **choosing-kv-cluster** — holding the lock: each cluster's figures from the
   `vm-lifecycle-worker` INSIDE it, on that cluster's own queue, one at a time, oldest
   first; the fit rule (`workflow_domains/vm_lifecycle/placement.py`: one node must hold
   the VM on CPU and memory; requested + the VM must stay under `1 − buffer` of the
   cluster, stopped VMs counted). **The first that fits is chosen; the later ones are not
   read.** No worker on a queue within 30 s is a `no-worker` row, never a failure.
   Nothing fits: `NoKvClusterFitsError` with the table summarised per reason.
4. **creating-vm** — what the VM is (flavor, vCPUs, memory, site, zone, MCE, namespace,
   the run's id, the create's deadline: 90 s, or the lease's end if sooner) goes to the
   chosen cluster's worker, which draws an 8-character serial, names the VM
   `ocp-<flavor>-kv<cpu>-<site>-<serial>` (the serial also its SMBIOS serial), builds the
   VirtualMachine with NO MAC and creates it Halted, labelled `redfish-enabled: "true"`;
   KubeMacPool assigns the MAC, read back for the BareMetalHost. A taken name is drawn again
   by the worker (3 draws); nothing is created after the
   deadline. **A cluster that fails the create is left, not the run** (`checking-kv-cluster`):
   the run waits 45 s, asks the cluster for its VM by the run-id label (it keeps one it
   finds — the create had landed), and otherwise moves on to the next cluster; a cluster
   that cannot even be asked gets a `delete-stray-vm` child, which removes any VM of the run
   there once the cluster answers. Only when every cluster fails or does not fit does the
   run fail (`NoKvClusterFitsError`).
5. **releasing-zone-lock** — as soon as the VM object exists, and on every other path.
   The next run's figures already count the VM, as a stopped VM: nothing is remembered
   between runs.
6. **awaiting-root-disk** — one heartbeating activity, every 8 s, until the root disk is
   bound and the VM admitted; bounded, 10 min; after the lock, in parallel with other runs.
7. **removing-vm** — only when the run will not complete: that deadline
   (`VmRootDiskNotBoundError`), a wait that fails for good, or a cancel. The run deletes
   ITS OWN VM, found by the run-id label it was created with. A re-run draws a new serial
   and gets a new MAC, so an orphan would only multiply.

The result carries the VM's name and MAC, the chosen cluster and the per-cluster table.
Every run is a **slot** of its InfraEnv on its MCE (`create-vm-<mce>_<infraEnv>_<slot>`):
asking again starts one more VM, never a 409; `count` fills N free slots with a per-run
report. Parallel runs never overbook a cluster: ten 32-vCPU VMs asked at once over two
100-core clusters with the 15% buffer land two on each, and six runs fail saying why.

A `vm-lifecycle-worker` runs **inside each KV cluster** (chart
`deploy/charts/vm-lifecycle-worker/`), polls `vm-lifecycle-activity-<its day1 file name>`,
acts as its own ServiceAccount and dials out to the hub's Temporal: the KV cluster
exposes nothing, the chart holds no Secret, and nothing on the hub holds a credential for
it. A VM's MAC is assigned by KubeMacPool, unique on its cluster (the operator, 2026-11-25);
the HyperConverged object will give each cluster its own range so that clusters do not
overlap. A cluster in day1 without a running worker is skipped.
This design replaced the hub limb and its two HTTP agents (`kv-capacity`, `kv-vm-api`)
on 2026-11-20; `docs/design/kubevirt-vm-placement.md` and `kv-agents-api.md` are superseded.

### `provision-cluster` — a hosted cluster from the YAML a person wrote

The body **is the hosted-cluster file**, as a person would commit it to day1 —
without `vlanId`, `dhcp_values`, `mcConfigMap` or `tuningConfigmap`; the cluster and its MCE folder
are query parameters (Swagger's "Try it out" shows this file pre-filled):

```
POST /workflows/cluster-lifecycle/provision-cluster?cluster=prep-e2e-site2-a&mce=ocp4-prep-mce-site2-a
Content-Type: application/yaml

owner:
  merkaz: "unit-a"
  anaf: "unit-b"
  mador: "unit-c"

mastertag: 4.22.8

nodepool:
- infraEnvName: dell-r660-site4-128c-1024gb-5tb
  replicas: 2
  labels:
    allowDeletion: false
  configs:
  - name: nm-conf
  - name: worker-kubeletconfig-reserved
  - name: nic-ring-buffer
  - name: workers-chrony-configuration
```

The run reads day1 `main` for the MCE folder (found by name under
`sites/*/mces/`, never derived), refuses a cluster that already has a file
there, and judges the text against the merged value layers the way the chart
renders them — every reason in one failure. Then **allocate-segment runs as an
awaited child** (a /24 at the folder's site, verified), the text plus the
allocation block is pushed to `provision-cluster/<cluster>` (created from main
by the push, **no `[skip ci]`**: the day1 CI chain runs on the branch, skips
allocation, renders the MachineConfigs and pushes its own `mcConfigMap` commit
on top, with `tuningConfigmap` — the tuning profiles the nodepools'
`tuningConfig` names, from day1's `tuning/` catalogue), the merge request is opened (GitLab's API, with
`remove_source_branch`), and the run **waits
for a person to merge it** — no deadline, a 5-minute durable timer, continued
as new every 48 polls so history stays short. After the merge it registers the
cluster in every day2 sig the value layers list under `day2.copy_to_sigs`
(`sites/<site>/<env>/mces/<mce>/<cluster>/.gitkeep` on the sig repo's default
branch), and last creates the cluster's **console shortcut** in the shortcuts
service — the cluster name without `ocp4-` → the console URL, owned by
`SHORTCUT_OWNER` (already ours = success; held by another owner =
`ShortcutTakenError`; any other refusal is retried until fixed, since the run
is past its merge) — and completes. A merge request closed without merging
fails the run.

Re-running the same cluster converges: the branch's file is compared
semantically (the chain's `mcConfigMap` and `tuningConfigmap` set aside), the open merge request is
found, folders already there are success. A cluster already on `main` fails
`ClusterAlreadyProvisionedError` at once; a second request while a run waits
for its merge is a 409. See `docs/design/provision-cluster.md` and the
[docs page](docs/site/cluster-lifecycle/provision-cluster.html).

### `allocate-segment` — give a cluster a segment

Takes the cluster, the **site** to allocate at (the day1 folder the cluster's
file goes in, as the Segments Manager spells it) and the **type** to allocate
as (default `HC`):

```json
{"cluster": "ocp4-prep-herzi-site1-a", "site": "site1"}
```

Cross-check the site against `GET /api/sites`, allocate a segment from the
Segments Manager — any Available segment at the site, which becomes the
requested type in the same step — and **read it back** to verify
status/cluster/vlan/type all match. The run ends there: nothing is written to
git. HC only for now; any other type is rejected up front.

It is normally started by **provision-cluster as an awaited child**, which
records the allocation in the cluster file it publishes; the route stays for a
manual allocation on the same id, so the two dedup. Until 2026-11-11 the run
pushed the `vlanId` + `dhcp_values` block to a day1 branch the caller named
(`values_branch`, the day1 pipeline's own) — that field is now a 422 saying so.
There is no DHCP scope wait: Argo CD reads `main` only, so the scope appears
after a human merges the cluster file, outside the run (see CLAUDE.md §4).

### `release-segment` — give a decommissioned cluster's segment back

Takes the cluster and the **type** to release (default `HC`):

```json
{"cluster": "ocp4-prep-herzi-site1-a"}
```

Find the segment the Segments Manager holds for that (cluster, type) — none is a
**completed no-op** (`released: false`), so a repeated trigger stays green. Then
look up the cluster's DHCP scope: normally it is already gone, because deleting
the cluster's values file made Argo CD delete the Application, whose finalizer
deleted the Crossplane `Request`, whose deletion made provider-http delete the
scope. If it survived, the run deletes it (the orchestrator's one write to the
DHCP API, and only ever a DELETE) and reads back its absence — **before** the
segment returns to the pool, so a re-allocated segment never collides with live
leases. Then it re-reads the segment's owner, releases it, and reads it back as
`Available` with no cluster and no type. There is no git step: deleting the file
was the decommission.

It is started by an Argo CD **PostDelete hook** in the hostedcluster-setup chart,
which runs only after every resource of the cluster's Application is gone. A
deprovision-cluster workflow will replace the hook later (see CLAUDE.md §4).

## API

The trigger is async throughout: POST returns **202 + workflow id** immediately;
poll `GET /workflows/runs/{workflow_id}` for phase (workflow query) and the final
result.

| route | starts |
| --- | --- |
| `POST /workflows/segment-lifecycle/initialize-segment` | one segment |
| `POST /workflows/segment-lifecycle/initialize-segment/bulk` | one workflow PER segment |
| `POST /workflows/segment-lifecycle/allocate-segment` | one allocation (manual; provision-cluster starts it as a child) |
| `POST /workflows/segment-lifecycle/release-segment` | one release (after the cluster's Application is gone) |
| `POST /workflows/server-lifecycle/add-server-to-mce` | one workflow PER server into an InfraEnv: one from its pool, `count` from its pool in parallel, or the machines in `server_names` |
| `POST /workflows/server-lifecycle/remove-server-from-mce` | one server out of an MCE's inventory |
| `POST /workflows/server-lifecycle/uninstall-server-from-hc` | one workflow PER server in `server_names`: out of its hosted cluster (if a node), ending once its Agent is free — it stays in the MCE (remove-server-from-mce takes it out) |
| `POST /workflows/server-lifecycle/publish-infraenv-to-pxe-server` | an InfraEnv's current iPXE script onto its site's PXE VM |
| `POST /workflows/server-lifecycle/update-nodepool` | one NodePool's replicas, by name (a scale-up or scale-down child); a scale-down may name its `server_names` or a `scale_down_policy` (OpenShift 4.19 and later only) |
| `POST /workflows/server-provisioning/provision-dell-server` | one Dell server, by iDRAC IP |
| `POST /workflows/server-provisioning/provision-dell-server/bulk` | one workflow PER iDRAC IP |
| `POST /workflows/vm-lifecycle/create-vm` | one workflow PER VM for a KubeVirt InfraEnv (`<flavor>-kv<cpu>-<zone>`; the site is the MCE's): one in the first free slot, or `count` of them; runs of one zone choose and create one after another |
| `POST /workflows/cluster-lifecycle/provision-cluster?cluster=&mce=` | one hosted cluster from the YAML in the body (`application/yaml`): segment, day1 branch, merge request, the wait for the merge, day2, the console shortcut |
| `GET  /workflows/runs/{workflow_id}` | (status, every domain) |

### Paths: `/workflows/<domain>/<workflow>`, status on `/workflows/runs`

A domain holds MANY workflows, so it is never itself an endpoint — each workflow
owns its own path under the `segment-lifecycle` prefix, and each new one is just
another route. Status is deliberately NOT under the domain: Temporal workflow ids
are globally unique, so one `GET /workflows/runs/{id}` serves every domain — and a
`{workflow_id}` catch-all under the domain prefix would swallow every sibling
workflow's path.

The bulk route takes `{"segments": [...]}` and starts **one workflow per segment**
— each gets its own deterministic id, its own run status and its own failure, so a
bad row can neither delay nor fail the others. It always answers 202 with a
per-item report (`started` / `already_running` / `failed`); a single status code
would hide which segments actually got a workflow.

## Design notes

- **Deployment-agnostic:** endpoints come from env (`TEMPORAL_HOST`,
  `SEGMENTS_MANAGER_URL`, `DAY1_REPO_URL`, `DHCP_API_URL`). The same images run on
  kind or OpenShift; only the chart's `values.yaml` (`config.*`) changes.
  `host.docker.internal` appears only there, never in code.
- **ConfigMap split by scope:** `workflows-orchestrator-config` (owned by the
  always-present brain release) holds what every domain shares — `TEMPORAL_*` and
  `SEGMENTS_MANAGER_URL`. Each domain adds its own `<domain>-config` (here
  `segment-lifecycle-config`: `DAY1_REPO_URL`, the DHCP policy and `DHCP_API_URL`;
  `server-lifecycle-config`: `MCE_CLUSTER` and `SERVER_SCAN_URL`). A domain's
  activity worker mounts both, so the brain must install before any limb.
- **ConfigMaps hold operator-editable data**, expanded and validated in code at
  worker startup rather than baked into an image. `DHCP_EXCLUSION_OCTET_RANGES` is
  the one structured knob left: a type → last-octet-ranges map, so changing the
  DHCP policy is a values edit plus a restart, no rebuild.
- **Pydantic data converter** is registered on every `Client.connect` (workers + api).
- **httpx timeout (60s) < activity `start_to_close_timeout` (90s)** so a network
  hang frees the worker before Temporal reaps the activity. Each
  `httpx.AsyncClient` is per-invocation (`async with`), so credentials never leak
  across concurrent runs.
- **Retries are unbounded** so transient outages are out-waited; only CLASSIFIED
  deterministic errors fail a run. An unclassified permanent error retries every
  minute forever, leaving the run RUNNING rather than FAILED — which is why every
  known-permanent error is named in a `non_retryable_error_types` list. That list
  is built from the exception CLASSES, not written out as strings: Temporal
  matches it by type name, so a literal with a typo in it reads as "retryable"
  with nothing to notice, while a wrong class name fails at worker startup.
- **Idempotency:** `create_segment` accepts a matching existing segment;
  `allocate_segment` is idempotent server-side per (cluster, site, type); the
  values-repo append is a no-op for a file already recording this allocation;
  `release_segment` answers 200 for an already-released segment and the DHCP
  scope DELETE 204 for an absent scope. add-server-to-mce's three creates each treat an existing resource as success once
  it matches, and a pool run refuses a server that already has a BareMetalHost
  (giving its lock back) rather than "succeed" over another install's host.
  Workflow ids are deterministic (`initialize-segment-<network>`,
  `allocate-segment-<TYPE>-<cluster>`, `release-segment-<TYPE>-<cluster>`,
  `add-server-to-mce-<mce>_<server>`, `remove-server-from-mce-<server>`,
  `uninstall-server-from-hc-<server>`, `update-nodepool-<cluster>`), so a duplicate trigger while running gets
  HTTP 409. add-server-to-mce's pool draws are the exception on purpose: each takes
  a free slot (`add-server-to-mce-<mce>_<infraEnv>_<slot>`), and the install lock keeps
  them on different machines.

## Configuration

| key | ConfigMap | notes |
| --- | --- | --- |
| `TEMPORAL_HOST`, `TEMPORAL_NAMESPACE` | `workflows-orchestrator-config` | shared by every domain |
| `SEGMENTS_MANAGER_URL` | `workflows-orchestrator-config` | shared by every domain |
| `DHCP_API_URL` | `segment-lifecycle-config` | release-segment's DHCP scope API (GETs anonymous) |
| `SEGMENTS_MANAGER_API_TOKEN` | Secret | mutating calls only; GETs are public |
| `DHCP_API_TOKEN` | Secret `dhcp-api-token` | the scope DELETE; a copy of the DHCP API's own token — rotate together |
| `DAY1_REPO_URL` | `cluster-lifecycle-config` | provision-cluster's day1 repo: read at `main`, pushed on `provision-cluster/<cluster>` (the same repo the upgrade-gitops chart names); create-vm's list of a zone's KV clusters is read from it too. GitLab: the merge request is opened through the API on this URL's host, the project being the URL's path (subgroups included) |
| `DHCP_EXCLUSION_OCTET_RANGES` | `cluster-lifecycle-config` | the DHCP policy written into every cluster file (was segment-lifecycle's until 2026-11-11) |
| `DAY2_SIG_REPO_URL_TEMPLATE` | `cluster-lifecycle-config` | the day2 sig repos: the URL with `{sig}`, one GitLab project per sig, the tree at its root |
| `REDBULL_GIT_TOKEN` | Secret `redbull-git-token`, key `REDBULL_GIT_TOKEN`, under its own name | push rights AND GitLab's API (`api` + `write_repository`), with a role that may push each sig project's protected default branch (Maintainer); `DAY1_GIT_TOKEN` is accepted too (a cluster with only `day1-git-token`), `REDBULL_GIT_TOKEN` wins; scrubbed from every error |
| `DAY2_GIT_TOKEN` | not rendered by the chart | the sig repos' push token; empty = `REDBULL_GIT_TOKEN`. The chart sets none: `redbull-git-token` is the group token and pushes day1 and every sig repo |
| `SHORTCUTS_API_URL` | `cluster-lifecycle-config` | the shortcuts service (go-links): `GET <url>/check/<name>`, `POST <url>/create` — provision-cluster's last step |
| `SHORTCUTS_API_TOKEN` | Secret `shortcuts-api-token`, key `SHORTCUTS_API_TOKEN` | its bearer token, created out of band; a rejected one holds runs at `creating-shortcut` (retried) |
| `SHORTCUT_OWNER` | `cluster-lifecycle-config` | the `username` every shortcut is created with; the service reports it back as the owner, which is how a re-run knows a taken name is its own |
| `CLUSTER_CONSOLE_URL_TEMPLATE` | `cluster-lifecycle-config` | the console URL a shortcut points at, `{cluster}` = the day1 file stem: `https://console-openshift-console.apps.{cluster}.<base domain>` |
| `HEADROOM_BUFFER_PERCENT` | `cluster-lifecycle-config`, optional | create-vm's placement policy, ONE value for every KV cluster: the share of each cluster's usable allocatable kept unrequested after placing a VM, 0–90, default 15. Handed to each run with its zone's cluster list |
| `MCE_CLUSTER` | `server-lifecycle-config` | which MCE this worker serves — names its queue, so it must match callers' `mce_cluster` |
| `SERVER_SCAN_URL` | `server-lifecycle-config` | inventory API base, INCLUDING `/api/v1` |
| `SERVER_SCAN_API_TOKEN` | Secret | an **admin** token — add-server-to-mce takes and releases the install lock, and puts a server with no Agent in maintenance |
| `{HP,DELL,CISCO,INTERSIGHT}_BMC_USERNAME`/`_PASSWORD` | Secret | what Ironic drives the BMC with |
| `DAY1_REPO_URL`, `DAY1_VALIDATE_COMMAND` | `server-lifecycle-config` | update-nodepool's day1 read and replicas commit, `https://`; OPTIONAL — unset, only update-nodepool is refused on that MCE |
| `DAY1_GIT_TOKEN` | Secret (credentials, or `day1.tokenSecret`) | its push token; optional with the URL |
| `ARGO_NAMESPACE`, `ARGO_VALUES_REF` | `server-lifecycle-config` | update-nodepool's Argo CD sync: the Applications' namespace (`openshift-gitops`) and the day1 source's `ref` (`values`) |
| `OME_URL` | `server-provisioning-config` | the OpenManage Enterprise appliance, `https://` |
| `OME_USERNAME`/`OME_PASSWORD` | Secret | discovers devices and deploys templates |
| `IDRAC_USERNAME` | `server-provisioning-config` | default `root` — the only iDRAC account touched |
| `IDRAC_ROOT_PASSWORD` | Secret | the password the template enforces; tried first |
| `IDRAC_FACTORY_PASSWORDS` | Secret | JSON list, at most 2 — what servers may arrive with |
| `DELL_TEMPLATES` | `server-provisioning-config` | `{"<Redfish model>": {"<iDRAC firmware>": "<OME template>"}}` |
| `SERVER_NAMER_URL` | `server-provisioning-config` | the naming service |
| `SERVER_NAMER_API_TOKEN` | Secret | optional bearer token |
| `SERVER_SCAN_URL`/`_API_TOKEN` | as above | the provisioning worker reads it too (a viewer token) |
| `KV_CLUSTER` | `vm-lifecycle-config` (in each KV cluster) | the KV cluster this worker runs in: EXACTLY its day1 file name (`ocp4-<lifecycle>-kv-<site>-<zone>-<id>`), never abbreviated — it names the queue; the site and the zone are read from it |
| `VM_STORAGE_CLASS_NAME` | `vm-lifecycle-config` | the root disk's storage class; must bind Immediately and allow RWX + Filesystem |
| `VM_NETWORK_NAME` | `vm-lifecycle-config` | `<namespace>/<name>` of the NetworkAttachmentDefinition every VM boots on (default `openshift-cnv/vm-external-network`, the platform's vm.yaml); the network is always multus |
| `VM_NODE_SELECTOR` | `vm-lifecycle-config` | JSON: the VM template's nodeSelector (`{}` for none) |
| `VM_REDFISH_LABEL`, `VM_ROOT_DISK_SIZE` | `vm-lifecycle-config` | `redfish-enabled=true` and `120Gi` by default |

server-scan holds no BMC credentials by design, so those live here; a vendor
with none configured fails that server's install rather than writing a Secret
Ironic cannot authenticate with.

## Run locally

Assumed already running: a Temporal server (`TEMPORAL_HOST`) and the Segments
Manager (`SEGMENTS_MANAGER_URL` — e.g. the OpenShift route), with `API_TOKEN`
matching `SEGMENTS_MANAGER_API_TOKEN`.

```bash
cp .env.example .env    # then point it at your Temporal / Segments Manager

# Workers (from the repo root)
pip install -r activities/segment_lifecycle/requirements.txt -r activities/cluster_lifecycle/requirements.txt
PYTHONPATH=. python -m workflow_domains.main_worker_init &
PYTHONPATH=. python -m activities.segment_lifecycle.worker_init &
PYTHONPATH=. python -m activities.cluster_lifecycle.worker_init &     # provision-cluster (git + the provider's API); create-vm's day1 list
# create-vm also needs a worker per KV cluster of the zone. The real one runs INSIDE a KV
# cluster (activities.vm_lifecycle.worker_init, KV_CLUSTER=<the day1 file name>, a kubeconfig
# for a developer). To see a zone of many without the clusters, attach fake ones to the same
# Temporal — the fleet the tests use, real limb code over an in-memory cluster API:
#   PYTHONPATH=. python -m tests.kv_fleet --temporal localhost:7233 \
#     --cluster ocp4-prod-kv-five-az1-a:100:1024 --cluster ocp4-prod-kv-five-az1-b:100:1024

# Unified API
pip install -r requirements.txt
PYTHONPATH=. uvicorn workflow_domains.api:app --port 8080
# Swagger UI: http://localhost:8080/docs
# curl -X POST localhost:8080/workflows/segment-lifecycle/initialize-segment \
#   -H 'content-type: application/json' \
#   -d '{"segment":"130.154.20.0/24","site":"site1","vlan_id":100,"epg_name":"EPG_PROD_01"}'
# curl localhost:8080/workflows/runs/initialize-segment-130.154.20.0
# curl -X POST -H 'content-type: application/yaml' --data-binary @cluster.yaml \
#   'localhost:8080/workflows/cluster-lifecycle/provision-cluster?cluster=prep-e2e-site2-a&mce=ocp4-prep-mce-site2-a'
# curl localhost:8080/workflows/runs/provision-cluster-ocp4-prep-e2e-site2-a
```

Inspect runs in the Temporal UI and verify the segment in the manager:
`curl "$SEGMENTS_MANAGER_URL/api/segments/by-segment?segment=130.154.20.0/24"` — a
completed run leaves it `Available`, with `type: null` until it is allocated.

### kind

```bash
docker build -f workflow_domains/Dockerfile -t workflows:dev .
docker build -f activities/segment_lifecycle/Dockerfile -t segment-lifecycle-worker:dev .

kind load docker-image workflows:dev segment-lifecycle-worker:dev --name prep-temporal
helm install workflows-orchestrator \
  ../redbull-platform/gitops/charts/workflows-orchestrator -n redbull-workflows --create-namespace
helm install segment-lifecycle-worker \
  ../redbull-platform/gitops/charts/segment-lifecycle-worker -n redbull-workflows
```

Neither chart creates the namespace itself — `--create-namespace` on the first
`helm install` is what creates `redbull-workflows` here. On redbull-platform that
namespace is pre-created by its own `namespaces` release instead, so both charts
are installed there with plain `-n redbull-workflows` (no `--create-namespace`).

## Deploying elsewhere (e.g. air-gapped OpenShift)

In the cluster this is Argo CD's job: the charts are vendored in redbull-platform
and pushing its `main` deploys. What to mirror is in this repo's GitHub Releases: each
lists the images it rebuilt under "To pull" and every image at that release, each as
`ghcr.io/team-redbull/<image>:<version>@<digest>`. To install by hand, push the two
worker images to a registry the cluster can pull from, then:

```bash
# The brain owns workflows-orchestrator-config (the global values), so install it first.
helm install workflows-orchestrator \
  ../redbull-platform/gitops/charts/workflows-orchestrator -n redbull-workflows --create-namespace \
  --set image.repository=<registry>/workflows-orchestrator \
  --set config.temporalHost=<temporal-host>:7233 \
  --set config.segmentsManagerUrl=https://<segments-manager-route>

# The limbs read the global values from workflows-orchestrator-config above and
# set their own.
helm install segment-lifecycle-worker \
  ../redbull-platform/gitops/charts/segment-lifecycle-worker -n redbull-workflows \
  --set activityWorker.image.repository=<registry>/segment-lifecycle-worker \
  --set secrets.segmentsManagerApiToken=<real-token>
helm install cluster-lifecycle-worker \
  ../redbull-platform/gitops/charts/cluster-lifecycle-worker -n redbull-workflows \
  --set activityWorker.image.repository=<registry>/cluster-lifecycle-worker \
  --set config.day1RepoUrl=https://<values-repo> \
  --set 'config.day2SigRepoUrlTemplate=https://<gitlab>/redbull/gitops-day2-prod/sigs/{sig}.git'
# plus the redbull-git-token Secret (REDBULL_GIT_TOKEN) both charts' values name.
```

Edit `config.dhcpExclusionOctetRanges` in the cluster-lifecycle-worker chart's
`values.yaml` (then restart that worker) to change the DHCP policy without a rebuild.

## Working on this repo with Claude Code

Nothing to install: clone, open the folder, and trust it when Claude Code asks (the
committed hook waits on that). Everything below is in git.

| piece | role |
| --- | --- |
| `CLAUDE.md` | the rationale: why the architecture and every rule are the way they are; always loaded |
| `.claude/rules/*.md` | path-scoped checklists, loaded when you touch workflow code, routers, activities, `shared/`, tests, CI, charts or docs |
| `.claude/skills/redbull-workflows/` | the procedure: new workflow, new domain or limb (with its chart), activities, changing or renaming a running workflow, docs, definition of done (`/redbull-workflows`) |
| `.claude/skills/temporal-developer` | Temporal's own SDK skill, vendored at `.agents/skills/temporal-developer/` (`skills-lock.json` pins it); SDK mechanics, the repo's rules win where they differ |
| `scripts/check_conventions.py` | the mechanical checks: run on every edit by the hook in `.claude/settings.json` (which also names the docs pages to update), in `pytest` (`tests/test_conventions.py`) and in CI's `test` job, which every image build needs |

Run the checks yourself at any time; the checker needs only a system `python3`:

```bash
python3 scripts/check_conventions.py                       # whole repo, cross-file rules too
python3 scripts/check_conventions.py --file path/to/x.py   # one file, what the hook sees
.venv/bin/python -m pytest -q                              # the suite, conventions included
```

Personal settings go in `.claude/settings.local.json` (gitignored); `.claude/settings.json`
is shared and holds only the hook.
