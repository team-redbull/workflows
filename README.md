# Cluster Orchestrator

An OpenShift cluster lifecycle orchestrator built on Temporal. Three domains today:

- **segment-lifecycle** owns the segments a hosted cluster runs on: **creating**
  them in the team's **Segments Manager**, and **allocating** one to a cluster
  and writing its DHCP block into the day1 values repo.
- **server-lifecycle** owns putting physical servers into an MCE inventory:
  **install-server** takes a healthy, unclaimed server from the **server-scan**
  inventory platform and creates the `BareMetalHost` + BMC `Secret` +
  `NMStateConfig` an InfraEnv needs. It replaces `bmh-generator-operator`, a
  Kopf operator that queried four vendor managers itself.
- **server-provisioning** owns getting a racked server INTO that inventory:
  **provision-dell-server** takes a Dell whose iDRAC has an IP and nothing else,
  and discovers it in OpenManage Enterprise, deploys its template (which sets
  root's password), builds the RAID 1 on the BOSS, sets the PERC drives
  Non-RAID, has the naming service rename it, and completes when server-scan
  lists it — ready for install-server. It replaces the DC team's manual OME and
  BIOS work; Cisco, HPE and Intersight get their own workflows later.

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
  settings.py / exceptions.py / consts.py / workflow_ids.py / logging_config.py
workflow_domains/                 The brain — one folder per domain, plus main_worker_init.py + api.py
  segment_lifecycle/              initialize_segment.py, allocate_segment.py, router.py
  server_lifecycle/               install_server.py (the SHAPE of the run),
                                    bond_selection.py (the pure rule it applies),
                                    router.py
  server_provisioning/            provision_dell_server.py, storage_plan.py and
                                    server_name.py (its pure rules), regions.py
                                    (iDRAC prefix -> region), router.py
  routers/                        What no domain owns: deps, shared models, runs.py (status)
activities/<domain>/              The limbs
  activities.py                   The @activity.defn surface ONLY — thin
  <technology>.py                 One module per dependency, plain parameters, no
                                    settings and no Temporal, so each is testable
                                    with no worker: values_repo.py (git),
                                    server_scan.py (the inventory API),
                                    cluster_api.py (the Kubernetes API, its
                                    idempotency rule and error classification),
                                    bmh_resources.py (the resource bodies),
                                    idrac.py (Redfish), ome.py (OpenManage),
                                    server_namer.py (the naming service)
  worker_init.py                  Registers this domain's activities, polls its queue
docs/                             The static documentation site (its own image)
```

The prod charts are vendored into the Argo CD repo, not this one:

| chart | what it deploys |
| --- | --- |
| `redbull-platform/gitops/charts/workflows-orchestrator/` | the brain — ONE release, shared by every domain |
| `redbull-platform/gitops/charts/segment-lifecycle-worker/` | the `segment-lifecycle-worker` limb |
| `redbull-platform/gitops/charts/server-lifecycle-worker/` | the `server-lifecycle-worker` limb |
| `redbull-platform/gitops/charts/server-provisioning-worker/` | the `server-provisioning-worker` limb — **not created yet**; CI's image bump fails until it exists |

Worker-file naming convention: the workflow (brain) worker is
`workflow_domains/main_worker_init.py`; each activity domain's worker is
`activities/<domain>/worker_init.py`.

## The workflows

Each has its own workflow queue and its own deterministic id. The two
segment-lifecycle workflows share the one `segment-lifecycle-activity` queue and
therefore the one limb deployment; `install-server` runs on
`server-lifecycle-activity` and reaches back to the segment-lifecycle queue for
one activity (below).

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

### `install-server` — put a server into an MCE inventory

`POST /workflows/server-lifecycle/install-server` with the **InfraEnv** to fill
and its **MCE cluster**.

#### In plain terms — read this first

Say you ask for a server for InfraEnv `x` on **MCE-A** and server-scan returns
three candidates, **S1, S2, S3**. The run does **not** reserve all three: it
works on S1 until S1 succeeds or is given back, and only then looks at S2. S2
and S3 stay free for everyone else meanwhile.

1. **Check S1** (seconds, nothing written): usable name, known BMC vendor, a
   BMC address, two link-up NICs on different physical ports, well-formed MACs,
   no BareMetalHost for it already in MCE-A. Fails one → skip to S2.
2. **Lock S1 in server-scan** for 2 h ("being installed into MCE-A by this
   run"). Another run holds it → skip to S2.
3. **Create** S1's Secret, BareMetalHost and NMStateConfig in MCE-A.
4. **Wait up to 1 h for an Agent** — the machine phoning home from the
   discovery ISO, proof that the BMC, bond, VLAN and DHCP all worked. The lock
   is renewed for 2 h as the wait starts.
5. **Agent** → extend S1's lock to **24 h** and finish; S2 and S3 never touched.
   **No Agent after 1 h** → delete S1's resources, **release** its lock (S1 is
   drawable again at once), back to step 1 with S2.

The run fails only when every candidate was skipped or tried (worst case about
3 h for three that never boot), with the reasons grouped.

**Why the lock, and what the 24 h hold is.** server-scan learns a machine is
taken only when a *cluster* reports it (its membership job, every 15 min where
enabled); until then it reads `AVAILABLE`. The "already has a BareMetalHost?"
check sees only *this* MCE, so without the lock a run for MCE-B could draw S1
minutes after MCE-A installed it. `/servers/available` never hands out a locked
machine. The 24 h hold after success writes nothing to any cluster — it only
keeps S1 out of other draws until server-scan can see it in a cluster itself.

**Who checks what.** server-scan: unclaimed, not in maintenance, BMC
reachable, health tier, the **network category HEALTHY — at least two links
observed UP** (since 2026-09-26; Dell NPAR already reduced to one entry per
physical port), and not locked. The workflow: *which* two NICs form the bond,
the name, BMC driver and address, MAC syntax, and no BareMetalHost in this MCE.
The bond rule overlaps server-scan's network gate on purpose — server-scan says
the machine *can* be bonded; the workflow picks the two NICs and is the last
check before a cluster write. (`min_nic_macs=2` predates that gate and now adds
little beyond "the MACs were read this run".)

| What happens when… | The run | The lock |
| --- | --- | --- |
| a candidate fails a check | skips it | never taken |
| another run holds the candidate | skips it | theirs, untouched |
| the Agent appears | succeeds | held 24 h |
| no Agent within 1 h | deletes the candidate's resources, tries the next | released at once |
| the lock lapsed and another run took the machine | deletes its *own* resources, tries the next | theirs, untouched |
| every candidate skipped or tried | fails, reasons grouped | none held |
| crash/failure while creating resources | stops, resources **left** on the MCE | expires after 2 h |
| operator cancels | stops, resources **left** to show progress | expires |
| no worker in that MCE | waits; `progress` names the queue | — |
| server-scan token not admin | fails at the first lock (`ServerScanAuthError`) | never taken |

**Known gaps.** (1) The 24 h hold can run out: with no membership job on that
MCE, or a cluster not built within a day, the machine reads `AVAILABLE` again
and another MCE could draw it — 24 h is server-scan's ceiling. (2) A crash
mid-create leaves the BareMetalHost on the MCE while the lock expires after
2 h; clean up by hand, or re-run with `server_name` set to that machine.

#### Phase by phase

1. **resolving-vlan** — the MCE's `INVENTORY` segment, read from the Segments
   Manager by **cluster name**. Before the draw, because no candidate could make
   a missing allocation usable. Runs on the *segment-lifecycle* queue, where that
   credential already lives.
2. **acquiring-server** — `GET /servers/available` against server-scan. The
   InfraEnv's name states which hardware it is for
   (`cisco-m6-bat-yam-64c-512gb`) and server names carry the same tokens behind
   an `ocp-` prefix, so the pattern is `^ocp-<infraEnv>`. `HEALTHY` only.
3. **selecting-server** — the first candidate that can actually be installed.
   Bond members are two link-up NICs on two **distinct physical ports**, taken
   from `interfaces[]` and never
   `nic_macs`: server-scan reduces NPAR partitions to one entry per port in the
   former and leaves the latter whole, so indexing MACs can bond two partitions
   of one wire. `UP` is required strictly, which no HPE server can satisfy —
   OneView reports no link state at all.

   **Every** reason a candidate is unusable is a *skip*, never a failed run —
   an unusable name, no bond, an unknown `bmc_vendor`, no BMC host, a malformed
   MAC, a BareMetalHost it already has, or an install lock another run holds
   (below). That is what the multi-candidate draw is for. If none survives,
   the reasons are reported as separate groups and the failure takes the
   specific error type when they all agree — which they always do for an
   explicitly named server, whose pool holds one.

   **reserving-server** — the chosen machine is locked in server-scan
   (`POST /servers/{id}/reservation`, server-scan's ADR-0035) before anything is
   written. A lock another run holds is a skip (`ServerReservedError` if the
   whole pool is held), as is a server that has left the inventory.
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
   and another run took the machine, this candidate is torn down instead.
   On success (**holding-server**) the lock is extended to a day — the machine
   is taken, and server-scan keeps listing it `AVAILABLE` until a membership
   job sees it in a cluster.
6. **rolling-back** — no Agent by the deadline is that **candidate's** failure,
   not the run's. The NMStateConfig and BareMetalHost are removed (the Secret
   cascades off the host's ownerReference) and then the lock is released
   (**releasing-server**), which returns the machine to the inventory, and the
   next candidate is tried from step 3. Only an exhausted
   pool fails the run, as `AgentNeverAppearedError`.

   Steps 3–6 are a **loop**, not a pipeline: a server cannot be shown to be
   installable without creating its resources and watching what happens, so the
   draw is a list of things to *try*. Teardown order is load-bearing — metal3
   must be told to detach the host *before* the delete, or its finalizer blocks
   forever trying to deprovision through the BMC that just failed to answer.

   This replaced a 10-minute Ironic-registration deadline, which was the wrong
   signal both ways: it passed hosts whose bond or VLAN was wrong (they register
   fine, then never boot), and on an unreachable BMC metal3 reports
   `registering` with operationalStatus OK and no errorType for as long as you
   watch — 14 hours, measured — so only the deadline itself distinguished a dead
   host from a slow one.

Its id is `install-server-<infraEnv>` — the **candidate pool**, not the target
pair, because the pool is `^ocp-<infraEnv>` with no MCE in it. Installs drawing
from one pool are therefore serial.

**The install lock is the guard across MCEs.** server-scan lists a machine
`AVAILABLE` until a cluster reports it, and the BareMetalHost probe in step 3
only sees the run's *own* MCE — so without a lock, a server installed into
MCE-A is drawn again by a run for MCE-B and both clusters drive one BMC. The lock
is keyed on the run's workflow id (a retry or renewal extends it, never loses a
race to itself), expires on its own (a dead run costs one TTL, not the
machine), and is released only after a teardown proved nothing on the cluster
still points at the machine; a failure anywhere else leaves it to expire,
because half-created resources may still name it. It needs server-scan's
**admin** role. Runs started before it existed replay without it
(`workflow.patched`). With the lock in place the per-pool id could become
per-(pool, MCE); that is a separate decision and not made here.

**The inventory VLAN is the MCE's, and is read before anything is drawn.** An
MCE owns one inventory network, allocated in the Segments Manager as
`INVENTORY` and found by its **cluster name** — so the VLAN is a property of the
cluster this run targets and of nothing else. It is resolved as step one,
because no candidate the run might draw could make a missing allocation usable,
and the lookup runs on the *segment-lifecycle* queue where that token already
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

**Two ways to trigger it.** `server_name` is optional, and leaving it out is the
normal mode:

| | `server_name` omitted — *"fill this InfraEnv"* | `server_name` given — *"install this machine"* |
| --- | --- | --- |
| server-scan query | `?pattern=^ocp-<infraEnv>&count=<candidate_count>` | `?name=<server_name>&count=1` |
| candidates drawn | `candidate_count` (default 3, 1–20) | one |
| already has a BareMetalHost | **skipped**, next candidate tried | **converged** — that is the point |
| workflow id | `install-server-<infraEnv>` | `install-server-name-<server_name>` |
| use it for | routine capacity: take whatever is free and healthy | re-running a specific host, or repairing one |

Both modes apply every other rule identically: `HEALTHY` only, at least two NIC
MACs, two link-up NICs on two distinct physical ports, a `bmc_vendor` this
orchestrator has a driver for, and well-formed MACs. So naming a server does not
force an unusable one through — it only narrows the pool to one and turns the
already-installed check off.

Drawing several candidates is what makes one unusable server a retry rather than
a failed run. When no candidate survives, the failure names every candidate with
its provider and each interface's link state, and lists separately those skipped
as already installed and those whose server-scan name cannot be a Kubernetes
resource name — three different people's problem, so they are never merged.

### Request body — `InstallServerInput`

| field | type | rules | what it is for |
| --- | --- | --- | --- |
| `infra_env` | str | non-empty | The InfraEnv to fill. Labels both resources, **and** is the server query (`^ocp-<infra_env>`). Must already exist, in `namespace`. |
| `mce_cluster` | str | non-empty | Which MCE. Names the `INVENTORY` segment the VLAN comes from **and** the activity queue the cluster writes go to. |
| `namespace` | str | non-empty | Where the three resources go. **Must be the InfraEnv's own namespace** — BMAC looks for the InfraEnv beside the BareMetalHost. |
| `server_name` | str \| null | default `null` | Install one specific machine instead of drawing from the pool (see above). |
| `candidate_count` | int | 1–20, default 3 | How many candidates to draw. Ignored when `server_name` is given. |
| `labels` | map | default `{}` | Extra labels for the BareMetalHost. Cannot override the InfraEnv label — that would split the host from its NMStateConfig. |

There is deliberately **no `vlan_id`**: it belongs to the MCE's segment, and a
supplied one could contradict it. The request body forbids unknown fields, so
sending `vlan_id` is a 422 rather than a silently ignored value.

### `provision-dell-server` — from an iDRAC IP to a server install-server can use

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

### `allocate-segment` — give a cluster a segment

Takes the cluster, the **values-repo branch** to record on, and the **type** to
allocate as (default `HC`):

```json
{"cluster": "ocp4-prep-herzi-site1-a", "values_branch": "feature/ocp4-prep-herzi-site1-a"}
```

Locate the cluster's values file on that branch of the day1 repo (the path gives
the site, cross-checked against `GET /api/sites`), allocate a segment from the
Segments Manager — any Available segment at the site, which becomes the requested
type in the same step — **read it back** to verify status/cluster/vlan/type all
match, then append the `vlanId` + `dhcp_values` block to the file and push it to
that branch with `[skip ci]`. The run ends there. HC only for now; any other type
is rejected up front, and a branch the repo does not have fails with
`ValuesBranchNotFoundError`.

It runs as a step **inside the day1 values repo's CI pipeline**, which runs on a
temporary branch only: the pipeline passes its own `$CI_COMMIT_BRANCH`, waits for
the run, then generates the MachineConfig files from the recorded `vlanId`; a
human merges afterwards. There is no DHCP scope wait: Argo CD reads `main` only,
so the scope appears after that merge, outside the run (see CLAUDE.md §4).

## API

The trigger is async throughout: POST returns **202 + workflow id** immediately;
poll `GET /workflows/runs/{workflow_id}` for phase (workflow query) and the final
result.

| route | starts |
| --- | --- |
| `POST /workflows/segment-lifecycle/initialize-segment` | one segment |
| `POST /workflows/segment-lifecycle/initialize-segment/bulk` | one workflow PER segment |
| `POST /workflows/segment-lifecycle/allocate-segment` | one allocation |
| `POST /workflows/server-lifecycle/install-server` | one server into an InfraEnv |
| `POST /workflows/server-provisioning/provision-dell-server` | one Dell server, by iDRAC IP |
| `POST /workflows/server-provisioning/provision-dell-server/bulk` | one workflow PER iDRAC IP |
| `GET  /workflows/runs/{workflow_id}` | (status, every domain) |

### Paths: `/workflows/<domain>/<workflow>`, status on `/workflows/runs`

A domain holds MANY workflows, so it is never itself an endpoint — each workflow
owns its own path under the `segment-lifecycle` prefix, and a third is then just
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
  `SEGMENTS_MANAGER_URL`, `DAY1_REPO_URL`). The same images run on
  kind or OpenShift; only the chart's `values.yaml` (`config.*`) changes.
  `host.docker.internal` appears only there, never in code.
- **ConfigMap split by scope:** `workflows-orchestrator-config` (owned by the
  always-present brain release) holds what every domain shares — `TEMPORAL_*` and
  `SEGMENTS_MANAGER_URL`. Each domain adds its own `<domain>-config` (here
  `segment-lifecycle-config`: `DAY1_REPO_URL` and the DHCP policy;
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
  values-repo append is a no-op for a file already recording this allocation.
  install-server's three creates each treat an existing resource as success once
  it matches, and a run skips candidates that already have a BareMetalHost.
  Workflow ids are deterministic (`initialize-segment-<network>`,
  `allocate-segment-<TYPE>-<cluster>`, `install-server-<infraEnv>` or
  `install-server-name-<server>`), so a duplicate trigger while running gets
  HTTP 409.

## Configuration

| key | ConfigMap | notes |
| --- | --- | --- |
| `TEMPORAL_HOST`, `TEMPORAL_NAMESPACE` | `workflows-orchestrator-config` | shared by every domain |
| `SEGMENTS_MANAGER_URL` | `workflows-orchestrator-config` | shared by every domain |
| `DAY1_REPO_URL` | `segment-lifecycle-config` | allocate-segment's values repo (the branch is per run) |
| `DHCP_EXCLUSION_OCTET_RANGES` | `segment-lifecycle-config` | DHCP policy |
| `SEGMENTS_MANAGER_API_TOKEN` | Secret | mutating calls only; GETs are public |
| `DAY1_GIT_TOKEN` | Secret `day1-git-token` | push rights; scrubbed from every error |
| `MCE_CLUSTER` | `server-lifecycle-config` | which MCE this worker serves — names its queue, so it must match callers' `mce_cluster` |
| `SERVER_SCAN_URL` | `server-lifecycle-config` | inventory API base, INCLUDING `/api/v1` |
| `SERVER_SCAN_API_TOKEN` | Secret | an **admin** token — install-server takes and releases the install lock |
| `{HP,DELL,CISCO,INTERSIGHT}_BMC_USERNAME`/`_PASSWORD` | Secret | what Ironic drives the BMC with |
| `OME_URL` | `server-provisioning-config` | the OpenManage Enterprise appliance, `https://` |
| `OME_USERNAME`/`OME_PASSWORD` | Secret | discovers devices and deploys templates |
| `IDRAC_USERNAME` | `server-provisioning-config` | default `root` — the only iDRAC account touched |
| `IDRAC_ROOT_PASSWORD` | Secret | the password the template enforces; tried first |
| `IDRAC_FACTORY_PASSWORDS` | Secret | JSON list, at most 2 — what servers may arrive with |
| `DELL_TEMPLATES` | `server-provisioning-config` | `{"<Redfish model>": {"<iDRAC firmware>": "<OME template>"}}` |
| `SERVER_NAMER_URL` | `server-provisioning-config` | the naming service |
| `SERVER_NAMER_API_TOKEN` | Secret | optional bearer token |
| `SERVER_SCAN_URL`/`_API_TOKEN` | as above | the provisioning worker reads it too (a viewer token) |

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
pip install -r activities/segment_lifecycle/requirements.txt
PYTHONPATH=. python -m workflow_domains.main_worker_init &
PYTHONPATH=. python -m activities.segment_lifecycle.worker_init &

# Unified API
pip install -r requirements.txt
PYTHONPATH=. uvicorn workflow_domains.api:app --port 8080
# Swagger UI: http://localhost:8080/docs
# curl -X POST localhost:8080/workflows/segment-lifecycle/initialize-segment \
#   -H 'content-type: application/json' \
#   -d '{"segment":"130.154.20.0/24","site":"site1","vlan_id":100,"epg_name":"EPG_PROD_01"}'
# curl localhost:8080/workflows/runs/initialize-segment-130.154.20.0
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
and pushing its `main` deploys. To install by hand, push the two worker images to
a registry the cluster can pull from, then:

```bash
# The brain owns workflows-orchestrator-config (the global values), so install it first.
helm install workflows-orchestrator \
  ../redbull-platform/gitops/charts/workflows-orchestrator -n redbull-workflows --create-namespace \
  --set image.repository=<registry>/workflows-orchestrator \
  --set config.temporalHost=<temporal-host>:7233 \
  --set config.segmentsManagerUrl=https://<segments-manager-route>

# The limb sets its own day1/DHCP-policy config; it reads the global values from
# workflows-orchestrator-config above.
helm install segment-lifecycle-worker \
  ../redbull-platform/gitops/charts/segment-lifecycle-worker -n redbull-workflows \
  --set activityWorker.image.repository=<registry>/segment-lifecycle-worker \
  --set config.day1RepoUrl=https://<values-repo> \
  --set secrets.segmentsManagerApiToken=<real-token> \
  --set secrets.day1GitToken=<real-token>
```

Edit `config.dhcpExclusionOctetRanges` in the chart's `values.yaml` (then restart
the activity workers) to change the DHCP policy without a rebuild.
