# CLAUDE.md

Guidance for a Temporal-based OpenShift cluster-lifecycle orchestrator — architectural decisions,
Temporal SDK gotchas, coding preferences. Read before generating code.

## 0. How this file is kept true — the layers, and where a rule lives

This file is the RATIONALE. Three committed layers make it hold for every session and
every teammate; use them in this order and keep them in step:

- **`.claude/rules/*.md`** — path-scoped INVARIANTS (workflow-side code, routers and UI
  pages, activities, `shared/`, tests, delivery), loaded automatically when a matching
  file is read or edited. Terse checklists citing the section here and the checker rule id.
- **`.claude/skills/redbull-workflows/`** — the PROCEDURE for adding a workflow, a domain
  or a limb (with its chart), changing an activity, changing or renaming a workflow with
  runs in flight, and the definition-of-done gate. Its step 3 makes DOCS part of every
  change: a table maps each kind of change to the page sections, README tables, chart
  READMEs and sections here it must update, in the same change. Invoke
  `/redbull-workflows`; it auto-activates on `workflow_domains/`, `activities/`,
  `shared/`, `deploy/`, `docs/`. It composes with **`temporal-developer`** (vendored in
  `.agents/skills/`, symlinked into `.claude/skills/`): that skill owns SDK mechanics,
  THIS repo's rules win where the two differ (the skill lists the divergences).
- **`.claude/skills/naming-convention/`** — HOW TO NAME a cluster (tenant, MCE, seed, KV),
  an InfraEnv, a NodePool, a server or VM, a zone or a label: it fetches the platform's
  convention, `Idan-Maimon/kubevirt-workflows` `docs/naming-convention.md`, the single source
  of truth, before anything is named. Where this file or the code spells a name differently,
  the convention wins and the mismatch is reported. Invoke `/naming-convention`.
- **`scripts/check_conventions.py`** — the ENFORCEMENT: AST checks of every rule that
  can be checked from source (sandbox imports, activity-call options, unbounded retries
  named through exception classes, no whole response body in an activity error,
  `ApplicationError` from workflow code, one `run()`
  argument, no `await` in a workflow's `finally` and no `workflow.in_workflow()` without the run's
  own id, `connect_temporal` only, git only through `activities/git_repo.py` — a `fresh_clone`
  that pushes, a `snapshot` for every read — day1's layout spelled only in `activities/day1_repo.py`,
  interface ↔ implementation ↔ worker registration,
  `_WORKER_SPECS`, router paths and 202s, Dockerfile CMDs, each limb's row in CI's build matrix
  naming its own image and chart, a settings package that re-exports nothing, a docs page per started workflow). It runs on every Edit/Write through the PostToolUse hook in
  `.claude/settings.json` (which also names the docs pages describing the edited file),
  as `tests/test_conventions.py`, and in CI's `test` job, which every image build needs.

A rule that can be checked belongs in the checker; one that cannot belongs here AND in
the matching rules file. When a rule changes, all three move in ONE commit. The checker
is the tie-breaker: an edit it flags is wrong until the rule itself is changed. A
documented exception is marked where it lives, `# conventions: allow <RULE> — <reason>`,
and recorded here. Five today: the agent call's bounded retry in `cluster_upgrade/dispatch.py`
(WF007, §5); upgrade-targets' missing progress query (WF014: a synchronous question
answered in the response, §2, so nothing polls it); `day1_writer.publish`'s re-run check, a
fresh clone that only reads (AC008: the writer's own "already there" half, on a branch per
cluster that a snapshot would keep cached for the pod's life); and two paths that look like
day1's and are not (AC009): day2's tree in `upgrade_gitops/day2_repo.py` and
`cluster_lifecycle/day2_sigs.py`, and the upgrade rules' `upgrade/mces/<mce>.yaml` in
`upgrade_gitops/upgrade_rules.py`.

## 1. Domain-driven monorepo: three layers, strictly separated

```
shared/                      Contract layer — the API between brain and limbs
  models/                    Pydantic data classes (typed state across boundaries)
  interfaces/                Activity signatures ONLY (@activity.defn, no body)
  exceptions.py  consts.py   Shared errors / constants (e.g. task-queue names)
  <rule>.py                  A pure rule BOTH sides apply, in one place: bmc_address.py,
                               cluster_names.py and infraenv_names.py (the naming
                               convention), vm_requests.py (a VM's size as the
                               scheduler counts it), upgrade_layout.py
  settings/<limb>.py         pydantic-settings BaseSettings (fail-fast typed config),
                               ONE module per worker + temporal.py; __init__ imports nothing
  logging_config.py          Shared worker logging setup
workflow_domains/            The orchestration "brain" (one lightweight deployment)
  <domain>/                  One folder per domain — mirrors activities/<domain>/
    <workflow>.py            Workflow logic (one file per workflow) — the SHAPE of
                               the run: phases, dispatches, timeouts, failures
    <policy>.py              Pure sandbox-safe rules that workflow needs and no
                               limb does (server_lifecycle/bond_selection.py,
                               vm_lifecycle/placement.py — create-vm's fit rule)
    router.py                That domain's APIRouter (prefix /workflows/<domain>)
  routers/                   API pieces owned by NO domain: deps, shared models,
                               runs.py (domain-agnostic run status), slots.py (the
                               free-slot walk a pool-draw route uses)
  main_worker_init.py        Registers every workflow, one Worker per workflow queue
  api.py                     Unified FastAPI/Swagger entrypoint (2nd entry, same image)
activities/<domain>/         The execution "limbs" (one deployment per domain)
  activities.py              The @activity.defn surface ONLY — thin: settings,
                               activity.logger, and a call into a module below
  <technology>.py            One module per dependency, taking PLAIN PARAMETERS and
                               holding no settings and no Temporal, so it is testable
                               with no worker: day1_writer.py (the cluster file on its
                               branch), git_provider.py (GitLab merge requests),
                               day2_sigs.py (the sig repos), dhcp_api.py (the
                               DHCP scope API), server_scan.py (the inventory API),
                               cluster_api.py (the Kubernetes API, its idempotency
                               rule and its error classification),
                               hosted_cluster_api.py (a HOSTED cluster's API, read
                               through its admin kubeconfig), and the worker inside
                               a KV cluster: kubevirt_api.py (its own cluster's API),
                               cluster_figures.py (the capacity arithmetic),
                               vm_spec.py (the VirtualMachine it builds),
                               vm_volumes.py (which claims, volumes and snapshots
                               are ONE VM's — delete-vm's rule),
                               vm_operations.py (each activity's whole body, over an
                               `api` object — what lets tests/kv_fleet.py run the
                               real limb code for dozens of fake clusters)
  worker_init.py             Registers activities, polls that queue
activities/git_repo.py       Shared by limbs, owned by no domain: the git client (a fresh
activities/day1_repo.py        clone for a writer, a snapshot cached by commit for a
                               reader, token scrubbing) and THE day1 repo reader — the
                               `Day1` query library (a cluster file with its value
                               layers; day1's clusters by site, env, kind and zone).
                               cluster-lifecycle, upgrade-gitops and server-lifecycle
                               all read day1 through it, none through another's
                               domain; create-vm asks the cluster-lifecycle limb
docs/                             The static documentation site (its own image, no code)
```

Prod charts are VENDORED into the Argo CD repo, at
`redbull-platform/gitops/charts/<service>/`: `workflows-orchestrator` (brain: ONE release for
all domains), `segment-lifecycle-worker`, `server-lifecycle-worker` and `server-provisioning-worker`
(limbs: one per domain), cluster-upgrade's `cluster-upgrade-worker`, `upgrade-agent-worker`
and `upgrade-gitops-worker` (since 2026-11-02; with `server-provisioning-worker`, these have no
`gitops/services/` entry yet, so CI bumps their tags and Argo CD deploys nothing),
`cluster-lifecycle-worker` (written 2026-11-11), and `vm-lifecycle-worker` — since 2026-11-20 a
PER-KV-CLUSTER chart, a copy of `deploy/charts/vm-lifecycle-worker/` here that CI keeps equal
(§9), with no `gitops/services/` entry: each KV cluster gets it through day2. (From 2026-11-05
until then it was a hub chart, one release for every zone; its `gitops/services/` entry and the
`kv-capacity` / `kv-vm-api` agent charts go from redbull-platform with it.) One generic ApplicationSet
sweeps `gitops/services/<service>/app.yaml`, so the service FOLDER NAME is the Argo app name,
the chart path and the release name at once. There is no per-environment values layer — a
chart's own `values.yaml` is exactly what the cluster runs — and pushing redbull-platform's
`main` DEPLOYS. CI here bumps the image tag cross-repo INTO that file. The archived
`github.com/team-redbull/helm-charts-<service>` repos are the superseded shape: read them for
nothing, edit them never (GitHub rejects the push anyway — read-only).

- **Workflows and activities are fundamentally separate:** code, deployments, images, task queues,
  RBAC/Secrets. Brain = one lightweight deployment; each `activities/<domain>/` = its own deployment
  + ServiceAccount + Secrets + (when needed) heavy image.
- **Worker-file naming:** brain entry `workflow_domains/main_worker_init.py`; each domain's
  `activities/<domain>/worker_init.py` — deliberately different so the two worker kinds are never
  confused. Dockerfile CMDs must match these module paths.
- **A domain folder owns that domain's WHOLE brain-side surface** — every workflow file plus the
  `router.py` exposing them — so `workflow_domains/<domain>/` mirrors `activities/<domain>/` and a
  new domain is one new folder. Only what NO domain owns stays in `workflow_domains/routers/`:
  `deps.py`, `models.py`, and the domain-agnostic `runs.py`.
- **`shared/` is the typed contract, lightweight ONLY** (`temporalio` + `pydantic` +
  `pydantic-settings`; NEVER `kubernetes`/`boto3`/`ansible-runner`/`httpx`). `interfaces/` holds
  `@activity.defn` signatures with `...` bodies; the impl lives in `activities/<domain>/`, registered
  against that name — prevents typos, enforces types, lets workflows route by exact signature.

## 2. Activity / deployment partitioning

- Driver for a **separate deployment** is **shared dependency set + RBAC/Secrets**, not the
  sub-workflow boundary. Segment-lifecycle activities live in `activities/segment_lifecycle/`
  (one dep+cred set: Segments Manager HTTP + bearer token, day1 git push token).
- Keep `shared/interfaces/` signatures clean so an activity (e.g. "get segment") can be
  re-registered on another queue by a future sub-workflow without moving code. The
  precedent: cluster-upgrade's `read_hosted_cluster` / `sync_argo_application`, also
  registered by the server-lifecycle worker for update-nodepool (§4).
- **One domain may need several limbs** when its work spans several credential sets — the rule
  above, applied. cluster-upgrade has three: `upgrade-gitops` on the hub (day1 push, mirror pull,
  Brain push tokens), and per MCE `cluster-upgrade` (the workflow ServiceAccount) and
  `upgrade-agent` (no Kubernetes credential at all: it POSTs to the upgrade agent service, our own
  service in its own repo, `team-redbull/k8s-agent`, run as a kubernetes-sigs agent-sandbox
  `Sandbox` in the MCE; the service vendors the agent contract,
  `tests/test_upgrade_agent_contract.py` keeps the two equal).
  The hub limb, `activities/upgrade_gitops/`, holds no Kubernetes credential either (§4). Each limb gets its OWN interface module
  (`shared/interfaces/{upgrade_gitops,cluster_upgrade,upgrade_agent}.py`), so the contract test
  still pairs one interface module with one `activities/<limb>/activities.py`; the domain keeps
  one models module and one `workflow_domains/cluster_upgrade/`. The MCE limb is
  `cluster-upgrade-worker` (`activities/cluster_upgrade/worker_init.py`, chart
  `deploy/charts/cluster-upgrade-worker/`), on `cluster-upgrade-activity-<MCE_CLUSTER>` like
  server-lifecycle, with `ARGO_NAMESPACE` / `ARGO_VALUES_REF` as its only other config; the
  workflows reach it through `dispatch.on_mce` / `start_watch`, and
  `tests/test_worker_registration.py` checks every activity sent that way is registered there.
- **vm-lifecycle is ONE WORKER INSIDE EACH KV CLUSTER, holding no Secret at all** (2026-11-20,
  with the operator; it REVERSES 2026-11-04/05's "ONE hub limb for every zone", §4). A
  `vm-lifecycle-worker` runs inside each KubeVirt (KV) cluster, polls
  `vm_lifecycle_activity_queue(<kv cluster>)` = `vm-lifecycle-activity-<KV_CLUSTER>`, acts on its own
  API server as its own ServiceAccount and dials OUT to the hub's Temporal (gRPC, TLS through the
  Route) — the per-MCE `server-lifecycle-worker` pattern, for the same reason: the queue name IS the
  target, the KV cluster exposes nothing, and nothing on the hub holds a credential for it.
  `KV_CLUSTER` is EXACTLY the cluster's day1 file name and is NEVER canonicalised: the hub builds the
  same queue name from the list it reads in day1. `activities/vm_lifecycle/`, chart
  `deploy/charts/vm-lifecycle-worker/` (copied to redbull-platform by CI), domain
  `workflow_domains/vm_lifecycle/` with FOUR workflows: `create-vm`, on `create-vm-workflow`;
  `kv-zone-lock`, on `kv-zone-lock-workflow`, id `kv-zone-lock-<site>-<zone>` — one long-lived run per
  (site, zone) that hands the zone to one create-vm run at a time; `delete-stray-vm`, on
  `delete-stray-vm-workflow`, id `delete-stray-vm-<kv cluster>-<create-vm run id>` (2026-11-24) — it
  removes a create-vm run's VM from a KV cluster the run moved on from without being able to check
  it (§4); and `delete-vm`, on `delete-vm-workflow`, id `delete-vm-<kv cluster>_<vm name>`
  (2026-11-26) — it deletes ONE VM by name with every volume of it (§4). Those two have NO route and
  no docs page of their own (create-vm starts them, and describes them); delete-vm has NO route
  either — it is to be remove-server-from-mce's child — and its own page. The zone's cluster list is NOT
  this limb's: it is day1's, so `list_kv_clusters` lives on the cluster-lifecycle limb, which holds
  the day1 token (next bullet). create-vm's id is a SLOT of the InfraEnv on its MCE
  (`create-vm-<mce>_<infraEnv>_<slot>`, `shared/workflow_ids.py`): the VM's name is drawn inside the
  run, so nothing known at the edge identifies it — add-server-to-mce's pool-draw shape, with the walk
  itself extracted to `workflow_domains/routers/slots.py` (add-server-to-mce keeps its own until a
  follow-up migrates it: it reports its own item model; what tied it to the busy count,
  `candidate_count`, went with the allocation on 2026-11-17). A slot id is REUSED by later runs, so
  it never identifies a VM or a waiter: the VM's label and the lock's line carry the RUN id.
- **cluster-lifecycle is ONE hub limb holding git tokens only.** provision-cluster reads day1 `main`
  (the shared `activities/day1_repo.py` reader), pushes a branch per cluster, opens the merge request
  through GitLab's API on the day1 repo's host (GitLab is the ONLY provider, §4 — same token as
  the push, `redbull-git-token`) and pushes each day2 sig project's default branch
  with the SAME token — `redbull-git-token` is the redbull GROUP token, so the chart names no second
  Secret (the limb's `DAY2_GIT_TOKEN` stays, empty = that token). No Kubernetes credential, nothing per MCE:
  `activities/cluster_lifecycle/`, queue `cluster-lifecycle-activity`, chart
  `gitops/charts/cluster-lifecycle-worker/`, domain `workflow_domains/cluster_lifecycle/` with ONE
  workflow, `provision-cluster`, on `provision-cluster-workflow`, id `provision-cluster-<stem>`
  (`shared/workflow_ids.py`). The segment comes from the segment-lifecycle limb, through
  allocate-segment as an AWAITED CHILD on its own id — never a second copy of the Segments Manager
  token here (§5). Because it holds the day1 token, it also answers OTHER domains' questions about
  day1 (2026-11-20; the `get_inventory_segment` precedent above — an activity lives where its
  credential is): `list_day1_clusters` (day1's clusters by site, env, kind and zone, for any
  workflow whose own limb has no day1 token) and `list_kv_clusters` (a zone's KV clusters oldest
  first, for create-vm, with the placement buffer — `HEADROOM_BUFFER_PERCENT`, this worker's, §4).
  Both under a "shared with…" header in `shared/interfaces/cluster_lifecycle.py`.
- **upi-lifecycle has TWO limbs: `upi-provisioning` on the hub and `upi-server` INSIDE each UPI
  cluster** (2026-11-27, with the operator). The domain builds and grows the layer UNDER the
  hosted clusters: OpenShift UPI `platform: none` clusters on vSphere, an MCE being one KIND.
  `create-upi-cluster` (queue `create-upi-cluster-workflow`, id `create-upi-cluster-<cluster>`)
  runs on the HUB limb (`activities/upi_provisioning/`, queue `upi-provisioning-activity`, chart
  `deploy/charts/upi-provisioning-worker/`): bootstrap-installer as Jobs in its namespace, each
  cluster's admin kubeconfig in a hub Secret, Argo CD's cluster Secret. `add-server-to-upi`
  (queue `add-server-to-upi-workflow`, id `add-server-to-upi-<cluster>_<server>`) runs on the
  worker INSIDE the target cluster (`activities/upi_server/`,
  `upi_server_activity_queue(<cluster>)`, chart `deploy/charts/upi-server-worker/`) — the per-MCE
  server-lifecycle pattern: its own ServiceAccount, BMC credentials and server-scan token, nothing
  on the hub holding a credential for the cluster. The MCE's day1 folder is the cluster-lifecycle
  limb's (`publish_mce_folder`: it holds the day1 token), the segments allocate-segment's (children).
- **Brain is ONE deployment for every domain**, not one per workflow — the `workflows-orchestrator`
  chart is standalone, deployed once. Each new domain adds `activities/<domain>/` + a
  `gitops/charts/<domain>-worker/` chart folder (plus its `gitops/services/` app.yaml) and registers
  against the already-running brain.
- **Resource naming — `-worker` names the POD, the bare domain names the DOMAIN.** Brain Deployment +
  ServiceAccount = `workflows-orchestrator`; its trigger API = `workflows-orchestrator-api` (reuses the
  `workflows-orchestrator` SA). Each domain's Deployment + SA is `<domain>-worker` — e.g.
  `segment-lifecycle-worker`. The suffix is deliberate: a bare `segment-lifecycle` sitting next to
  `workflows-orchestrator` in the namespace reads as a peer microservice ("the segment lifecycle
  service"), when it is really the activity worker that implements that domain's activities and polls
  its queue. `-worker` says "queue-polling process"; `-activity` would collide exactly with the queue
  name, and `-domain` reads as the domain itself rather than the process. Chart/release/image/Argo-app
  names match resource names, so all of those carry `-worker` too.
- **Domain vs workflow naming — one domain holds MANY workflows.** Anything shared by every workflow
  in a domain is named after the DOMAIN: the activity queue (`segment-lifecycle-activity`), the limb
  ConfigMap (`<domain>-config`), `shared/models|interfaces/<domain>.py`, and the API PREFIX
  `/workflows/<domain>` — NOT the limb Deployment/SA, which are the process and take `-worker` per
  the rule above. Anything belonging to ONE workflow is named after the WORKFLOW: its module
  (`workflow_domains/segment_lifecycle/initialize_segment.py`), its class, its own task queue
  (`initialize-segment-workflow`), its workflow ids (`initialize-segment-<network>`), its
  RunArgs/Progress/Result models, and its ROUTE under the domain prefix. Workflow ids
  MUST carry the workflow name — two workflows acting on the same segment would otherwise collide on
  one id — and, where the workflow's scope includes one, the segment TYPE: allocation is scoped per
  (cluster, site, type), so `allocate-segment-<TYPE>-<cluster>` without the type would make two
  legitimate allocations of one cluster collide. (initialize-segment's id has no type because a
  segment is created without one — §4.) The domain currently holds THREE workflows
  (`initialize-segment`, `allocate-segment`, `release-segment`), each on its own workflow queue,
  sharing the one limb deployment. release-segment's id mirrors allocate's
  (`release-segment-<TYPE>-<cluster>`): it gives back the one allocation a (cluster, type) holds. Id builders live in `shared/workflow_ids.py` — ONE definition per scheme, importable
  from both routers and workflow code. The routers are the only callers today, but the location is
  deliberate: a workflow file can never import a router (FastAPI ≠ sandbox-safe), so the moment a
  workflow needs a sibling's id a router-side scheme would have to be duplicated.
- **API paths are `/workflows/<domain>/<workflow>`; status is `/workflows/runs/{workflow_id}`.** A
  domain is a prefix, never an endpoint: the bare `/workflows/<domain>` must stay free, or the first
  workflow silently claims the whole domain. Status is domain-agnostic ON PURPOSE — workflow ids are
  globally unique, so `workflow_domains/routers/runs.py` serves every domain, and a per-domain
  `GET /workflows/<domain>/{workflow_id}` would additionally swallow every sibling workflow's path.
  That router addresses the `progress` query BY NAME and returns progress/result as decoded JSON:
  a workflow wanting a live progress surface just defines `@workflow.query def progress`; one that
  doesn't degrades to status-only. `GET /workflows/runs?workflow_type=<WorkflowClass>` lists one
  workflow's newest runs (status and times only; the type is pattern-checked because it goes into
  the visibility query). ONE route is synchronous on purpose: `GET
  /workflows/cluster-upgrade/upgrade-targets` runs a short read-only workflow (`upgrade-targets`,
  60 s execution timeout) and returns its answer in the response — a question, not a job, so it
  has no run to poll (DESIGN D22). Every route that STARTS work stays asynchronous.
- **A temporary page may live at `/ui/<domain>`, served by the API it calls** (cluster-upgrade's
  and server-lifecycle's, `workflow_domains/<domain>/ui.py`, until the ClickCluster Platform UI
  exists; each links to the other). ONE self-contained HTML file in the domain folder, nothing from
  another host (production is air-gapped; `tests/test_<domain>_ui.py` checks it), same origin so no
  CORS. It shows only the runs the API reports, and only what a run reports: there is no demo
  mode or recorded data (removed 2026-10-31, the owner found it not needed). Its test checks it
  against the workflow instead, so the page cannot drift from the API: it has words for every
  phase a run reports, and a start form offers only the request fields its test pins. It adds no
  auth: the API has none (§9).

## 3. Deployment-target agnostic

- No orchestrator code knows kind vs OpenShift — all endpoints come from env vars (`TEMPORAL_HOST`,
  `SEGMENTS_MANAGER_URL`, `DAY1_REPO_URL`, ...). Same images run anywhere; only Helm
  `values.yaml` (`config.*`) differs.
- Local kind reaches host services via `host.docker.internal` — that string lives ONLY in Helm
  values, never in code.
- Env naming: a service prefix is used ONLY for that service's own values (its URL, its URI paths) —
  `DAY1_*` and `DHCP_*` are the current examples (`DHCP_API_URL`/`DHCP_API_TOKEN` are that
  service's; `DHCP_EXCLUSION_OCTET_RANGES` predates this rule and is the one exception). Our own
  policy inputs carry no prefix. Do NOT put a
  policy of ours behind a dependency's prefix: it reads as their config and travels with them when
  they go.

## 4. External dependencies are black boxes

- **provision-dell-server (domain `server-provisioning`) takes a Dell from "iDRAC has an IP" to
  "this machine is configured"**, after which add-server-to-mce can draw it. Designed with the
  operator 2026-09-30.
  The pieces that are decisions, not incidental:
  - **Credentials by POSITION, never by value in history.** Root only (iDRAC user 2); the OME
    account on the iDRAC is NEVER touched (OME would lose the machine). Candidates are
    `IDRAC_ROOT_PASSWORD` (the one the template enforces) then `IDRAC_FACTORY_PASSWORDS` (≤2), one
    request each — iDRAC9 blocks an address after 3 failures, so only an all-wrong round trips it,
    and the next round waits `_LOCKOUT_WAIT` out. A machine that came in on a factory password is
    re-discovered in OME with the target one after the template, or OME loses it.
  - **The TEMPLATE is per (Redfish model, iDRAC firmware)** — `DELL_TEMPLATES`, no fallback.
    It sets root's password; the run verifies that by logging in.
  - **Storage: RAID 1 on the BOSS pair, every PERC drive Non-RAID, NEVER a delete.** Policy in
    `storage_plan.py`; staged `OnReset` so ONE reboot applies all; after the template, so the
    layout checked is final. An existing volume or in-use drive fails the run.
  - **The naming service is the author of the name** (it reads OME and rounds its own way);
    `server_namer.py` is the opening for its real contract. The run checks the name read back
    from the OME PROFILE (what server-scan reads) against the convention, the run's region and
    the iDRAC's service tag — never re-deriving cores/memory/disk.
  - **Region comes from the iDRAC prefix at the API EDGE** (`regions.py`, a const), carried as
    input — never looked up on replay.
  - **A server server-scan lists as claimed by a cluster is refused** before anything reboots it.
  - **Dell's own client code is the reference for every OME/iDRAC shape** — the
    `dellemc.openmanage` collection (`docs/design/dell-ome-idrac-api.md` lists each
    fact with its source file). Change a request only against that code, and keep
    `tests/dell_simulator.py` in step: the e2e test runs the real limb against it.
    Traps it found: NVMe RaidStatus lives under `DellPCIeSSD`; a PERC runs Non-RAID
    at once (RealTimeNoRebootConfiguration) so NO reset while a job runs; a
    templated device is `ProfileState > 0` + same `TemplateId`, and its
    `DeploymentTaskId` is the job to wait on.
  - **DONE = CONFIGURED, NOT INVENTORIED. Do not re-add the server-scan wait.** The run ends
    once root is on the enforced password, the template is applied, the storage layout verifies
    and the OME profile carries the right name. It used to end instead at "server-scan lists it",
    polling a Mongo-backed read every 10 min for up to 8 h (`awaiting-server-scan`,
    `_SERVER_SCAN_DEADLINE`, `ServerScanNeverSawServerError`). Reversed by the operator on
    2026-09-30: server-scan discovers by itself on a 6-hourly collection, so the wait added hours
    to every run to observe something the run cannot influence, and made a slow or paused
    collector fail a fleet of correctly provisioned machines. The EARLY server-scan read stays —
    that one refuses a machine a cluster is using, which is a safety check, not a completion one.
    Either way, never ask server-scan to refresh and never call `/servers/available` (live vendor
    recheck).

- **server-scan is the inventory source of record for add-server-to-mce**
  (`SERVER_SCAN_URL`), and it ALLOCATES each run ONE server (server-scan's
  ADR-0063, 2026-11-16; adopted here 2026-11-17, the operator: "every workflow
  will ask for one server, no need for candidate"). `allocate_server` =
  `POST /servers/available` {pattern `^ocp-<flavor>-<size>-<site>-` FROM THE
  INFRAENV'S `spec.agentLabels` (`redbull.io/flavor`, `redbull.io/size`,
  `redbull.io/site`; the operator, 2026-11-22, on the naming convention in
  `Idan-Maimon/kubevirt-workflows` `docs/naming-convention.md`: InfraEnv
  `<flavor>-<mtl|kv<cpu>>-<zone>` = `c200-mtl-az1`, servers
  `ocp-<flavor>-<mtl|kv<cpu>>-<site>-<serial|mac>` — the zone is in no server's
  name, the site in no InfraEnv's, so the NAME cannot be the query any more; do
  not go back to parsing it). `check_infra_env` reports the labels
  (`InfraEnvState.agent_labels`, always emitted, `{}` for none — the limb never
  judges them, publish-infraenv-to-pxe-server needs none); a POOL draw judges them
  in `validating-infraenv` (`agent_label_problems`: each present and ONE lowercase
  `[a-z0-9]+` word) and fails `InfraEnvAgentLabelsError`, every label named,
  before anything is allocated; a named install needs none. The zone label is NOT
  filtered on: server-scan's allocation takes no zone yet (the convention's open
  item 2). Gated on the KEY's presence, not a patch (§5(a)): a limb predating it,
  and every run recorded before, keep the InfraEnv name's FIRST THREE words
  (the operator, 2026-11-19: `c200-mtl-haifa-az1` → `^ocp-c200-mtl-haifa-`,
  `pool_prefix_from_name`, `_POOL_WORDS`) — the allocation's input is not
  compared on replay. The trailing `-` is load-bearing, the operator 2026-11-17:
  `…-five` must not draw `…-five2-…`) | name, `max_health:
  HEALTHY`, `min_nic_macs: 2`, holder, workflow id, mce_cluster, infra_env,
  namespace, `ttl_seconds` = the install TTL}: server-scan draws a candidate,
  LOCKS it for the run, live-rechecks it against its vendor manager WHILE locked
  and returns it, or releases it and tries its next (≤ 5). So the server comes
  back already the run's — no separate reserve, no race. The workflow never
  queries a vendor manager itself: server-scan collects HP OneView / UCS Central
  / Dell OME / Intersight / standalone Redfish on a 6-hour cron — what replaced
  the `bmh-generator-operator` and its four vendor SDKs. It returns DATA ONLY,
  never BMC credentials (this worker's own Secret). Every "nothing reservable"
  is its 503 code `NO_AVAILABLE_SERVER` with `details.reasons` →
  `ServerNotAvailableError` (permanent, the run fails); a 503 WITHOUT that code
  (a proxy) is waited out. Seconds as a rule, ~22 min at its worst (5 × UCS's
  probe deadline): httpx 25 min, `start_to_close` 30 min, heartbeat every 20 s
  under a 2 min `heartbeat_timeout`. NOT IDEMPOTENT — server-scan ties a lock to
  its request by a per-request `claim_token`: a retry after a lost answer takes
  ANOTHER server and the first frees itself at the TTL, as does one locked by an
  allocation a cancel cut off. Gated `add-server-allocates-one-server`; a run
  recorded before still draws a candidate list with `GET /servers/available`
  (`acquire_servers`) — stored data only since ADR-0063, which answers `health`
  with a 400, so the GET sends `max_health` and a 400/422 is the permanent
  `ServerScanRequestInvalidError` (it used to retry forever).
- **ONE SERVER PER RUN — a refused server FAILS the run, never a next candidate**
  (the operator, 2026-11-17). The loop's checks stay (name, BMC vendor/host, a
  BareMetalHost already in a pool run's namespace, PXE site, an older limb's
  in-use refusal), and so do the no-Agent rollback and the lapsed-lock
  teardown; each now gives the allocated lock back (where it is still ours) and
  fails with that one reason's type, the message naming the server
  (`_no_installable_candidate(..., allocated=True)`). A run recorded before
  keeps skipping to its next candidate. `candidate_count` is gone from the
  request (a 422 saying why, `_refuse_candidate_count`); it stays on
  `AddServerToMceInput` for history, quiet at its default.
  **An ALLOCATED server's NICs and MACs are not judged again** (the operator,
  same day: "the server scan already checks it"): server-scan allocates only a
  server whose network health is HEALTHY (two link-UP ports read) with
  `min_nic_macs` read, live-checked while locked. The run still PICKS the two
  bond members — the NMStateConfig's bond and the BareMetalHost's boot MAC are
  written from them — link-UP first in server-scan's order, one per physical
  port (`select_bond_members(..., allow_link_down=True)`, `vouched`); only
  fewer than two ports with a MAC (nothing to bond) is still `no-bond`. A MAC
  the limb cannot use is its `InvalidMacError` (permanent) before the
  BareMetalHost or the NMStateConfig is written. A server the allocation
  refused that the run takes anyway (own leftover lock, `allow_unhealthy`) is
  judged as before.
  **No Agent in the hour → MAINTENANCE, then the release** (the operator, same
  day: "put the server in maintenance and then release the server"):
  `put_server_in_maintenance` (`PUT /servers/{id}/maintenance`, a reason naming
  the run, the InfraEnv and MCE and where the host's state points, ≤ 500 chars,
  read back) in phase `setting-maintenance`, after the teardown and the PXE
  unmap, BEFORE the lock goes back — server-scan's allocation skips a server in
  maintenance, so the next run is not handed a machine that cannot boot into
  the InfraEnv until a person ends it there. Refused (ADMIN token missing, a
  rejected request) the lock is KEPT to its TTL instead and the failure says
  so; a server server-scan no longer knows is released as before. Both ride on
  the `add-server-allocates-one-server` gate (no run of that shape had ever
  run; its two goldens were re-recorded the same day).
- **add-server-to-mce LOCKS the machine it installs in server-scan** (server-scan's
  ADR-0035, `POST`/`DELETE /servers/{id}/reservation`, `reserve_server`/
  `release_server` on the MCE's queue) — taken by the allocation itself since
  2026-11-17, and by `reserve_server` only for a named server the allocation
  refused that the run takes anyway (its own leftover lock, `allow_unhealthy`).
  server-scan lists a machine AVAILABLE until a cluster reports it, while the
  BareMetalHost probe only sees the run's OWN MCE — so without the lock a server
  installed into MCE-A is drawn again for MCE-B. Holder `add-server-to-mce` +
  the run's workflow id, so the run's own renewal EXTENDS it (server-scan matches BOTH, so a run started as
  install-server keeps holder `install-server` to the end — `_reservation_holder`,
  by workflow type); renewed as the Agent wait begins; extended to a day after
  success, and then held ONLY until server-scan knows: the run reads
  `lookup_server_membership` every 5 min (`awaiting-server-scan`, gated
  `add-server-release-once-installed`; the operator, 2026-11-09) and releases its
  lock (`releasing-lock`) once the server is INSTALLED_TO_INVENTORY or INSTALLED —
  server-scan then hides it from every draw itself. Bounded by the day: not seen,
  server-scan unreadable for the rest of it (the reads are best effort,
  `schedule_to_close_timeout` = what is left), or it naming ANOTHER MCE, the lock
  is KEPT to expire and the run still COMPLETES — the Agent proved the install;
  `lock_released` / `server_scan_state` / `lock_detail` on the result. The run
  stays RUNNING meanwhile (~5-15 min: server-scan's agents and nodes jobs run every 5 min,
  matching an Agent by its serial, else its hostname — the BMH's server name), so its slot and a named server's id are
  busy until then. The server leaving the MCE first does NOT free it: remove-server-from-mce
  released it from 2026-10-31 until the operator removed that on 2026-11-17 (below);
  add-server-to-mce otherwise releases it ONLY after a teardown — after the no-Agent one, once the server is
  in maintenance (above) — (a failure elsewhere leaves it to its
  TTL, since half-created resources may still name the machine). A RE-RUN of the
  same named install (same workflow id) after such a failure meets its own lock:
  server-scan refuses a locked machine even to its holder (the GET hid it; the
  allocation answers `RESERVED`), so it failed for two hours on a bare "no
  assignable server" (the 2026-10-31 incident). So a named machine it refuses is read once more
  (`lookup_install_candidate`, gated `named-install-own-lock-rerun`): the run goes
  on when the only reason is its OWN workflow id's lock, and otherwise fails
  saying which (another run's lock, a claim, maintenance, its health). Needs an ADMIN
  `SERVER_SCAN_API_TOKEN`. Gated by `workflow.patched("server-scan-install-lock")`
  so runs started before it replay unchanged — keep the patch until none can
  remain in history.
- **A CANCELLED add-server-to-mce ROLLS BACK** (the operator, 2026-11-09 — it used
  to leave everything in place, "a half-created set shows how far it got"; that
  is reversed, do not restore it). `run` catches the cancel (asyncio.CancelledError,
  or the ActivityError/ChildWorkflowError wrapping it — `is_cancelled_exception`),
  runs `_roll_back_after_cancel`, and RE-RAISES, so the run ends CANCELED, never
  FAILED. What a cancel must undo is recorded BEFORE each call that may lock,
  write or map (`_held`, `_mce_written`, `_pxe_mapped`), because a cancel stops
  waiting for an activity, not the activity: a call it cut off may still land.
  Every activity call is `ActivityCancellationType.ABANDON` (`_ON_CANCEL`): none
  heartbeats, so a cancel REQUEST could never reach one, and sending it raced
  the activity's own completion (ACTIVITY_UNKNOWN, a failed workflow task —
  found by the cancel tests).
  So the rollback first waits `_CANCEL_SETTLE` (one attempt's budget + 30 s),
  then: anything possibly written on the MCE → remove-server-from-mce as an
  awaited child (ABANDON), as a recorded uninstall-server-from-hc's inventory half —
  power off, BMH + NMStateConfig, any Agent, its PXE MACs, verification —
  never reimplemented here, then the lock released HERE unless the child's
  `pxe_failed` is set (gated `add-server-cancel-releases-after-removal`, since the
  child releases no lock from 2026-11-17; a child that still did, `lock_released`,
  is not released twice); when the child finds nothing on the MCE, or nothing
  was written, an IPMI mapping is unmapped and the lock released here. After the Agent (`_installed`) a cancel
  undoes NOTHING: the server stays, the wait for server-scan just stops (the
  operator's choice). Before any lock there is nothing to undo. The rollback
  never raises — a failing step is named in `progress.cancel_rollback` — and
  TERMINATE runs no code, so it never rolls back. Gated
  `add-server-rollback-on-cancel`: a run recorded before it stops as it did.
- **An IPMI server boots from the NETWORK, so add-server-to-mce maps it on the
  site's PXE VM first.** IPMI (a UCS-managed `CISCO` blade; `boots_from_network`
  in `shared/bmc_address.py`) has no virtual media. After the install lock and
  BEFORE the Secret/BareMetalHost, `register_pxe_boot` `PUT /pxe-map/<mac>`
  `{"infra_env": {"mce_cluster", "name"}}` for the BareMetalHost's BOOT MAC
  ONLY to `PXE_MAP_URLS[site]` (bearer `PXE_MAP_TOKEN`). The boot MAC is bond
  member nic1, which `select_bond_members` takes from the FIRST link-up NIC
  server-scan lists — the same MAC is the BMH's `bootMACAddress`, so the PXE VM
  answers exactly the NIC Ironic boots. Decided with the operator 2026-10-19;
  it replaced mapping both bond MACs (a blade whose firmware PXE-boots from the
  other NIC now gets a 404). Runs recorded before `pxe-map-boot-mac-only` map
  and unmap both. The Agent is still matched on BOTH. The MAC chains to the
  InfraEnv's BOOT ENTRY, `/var/www/pxe-map/<mce_cluster>/<infra_env>.ipxe`, ONE
  per InfraEnv. The site is the MCE inventory SEGMENT's. A site with no entry is
  a skip (`no-pxe-site`).
- **The boot entry is published only once the discovery image includes the
  blade's NMStateConfig.** Every NMStateConfig regenerates the InfraEnv's image
  and the image carries only the configs it was generated from, so with
  installs of one InfraEnv in parallel a host could otherwise boot an image
  from before its own static network config. So for an IPMI blade the
  NMStateConfig is created FIRST (before the Secret/BMH that let Ironic power it
  on), then `publish_infra_env_boot` polls (`publishing-pxe-boot`, 10 s, bounded
  10 min) until the InfraEnv shows `ImageCreated=True` and `status.createdTime`
  >= the NMStateConfig's `creationTimestamp`, and only then `PUT
  /pxe-map/<mce_cluster>/<infra_env>` `{"ipxe_url", "image_created_at"}`. The
  PXE VM compares-and-sets on `image_created_at` (§6): older is 409, which is
  SUCCESS for the caller (`superseded` — the newer image includes its config).
  No new image by the deadline fails the run with
  `InfraEnvImageNotUpdatedError` after a full rollback (NMStateConfig, MACs,
  lock) — assisted-service not regenerating is the InfraEnv's problem, not the
  candidate's. Both timestamps are whole seconds; the same-second window is
  harmless because assisted-service builds the PXE initrd per DOWNLOAD from the
  InfraEnv's current static network config (verified in assisted-image-service
  `initrd.go`) and the host fetches it minutes later. A REST check of
  assisted-service's `static_network_config` was ruled out: its plain-HTTP
  listener serves only `ipxe-script` (`WithIPXEScriptMiddleware`). Redfish
  servers keep Secret → BMH → NMStateConfig and never touch the PXE VM.
- The URL never enters history (read and PUT inside the activity). The no-Agent
  rollback DELETEs `/pxe-map/<mac>` (404 = success) between the teardown and the
  lock release — never after the release, and never on the lock-LAPSE rollback,
  because another run may by then have mapped the same MACs to its own InfraEnv.
  An installed host keeps its mapping; the boot entry is shared and never
  deleted by a run. Gated by `workflow.patched` (`pxe-map-ipmi-boot`,
  `pxe-map-rollback-cleanup`, `pxe-map-infraenv-boot` — runs recorded before
  it map the URL straight onto the MACs, `PxeBootRequest.mce_cluster` None —
  and `pxe-map-boot-mac-only`). The PXE VMs' pxe-map-api must be upgraded BEFORE this limb: an old one
  refuses the `infra_env` body (422) and lacks the entry route (404), both
  non-retryable and named in the error. Needs `get` on `infraenvs` and
  `nmstateconfigs` in the limb's RBAC (both already granted).
- **publish-infraenv-to-pxe-server republishes one InfraEnv's boot entry on demand, tied
  to no host.** (`workflow_domains/server_lifecycle/publish_infraenv_to_pxe_server.py`,
  own queue `publish-infraenv-to-pxe-server-workflow`, id
  `publish-infraenv-to-pxe-server-<mce>-<infra_env>` — keyed on the FILE it writes, one
  per (MCE, InfraEnv) — API `POST /workflows/server-lifecycle/publish-infraenv-to-pxe-server`,
  `namespace` defaulting to `inventory`, MCE canonicalised at the edge because
  it is also the entry's directory on the PXE VM.) Site from the MCE's
  inventory segment (`get_inventory_segment`, segment-lifecycle queue,
  cluster re-checked) — the same PXE VM add-server-to-mce maps hosts on; then
  `check_infra_env`; then the SAME `publish_infra_env_boot` activity with
  `server_name=None`, which skips the NMStateConfig comparison and publishes
  the InfraEnv's current image once `infraenv_image_not_ready_reason` passes
  (ImageCreated True, ipxeScript, createdTime), polled 10 s / bounded 10 min →
  `InfraEnvImageNotUpdatedError`. The PXE VM's compare-and-set means it can
  never roll an entry back: a newer image there is `superseded`, success.
  Writes nothing on the MCE.
- **add-server-to-mce checks the InfraEnv exists on the MCE before drawing**
  (`check_infra_env`, phase `validating-infraenv`, gated by
  `infraenv-precheck`): a missing one fails the run with `InfraEnvNotFoundError`
  instead of locking, booting and waiting an hour on every candidate, and a pool
  draw's InfraEnv whose agentLabels cannot name its servers fails
  `InfraEnvAgentLabelsError` (the server-scan bullet above). An
  early-bound InfraEnv (`spec.clusterRef`) is logged, not refused. The API
  defaults `namespace` to `inventory` — on the edge model only, so a recorded
  run's input never changes on replay.
- **An MCE name is accepted with or without its `ocp4-` prefix, and always used
  WITH it** (`shared/cluster_names.canonical_mce_cluster`). Applied on the
  server-lifecycle API's request models and to the worker's `MCE_CLUSTER`
  setting — the two ends of the queue name — and NEVER on a model decoded from
  history, where renaming a recorded run's MCE would change the queue it replays
  against.
- **remove-server-from-mce takes a server OUT of an MCE's inventory — only inventory.**
  (`workflow_domains/server_lifecycle/remove_server_from_mce.py`, own queue
  `remove-server-from-mce-workflow`, id `remove-server-from-mce-<server lowercased>`, API
  `POST /workflows/server-lifecycle/remove-server-from-mce`, `namespace` defaulting to
  `inventory`.) It finds the host's Agents by BMAC's `agent-install.openshift.io/bmh`
  label or by ANY MAC of the machine — from the BMH / NMStateConfig and every
  MAC server-scan holds for the name (`GET /servers?search=` + exact name on
  `/servers/{id}`, never `/servers/available`), which is what finds an Agent
  whose BMH is already gone. Any of the three may be missing. It refuses with
  `AgentBoundToClusterError` — changing nothing — if any is bound
  (`spec.clusterDeploymentName`). Order is load-bearing: the NMStateConfig + BMH
  deleted FIRST (`delete_host_resources`) — NO power-off of its own, NO
  `detached` annotation, metal3's `baremetalhost.metal3.io` finalizer NEVER
  touched — then the BMH read every 15 s until metal3 lets it go (15 min, else
  `BmhDeletionTimeoutError`, the Agent untouched), its BMC Secret deleted only
  then (metal3 needs it), and the Agent LAST — a host still running discovery
  re-registers a deleted Agent within a minute, and metal3 deprovisions a deleted
  host (powers it off, deletes its ironic node) before releasing its finalizer.
  Decided with the operator 2026-11-24, after 2026-11-23: the old power-off
  (`spec.online: false`, `HostPowerOffTimeoutError`) could never pass for a host
  mid-deploy — metal3 ignores `spec.online` while `provisioning` — and the shared
  `teardown_bmh_resources` dropped the finalizer after a fixed 30 s, mid-way
  through a deprovision that takes 30-45 s with a BMC that answers, orphaning
  ironic nodes whose power sync then forced twelve servers' power every minute.
  Do not re-add the power-off, the detach or a finalizer drop here. Gated
  `remove-server-delete-without-power-off`: a run recorded before still powers
  off and tears down. add-server-to-mce's rollbacks (no Agent, a lapsed lock, an
  unpublished IPMI boot entry) do the same since then — delete, wait 15 s /
  15 min, Secret after, never the finalizer, `BmhDeletionTimeoutError` with
  nothing after it (no PXE unmap, no maintenance, no release; the lock runs out)
  — gated `add-server-rollback-keeps-finalizer`; the wait is ONE policy for both,
  `workflow_domains/server_lifecycle/bmh_deletion.py`. `teardown_bmh_resources`
  (detach, 30 s, finalizer dropped) is left only for runs recorded before.
  The delete and the Agent delete each re-check the binding right before acting. It ENDS by re-inspecting until none of the three is left
  (bounded, 5 min): anything still there — typically an Agent re-registered by
  a host with no BMH to power it off through — fails with
  `ServerStillPresentError` naming it, never a success. After the Agent delete
  and before that check (`removing-pxe-boot`, gated `remove-server-pxe-cleanup`)
  it clears EVERY MAC the host is known by (`InventoryHostState.macs`: BMH boot
  MAC + NMStateConfig + server-scan; a limb predating the field falls back to
  `server_scan_macs` by `model_fields_set`) from the PXE VM of the MCE's OWN
  site only (`get_inventory_segment` on the segment-lifecycle queue, cluster
  re-checked; gated `remove-server-pxe-own-site`), with
  `DELETE /pxe-map/<mac>?mce_cluster=<this MCE>`. It used to sweep EVERY VM in
  `PXE_MAP_URLS` and failed there: the worker has ONE `PXE_MAP_TOKEN`, each
  site's VM its own, so every other site answered 401 — install worked, remove
  failed. BEST EFFORT since: no segment, a rejected token/request, or a VM
  unreachable for 10 min (`schedule_to_close_timeout` on both of the step's calls, the
  segment lookup and the delete: the one bounded step in the domain, up to about 20 min,
  because nothing about the removal depends on the PXE VM) goes into
  `pxe_failed` and the run still completes. The PXE
  VM refuses (409, reported in `pxe_kept`, not a failure) a MAC chaining to
  ANOTHER MCE's boot entry and removes anything else — this MCE's entry, a
  pre-boot-entry direct URL, a corrupt file — under the lock MAC writes take
  (compare-and-delete server-side, §6). Without it a removed blade powered on
  again would PXE-boot this MCE's discovery image and re-register. A server
  found NOWHERE on the MCE touches the PXE map neither (nothing ties its MACs
  to this MCE). The InfraEnv's boot entry is shared and never removed. An OLD
  pxe-map-api ignores `?mce_cluster` and deletes unconditionally — upgrade the
  PXE VMs first. It ENDS AT THE CHECK and NEVER reads or releases server-scan's
  install lock — a lock is its holder's (the operator, 2026-11-17: "isn't
  necessary"; do not re-add it). Each caller holding one releases its own after
  this run: a cancelled add-server-to-mce in its rollback, an uninstall-server-from-hc
  recorded before 2026-11-19 once its child completed (a new one starts no child); add-server-to-mce's day-long hold already ends
  once server-scan sees the install (2026-11-09). Gated `remove-server-no-lock-release`:
  a run recorded with `remove-server-release-own-mce-lock` (2026-10-31 — a server
  removed the day it was installed stayed undrawable for the rest of that hold) and
  not the new patch still ends with `releasing-server`: the lock read
  (`lookup_server_membership`) and released ONLY when live, `add-server-to-mce`'s
  and for THIS MCE, never on found=false or a PXE failure, never failing the run
  (`lock_released`, `lock_detail`). Needs `delete` on agents in the limb's RBAC.
- **uninstall-server-from-hc takes ONE server out of its hosted cluster and ENDS AT A
  FREE AGENT, whatever its Agent's state** (`workflow_domains/server_lifecycle/uninstall_server_from_hc.py`, own
  queue `uninstall-server-from-hc-workflow`, id `uninstall-server-from-hc-<server lowercased>`,
  API `POST /workflows/server-lifecycle/uninstall-server-from-hc` — `server_names` ONLY,
  a string or a list of 1-20, each once case-insensitively, plus `mce_cluster`
  canonicalised and `namespace` defaulting to `inventory`. EVERY NEW RUN ENDS ONCE
  THE AGENT IS UNBOUND AND VERIFIED CLEAN, the server left in its MCE's InfraEnv
  (`kept_in_inventory: true`), and TAKES NO server-scan LOCK (the operator,
  2026-11-19: "don't call remove-server-from-mce and finish when it see the agent
  is not bound to cluster"; no lock "because the server not be in available status
  in server scanner on all the steps of the flow"; gated
  `uninstall-server-from-hc-ends-at-free-agent`, `_ends_at_free_agent`). This
  REVERSES 2026-11-02's "every uninstall ends in remove-server-from-mce" — do not
  re-add the child or the lock: a server leaves the MCE through remove-server-from-mce's
  own route. An Agent already free at inspection (an INVENTORY HOST) completes at
  once, nothing written. `keep_inventory` (added 2026-10-24, REMOVED 2026-11-02)
  stays a 422 — nothing is left for it to choose. A run recorded before
  2026-11-19 ends as it began: from 2026-11-02 in the child whatever its input
  (`uninstall-server-from-hc-always-removes`), before that kept in the inventory
  when started with it (`_keep_inventory`, the `keep-inventory` goldens). The
  rest of this bullet down to HOSTED-CLUSTER ACCESS — the inventory host's
  re-inspection, the child, `reserving-server`, `holding-server`,
  `removing-from-inventory`, `releasing-server`, THE LOCK — describes those
  recorded runs only. NO `count`, NodePool or
  HostedCluster field: server-scan and the Agent's binding say where it is. A
  string is `{workflow_id, run_id}` and a 409 while it runs, a list the per-run
  report add-server-to-mce's named list uses (`ServerRunItem`); `UninstallServerFromHcRequest`
  is its OWN model with `run_input()`, guarded by `test_every_run_field_can_be_sent`.)
  Designed with the operator 2026-10-23. The PATH is picked by the BINDING seen
  at inspection, never by the Agent's state string: a HOSTED NODE is an Agent
  bound (`spec.clusterDeploymentName`) with an AgentMachine behind it, OR an
  Agent an AgentMachine's `status.agentRef` still names — the Agent provider
  clears the binding, the `agentMachineRef` label and its namespace annotation
  BEFORE the Machine finishes deleting (it holds the Machine on
  `pre-terminate.delete.hook.machine.cluster.x-k8s.io/agentmachine` until the
  Agent is unbound), so without the agentRef match a re-run mid-unbinding would
  skip the hosted half instead of resuming it. Anything else is an INVENTORY HOST
  and skips that half — a new run then completes; a recorded one is INSPECTED AGAIN, fresh, right before remove-server-from-mce
  (`verifying-agentmachine`, gated `uninstall-server-inventory-verify`; the
  operator, 2026-10-24: "should still verify no AgentMachine, Machine"): a free
  Agent is what ANY NodePool's new AgentMachine takes, so one now bound or
  labelled fails `AgentRetakenError`, any other AgentMachine / Machine /
  control-plane link `ServerStillPresentError`, nothing on the MCE written. A recorded run's every path ends in remove-server-from-mce as an AWAITED CHILD
  (`remove_server_from_mce_workflow_id`, `REMOVE_SERVER_FROM_MCE_WORKFLOW_QUEUE`, ABANDON on parent
  close) — the inventory half, PXE cleanup included, is never reimplemented here
  and remove-server-from-mce's own route keeps working alone; `WorkflowAlreadyStartedError`
  on it is `RemoveServerFromMceAlreadyRunningError` naming the id, its failure
  `RemoveServerFromMceFailedError` carrying the child's type; nothing on the MCE
  completes through the child's `found=false`. ORDER: `checking-inventory`
  (`lookup_server_membership`: `GET /servers?search=` + exact name on
  `/servers/{id}`, never `/servers/available`; refused ONLY on a contradiction —
  unknown `ServerNotInServerScanError`, `contested_with`, another MCE —
  `ClusterMembershipMismatchError`; AVAILABLE is accepted) → `inspecting`
  (`inspect_bound_host`: BMH, Agents by bmh label / any MAC, AgentMachines listed
  cluster-wide, the owning Machine via ownerReferences — deletionTimestamp,
  nodeRef, hooks, conditions — and the HostedCluster; the Node name is captured
  HERE; refused: two bound Agents `MultipleBoundAgentsError`, bound into no
  HostedCluster's control plane with nothing behind it — early-bound ACM/ZTP —
  `NotHostedClusterNodeError` (an ORPHAN binding into a control plane is a
  leftover, next bullet), server-scan INSTALLED naming another cluster or
  INSTALLED_TO_INVENTORY while bound — an unbound Agent contradicts no
  INSTALLED reading, a re-run's normal view) → (`reserving-server`, recorded
  runs only) → the hosted half: `deleting-machine` (`ensure_machine_deleting`, the one write the
  NORMAL path makes, a no-op when it already has a deletionTimestamp or is gone) →
  `awaiting-machine-deletion` (30 s polls of Machine + Agent: `draining` while the
  Agent is bound, 20 min from the first deleting observation, then
  `MachineDrainTimeoutError` naming conditions and pending hooks; `unbinding`
  once released, 1 h to `known-unbound` — add-server-to-mce's bare-metal reasoning —
  then `AgentUnbindTimeoutError`, or `MachineDeletionTimeoutError` when the Agent
  is free and the Machine still stands) → `verifying-agentmachine` (gone, 10 min
  since 2026-11-25 — 5 before, `ServerStillPresentError`) → `verifying-node` (no Node is the server — by
  providerID, nodeRef or hostname, NEVER skipped for a missing nodeRef) →
  `verifying-agent` (no
  binding, no label, known-unbound, approved, BMH present and not in error; 5 min,
  `AgentStillBoundError`) — where a NEW RUN COMPLETES; a recorded one goes on →
  `holding-server` (24 h BEFORE the child, so the hold
  covers the inventory half) → `removing-from-inventory` → `releasing-server`
  (the lock given back, below). Every skip goes into
  `result.skipped`. NOTHING IS UNDONE after the Machine delete — it cannot be —
  and a re-run of the same id RESUMES. NODEPOOL REPLICAS ARE NOT ITS BUSINESS:
  the run deletes the Machine ITSELF (no prerequisite) and the NodePool backfills
  = REPLACE. `NodePool.spec.replicas` belong to the day1 file and change ONLY
  through update-nodepool (decided with the operator 2026-11-03, next bullets);
  never change them here — a shrink is update-nodepool's scale-down. The
  provider's `findAgent` takes ANY free Agent the NodePool matches, so with no
  other free Agent the replacement takes THIS server back: every wait pass checks
  the Agent's `agentMachineRef` label (+ namespace annotation) against the
  AgentMachines the run started from (`HostAgent.labelled_agent_machine`,
  `AgentBindingState.agent_machine`, both gated on key presence) and stops with
  `AgentRetakenError`; `release_agent` refuses an Agent whose labelled
  AgentMachine a live Machine holds (`AgentBindingChangedError`). THE RUN LABELS NO
  AGENT — do not re-add it. From 2026-11-12 (the operator) it labelled every Agent
  of the server `redbull.io/uninstalling: "true"` right after the lock
  (`marking-agent`, `mark_agents_uninstalling`, `UNINSTALLING_AGENT_LABEL`, gated
  `uninstall-server-mark-agents-uninstalling`), for a NodePool whose
  agentLabelSelector excluded it — whose chart half was never built; the operator
  removed the step on 2026-11-17 ("no need the step of the labeling the agent").
  Gated `uninstall-server-no-agent-mark`: a run recorded in between still labels
  (and its failures name the `oc label … -` that takes it off), so the activity,
  its registration and the `patch` on agents stay until none can remain; the
  `patch` also serves `release_agent`. THE LOCK of a RECORDED run (a new one takes none, above; holder
  `uninstall-server-from-hc`, TTL = two drains + unbind + three verifications + 10 min): the
  hosted cluster's nodes job frees the server on its Node's absence and the MCE's
  agents job re-claims it INSTALLED_TO_INVENTORY on its own 5-min cadence, so
  between them it can read AVAILABLE while still in this InfraEnv and be drawn by
  another MCE's install. Taken after the inspection, before any write; a lock
  held elsewhere fails the run (add-server-to-mce's hold on a freshly installed
  machine does, until its run sees server-scan report the machine — usually
  5-15 min, at most a day — or release it in server-scan); on failure left
  to its TTL; a refused 24 h extension is logged and the child still runs.
  Once the child completed with `pxe_failed` empty (found=false included:
  nothing on the MCE), the run RELEASES ITS OWN lock (`releasing-server`, gated
  `uninstall-server-release-after-removal`; the operator's decision,
  2026-10-31 — an uninstalled server could not be installed anywhere for a
  day). Kept on a PXE failure (its MACs may still chain to this MCE's
  discovery image), and on an old run kept in the inventory; a child
  failure never reaches it; a refused release is named in `skipped`, never a
  failure. `lock_released` on the result.
  HOSTED-CLUSTER ACCESS: `HostedCluster.status.kubeconfig.name` in the
  HostedCluster's namespace, the kubeconfig parsed BY THE LIMB
  (`activities/server_lifecycle/hosted_cluster_api.py` — the kubernetes client's
  loader caches cert/key files for the process lifetime; here they live in a
  per-call temp dir) with the server overridden to
  `https://kube-apiserver.<hcp-ns>.svc:6443` (the external route may not be
  reachable from the pod network), TLS verified against its CA, nothing from it
  in history, logs or errors; retryable `HostedClusterUnreachableError`
  (connection, 5xx, 401), non-retryable `HostedClusterAccessError` (no
  kubeconfig, unusable Secret, 403, TLS verification). THE AGENT'S
  `clusterDeploymentName.namespace` IS THE CONTROL-PLANE NAMESPACE, not the
  HostedCluster's: HyperShift creates the AgentCluster — and with it the
  ClusterDeployment — there, named after the HostedCluster, so the HostedCluster
  is found by listing them and matching HyperShift's
  `<ns>-<name, dots as dashes>` against the AgentMachine's namespace
  (`hosted_resources.hosted_cluster_for`). Shapes are taken from
  cluster-api-provider-agent and HyperShift's own code, cited in
  `hosted_resources.py`. Needs, beyond the InfraEnv Roles (plus `patch` on
  agents there), a ClusterRole (`rbac.hostedClusters`): machines and
  agentmachines get/list/delete/patch, hostedclusters get/list and secrets get
  CLUSTER-WIDE — the widest grant the limb holds; the vendored chart in
  redbull-platform needs the same. Never: a NodePool change, a drain or Node
  delete CAPI can still do, anyone's finalizer or hook but the Agent provider's,
  a PXE map write (remove-server-from-mce's), a new deployment.
- **uninstall-server-from-hc CLEARS what a broken HyperShift deletion left, and verifies
  every finalizer's job instead of assuming it** (`workflow.patched(
  "uninstall-server-hosted-leftovers")`; runs recorded before it keep the flow
  above). Decided with the operator 2026-10-24: HyperShift bugs leave an
  AgentMachine + Machine with no Node, a Machine with no AgentMachine, an
  AgentMachine with no Machine, a Node with neither. Each object is cleaned up
  by a finalizer or hook — CAPI drains and deletes a Node ONLY through its
  Machine; cluster-api-provider-agent unbinds an Agent ONLY from its Machine's
  pre-terminate hook (`handleDeletionHook`), drops its AgentMachine finalizer
  WITHOUT the unbind when the owner Machine is gone and keeps it for ever with
  no owner at all — so a broken chain never finishes on its own. Read
  upstream (cited in `hosted_resources.py`): the Machine's and the Node's
  `spec.providerID` is `agent://<agent name>`, the Node's name the Agent's
  hostname. So: INSPECTION finds the Machine by ANY link (owner, an
  infrastructureRef naming the AgentMachine, providerID) and the HostedCluster
  by whatever points into its control plane (AgentMachine, Machine, or a
  binding whose namespace a HostedCluster uses); two Machines / two Nodes /
  two control planes = `ConflictingHostedObjectsError`. `checking-hosted-node`
  (`find_hosted_nodes`, BEFORE the lock) finds the Node that IS the server —
  providerID, or a name (nodeRef, hostname, server name) with no OTHER
  providerID — in the MCE-linked cluster, else the one server-scan says it is
  INSTALLED in; a READY Node nothing on the MCE links is refused
  (`HostedNodeStillPresentError`, nothing to release the host through). Then,
  where the owner cannot act, the run makes ITS write: `draining-node` — a Node
  no Machine stands behind, cordon + Eviction API for all but DaemonSet and
  mirror Pods (CAPI's drain rules), 20 min, `NodeDrainTimeoutError`, BEFORE the
  unbind that reboots the host, never for a NotReady Node; `releasing` — the
  provider's EXACT unbind (`release_agent`: label + namespace annotation,
  machineConfigPool "", ignition token ref + headers, clusterDeploymentName,
  merge patch with the read resourceVersion, `AgentBindingChangedError` for a
  binding elsewhere) when no Machine is left, or the Machine sits at its
  pre-terminate hook with its AgentMachine gone — then, once the Agent is in a
  provider release state, `release_machine_hook` drops the provider's hook;
  `verifying-agentmachine` (`clean_up_agent_machines`, re-derived FRESH: any
  AgentMachine naming the Agent by agentRef or label, a Machine-less one deleted,
  one stuck deleting has ONLY the provider's finalizer dropped after 5 min (the
  operator, 2026-11-25; 1 min before, gated `uninstall-agentmachine-finalizer-grace-5m`,
  the check's deadline 10 min so the drop always gets its turn), only
  once the Agent is released, and only with NO owner Machine at all — one whose
  owner is merely gone the provider drops itself and is left to it (the
  operator, 2026-11-24: a hold is removed only where no controller ever will;
  RE-CHECK the hook and finalizer against cluster-api-provider-agent on every
  provider upgrade, `hosted_resources.py`); any Machine still linked =
  `ServerStillPresentError`, a NodePool replacement taking it again);
  `verifying-node` deletes a Node still there once NotReady (the host is back in
  discovery) and fails on one still Ready. Writes go in `result.cleaned_up`, each
  with why the owner could not; the shape found in `leftovers_at_start`. Needs
  the new limb (new activities) AND the widened RBAC — ship the RBAC first.
- **update-nodepool owns a hosted cluster's NodePool replicas, through day1**
  (`workflow_domains/server_lifecycle/update_nodepool.py`, own queue
  `update-nodepool-workflow`, id `update-nodepool-<cluster>` — per CLUSTER, not
  per kind: every kind shares one day1 file and one Argo CD Application — API
  `POST /workflows/server-lifecycle/update-nodepool` with `mce_cluster`
  (canonicalised), `cluster` (the day1 file's name), `nodepool` and `replicas`
  (0 allowed), plus the optional scale-down fields below; a 409 too while
  `upgrade-cluster-<cluster>` is RUNNING, since a replicas commit under an
  upgrade stops it at its next commit.) Designed with the operator 2026-11-03.
  The pieces that are decisions:
  - **ONE NodePool, by name, ALWAYS** — `replicas` is that NodePool's own, set
    EXACTLY, NO balance rule, whatever its kind or platform, the others
    untouched (by name since 2026-11-08; the ONLY target since 2026-11-18, the
    operator: "no need infraenv kind" — do not re-add the kind). `infra_env_kind`
    (a TOTAL spread over the cluster's NodePools of one InfraEnv kind, the
    selector's InfraEnv `K` or `K-*`, refused `NodePoolsUnbalancedError` when
    already more than one apart, moved one replica at a time by
    `nodepool_replicas.plan`) is a 422 at the edge (`_refuse_infra_env_kind`)
    and stays optional on `UpdateNodePoolInput` and in the workflow ONLY so runs
    recorded with it finish as they began (their goldens replay it); delete the
    by-kind branch and `nodepool_replicas.py` once none is RUNNING. A name on
    neither side is `NodePoolNotFoundError`; on one side only, drift. ONLY the
    cluster's own NodePools (`upgrade_state.nodepools_of`, by
    `spec.clusterName`), never the rest of the MCE.
  - **Live and day1 must agree BOTH WAYS** (same NodePool, InfraEnv, replicas)
    or `NodePoolReplicasDriftError` with nothing written. The one exception is
    the RESUME: day1 already holding the number asked while the MCE lags (a
    child that committed and never synced) goes to the child, whose commit is
    then "already so".
  - **The change is ONE day1 commit, then an Argo CD sync — never a NodePool
    write.** The children, scale-up-nodepool / scale-down-nodepool (own queues,
    ids `scale-{up,down}-nodepool-<cluster>`, awaited, ABANDON, NO route):
    `commit_day1_replicas` (`Day1ChangeKind.REPLICAS`, compare-and-set on the
    blob read; only `nodepool[i].replicas` may change; message
    `nodepool(<cluster>): …`), `sync_argo_application` to exactly that
    revision with only the moved NodePools allowed out of sync, and
    `read_hosted_cluster` read back. DONE = the NodePools show the new
    `spec.replicas` — NOT new nodes Ready (the operator: "when it sees the
    nodepools in the MCE configured to the desired state").
  - **Which servers leave a scale-down — the REQUEST may say** (the operator,
    2026-11-18, reversing 2026-11-03's "the NodePool's, never the request's"),
    the first that applies: `server_names` — EXACTLY those, as many as the
    replicas removed (the parent checks the count before the child starts),
    each matched to its Machine by the limb (`read_scale_down_candidates
    with_servers`: Machine → AgentMachine → `status.agentRef` Agent → its
    `agent-install.openshift.io/bmh` label, else hostname, else Node,
    lowercased — add-server-to-mce names all three after the server); else
    `scale_down_policy`; else the NodePool's own label OR annotation
    `redbull.io/scale-down-policy`; else nothing marked, CAPI picks at random.
    Both request fields at once is a 422 (and `ScaleDownServersInvalidError` in
    the workflow). Both fields are features for OpenShift clusters from version
    4.19 on ONLY (the operator, 2026-11-18) — a DOCUMENTED limit, not enforced:
    no code checks the cluster's version. A run that scales UP or changes nothing IGNORES both and
    lists them in `result.ignored` — never a failure (the operator). Named
    servers rank nothing, so the hosted cluster is not read; they are refused,
    nothing written (`ScaleDownServersInvalidError`), for a wrong count, a
    name twice, a server no Machine of the NodePool is (or two are), a worker
    that sends no `server` key (gated on its presence), and an UNNAMED
    CAPI-unhealthy Machine that sorts before a named one — 4.18's CAPI ties a
    mark with unhealthy and takes the tied ones by name, so it would delete
    that one instead (`scale_down_selection.server_refusal`;
    `test_named_servers_that_pass_are_exactly_what_capi_removes` checks every
    combination at 50, 75 and 80). Plans recorded before carry no
    `server_names` and take the policy path unchanged. The policies:
    `fewest-pods`, `least-requested` (pod requests / allocatable),
    `least-utilized` (`metrics.k8s.io`; no API = `NodeMetricsUnavailableError`,
    a Node without metrics yet is retried); the NodePool's own value unknown =
    `ScaleDownPolicyInvalidError` (unless the request names servers or a
    policy). The mechanism is CAPI's own
    `cluster.x-k8s.io/delete-machine` ANNOTATION (not a label — the CAPI book's
    scaling page says "label", its code and its annotations reference say
    annotation), value = the scale-down's workflow id, set and read back on the
    chosen Machines BEFORE the commit (the operator: "put the label … on the
    machine and just then push to git"); Machines found by HyperShift's
    `hypershift.openshift.io/nodePool: <ns>/<name>` annotation. VERIFIED against
    the sources (2026-11-05), because the ORDER depends on it: HyperShift
    (`nodepool/capi.go`) backs InPlace with one MachineSet and Replace with a
    MachineDeployment and sets NO delete policy, so CAPI's Random applies;
    Random ranks the mark above every healthy Machine everywhere but only TIES
    it with an unhealthy one in 4.18's CAPI (openshift/cluster-api release-4.18,
    upstream ≤1.8; fixed by kubernetes-sigs/cluster-api#11431: 75 in
    release-4.19/4.20, 80 in 1.12), ties by name. So CAPI-UNHEALTHY Machines
    (`machine_selection.capi_healthy` = CAPI's `isMachineHealthy`) go FIRST, by
    NAME, then this run's own mark, then a Node our hosted read sees broken,
    then the policy — and `test_capi_removes_exactly_the_marked_machines_in_every_version`
    simulates CAPI's ranking at 50, 75 and 80 to prove the marked set IS what
    CAPI removes. Do not reorder those tiers. (Named servers are not ranked:
    their refusal above keeps the same promise.) The mark only orders Machines
    inside ONE MachineSet ("only affects MachineSet scale-down"), so a NodePool
    whose Machines span two MachineSets (a Replace rollout) is refused
    (`ScaleDownRolloutInProgressError`). Refused with nothing written too: a
    Machine of a shrinking NodePool already deleting, a mark this run did not
    write (`ScaleDownMarkConflictError`), Machines ≠ replicas. A commit that pushed
    nothing (`Day1FileChangedError`, `Day1ChangeRejectedError`, file not found)
    takes the marks off again; after the commit landed they are NEVER removed.
    Then one heartbeating watch (10 min) that the marked Machines are the ones
    deleting — an unmarked one is `ScaleDownWrongMachineError`. The key is
    written in day1 as an ANNOTATION of the entry (`annotations:`): the
    hostedclusters-setup chart renders those onto the NodePool since 2026-11-08
    (GitHub copy). Not a label — the operator kept the chart's labels to its
    three (`allowDeletion` / `maxReplicas` / `minReplicas`); the run still
    reads a label of the key, for one set by hand. The air-gapped GitLab copy
    renders no entry annotations until it gets the same change — annotate the
    NodePool by hand there, or send `scale_down_policy`.
  - **`maxReplicas` / `minReplicas` are NOT the run's** (the operator,
    2026-11-08): the commit changes `replicas` only. Unwritten, the chart
    derives them from `replicas` on every render, so they follow; written in
    the entry (the pre-slim style), they keep their value. Nothing we know of
    reads them. Do not add them to the REPLICAS edit.
  - **The servers that leave stay in the MCE** as free Agents (the operator,
    2026-11-03) — no remove-server-from-mce child.
  - **EVERY call runs on the MCE's server-lifecycle worker** (the operator,
    2026-11-08: "the update nodepool will be under the server lifecycle
    worker"; `nodepool_scaling.on_mce`, one route). It first borrowed the
    cluster-upgrade limbs — the hub upgrade-gitops worker for day1, the MCE's
    cluster-upgrade worker for NodePools and Argo CD — which meant deploying
    two workers that run nowhere (and upgrade-gitops "is not production",
    above). Now: the day1 half is two server-lifecycle activities,
    `read_day1_nodepools` / `commit_day1_replicas`, calling the hub limb's
    TECHNOLOGY module (`activities/upgrade_gitops/day1_repo.py`) with this
    limb's own settings — the commit refuses any kind but REPLICAS
    (`Day1ChangeRejectedError`), so this token moves replicas and nothing else;
    the NodePool half is cluster-upgrade's `read_hosted_cluster` /
    `sync_argo_application` RE-REGISTERED on this queue (§2's move — their code
    and their ClusterUpgradeActivitySettings, MCE_CLUSTER + ARGO_* defaults,
    unchanged; `test_the_server_lifecycle_worker_borrows_only_…` pins that
    nothing more is borrowed). `DAY1_REPO_URL` / `DAY1_GIT_TOKEN` are OPTIONAL
    on this limb, unlike everywhere else: a worker without them keeps serving
    every other workflow, and update-nodepool fails at its first call with
    `Day1NotConfiguredError` (permanent), nothing read or written — never a
    crash-loop of an MCE's installs over a feature it does not use. The chart
    adds NodePools get/list to the `rbac.hostedClusters` ClusterRole, a Role
    on Argo CD Applications (get/patch) in `argo.namespace`, the `day1.*`
    values, and the image installs git. An older server-lifecycle worker lacks
    the day1 activities (retried until the new one polls) and reports no
    `infra_env` (the run fails naming it, gated on the key's presence).
- **create-vm (domain `vm-lifecycle`) makes ONE Halted VirtualMachine for a KubeVirt InfraEnv, on the
  OLDEST KV cluster of the InfraEnv's zone that has room — the step before add-server-to-mce for an
  InfraEnv whose hosts are VMs. THE ZONE'S CLUSTERS COME FROM DAY1, THEIR NUMBERS FROM THE CLUSTERS
  THEMSELVES, ONLY ONE RUN AT A TIME CHOOSES IN A ZONE, AND THE CHOSEN CLUSTER BUILDS THE VM.**
  Redesigned with the operator 2026-11-19/20 (`docs/design/create-vm-placement.md`, the design, and
  `docs/design/create-vm-placement-plan.md`, both ending in "As implemented"), implemented
  2026-11-20. **This REVERSES the 2026-11-04/05 design, whose decisions were recorded here as "none
  to reopen"** — the operator reopened them, for two reasons: every KV cluster needed two extra
  services, Routes, fleet tokens and the repo's one unverified-TLS exception; and nothing
  coordinated two runs, so parallel creates all picked the same cluster (seen in the 2026-11-05
  lab). What went, and must not come back:
  - **The two agents on each KV cluster — do not re-add them.** `kv-capacity` and `kv-vm-api` (repo
    `kv-agents`, charts in redbull-platform), their Routes, their fleet tokens
    (`KV_CAPACITY_TOKEN`, `KV_VM_API_TOKEN`), the URL templates, `KV_AGENT_CA` (TLS to the agents, not
    verified by default), the vendored wire contract and
    `tests/test_kv_agents_contract.py`, `KvAgentAuthError`, `KvCapacityUnavailableError`,
    `KvVmApiUnavailableError`.
  - **The hub vm-lifecycle limb — do not re-add it.** One release for every zone, its own day1
    clone (`activities/vm_lifecycle/day1_repo.py`), queue `vm-lifecycle-activity` polled on the hub.
  - **Best-fit ranking across the zone (`rank_kv_clusters`, `KvPlacement`) — do not re-add it.** It
    read every cluster at once and took the fullest-after; the order is now oldest first, first fit
    (AD28: the active cluster fills before the standby one), and later clusters are not read.
  - **The run drawing the VM's suffix — do not re-add it.** `workflow.uuid4()`, `known_macs`, the
    MAC-tail check, `suffix_hex`, `_MAX_DRAWS` in the workflow and `VmNameExhaustedError`: the run
    never sees a MAC until the cluster's worker answers with it.
  - **A fleet-wide `VM_MAC_PREFIX` (`02:a1:b2`) and the 30% buffer on the vm-lifecycle limb.**
  - **The 2026-11-04 naming** (`<conf>[-<letter>]-zone-<zone>-<cpu>` / `…-metal`, seed
    `ocp4-seed-<site>-<zone>`, KV clusters `ocp4-kubevirt-<site>-<zone>…`, VM
    `ocp-<infraenv>-<6 hex>`). No VM of that shape was ever created in production (the agents were
    never deployed), so nothing is patched: the workflow was rewritten without
    `workflow.patched()`, its goldens deleted, and any run of the old shape is terminated by the
    operator before the brain is replaced.

  The decisions now, with the operator unless marked otherwise:
  - **THE NAMING CONVENTION says everything, parsed ONCE at the API edge** (`shared/infraenv_names.py`,
    `shared/cluster_names.py`) and carried in the input, never re-derived on replay (the
    provision-dell-server region precedent). It is the platform convention,
    `Idan-Maimon/kubevirt-workflows` `docs/naming-convention.md` — out of draft 2026-11-22 and THE
    source of truth: the `naming-convention` skill fetches it before anything is named, and where
    this file disagrees, the convention wins. A tenant cluster or an MCE is
    `ocp4-<lifecycle>-<flavor>-<site>-<id>`; a SEED or a KV cluster also carries its zone,
    `ocp4-<lifecycle>-<seed|kv>-<site>-<zone>-<id>` (`ocp4-prod-seed-five-az1-a`,
    `ocp4-prod-kv-five-az1-a`); `<id>` a letter given in creation order, flavors `mce`, `seed` and
    `kv` reserved. An InfraEnv is `<flavor>-mtl-<zone>` (bare metal) or `<flavor>-kv<cpu>-<zone>`
    (KubeVirt: `<cpu>` vCPUs), the zone `az<N>`, the flavor carrying its optional variant letter, and NO SITE: an
    InfraEnv belongs to one MCE, so the run's SITE and LIFECYCLE come from the MCE's name. A VM is
    `ocp-<flavor>-kv<cpu>-<site>-<serial>` (`ocp-c200-kv32-five-k7q2m9xd`; until 2026-11-25 the
    convention's `<its MAC, 12 hex>`, below — a divergence from the convention, still to be
    raised with its owner, not changed there): the zone is a label, never in the name. Its size LABEL is `redbull.io/size: kv<cpu>` (`kv32`), the
    value the InfraEnv's Agents carry (the operator, 2026-11-22). THE VM'S MEMORY IS NOT THE
    CONVENTION'S — it names the size by vCPUs only and defines no memory. The ratio is this
    repo's, `MEMORY_GIB_PER_CPU` in `shared/infraenv_names.py`, the ONE place it lives:
    **`<cpu> × 7.5` GiB** (a fraction, `(15, 2)`: `cpu × 15 // 2`, integers only), so a KubeVirt
    InfraEnv's vCPU count is EVEN, 2..256 — an odd size is refused BY THE EDGE PARSER ONLY (a 422);
    a VM's name and the figures still read any size, VMs created before the rule included.
    Decided with the operator 2026-11-24, in the code since that day (it was × 8): measured on the
    R660 KV nodes, at 8 every size was one VM short per node of what its CPUs allow
    (`docs/design/create-vm-placement.md` §6.1.1; memory overcommit rejected, do not re-add it).
    Computed for the node types in use, 8 GiB per core with 128 and 192 CPUs (16/8/4/2 and
    24/12/6/3 VMs of kv8/16/32/64 per node; `TestTheDesignsNodeTable` runs the fit rule on both):
    ANOTHER node type needs that table recomputed BEFORE it joins a KV cluster — at 64, 184, 200,
    216 and 232 CPUs the same model leaves a size one VM short. REVERSED on 2026-11-22, when the
    convention left draft — do not restore: the seed `ocp4-seed-<site>-<zone number>` (the
    operator's 2026-11-20 choice against the draft), the KV cluster without its zone
    (`ocp4-prod-kv-five-a`), the InfraEnv with its site (`c200-kv32-five-az1`) and the route's
    "MCE at the InfraEnv's site" 422 that came with it, and the size label `<cpu>c` (2026-11-21).
    Nothing of those shapes ran in production; the lab goldens replay unchanged (the workflow
    never parses a name). VOCABULARY the operator insists on: "day1" names only the REPO; a seed
    cluster is a "seed", never an "mce"; a KubeVirt cluster is a "KV cluster"; in day1 the two
    kinds are `hc` and `kv`.
  - **The zone's clusters are the FILES of its seeds' folders, of the MCE's LIFECYCLE**,
    `sites/<site>/mces/ocp4-<lifecycle>-seed-<site>-<zone>-<id>/hostedClusters/` at day1 `main`
    (a zone may have several seeds, `-a`, `-b`: their clusters are ordered together; a prod MCE's
    VMs go on prod KV clusters only — the operator, 2026-11-22), read by the
    **cluster-lifecycle limb** (`list_kv_clusters`, §2: it holds the day1 token) through the `Day1`
    query library (`activities/day1_repo.py`: `Day1.kv_clusters(site, zone, lifecycle)`) on the
    snapshot of `main` cached by commit (§5), sorted by the name's `id` letter: OLDEST FIRST. The
    run carries `lifecycle` (`CreateVmInput`, None only in a run recorded before it, which the limb
    refuses with `Day1QueryInvalidError` rather than guess). `kv` is decided by
    FOLDER: the seed folders sit beside the MCE folders in `sites/<site>/mces/`, and only a file
    under a folder whose name parses as a seed's (`seed_of`) is `kv`; a file under any other folder
    there, an MCE's, is `hc` — never by name (day1 has `ocp4-test-kubevirt-site4` under an MCE, an
    ordinary `hc`) and never by `platform: kubevirt`. A file is USABLE only when day1 CONFIRMS it
    for the zone: every NodePool labels its nodes `topology.kubernetes.io/zone` with the seed's zone,
    the name is `ocp4-<lifecycle>-kv-<site>-<zone>-<id>` and its site, zone and lifecycle are the
    seed folder's, and the seed is of this site (the convention's §4.b: the zone in the name, the
    seed's and the NodePools' must agree) (`ClusterRecord.blocking` / `confirmed_zone`;
    `KvClusterEntry.zone` and `.id`; `create_vm.usable`). Any other file is a `day1-problem` row and
    NEVER read. **Judging day1's values is not create-vm's job**: a YAML
    warning (a key written twice in any value layer — `sites/configValues.yaml` puts it in every
    record) is in `problems` and blocks NOTHING; only `blocking` (the zone label, the name, the
    site) takes a file out, because placement depends on those. The
    list is read BEFORE the lock, so the locked part stays short, and carries the commit
    (`day1_revision` in the result). The zone lock stays per (site, zone), whatever the lifecycle:
    runs of two lifecycles choose among disjoint clusters, and only wait for each other.
  - **ONE RUN AT A TIME CHOOSES AND CREATES IN A ZONE: the zone lock**
    (`workflow_domains/vm_lifecycle/kv_zone_lock.py`, `KvZoneLockWorkflow`, §5). The run takes its
    zone's lock, reads figures, chooses, has the VM created, and **releases as soon as the VM OBJECT
    exists** (and on every other path). The next run's figures then already count that VM, as a
    stopped VM — so NOTHING IS REMEMBERED BETWEEN RUNS: there is no pending list, no reservation,
    and server-scan takes no part (`docs/design/kv-placement-server-scan.md` was rejected
    2026-11-17). The lock holds only the line and the lease; it never reads a cluster. **The lease
    is 5 minutes**: a holder that died is passed over at its end, its late release ignored. The
    10-minute disk wait happens AFTER the release, in parallel with every other run.
  - **The create can never land after its deadline.** The run sends `create_before` with the create
    and the cluster's worker creates nothing after it (`VmLeaseExpiredError`, permanent, "nothing was
    created") — though a retry still returns the VM it already made, whatever the time (the lookup
    comes first). `create_before` is `_CREATE_ON_ONE_CLUSTER` (**90 s**) from the create's start,
    or the lease's end less `_LEASE_GUARD` (**45 s**, the lead's number, to confirm: 25 s for one
    API call in flight when the worker checks, 20 s for the hub's and the KV cluster's clocks to
    disagree) if sooner. So a create cannot add a VM to a cluster the next run measured without it.
  - **A CLUSTER THAT FAILS THE CREATE IS LEFT, NOT THE RUN** (the operator, 2026-11-24: "i don't want
    to fail the workflow, but to move to the next kv cluster. the only thing to make sure for is if
    the vm was already created in the kv cluster that afterwards failed so there won't be 2 vms").
    Until then a create that kept failing (an API outage, the worker gone) held the zone to the
    lease's end and failed the run `VmLeaseExpiredError`, and a refusal (namespace missing, a 403,
    every MAC taken, a 400/422) failed it at once — never trying the next cluster. Now ANY create
    failure on a cluster — retried to the deadline, or refused — is a `create-failed` row and the
    scan goes on (phase `checking-kv-cluster`, then the next cluster, under this lease or the next);
    only when every cluster has failed or does not fit does the run fail, `NoKvClusterFitsError`.
    NEVER TWO VMs: before leaving the cluster the run FENCES it — waits until 45 s after the create
    was over (its deadline, or the refusal), after which nothing of the run can still land there —
    and CHECKS it, sending the same create with a deadline already past, which the worker answers
    with the run's VM if one is there and can create nothing (`create_vm._after_a_failed_create`):
      * a VM is there: the create landed after all. The run KEEPS it and goes on with it.
      * none is there (`VmLeaseExpiredError`, or `KvClusterMismatchError` — a worker of another
        cluster refused it before anything): the cluster is clean; next cluster.
      * no answer within `_CHECK_DEADLINE` (1 min): nobody can know. The run goes on all the same
        (the operator's choice, 2026-11-24, of three — "move on, clean up later", over waiting for
        the cluster, or the run waiting to clean up) and starts the ABANDONed child
        `delete-stray-vm` (`delete_stray_vm.py`), which deletes every VM labelled with the run's
        id on that cluster, retried without bound and with no schedule-to-start limit, so it waits
        for the cluster for as long as the outage lasts; a 403 or another cluster's worker fails
        it, naming the label. For that outage the run's VM may exist on BOTH clusters — the
        failed one's Halted, used by nobody, counted against that cluster — and the run's result
        lists the cleanup (`stray_cleanups`). A run that fails after leaving one says so too.
    A cluster with its API down costs a run about 3¼ minutes (90 s + 45 s + 1 min), not 5.
    Tested: `TestAClusterThatFailsTheCreate`, the worker-gone case on a real dev server (the
    test server never times out an activity whose worker died mid-attempt); goldens
    `moved-on-after-a-refused-create`, `kept-the-vm-that-landed`, `moved-on-from-a-dark-cluster`,
    `tests/histories/delete_stray_vm/`. `draws-exhausted` and `lease-expired-at-the-worker` were
    re-recorded and the lab's outage golden dropped (the branch had run nowhere but the lab, so
    no `workflow.patched()`). The disk wait is unchanged: a VM that exists is waited for on its
    own cluster, and removed there (the 10-minute rollback).
  - **Oldest first, FIRST fit, the clusters after it NOT read.** The fit rule is
    `workflow_domains/vm_lifecycle/placement.py` (pure, beside the workflow, the `bond_selection.py`
    precedent), on ONE cluster's figures, in this order: `unreachable` (the worker could not read
    its cluster), `no-nodes`, `fit-node` (no single node has room for the VM's CPU AND memory — the
    largest free CPU and the largest free memory can be on two nodes), `fit-buffer` (requested +
    the VM past `1 − buffer` of the allocatable, CPU then memory). In INTEGERS (millicores, bytes,
    percent): `(requested + vm) × 100 <= allocatable × (100 − buffer)`, so it is exact and replays
    identically. The run adds `day1-problem` and `no-worker`. Every reason is a ROW of the run's
    table (`SKIP_REASONS`, one dict, a test keeps the workflow from recording a reason by a literal),
    never an exception. The run never chooses a node. THE SPLIT IS ON PURPOSE: a worker only knows
    its own cluster and only MEASURES; "the oldest that fits" is decided by whoever sees them all.
  - **The buffer is ONE value for every KV cluster, 15% by default**: `HEADROOM_BUFFER_PERCENT`
    (ours, unprefixed, 0..90) on the hub `cluster-lifecycle-worker`, handed to the run with the
    cluster list, so a run records the buffer it used and a changed value applies to runs started
    after it. Per cluster, never per node. NOT a setting of each cluster's worker.
  - **The figures** (`read_kv_cluster_figures`, `activities/vm_lifecycle/cluster_figures.py`, pure):
    allocatable over the USABLE nodes; unfinished pods' requests as the kube-scheduler counts them
    (init and sidecar containers, overhead, a pod-level request); STOPPED VMs — labelled
    `redfish-enabled=true`, in ANY namespace, with ANY run strategy, whose VMI is not
    Scheduled/Running/Unknown — at what their pod WILL request; each usable node's headroom as a
    CPU and memory PAIR. A VM's size in request terms (CPU `vCPUs × 15 // 16` floored at ONE core,
    memory the guest's plus KubeVirt's overhead, `300 MiB + 8 MiB × vCPUs + guest ÷ 512`) is
    defined ONCE, in `shared/vm_requests.py`, read on both sides. The overhead is MEASURED, not a
    pad (2026-11-24; four launcher pods on cluster-qcl8n, the formula never under a pod's request
    and at most 10 MiB over): the flat 1 GiB it replaced under-counted from 32 vCPUs up (kv64's
    is about 1.8 GiB), so placement could approve a VM whose pod then had no node. No
    `workflow.patched()` came with it: every golden replays unchanged, and no run of this
    workflow's shape had run outside the lab.
    Every number an integer; `parse_quantity` at the worker and nowhere else. An unreadable cluster,
    or a request naming another cluster than the worker's own, is `readable: false` — a VALUE,
    never an exception: the caller holds the zone's lock and must neither retry nor wait. A
    "usable" node is Ready and not cordoned (the design) AND carries no `NoSchedule`/`NoExecute`
    taint AND matches `VM_NODE_SELECTOR` when the cluster has one (the lead's additions, to confirm:
    a node a VM's pod cannot go to is not room; stopped VMs are still counted cluster-wide, which
    errs on the full side).
  - **The cluster's worker builds the VM** (`vm_spec.py`, `vm_operations.create_vm`). The run sends
    only what the VM IS — flavor, cpu, memory, site, zone, MCE, namespace, its id, `create_before`.
    The worker draws a SERIAL — 8 random lowercase letters and digits (`draw_serial`, uniform) —
    names the VM `ocp-<flavor>-kv<cpu>-<site>-<serial>`, writes the serial as its SMBIOS serial
    (`firmware.serial`, as a bare-metal node's name carries its serial; the Agent's inventory
    then reports it, while kubevirt-redfish reports no SerialNumber today, so server-scan still
    matches a VM's Agent by its hostname), and creates it with NO MAC. The worker refuses a request for
    another cluster or another site (`KvClusterMismatchError`). A name any VM of the cluster has,
    in any namespace (Kubernetes refuses a duplicate only within one), or an AlreadyExists, is
    drawn again BY THE WORKER, 3 draws (`MAX_DRAWS`), with no round trip to the hub; then
    `VmSerialExhaustedError`. The VM read back must carry the MAC KubeMacPool gave it; one
    without is DELETED, with its root disk, and the create fails `VmMacNotAssignedError`
    (permanent; the run leaves the cluster) — a BareMetalHost needs a fixed boot MAC. A name is
    unique on its cluster by the check, and across a site's clusters by chance: 36^8 = 2.8
    trillion serials, about 1 in 55,000 that two of 10,000 VMs of one flavor and size at one site
    share one; a clash fails add-server-to-mce on the existing BareMetalHost, loudly.
  - **THE MAC IS KUBEMACPOOL'S, ASSIGNED, NEVER REQUESTED** (the operator, 2026-11-25: "i don't
    mind to let kubemacpool to pick the mac - but i need identifier for the vm name"; "go with
    the 8 char serial"). REVERSES 2026-11-20's "MAC uniqueness NEVER relies on KubeMacPool"
    (the worker's own check of a drawn MAC against every VM of its cluster) and the per-cluster
    `VM_MAC_PREFIX` (`0e` + the cluster's VLAN ID, REQUIRED in each worker's ConfigMap) — both
    gone, so the worker chart carries no per-cluster MAC value and can be deployed from day2's
    `defaults/`. WHY ASSIGNED: KubeMacPool does NOT refuse a duplicate REQUESTED MAC, inside its
    range or out (tested on cluster-qcl8n 2026-11-25, below); it keeps unique only the MACs it
    assigns. VERIFIED the same day there: two VMs of the worker's own body (no MAC,
    `firmware.serial` set) were admitted and given `02:b0:96:22:25:eb` and `…:ec`, already in the
    create response. So every KV cluster MUST run KubeMacPool and the VM namespace must not be labelled
    `mutatevirtualmachines.kubemacpool.io=ignore`. NOT YET SOLVED, on purpose: uniqueness
    ACROSS clusters — each keeps the random `02:xx:xx` /24 KubeMacPool picked (cluster-qcl8n:
    `02:b0:96`), and two clusters can share one. The range is to be set per cluster in the
    HyperConverged object (`spec.networking.kubeMacPoolConfiguration`), with the operator, later
    — e.g. from a hash of the cluster's name plus a collision check over the names in git. For a
    few hours that day the worker drew a MAC inside KubeMacPool's range and requested it, with a
    `KUBEMACPOOL_NAMESPACE` setting and a Role to read the range: never shipped, removed by the
    test above. Otherwise still plain `kubevirt.io` and `cdi.kubevirt.io` only.
  - **A run is known by its RUN ID, not its workflow id** (the lead's change to the design's
    `request_id`, forced by the slot scheme: a slot's workflow id is reused by later runs, and
    holds characters a label value cannot). The VM is labelled `redbull.io/created-by: <run id>`
    (the workflow id is the annotation `redbull.io/workflow-id`); the lock's line carries
    (workflow id, run id) and a grant is signalled to exactly that run. A retried create finds its
    own VM by the label and returns it (`created` False) instead of drawing a second one.
  - **One create attempt ends inside its timeout, and a run holds ONE VM** (the lead's, after a
    review found two attempts could overlap). An attempt starts no call to the cluster's API past
    `CREATE_BUDGET_SECONDS` (55 s; one call is at most 25 s), so it is over before the 90 s
    `start_to_close` lets Temporal start the next beside it, and before a cancel's settle wait
    ends. After the create the VMs carrying the run's id are READ BACK (§6): more than one — a
    create the API server finished after its caller gave up — converges on the OLDEST, whichever
    attempt meets them, and the rest are deleted.
  - **A run that does not complete leaves no VM.** The root disk unbound after 10 min
    (`await_vm_ready`, ONE heartbeating activity, every 8 s — the operator, 2026-11-05 — its clock
    in the heartbeat details): `delete_vm_on_cluster`, then `VmRootDiskNotBoundError`, which says a
    WaitForFirstConsumer storage class never binds a Halted VM's disk. A wait that fails for good
    (403, a mismatch): the same removal, bounded, then the wait's own error. Why delete: a re-run
    of the slot is a NEW execution with a NEW serial and MAC, so it cannot converge on the orphan, and a
    stopped orphan is counted against its cluster for ever. The delete finds the VM by the run-id
    label and deletes by the UID it read; with the name and MAC it also checks them
    (`VmIdentityConflictError` for a VM that is someone else's, left alone).
  - **A CANCEL ROLLS BACK** (the lead's addition, to confirm; add-server-to-mce's precedent):
    waiting, the run leaves the line (`withdraw`); holding, it releases; after the create was
    sent, it removes the VM by its run-id label — after `_CANCEL_SETTLE` (2 min) when the create's
    answer was cut off, since a cancel stops waiting for a call, not the call — and ends
    CANCELED. The rollback's delete is bounded (10 min, a caught `schedule_to_close_timeout`) and
    never raises; `progress.cancel_rollback` says what it did. TERMINATE runs no code: the lock
    passes on at the lease's end, and a VM is found by its label. A workflow RESET to before the
    create is a new run id and makes a second VM (a known limit).
  - **The lease also bounds the scan** (the lead's addition, to confirm; the design left it open).
    A cluster with no worker costs 30 s, so ten are a whole lease. The run never acts on figures
    once its lease is spent: it checks the clock before every read and before the create, gives a
    spent lease up, asks for the lock again and carries on FROM THE CLUSTER IT HAD REACHED, which
    it reads afresh — at most `_MAX_LEASES` (3), then `KvZoneLeaseExpiredError`
    (WORKFLOW-RAISED). The lock wait itself is bounded at 30 min (`KvZoneLockTimeoutError`), and a
    waiting run asks again every 2 min, which is what brings it back if a person terminates the
    lock workflow.
  - **Timeouts, each a decision.** `schedule_to_start_timeout` on the day1 list only (5 min, the
    short-run "no worker" signal, nothing held yet). The figures read: 30 s to be picked up — none
    is a `no-worker` ROW, Temporal never retries that timeout — 60 s to answer (the worker cuts its
    four list calls off together at 20 s), and 120 s `schedule_to_close` as a whole (the lead's, to
    confirm: whatever keeps a read from answering, a worker of a wrong image included, the cluster
    ends as an `unreachable` row instead of holding the zone). The create: `schedule_to_close` at
    its own deadline (90 s at most; the run then checks the cluster and moves on, above), and the
    check after a failed create 1 min — both NO schedule-to-start, a worker that restarts is
    waited for inside them. NONE on the wait or the delete: once the VM exists, a worker pod away
    for five minutes must not fail the run, or its rollback, and orphan the VM (§5).
  - **THE VM IS THE PLATFORM'S `vm.yaml`** (redbull-platform, the operator 2026-11-21: "vm.yaml
    is the target"): the same body, network included (multus `openshift-cnv/vm-external-network`
    unless the cluster's NAD has another name). What the worker adds on purpose, and only this:
    the namespace, the labels beside `redfish-enabled` and the workflow-id annotation, the
    SMBIOS `firmware.serial` (the name's serial; since 2026-11-25 — the worker adds NO
    `macAddress`, KubeMacPool writes one in at admission), and a CPU request floored at one
    core where vm.yaml's integer `cpu × 15 / 16` gives 0 for one vCPU.
    `tests/test_vm_spec.py::test_the_body_is_the_platforms_vm_yaml` pins it field by field.
  - **End state Halted** (`runStrategy: Halted`, the redfish label): kubevirt-redfish owns the power
    state and Ironic starts it; the worker's RBAC has no verb that starts, stops or patches a VM.
    **The VM namespace is add-server-to-mce's `namespace`, verbatim** (default `inventory` on the
    edge only), a kubevirt-redfish chassis namespace the platform created — never created here;
    missing is `VmNamespaceNotFoundError`. The cluster's VM values are the worker's own:
    `VM_STORAGE_CLASS_NAME` (REQUIRED: it must bind Immediately and allow ReadWriteMany +
    Filesystem — ceph-rbd refuses that pair, found in the 2026-11-05 lab), `VM_NETWORK_NAME`
    (the NAD's `<namespace>/<name>`, default `openshift-cnv/vm-external-network`: the VM's network
    is ALWAYS multus and only its name is a value — never the network's type. A map value with a
    chart default was MERGED by Helm with the lab's `pod` into a network of two types, which
    KubeVirt refused on every create, 2026-11-20; a scalar cannot merge),
    `VM_NODE_SELECTOR`, `VM_REDFISH_LABEL`, and `VM_ROOT_DISK_SIZE` (the lead's, default `120Gi`).
  - **kubevirt-redfish runs `systemIdConvention: legacy`** (the operator, 2026-11-05): the Redfish
    system id IS the VM name, so the id, the VM and the server name server-scan lists are one
    string, now `ocp-<flavor>-kv<cpu>-<site>-<serial>`. add-server-to-mce's pool match holds for
    it again: since 2026-11-22 it draws `^ocp-<flavor>-<size>-<site>-` from the InfraEnv's
    agentLabels (the server-scan bullet), which for a KubeVirt InfraEnv labelled
    `redbull.io/size: kv<cpu>` is exactly this VM's prefix; the zone is not filtered on yet (the
    convention's open item 2). Its InfraEnv-name fallback (the first three words, 2026-11-19, for a
    limb predating the labels) matches no VM: under the convention the name has no site. The
    KubeVirt branch, when built, draws no pool anyway (the install puts exactly that VM in).
  - **Deploy order**: terminate any old-shape run → redbull-platform's cleanup (the hub limb's
    `gitops/services/` entry, the two agent charts) → the brain → the cluster-lifecycle limb (the
    listing; `HEADROOM_BUFFER_PERCENT` has a default, so the chart need not ship first) → the
    chart sync → the vm-lifecycle image. Per zone its seed folder in day1; per KV cluster, at its
    creation, its day1 file (NodePools labelling the zone) and, through day2, the
    `vm-lifecycle-worker` chart (`kvCluster` = the day1 file name, the Temporal Route and CA, the
    storage class, the network; no MAC value since 2026-11-25). A cluster in day1
    without a running worker is skipped as `no-worker`. add-server-to-mce's KubeVirt branch
    (starting create-vm as an awaited child when the InfraEnv is KubeVirt, AD29) is the operator's
    LATER step; the input and result are shaped for it, and NO branch exists yet.
  - **What the lead added beyond the reviewed plan, awaiting the operator's confirmation** (each is
    marked above): the 45 s lease guard; the lease re-acquire, `_MAX_LEASES` and
    `KvZoneLeaseExpiredError`; the cancel rollback and the delete by run id; usable nodes without
    repelling taints and under `VM_NODE_SELECTOR`; the figures read's 120 s bound;
    `VM_ROOT_DISK_SIZE`; `KvClusterMismatchError`; the create budget and the read-back; the
    brain's `temporalio>=1.32` floor (`workflow.cancellation_reason()`, §5); and `flavor` REQUIRED
    on the history-decoded `CreateVmInput` (against §5's "defaults to None" rule, because no run
    of the old shape can be replayed by the rewritten workflow anyway).
  - **How it is tested without dozens of KV clusters**: `tests/kv_fleet.py` gives every fake KV
    cluster an in-memory Kubernetes API (with the surface of `kubevirt_api.KubevirtApi`) and its
    OWN worker on its OWN queue, running the REAL limb code (`vm_operations`) under the REAL
    workflows — only the cluster behind the API is fake, and a VM created there IS in the next
    figures. `tests/manual_time.py` moves the test server's clock by hand (§5). Goldens for both
    workflows are recorded with the production waits. `python -m tests.kv_fleet` attaches fake
    clusters to a real Temporal next to a real cluster's worker. The labs' notes, manifests and
    day1 mocks are NOT in the repo (`docs/lab/` is git-ignored, kept by whoever ran them; dropped
    from the remote 2026-11-25); what they found is below, and their histories are goldens.
    **The 2026-11-20 lab** (cluster-qcl8n) ran THIS design on a real
    cluster through Argo CD: one real KV cluster's worker plus seven fake clusters in a pod, the
    real brain, Temporal and a day1 mock. What it found or confirmed: the worker's figures equal the
    nodes' own accounting to the millicore and the byte; a create is Halted with its disk bound in
    about half a minute; ten runs on one zone are placed one at a time and the rest fail
    `NoKvClusterFitsError` (a cluster in day1 with no worker costs each run 30 s); a run survives
    its cluster's worker pod, the brain pod and Temporal's frontend and history pods being killed
    under it; a cancel during the disk wait and the 10-minute rollback on a WaitForFirstConsumer
    class both leave no VM (its DataVolume sits in `PendingPopulation`); and KubeMacPool on
    OpenShift Virtualization did NOT refuse a duplicate MAC on Halted VMs (MACs of `0e:`, outside
    its range). RE-TESTED 2026-11-25 on the same cluster INSIDE its range (`02:b0:96:fe:fe:01`,
    two Halted VMs in `inventory` and one in `default`): all three created, KubeMacPool's log
    marking the MAC "allocated" for each — it does not refuse a duplicate REQUESTED MAC at all,
    in or out of its range. Seven of its histories are goldens
    (`tests/histories/{create_vm,kv_zone_lock}/lab-cluster-qcl8n-*.json`).
    **The 2026-11-24 lab** (the same cluster, "the second run") ran the branch with
    the convention's names and 7.5 GiB VMs APART from the platform's own release there — its own
    Kubernetes namespace and its own TEMPORAL namespace, because that release's brain polls
    `create-vm-workflow` in Temporal's `default` with `main`'s other definition, and two
    definitions on one queue split every run between them. A lab or a canary of a workflow
    whose definition differs from the deployed one always takes its own Temporal namespace.
    What it found or confirmed: the memory the placement counts for a VM is 6.6 MiB OVER a real
    launcher pod's request, never under, and its CPU 5 m UNDER (the console-log sidecar asks
    for 5 m that `pod_cpu_millicores` does not count — reported, the operator's number); two
    seeds of one zone are read in id order across both, and a prep MCE's VMs never reach a prod
    cluster though both lifecycles wait on the one lock; a hundred runs at once end in 30 s
    with every cluster at or under its buffer. On real time: a lease used up by ten dead
    clusters is followed by a second, twenty-six take three (`KvZoneLeaseExpiredError` needs
    more than thirty no-worker clusters, more than one lifecycle of a zone can name with its
    id letters — only clusters SLOW to answer reach it); a terminated holder costs its zone one
    lease; a lock workflow terminated by hand is started again by its waiter's next ask; a
    cancelled holder passes the zone on at once; the ten-minute rollback keeps its clock across
    the worker's AND the brain's pod dying. What it showed of a cluster whose reads work and whose
    creates fail (an API outage) — the run held its zone a whole lease and failed — is what the
    operator then reversed: such a cluster is left for the next (above). Fifteen more goldens
    (`lab-2-cluster-qcl8n-*`; the outage run's was dropped with that change). The fake
    fleet's pod is `Recreate`: its clusters are in memory, so two pods alive at once are two
    worlds behind one queue.
    The 2026-11-05 lab (cluster-64spr) proved the AGENT-era design and is
    history; two of its findings still hold: a 16 GiB guest's launcher pod requests about 308 MiB
    over the guest (the flat 1 GiB pad kept from it was replaced by the measured formula on
    2026-11-24, above), and ceph-rbd refuses the root disk's ReadWriteMany + Filesystem.
- **delete-vm (domain `vm-lifecycle`) deletes ONE VM by name from its KV cluster and COMPLETES ONLY
  WHEN NOTHING OF IT IS LEFT IN KUBERNETES, ITS PERSISTENTVOLUMES INCLUDED**
  (`workflow_domains/vm_lifecycle/delete_vm.py`, own queue `delete-vm-workflow`, id
  `delete-vm-<kv cluster>_<vm name>`). Planned and built with the operator 2026-11-26
  (`~/.claude/plans/i-want-u-to-frolicking-hopcroft.md`); (operator) marks his decisions, the rest
  is the lead's. Why it exists: nothing removed an INSTALLED VM — remove-server-from-mce takes the
  host out of the MCE and stops, so the VM stayed Halted on its KV cluster, counted against its
  capacity for ever, its disks in the array; and `delete_vm_on_cluster` is a rollback tool (it
  finds a VM by a create-vm RUN id, waits for nothing, touches no claim). That activity and its
  three callers are UNCHANGED. The pieces that are decisions:
  - **Child only, NO ROUTE** (operator). It holds no MCE credential, so it cannot check that the
    VM's BareMetalHost is gone, and metal3 needs the VM's Redfish to release a host.
    remove-server-from-mce stays the one public way to remove a VM. **THE INTEGRATION IS NOT BUILT**:
    remove-server-from-mce starting it (right after metal3 lets the host go — operator), and
    add-server-to-mce writing the BareMetalHost's `redbull.io/kv-cluster` / `redbull.io/created-by`
    labels it would read, are a later change with open questions to DISCUSS with the operator
    first (the plan's "Later" section). Until then only tests and the Temporal CLI start it.
  - **Input**: `kv_cluster`, `namespace`, `vm_name`, `mce_cluster` required; `created_by`, `mac`,
    `why` optional. **By NAME, guarded by the VM's own labels**: the VM must carry
    `redbull.io/created-by` (a hand-made VM is NEVER deleted) and `redbull.io/mce` equal to the
    request's; `created_by` and `mac` must match when given; a `redbull.io/kv-cluster` label must
    name the worker's own cluster (absent on a VM made before the label: not compared). Otherwise
    `VmIdentityConflictError`, nothing written — checked at the inspection and FRESH at the delete.
    Every delete is by the UID read.
  - **The VM also carries `redbull.io/kv-cluster: <day1 file name>` since this change**
    (`vm_spec.KV_CLUSTER_LABEL`; the naming convention's §4 since its PR #2, 2026-11-26). Written by
    the cluster's worker; no patch — only a limb's body changed, every create-vm golden replays.
  - **Three activities on the cluster's own worker**: `inspect_vm_for_deletion` (observe-only; the
    ONLY call with a schedule-to-start limit, 5 min: nothing is written yet) →
    `delete_vm_and_volumes` (re-read, re-checked; VM → its DataVolumes → each claim's CDI
    VolumeImportSource → the claim; the import source goes BEFORE its claim because its name is
    only on the claim) → `await_vm_gone` (ONE heartbeating activity, 8 s polls, 15 min, `done` /
    `left` / `deadline`). Phases `inspecting`, `deleting`, `awaiting-release`, `completed` /
    `failed`. Permanent, from the classes: `KvClusterAccessError`, `KvClusterMismatchError`,
    `VmIdentityConflictError`, `VmRequestRejectedError`. No new limb-raised exception. Nothing found
    anywhere is a COMPLETED run, `found: false`. That schedule-to-start timeout is answered by the
    workflow as `KvClusterWorkerAbsentError` (WORKFLOW-RAISED, naming the queue, "nothing was
    deleted") instead of Temporal's bare "activity ScheduleToStart timeout" (the lab, below).
  - **"Not stale in the array" = the PersistentVolume object is gone** — deployment-agnostic (§3):
    no ceph or Trident call, no driver CRD. An ASSUMPTION about CSI provisioners (a `Delete` PV is
    removed only once the driver deleted the volume), checked on ceph in the lab below and NOT YET
    on Trident (the air-gap: after a delete, `oc get pv` shows nothing and `tridentctl get volume`
    has no entry). Two cases are KNOWN where it does not hold, so the run looks for both: `Retain`
    (the PV goes `Released`, the volume stays) and snapshots (the backend keeps the volume under one).
  - **DELETE, THEN FAIL** (operator): when the array cannot be proven clean the run deletes
    everything it may and then FAILS — never a success with a note. `VmVolumesLeftInStorageError`
    (WORKFLOW-RAISED): a `Retain` PV, a PV still there at the deadline, a VolumeSnapshot of one of
    the VM's disks, or a look-alike claim (below) — each PV named with its CSI driver and volume
    handle. `VmDeletionTimeoutError` (WORKFLOW-RAISED): the VM, a DataVolume or a claim still there
    at the deadline, with its finalizers and the pods mounting it. A re-run RESUMES; after a
    "delete, then fail" it fails the same way until a person removed the snapshot or the Released
    volume, then completes `found: false`.
  - **NEVER deleted by the run**: a PersistentVolume (deleting a `Retain` one loses the only pointer
    to its backend volume), a VolumeSnapshot (someone's backup), a pod (the worker keeps no pod
    verb; a pod holding a claim is NAMED in the failure).
  - **A RUNNING VM is deleted too**, `was_running` on the result: abnormal (after metal3 released
    the host the VM is Halted), but the worker has no verb to stop a VM, the caller has proved the
    host left the MCE, and refusing would leave a powered-on VM nobody owns.
  - **Which volumes are the VM's is ONE pure rule, `activities/vm_lifecycle/vm_volumes.py`**: a
    claim of the VM's namespace is the VM's when its spec names it, or its name is `<vm>-root` or
    kubevirt-redfish's boot ISO (`<vm>-bootiso-<unix ts>-<6 digits>`, a BARE claim nothing owns,
    deleted by kubevirt-redfish on eject — so a leftover only when metal3's eject did not happen),
    or it carries the fork's `vm.redfish: <vm>` label (the CDI import path only), or an owner
    reference names the VM, one of its claims or DataVolumes. Read in the team's kubevirt-redfish
    fork (`pkg/kubevirt/client.go`, commit 9f98767): it cuts any name over 63 characters to its
    first 49 plus a hash and `truncated`, so a CUT boot ISO name is certain only while it still
    holds all of `<vm>-bootiso-` (a VM name ≤ 40; today's are about 27). Past that it is a
    `cut-name` LOOK-ALIKE — two long names differing in their last characters cut alike — which is
    reported, NEVER deleted, and fails the run. A PV is the VM's by a bound claim or by a
    `claimRef` naming one of them or a name the NAME rules know (how a re-run finds a volume after
    the claims are gone); a snapshot by its source claim, the same two ways.
  - **What the wait watches comes from the run's HISTORY** (the advisor's review): the workflow
    hands `await_vm_gone` the union of what the inspection and the delete saw. A volume is found
    through its claim, so an attempt that starts after the claims went would otherwise see no
    volume and report `done` over one still there (`test_a_wait_resumed_after_the_claims_went…`).
  - **The wait's 15 minutes count only the time the cluster was SEEN** — the seconds between two
    polls that both answered, kept in the heartbeat details (`waited_seconds`), NOT a wall clock
    since the first attempt (create-vm's ready wait keeps that one). Found in the lab below: the
    cluster was away for twenty minutes of a wait, and the run failed the moment it answered again,
    over a volume the cluster had simply not got to yet. Do not go back to `started_at` here.
  - **No zone lock** (freeing room only makes a create-vm run's figures err on the full side), no
    rollback, NOTHING on an exit path (WF017 has nothing to guard); a cancel ends the run CANCELED
    and undoes nothing. The two non-heartbeating calls are `ABANDON` on cancel, the wait `TRY_CANCEL`.
  - **RBAC, which REVERSES the chart's "no write to a claim"** (dated in its README): the
    `vm-lifecycle-worker` ClusterRole gains persistentvolumeclaims `list, delete`,
    persistentvolumes `get, list`, `snapshot.storage.k8s.io` volumesnapshots `list`,
    `cdi.kubevirt.io` volumeimportsources `delete`. Still no pod, VM-patch or power verb, and no
    delete of a PV or a snapshot. **Deploy order: the chart on every KV cluster → the brain → the
    vm-lifecycle image**: an old image answers "not registered" (the run waits); a new image on an
    old ClusterRole answers 403, permanent.
  - **PARKED for the integration's discussion, so NOT built** (operator, 2026-11-26): "wait, do not
    fail" — no schedule-to-start anywhere and a 403 RETRIED on purpose, because delete-vm's only
    caller will have deleted the host and a failed run loses the one record of where the VM is.
    Adopting it changes activity options only.
  - **Tested on the fleet** (`tests/kv_fleet.py` now has real claim, PersistentVolume and snapshot
    stores: a VM's delete takes its DataVolumes, a DataVolume its claim, a claim — once no pod
    mounts it — its volume, removed after a settable number of reads or left `Released` under
    `Retain`): `tests/test_vm_volumes.py`, `tests/test_vm_lifecycle_delete_limb.py`,
    `tests/test_delete_vm_workflow.py`, goldens `tests/histories/delete_vm/` on the production
    constants.
  - **THE 2026-11-26 LAB (cluster-qcl8n) ran delete-vm alone, as its parent will start it**: the
    branch's brain and the KV cluster's worker run locally on their own Temporal dev server, the
    worker acting as the CHART's ServiceAccount (its rendered ClusterRole, a token kubeconfig — so a
    missing verb would have shown), on VMs made by the real limb code (`vm_operations.create_vm`)
    in the lab's own namespace, on external ceph (cephfs root disks, an rbd boot ISO). Proven there:
    a Halted VM gone with its DataVolume, claim and PersistentVolume in 16 s, ceph's cephfs driver
    logging `successfully deleted volume <handle>` before the PersistentVolume went — while for
    RBD the driver's DeleteVolume only QUEUES the image's removal (`successfully added task to move
    image …`: ceph's trash, emptied by a manager task) and then answers, so there "the
    PersistentVolume is gone" means "its removal was handed to ceph", not "already removed";
    nothing there → `found: false`; a `Retain` class → everything deleted, then
    `VmVolumesLeftInStorageError` naming the Released volume and its handle, the same on a re-run,
    and `found: false` once the volume was removed; a VolumeSnapshot of the root disk → the same
    failure naming the snapshot, AND ceph's driver reported the volume deleted and the
    PersistentVolume went while the snapshot stood (the reason snapshots are looked for); a boot ISO
    inserted (CDI path, with its VolumeImportSource) plus an older bare one → all three claims and
    volumes gone; a RUNNING VM → deleted, `was_running`, with the worker killed (`kill -9`) during
    the wait and the next attempt resuming from its heartbeat (the SDK does not send every
    heartbeat at once, so a hard kill can lose up to about a minute of the watched time — the wait
    is then that much longer, never shorter); a pod holding the root disk → `VmDeletionTimeoutError`
    naming the pod (that run was on the OLD wall clock and met the outage: 33 polls, not a clean
    15 minutes), and a claim left Terminating by the cluster → the same on the new clock, after
    15 minutes of WATCHING, 19 on the wall; a hand-made VM, another MCE's, another run's, another
    MAC, another cluster's `kv-cluster` label → `VmIdentityConflictError`, nothing deleted; a worker
    on the PRE-delete-vm ClusterRole → `KvClusterAccessError` at the first new verb, the VM
    untouched, and the same worker completing once its role was the chart's (the deploy order); no
    worker → the run failed after exactly 5 min, nothing read. A re-run whose VM and claim were
    already gone found the leftover volume by its `claimRef` and completed. FOUND AND FIXED: the
    cluster's API and controller manager went away for twenty minutes of one wait (that cluster's
    chronic trouble: `kube-controller-manager` loses its lease on a slow API, 220 restarts in six
    days) and the wall-clock deadline failed two runs the moment it answered again — hence the
    watched-time clock above; and the bare ScheduleToStart message — hence
    `KvClusterWorkerAbsentError`. Also seen: a DataVolume's own claim names a CDI VolumeImportSource
    (`volume-import-source-<uid>`), deleted like a boot ISO's — and the result's
    `import_sources_deleted` lists the sources the VM's claims NAMED, which a retry may find the
    garbage collector already took. NOT proven there: the boot ISO made
    by kubevirt-redfish itself (the lab made the claims by hand in the fork's shapes; pressing
    InsertMedia needed its Redfish login), the ARRAY listed directly (`rbd ls` / `ceph fs subvolume
    ls` — only the driver's log was read), Trident, and delete-vm as a real CHILD of
    remove-server-from-mce. Twelve of its histories are goldens
    (`tests/histories/delete_vm/lab-cluster-qcl8n-*.json`).
- **Select bond members from `interfaces[]`, NEVER from `nic_macs`.**
  server-scan reduces Dell NPAR partitions to one entry per physical port in
  `interfaces` but leaves `nic_macs` whole on purpose, so a 4-port partitioned
  card reports 4 interfaces and 16 MACs. Indexing the MAC list positionally —
  as bmhgen did — can bond two partitions of ONE physical port: one wire, no
  redundancy, and it looks correct until that wire fails.
- **add-server-to-mce requires link_state UP strictly**, and that excludes whole
  vendors by construction: HPE OneView's `portMap` carries no link state at
  all (server-scan stores UNKNOWN unconditionally) and Intersight vNICs
  usually report none. Those servers fail bond selection with
  `NoBondableInterfacesError` naming the provider and the states observed —
  deliberately loud, so the gap is visible rather than looking like an empty
  inventory. Decided with the operator, 2026-09-25. Since 2026-11-17
  server-scan enforces it BEFORE it allocates (network health HEALTHY = two
  link-UP ports read), so such a server is never handed to a run and the run
  no longer refuses on link state itself (ONE SERVER PER RUN, above); a run
  recorded before still does.
- **add-server-to-mce's `allow_unhealthy` is HIDDEN ON PURPOSE — not dead code.** The operator's
  "easter egg" (2026-10-31), after an incident that had to install servers with one NIC up
  and no RAID. Out of OpenAPI/Swagger (`SkipJsonSchema`, pinned by `TestTheHiddenAllowUnhealthy`),
  the README, the docs site and every UI; left out of payloads while unused (`_QuietDefaults`),
  so the Temporal UI does not show it either. Only with `server_names` (422 on a pool draw).
  It waives HEALTH — the server's own condition: server-scan's health tier, network category,
  BMC reachability, live recheck, and a bond member whose link is not UP (still two members on
  distinct ports from `interfaces[]`, nic1 the first link-UP NIC) — NEVER safety: unknown to
  server-scan, claimed, contested, maintenance, another run's lock, InfraEnv, BMC credentials,
  a BMH/Agent present. A named machine the allocation refuses is read once more
  (`lookup_install_candidate`) and sorted by `health_waiver.py`; every waiver lands in
  `allowed_unhealthy` (progress, result, each create's input). No patch gate: no recorded run
  carries the field.
- **A conflicting NMStateConfig is REPLACED** (the operator, 2026-10-31: an install failed on an
  AlreadyExists NMStateConfig, and deleting it by hand was the only way on).
  `replace_conflicting_nmstate_config` runs once per candidate right after its lock and BEFORE
  anything else is written for it — the PXE map, every create — in BOTH orders. Absent or
  matching: nothing written. Differing: deleted with UID + resourceVersion preconditions,
  created from the install, read back and logged `nmstateconfig-replaced`
  (`result.nmstate_replaced`). Delete-and-create, never update: the Role has no update verb,
  and the NEW creationTimestamp is what the IPMI boot-entry publish waits on. Gated
  `nmstateconfig-replace-unless-in-use`. NO BareMetalHost/Agent check first — do not re-add
  it (the operator, same day: "no reason to perform double checking"). server-scan offering
  the machine is the proof it is free, the install lock covers the window before server-scan
  sees a fresh install, and a BMH left without an Agent is a failed install whose NMStateConfig
  is exactly what needs fixing — the check would have refused the very re-run that incident
  needed. For a few hours that day the limb did refuse (`ServerLooksInstalledError`, and `list`
  on baremetalhosts in the Role); the workflow still handles that error from an older limb.
- **The inventory VLAN belongs to the MCE, and is resolved once per run** —
  `get_inventory_segment` on the SEGMENT-LIFECYCLE queue, not a second copy of
  that token on the server-lifecycle limb. There is deliberately no `vlan_id`
  input: a caller-supplied VLAN could contradict the segment the cluster owns.
- **An MCE has ONE inventory network, found by CLUSTER NAME.** It was briefly
  split by how a BMC is driven — `INVENTORY_REDFISH` for HP/Dell/Intersight,
  `INVENTORY_IPMI` for a UCS blade — which made the segment a property of the
  chosen machine and forced the lookup inside candidate selection. Reverted on
  both sides on 2026-09-27, by the team lead's decision: two inventory scopes
  per MCE is a second thing to allocate, track and keep in step, and an MCE
  holding only one of them made "this cluster takes no servers driven that way"
  a state every consumer had to model. So the lookup is step ONE again, before
  a server is allocated (and locked), and a missing allocation fails the run —
  no server could make it usable. `bmc_vendor` still picks the Ironic DRIVER;
  it no longer picks a network.
- **In add-server-to-mce, every reason a server is unusable is ONE NAMED REASON** —
  since the allocation (2026-11-17, above) the run's one server, refused, gives
  its lock back and fails with it; a run recorded before still treats it as a
  SKIP to its next candidate (its draw existed so one unusable server was a
  retry, and a reason handled outside the selection loop would have broken that
  for part of the fleet — a pool of three dying on a STANDALONE machine at the
  front). The reasons are two tables in `add_server_to_mce.py` (`_REJECTION_TYPE`,
  `_REJECTION_SUMMARY`) keyed by the same strings the loop records; an old run
  reports them as separate groups when nothing survives, the failure taking the
  specific type only if they all agree. The page at `/ui/server-lifecycle`
  parses both shapes with those summaries. A reason missing from either table is
  a KeyError raised in WORKFLOW code, which hangs the run rather than failing it
  — so a test keeps the loop and both tables in step.

- **The workflow is the ENTRY POINT; the Segments Manager is a dependency, never a trigger.** A
  caller POSTs the full segment DEFINITION to `POST /workflows/segment-lifecycle/initialize-segment`
  and the workflow creates the segment itself (`create_segment`). The reverse used to be true — the
  Segments Manager created the segment then fired a best-effort HTTP trigger at us — which left
  creation outside Temporal: invisible in the UI and silently skipped whenever that call failed. Keep
  it this way: anything a workflow's outcome depends on belongs INSIDE the run, as a retried
  activity, not in a caller's fire-and-forget call. New domains follow the same shape.
- **The Segments Manager is the VALIDATOR OF RECORD.** Site known, CIDR inside the site pool, no
  overlap, VLAN free — all its rules, none re-derived here. A rejected definition
  surfaces as a FAILED run, not a 4xx on the trigger, because creation happens inside the workflow.
- **A segment's TYPE is ALLOCATION state, not part of its definition.** A segment is created with
  NO type (`InitializeSegmentInput` has no `type`; the Segments Manager rejects one on create), joins
  one shared Available pool per site, and gets its type only when allocate-segment reserves it —
  `AllocateSegmentInput.type` is the ONE place a type enters the system, and the Segments Manager
  stamps it onto whichever Available segment it hands out, in the same atomic update. Release clears
  it again. So there are no per-type pools and no inventory to rebalance between them, which is why
  **convert-segment was removed — do not re-add it** (with `PUT /api/segments/type`, its models, its
  queue and `SegmentConversionConflictError`). The per-type things that remain are all about an
  ALLOCATION: the allocate id, the SM's (cluster, site, type) idempotency, the DHCP exclusion policy.
- **The day1 values repo is reached by git subprocess** (`DAY1_REPO_URL`), never a Python git
  dependency — see §5. ONE git client, `activities/git_repo.py`, shared by every limb that clones
  (cluster-lifecycle, upgrade-gitops, server-lifecycle for update-nodepool; segment-lifecycle no
  longer touches git, and neither does vm-lifecycle since 2026-11-20 — its worker runs inside a KV
  cluster and create-vm's day1 list is read on the cluster-lifecycle limb).
- **The cluster-upgrade hub limb (`upgrade-gitops-worker`) works on `main` only**: Argo CD
  renders nothing else. It reads the rules after day1's own `scripts/validate_upgrade_rules.py`
  passed them, and a cluster file the way the hostedclusters-setup chart renders it — the value
  layers merged as Helm does, the HostedCluster `openshift-<clusterName>` in `hcp-<clusterName>`
  (`clusterName` the file's own, else its name minus `ocp4-`), NodePools `.name` else
  `<clusterName>-<infraEnv>` — every duplicate a `problems` line, never an exception. A commit edits
  ONLY its key lines as text, re-parses, and refuses (`Day1ChangeRejectedError`) unless only those
  keys moved and the file now renders what was asked; "already so" is checked BEFORE the blob
  compare-and-set, or a retry would read its own landed push as someone else's edit
  (`Day1FileChangedError`). `DAY1_VALIDATE_COMMAND` runs in the clone first; ONE clone per
  attempt, and a rejected push raises the retryable error so Temporal re-clones — never a retry
  loop inside the attempt, never forced. The clone's `origin` is reset to the URL WITHOUT the
  token right after cloning and the push goes to the authenticated URL built in memory, so no
  file of the clone holds a token while day1's or Brain's own scripts run in it. Those scripts
  still run as this pod's user, which can read its environment and the mounted registry auth;
  moving them to a separate Job is the fix (the chart README says so). Releases are pinned over
  the registry v2 API and checked with `oc adm release info` (the image carries `oc`): only a
  deterministic "not found / manifest unknown" is a gap; a timeout, a missing `oc`, non-JSON, an
  outage or a refused credential is the retryable `ReleaseRegistryError`. No `DAY2_REPO_URL` /
  `BRAIN_REPO_URL` is a `note` in the result, never an error, so a lab runs without them. Brain
  never holds a gate: `write_incident` always returns, with a `note` when Brain is missing,
  unreachable after its third attempt, or refuses; a page its validator refuses, or a validator
  that times out or crashes, is not pushed (fail closed), and the run is told.
- **provision-cluster is the ENTRY POINT for a new hosted cluster, and owns its day1 file, its
  branch, its merge request and its day2 folders** (domain `cluster-lifecycle`; designed with the
  operator 2026-10-24 … 2026-11-11, plan `~/.claude/plans/i-want-you-to-pure-hickey.md`,
  `docs/design/provision-cluster.md`). The caller hands over the COMPLETE hosted-cluster YAML a
  person wrote — the request BODY, `application/yaml`, verbatim; Swagger pre-fills the slim shape
  (`EXAMPLE_CLUSTER_FILE` in the router) — plus `cluster` (canonicalised to the file stem
  `ocp4-<clusterName>`) and `mce` (the MCE FOLDER name, verbatim: `ocp4-mce-site2` and
  `ocp4-mce-site4-prep` are real folders no convention parses; the folder is found by NAME among
  `sites/*/mces/`, never derived). The run: (1) `inspect_cluster_target` at day1 `main` — the folder,
  no file of that name there yet, and the text judged against the merged value layers the way the
  chart renders them (REQUIRED `mastertag`, non-empty `nodepool`, `owner.{merkaz,anaf,mador}`;
  FORBIDDEN `vlanId`, `dhcp_values`, `mcConfigMap`, `tuningConfigmap`, `AutomateAllocation`; `clusterName` only equal to
  the derived one; config names DNS-1123 labels; `configs` OPTIONAL per pool — a KubeVirt pool has
  none) — EVERY reason in one `ClusterValuesRejectedError`; (2) allocate-segment as an AWAITED
  child (§5); (3) the text plus the allocation block (`vlanId`, `dhcp_values` with the type's
  `DHCP_EXCLUSION_OCTET_RANGES` — the policy and `build_dhcp_values` moved to this limb with the file;
  NO marker comment, and the day1 chain's `# === Added by CI ===` header goes too: the operator,
  2026-11-11 — markers separated several writers' commits to one file, and one push now writes it
  whole; do not re-add either) pushed to `provision-cluster/<stem>`,
  created from `main` by the push, with NO `[skip ci]`; (4) the merge request, found or created;
  (5) a HUMAN wait for the merge, no deadline (§5); (6) only AFTER the merge, the folder
  `sites/<site>/<env>/mces/<mce>/<stem>/.gitkeep` in every sig the layers list under
  `day2.copy_to_sigs`, on that sig repo's default branch — day2 renders `mastertag` from day1 `main`,
  so a folder created before the merge breaks the sig's whole render (APPLY-DAY2-COPY §4b); the
  env is `day2.env` else the cluster name's first field, cross-checked against the MCE folder's
  when it is `ocp4-<env>-mce-…`, derived only when there are sigs; (7) LAST, the console SHORTCUT
  (go-link; the operator, 2026-11-15, `ensure_cluster_shortcut`, phase `creating-shortcut`, gated
  `provision-cluster-shortcut`): name = the cluster name without `ocp4-`, URL =
  `CLUSTER_CONSOLE_URL_TEMPLATE` with `{cluster}` = the stem (day1 records no base domain),
  `username` = `SHORTCUT_OWNER`, against `SHORTCUTS_API_URL` with `SHORTCUTS_API_TOKEN`. `GET
  /check/<name>` first: taken by `SHORTCUT_OWNER` = success, nothing posted (the service records the
  username sent as the owner — the operator confirmed it; the check reports no URL, so ours is never
  re-pointed); taken by ANYONE ELSE = `ShortcutTakenError`, the one permanent answer; else `POST
  /create`, then the check again as the read-back. Every other failure, a 401/403/400/422 included,
  is the RETRYABLE `ShortcutsApiError` on purpose — the day2 reasoning: past the merge a failed run
  cannot be re-run. **MachineConfigs stay the day1
  CI chain's** (the operator, 2026-11-11, deferring the chart-at-sync design in
  `~/.claude/plans/mc-sources-rendered-at-sync.md`): the chain runs on the branch, skips allocation
  (`vlanId` is there), renders the MachineConfigs and pushes `CI: Update mcConfigMap from mc-files
  [skip ci]` on top — so the workflow NEVER writes `mcConfigMap`, nor `tuningConfigmap`, the tuning
  profiles (Tuned, PerformanceProfile) the chain injects in the same commit from day1's `tuning/`
  catalogue by the nodepools' `tuningConfig` names — and a re-run's publish compares the branch file
  SEMANTICALLY (its YAML mapping with both chain keys dropped, `CHAIN_APPENDED_KEYS`) against this run's text,
  `changed=False` when equal, `ClusterFileConflictError` otherwise; never byte-for-byte, the
  injector re-spaces blank lines too. Nothing is undone on failure: the allocation is idempotent,
  the branch and the merge request converge on a re-run, day2 folders are "already there = success".
  Not the workflow's: rendering a MachineConfig, pushing `main`, merging, syncing the Application (a
  person, by hand, in production), the `*.apps` DNS DisposableRequest (still the chart's, until its
  contract is reported from the air-gap) — see `docs/design/provision-cluster.md`. The chart's go-link
  DisposableRequest must GO (or create under `SHORTCUT_OWNER`): one made first under another owner
  fails every run at `ShortcutTakenError`.
- **create-upi-cluster: bootstrap-installer stays the ONE install implementation; the workflow
  drives it** (the operator, 2026-11-27, automating his "Creation MCE 4.20" runbook). Two Jobs per
  run, `upi-<cluster>-<prepare|install>-<run token>`, from INSTALLER_IMAGE: `prepare` =
  `bootstrap.py --skip-terraform --skip-argocd` (configs, ignition, `*.apps` DNS; then the admin
  kubeconfig and ONLY the node IP lists — cluster-context.yaml also holds the vCenter password — into
  `upi-<cluster>-kubeconfig`), `install` = `--skip-ignition --skip-dns --skip-argocd` (terraform apply,
  its "yes" prompt answered by the script, and its CSR loop). The work volume `upi-<cluster>-work`
  holds terraform's state and is NEVER deleted; a `prepare` refuses one whose state lists resources
  (destroy first). bootstrap-installer's CSR loop exits 0 on timeout, so READINESS is the limb's
  (`await_upi_cluster_ready`: every recorded IP a Ready Node, ClusterVersion Available with a
  version). Argo CD registration is the LIMB's, not bootstrap-installer's (`oc --server --token`
  from a hub pod cannot verify the hub's TLS): `cluster-<cluster>` with `name: <cluster>` — the
  hub's `<site>-mces` ApplicationSet targets `destination.name: <folder>`, so an MCE's folder name,
  its Argo CD cluster name and its Segments Manager cluster are ONE string. For an MCE, the run then
  adds `sites/<site>/mces/<cluster>/{values.yaml, version.yaml}` by merge request and waits for the
  merge (provision-cluster's shape); never an empty file under `hostedClusters/` (it breaks the
  generator). allocate-segment allocates MCE, INVENTORY and UPI since the same day (a UPI segment's
  holder is a LIST: `SegmentEntry.held_by`).
- **add-server-to-upi: one named server, bond0 over two link-UP NICs, DHCP on `bond0.<vlan>`, the
  ISO on the BMC's virtual media** (the operator, 2026-11-27; what cluster-expender tried as a CLI).
  The bond is add-server-to-mce's `select_bond_members` (reused, `allow_link_down=True`: server-scan
  vouches); an IPMI BMC (no virtual media) or an unknown vendor is refused and the lock given back.
  The nodes-config binds MACs to LOGICAL names (`nic1`, `nic2`); the ISO is built by `oc adm
  node-image create` as the worker's ServiceAccount (cluster-admin by default — node-image needs it)
  and served by the worker pod's own nginx sidecar from its volume (ONE replica). Only THIS host's
  CSRs are approved (the bootstrapper's client CSR with CN `system:node:<hostname>`, the serving CSR
  BY that node). After the join, server-scan's lock is extended a day; on a failure it is left to
  its TTL and nothing is undone. Not built yet: a pool draw, PXE for IPMI blades, a remove path.
- **GitLab is the ONLY git provider the services know. Do not re-add a second one.** This repo's
  own source and CI live on GitHub; the air-gapped production is GitLab, with SUBGROUPS (which
  GitHub has not). provision-cluster's limb first carried both — `DAY1_GIT_PROVIDER` gitlab|github,
  a `GitHubProvider` opening pull requests, and `DAY2_SIG_PATH_TEMPLATE` for sigs kept as folders of
  ONE GitHub repo. All three were removed on 2026-11-14 (the operator: "i don't want to manage 2
  different conventions" — the air-gapped service must not even know another option exists). So:
  the merge request goes through `<host of DAY1_REPO_URL>/api/v4/projects/<the URL's whole path as
  ONE URL-encoded segment>` (`redbull%2Fgitops-day1%2Fplatform-config`) with `PRIVATE-TOKEN`; a sig
  is ALWAYS its own project (`redbull/gitops-day2-prod/sigs/<sig>`, the tree at its root), so
  `DAY2_SIG_REPO_URL_TEMPLATE` must name `{sig}`; the settings ignore the two retired keys
  (`extra="ignore"`), so the image ships before a chart stops rendering them. It is tested against
  a REAL GitLab, not GitHub: the instance in namespace `gitlab` on the dev cluster, whose `redbull`
  group mocks the air-gap's repos in their subgroups — proven there 2026-11-14 with a GROUP access
  token (Maintainer, `api` + `write_repository`), the table in `docs/design/provision-cluster.md`.
  What that run fixed or pinned: a FAST-FORWARD merge reports neither `merge_commit_sha` nor
  `squash_commit_sha`, so `merged_sha` falls back to the merge request's head (`sha`); GitLab's 409
  for a second open merge request from one branch stays the retryable `GitProviderError` (the retry
  finds it); a token whose role may not push a sig's protected default branch, or a sig with no
  project, is a refused git command — `ValuesRepoGitError`, RETRIED on purpose: the cluster file is
  already merged, a failed run could not be re-run (`ClusterAlreadyProvisionedError`), so the run
  waits at `registering-day2` until the role or the project is fixed. Not supported: a GitLab under
  a relative URL root (the clone URL's host is taken as GitLab's root).
- **allocate-segment allocates and VERIFIES; it no longer touches git** (2026-11-11). Until then it
  located the cluster file on a day1 branch the caller named (`values_branch`, the pipeline's
  `$CI_COMMIT_BRANCH`) and pushed the `vlanId` + `dhcp_values` block there with `[skip ci]`, as a
  step inside the day1 pipeline. Since provision-cluster owns the cluster file, the run takes the
  SITE as input (the day1 folder the file goes in, passed by the parent; cross-checked against
  `GET /api/sites`, `UnknownSite`), allocates, reads back, and ends. `values_branch` stays on the
  input model (history), is IGNORED by the run and REFUSED at the edge with a 422 naming
  provision-cluster; `site` is required there (`AllocateSegmentRequest`), `SiteMissingError` from the
  run. Gone with it: `locate_cluster_file`, `append_allocation_to_cluster_values`,
  `values_repo.py`, `ClusterValuesConflictError`, `ValuesBranchMissing`, the limb's git binary, and
  `DAY1_REPO_URL`/`DAY1_GIT_TOKEN`/`DHCP_EXCLUSION_OCTET_RANGES` from its settings (its chart keeps
  rendering them until its image stops reading them — §8). The standalone route stays for a manual
  allocation, on the same id, so a manual run and provision-cluster's child dedup against each
  other. The day1 CI's `allocate_segments` job must stop calling the route (it would get the 422)
  — a day1 change, made in the air-gap with provision-cluster's rollout.
- **THE DHCP SCOPE WAIT IS GONE. Do not re-add it to allocate-segment.** After the push, the run used
  to poll the DHCP scope API (`DHCP_API_URL`, `GET /api/v1/scopes/{network}`, anonymous, 404 = not
  yet) every 15s for up to 15 min until the live scope carried the exclusions it had written, then
  fail `DhcpScopeNotConverged` (`get_dhcp_scope`, `DhcpScopeState`, `DhcpApiError`,
  `dhcp_scope_ready`, phase `awaiting-dhcp-scope`). It left because Argo CD reads the day1 repo's
  `main` ONLY (`hcAppset.yaml` pins `revision`/`targetRevision: main`): the scope appears only after
  the merge, the merge comes after the pipeline, and the pipeline is waiting on this run — a
  deadlock, not a slow wait. The principles it embodied still hold: git is the single source of
  truth and Crossplane the only CREATOR/UPDATER of scopes (we never POST or PUT one), and convergence
  is judged on the EXCLUSIONS we wrote, never the DHCP API's own derived range. `get_dhcp_scope`,
  `DhcpScopeState`, `DhcpApiError` and `DHCP_API_URL` are back — for release-segment's safety net
  (next bullet), not for this wait; when provision-cluster verifies the scope after ITS merge wait
  (a follow-up), it reuses them with a bounded deadline (`git log -S dhcp_scope_ready` has the
  removed loop).
- **release-segment gives a decommissioned cluster's segment back — and is the ONE DHCP API writer,
  delete-only.** Decommissioning a hosted cluster is deleting its `<cluster>.yaml` from the day1
  repo's `main`: the ApplicationSet deletes the Application, its resources finalizer deletes the
  Crossplane `Request`, and provider-http DELETEs the scope. Nothing in that cascade releases the
  segment, so release-segment (input `cluster` + `type`, HC only) finds the Segments Manager's
  allocation, deletes the scope IF it survived (an orphaned Request, a cascade that never ran) and
  reads back its absence, then re-reads the owner, releases and reads back Available/no
  cluster/no type. The scope goes BEFORE the pool — a re-allocated segment must never collide with
  live leases — so a DHCP API outage holds the release back (RUNNING, retried) by design. This
  amends "Crossplane the only writer": it remains the only CREATOR/UPDATER; release-segment DELETEs,
  and only once the Application and its Request are gone (a Request still alive re-POSTs the scope
  ~60s later — `DhcpScopeStillPresentError` catches the case where it already has). The DELETE needs
  a token, so the worker carries a COPY of the DHCP API's (§8). The client is
  `activities/segment_lifecycle/dhcp_api.py` (plain parameters, §1). No git step: the file deletion
  IS the decommission. Nothing allocated is a COMPLETED no-op (`released=false`), never a failure.
- **release-segment's trigger is an Argo CD PostDelete hook — a bridge until deprovision-cluster.**
  The hostedcluster-setup chart carries a `PostDelete` hook Job that POSTs `{"cluster", "type": "HC"}`
  to the API (202 and 409 are success). Argo CD runs it only after EVERY resource of the Application
  is deleted (NodePool and HostedCluster included, in the air-gapped chart). Argo CD holds the
  Application while the hook Job RUNS but does NOT retry a FAILED one — verified on GitOps 1.18 /
  Argo CD 3.1, a Failed hook counts as complete and the Application is deleted anyway (its docs'
  `DeletionError` hold does not happen here). So the hook's script retries the POST itself, capped
  backoff, up to 55 min (a bounded MACHINE wait, §5); a final failure leaves the Failed Job as the only
  record, its log ending with the request to send by hand. Argo CD Notifications were rejected: the
  catalogue's `on-deleted` trigger fires on `deletionTimestamp != nil`, i.e. at the START of
  deletion, once and fire-and-forget — the shape this section rejects for every trigger (the
  workflow is the entry point). When a deprovision-cluster workflow exists it owns the teardown (delete
  the file, a bounded wait for the Application to disappear) and starts release-segment as a child;
  the hook is deleted then, release-segment does not change.
- **THE FIREWALL FLOW IS GONE. Do not re-add it.** There used to be another dependency here: the
  **next** connectivity service, another team's air-gapped firewall approver. initialize-segment
  discovered same-site peers, submitted open-rules requests (plus MCE↔BMC rules from a
  `SITE_NETWORKS` ConfigMap), mirrored the pending request ids into the Segments Manager UI, polled
  indefinitely for a HUMAN approval with `continue_as_new`, published a failure note on terminal
  failure, and finally unlocked the segment `Locked -> Available`. Every firewall between segments
  is open now, so all of it was removed — along with the `Locked` status, the `NEXT_*`/`PORTS_*`/
  `SITE_NETWORKS`/`SITES_WITH_OPEN_CONNECTIVITY` config, the `next-api-credentials` Secret and the
  test-only mock service. A segment is born Available. If firewalls ever return, the shape above is
  the precedent — but it is history, not current design.
- **cluster-upgrade's MCE limb names the HostedCluster by ITS name, not the day1 file's.** The
  hostedclusters-setup chart renders `<cluster>.yaml` as `openshift-<clusterName>` in
  `hcp-<clusterName>`; the file's name names only the Argo CD Application. Planning takes the real
  name from `Day1Cluster.hosted_cluster(_namespace)` into `UpgradeContext`, and every MCE request
  carries it as optional `hosted_cluster` / `hosted_cluster_namespace` (`cluster` keeps meaning the
  file): the limb GETs the HostedCluster there, the evidence takes NodePools by that
  `spec.clusterName`, a sync allows `HostedCluster/<its name>` out of sync. None (a run planned
  before) = the HostedCluster named after the file, found by listing.
- **The hcAppset's Applications are MULTI-SOURCE: the chart, then day1 as `ref: values`.** Argo CD
  3.1 keeps one revision PER SOURCE there (`status.sync.revisions`, `operation.sync.revisions`,
  `syncResult.revisions`; the singular fields are empty). The limb pins the day1 commit in the
  source whose `ref` is `ARGO_VALUES_REF` and keeps the chart at the revision Argo CD last
  compared; its sync patch carries `sync.sources` (= spec.sources) AND one revision per source, as
  `argocd app sync --revisions <sha> --source-positions <n>` does — without `sources` the controller
  reads `$values` at day1's HEAD, not at the commit (`GetRefSources`, util/argo/argo.go,
  release-3.1). An Application with sources but no such ref is refused, never guessed.

## 5. Temporal SDK rules (gotchas — must follow)

- **Sandbox + Pydantic:** in workflow files wrap all `shared/` imports in
  `with workflow.unsafe.imports_passed_through():` (Pydantic C-extensions crash the sandbox; also
  skips reload). Worker entrypoints run outside the sandbox — import normally.
- **Connect ONLY through `shared/temporal_client.connect_temporal`** (workers AND `api.py`). It
  passes `pydantic_data_converter` (else Pydantic payloads can't serialize) and, when
  `TEMPORAL_TLS_CA` is set, TLS verified against that CA. Plain gRPC is the default and what
  every client inside the hub uses; a worker on another MCE reaching Temporal through a Route
  sets the CA (chart `temporal.tls.*`, `deploy/charts/server-lifecycle-worker/README.md`).
  Verification is never switched off.
- **Logging:** `workflow.logger` in workflows (no replay spam), `activity.logger` in activities.
  Shared compact formatter in `shared/logging_config.py` (silences `httpx`, strips the activity-info suffix).
- **Routing:** every `execute_activity` sets `task_queue=` to the target domain's queue (constants in
  `shared/consts.py`) so work lands on the right deployment.
- **Timeouts/retries:** every activity sets `start_to_close_timeout` + a `RetryPolicy` with UNBOUNDED
  attempts (no `maximum_attempts`/`schedule_to_close_timeout`) and a capped `maximum_interval` —
  transient outages are out-waited. So retries stop ONLY for CLASSIFIED failures: every known-permanent
  error must be in `non_retryable_error_types` (e.g. `SegmentNotFoundError`, `SegmentsManagerAuthError`)
  or raised by the activity as `ApplicationError(..., non_retryable=True)`. Unclassified permanent errors
  retry every minute forever — run sits RUNNING (not FAILED) with the failure on the activity in the UI.
  The ONE shape that carries a `schedule_to_close_timeout` is a BEST-EFFORT call whose
  `ActivityError` the workflow catches and answers itself (checker WF005 allows only that):
  add-server-to-mce's reads of server-scan after the install, remove-server-from-mce's PXE cleanup,
  and create-vm's four — the figures read of one KV cluster (120 s: whatever keeps it from
  answering, the cluster becomes a row of the table instead of holding its zone's lock), the
  create on one cluster (to its own 90-s deadline) and the check of a cluster that failed it
  (1 min; either way the run moves on to the next cluster, §4), and the cancel rollback's delete
  (10 min, then the run still ends CANCELED and says where the VM is).
- **Name an error type ONCE — build the non-retryable list from the CLASSES.** Temporal matches
  `non_retryable_error_types` against the error's type NAME, so a string literal with a typo in it
  reads as "retryable" and there is nothing to notice. add-server-to-mce keeps a
  `_PERMANENT_ACTIVITY_ERRORS` tuple of the exception classes and passes
  `[e.__name__ for e in ...]`; a wrong name is then an ImportError at worker startup. The same
  applies to the types raised FROM workflow code: `ApplicationError(..., type=ThatError.__name__)`
  with the class in `shared/exceptions.py` (marked WORKFLOW-RAISED there), never a literal — a
  literal keeps failing under the old name after the class is renamed, and the status endpoint and
  any alerting key on that name. Workflow-raised types must NOT appear in
  `non_retryable_error_types`: it is inert there, and an inert entry reads as a protection.
- **One Worker PER WORKFLOW, all in the one brain process:** a Temporal `Worker` polls exactly one
  task queue, and each workflow has its own queue — so `main_worker_init.py` holds a `_WORKER_SPECS`
  list of `(queue, [WorkflowClass])` and enters every `Worker` into a single `contextlib.AsyncExitStack`,
  which starts them concurrently and drains them all on one SIGTERM. Adding a workflow is ONE entry
  there plus its queue in `shared/consts.py` — never a second deployment. The per-workflow queue buys
  independent concurrency limits, drain/pause and backlog metrics; workflow queues never route to a
  different deployment (only ACTIVITY queues do, which is why those are per-domain instead).
- **Plain exceptions do NOT fail a workflow:** a non-FailureError in workflow code fails the workflow
  *task*, which retries forever (run hangs RUNNING). Deterministic failures raised FROM WORKFLOW code
  must be `temporalio.exceptions.ApplicationError(...)` — do NOT set `non_retryable=True` there (inert;
  workflow failures aren't retried). Activity-raised custom exceptions are fine — the SDK converts them
  to ApplicationError (`type` = class name); there `non_retryable=True` IS meaningful.
- **Single-model workflow argument only:** typed conversion is silently SKIPPED when payload count ≠
  declared `run()` param count (a `run(input, resume=None)` started with one payload gets a raw dict).
  Give `run()` exactly ONE Pydantic arg, wrapping public input plus any internal state (e.g.
  `InitializeSegmentRunArgs{input}` — the wrapper stays even with one field, because it is the shape
  the `run()` signature is pinned to).
- **Polling loops:** `workflow.sleep(...)` is a durable replay-safe server-side timer (never
  `time.sleep`). Changing poll constants is a non-deterministic change for in-flight runs.
  Bounded vs unbounded is a MEANING, not a style. MACHINE convergence gets a real deadline and fails
  loudly (precedent: allocate-segment's removed DHCP scope wait, 15 min on a 15s durable timer, then
  `DhcpScopeNotConverged` — §4). A wait on a HUMAN gets no deadline ever — back off to a capped
  interval and `continue_as_new` every N cycles so history stays bounded. THE LIVE PRECEDENT is
  provision-cluster's merge wait (`awaiting-merge`): `get_merge_request_state` every `_MERGE_POLL`
  (5 min, a durable timer, a constant — changing it is a non-deterministic change), no deadline
  ever, `continue_as_new` carrying `ProvisionClusterState` every `_POLLS_PER_RUN` polls (48, four
  hours) or when the server suggests it, so a continued run skips every phase whose outcome it
  restored and calls nothing twice (`tests/test_provision_cluster_workflow.py` pins it);
  `MergeRequestClosedError` when a person closes without merging. (initialize-segment's removed
  firewall-approval wait, §4, was the earlier precedent.)
- **httpx timeout < activity `start_to_close_timeout`** (currently 60s < 90s): give every
  `httpx.AsyncClient` an explicit `timeout=` below the activity timeout so a network hang fails the call
  and frees the worker before Temporal reaps the activity.
- **Per-invocation HTTP client:** create `httpx.AsyncClient` INSIDE each activity via `async with` so
  auth tokens/cookies scope to one invocation and never leak across concurrent runs.
- **External text in an activity error is BOUNDED** (checker AC007). History records an activity
  error verbatim, and Temporal's frontend replaces any failure over `limit.blobSize.error` (2 MiB by
  default) with a NON-RETRYABLE `Failure exceeds size limit.` — the original kept only as its
  cause, cut to 512 KiB — so a 5xx that should have been out-waited FAILS the run instead. An HTTP
  body or a dependency's message goes into an exception as its first few hundred characters: a
  slice (`resp.text[:500]`) or a bounding helper (`_bounded` in `server_scan.py` and the
  segment-lifecycle activities, which says how much was cut). Found 2026-11-09: an
  add-server-to-mce run failed at `acquire_servers` because `server_scan._detail` copied an error
  answer of more than 2 MiB whole (reproduced on a local dev server; server-scan's own `main` cannot
  send one, so it came from whatever answered at `SERVER_SCAN_URL`).
- **Git-subprocess activities** (checker AC008; `activities/git_repo.py`, THE one git client: cluster-lifecycle's
  `day1_writer.py` / `day2_sigs.py` / `day1_clusters.py`, upgrade-gitops's day1/day2/Brain modules and
  server-lifecycle's update-nodepool activities, the day1 reads through `activities/day1_repo.py`; no
  other module runs git): git runs via
  `asyncio.create_subprocess_exec` (no Python git dependency; the limb Dockerfile installs the binary),
  each command under a timeout below the git activities' 180s `start_to_close_timeout`. A WRITER opens
  ONE `git_repo.fresh_clone(...)` per attempt (day1 `main`: `day1_repo.fresh_main`) — a fresh
  `TemporaryDirectory`, so retries start clean — and works through the `Clone` it yields (`head`,
  `blob`, `commit_and_push`, `run_command`). A READER takes `git_repo.snapshot(...)` (day1 `main`:
  `day1_repo.main_snapshot`, which yields `Day1`): it asks the remote for the branch's head EVERY
  time and clones only when that head is not the commit this process already holds — the same
  commit is the same tree, so nothing cached ever answers for the remote, and an unreachable one
  fails as a clone does. The snapshot is shared and read-only (a repo's own script runs in a
  throwaway copy, `scratch`), and every read result carries the commit it read (`revision`). A
  `fresh_clone` block must reach `commit_and_push` (AC008). day1's layout
  (`sites/<site>/mces/<folder>/hostedClusters/`) is written down in `activities/day1_repo.py` ONLY
  (AC009): a read asks `Day1`, a write builds its path with that module's helpers, and a workflow
  whose limb holds no day1 token calls `list_day1_clusters` on the cluster-lifecycle limb — typed
  facts about one commit, not a generic git activity. Only what is GIT lives there: which file, and what
  "already done" means (create-never-overwrite, a blob compare-and-set, a folder present), stays in
  the limb's own module, inside the same clone as its push. Never generic git ACTIVITIES — a clone
  cannot cross activities, the check must share the push's clone, and one git worker would hold every
  limb's token (§2). The push token is injected into the clone URL in
  memory only and SCRUBBED from every error message BEFORE the exception is constructed — activity
  errors are recorded verbatim in Temporal history and the UI, so redacting at the logging layer alone
  is too late. Every clone first runs
  `git ls-remote --exit-code --heads <url> refs/heads/<branch>` (the FULL ref — a bare pattern
  tail-matches): exit 2 is `ValuesBranchNotFoundError`, any other failure `ValuesRepoGitError`
  (unreachable is not missing). The CALLER classifies both: the workflows retry a missing branch
  (`main`, a sig's default branch, provision-cluster's own branch just checked — a repo to fix, not a
  run to fail; `tests/test_error_classification.py`), and upgrade-gitops's `write_incident` turns it
  into a note (Brain never holds a gate). Its non-retryable caller was allocate-segment's
  caller-named branch, gone since 2026-11-11 (§4).
- **Cross-workflow orchestration — two shapes, chosen by whether the parent's OUTCOME depends on
  the child.** A FAN-OUT (one run per item, each answering to its own id and failure, the /bulk
  philosophy) starts siblings DETACHED: `workflow.start_child_workflow(...,
  parent_close_policy=ParentClosePolicy.ABANDON)` on the sibling's workflow queue, catching
  `WorkflowAlreadyStartedError` as an `already_running` report item, never waiting. No current
  workflow fans out (convert-segment, since removed, §4, was the precedent). A STEP the parent cannot
  continue without is an AWAITED child: `workflow.execute_child_workflow(<Class>.run, <RunArgs>,
  id=<its own id from shared/workflow_ids.py>, task_queue=<its own queue>,
  parent_close_policy=ParentClosePolicy.ABANDON)`, `WorkflowAlreadyStartedError` mapped to a named
  WORKFLOW-RAISED `<Child>AlreadyRunningError`, `ChildWorkflowError` to `<Child>FailedError` carrying
  the child's own type and message (unwrapping an `ActivityError` behind it) — provision-cluster
  → allocate-segment, add-server-to-mce's cancel rollback → remove-server-from-mce, and
  uninstall-server-from-hc → remove-server-from-mce for runs recorded before 2026-11-19
  (a new uninstall starts no child, §4). ABANDON either way: a child
  verifies by read-back and must never be cut off between the mutation and the verify; its id is
  the dedup key a re-run or a manual start meets. The builders-in-`shared/workflow_ids.py`
  arrangement is what makes both possible (a workflow can never import a router).
- **NEVER cancel a sibling to make room for your own work — choose inputs that cannot have one.**
  A cancel result cannot tell you whether anything was running: Temporal ACCEPTS a cancel against an
  already-CLOSED execution and reports it accepted, failing only for an id that has NEVER existed. So
  `cancel()` cannot distinguish "killed a live sibling" from "no-op'd against one that finished days
  ago", and the answer FLIPS once the closed run ages out of retention. NARROWING THE INPUT to
  eliminate the interaction beats every attempt to detect it. Learned on convert-segment (since
  removed, §4), which used to cancel a stale sibling run before re-typing a segment: the version that cancelled unconditionally
  reported a phantom cancellation and paid a 30 s pause on nearly every segment, and the version that
  gated the cancel on status was correct but kept the machinery — while restricting the SELECTION to
  segments that could not have a live sibling removed the problem outright. Apply the same move to the
  next such interaction rather than adding detection.
- **Runs that must not act at the same time take a LOCK WORKFLOW — the per-zone lock is THE
  serialization precedent** (`workflow_domains/vm_lifecycle/kv_zone_lock.py`, create-vm, 2026-11-20).
  When two runs deciding from the same live state would decide the same thing (ten create-vm runs
  all seeing one empty cluster), neither a workflow id (they are legitimately separate runs) nor
  narrowing the input (the bullet above) helps: one long-lived workflow per contended thing hands
  it to one run at a time. The shape, each part there for a reason:
  - **It holds only the line and the lease — never the state the runs decide on.** The state stays
    where it is true (each KV cluster's own figures); the holder keeps the lock until what it did
    is VISIBLE there (the VM object exists), so the next holder reads it and nothing is remembered
    between runs. A lock that remembered a "pending" list would be a second copy to keep in step.
  - **Code talks to code by SIGNALS** (Updates are for people, below): `request_lock`,
    `lock_granted`, `release`, `withdraw`. Workflow code cannot signal-with-start, so a run asks by
    `start_child_workflow(..., parent_close_policy=ABANDON)` with itself as the first of the line
    and, on `WorkflowAlreadyStartedError`, signals the lock that already runs. The lock sets its
    state in `@workflow.init`: a signal delivered with the first task is handled before `run()`.
  - **A run is known by its RUN id**, and a grant is signalled to exactly that run
    (`get_external_workflow_handle(id, run_id=…)`): workflow ids are reused (slots), and a later run
    must never receive a grant it did not ask for.
  - **A waiter that is gone costs nothing.** Signalling a closed run fails (an `ApplicationError`,
    `ExternalWorkflowExecutionNotFound` — measured, also for a run id whose workflow id a newer run
    now uses): the lock catches it and grants the next in the same task.
  - **Every grant is a LEASE** (5 min, a durable timer), its end sent with the grant; a release
    carrying another lease's id is ignored. What must not happen after the lease is enforced where
    it would happen: the holder passes the lease's end on, and the worker refuses to act after it.
  - **A waiter asks again** on a timer (2 min) and the lock keeps a run in its line once: a lock a
    person terminated is started afresh by the next ask, so nothing strands. A run that gives up
    (its own bound, a cancel) sends `withdraw`; a grant a run is not waiting for is released by
    its signal handler at once.
  - **The lock never fails and calls no activity** (tests pin both): a plain exception there would
    hang it and stop every run of its zone. History is bounded by `continue_as_new` every N grants,
    carrying the line and the lease, after `all_handlers_finished`; signals go to the workflow id,
    never a run id, so they reach the continued run.
- **A workflow exit path acts only on an ordinary failure or on a cancel asked of ITS OWN run —
  never in a `finally`, never on `workflow.in_workflow()` alone** (checker WF017). There are THREE
  ways out of a run's coroutine besides its own end, and two of them may call no workflow API:
  (1) a cancel a person asked for — the one that rolls back; (2) a worker dropping the run from its
  cache (a full cache, a failed task), which cancels the run's tasks exactly as a cancel does, with
  no reason set; (3) **a worker that SHUTS DOWN and simply drops the runs it had in memory**: their
  coroutines are not cancelled at all, and Python throws them away later, when it collects — INSIDE
  whichever workflow task is on the thread then, of ANY workflow of the brain. A `finally` or an
  `except BaseException` wakes up there with `GeneratorExit`, and `workflow.in_workflow()` is TRUE:
  it asks whether some workflow's task is running, not whose. What the path then sends — a signal,
  an activity — is recorded in THAT run's history, which no replay of it ever produces again: the
  other run fails every workflow task from its next replay on (`NondeterminismError`), until a
  person resets it. create-vm did exactly this until 2026-11-24 (`finally: await
  self._release(lease)`, and `in_workflow() and cancellation_reason() is not None` — which is also
  true when the OTHER run is the one being cancelled): found as a `release` signal for a lease the
  recorded run never held, sixteen events before it was granted any, in a history that then failed
  to replay — now and then in the suite's record-then-replay test (two runs in twelve once Python
  was made to collect often), and in production the same window opens at every brain rollout that
  finds a create-vm run inside its lock. So: the run keeps its own run id (`workflow.info().run_id`, read in
  `@workflow.init`) and every exit path asks `_own_task()` — in a workflow AND in this run — before
  it touches a workflow API (`_cancel_requested()` = that and `workflow.cancellation_reason() is not
  None`; temporalio ≥ 1.32, the brain's floor since create-vm); it acts in `except`, on
  `isinstance(err, Exception)` or `_cancel_requested()`, and re-raises; an `except BaseException`
  that would swallow re-raises when it is not in its own task. The dropped run loses nothing: it is
  still open on the server, and the next brain replays it. `tests/test_create_vm_workflow.py`
  (`…dropped_at_shutdown…`, `…being_cancelled`) pins it with a bystander workflow that collects
  garbage inside its own task; no other workflow of the repo awaits on such a path (scanned
  2026-11-24), and WF017 keeps a new one from doing it.
- **Evolving a model that crosses the boundary — history and a lagging limb both hold OLD payloads.**
  CI ships the brain BEFORE the limb (`build.yml`: the limbs' `build` job `needs` the brain's), and every
  in-flight run replays payloads recorded by the previous code. So (a) a field ADDED to a model an
  activity RETURNS is simply absent from those payloads: a workflow check on it gates on
  `"field" in model.model_fields_set` — the key's presence — never on its value, or replay fails the
  run where the original didn't (nondeterminism) and a new brain fails every run against an old limb.
  allocate-segment's read-back `type` check is the precedent; the limb always emits the key (a null
  included), which is what keeps the gate honest. (b) A model decoded from history (workflow input,
  activity input/result) never gets `extra="forbid"` — a removed field still sits in old payloads and
  would wedge the run. Strictness about unknown fields goes on an API-edge subclass instead
  (`InitializeSegmentRequest` in the router, refusing the retired `type`). (c) A field ADDED to a
  model decoded from history defaults to `None` there and is made REQUIRED only on the edge subclass;
  the reader refuses `None` with a named error rather than guessing (`values_branch`: optional on
  `AllocateSegmentInput`/`ClusterValuesAppendRequest`/`ClusterFileLookupRequest`, required on
  `AllocateSegmentRequest`, `ValuesBranchMissing` from the workflow and the activities). (d) Changing
  an activity's argument from a primitive to a model means a lagging limb cannot decode it — the SDK
  retries that, so runs in the brain-before-limb window are DELAYED; prefer it when keeping the old
  shape would let the old limb act on the wrong input and FAIL them (`locate_cluster_file(cluster:
  str)` would have searched `main` and raised `ClusterFileNotFoundError`).
- **A person's decision on a running workflow is a Temporal UPDATE with a validator, never a
  signal** (upgrade-cluster: approve, approve-admin-ack, continue, confirm-day2, pause, resume,
  abort, retry, tier-2 actions). The caller must learn AT ONCE whether the run took it: a `continue` while no NodePool
  waits is a 409 naming what the run does wait for, where a signal would vanish into a run that
  ignores it. The VALIDATOR only reads state and refuses by raising
  `ApplicationError(type=UpgradeUpdateRefusedError.__name__)`; the HANDLER is a plain `def` that
  records who and why and sets a flag; the `run` loop does all the work, so every side effect stays
  in one ordered place. A human wait is a bare `wait_condition` on that flag — no timer, no poll, no
  deadline — so a week between two NodePools adds nothing to history. `@workflow.init` takes the run
  argument so an Update reaching a CONTINUED run before `run()` starts is validated against the
  restored state; `await workflow.wait_condition(workflow.all_handlers_finished)` precedes every
  `continue_as_new` and every return. The router's decision routes send the Update and map the
  answer (409 refused, 404 no such run, 504 no brain worker); they never read the run's state to
  second-guess its validator.
- **A long MACHINE wait is ONE heartbeating activity, not a workflow poll loop**
  (`watch_control_plane`, `watch_nodepool`, `watch_nodepool_gate`): it polls inside the activity,
  heartbeats every poll (`heartbeat_timeout` 2 min), keeps its progress snapshot and clocks in the
  heartbeat details so a retry after a dead worker resumes them, and returns `done | stalled |
  deadline` on its own clock, with `start_to_close_timeout` only a margin past that deadline. A
  three-hour control-plane upgrade is then a handful of history events. WHICH clock is a decision:
  a wall clock since the first attempt (create-vm's ready wait) also runs through an outage, so a
  wait whose deadline means "the cluster had this long to finish" counts only the time between two
  polls that both answered (delete-vm's `await_vm_gone`, §4) — and what such a wait watches is
  handed to it from the run's history, never found again by an attempt that starts later. Retries stay unbounded per
  the rule above. Started with `workflow.start_activity` (`TRY_CANCEL`) so an abort can cancel it;
  commits and syncs are `WAIT_CANCELLATION_COMPLETED`, so a cancel never abandons a push half-way.
  The one write that is `TRY_CANCEL` is the Brain incident after an agent answer
  (`dispatch.until_done`, gated `upgrade-agent-incident-yields-to-stop`): it is append-only and
  nothing about the upgrade depends on it, so an abort never waits on Brain.
- **Cancellation does not stop work in flight — so a stop is a FLAG checked before every act.**
  Cancelling a task or a child that is awaiting a non-heartbeating `WAIT_CANCELLATION_COMPLETED`
  call is swallowed: the call finishes, its result comes back as if nothing happened, and the code
  goes on to its next commit (found in review, 2026-10-31). So upgrade-cluster never relies on it
  for anything but a `TRY_CANCEL` watch: an abort or a pause is checked by `_checkpoint()` right
  before every commit, sync, child start and agent call, and the parent drives its control-plane
  child with SIGNALS (`stop`, `hold`, `retry`; the child reports needs-human back by signal) — code
  to code, fire-and-forget, where Updates are for people. A child that may outlive the parent's
  patience (ABANDON) must be stoppable this way.
- **No `schedule_to_start_timeout` on a long-lived run's activities.** Temporal never retries
  that timeout, so on upgrade-cluster it would FAIL a weeks-long run the moment a limb pod took five
  minutes to come back. Its activities wait for a worker instead (visible as a pending activity);
  the one exception is the agent call, whose timeout the workflow catches and answers as
  needs-human. Short runs (add-server-to-mce and its siblings) keep theirs as the "no worker" signal.
- **The one bounded retry: the LLM agent call** (`invoke_upgrade_agent`, `maximum_attempts=2`, per
  Temporal's AI guidance). Two attempts cover a dead worker; past that, an agent that cannot answer
  is answered in the workflow as `needs_human`, so the run waits for a person instead of retrying
  forever. Safe because the request carries an invocation id the agent service keys its run and
  verdict on: a retry joins or returns the first attempt's. The limb raises only when a second
  attempt can help (unreachable, timeout, 429, 5xx); a refusal (other 4xx) is non-retryable, and a
  reply that is not an AgentVerdict comes back as `needs_human`.
- **Replay tests guard runs in flight** (`tests/test_upgrade_cluster_replay.py`): histories recorded
  from scenarios on the fake fleet replay at once with `temporalio.worker.Replayer`, and GOLDEN
  histories committed under `tests/histories/cluster_upgrade/` replay on the current code. A golden
  replay failing means the change needs `workflow.patched()` — never re-record to make it pass;
  re-record (`UPDATE_REPLAY_HISTORIES=1`) only while no run of the old shape can exist. **A golden
  is recorded with the PRODUCTION constants**: a workflow whose flow depends on a constant (a
  lease's length, a guard, a re-ask interval, a continue-as-new threshold) does not replay a
  history recorded with that constant patched — create-vm's dead-worker scenarios failed exactly so
  when first recorded under shrunk waits. `tests/manual_time.py` is how they are recorded on the
  real waits: it moves the test server's clock by hand, past tasks nobody serves (the server never
  skips time by itself under one, and left to skip freely it jumps an idle long-lived workflow to
  its ten-year timeout). kv-zone-lock is ALWAYS in flight, so its goldens
  (`tests/histories/kv_zone_lock/`) matter on every deploy.

## 6. Idempotency (required for all activities)

- Network calls fail and Temporal retries — activities must be strictly idempotent: UPSERTs,
  check-before-create, idempotency keys; treat "already exists / already done" as success. Examples:
  `create_segment` treats an existing segment MATCHING the definition as success and only fails on a
  genuine disagreement (`SegmentConflictError`) — comparing identity only (site, vlan, epg), never
  allocation state (type, status, cluster), so a segment allocated since it was created still
  matches; `allocate_segment` is
  idempotent SERVER-side per (cluster, site, type) — a repeat call returns the existing allocation;
  `append_allocation_to_cluster_values` compares the ALLOCATION, not the block text — a marker block
  already recording this `vlanId` on this `network` is success (`changed=False`, nothing pushed)
  whatever else it contains, and any other allocation (or a block too mangled to read one out of)
  raises `ClusterValuesConflictError`. Text equality was the earlier rule and it made a per-cluster
  operator edit — a hand-picked `startRange`/`endRange`, an extra exclusion — fail every later
  re-run as a phantom conflict. The workflow's promise is that the file records the vlan and network
  the Segments Manager confirmed; the DHCP detail around them belongs to day1 and to the operator,
  which also means a changed exclusion POLICY is never retrofitted onto an allocated cluster.
  `release_segment` answers 200 for an already-Available segment and `delete_dhcp_scope` 204 for an
  absent scope, both server-side; release-segment itself COMPLETES as a no-op when the cluster holds
  nothing, so a repeated trigger is harmless.
- **Verify after mutate, before recording:** allocate-segment reads the allocation back
  (`get_segment`: status/cluster/vlan/type must all match — the allocation WROTE the type) BEFORE the
  git write, so the values repo can
  never record a vlan the Segments Manager does not confirm. A mutation whose outcome another system
  will act on gets a read-back check between the mutation and the recording. release-segment reads
  the scope back after its DELETE (before the segment returns to the pool) and the segment back
  after its release. create-vm's worker reads back the VMs carrying the run's id after its create,
  before the VM's name is recorded: the run must hold exactly ONE, and a second (a create the API
  server finished after its caller gave up on it) is converged on the oldest by whichever attempt
  meets the pair (§4).
- **Idempotent by the RUN's id when nothing else identifies the thing** (create-vm, 2026-11-20): the
  VM's name is drawn inside the activity, so a retry cannot find "its" VM by name. The request
  carries the run id, the worker labels the VM with it, and every attempt looks for that label
  FIRST — before any time bound, so a retry after the lease still returns what it made. Not the
  workflow id: a slot's id is reused by later runs, which must each get a new VM.
- Workflow IDs are deterministic (`initialize-segment-<network address, CIDR mask dropped>`,
  e.g. `initialize-segment-130.154.20.0`; `allocate-segment-<TYPE>-<cluster>`;
  `release-segment-<TYPE>-<cluster>`) for natural dedup — a
  duplicate trigger while running gets HTTP 409 (in the bulk route, an `already_running` item).
  Builders in `shared/workflow_ids.py`.
- **Cross-workflow races on one record: compare-and-set, server-side.** When two runs may mutate the
  same record, the mutation carries the value it expects to replace and the owning service applies it
  atomically, so the loser gets a 409 (classified non-retryable) instead of silently overwriting the
  winner — while a plain Temporal retry must still short-circuit as success when the stored value
  already equals the NEW one. Precedent: convert-segment's `expected_type` (removed with the
  workflow, §4). release-segment needs it and cannot have it yet: the Segments Manager's release
  checks no owner, so the workflow RE-READS the owner right before releasing
  (`AllocationOwnerMismatchError` if another cluster holds it, success if already Available) — a
  client-side stand-in with a residual window. The proper fix is an `expected_cluster_name` on the
  SM's release endpoint (a segments-manager change).

## 7. Strict validation & clean typed state

- **Strict, absolute verification** — no broad/tolerant thresholds; fail loudly if even one of N
  required conditions is missing (an unknown site, an empty pool, a search hit whose status or
  cluster assignment contradicts what was asked for).
- **Typed state only** — pass Pydantic models/dataclasses across boundaries, never untyped dicts.
  Structural validation on our own models; trust black-box externals rather than re-deriving their rules.

## 8. Configuration

- Env vars are the config surface (`.env.example` documents them; `.env` gitignored).
- **ConfigMaps split by scope, not chart-convenience:** `workflows-orchestrator-config` (GLOBAL; owned by the
  always-present `workflows-orchestrator` brain release) holds only shared values — `TEMPORAL_HOST`,
  `TEMPORAL_NAMESPACE`, `SEGMENTS_MANAGER_URL`. `<domain>-config` (owned by that domain's
  chart) holds its own endpoints/policy — e.g. `cluster-lifecycle-config` = provision-cluster's keys
  (`DAY1_REPO_URL`, `DHCP_EXCLUSION_OCTET_RANGES`, `DAY2_SIG_REPO_URL_TEMPLATE`, `SHORTCUTS_API_URL`,
  `SHORTCUT_OWNER`, `CLUSTER_CONSOLE_URL_TEMPLATE`, and create-vm's `HEADROOM_BUFFER_PERCENT` when it is
  to differ from its default; the credentials from the shared `redbull-git-token`
  Secret under its own key, `REDBULL_GIT_TOKEN` — the group token, day1 and the sigs alike — and
  `SHORTCUTS_API_TOKEN` from the out-of-band `shortcuts-api-token` Secret) and `segment-lifecycle-config` = release-segment's
  `DHCP_API_URL` (its token in the `dhcp-api-token` Secret) — the day1 and DHCP-policy keys it
  rendered for allocate-segment are dropped only once its image stops reading them. `vm-lifecycle-config`
  is the exception to "a limb mounts the global ConfigMap": it lives in a KV cluster, where no brain
  release exists, so (like the per-MCE limbs' charts) it renders its own `TEMPORAL_*` next to the
  cluster's own values (`KV_CLUSTER`, `VM_*`). A
  domain worker mounts BOTH + its Secrets, so the
  brain release must install before any limb (else `CreateContainerConfigError` on the missing
  global ConfigMap) — and the chart must ship the new keys BEFORE (or with) an image that requires
  them, or the worker crash-loops on its fail-fast settings.
- **Settings are ONE MODULE PER WORKER, `shared/settings/<limb>.py`, and the package re-exports
  nothing** (checker SH007). Until 2026-11-14 they were one `shared/settings.py`: every image loads
  its own class from it, so every image loaded the file, and each of its 34 commits in ten weeks —
  nearly all "this worker gets a key" — rebuilt and re-tagged ten images, each one more image to
  mirror into the air-gap. Split with the operator the day CI began building an image only for the
  files it LOADS (§9): import a class from ITS module, `from shared.settings.vm_lifecycle import
  VmLifecycleActivitySettings`; an import in `shared/settings/__init__.py` would undo it.
  **`shared/exceptions.py` stays ONE file on purpose** (the operator, same day): the brain needs
  every class anyway (the non-retryable lists), several are raised by more than one limb (the git
  errors), and "errors live in one place, named through the class" is what makes a mistyped type
  fail at startup (§5) — so a new exception still rebuilds every image, and that is accepted.
  The classes: `TemporalSettings` (workers + api.py),
  `SegmentLifecycleActivitySettings`, `ServerLifecycleActivitySettings`,
  `ServerProvisioningActivitySettings`, `VmLifecycleActivitySettings` (each that domain's activity
  worker only; this one is the worker INSIDE a KV cluster and holds only that cluster's own values:
  `KV_CLUSTER`, REQUIRED and NEVER canonicalised — it must equal the day1 file name — parsed at start
  as `ocp4-<lifecycle>-kv-<site>-<zone>-<id>`, the site and the zone read from it (a request for
  another site or zone is `KvClusterMismatchError`); no MAC value (`VM_MAC_PREFIX` removed
  2026-11-25, KubeMacPool assigns, ignored if a chart still renders it); `VM_STORAGE_CLASS_NAME`, REQUIRED; `VM_NETWORK_NAME`, the NAD, default vm.yaml's, and
  `VM_NODE_SELECTOR`, JSON; `VM_REDFISH_LABEL`; `VM_ROOT_DISK_SIZE`. No day1 key, no token, no
  buffer since 2026-11-20) and
  `ClusterLifecycleActivitySettings` (`HEADROOM_BUFFER_PERCENT`, create-vm's placement buffer, ONE
  value for every KV cluster, 0..90, default 15 — optional, so the image may ship before the chart
  renders it; `DAY1_REPO_URL`, `REDBULL_GIT_TOKEN` — the air-gap Secret's key, the
  operator 2026-11-11; `DAY1_GIT_TOKEN` accepted as an alias, the air-gap name winning,
  `DHCP_EXCLUSION_OCTET_RANGES` with its validator — moved here from segment-lifecycle 2026-11-11 —
  `DAY2_SIG_REPO_URL_TEMPLATE`, which must name `{sig}` (one GitLab project per sig),
  `DAY2_GIT_TOKEN`, the day1 token when empty, and the shortcut's `SHORTCUTS_API_URL`,
  `SHORTCUTS_API_TOKEN`, `SHORTCUT_OWNER` and `CLUSTER_CONSOLE_URL_TEMPLATE`, which must name
  `{cluster}` and nothing else). `ServerLifecycleActivitySettings` carries `DAY1_*` too, OPTIONAL
  there — update-nodepool's alone, §4 — and the server-lifecycle worker also builds
  `ClusterUpgradeActivitySettings` (`shared/settings/cluster_upgrade.py`) for the two
  cluster-upgrade activities it re-registers, from the same pod env. Field names = Helm ConfigMap/Secret keys
  lowercased — keep aligned with redbull-platform's `gitops/charts/workflows-orchestrator/templates/config.yaml`
  (global), `gitops/charts/cluster-lifecycle-worker/templates/config.yaml` (day1 URL +
  DHCP policy + day2 sig URL template, and `HEADROOM_BUFFER_PERCENT` when it is to differ from 15),
  `deploy/charts/vm-lifecycle-worker/templates/configmap.yaml` (a KV cluster's own values) and `gitops/charts/segment-lifecycle-worker/templates/config.yaml`
  (DHCP API URL + the credential Secrets). Which ConfigMap
  a key lives in is INDEPENDENT of which settings class declares it (pydantic reads the flat merged pod
  env — `SEGMENTS_MANAGER_URL` sits in the global ConfigMap yet stays a
  `SegmentLifecycleActivitySettings` field; the brain ignores extras via `extra="ignore"`). Do NOT
  import settings from inside a workflow definition (sandbox) — only from entrypoints / api.py / activities.
- **`DHCP_API_TOKEN` is a COPY of the DHCP API's own token, rendered from git.** The source of truth is
  redbull-platform's `gitops/charts/dhcp-scope-manager/charts/dhcp-api-token/values.yaml`, whose Secret
  is pinned to the `dhcp-scope-manager` namespace (envFrom cannot cross namespaces). The
  segment-lifecycle-worker chart renders its own `dhcp-api-token` Secret from `secrets.dhcpApiToken`,
  committed — never `oc create secret` (invisible to git) and never a `lookup`-generated value (Argo CD
  re-mints it every sync). Rotate both values in ONE commit and roll the worker; a stale copy fails
  every release with `DhcpApiAuthError` (non-retryable), never silently. `secrets.existingDhcpSecret` is
  the ESO hook, like the others.
- **ConfigMaps hold minimum, operator-editable data** — anything changeable without a rebuild (today
  just `DHCP_EXCLUSION_OCTET_RANGES`) is expanded/validated in code (fail-fast at worker startup)
  rather than baked into an image. Its VALUE lives in the chart's `values.yaml`
  (`config.dhcpExclusionOctetRanges`) and the ConfigMap template only renders it — an operator knob is
  reviewed as structured YAML next to every other tunable, not as JSON embedded in a template. (This
  reverses the earlier rule that such knobs sat in the template itself.)
- **The DHCP policy is ONE knob KEYED BY SEGMENT TYPE, and /24 is ASSERTED:**
  `DHCP_EXCLUSION_OCTET_RANGES` (a type -> last-octet-ranges map, e.g.
  `{"HC": [[1, 10], [241, 254]]}`) is the whole DHCP surface. Per type because each type reserves a
  different slice of its /24 and the lookup is DYNAMIC (the allocation's type) — a MAP, not a set of
  flat per-type keys, because nothing statically references one type. A type that is NOT LISTED
  excludes nothing — a type earns an entry by reserving part of its /24, and nobody should have to
  write `MCE: []` to say "nothing". `HC` is the one required key: the only type allocate-segment
  allocates today, where a forgotten policy would quietly hand out the addresses production reserves
  instead of failing. Types listed ahead of the code that allocates them are validated the same way
  now. The type travels to the activity on `ClusterValuesAppendRequest.type`; `build_dhcp_values`
  takes ONE type's ranges — the lookup lives in the activity.
- **The written block is NETWORK + EXCLUSIONS, never a distribution range.** `startRange`/`endRange`
  are deliberately omitted: absent both, `dhcp_scope_manager` derives `.1-.253` (stopping short of
  the `.254` gateway it derives for a /24), and the exclusions carve the ends back out of that —
  which is what the real day1 files do, and what production expects. Deriving bounds here instead
  would duplicate a derivation that already exists in the DHCP API, its CI validator and the chart,
  and all three would have to agree forever. It also makes "no exclusions" a non-case rather than a
  special one: the block is just a network, exactly like a hand-written minimal cluster file. Anything
  that verifies a live scope (the removed DHCP wait did; a future create-cluster step will) therefore
  compares its EXCLUSIONS against what was pushed — comparing the derived range would only confirm
  the DHCP API agrees with itself. `build_dhcp_values` rejects any non-/24 segment with `UnsupportedSegmentPrefix`;
  supporting another mask is a deliberate refactor that must also emit `subnetMask` + `gateway`
  (the DHCP stack derives the `.254` gateway for a /24 only).

## 9. Deploy & run (local)

- NO chart creates a Namespace (each deploys into whichever namespace the release targets —
  `helm install -n <ns> [--create-namespace]`, or redbull-platform's `namespaces` release pre-creates
  it): redbull-platform's `gitops/charts/workflows-orchestrator/` (ConfigMap + brain + SA),
  `gitops/charts/segment-lifecycle-worker/`, `gitops/charts/server-lifecycle-worker/`,
  `gitops/charts/server-provisioning-worker/` (each: ConfigMap + Secrets + limb +
  SA; the provisioning one needs no RBAC, only egress to the iDRACs, OME, the naming service and
  server-scan), `gitops/charts/upgrade-gitops-worker/` (the cluster-upgrade hub limb; its only
  copy, moved from `deploy/charts/` on 2026-11-02) and `gitops/charts/cluster-lifecycle-worker/`
  (provision-cluster's hub limb, which also lists day1's clusters for other domains and holds
  create-vm's placement buffer: ConfigMap + limb + SA, `automountServiceAccountToken: false`; it
  NAMES `redbull-git-token` for `REDBULL_GIT_TOKEN`, the one group token for day1 and the sigs;
  built by CI's `build` matrix like every limb, §9). This repo ships the charts of the limbs that
  run OUTSIDE the hub — per MCE, per KV cluster — written to be copied into
  redbull-platform for Argo CD (example ApplicationSet in `deploy/argocd/`):
  `deploy/charts/server-lifecycle-worker/`, `deploy/charts/cluster-upgrade-worker/`,
  `deploy/charts/upgrade-agent-worker/` and `deploy/charts/vm-lifecycle-worker/` (2026-11-20:
  one release INSIDE each KV cluster, its ConfigMap that cluster's own values, a ClusterRole of
  exactly the verbs `activities/vm_lifecycle/kubevirt_api.py` uses — list nodes and pods, get
  namespaces, get/list/delete PVCs, get/list PersistentVolumes, get/list/create/delete
  VirtualMachines, list VMIs, get/delete DataVolumes, delete VolumeImportSources, list
  VolumeSnapshots (the claim, volume, import-source and snapshot verbs since 2026-11-26, for
  delete-vm, §4: ship the chart BEFORE the image) — and NO Secret;
  the render fails without `kvCluster` or the storage class;
  until then `gitops/charts/vm-lifecycle-worker/` was a hub chart with two agent-token Secrets)
  — the last three copied into redbull-platform's
  `gitops/charts/<same name>/` BY CI (`sync-upgrade-charts`, `scripts/sync_platform_charts.sh`,
  the operator's request 2026-11-02): on every `main` push that changes them, the whole chart,
  keeping the image tag and chart version the build jobs wrote there, rendered, committed and
  pushed before the image jobs run. Edit them HERE only; a hand edit there is overwritten. The
  script keeps a destination's image only when the image sits where the chart keeps it
  (`.image.repository`): redbull-platform's old hub `vm-lifecycle-worker` had it under
  `activityWorker.image`, and yq's `null` for the missing field would have been written back as
  the repository and the tag — a destination of another shape is REPLACED, keeping only its chart
  version and appVersion (the `<chart>/vX.Y.Z` tags go on from them), and its image tag is
  written by the image's next build (`.github/image-builds.json`: `helm-image-path: image`).
  server-lifecycle-worker's redbull-platform chart is its own reshaped chart, not synced. On a
  remote MCE or KV cluster each renders its own Temporal endpoint, since no brain ConfigMap exists
  there; `deploy/` is in `.dockerignore` and triggers no image build (those three charts trigger
  the sync). The server-lifecycle limb is the first to need real RBAC: it creates
  secrets, metal3.io/baremetalhosts and agent-install.openshift.io/nmstateconfigs in the target
  namespace, where segment-lifecycle only makes HTTP and git calls — and, for uninstall-server-from-hc,
  a ClusterRole over CAPI Machines, AgentMachines, HostedClusters and Secrets (§4) — the same
  grant update-nodepool's scale-down uses to read and mark Machines, plus NodePools get/list and a
  Role on Argo CD Applications (get/patch) in `argo.namespace` for update-nodepool's read and sync.
  cluster-upgrade-worker holds NO credential Secret — its ServiceAccount is the whole identity —
  and its RBAC is derived call by call from `activities/cluster_upgrade/activities.py` (its
  README's table): a ClusterRole reading HostedClusters, NodePools, MachineSets, IDMS and Secrets
  (the admin kubeconfig) and deleting pods (`delete_hcp_pod`), a Role in `ARGO_NAMESPACE`
  (applications get/list/patch — list for a lab cluster's day2 Applications, D27) and one on ConfigMap
  `hypershift/supported-versions`. Never a write to a
  HostedCluster or NodePool (DESIGN D1). **The three cluster-upgrade images are VERSION-TAGGED like
  every other** (the operator, 2026-11-02: "use tags and not commit sha"): each is a row of
  `.github/image-builds.json`, built by the reusable workflow in CI's `build` matrix, so a `main` build publishes `vX.Y.Z`, bumps `image.tag` in
  redbull-platform's `gitops/charts/<limb>-worker/values.yaml` and tags `<limb>-worker/vX.Y.Z`
  there. Until 2026-11-02 they were image-only jobs tagged `<branch-slug>-<short sha>` with the tag
  set by hand; do not go back to that. None of the three has a `gitops/services/` entry yet, so
  their bumps deploy nothing; deploy them by adding one (per MCE for the two per-MCE limbs).
  update-nodepool does NOT need them: it runs on the server-lifecycle worker alone (§4). The
  domain keeps THREE images, one per limb (§2): the operator confirmed it the same day.
- **CI builds an image only when a file IT LOADS changed** (`scripts/image_changes.py`, the
  `changes` job; with the operator, 2026-11-14). The rule used to be by folder — `shared/` rebuilt
  every Python image, `activities/` every limb — so the GitLab-only change to ONE limb re-tagged
  ten images. Now each image's files are followed STATICALLY from its entrypoint (the Dockerfile's
  `python -m` module; the brain also from `workflow_domains.api`, which the chart starts from the
  same image) through every `import` statement, lazy ones included, plus its own Dockerfile and
  requirements; `tests/test_image_changes.py` imports each entrypoint in a fresh interpreter and
  checks that everything really loaded is in that set, so the script can over-build, never
  under-build. WHEN IT CANNOT TELL, IT BUILDS WIDE: a data file in a copied folder (the UI pages'
  `index.html`), a deleted or renamed path, a module that does not parse, `.dockerignore`, a crash
  of the script. **What only decides how CI runs is NO image's input** (the operator, 2026-11-16,
  narrowing the rule above: until then `build.yml`, the build rows and the script itself rebuilt
  every image, so a CI-only change gave all ten a new version with the same contents, ten images
  to mirror for nothing): `build.yml`, `image_changes.py`, `release_notes.py` and their tests
  rebuild nothing, and `.github/image-builds.json` rebuilds the images whose ROW changed, the
  base's rows against today's, as data (`--base`; a reformat or reorder changes no row; a base
  from before that file is read from its `build.yml`, whose jobs then held the same inputs; rows
  unreadable at the base build every image). That holds because every input an image's build
  gets is in its row, `build.yml` none (`tests/test_image_builds.py`). So a fix to how the script
  finds an image's files rebuilds nothing by itself: the image it under-built waits for its next
  change; change `.dockerignore` to rebuild every image on purpose. An interface (`shared/interfaces/<limb>.py`)
  is loaded by the BRAIN only — the limb registers by name and never imports it. Each limb image
  still CONTAINS the whole `activities/` tree (the Dockerfile's COPY); it is not rebuilt for files
  it never imports. Every image is a row of `.github/image-builds.json` (the reusable workflow's
  five inputs for it, `chart-repo` included); `image_changes.py --first` hands
  `build-workflow-brain` the brain's row and `--matrix` hands the `build` job every other listed
  image's, so the workflow never decides a second time (checker DL002: a limb's row names its own
  image and chart). A new limb is found from its Dockerfile and adds only that row. **The diff base is the
  last push on the branch whose WHOLE run succeeded**, not the previous push: with per-image
  precision an image whose build failed, or whose push GitHub skipped (a CI-skip marker in the
  message), would otherwise stay stale until its own files changed again. `python3
  scripts/image_changes.py --table` prints which images load each file.
- **CI builds the brain FIRST, then every other changed image AT ONCE** (the operator's request,
  2026-11-16; until then
  ten jobs in one sequential chain, each waiting for the one before: a run that built every image
  took the sum of their builds, and one failure skipped every image behind it). `build-workflow-brain`
  runs alone because the brain is deployed before any limb (§5); `sync-upgrade-charts` runs beside
  it, so a chart still lands before its image (§8); then `build`, one matrix job per changed
  image, `fail-fast: false`: a failing image stops no other and is rebuilt by a later push (the
  diff base above). A brain or a chart sync that FAILED keeps every other image back; one that was
  SKIPPED (not in the push) does not. **At most three at a time**: each job ends by pushing its
  bump to the same redbull-platform `main`, and the reusable workflow retries a rejected push only
  five times — with n jobs pushing together one loses at most n-1 times — so raising it waits on a
  sturdier retry in `team-redbull/.github`. Runs of `build.yml` on one branch are serialized
  (`concurrency`), because two `main` runs at once would read the same latest `<chart>/vX.Y.Z` and
  publish one version twice; a waiting run GitHub cancels loses nothing, its change is in the next
  run's range.
- **Every `main` run that changes an image is a GitHub Release of this repo, naming every image**
  (the operator's request, 2026-11-16: nothing said which images a merge changed, and the air-gap
  mirrors only what it is told to). `release-notes` (`scripts/release_notes.py`) runs after the
  builds, whatever they did, on every branch that built something and on every `main` push, and
  writes the run's summary page: each image REBUILT since the previous release with its new
  version, the one it replaces, its digest and the commits that changed it, and each one that was
  NOT at the version it stays at, so the list to pull is complete. **A release is measured from the
  previous release**: each one's notes end with a comment GitHub does not show, its commit and
  every image's version, and the next lists every image whose version differs and the commits
  since, so a release whose publishing failed is caught up by the next push. **The first is a
  baseline** (the operator, same day): with no release before it, every image is listed at its
  current version and digest, unchanged, with the whole set to pull, and nothing is rebuilt for it
  (merging the change that brought the notes rebuilt nothing, by the rule above). It never
  decides again which images were built — it takes `changes`' list — and attributes each commit
  of the range to the images that commit affects with `image_changes.affected_images`, the
  decision's own rule. Versions are redbull-platform's `<chart>/vX.Y.Z` tags (each chart directory
  is named after its image, `tests/test_image_builds.py`), recorded by `changes` before anything
  builds and read again after, so a built image whose tag did not move is listed NOT RELEASED at
  the version it stays at; digests are ghcr.io's, asked anonymously (the packages are public). On
  `main`, `release` publishes the notes as release `release-<commit date>-<short sha>` on the built
  commit when they list a changed image or are the baseline (a re-run of failed jobs rewrites the
  same release: a commit's own release is never its previous one) and runs none of the repo's code, so
  the one job there with a write token executes nothing from the checkout. **Neither ever fails
  the run** (`continue-on-error`): the diff base is the last run that SUCCEEDED, so a red notes job
  would rebuild, and re-version, every image the run already shipped. A run that changed nothing
  since the previous release publishes nothing; a branch run's page lists what it built as
  `<branch-slug>-<sha>`. If the previous release cannot be read, the notes compare with the
  versions before the run and no baseline is published. Known gap,
  the reusable workflow's: an image job checks out the branch's head when it STARTS (`ref:
  github.ref_name`), not the run's commit, so an image can hold a commit pushed after its run
  began; that commit is listed by the next run, which builds it again.
- Assumed already running: a Temporal server and the Segments Manager (via OpenShift routes or
  localhost — no port assumptions in code).
- Trigger via the unified API: `uvicorn workflow_domains.api:app --port 8080`, Swagger at `/docs`.
  `POST /workflows/server-lifecycle/add-server-to-mce` is ASYNC (202 + workflow id) and takes the
  InfraEnv to fill plus its MCE cluster; the InfraEnv's agentLabels state which hardware it is
  for (flavor `c200`, size `mtl`, site `five`) and server names carry the same words behind an
  `ocp-` prefix, so the labels ARE the server query (§4). EVERY POOL RUN IS ON A SLOT of its HUB's CANDIDATE POOL — the
  MCE and the InfraEnv. The MCE is in the id (since 2026-10-25) so a hub finds only its own runs;
  the machines the pool draws from (`^ocp-<flavor>-<size>-<site>-`) are still shared by every hub, so a run
  sizing its spares no longer counts another hub's runs, and the install lock keeps two hubs off
  one machine. The install lock (§4) guards the machine itself, so draws are PARALLEL: a request
  without `count` starts ONE new run and `count` (1-20, on the request ONLY — never in a run's
  input, which is decoded from history) starts `count`, one per SLOT, id
  `add-server-to-mce-<mce>_<infraEnv>_<slot>` (`_` follows the MCE and joins the parts after it: it
  cannot occur in a Kubernetes name, so a hub's runs are all under `add-server-to-mce-<mce>_` and MCE
  `ocp4-a` never matches MCE `ocp4-a-b`'s; an id is not in a run's replayed history, so a run in
  flight keeps the id it started with — `install-server-<infraEnv>_<slot>` and
  `install-server-name-<server>` before the MCE, `install-server-<mce>_…` before the rename).
  It walks slots `_1`..`_100`, steps over
  every slot whose run is still RUNNING (WorkflowAlreadyStartedError) and fills free ones, so a
  second request during the first batch adds installs — it used to collide on `_1`..`_N`. A
  closed slot is reused (Temporal's default ALLOW_DUPLICATE id reuse; `TestAFinishedSlotIsFreeAgain`).
  A start Temporal refuses for any other reason stops the scan (a 502 with its reason when the
  request is one run). Every run asks server-scan for ONE server and every pool run's input
  is the same: server-scan allocates each its own, locking it in the same call (§4) — until
  2026-11-17 each run drew `candidate_count` = (busy slots) + new runs + 2 candidates instead.
  Safe because server-scan's lock is a compare-and-set on the document revision that never
  hands out a live lock, and the run writes nothing before holding it —
  `TestParallelRunsOnOnePool` runs two at once. The test harness's mock allocation must keep
  refusing a locked server (the caller's own included), or that test proves nothing.
  ONE ROUTE, ONE RUN PER SERVER — never a batch workflow. Named machines are `server_names`: a
  string is one run (`{workflow_id, run_id}`, 409 while it installs), a list (1-20, each once,
  case-insensitively) one run per name with a per-run report, even for a list of one — the reply
  follows the request's shape. Every named run is on `add-server-to-mce-<mce>_<server>`, so one machine
  is one run however it is asked for. `count` sizes a POOL draw, so next to `server_names` it
  is a 422, never silently ignored; `candidate_count` is a 422 anywhere since 2026-11-17, saying
  it was removed. `namespace` and `labels` are HIDDEN from Swagger (`SkipJsonSchema`; the
  operator, 2026-11-09 — expert knobs, so Swagger shows `infra_env`, `mce_cluster`, `count`,
  `server_names`), yet accepted, validated and used as before, defaults unchanged;
  `TestTheHiddenExpertFields` pins both halves. Hidden is not
  protected: the API has no auth. Still documented in the README and the docs page, unlike
  `allow_unhealthy`. `AddServerToMceRequest` is therefore its
  OWN model, not an AddServerToMceInput subclass: one request is many runs, each built by
  `run_input` with ONE `server_name`; `test_every_run_field_can_be_sent` keeps the two in step,
  and the old `server_name` field is a 422 pointing at `server_names`. Decided with the operator
  2026-10-23: `install-server/bulk` (the `count` draw from 2026-10-20) read as "a list of
  servers", the parallel draw went unfound, and a caller asking twice for one InfraEnv met the
  409 — so `/bulk` was REMOVED (404) and everything moved onto install-server (now
  add-server-to-mce). Retry-safety of a
  `count` request was weighed and left out: its callers are humans, not controllers. REVISED with
  the operator 2026-10-24: a request WITHOUT `count` was still ONE run on the pool's id
  (`install-server-<infraEnv>`), a 409 until the first finished — up to an hour — and asking the
  pool again IS asking for one more server, so it now takes a slot too and is never a 409 (only
  when all 100 slots run). Its reply stays `{workflow_id, run_id}`; only `count` asks for the
  report. The id builder is `add_server_to_mce_name_workflow_id`, named-only: nothing builds the pool
  id any more, and runs started on it keep it. A named machine keeps its 409.
  THE FOUR SERVER-LIFECYCLE WORKFLOWS WERE RENAMED on 2026-11-01 (the operator: each name says
  what it does to what): install-server → add-server-to-mce, remove-server →
  remove-server-from-mce, uninstall-server → uninstall-server-from-hc, publish-infraenv-boot →
  publish-infraenv-to-pxe-server — route, ids, queue, type, models, module, tests, docs and lock
  holder alike. Not aliases: each OLD route answers 410 naming the new one and starts nothing
  (`_RENAMED_PATHS` in the router). Runs started before replay as their OLD type — a subclass at
  the bottom of each module, overriding `run` because the SDK refuses an inherited one — on their
  OLD queue (`LEGACY_*_WORKFLOW_QUEUE`), each choosing by `workflow.info().workflow_type`: its old
  lock holder (`install-server`, `uninstall-server` — server-scan extends and releases a lock
  only for the SAME holder and id), and, for an uninstall, its OLD child (`RemoveServerWorkflow`,
  `remove-server-<server>`, the old queue: a child's id is in the parent's history, and the
  goldens fail without it). remove-server-from-mce recognises both spellings of each holder it
  releases or gives way to. The goldens recorded before stay where they are
  (tests/histories/{install,remove,uninstall}_server/, replayed by the legacy classes — never
  re-recorded); the new types have their own. NOT renamed: patch ids (history markers),
  activity names (`publish_infra_env_boot` included), `UninstallServerFromHcResult.remove_server`
  / `remove_server_workflow_id` (the JSON of results), the replay recorder's identity
  placeholder `install-server-replay`. Delete each legacy class, its registration and its queue
  once `WorkflowType='<old type>' AND ExecutionStatus='Running'` is empty — RemoveServerWorkflow
  only once UninstallServerWorkflow's is too.
  `POST /workflows/vm-lifecycle/create-vm` is ASYNC too and is ALSO a slot draw: without `count`
  one run in the first free slot (`create-vm-<mce>_<infraEnv>_<slot>`, never a 409 unless all 100
  slots run), with `count` (1-20) one run per free slot and a per-run report — the VM's name is drawn
  inside the run, so nothing at the edge can key it (§4). `infra_env` must parse as a KubeVirt
  InfraEnv of the convention, `<flavor>-kv<cpu>-<zone>` (zone `az<N>`, 1..256 vCPUs; a
  `-mtl-` InfraEnv, the draft's `<flavor>-kv<cpu>-<site>-<zone>` and the 2026-11-04
  `…-zone-<zone>-<cpu>` form are refused), `mce_cluster` as `ocp4-<lifecycle>-mce-<site>-<id>` (its
  site is the VM's, its lifecycle picks the seeds), and the VM name the cluster's worker will build —
  the InfraEnv's flavor and size, the MCE's site — must fit a Kubernetes name with its `-root`
  DataVolume — each a 422 otherwise. The flavor, zone, cpu and memory (the InfraEnv's) and the site
  and lifecycle (the MCE's) are derived here once and carried. `count` runs are ordinary parallel runs: each takes its zone's lock in
  turn, so they no longer all land on one cluster.
  `POST /workflows/segment-lifecycle/initialize-segment` is ASYNC (202 + workflow id) and takes
  the full segment definition; poll `GET /workflows/runs/{workflow_id}` for progress/result. The
  `/bulk` variant takes a list and starts ONE WORKFLOW PER SEGMENT (never one batch workflow — each
  segment has its own dedup id, its own run status and its own failure), answering 202 with a
  per-item report rather than a single pass/fail code. `POST .../allocate-segment`
  (`{"cluster", "site"}`) is a MANUAL allocation on the same id provision-cluster's child uses, so
  the two dedup; `values_branch` is a 422 (§4). `POST .../release-segment` (`{"cluster"}`) is
  called by the hostedcluster-setup chart's PostDelete hook once a cluster's Application is gone (§4).
  `POST /workflows/cluster-lifecycle/provision-cluster?cluster=<name>&mce=<folder>` is ASYNC and its
  BODY IS THE HOSTED-CLUSTER YAML (`Content-Type: application/yaml`; Swagger's "Try it out" shows
  the slim example to edit): the edge refuses only what could not be a cluster file (empty, over
  256 KiB, not UTF-8, not one YAML mapping) and a bad `cluster`/`mce` (422); everything semantic is
  a FAILED run (§4). A second request for a cluster whose run waits for its merge is a 409; one for
  a cluster already on day1 `main` starts a run that fails `ClusterAlreadyProvisionedError` at once.
- In-cluster the API is `workflows-orchestrator-api` (ClusterIP:8080) plus an OpenShift **Route**
  (`workflowsApi.route.*` in the `workflows-orchestrator` chart) — it needs a hostname because starting a workflow
  is now an operator action. NOTE: `workflow_domains/api.py` has NO auth of its own; anyone who can reach
  that hostname can start a workflow — release-segment's route ends in a DHCP scope DELETE and a
  segment release. Disable the Route (and port-forward) where that matters; the PostDelete hook
  reaches the API by its in-cluster Service, not the Route.
