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
- **`scripts/check_conventions.py`** — the ENFORCEMENT: AST checks of every rule that
  can be checked from source (sandbox imports, activity-call options, unbounded retries
  named through exception classes, `ApplicationError` from workflow code, one `run()`
  argument, `connect_temporal` only, interface ↔ implementation ↔ worker registration,
  `_WORKER_SPECS`, router paths and 202s, Dockerfile CMDs, CI jobs, a docs page per
  started workflow). It runs on every Edit/Write through the PostToolUse hook in
  `.claude/settings.json` (which also names the docs pages describing the edited file),
  as `tests/test_conventions.py`, and in CI's `test` job, which every image build needs.

A rule that can be checked belongs in the checker; one that cannot belongs here AND in
the matching rules file. When a rule changes, all three move in ONE commit. The checker
is the tie-breaker: an edit it flags is wrong until the rule itself is changed. A
documented exception is marked where it lives, `# conventions: allow <RULE> — <reason>`,
and recorded here. Two today: the agent call's bounded retry in `cluster_upgrade/dispatch.py`
(WF007, §5) and upgrade-targets' missing progress query (WF014: a synchronous question
answered in the response, §2, so nothing polls it).

## 1. Domain-driven monorepo: three layers, strictly separated

```
shared/                      Contract layer — the API between brain and limbs
  models/                    Pydantic data classes (typed state across boundaries)
  interfaces/                Activity signatures ONLY (@activity.defn, no body)
  exceptions.py  consts.py   Shared errors / constants (e.g. task-queue names)
  settings.py                pydantic-settings BaseSettings (fail-fast typed config)
  logging_config.py          Shared worker logging setup
workflow_domains/            The orchestration "brain" (one lightweight deployment)
  <domain>/                  One folder per domain — mirrors activities/<domain>/
    <workflow>.py            Workflow logic (one file per workflow) — the SHAPE of
                               the run: phases, dispatches, timeouts, failures
    <policy>.py              Pure sandbox-safe rules that workflow needs and no
                               limb does (server_lifecycle/bond_selection.py)
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
                               with no worker: values_repo.py (git), dhcp_api.py (the
                               DHCP scope API), server_scan.py (the inventory API),
                               cluster_api.py (the Kubernetes API, its idempotency
                               rule and its error classification),
                               hosted_cluster_api.py (a HOSTED cluster's API, read
                               through its admin kubeconfig)
  worker_init.py             Registers activities, polls that queue
activities/git_repo.py       Shared by limbs, owned by no domain: the git client (clone,
activities/day1_repo.py        push, token scrubbing) and THE day1 repo reader (a
                               cluster file with its value layers, an MCE folder's
                               files) — vm-lifecycle and upgrade-gitops both read day1
                               through it, neither through the other's domain
docs/                             The static documentation site (its own image, no code)
```

Prod charts are VENDORED into the Argo CD repo, at
`redbull-platform/gitops/charts/<service>/`: `workflows-orchestrator` (brain: ONE release for
all domains), `segment-lifecycle-worker`, `server-lifecycle-worker` and `server-provisioning-worker`
(limbs: one per domain), cluster-upgrade's `cluster-upgrade-worker`, `upgrade-agent-worker`
and `upgrade-gitops-worker` (since 2026-11-02; with `server-provisioning-worker`, these have no
`gitops/services/` entry yet, so CI bumps their tags and Argo CD deploys nothing), and
`vm-lifecycle-worker` (written 2026-11-05, in redbull-platform's working tree until pushed: CI's
bump of it fails until then, so its job is LAST in the chain). One generic ApplicationSet
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
  re-registered on another queue by a future sub-workflow without moving code.
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
- **vm-lifecycle is ONE hub limb for every zone, holding no Kubernetes credential.** create-vm
  reaches each KubeVirt cluster through the two agents the platform deploys there behind Routes
  (`kv-capacity`, read-only facts; `kv-vm-api`, the idempotent VM create/read/delete — their own
  repo `kv-agents`, charts in redbull-platform, one fleet bearer token each) and lists a zone's
  clusters from the day1 repo with the day1 git token. So there is nothing to run per MCE and
  nothing per KubeVirt cluster: `activities/vm_lifecycle/`, queue `vm-lifecycle-activity`, chart
  `gitops/charts/vm-lifecycle-worker/`, domain `workflow_domains/vm_lifecycle/` with ONE workflow,
  `create-vm`, on `create-vm-workflow`. Its id is a SLOT of the InfraEnv on its MCE
  (`create-vm-<mce>_<infraEnv>_<slot>`, `shared/workflow_ids.py`): the VM's name is drawn inside the
  run, so nothing known at the edge identifies it — add-server-to-mce's pool-draw shape, with the walk
  itself extracted to `workflow_domains/routers/slots.py` (add-server-to-mce keeps its own until a
  follow-up migrates it: its `candidate_count` depends on the busy count).
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
  (`SERVER_SCAN_URL`). The workflow makes ONE read — `GET /servers/available`
  — plus the install lock below, and never queries a vendor manager itself: server-scan already collects HP
  OneView / UCS Central / Dell OME / Intersight / standalone Redfish on a
  6-hour cron and knows which servers are unclaimed. This is what replaced the
  `bmh-generator-operator` Kopf operator and its four vendor SDKs. The endpoint
  live-rechecks each candidate it returns, so freshness is its problem, not
  ours; `live_recheck_performed` per item says when it degraded to the stored
  document. It returns DATA ONLY — never BMC credentials, which server-scan
  does not hold; those stay in this worker's own Secret.
- **add-server-to-mce LOCKS the machine it chose in server-scan** (server-scan's
  ADR-0035, `POST`/`DELETE /servers/{id}/reservation`, `reserve_server`/
  `release_server` on the MCE's queue). `/servers/available` hands candidates
  out unlocked and lists a machine AVAILABLE until a cluster reports it, while
  the BareMetalHost probe only sees the run's OWN MCE — so without the lock a
  server installed into MCE-A is drawn again for MCE-B. Taken after the probe,
  before any write; holder `add-server-to-mce` + the run's workflow id, so a retry
  or a renewal EXTENDS it (server-scan matches BOTH, so a run started as
  install-server keeps holder `install-server` to the end — `_reservation_holder`,
  by workflow type); renewed as the Agent wait begins; held a day after
  success, unless the server leaves the MCE first — remove-server-from-mce then releases
  it (the operator's decision, 2026-10-31, below); add-server-to-mce itself
  releases it ONLY after a teardown (a failure elsewhere leaves it to its
  TTL, since half-created resources may still name the machine). A lock held
  elsewhere is a skip (`reserved`), like every candidate reason. A RE-RUN of the
  same named install (same workflow id) after such a failure never reached
  `reserve_server`: `/servers/available` hides a locked machine even from its
  holder, so it failed for two hours on a bare "no assignable server" (the
  2026-10-31 incident). So a named machine it refuses is read once more
  (`lookup_install_candidate`, gated `named-install-own-lock-rerun`): the run goes
  on when the only reason is its OWN workflow id's lock, and otherwise fails
  saying which (another run's lock, a claim, maintenance, its health). Needs an ADMIN
  `SERVER_SCAN_API_TOKEN`. Gated by `workflow.patched("server-scan-install-lock")`
  so runs started before it replay unchanged — keep the patch until none can
  remain in history.
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
  instead of locking, booting and waiting an hour on every candidate. An
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
  (`spec.clusterDeploymentName`). Order is load-bearing: POWER OFF first
  (`spec.online: false`, bounded wait on `status.poweredOn`, else
  `HostPowerOffTimeoutError` with nothing deleted), then the BMH + NMStateConfig
  via the same `teardown_bmh_resources` as add-server-to-mce's rollback, and the
  Agent LAST — a host still running discovery re-registers a deleted Agent
  within a minute. The power-off and the Agent delete each re-check the binding
  right before acting. It ENDS by re-inspecting until none of the three is left
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
  PXE VMs first. LAST, server-scan's install lock (`releasing-server`, gated
  `remove-server-release-own-mce-lock`; the operator's decision, 2026-10-31 —
  a server removed the day it was installed stayed undrawable for the rest of
  add-server-to-mce's day-long hold): once nothing is left and `pxe_failed` is
  empty, the lock is read (`lookup_server_membership`, which returns `lock`
  now — read by the key's presence, §5) and released with the holder +
  workflow id read, so server-scan compares and deletes exactly that lock —
  ONLY when it is live, `add-server-to-mce`'s and for THIS MCE (canonical names).
  Left alone: another MCE's, `uninstall-server-from-hc`'s (its run releases its own
  after this child completes — never raced), any other holder's; not read at
  all on found=false or a PXE failure. Never fails the run: `lock_released`,
  `lock_detail`. Needs `delete` on agents in the limb's RBAC.
- **uninstall-server-from-hc takes ONE server all the way out of an MCE, whatever its
  Agent's state** (`workflow_domains/server_lifecycle/uninstall_server_from_hc.py`, own
  queue `uninstall-server-from-hc-workflow`, id `uninstall-server-from-hc-<server lowercased>`,
  API `POST /workflows/server-lifecycle/uninstall-server-from-hc` — `server_names` ONLY,
  a string or a list of 1-20, each once case-insensitively, plus `mce_cluster`
  canonicalised and `namespace` defaulting to `inventory`. EVERY UNINSTALL ENDS IN
  remove-server-from-mce: `keep_inventory` (added 2026-10-24 — stop after the hosted
  half, the server left in the InfraEnv as a free Agent) was REMOVED by the
  operator on 2026-11-02; do not re-add it. The request answers it with a 422
  saying so; a run started since `uninstall-server-from-hc-always-removes` ignores it
  in its input; a run recorded before still ends as it began (`_keep_inventory`,
  `kept_in_inventory: true`, the `keep-inventory` goldens). NO `count`, NodePool or
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
  and skips that half — but is INSPECTED AGAIN, fresh, right before remove-server-from-mce
  (`verifying-agentmachine`, gated `uninstall-server-inventory-verify`; the
  operator, 2026-10-24: "should still verify no AgentMachine, Machine"): a free
  Agent is what ANY NodePool's new AgentMachine takes, so one now bound or
  labelled fails `AgentRetakenError`, any other AgentMachine / Machine /
  control-plane link `ServerStillPresentError`, nothing on the MCE written. EVERY path ends in remove-server-from-mce as an AWAITED CHILD
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
  INSTALLED reading, a re-run's normal view) → `reserving-server` ALWAYS → the
  hosted half: `deleting-machine` (`ensure_machine_deleting`, the one write the
  NORMAL path makes, a no-op when it already has a deletionTimestamp or is gone) →
  `awaiting-machine-deletion` (30 s polls of Machine + Agent: `draining` while the
  Agent is bound, 20 min from the first deleting observation, then
  `MachineDrainTimeoutError` naming conditions and pending hooks; `unbinding`
  once released, 1 h to `known-unbound` — add-server-to-mce's bare-metal reasoning —
  then `AgentUnbindTimeoutError`, or `MachineDeletionTimeoutError` when the Agent
  is free and the Machine still stands) → `verifying-agentmachine` (gone, 5 min,
  `ServerStillPresentError`) → `verifying-node` (no Node is the server — by
  providerID, nodeRef or hostname, NEVER skipped for a missing nodeRef) →
  `verifying-agent` (no
  binding, no label, known-unbound, approved, BMH present and not in error; 5 min,
  `AgentStillBoundError`) → `holding-server` (24 h BEFORE the child, so the hold
  covers the inventory half) → `removing-from-inventory` → `releasing-server`
  (the lock given back, below). Every skip goes into
  `result.skipped`. NOTHING IS UNDONE after the Machine delete — it cannot be —
  and a re-run of the same id RESUMES. NODEPOOL REPLICAS ARE NOT ITS BUSINESS:
  the run deletes the Machine ITSELF (no prerequisite) and the NodePool backfills
  = REPLACE. Who owns `NodePool.spec.replicas` (a shrink) is NOT DECIDED — the
  operator is taking it to the team (2026-10-24); never change them here. The
  provider's `findAgent` takes ANY free Agent the NodePool matches, so with no
  other free Agent the replacement takes THIS server back: every wait pass checks
  the Agent's `agentMachineRef` label (+ namespace annotation) against the
  AgentMachines the run started from (`HostAgent.labelled_agent_machine`,
  `AgentBindingState.agent_machine`, both gated on key presence) and stops with
  `AgentRetakenError`; `release_agent` refuses an Agent whose labelled
  AgentMachine a live Machine holds (`AgentBindingChangedError`). THE LOCK even for an uninstall (holder
  `uninstall-server-from-hc`, TTL = two drains + unbind + three verifications + 10 min): the
  hosted cluster's nodes job frees the server on its Node's absence and the MCE's
  agents job re-claims it INSTALLED_TO_INVENTORY on its own 15-min cadence, so
  between them it can read AVAILABLE while still in this InfraEnv and be drawn by
  another MCE's install. Taken after the inspection, before any write; a lock
  held elsewhere fails the run (add-server-to-mce's day-long `_INSTALLED_HOLD` on a
  freshly installed machine does — release it in server-scan); on failure left
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
  one stuck deleting has ONLY the provider's finalizer dropped after 1 min and
  only once the Agent is released; any Machine still linked =
  `ServerStillPresentError`, a NodePool replacement taking it again);
  `verifying-node` deletes a Node still there once NotReady (the host is back in
  discovery) and fails on one still Ready. Writes go in `result.cleaned_up`, each
  with why the owner could not; the shape found in `leftovers_at_start`. Needs
  the new limb (new activities) AND the widened RBAC — ship the RBAC first.
- **create-vm (domain `vm-lifecycle`) makes ONE Halted VirtualMachine for a KubeVirt InfraEnv, on the
  best-fitting KubeVirt cluster of the InfraEnv's zone — the step before add-server-to-mce for an
  InfraEnv whose hosts are VMs.** Designed with the operator 2026-11-04/05 (plan file
  `~/.claude/plans/let-s-plan-create-vm-workflow-harmonic-penguin.md`; `docs/design/kubevirt-vm-placement.md`
  and `docs/design/kv-agents-api.md`). The decisions, none to reopen:
  - **THE NAMING CONVENTION says everything, parsed ONCE at the API edge** (`shared/infraenv_names.py`,
    `shared/cluster_names.py`) and carried in the input, never re-derived on replay (the
    provision-dell-server region precedent). An InfraEnv is `<conf>[-<letter>]-zone-<zone>-metal`
    (bare metal) or `<conf>[-<letter>]-zone-<zone>-<cpu>` (KubeVirt: `<cpu>` vCPUs, `<cpu> × 8` GiB —
    `MEMORY_GIB_PER_CPU`, the ONE place the ratio lives). An MCE is `ocp4-<env>-mce-<site>-<letter>`:
    the SITE comes from it. Per (site, zone) ONE seed cluster `ocp4-seed-<site>-<zone>` hosts that
    zone's KubeVirt clusters, named `ocp4-kubevirt-<site>-<zone>…`. VOCABULARY the operator insists on:
    "day1" names only the REPO; a seed cluster is a "seed", never an "mce"; the convention's KubeVirt
    clusters are "kv clusters"; a list of all clusters would be just "clusters".
  - **The zone's clusters are the FILES of the seed folder**
    `sites/<site>/mces/ocp4-seed-<site>-<zone>/hostedClusters/` at day1 `main`, read by the
    vm-lifecycle limb ITSELF (`list_kv_clusters`, its own clone, the shared `activities/git_repo.py`
    client, the segment-lifecycle chart's `day1-git-token` Secret as a third consumer — one Secret,
    three consumers, the dhcp-token-chain shape). The operator REJECTED putting the read on the
    upgrade-gitops limb (2026-11-05): upgrade-gitops is not production, and create-vm must not depend
    on it. NOT filtered by day1 `platform: kubevirt` — that key means a cluster whose NODES are VMs.
    The folder is built from VALIDATED tokens and read with `iterdir()`, never globbed with input.
  - **The day1 reader is SHARED, in `activities/day1_repo.py` next to `git_repo.py`, owned by no
    domain** (clone/head/blob, the layer-merging `load_cluster_values` with its `ClusterValues`,
    `list_cluster_files`): vm-lifecycle's `list_kv_clusters` and upgrade-gitops's `read_cluster`
    both build on it, and neither limb imports the other's domain for it (the operator,
    2026-11-05 — it first sat in `activities/vm_lifecycle/`, "owned by create-vm", which made the
    upgrade limb look dependent on create-vm; `tests/test_day1_repo.py` pins the one-object
    guarantee). `shared/` cannot hold it (the contract layer, SH001; it needs PyYAML and git).
    `find_cluster_file`, `read_rules`, `read_cluster`, the NodePool parsing and every edit stay in
    `activities/upgrade_gitops/`; the seed-folder walk stays in `activities/vm_lifecycle/`.
  - **Reaching a KV cluster = our own agents, NOT a kubeconfig, NOT Thanos.** Per cluster TWO
    services behind Routes, one fleet bearer token each: `kv-capacity` (GET `/v1/capacity`: nodes
    with allocatable and the Pending|Running requests, our Halted launcher-less VMs' would-be
    requests, every VM's MAC) and `kv-vm-api` (PUT/GET/DELETE `/v1/vms/{name}`, rendering
    `vm.yaml` server-side; storage class, network and node selector are its per-cluster values).
    Two, not one, by §2's rule (different RBAC and blast radius ⇒ different deployments); DUMB on
    purpose — the fit test, the buffer and the ranking live HERE (`activities/vm_lifecycle/placement.py`),
    so the policy has one home. The wire contract is vendored in `shared/models/vm_lifecycle.py` and
    pinned by `tests/test_kv_agents_contract.py` (`UPDATE_KV_AGENTS_CONTRACT=1`,
    `KV_AGENTS_SERVICE_DIR`), as the upgrade agent's is. **TLS to the agents is NOT VERIFIED by
    default** (`KV_AGENT_CA` empty) — the operator's explicit choice, 2026-11-04: the Routes carry
    the ingress wildcard certificate, whose CA the image cannot carry; the one place in the repo
    verification is off by default, and a CA path turns it on.
  - **Placement: fit PER NODE, buffer PER CLUSTER, best-fit on the dominant resource.** The VM's
    size in the scheduler's terms is what `vm.yaml` makes the launcher pod request — CPU
    `cpu × 15 / 16` cores (integer division), memory the guest's plus 1 GiB — defined ONCE in
    `placement.py`. One schedulable node must hold the pod; the cluster's fill AFTER placing (active
    requests + Halted reservations + this VM, over the schedulable allocatable) stays ≤ `1 − B` on
    CPU and memory (`HEADROOM_BUFFER_PERCENT`, our policy, unprefixed, per cluster never per node);
    the fullest-after wins, ties by name. **Halted VMs with no launcher pod COUNT toward the cluster
    fill** (reversing the design doc's first draft, 2026-11-04): kubevirt-redfish will start them, and
    two creates in a row must not both see an empty cluster. Every reason a cluster is skipped is a
    ROW in the table (`SKIP_REASONS`, mirrored by the workflow's `_SKIP_SUMMARY`, a test keeps them
    equal — the `_REJECTION_SUMMARY` precedent), never an exception; the rank activity raises only
    when NO cluster could be read (`KvCapacityUnavailableError`, retried) or EVERY reachable one
    refused the token (`KvAgentAuthError`, permanent).
  - **Identity: the run draws the suffix, the limb composes the MAC, name and MAC agree by
    construction.** Six hex digits from `workflow.uuid4()` (replay-safe), VM
    `ocp-<infraenv>-<6 hex>`, MAC `VM_MAC_PREFIX:xx:xx:xx` from the same digits — KubeVirt
    re-randomises an unset MAC on restart and the BareMetalHost needs a fixed boot MAC. A draw whose
    MAC TAIL is any VM's in the zone (`known_macs`, unioned from every reachable report; compared on
    the last three octets because the workflow never sees the prefix) is dropped without a call; a
    409 from kv-vm-api (born in between) redraws; `_MAX_DRAWS` = 3, then `VmNameExhaustedError`. The
    PUT is idempotent by name AND identity (200 for the same namespace/MAC/size/labels, `created`
    False — an in-run retry artifact, never something a re-run relies on; `runStrategy` is never
    compared).
  - **NO candidate loop on the create; a root disk unbound after 10 min is ROLLED BACK, then the run
    fails.** The PUT on the chosen cluster is out-waited and never moved to another (Halted, the
    scheduler is not consulted; a move after an ambiguous failure could leave two VMs). The wait is
    ONE heartbeating activity (`await_vm_ready`, every 8 s — the operator, 2026-11-05 — its clock in
    the heartbeat details), `deadline` on its own clock; then `delete_vm_on_cluster` by name AND MAC
    (404 = success; a 409 means someone replaced the VM — left alone, `VmIdentityConflictError`) and
    `VmRootDiskNotBoundError`, which says a WaitForFirstConsumer storage class never binds a Halted
    VM's disk. Why delete: a re-run of the slot is a NEW execution with a NEW suffix, so it cannot
    converge on the orphan — it would add one per attempt (add-server-to-mce's rule: nothing survives
    on a cluster that did not take it).
  - **End state Halted** (`runStrategy: Halted`, label `redfish-enabled: "true"`): kubevirt-redfish
    owns the power state and Ironic starts it. **The VM namespace is add-server-to-mce's `namespace`,
    verbatim** (default `inventory` on the edge only), a kubevirt-redfish chassis namespace the
    platform created — never created here; missing is `VmNamespaceNotFoundError`.
  - **`schedule_to_start_timeout` on the two READS only** (`list_kv_clusters`, `rank_kv_clusters`,
    the short-run "no worker" signal); NONE on the create, the wait or the delete — Temporal never
    retries that timeout, and after the PUT a limb pod away for five minutes must not fail the run,
    or its rollback, and orphan the VM (§5).
  - **Deploy order**: the agents on every KV cluster of a zone (Routes up, two fleet tokens issued,
    `vm.yaml` with a `macAddress` — a redbull-platform change) → the brain → the `kv-capacity-token`
    / `kv-vm-api-token` Secrets out of band → the `vm-lifecycle-worker` chart + app.yaml, CI bumps
    its tag. Until the agents are up a run waits with every cluster `no-agent`. add-server-to-mce's
    KubeVirt branch (starting create-vm as an awaited child when the InfraEnv is KubeVirt) is the
    operator's LATER step; the input and result are shaped for it, and NO branch exists yet.
  - **Settled with the operator 2026-11-05**: the CPU request is FLOORED AT ONE CORE
    (`max(1, cpu × 15 // 16)` in `placement.py` AND in the kv-vm-api template — vm.yaml's bare
    formula makes a 1-vCPU VM request 0; the parser keeps 1..256); the apps domain is
    `apps.<clusterName>.<base>`, so the URL templates use `{cluster}` (the other placeholders stay
    available); NO extra VM labels for now (`labels` is in the contract, sent empty).
    **kubevirt-redfish runs `systemIdConvention: legacy`** (the operator, 2026-11-05, before any
    production VM exists): the Redfish system id IS the VM name, so the id, the VM and the server
    name server-scan lists are one string, `ocp-<infraenv>-<suffix>`, and add-server-to-mce's
    `^ocp-<infraEnv>` match holds. `enhanced` (`<ns>.<vm>`) was the chart's setting until then;
    its one advantage, duplicate names across chassis namespaces, cannot arise for create-vm's
    VMs (the suffix is the MAC's, unique zone-wide). Changed in redbull-platform's chart values
    and lab manifests; each cluster's out-of-band config Secret is re-rendered by the operator.
    **Proven in the lab 2026-11-05** (`docs/lab/create-vm/`: cluster-64spr, OCP/CNV 4.22, nine
    simulated KV clusters = agent release pairs pinned to nodes, a mock day1 served locally, the brain
    and the limb from source): the create, Halted with the MAC and the label; best-fit among three
    fitting clusters on real numbers; a Halted reservation tipping the next create elsewhere; every
    skip reason as a row; `NoKvClusterFitsError` / `NoKvClusterError` / `KvAgentAuthError`; the
    10 min rollback (`VmRootDiskNotBoundError`, VM and DataVolume gone) with the wait resumed from
    its heartbeat on a restarted limb. Measured there: a 16 GiB guest's launcher pod requests
    308 MiB over the guest, so the 1 GiB pad is conservative (kept); the Routes answer in well
    under the 10 s probe (kept). FOUND there: vm.yaml's root disk is `ReadWriteMany` +
    `Filesystem`, which ceph-rbd REFUSES (`multi node access modes are only supported on rbd
    block type volumes`) — a cluster's `vm.storageClassName` must be a class whose StorageProfile
    allows that pair (cephfs in the lab), or every run ends in the rollback; and `count` runs in
    PARALLEL all rank BEFORE any creates, so they land on the same cluster — Halted reservations
    protect SEQUENTIAL creates only (observed, not redesigned). The simulator
    `tests/kv_agents_simulator.py` + `tests/test_create_vm_e2e.py` run the real workflow and the
    real limb over a fleet in every state (the dell_simulator precedent); goldens in
    `tests/histories/create_vm/` include the lab's first real run.
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
  inventory. Decided with the operator, 2026-09-25.
- **add-server-to-mce's `allow_unhealthy` is HIDDEN ON PURPOSE — not dead code.** The operator's
  "easter egg" (2026-10-31), after an incident that had to install servers with one NIC up
  and no RAID. Out of OpenAPI/Swagger (`SkipJsonSchema`, pinned by `TestTheHiddenAllowUnhealthy`),
  the README, the docs site and every UI; left out of payloads while unused (`_QuietDefaults`),
  so the Temporal UI does not show it either. Only with `server_names` (422 on a pool draw).
  It waives HEALTH — the server's own condition: server-scan's health tier, network category,
  BMC reachability, live recheck, and a bond member whose link is not UP (still two members on
  distinct ports from `interfaces[]`, nic1 the first link-UP NIC) — NEVER safety: unknown to
  server-scan, claimed, contested, maintenance, another run's lock, InfraEnv, BMC credentials,
  a BMH/Agent present. A named machine `/servers/available` refuses is read once more
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
  a candidate exists, and a missing allocation fails the run rather than
  skipping a candidate — no candidate could make it usable. `bmc_vendor` still
  picks the Ironic DRIVER; it no longer picks a network.
- **In add-server-to-mce, EVERY reason a candidate is unusable is a SKIP.** The
  multi-candidate draw exists so one unusable server is a retry rather than a
  failed run, and a reason handled outside the selection loop silently breaks
  that promise for part of the fleet — a pool of three dying on a STANDALONE
  machine at the front while two installable servers sit behind it. The reasons
  are two tables in `add_server_to_mce.py` (`_REJECTION_TYPE`, `_REJECTION_SUMMARY`)
  keyed by the same strings the loop records; when nothing survives they are
  reported as separate groups and the failure takes the specific type only if
  they all agree. A reason missing from either table is a KeyError raised in
  WORKFLOW code, which hangs the run rather than failing it — so a test keeps
  the loop and both tables in step.

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
  dependency — see §5. ONE git client, `activities/git_repo.py`, shared by segment-lifecycle
  (`values_repo.py` keeps its own names for it) and the cluster-upgrade hub limb.
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
- **allocate-segment writes to the BRANCH THE RUN NAMES, and ends at the push.** Until a
  create-cluster workflow exists, allocate-segment is a step INSIDE the day1 values repo's GitLab
  pipeline, which runs only on non-`main` branches: a human pushes a temp branch with the new
  `<cluster>.yaml`, the pipeline calls allocate-segment (replacing its old in-house VLAN allocator),
  waits for the run, generates the MachineConfig files from the recorded `vlanId`, and a human merges
  afterwards. So `values_branch` is a PER-RUN input (the pipeline's `$CI_COMMIT_BRANCH`), never
  config — `DAY1_BRANCH` was removed, and there is no fallback, because a fallback pushes to `main`
  by omission. Required on the API edge (`AllocateSegmentRequest`, which also refuses option-shaped
  or malformed names); a branch the remote lacks is `ValuesBranchNotFoundError` (non-retryable); our
  commit carries `[skip ci]` or it would start a second pipeline that re-runs the allocator. The
  branch is NOT in the workflow id: the SM allocates per (cluster, site, type) whatever the branch,
  so two branches for one cluster must collide on one run. A future create-cluster workflow keeps
  this shape — it merges AFTER its allocate-segment child returns.
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
  (next bullet), not for this wait; when create-cluster verifies the scope after ITS merge step, it
  reuses them with a bounded deadline (`git log -S dhcp_scope_ready` has the removed loop).
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
  interval and `continue_as_new` every N cycles so history stays bounded (precedent:
  initialize-segment's removed firewall-approval wait, §4). NO workflow polls today; both halves of
  the rule stand for the next workflow that needs one.
- **httpx timeout < activity `start_to_close_timeout`** (currently 60s < 90s): give every
  `httpx.AsyncClient` an explicit `timeout=` below the activity timeout so a network hang fails the call
  and frees the worker before Temporal reaps the activity.
- **Per-invocation HTTP client:** create `httpx.AsyncClient` INSIDE each activity via `async with` so
  auth tokens/cookies scope to one invocation and never leak across concurrent runs.
- **Git-subprocess activities** (`activities/segment_lifecycle/values_repo.py`): git runs via
  `asyncio.create_subprocess_exec` (no Python git dependency; the limb Dockerfile installs the binary),
  each command under a timeout below the git activities' 180s `start_to_close_timeout`, every clone in
  a fresh `TemporaryDirectory` so retries start clean. The push token is injected into the clone URL in
  memory only and SCRUBBED from every error message BEFORE the exception is constructed — activity
  errors are recorded verbatim in Temporal history and the UI, so redacting at the logging layer alone
  is too late. The branch is a parameter from an unauthenticated API, so every clone first runs
  `git ls-remote --exit-code --heads <url> refs/heads/<branch>` (the FULL ref — a bare pattern
  tail-matches): exit 2 is the deterministic `ValuesBranchNotFoundError`, any other failure the
  retryable `ValuesRepoGitError` (unreachable is not missing). Without it a mistyped branch would
  retry forever as a clone failure.
- **Cross-workflow orchestration:** a workflow that spawns sibling runs starts them as DETACHED
  child workflows — `workflow.start_child_workflow(...,
  parent_close_policy=ParentClosePolicy.ABANDON)` on the sibling's workflow queue, catching
  `WorkflowAlreadyStartedError` as an `already_running` report item — and never waits on them (each
  child answers to its own id and its own failure, the /bulk philosophy). NO CURRENT WORKFLOW SPAWNS
  SIBLINGS: convert-segment (since removed, §4) was the precedent, starting an initialize-segment run
  per converted segment to re-open its firewall rules, and that fan-out went with the firewall flow
  (§4) before the workflow itself went. The rule
  stands for the next workflow that needs one — as does the builders-in-`shared/workflow_ids.py`
  arrangement that made it possible (a workflow can never import a router).
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
- **Evolving a model that crosses the boundary — history and a lagging limb both hold OLD payloads.**
  CI ships the brain BEFORE the limb (`build.yml`: the limb job `needs` the brain's), and every
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
  three-hour control-plane upgrade is then a handful of history events. Retries stay unbounded per
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
  re-record (`UPDATE_REPLAY_HISTORIES=1`) only while no run of the old shape can exist.

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
  after its release.
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
  chart) holds its own endpoints/policy — e.g. `segment-lifecycle-config` = the allocate-segment keys
  (`DAY1_REPO_URL`, `DHCP_EXCLUSION_OCTET_RANGES`; the push credential in the `day1-git-token`
  Secret) and release-segment's `DHCP_API_URL` (its token in the `dhcp-api-token` Secret). A
  domain worker mounts BOTH + its Secrets, so the
  brain release must install before any limb (else `CreateContainerConfigError` on the missing
  global ConfigMap) — and the chart must ship the new keys BEFORE (or with) an image that requires
  them, or the worker crash-loops on its fail-fast settings.
- **`shared/settings.py`** groups: `TemporalSettings` (workers + api.py),
  `SegmentLifecycleActivitySettings`, `ServerLifecycleActivitySettings`,
  `ServerProvisioningActivitySettings` and `VmLifecycleActivitySettings` (each that domain's activity
  worker only; the last: `DAY1_REPO_URL`/`DAY1_GIT_TOKEN` again, `KV_CAPACITY_URL_TEMPLATE` /
  `KV_VM_API_URL_TEMPLATE` with their placeholders checked at startup, the two tokens, `KV_AGENT_CA`,
  `HEADROOM_BUFFER_PERCENT` 0..90 and `VM_MAC_PREFIX` — locally administered unicast, checked). Field names = Helm ConfigMap/Secret keys
  lowercased — keep aligned with redbull-platform's `gitops/charts/workflows-orchestrator/templates/config.yaml`
  (global) and `gitops/charts/segment-lifecycle-worker/templates/config.yaml` (day1 URL + DHCP
  policy + DHCP API URL + the credential Secrets). Which ConfigMap
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
  copy, moved from `deploy/charts/` on 2026-11-02) and `gitops/charts/vm-lifecycle-worker/`
  (create-vm's hub limb: ConfigMap + the two agent-token Secrets behind `existing*` hooks + limb +
  SA, `automountServiceAccountToken: false`, an optional CA ConfigMap; it NAMES the
  segment-lifecycle chart's `day1-git-token` and never renders it again; `build-vm-lifecycle-worker`
  is last in CI's sequential chain). This repo ships the PER-MCE limbs' charts, written to be copied into
  redbull-platform for Argo CD (example ApplicationSet in `deploy/argocd/`):
  `deploy/charts/server-lifecycle-worker/`, `deploy/charts/cluster-upgrade-worker/` and
  `deploy/charts/upgrade-agent-worker/` — the last two copied into redbull-platform's
  `gitops/charts/<same name>/` BY CI (`sync-upgrade-charts`, `scripts/sync_platform_charts.sh`,
  the operator's request 2026-11-02): on every `main` push that changes them, the whole chart,
  keeping the image tag and chart version the build jobs wrote there, rendered, committed and
  pushed before the image jobs run. Edit them HERE only; a hand edit there is overwritten.
  server-lifecycle-worker's redbull-platform chart is its own reshaped chart, not synced. On a
  remote MCE each renders its own Temporal endpoint, since no brain ConfigMap exists there;
  `deploy/` is in `.dockerignore` and triggers no image build (those two charts trigger the sync). The server-lifecycle limb is the first to need real RBAC: it creates
  secrets, metal3.io/baremetalhosts and agent-install.openshift.io/nmstateconfigs in the target
  namespace, where segment-lifecycle only makes HTTP and git calls — and, for uninstall-server-from-hc,
  a ClusterRole over CAPI Machines, AgentMachines, HostedClusters and Secrets (§4).
  cluster-upgrade-worker holds NO credential Secret — its ServiceAccount is the whole identity —
  and its RBAC is derived call by call from `activities/cluster_upgrade/activities.py` (its
  README's table): a ClusterRole reading HostedClusters, NodePools, MachineSets, IDMS and Secrets
  (the admin kubeconfig) and deleting pods (`delete_hcp_pod`), a Role in `ARGO_NAMESPACE`
  (applications get/list/patch — list for a lab cluster's day2 Applications, D27) and one on ConfigMap
  `hypershift/supported-versions`. Never a write to a
  HostedCluster or NodePool (DESIGN D1). **The three cluster-upgrade images are VERSION-TAGGED like
  every other** (the operator, 2026-11-02: "use tags and not commit sha"): `build-cluster-upgrade-worker`,
  `build-upgrade-agent-worker` and `build-upgrade-gitops-worker` run the reusable workflow at the
  end of the sequential chain, so a `main` build publishes `vX.Y.Z`, bumps `image.tag` in
  redbull-platform's `gitops/charts/<limb>-worker/values.yaml` and tags `<limb>-worker/vX.Y.Z`
  there. Until 2026-11-02 they were image-only jobs tagged `<branch-slug>-<short sha>` with the tag
  set by hand; do not go back to that. None of the three has a `gitops/services/` entry yet, so
  their bumps deploy nothing; deploy them by adding one (per MCE for the two per-MCE limbs). The
  domain keeps THREE images, one per limb (§2): the operator confirmed it the same day.
- Assumed already running: a Temporal server and the Segments Manager (via OpenShift routes or
  localhost — no port assumptions in code).
- Trigger via the unified API: `uvicorn workflow_domains.api:app --port 8080`, Swagger at `/docs`.
  `POST /workflows/server-lifecycle/add-server-to-mce` is ASYNC (202 + workflow id) and takes the
  InfraEnv to fill plus its MCE cluster; the InfraEnv's name states which hardware it is for
  (`cisco-m6-bat-yam-64c-512gb`) and server names carry the same tokens behind an `ocp-` prefix,
  so the InfraEnv IS the server query. EVERY POOL RUN IS ON A SLOT of its HUB's CANDIDATE POOL — the
  MCE and the InfraEnv. The MCE is in the id (since 2026-10-25) so a hub finds only its own runs;
  the machines the pool draws from (`^ocp-<infraEnv>`) are still shared by every hub, so a run
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
  request is one run). Each run draws `candidate_count` =
  (busy slots) + new runs + 2 (max 20) by default, because every run on the pool locks one
  machine from the same top of the list. Safe because server-scan's reserve is a compare-and-set on the document revision that
  refuses a live lock to any other workflow id, and the run writes nothing before holding it —
  `TestParallelRunsOnOnePool` runs two at once. The test harness's mock lock must keep refusing
  another workflow id's live lock, or that test proves nothing.
  ONE ROUTE, ONE RUN PER SERVER — never a batch workflow. Named machines are `server_names`: a
  string is one run (`{workflow_id, run_id}`, 409 while it installs), a list (1-20, each once,
  case-insensitively) one run per name with a per-run report, even for a list of one — the reply
  follows the request's shape. Every named run is on `add-server-to-mce-<mce>_<server>`, so one machine
  is one run however it is asked for. `count` and `candidate_count` size a POOL draw, so either
  next to `server_names` is a 422, never silently ignored. `AddServerToMceRequest` is therefore its
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
  inside the run, so nothing at the edge can key it (§4). `mce_cluster` must parse as
  `ocp4-<env>-mce-<site>-<letter>` and `infra_env` as a KubeVirt InfraEnv, each a 422 otherwise.
  `POST /workflows/segment-lifecycle/initialize-segment` is ASYNC (202 + workflow id) and takes
  the full segment definition; poll `GET /workflows/runs/{workflow_id}` for progress/result. The
  `/bulk` variant takes a list and starts ONE WORKFLOW PER SEGMENT (never one batch workflow — each
  segment has its own dedup id, its own run status and its own failure), answering 202 with a
  per-item report rather than a single pass/fail code. `POST .../allocate-segment` is normally
  called by the day1 pipeline (`{"cluster", "values_branch": "$CI_COMMIT_BRANCH"}`), which then
  polls the same runs endpoint until COMPLETED (§4). `POST .../release-segment` (`{"cluster"}`) is
  called by the hostedcluster-setup chart's PostDelete hook once a cluster's Application is gone (§4).
- In-cluster the API is `workflows-orchestrator-api` (ClusterIP:8080) plus an OpenShift **Route**
  (`workflowsApi.route.*` in the `workflows-orchestrator` chart) — it needs a hostname because starting a workflow
  is now an operator action. NOTE: `workflow_domains/api.py` has NO auth of its own; anyone who can reach
  that hostname can start a workflow — release-segment's route ends in a DHCP scope DELETE and a
  segment release. Disable the Route (and port-forward) where that matters; the PostDelete hook
  reaches the API by its in-cluster Service, not the Route.
