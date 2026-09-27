# CLAUDE.md

Guidance for a Temporal-based OpenShift cluster-lifecycle orchestrator — architectural decisions,
Temporal SDK gotchas, coding preferences. Read before generating code.

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
                               runs.py (domain-agnostic run status)
  main_worker_init.py        Registers every workflow, one Worker per workflow queue
  api.py                     Unified FastAPI/Swagger entrypoint (2nd entry, same image)
activities/<domain>/         The execution "limbs" (one deployment per domain)
  activities.py              The @activity.defn surface ONLY — thin: settings,
                               activity.logger, and a call into a module below
  <technology>.py            One module per dependency, taking PLAIN PARAMETERS and
                               holding no settings and no Temporal, so it is testable
                               with no worker: values_repo.py (git), server_scan.py
                               (the inventory API), cluster_api.py (the Kubernetes
                               API, its idempotency rule and its error classification)
  worker_init.py             Registers activities, polls that queue
docs/                             The static documentation site (its own image, no code)
```

Prod charts are VENDORED into the Argo CD repo, at
`redbull-platform/gitops/charts/<service>/`: `workflows-orchestrator` (brain: ONE release for
all domains), `segment-lifecycle-worker` and `server-lifecycle-worker` (limbs: one per domain). One generic ApplicationSet
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
  segment is created without one — §4.) The domain currently holds TWO workflows
  (`initialize-segment`, `allocate-segment`), each on its own workflow queue, sharing the one limb
  deployment. Id builders live in `shared/workflow_ids.py` — ONE definition per scheme, importable
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
  doesn't degrades to status-only.

## 3. Deployment-target agnostic

- No orchestrator code knows kind vs OpenShift — all endpoints come from env vars (`TEMPORAL_HOST`,
  `SEGMENTS_MANAGER_URL`, `DAY1_REPO_URL`, ...). Same images run anywhere; only Helm
  `values.yaml` (`config.*`) differs.
- Local kind reaches host services via `host.docker.internal` — that string lives ONLY in Helm
  values, never in code.
- Env naming: a service prefix is used ONLY for that service's own values (its URL, its URI paths) —
  `DAY1_*` and `DHCP_*` are the current examples. Our own policy inputs carry no prefix. Do NOT put a
  policy of ours behind a dependency's prefix: it reads as their config and travels with them when
  they go.

## 4. External dependencies are black boxes

- **server-scan is the inventory source of record for install-server**
  (`SERVER_SCAN_URL`). The workflow makes ONE read — `GET /servers/available`
  — and never queries a vendor manager itself: server-scan already collects HP
  OneView / UCS Central / Dell OME / Intersight / standalone Redfish on a
  6-hour cron and knows which servers are unclaimed. This is what replaced the
  `bmh-generator-operator` Kopf operator and its four vendor SDKs. The endpoint
  live-rechecks each candidate it returns, so freshness is its problem, not
  ours; `live_recheck_performed` per item says when it degraded to the stored
  document. It returns DATA ONLY — never BMC credentials, which server-scan
  does not hold; those stay in this worker's own Secret.
- **Select bond members from `interfaces[]`, NEVER from `nic_macs`.**
  server-scan reduces Dell NPAR partitions to one entry per physical port in
  `interfaces` but leaves `nic_macs` whole on purpose, so a 4-port partitioned
  card reports 4 interfaces and 16 MACs. Indexing the MAC list positionally —
  as bmhgen did — can bond two partitions of ONE physical port: one wire, no
  redundancy, and it looks correct until that wire fails.
- **install-server requires link_state UP strictly**, and that excludes whole
  vendors by construction: HPE OneView's `portMap` carries no link state at
  all (server-scan stores UNKNOWN unconditionally) and Intersight vNICs
  usually report none. Those servers fail bond selection with
  `NoBondableInterfacesError` naming the provider and the states observed —
  deliberately loud, so the gap is visible rather than looking like an empty
  inventory. Decided with the operator, 2026-09-25.
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
- **In install-server, EVERY reason a candidate is unusable is a SKIP.** The
  multi-candidate draw exists so one unusable server is a retry rather than a
  failed run, and a reason handled outside the selection loop silently breaks
  that promise for part of the fleet — a pool of three dying on a STANDALONE
  machine at the front while two installable servers sit behind it. The reasons
  are two tables in `install_server.py` (`_REJECTION_TYPE`, `_REJECTION_SUMMARY`)
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
  dependency — see §5.
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
  truth and Crossplane the only writer (we never POST a scope), and convergence is judged on the
  EXCLUSIONS we wrote, never the DHCP API's own derived range. When create-cluster verifies the scope
  after ITS merge step, lift the removed code (`git log -S get_dhcp_scope`) into that workflow,
  bounded deadline and all.
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

## 5. Temporal SDK rules (gotchas — must follow)

- **Sandbox + Pydantic:** in workflow files wrap all `shared/` imports in
  `with workflow.unsafe.imports_passed_through():` (Pydantic C-extensions crash the sandbox; also
  skips reload). Worker entrypoints run outside the sandbox — import normally.
- **Pydantic data converter on EVERY `Client.connect(...)`** (workers AND `api.py`): pass
  `temporalio.contrib.pydantic.pydantic_data_converter`, else Pydantic payloads can't serialize.
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
  reads as "retryable" and there is nothing to notice. install-server keeps a
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
- **Verify after mutate, before recording:** allocate-segment reads the allocation back
  (`get_segment`: status/cluster/vlan/type must all match — the allocation WROTE the type) BEFORE the
  git write, so the values repo can
  never record a vlan the Segments Manager does not confirm. A mutation whose outcome another system
  will act on gets a read-back check between the mutation and the recording.
- Workflow IDs are deterministic (`initialize-segment-<network address, CIDR mask dropped>`,
  e.g. `initialize-segment-130.154.20.0`; `allocate-segment-<TYPE>-<cluster>`) for natural dedup — a
  duplicate trigger while running gets HTTP 409 (in the bulk route, an `already_running` item).
  Builders in `shared/workflow_ids.py`.
- **Cross-workflow races on one record: compare-and-set, server-side.** When two runs may mutate the
  same record, the mutation carries the value it expects to replace and the owning service applies it
  atomically, so the loser gets a 409 (classified non-retryable) instead of silently overwriting the
  winner — while a plain Temporal retry must still short-circuit as success when the stored value
  already equals the NEW one. Precedent: convert-segment's `expected_type` (removed with the
  workflow, §4). No current workflow needs it; the rule stands for the next one that does.

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
  Secret). A
  domain worker mounts BOTH + its Secrets, so the
  brain release must install before any limb (else `CreateContainerConfigError` on the missing
  global ConfigMap) — and the chart must ship the new keys BEFORE (or with) an image that requires
  them, or the worker crash-loops on its fail-fast settings.
- **`shared/settings.py`** groups: `TemporalSettings` (workers + api.py),
  `SegmentLifecycleActivitySettings` and `ServerLifecycleActivitySettings`
  (each that domain's activity worker only). Field names = Helm ConfigMap/Secret keys
  lowercased — keep aligned with redbull-platform's `gitops/charts/workflows-orchestrator/templates/config.yaml`
  (global) and `gitops/charts/segment-lifecycle-worker/templates/config.yaml` (day1 URL + DHCP
  policy + the credential Secret). Which ConfigMap
  a key lives in is INDEPENDENT of which settings class declares it (pydantic reads the flat merged pod
  env — `SEGMENTS_MANAGER_URL` sits in the global ConfigMap yet stays a
  `SegmentLifecycleActivitySettings` field; the brain ignores extras via `extra="ignore"`). Do NOT
  import settings from inside a workflow definition (sandbox) — only from entrypoints / api.py / activities.
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

- Three charts, NONE creates a Namespace (each deploys into whichever namespace the release targets —
  `helm install -n <ns> [--create-namespace]`, or redbull-platform's `namespaces` release pre-creates
  it): redbull-platform's `gitops/charts/workflows-orchestrator/` (ConfigMap + brain + SA),
  `gitops/charts/segment-lifecycle-worker/` and `gitops/charts/server-lifecycle-worker/`
  (each: ConfigMap + Secrets + limb + SA). This repo ships NO chart at all — `helm/` is gone with
  the mock service it held. The server-lifecycle limb is the first to need real RBAC: it creates
  secrets, metal3.io/baremetalhosts and agent-install.openshift.io/nmstateconfigs in the target
  namespace, where segment-lifecycle only makes HTTP and git calls.
- Assumed already running: a Temporal server and the Segments Manager (via OpenShift routes or
  localhost — no port assumptions in code).
- Trigger via the unified API: `uvicorn workflow_domains.api:app --port 8080`, Swagger at `/docs`.
  `POST /workflows/server-lifecycle/install-server` is ASYNC (202 + workflow id) and takes the
  InfraEnv to fill plus its MCE cluster; the InfraEnv's name states which hardware it is for
  (`cisco-m6-bat-yam-64c-512gb`) and server names carry the same tokens behind an `ocp-` prefix,
  so the InfraEnv IS the server query. Its id keys on the CANDIDATE POOL — the InfraEnv alone, with
  no MCE in it — which makes installs drawing from one pool serial: server-scan hands out
  candidates without reserving them, so two concurrent runs could otherwise draw the same machine,
  and two MCEs filling an InfraEnv of the same name draw from the same pool.
  `POST /workflows/segment-lifecycle/initialize-segment` is ASYNC (202 + workflow id) and takes
  the full segment definition; poll `GET /workflows/runs/{workflow_id}` for progress/result. The
  `/bulk` variant takes a list and starts ONE WORKFLOW PER SEGMENT (never one batch workflow — each
  segment has its own dedup id, its own run status and its own failure), answering 202 with a
  per-item report rather than a single pass/fail code. `POST .../allocate-segment` is normally
  called by the day1 pipeline (`{"cluster", "values_branch": "$CI_COMMIT_BRANCH"}`), which then
  polls the same runs endpoint until COMPLETED (§4).
- In-cluster the API is `workflows-orchestrator-api` (ClusterIP:8080) plus an OpenShift **Route**
  (`workflowsApi.route.*` in the `workflows-orchestrator` chart) — it needs a hostname because starting a workflow
  is now an operator action. NOTE: `workflow_domains/api.py` has NO auth of its own; anyone who can reach
  that hostname can start a workflow. Disable the Route (and port-forward) where that matters.
