# CLAUDE.md

Guidance for working in this repo: a Temporal-based OpenShift cluster lifecycle
orchestrator. Read this before generating code — it captures the architectural
decisions, Temporal SDK rules, and coding preferences established for this project.

## 1. Domain-Driven Monorepo: three layers, strictly separated

```
shared/                          Contract layer — the API between brain and limbs
  models/                        Pydantic data classes (typed state across boundaries)
  interfaces/                    Activity signatures ONLY (@activity.defn, no body)
  exceptions.py                  Shared custom errors
  consts.py                      Shared constants (e.g. task-queue names)
  settings.py                    pydantic-settings BaseSettings (fail-fast typed config)
  logging_config.py              Shared worker logging setup
workflows/                       The orchestration "brain" (lightweight deployment)
  <name>.py                      Workflow logic
  main_worker_init.py            Registers workflows, polls the workflow queue
activities/<domain>/             The execution "limbs" (one deployment per domain)
  activities.py                  Concrete activity implementations
  worker_init.py                 Registers activities, polls that domain's queue
dev/mock-connectivity/           Test-only stand-in for the external next service (image for helm/mock-connectivity/)
helm/workflow-worker/            Helm chart for the brain — ONE release, shared by every domain
helm/connectivity/               Helm chart for the connectivity limb (one chart per domain)
helm/mock-connectivity/          Helm chart for the mock next service — e2e/test environments ONLY, never prod
api.py                           Unified FastAPI/Swagger entrypoint to start workflows
```

- **Workflows and activities are fundamentally separate** — separate code, separate
  deployments, separate images, separate task queues, separate RBAC/Secrets.
- The workflow "brain" packages into one lightweight deployment.
- Each `activities/<domain>/` packages into its own deployment with its own
  ServiceAccount, Secrets, and (when needed) heavy container image.
- **Worker-file naming convention:** the brain's worker entrypoint is
  `workflows/main_worker_init.py`; each activity domain's is
  `activities/<domain>/worker_init.py` — deliberately different names so the two
  worker kinds are never confused. Dockerfile CMDs must match these module paths.

### `shared/` rules (the contract)
- Lightweight ONLY: `temporalio` + `pydantic` + `pydantic-settings`. **Never** heavy
  execution libraries here (no `kubernetes`, `boto3`, `ansible-runner`, `httpx`, etc.).
- `interfaces/` holds `@activity.defn` signatures with a `...`/`pass` body. The real
  implementation lives in `activities/<domain>/` and is registered against that name.
- This layer is the typed contract: it prevents typos, enforces type safety, and lets
  workflows route by exact signature.

## 2. Activity / deployment partitioning

- The driver for a **separate deployment** is **shared dependency set + RBAC/Secrets**,
  NOT the sub-workflow boundary per se.
- Connectivity activities live under `activities/connectivity/` (one cohesive dep+cred
  set: Segments Manager HTTP + bearer token, next-service HTTP).
- Keep signatures in `shared/interfaces/` clean so an activity (e.g. "unlock segment")
  can be re-registered on another queue by a future sub-workflow without moving code.
- **The brain is ONE deployment for every domain**, not one per workflow. Its Helm
  chart (`helm/workflow-worker/`) is standalone and deployed exactly once; each new
  workflow domain adds its own `activities/<domain>/` + `helm/<domain>/` chart (the
  limb) and registers against `workflow-worker`'s already-running brain — it does not
  get (or need) its own copy of the brain.

## 3. Deployment-target agnostic

- Nothing in orchestrator code knows about kind vs OpenShift. All endpoints come from
  env vars (`TEMPORAL_HOST`, `SEGMENT_MANAGER_URL`, `NEXT_URL`, `NEXT_*_URI`, ...).
- The same images run anywhere; only Helm `values.yaml` (`config.*`) differs.
- Local kind reaches host services via `host.docker.internal` — that string lives ONLY
  in Helm values, never in code.
- Env naming: a service-name prefix (e.g. `NEXT_`) is used ONLY for values that belong
  to that service (its URL, its URI paths). Our own policy inputs (`DOMAIN`, `PORTS_*`)
  carry no service prefix.

## 4. External dependencies are black boxes

- The **next connectivity (firewall) service** is another team's service in production
  (air-gapped). The orchestrator token-renews (`NEXT_TOKEN_RENEWAL_URI`), POSTs
  open-rules requests (`NEXT_OPEN_RULES_URI`) and polls request status
  (`NEXT_CHECK_STATUS_URI`) against `NEXT_URL`, **trusting the responses**
  (structural parse only).
- `dev/mock-connectivity/` is a stand-in for e2e/test environments, isolated from
  production code paths. It's deployed via its own chart, `helm/mock-connectivity/`
  (never installed alongside a production `connectivity` release) — point
  `config.nextUrl` at its Service in test environments; in prod, at the real service.
  Its `COMPLETION_DELAY_SECONDS` simulates the human approval — set directly in
  `helm/mock-connectivity/templates/config.yaml` (edit + restart, no rebuild).
- next request approval is HUMAN-driven and unbounded (minutes → hours → more).
  Workflows must never deadline-fail while waiting: poll with durable timers +
  backoff and `continue_as_new` to keep history bounded.
- **Pending request ids are mirrored into the Segments Manager UI** while the wait
  lasts: after submitting open-rules the workflow calls `publish_request_ids`
  (`PUT /api/segments/connectivity-requests`, replace semantics) and republishes
  whenever the pending set shrinks; the final EMPTY list removes the display, then
  the segment is unlocked. The manager's UI shows the ids behind a "Requests ID"
  button beside the status badge. The workflow captures `workflow.now()` once, at
  submission time (`_open_rules`), and sends it as `submitted_at` on every publish
  call (including republishes and across `continue_as_new`, via
  `ConnectivityResumeState.submitted_at`) — the manager's popover uses it to show
  elapsed time since submission.

## 5. Temporal SDK rules (gotchas — must follow)

- **Sandbox + Pydantic:** in workflow files, wrap all `shared/` imports in
  `with workflow.unsafe.imports_passed_through():`. Pydantic's C-extensions crash the
  sandbox otherwise, and it avoids reload overhead. (Worker entrypoints run outside the
  sandbox, so they import `shared` normally.)
- **Pydantic data converter everywhere a Client connects:** pass
  `temporalio.contrib.pydantic.pydantic_data_converter` to EVERY `Client.connect(...)`
  — both workers AND `api.py`. Without it, Pydantic payloads cannot be serialized.
- **Logging:** use `workflow.logger` inside workflows (prevents replay spam) and
  `activity.logger` inside activities. Shared formatter lives in
  `shared/logging_config.py` (compact format; silences chatty `httpx`; strips the bulky
  Temporal activity-info dict suffix).
- **Routing:** every `execute_activity` sets `task_queue=` to the target domain's queue
  (constants in `shared/consts.py`) so work lands on the correct deployment.
- **Timeouts/retries:** every activity sets `start_to_close_timeout` + a `RetryPolicy`.
  Mark non-retryable error types explicitly when a failure is deterministic
  (e.g. `non_retryable_error_types=["SegmentNotFoundError"]`).
- **Plain exceptions do NOT fail a workflow:** a non-FailureError raised in workflow
  code fails the workflow *task*, which retries forever (the run hangs RUNNING).
  Deterministic failures raised FROM WORKFLOW CODE must be
  `temporalio.exceptions.ApplicationError(..., non_retryable=True)`. (Activity-raised
  custom exceptions are fine — the SDK converts them to ApplicationError with `type` =
  class name.)
- **Single-model workflow argument only:** typed payload conversion is silently
  SKIPPED when the number of payloads differs from the number of declared `run()`
  parameters — a `run(input, resume=None)` started with one payload receives a raw
  dict. Always give `run()` exactly ONE Pydantic argument (wrap extra/internal state
  in it, e.g. `ConnectivityRunArgs{input, resume}`).
- **Polling loops:** `workflow.sleep(...)` is a durable, replay-safe server-side timer
  (never `time.sleep`). For unbounded waits, back off to a capped interval and
  `workflow.continue_as_new(...)` every N cycles so history stays bounded. Changing
  poll constants is a non-deterministic change for in-flight runs.
- **httpx timeout < activity `start_to_close_timeout`:** give every `httpx.AsyncClient`
  an explicit `timeout=` strictly below the activity timeout (currently 10s < 30s), so a
  network hang fails the HTTP call and frees the worker before Temporal reaps the
  activity and leaves the thread hung.
- **Per-invocation HTTP client:** create `httpx.AsyncClient` INSIDE each activity via
  `async with`, so auth tokens/cookies are scoped to one invocation and never leak
  across concurrent activity runs. next tokens are renewed fresh per invocation.

## 6. Idempotency (required for all activities)

- Network calls fail and Temporal retries — activities must be strictly idempotent.
- Do not blindly duplicate infrastructure: use UPSERTs, check-before-create, idempotency
  keys, and treat "already exists / already done" responses as success.
- Examples here: `unlock_segment` treats "Segment already unlocked" (200) as success;
  `submit_open_rules` is idempotent in effect (identical firewall rules converge; a
  retried-but-accepted POST leaves only an orphan request id that is never polled);
  `publish_request_ids` is a replace-style PUT (re-sending the current list answers
  "Segment already up to date").
  Workflow IDs are deterministic (`connectivity-<TYPE>-<segment CIDR, / -> ->`) for
  natural dedup — a duplicate trigger while running gets HTTP 409.

## 7. Strict validation & clean typed state (coding preferences)

- **Strict, absolute verification** — no broad/tolerant thresholds. If N conditions are
  required, fail loudly if even one is missing (e.g. unsupported segment type, empty
  MCE pool, unknown next request status).
- **Typed state only** — pass Pydantic models (or dataclasses) between workflows and
  activities. Never untyped dicts across the boundary.
- Keep validation where it belongs: structural validation on our own models; trust
  black-box external services rather than re-deriving their rules.

## 8. Configuration

- Env vars are the config surface. `.env.example` documents them; `.env` is gitignored.
- `shared/settings.py` holds pydantic-settings `BaseSettings` groups: `TemporalSettings`
  (workers + api.py) and `ConnectivityActivitySettings` (activity worker only).
  **Field names equal the Helm ConfigMap/Secret keys lowercased** — keep
  `shared/settings.py` and `helm/connectivity/templates/config.yaml` aligned.
  Do NOT import settings from inside a workflow definition (sandbox) — only from worker
  entrypoints / `api.py` / activities.
- **ConfigMaps hold minimum, operator-editable data.** Anything an operator may change
  without a rebuild (e.g. the `PORTS_*` per-direction port policy, compact JSON per
  protocol) lives in the ConfigMap — defined directly in the ConfigMap TEMPLATE, not in
  values.yaml — and is expanded/validated in code (fail-fast at worker startup).

## 9. Deploy & run (local)

- Three charts: `helm/workflow-worker/` (ConfigMap + the brain, its own ServiceAccount),
  `helm/connectivity/` (ConfigMap + Secret + the connectivity limb, its own
  ServiceAccount), and `helm/mock-connectivity/` (ConfigMap + Deployment + Service, its
  own ServiceAccount) — the last **only** for e2e/test environments where the real next
  service is unreachable; never install it alongside a production `connectivity`
  release. **None creates a Namespace** — all three deploy into whichever namespace the
  release targets (`helm install -n <ns> [--create-namespace]` standalone;
  redbull-platform's `namespaces` release pre-creates it there). For a bare local
  kind/uvicorn run without any chart, run the mock directly
  (`uvicorn app:app` inside `dev/mock-connectivity/`) and point `config.nextUrl` at it.
- Assumed already running: a Temporal server and the Segments Manager (reached via
  OpenShift routes or localhost — no port assumptions in code).
- Trigger workflows via the unified API: `uvicorn api:app --port 8080`, Swagger at
  `/docs`. `POST /workflows/connectivity` is ASYNC: 202 + workflow id immediately;
  poll `GET /workflows/connectivity/{workflow_id}` for progress/result.
