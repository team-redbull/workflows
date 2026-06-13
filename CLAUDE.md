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
  logging_config.py              Shared worker logging setup
workflows/                       The orchestration "brain" (lightweight deployment)
  <name>.py                      Workflow logic
  main_worker.py                 Registers workflows, polls the workflow queue
activities/<domain>/             The execution "limbs" (one deployment per domain)
  <name>_tasks.py                Concrete activity implementations
  main_worker.py                 Registers activities, polls that domain's queue
dev/mock-generator/              LOCAL-DEV ONLY stand-in for an external black-box API
helm/segment-allocation/         Helm chart deploying all resources (kind or OpenShift)
api.py                           Unified FastAPI/Swagger entrypoint to start workflows
```

- **Workflows and activities are fundamentally separate** — separate code, separate
  deployments, separate images, separate task queues, separate RBAC/Secrets.
- The workflow "brain" packages into one lightweight deployment.
- Each `activities/<domain>/` packages into its own deployment with its own
  ServiceAccount, Secrets, and (when needed) heavy container image.

### `shared/` rules (the contract)
- Lightweight ONLY: `temporalio` + `pydantic`. **Never** heavy execution libraries
  here (no `kubernetes`, `boto3`, `ansible-runner`, `httpx`, etc.).
- `interfaces/` holds `@activity.defn` signatures with a `...`/`pass` body. The real
  implementation lives in `activities/<domain>/` and is registered against that name.
- This layer is the typed contract: it prevents typos, enforces type safety, and lets
  workflows route by exact signature.

## 2. Activity / deployment partitioning

- The driver for a **separate deployment** is **shared dependency set + RBAC/Secrets**,
  NOT the sub-workflow boundary per se.
- For now, segment-allocation activities live under `activities/segment_allocation/`
  (one cohesive dep+cred set: Segments Manager HTTP, generator HTTP, future gitops).
- Keep signatures in `shared/interfaces/` clean so an activity (e.g. "update segments
  mongo") can be re-registered on another queue by a future sub-workflow without moving
  code.

## 3. Deployment-target agnostic

- Nothing in orchestrator code knows about kind vs OpenShift. All endpoints come from
  env vars (`TEMPORAL_HOST`, `SEGMENT_MANAGER_URL`, `GENERATOR_URL`, ...).
- The same images run anywhere; only Helm `values.yaml` (`config.*`) differs.
- Local kind reaches host services (`temporal server start-dev`, Segments Manager) via
  `host.docker.internal` — that string lives ONLY in Helm values, never in code.

## 4. External dependencies are black boxes

- The segment generator (IPAM) is another team's service in production. The orchestrator
  just makes an HTTP call to `GENERATOR_URL` and **trusts the response** (structural
  parse only).
- `dev/mock-generator/` is a local stand-in, isolated and dev-only. It is the ONLY place
  site-prefix logic (`site1:192,site2:193,site3:194`) lives. In prod, point
  `GENERATOR_URL` at the real service and set `mockGenerator.enabled=false`.
- Site-prefix / validity is NOT re-checked in the orchestrator: prod segments are already
  valid and the Segments Manager validates on write. `site` is a plain `str`.

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
  Mark non-retryable error types explicitly when a failure is deterministic.
- **httpx timeout < activity `start_to_close_timeout`:** give every `httpx.AsyncClient`
  an explicit `timeout=` strictly below the activity timeout (currently 10s < 30s), so a
  network hang fails the HTTP call and frees the worker before Temporal reaps the
  activity and leaves the thread hung.
- **Per-invocation HTTP client:** create `httpx.AsyncClient` INSIDE each activity via
  `async with`, so auth/session cookies are scoped to one invocation and never leak
  across concurrent activity runs.

## 6. Idempotency (required for all activities)

- Network calls fail and Temporal retries — activities must be strictly idempotent.
- Do not blindly duplicate infrastructure: use UPSERTs, check-before-create, idempotency
  keys, and treat "already exists" responses as success.
- Examples here: `create_segment` treats duplicate-VLAN/409 as success; `allocate_segment`
  relies on the Segments Manager returning the cluster's existing segment if already
  allocated. Workflow IDs are deterministic (`segment-allocation-<site>-<cluster>`) for
  natural dedup.

## 7. Strict validation & clean typed state (coding preferences)

- **Strict, absolute verification** — no broad/tolerant thresholds. If N conditions are
  required, fail loudly if even one is missing.
- **Typed state only** — pass Pydantic models (or dataclasses) between workflows and
  activities. Never untyped dicts across the boundary.
- Keep validation where it belongs: structural validation on our own models; trust
  black-box external services rather than re-deriving their rules.

## 8. Configuration

- Env vars are the config surface. `.env.example` documents them; `.env` is gitignored.
- Decision in progress: migrate `os.environ.get(...)` to a `pydantic-settings`
  `BaseSettings` (`shared/settings.py`) for fail-fast typed config + `.env` loading.
  Do NOT import settings from inside a workflow definition (sandbox) — only from worker
  entrypoints / `api.py` / activities.

## 9. Deploy & run (local)

- Helm chart at `helm/segment-allocation/` creates namespace + ConfigMap + Secret +
  two workers (each its own ServiceAccount) + the dev mock generator (gated by
  `mockGenerator.enabled`).
- Prereqs on host: `temporal server start-dev` (`:7233`, UI `:8223`), Segments Manager
  (`:8000`). Build the three images, `kind load` into `prep-temporal`, `helm install`.
- Trigger workflows via the unified API: `uvicorn api:app --port 8080`, Swagger at
  `/docs`, `POST /workflows/segment-allocation`.
