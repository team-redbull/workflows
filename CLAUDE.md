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
workflows/                   The orchestration "brain" (one lightweight deployment)
  <name>.py                  Workflow logic
  main_worker_init.py        Registers workflows, polls the workflow queue
  api.py                     Unified FastAPI/Swagger entrypoint (2nd entry, same image)
activities/<domain>/         The execution "limbs" (one deployment per domain)
  activities.py / worker_init.py   Concrete impls / registers activities, polls that queue
dev/mock-segment-connectivity/    Test-only stand-in for the external next service
helm/workflows/  helm/segment-connectivity/  helm/mock-segment-connectivity/
                             Brain (ONE release, all domains) / limb (one per domain) / mock (e2e ONLY)
```

- **Workflows and activities are fundamentally separate:** code, deployments, images, task queues,
  RBAC/Secrets. Brain = one lightweight deployment; each `activities/<domain>/` = its own deployment
  + ServiceAccount + Secrets + (when needed) heavy image.
- **Worker-file naming:** brain entry `workflows/main_worker_init.py`; each domain's
  `activities/<domain>/worker_init.py` — deliberately different so the two worker kinds are never
  confused. Dockerfile CMDs must match these module paths.
- **`shared/` is the typed contract, lightweight ONLY** (`temporalio` + `pydantic` +
  `pydantic-settings`; NEVER `kubernetes`/`boto3`/`ansible-runner`/`httpx`). `interfaces/` holds
  `@activity.defn` signatures with `...` bodies; the impl lives in `activities/<domain>/`, registered
  against that name — prevents typos, enforces types, lets workflows route by exact signature.

## 2. Activity / deployment partitioning

- Driver for a **separate deployment** is **shared dependency set + RBAC/Secrets**, not the
  sub-workflow boundary. Segment-connectivity activities live in `activities/segment_connectivity/`
  (one dep+cred set: Segments Manager HTTP + bearer token, next HTTP).
- Keep `shared/interfaces/` signatures clean so an activity (e.g. "unlock segment") can be
  re-registered on another queue by a future sub-workflow without moving code.
- **Brain is ONE deployment for every domain**, not one per workflow — `helm/workflows/` is
  standalone, deployed once. Each new domain adds `activities/<domain>/` + `helm/<domain>/` and
  registers against the already-running brain.
- **Resource naming:** brain Deployment + ServiceAccount = `workflows`; its trigger API =
  `workflows-api` (reuses the `workflows` SA). Each domain's Deployment + SA is named by the domain
  only — e.g. `segment-connectivity`, NOT `segment-connectivity-activity-worker`. Chart/release
  names match resource names.

## 3. Deployment-target agnostic

- No orchestrator code knows kind vs OpenShift — all endpoints come from env vars (`TEMPORAL_HOST`,
  `SEGMENTS_MANAGER_URL`, `NEXT_URL`, `NEXT_*_URI`, ...). Same images run anywhere; only Helm
  `values.yaml` (`config.*`) differs.
- Local kind reaches host services via `host.docker.internal` — that string lives ONLY in Helm
  values, never in code.
- Env naming: a service prefix (e.g. `NEXT_`) is used ONLY for that service's own values (its URL,
  its URI paths). Our own policy inputs (`DOMAIN`, `PORTS_*`) carry no prefix.

## 4. External dependencies are black boxes

- The **next connectivity (firewall) service** is another team's air-gapped service. The orchestrator
  token-renews (`NEXT_TOKEN_RENEWAL_URI`), POSTs open-rules (`NEXT_OPEN_RULES_URI`), and polls status
  (`NEXT_CHECK_STATUS_URI`) against `NEXT_URL`, **trusting responses** (structural parse only).
- `dev/mock-segment-connectivity/` stands in for e2e/test, isolated from prod paths; deployed via
  `helm/mock-segment-connectivity/` (never alongside a prod `segment-connectivity` release). Point
  `config.nextUrl` at its Service in test, at the real service in prod. Its `COMPLETION_DELAY_SECONDS`
  (set in that chart's config.yaml — edit + restart, no rebuild) simulates the human approval.
- **Approval is HUMAN-driven and unbounded** (minutes → hours → more). Workflows must never
  deadline-fail while waiting: durable timers + backoff + `continue_as_new` to keep history bounded.
- **Pending request ids mirror into the Segments Manager UI** while waiting: after submitting
  open-rules the workflow calls `publish_request_ids` (`PUT /api/segments/segment-connectivity-requests`,
  replace semantics), republishes whenever the pending set shrinks; the final EMPTY list removes the
  display (behind the UI's "Requests ID" button), then the segment unlocks. `workflow.now()` is
  captured once at submission (`_open_rules`) and sent as `submitted_at` on every publish (incl.
  republishes and across `continue_as_new`, via `SegmentConnectivityResumeState.submitted_at`) for
  the UI's elapsed-time popover.
- **Terminal failure is surfaced, not silent:** on a post-validation non-retryable failure or
  cancellation, best-effort `publish_segment_connectivity_failure` clears the pending-ids display and
  publishes a failure note with the orphaned ids (`PUT /api/segments/segment-connectivity-failure`).
  The segment intentionally stays Locked (connectivity NOT established). Any error is swallowed
  (display still cleared). TERMINATION (vs cancellation) can never run this cleanup.

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
- **Plain exceptions do NOT fail a workflow:** a non-FailureError in workflow code fails the workflow
  *task*, which retries forever (run hangs RUNNING). Deterministic failures raised FROM WORKFLOW code
  must be `temporalio.exceptions.ApplicationError(...)` — do NOT set `non_retryable=True` there (inert;
  workflow failures aren't retried). Activity-raised custom exceptions are fine — the SDK converts them
  to ApplicationError (`type` = class name); there `non_retryable=True` IS meaningful.
- **Single-model workflow argument only:** typed conversion is silently SKIPPED when payload count ≠
  declared `run()` param count (a `run(input, resume=None)` started with one payload gets a raw dict).
  Give `run()` exactly ONE Pydantic arg, wrapping extra/internal state (e.g.
  `SegmentConnectivityRunArgs{input, resume}`).
- **Polling loops:** `workflow.sleep(...)` is a durable replay-safe server-side timer (never
  `time.sleep`). For unbounded waits, back off to a capped interval and `continue_as_new` every N
  cycles so history stays bounded. Changing poll constants is a non-deterministic change for in-flight runs.
- **httpx timeout < activity `start_to_close_timeout`** (currently 60s < 90s): give every
  `httpx.AsyncClient` an explicit `timeout=` below the activity timeout so a network hang fails the call
  and frees the worker before Temporal reaps the activity.
- **Per-invocation HTTP client:** create `httpx.AsyncClient` INSIDE each activity via `async with` so
  auth tokens/cookies scope to one invocation and never leak across concurrent runs; next tokens are
  renewed fresh per invocation.

## 6. Idempotency (required for all activities)

- Network calls fail and Temporal retries — activities must be strictly idempotent: UPSERTs,
  check-before-create, idempotency keys; treat "already exists / already done" as success. Examples:
  `unlock_segment` treats "Segment already unlocked" (200) as success; `submit_open_rules` converges
  (a retried-but-accepted POST leaves only an orphan request id never polled); `publish_request_ids`
  is a replace-style PUT.
- Workflow IDs are deterministic (`segment-connectivity-<TYPE>-<network address, CIDR mask dropped>`,
  e.g. `segment-connectivity-HC-130.154.20.0`) for natural dedup — a duplicate trigger while running
  gets HTTP 409.

## 7. Strict validation & clean typed state

- **Strict, absolute verification** — no broad/tolerant thresholds; fail loudly if even one of N
  required conditions is missing (unsupported segment type, empty MCE pool, unknown next request status).
- **Typed state only** — pass Pydantic models/dataclasses across boundaries, never untyped dicts.
  Structural validation on our own models; trust black-box externals rather than re-deriving their rules.

## 8. Configuration

- Env vars are the config surface (`.env.example` documents them; `.env` gitignored).
- **ConfigMaps split by scope, not chart-convenience:** `workflows-config` (GLOBAL; owned by the
  always-present `helm/workflows/` brain release) holds only shared values — `TEMPORAL_HOST`,
  `TEMPORAL_NAMESPACE`, `DOMAIN`, `SEGMENTS_MANAGER_URL`. `<domain>-config` (owned by that domain's
  chart) holds its own endpoints/policy — e.g. `segment-connectivity-config` = `NEXT_*` URIs +
  `PORTS_*`. A domain worker mounts BOTH + its Secret, so the brain release must install before any
  limb (else `CreateContainerConfigError` on the missing global ConfigMap).
- **`shared/settings.py`** groups: `TemporalSettings` (workers + api.py) and
  `SegmentConnectivityActivitySettings` (activity worker only). Field names = Helm ConfigMap/Secret keys
  lowercased — keep aligned with `helm/workflows/templates/config.yaml` (global) and
  `helm/segment-connectivity/templates/config.yaml` (connectivity keys + token Secret). Which ConfigMap
  a key lives in is INDEPENDENT of which settings class declares it (pydantic reads the flat merged pod
  env — `DOMAIN`/`SEGMENTS_MANAGER_URL` sit in the global ConfigMap yet stay
  `SegmentConnectivityActivitySettings` fields; the brain ignores extras via `extra="ignore"`). Do NOT
  import settings from inside a workflow definition (sandbox) — only from entrypoints / api.py / activities.
- **ConfigMaps hold minimum, operator-editable data** — anything changeable without a rebuild (e.g. the
  `PORTS_*` per-direction port policy, compact JSON per protocol) lives directly in the ConfigMap
  TEMPLATE (not values.yaml), expanded/validated in code (fail-fast at worker startup).

## 9. Deploy & run (local)

- Three charts, NONE creates a Namespace (all deploy into whichever namespace the release targets —
  `helm install -n <ns> [--create-namespace]`, or redbull-platform's `namespaces` release pre-creates
  it): `helm/workflows/` (ConfigMap + brain + SA), `helm/segment-connectivity/` (ConfigMap + Secret +
  limb + SA), `helm/mock-segment-connectivity/` (ConfigMap + Deployment + Service + SA) — the last for
  e2e/test ONLY, never alongside a prod `segment-connectivity` release. For a bare kind/uvicorn run
  without a chart, run the mock directly (`uvicorn app:app` in `dev/mock-segment-connectivity/`) and
  point `config.nextUrl` at it.
- Assumed already running: a Temporal server and the Segments Manager (via OpenShift routes or
  localhost — no port assumptions in code).
- Trigger via the unified API: `uvicorn workflows.api:app --port 8080`, Swagger at `/docs`.
  `POST /workflows/segment-connectivity` is ASYNC (202 + workflow id); poll
  `GET /workflows/segment-connectivity/{workflow_id}` for progress/result.
