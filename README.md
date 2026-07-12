# Cluster Orchestrator — Connectivity

Connectivity sub-workflow of an OpenShift cluster lifecycle orchestrator built on
Temporal. Given a `segment` (CIDR) and its `type`, it opens firewall rules against
every same-site MCE segment via the black-box **next** connectivity service, waits for the
(human-approved) requests to complete, then flips the segment's status
`Locked -> Available` in the team's **Segments Manager**.

Phase 1 implements the `HC` type; other types fail loudly until implemented.

## Layout

```
api.py                          Unified FastAPI/Swagger entrypoint (async trigger + status)
shared/                         Contract layer (temporalio + pydantic only)
  models/connectivity.py        Typed state across the workflow/activity boundary
  interfaces/connectivity.py    Activity signatures (no bodies)
  settings.py / exceptions.py / consts.py / logging_config.py
workflows/                      The brain (ConnectivityWorkflow + main_worker_init.py)
activities/connectivity/        The limb (activity impls + worker_init.py)
dev/mock-connectivity/          LOCAL-DEV stand-in for the next service (black box)
helm/connectivity/              Helm chart deploying the two workers (kind or OpenShift)
```

Worker-file naming convention: the workflow (brain) worker is
`workflows/main_worker_init.py`; each activity domain's worker is
`activities/<domain>/worker_init.py`.

## Flow

1. `validate_segment_exists(segment, type)` — fail fast before touching the firewall.
2. `list_mce_segments(segment, type)` — every MCE CIDR in the same site as the
   input segment (the peer set for all supported types).
3. `submit_open_rules(...)` x2 per MCE segment (both directions), all in parallel.
   Port policy per direction comes from the ConfigMap (`PORTS_HC_TO_MCE`, ...).
4. `publish_request_ids(segment, ids)` — `PUT /api/segments/connectivity-requests`
   so the Segments Manager UI shows the pending request ids beside the segment's
   status while approval is awaited.
5. Poll `check_connectivity_requests(ids)` until every request is `complete`.
   Approval is HUMAN-driven (minutes -> hours+): the workflow polls forever with
   backoff (15s -> 5m cap) and rolls history over with `continue_as_new` — it
   never fails on a slow approval. Whenever requests complete, the published id
   list shrinks accordingly; the final empty update removes the display.
6. `unlock_segment(segment)` — `POST /api/segments/unlock` (status Locked -> Available).

The trigger is async: `POST /workflows/connectivity` returns **202 + workflow id**
immediately; poll `GET /workflows/connectivity/{workflow_id}` for phase/pending
counts (workflow query) and the final result.

## Design notes

- **Deployment-agnostic:** endpoints come from env (`TEMPORAL_HOST`,
  `SEGMENT_MANAGER_URL`, `NEXT_URL`, `NEXT_*_URI`). The same images run on kind or
  OpenShift; only the Helm `values.yaml` (`config.*`) changes.
  `host.docker.internal` appears only there, never in code.
- **next is a black box:** the orchestrator token-renews and HTTP-calls `NEXT_URL`;
  `dev/mock-connectivity` is the dev-only stand-in and is NOT deployed by the chart —
  `config.nextUrl` simply points at it in dev and at the real service in prod.
- **Ports live in the ConfigMap** (`helm/connectivity/templates/config.yaml`), as
  compact JSON per protocol; the activity layer expands them into the next API's
  structure and validates the syntax at worker startup. Changing ports = edit the
  ConfigMap + restart the activity workers. No rebuild.
- **Pydantic data converter** is registered on every `Client.connect` (workers + api).
- **httpx timeout (10s) < activity start_to_close_timeout (30s)** so a network hang
  frees the worker before Temporal reaps the activity. Each `httpx.AsyncClient` is
  per-invocation (`async with`), so next tokens never leak across concurrent runs.
- **Idempotency:** unlock treats "already unlocked" as success; re-submitting
  identical open-rules requests converges to the same firewall state;
  `publish_request_ids` is a replace-style PUT (re-sends are a no-op); workflow ids
  are deterministic (`connectivity-<TYPE>-<segment with / replaced by ->`), so a
  duplicate trigger while running gets HTTP 409.

## Run locally

Assumed already running: a Temporal server (`TEMPORAL_HOST`) and the Segments
Manager (`SEGMENT_MANAGER_URL` — e.g. the OpenShift route), with `API_TOKEN`
matching `SEGMENT_MANAGER_API_TOKEN`.

```bash
cp .env.example .env    # then point it at your Temporal / Segments Manager

# The mock next service (dev only; approval delay configurable)
cd dev/mock-connectivity && COMPLETION_DELAY_SECONDS=60 uvicorn app:app --port 9000 &

# Workers (from the repo root)
pip install -r activities/connectivity/requirements.txt
PYTHONPATH=. python -m workflows.main_worker_init &
PYTHONPATH=. python -m activities.connectivity.worker_init &

# Unified API
pip install -r requirements.txt
PYTHONPATH=. uvicorn api:app --port 8080
# Swagger UI: http://localhost:8080/docs
# curl -X POST localhost:8080/workflows/connectivity \
#   -H 'content-type: application/json' -d '{"segment":"130.154.20.0/24","type":"HC"}'
# curl localhost:8080/workflows/connectivity/connectivity-HC-130.154.20.0-24
```

Inspect runs in the Temporal UI and verify the segment's `status` in the manager:
`curl "$SEGMENT_MANAGER_URL/api/segments?type=HC"`.

While the workflow waits for approval (~60s with the mock's default delay), the
Segments Manager UI shows a **Requests ID** button beside the segment's status —
click it for a popover with the pending next request ids. The button disappears
on its own once every request completes and the segment unlocks.

### kind

```bash
docker build -f workflows/Dockerfile -t connectivity-workflow:dev .
docker build -f activities/connectivity/Dockerfile -t connectivity-activity:dev .
docker build -t mock-connectivity:dev dev/mock-connectivity   # run outside the chart

kind load docker-image connectivity-workflow:dev connectivity-activity:dev --name prep-temporal
helm install connectivity helm/connectivity
```

## Deploying elsewhere (e.g. air-gapped OpenShift)

Push the two worker images to a registry the cluster can pull from, then:

```bash
helm install connectivity helm/connectivity \
  --set workflowWorker.image.repository=<registry>/connectivity-workflow \
  --set activityWorker.image.repository=<registry>/connectivity-activity \
  --set config.temporalHost=<temporal-host>:7233 \
  --set config.segmentManagerUrl=https://<segments-manager-route> \
  --set config.nextUrl=https://<real-next-service> \
  --set config.nextTokenRenewalUri=<real-path> \
  --set config.nextOpenRulesUri=<real-path> \
  --set config.nextCheckStatusUri=<real-path> \
  --set secrets.segmentManagerApiToken=<real-token>
```

No mock is ever deployed by the chart — `config.nextUrl` is the only knob. Edit
the `PORTS_*` keys in the live `orchestrator-config` ConfigMap (then restart the
activity workers) to change the port policy without a rebuild.
