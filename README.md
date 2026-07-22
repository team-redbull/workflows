# Cluster Orchestrator — Segment-connectivity

Segment-connectivity sub-workflow of an OpenShift cluster lifecycle orchestrator built on
Temporal. Given a `segment` (CIDR) and its `type`, it opens firewall rules against
every same-site segment of the other types its type peers with, via the black-box
**next** connectivity service, waits for the (human-approved) requests to complete,
then flips the segment's status `Locked -> Available` in the team's **Segments Manager**.

All four types are implemented: `HC`, `INVENTORY` and `PXE` each peer with same-site
`MCE` segments, and `MCE` peers with all three of them — symmetric, driven by the
`PORTS_*` config rather than hardcoded per type. `MCE` segments additionally get one
mandatory, one-directional rule to their site's static BMC network (not tracked by
the Segments Manager — see the Flow section below).

## Layout

```
shared/                         Contract layer (temporalio + pydantic only)
  models/segment_connectivity.py     Typed state across the workflow/activity boundary
  interfaces/segment_connectivity.py Activity signatures (no bodies)
  settings.py / exceptions.py / consts.py / logging_config.py
workflows/                      The brain (SegmentConnectivityWorkflow + main_worker_init.py + api.py)
activities/segment_connectivity/        The limb (activity impls + worker_init.py)
dev/mock-segment-connectivity/          LOCAL-DEV stand-in for the next service (black box)
helm/workflows/           Helm chart for the brain — ONE release, shared by every domain
helm/segment-connectivity/               Helm chart for the segment-connectivity limb (kind or OpenShift)
```

Worker-file naming convention: the workflow (brain) worker is
`workflows/main_worker_init.py`; each activity domain's worker is
`activities/<domain>/worker_init.py`.

## Flow

1. `get_segment_site(segment, type)` — validate the segment exists (fail fast
   before touching the firewall) and learn its site in one fetch.
2. `list_peer_segments(source_type, site)` — every same-site segment of the
   other types `source_type` peers with, derived from the configured
   `PORTS_*` profiles (e.g. `HC` -> only `MCE`; `MCE` -> `HC` + `INVENTORY` + `PXE`).
3. `submit_open_rules(...)` x2 per peer segment (both directions), all in parallel.
   Port policy per direction comes from the ConfigMap (`PORTS_HC_TO_MCE`, ...).
   `MCE` segments additionally get one mandatory, one-directional
   `submit_bmc_open_rules(...)` toward `get_bmc_segment(site)` — the site's
   static BMC CIDR from `BMC_SEGMENTS_BY_SITE` (BMC is not tracked by the
   Segments Manager, so this is a ConfigMap lookup, never a Segments Manager
   query, and never peers back).
4. `publish_request_ids(segment, ids, submitted_at)` — `PUT /api/segments/segment-connectivity-requests`
   so the Segments Manager UI shows the pending request ids beside the segment's
   status while approval is awaited. `submitted_at` (captured once via
   `workflow.now()` when the rules were submitted) drives the "time since submit"
   header in the UI popover.
5. Poll `check_segment_connectivity_requests(ids)` until every request is `complete`.
   Approval is HUMAN-driven (minutes -> hours+): the workflow polls forever with
   backoff (15s -> 5m cap) and rolls history over with `continue_as_new` — it
   never fails on a slow approval. Whenever requests complete, the published id
   list shrinks accordingly; the final empty update removes the display.
6. `unlock_segment(segment)` — `POST /api/segments/unlock` (status Locked -> Available).

Activity retries are unbounded (transient outages of the Segments Manager or the
next service are out-waited); only classified deterministic errors — segment not
found, bad API token, missing port profile, unconfigured BMC segment, unsupported
type, unexpected request status — fail the workflow. On such a terminal failure
(or cancellation) the workflow best-effort clears the pending-ids display and
publishes a "workflow failed" note beside the segment's status (the segment stays
Locked; best-effort — the note endpoint exists in the Segments Manager).

The trigger is async: `POST /workflows/segment-connectivity` returns **202 + workflow id**
immediately; poll `GET /workflows/segment-connectivity/{workflow_id}` for phase/pending
counts (workflow query) and the final result.

## Design notes

- **Deployment-agnostic:** endpoints come from env (`TEMPORAL_HOST`,
  `SEGMENTS_MANAGER_URL`, `NEXT_URL`, `NEXT_*_URI`). The same images run on kind or
  OpenShift; only the Helm `values.yaml` (`config.*`) changes.
  `host.docker.internal` appears only there, never in code.
- **ConfigMap split by scope:** `workflows-config` (owned by the
  `workflows` chart) holds the values every domain shares — `TEMPORAL_*`,
  `DOMAIN`, `SEGMENTS_MANAGER_URL`. Each workflow adds its own `<domain>-config`
  (here `segment-connectivity-config`: the `NEXT_*` endpoints + port policy). A domain's
  activity worker mounts both, so install `workflows` before the limb.
- **next is a black box:** the orchestrator token-renews and HTTP-calls `NEXT_URL`;
  `dev/mock-segment-connectivity` is the dev-only stand-in and is NOT deployed by the chart —
  `config.nextUrl` simply points at it in dev and at the real service in prod.
- **Ports live in the ConfigMap** (`helm/segment-connectivity/templates/config.yaml`), as
  compact JSON per protocol; the activity layer expands them into the next API's
  structure and validates the syntax at worker startup. Changing ports = edit the
  ConfigMap + restart the activity workers. No rebuild.
- **BMC is ConfigMap-only, not Segments-Manager-tracked:** `BMC_SEGMENTS_BY_SITE`
  maps site name -> static BMC CIDR; `PORTS_MCE_TO_BMC` is its port policy, same
  shape as every other `PORTS_*` key. Every `MCE` segment opens exactly one
  one-directional rule toward it — never the reverse, and never a peer-discovery
  query.
- **Pydantic data converter** is registered on every `Client.connect` (workers + api).
- **httpx timeout (10s) < activity start_to_close_timeout (30s)** so a network hang
  frees the worker before Temporal reaps the activity. Each `httpx.AsyncClient` is
  per-invocation (`async with`), so next tokens never leak across concurrent runs.
- **Idempotency:** unlock treats "already unlocked" as success; re-submitting
  identical open-rules requests converges to the same firewall state;
  `publish_request_ids` is a replace-style PUT (re-sends are a no-op); workflow ids
  are deterministic (`segment-connectivity-<TYPE>-<segment network address, CIDR mask
  dropped>`), so a duplicate trigger while running gets HTTP 409.

## Run locally

Assumed already running: a Temporal server (`TEMPORAL_HOST`) and the Segments
Manager (`SEGMENTS_MANAGER_URL` — e.g. the OpenShift route), with `API_TOKEN`
matching `SEGMENTS_MANAGER_API_TOKEN`.

```bash
cp .env.example .env    # then point it at your Temporal / Segments Manager

# The mock next service (dev only; approval delay configurable)
cd dev/mock-segment-connectivity && COMPLETION_DELAY_SECONDS=60 uvicorn app:app --port 9000 &

# Workers (from the repo root)
pip install -r activities/segment_connectivity/requirements.txt
PYTHONPATH=. python -m workflows.main_worker_init &
PYTHONPATH=. python -m activities.segment_connectivity.worker_init &

# Unified API
pip install -r requirements.txt
PYTHONPATH=. uvicorn workflows.api:app --port 8080
# Swagger UI: http://localhost:8080/docs
# curl -X POST localhost:8080/workflows/segment-connectivity \
#   -H 'content-type: application/json' -d '{"segment":"130.154.20.0/24","type":"HC"}'
# curl localhost:8080/workflows/segment-connectivity/segment-connectivity-HC-130.154.20.0
```

Inspect runs in the Temporal UI and verify the segment's `status` in the manager:
`curl "$SEGMENTS_MANAGER_URL/api/segments?type=HC"`.

While the workflow waits for approval (~60s with the mock's default delay), the
Segments Manager UI shows a **Requests ID** button beside the segment's status —
click it for a popover showing time elapsed since submission plus the pending
next request ids. The button disappears on its own once every request completes
and the segment unlocks.

### kind

```bash
docker build -f workflows/Dockerfile -t workflows:dev .
docker build -f activities/segment_connectivity/Dockerfile -t segment-connectivity:dev .
docker build -t mock-segment-connectivity:dev dev/mock-segment-connectivity   # run outside the chart

kind load docker-image workflows:dev segment-connectivity:dev --name prep-temporal
helm install workflows helm/workflows -n redbull-workflows --create-namespace
helm install segment-connectivity helm/segment-connectivity -n redbull-workflows
```

Neither chart creates the namespace itself — `--create-namespace` on the first
`helm install` is what creates `redbull-workflows` here. On redbull-platform that
namespace is pre-created by its own `namespaces` release instead, so both charts
are installed there with plain `-n redbull-workflows` (no `--create-namespace`).

## Deploying elsewhere (e.g. air-gapped OpenShift)

Push the two worker images to a registry the cluster can pull from, then:

```bash
# The brain owns workflows-config (the global values), so install it first.
helm install workflows helm/workflows -n redbull-workflows --create-namespace \
  --set image.repository=<registry>/workflows \
  --set config.temporalHost=<temporal-host>:7233 \
  --set config.segmentsManagerUrl=https://<segments-manager-route> \
  --set config.domain=<domain>

# The limb only sets its own next endpoints + token; it reads the global values
# from workflows-config above.
helm install segment-connectivity helm/segment-connectivity -n redbull-workflows \
  --set activityWorker.image.repository=<registry>/segment-connectivity \
  --set config.nextUrl=https://<real-next-service> \
  --set config.nextTokenRenewalUri=<real-path> \
  --set config.nextOpenRulesUri=<real-path> \
  --set config.nextCheckStatusUri=<real-path> \
  --set secrets.segmentsManagerApiToken=<real-token>
```

No mock is ever deployed by either chart — `config.nextUrl` is the only knob. Edit
the `PORTS_*` keys in the live `workflows-config` ConfigMap (then restart the
activity workers) to change the port policy without a rebuild.
