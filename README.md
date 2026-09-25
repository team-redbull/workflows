# Cluster Orchestrator

An OpenShift cluster lifecycle orchestrator built on Temporal. Two domains today:

- **segment-lifecycle** owns the segments a hosted cluster runs on: **creating**
  them in the team's **Segments Manager**, and **allocating** one to a cluster
  and writing its DHCP block into the day1 values repo.
- **server-lifecycle** owns putting physical servers into an MCE inventory:
  **install-server** takes a healthy, unclaimed server from the **server-scan**
  inventory platform and creates the `BareMetalHost` + BMC `Secret` +
  `NMStateConfig` an InfraEnv needs. It replaces `bmh-generator-operator`, a
  Kopf operator that queried four vendor managers itself.

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
  models/segment_lifecycle.py     Typed state across the workflow/activity boundary
  interfaces/segment_lifecycle.py Activity signatures (no bodies)
  settings.py / exceptions.py / consts.py / workflow_ids.py / logging_config.py
workflow_domains/                 The brain — one folder per domain, plus main_worker_init.py + api.py
  segment_lifecycle/              The two workflows + that domain's router.py
  routers/                        What no domain owns: deps, shared models, runs.py (status)
activities/segment_lifecycle/     The limb (activity impls + worker_init.py)
docs/                             The static documentation site (its own image)
```

The prod charts are vendored into the Argo CD repo, not this one:

| chart | what it deploys |
| --- | --- |
| `redbull-platform/gitops/charts/workflows-orchestrator/` | the brain — ONE release, shared by every domain |
| `redbull-platform/gitops/charts/segment-lifecycle-worker/` | the `segment-lifecycle-worker` limb |
| `redbull-platform/gitops/charts/server-lifecycle-worker/` | the `server-lifecycle-worker` limb |

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
and its **MCE cluster**. The run:

1. **resolving-vlan** — reads the MCE's `INVENTORY` segment from the Segments
   Manager (`get_inventory_segment`, on the *segment-lifecycle* queue, where
   that token already lives). There is no `vlan_id` input: a caller-supplied
   VLAN could contradict the segment the cluster owns.
2. **acquiring-server** — `GET /servers/available` against server-scan. The
   InfraEnv's name states which hardware it is for
   (`cisco-m6-bat-yam-64c-512gb`) and server names carry the same tokens behind
   an `ocp-` prefix, so the pattern is `^ocp-<infraEnv>`. `HEALTHY` only.
3. **selecting-bond** — two link-up NICs on two **distinct physical ports**.
   Members come from `interfaces[]`, never `nic_macs`: server-scan reduces NPAR
   partitions to one entry per port in the former and leaves the latter whole,
   so indexing MACs can bond two partitions of one wire. `UP` is required
   strictly, which no HPE server can satisfy — OneView reports no link state at
   all — so those fail loudly rather than being silently skipped.
4. **creating-secret / -baremetalhost / -nmstateconfig** — all idempotent; an
   existing resource is success, never an overwrite. NIC names in the
   NMStateConfig are logical placeholders (`nic1`, `nic2`) bonded 802.3ad with
   the VLAN riding the bond.
5. **verifying-registration** — a bounded poll. A stored BareMetalHost only
   means the API server accepted it; a wrong BMC address or credential surfaces
   nowhere but here.

Its id is `install-server-<infraEnv>-<mce>`, so installs into one target are
serial — server-scan hands out candidates without reserving them, and two
concurrent runs could otherwise draw the same machine.

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
  `segment-lifecycle-config`: `DAY1_REPO_URL` and the DHCP policy). A domain's activity
  worker mounts both, so the brain must install before any limb.
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
  known-permanent error is named in a `non_retryable_error_types` list.
- **Idempotency:** `create_segment` accepts a matching existing segment;
  `allocate_segment` is idempotent server-side per (cluster, site, type); the
  values-repo append is a no-op for a file already recording this allocation.
  Workflow ids are deterministic (`initialize-segment-<network>`,
  `allocate-segment-<TYPE>-<cluster>`), so a duplicate trigger while running gets
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
| `SERVER_SCAN_URL` | `server-lifecycle-config` | inventory API base, INCLUDING `/api/v1` |
| `SERVER_SCAN_API_TOKEN` | Secret | a **viewer** token — the lookup is a GET |
| `{HP,DELL,CISCO,INTERSIGHT}_BMC_USERNAME`/`_PASSWORD` | Secret | what Ironic drives the BMC with |

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
