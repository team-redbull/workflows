# Cluster Orchestrator — Segment Allocation

First sub-workflow of an OpenShift cluster lifecycle orchestrator built on Temporal.
Given a `cluster_name` and `site`, it secures a network segment + VLAN + EPG for the
cluster and records the allocation in the team's **Segments Manager**.

## Layout

```
api.py                          Unified FastAPI/Swagger entrypoint for all workflows
shared/                         Contract layer (temporalio + pydantic only)
  models/segment.py             Typed state across the workflow/activity boundary
  interfaces/segment_activities Activity signatures (no bodies)
  exceptions.py / enums.py
workflows/                      The brain (workflow + worker)
activities/segment_allocation/  The limb (activity impls + worker)
dev/mock-generator/             LOCAL-DEV stand-in for the external IPAM (black box)
helm/segment-allocation/        Helm chart deploying all resources (kind or OpenShift)
```

## Flow

1. `get_available_segment(site)` — pick an unallocated segment if one exists.
2. else `generate_segment(site)` (external IPAM) + `create_segment(spec)`.
3. `allocate_segment(cluster, site)` — idempotent assign in the Segments Manager.

## Design notes

- **Deployment-agnostic:** endpoints come from env (`TEMPORAL_HOST`, `SEGMENT_MANAGER_URL`,
  `GENERATOR_URL`). The same images run on kind or OpenShift; only the Helm `values.yaml`
  (`config.*`) changes. `host.docker.internal` appears only there, never in code.
- **Generator is a black box:** the orchestrator just HTTP-calls `GENERATOR_URL`. The
  `dev/mock-generator` is the only place site-prefix logic lives, and it is dev-only.
- **Pydantic data converter** is registered on every `Client.connect` (workers + starter).
- **httpx timeout (10s) < activity start_to_close_timeout (30s)** so a network hang frees
  the worker before Temporal reaps the activity. Each `httpx.AsyncClient` is per-invocation
  (`async with`), so the auth cookie never leaks across concurrent activity runs.

## Run locally (kind cluster `prep-temporal`)

Prereqs on the host: `temporal server start-dev` (`:7233`), Segments Manager (`:8000`).

```bash
# Build images
docker build -f workflows/Dockerfile -t segment-allocation-workflow:dev .
docker build -f activities/segment_allocation/Dockerfile -t segment-allocation-activity:dev .
docker build -t segment-generator-mock:dev dev/mock-generator

# Load into kind
kind load docker-image segment-allocation-workflow:dev segment-allocation-activity:dev \
  segment-generator-mock:dev --name prep-temporal

# Deploy (creates the namespace + all resources)
helm install segment-allocation helm/segment-allocation

# Start the unified API from the host
pip install -r requirements.txt
PYTHONPATH=. uvicorn api:app --port 8080
# Swagger UI: http://localhost:8080/docs
# or: curl -X POST localhost:8080/workflows/segment-allocation \
#       -H 'content-type: application/json' -d '{"cluster_name":"web-cluster","site":"site1"}'
```

Inspect at the Temporal UI (http://localhost:8223) and verify in the manager:
`curl http://localhost:8000/api/segments`.

## Deploying elsewhere (e.g. OpenShift)

Push the three images to a registry the cluster can pull from, then:

```bash
helm install segment-allocation helm/segment-allocation \
  --set workflowWorker.image.repository=<registry>/segment-allocation-workflow \
  --set activityWorker.image.repository=<registry>/segment-allocation-activity \
  --set config.temporalHost=<temporal-host>:7233 \
  --set config.segmentManagerUrl=http://<segments-manager-host> \
  --set config.generatorUrl=<external-ipam-url> \
  --set mockGenerator.enabled=false \
  --set secrets.segmentsPassword=<real-password>
```

`mockGenerator.enabled=false` skips deploying the dev IPAM stand-in once `generatorUrl`
points at the real external service.
