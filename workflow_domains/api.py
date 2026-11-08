"""Unified FastAPI/Swagger entrypoint to start orchestrator workflows.

Thin composition root: it owns the Temporal client (opened once in `lifespan`,
shared to routers via `app.state`) and mounts one prefixed APIRouter per
workflow domain, plus the domain-agnostic run-status router. Adding a domain's
HTTP surface = write workflow_domains/<domain>/router.py and add one
`include_router` call below — never edit route handlers here.

Paths are `/workflows/<domain>/<workflow>` for triggers (a domain holds many
workflows, so it is never itself an endpoint) and `/workflows/runs/{id}` for
status (workflow ids are globally unique — one status route serves them all).

Run locally:  PYTHONPATH=. uvicorn workflow_domains.api:app --port 8080   (Swagger at /docs)
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from shared.settings import TemporalSettings
from shared.temporal_client import connect_temporal
from workflow_domains.cluster_upgrade.router import router as cluster_upgrade_router
from workflow_domains.cluster_upgrade.ui import router as cluster_upgrade_ui_router
from workflow_domains.routers.runs import router as runs_router
from workflow_domains.segment_lifecycle.router import (
    router as segment_lifecycle_router,
)
from workflow_domains.server_lifecycle.router import (
    router as server_lifecycle_router,
)
from workflow_domains.server_lifecycle.ui import router as server_lifecycle_ui_router
from workflow_domains.server_provisioning.router import (
    router as server_provisioning_router,
)
from workflow_domains.vm_lifecycle.router import router as vm_lifecycle_router

_settings = TemporalSettings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.temporal_client = await connect_temporal(_settings)
    yield


app = FastAPI(title="Cluster Orchestrator API", lifespan=lifespan)
app.include_router(segment_lifecycle_router)
app.include_router(server_lifecycle_router)
app.include_router(server_provisioning_router)
# upgrade-cluster and upgrade-targets: a started run's activities wait,
# visible as pending, on any limb not deployed yet (DESIGN O18).
app.include_router(cluster_upgrade_router)
# create-vm: a run's activities wait, visible as pending, on the vm-lifecycle
# limb and on the KubeVirt clusters' agents until both are deployed.
app.include_router(vm_lifecycle_router)
app.include_router(runs_router)
# The temporary cluster-upgrade page, on the routes above.
app.include_router(cluster_upgrade_ui_router)
# The temporary server-lifecycle page: install, remove, uninstall and
# publish-infraenv-to-pxe-server runs, on the routes above.
app.include_router(server_lifecycle_ui_router)
