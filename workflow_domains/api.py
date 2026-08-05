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
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter

from shared.settings import TemporalSettings
from workflow_domains.routers.runs import router as runs_router
from workflow_domains.segment_lifecycle.router import (
    router as segment_lifecycle_router,
)

_settings = TemporalSettings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.temporal_client = await Client.connect(
        _settings.temporal_host,
        namespace=_settings.temporal_namespace,
        data_converter=pydantic_data_converter,
    )
    yield


app = FastAPI(title="Cluster Orchestrator API", lifespan=lifespan)
app.include_router(segment_lifecycle_router)
app.include_router(runs_router)
