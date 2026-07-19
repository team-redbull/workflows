"""Unified FastAPI/Swagger entrypoint to start orchestrator workflows.

Thin composition root: it owns the Temporal client (opened once in `lifespan`,
shared to routers via `app.state`) and mounts one prefixed APIRouter per
workflow domain. Adding a domain's HTTP surface = write
workflows/routers/<domain>.py and add one `include_router` call below — never
edit route handlers here.

Run locally:  PYTHONPATH=. uvicorn workflows.api:app --port 8080   (Swagger at /docs)
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter

from shared.settings import TemporalSettings
from workflows.routers import segment_connectivity

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
app.include_router(segment_connectivity.router)
