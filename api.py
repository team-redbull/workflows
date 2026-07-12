"""Unified FastAPI/Swagger entrypoint to start orchestrator workflows.

The connectivity trigger is ASYNC: POST returns 202 with the workflow id
immediately (next-request approval is human-driven and can take hours), and the
caller polls GET /workflows/connectivity/{workflow_id} for status/result.

Run locally:  PYTHONPATH=. uvicorn api:app --port 8080   (Swagger at /docs)
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import timedelta

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode

from shared.consts import CONNECTIVITY_WORKFLOW_QUEUE
from shared.models.connectivity import (
    ConnectivityInput,
    ConnectivityProgress,
    ConnectivityResult,
    ConnectivityRunArgs,
)
from shared.settings import TemporalSettings
from workflows.connectivity import ConnectivityWorkflow

_settings = TemporalSettings()


# API-layer response models — these never cross the workflow boundary.
class StartConnectivityResponse(BaseModel):
    workflow_id: str
    run_id: str


class ConnectivityStatusResponse(BaseModel):
    workflow_id: str
    status: str  # RUNNING / COMPLETED / FAILED / TERMINATED / ...
    progress: ConnectivityProgress | None = None  # while RUNNING (workflow query)
    result: ConnectivityResult | None = None  # when COMPLETED


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.temporal_client = await Client.connect(
        _settings.temporal_host,
        namespace=_settings.temporal_namespace,
        data_converter=pydantic_data_converter,
    )
    yield


app = FastAPI(title="Cluster Orchestrator API", lifespan=lifespan)


def _connectivity_workflow_id(connectivity_input: ConnectivityInput) -> str:
    """Deterministic, URL-safe id: natural dedup per (type, segment)."""
    return (
        f"connectivity-{connectivity_input.type.value}-"
        f"{connectivity_input.segment.replace('/', '-')}"
    )


@app.post(
    "/workflows/connectivity",
    response_model=StartConnectivityResponse,
    status_code=202,
)
async def start_connectivity(connectivity_input: ConnectivityInput) -> StartConnectivityResponse:
    """Start the connectivity workflow and return immediately (202)."""
    client: Client = app.state.temporal_client
    workflow_id = _connectivity_workflow_id(connectivity_input)
    try:
        handle = await client.start_workflow(
            ConnectivityWorkflow.run,
            ConnectivityRunArgs(input=connectivity_input),
            id=workflow_id,
            task_queue=CONNECTIVITY_WORKFLOW_QUEUE,
        )
    except WorkflowAlreadyStartedError:
        raise HTTPException(
            status_code=409,
            detail=f"Connectivity workflow already running: {workflow_id}",
        )
    return StartConnectivityResponse(workflow_id=handle.id, run_id=handle.result_run_id or "")


@app.get(
    "/workflows/connectivity/{workflow_id}",
    response_model=ConnectivityStatusResponse,
    response_model_exclude_none=True,
)
async def get_connectivity_status(workflow_id: str) -> ConnectivityStatusResponse:
    """Status of a connectivity workflow: state, live progress, final result."""
    client: Client = app.state.temporal_client
    handle = client.get_workflow_handle(workflow_id)
    try:
        description = await handle.describe()
    except RPCError as exc:
        if exc.status == RPCStatusCode.NOT_FOUND:
            raise HTTPException(status_code=404, detail=f"Workflow not found: {workflow_id}")
        raise

    status = description.status.name if description.status else "UNKNOWN"
    progress: ConnectivityProgress | None = None
    result: ConnectivityResult | None = None

    if description.status == WorkflowExecutionStatus.RUNNING:
        # Best effort — degrade to status-only if the workflow worker is down,
        # so this endpoint never hangs on a query.
        try:
            progress = await handle.query(
                ConnectivityWorkflow.progress,
                rpc_timeout=timedelta(seconds=5),
            )
        except Exception:  # noqa: BLE001 — worker unavailable / query timeout
            progress = None
    elif description.status == WorkflowExecutionStatus.COMPLETED:
        result = await handle.result()

    return ConnectivityStatusResponse(
        workflow_id=workflow_id,
        status=status,
        progress=progress,
        result=result,
    )
