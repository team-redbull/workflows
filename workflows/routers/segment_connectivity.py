"""Segment-connectivity workflow HTTP surface.

ASYNC trigger: POST returns 202 with the workflow id immediately (next-request
approval is human-driven and can take hours); the caller polls
GET /workflows/segment-connectivity/{workflow_id} for status/result.

All routes hang off one prefixed APIRouter so the shared `/workflows/<domain>`
prefix lives in exactly one place (mounted by workflows/api.py).
"""

from __future__ import annotations

from datetime import timedelta

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from temporalio.client import Client, WorkflowExecutionStatus, WorkflowFailureError
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode

from shared.consts import SEGMENT_CONNECTIVITY_WORKFLOW_QUEUE
from shared.models.segment_connectivity import (
    SegmentConnectivityInput,
    SegmentConnectivityProgress,
    SegmentConnectivityResult,
    SegmentConnectivityRunArgs,
)
from workflows.routers.deps import get_temporal_client
from workflows.segment_connectivity import SegmentConnectivityWorkflow

router = APIRouter(prefix="/workflows/segment-connectivity", tags=["segment-connectivity"])


# API-layer response models — these never cross the workflow boundary.
class StartSegmentConnectivityResponse(BaseModel):
    workflow_id: str
    run_id: str


class SegmentConnectivityStatusResponse(BaseModel):
    workflow_id: str
    status: str  # RUNNING / COMPLETED / FAILED / TERMINATED / ...
    progress: SegmentConnectivityProgress | None = None  # while RUNNING (workflow query)
    result: SegmentConnectivityResult | None = None  # when COMPLETED
    error: str | None = None  # when FAILED/CANCELED/TERMINATED/TIMED_OUT


def _workflow_id(connectivity_input: SegmentConnectivityInput) -> str:
    """Deterministic, URL-safe id: natural dedup per (type, segment).

    The CIDR mask is dropped from the id (e.g. 130.154.20.0/24 -> the id ends
    ...-130.154.20.0), so two requests for the same network address dedup
    regardless of how the mask was written.
    """
    network = connectivity_input.segment.split("/", 1)[0]
    return f"segment-connectivity-{connectivity_input.type.value}-{network}"


@router.post("", response_model=StartSegmentConnectivityResponse, status_code=202)
async def start_segment_connectivity(
    connectivity_input: SegmentConnectivityInput,
    client: Client = Depends(get_temporal_client),
) -> StartSegmentConnectivityResponse:
    """Start the segment-connectivity workflow and return immediately (202)."""
    workflow_id = _workflow_id(connectivity_input)
    try:
        handle = await client.start_workflow(
            SegmentConnectivityWorkflow.run,
            SegmentConnectivityRunArgs(input=connectivity_input),
            id=workflow_id,
            task_queue=SEGMENT_CONNECTIVITY_WORKFLOW_QUEUE,
        )
    except WorkflowAlreadyStartedError:
        raise HTTPException(
            status_code=409,
            detail=f"Segment-connectivity workflow already running: {workflow_id}",
        )
    return StartSegmentConnectivityResponse(
        workflow_id=handle.id, run_id=handle.result_run_id or ""
    )


@router.get(
    "/{workflow_id}",
    response_model=SegmentConnectivityStatusResponse,
    response_model_exclude_none=True,
)
async def get_segment_connectivity_status(
    workflow_id: str,
    client: Client = Depends(get_temporal_client),
) -> SegmentConnectivityStatusResponse:
    """Status of a segment-connectivity workflow: state, live progress, final result."""
    handle = client.get_workflow_handle(workflow_id)
    try:
        description = await handle.describe()
    except RPCError as exc:
        if exc.status == RPCStatusCode.NOT_FOUND:
            raise HTTPException(status_code=404, detail=f"Workflow not found: {workflow_id}")
        raise

    status = description.status.name if description.status else "UNKNOWN"
    progress: SegmentConnectivityProgress | None = None
    result: SegmentConnectivityResult | None = None
    error: str | None = None

    if description.status == WorkflowExecutionStatus.RUNNING:
        # Best effort — degrade to status-only if the workflow worker is down,
        # so this endpoint never hangs on a query.
        try:
            progress = await handle.query(
                SegmentConnectivityWorkflow.progress,
                rpc_timeout=timedelta(seconds=5),
            )
        except Exception:  # noqa: BLE001 — worker unavailable / query timeout
            progress = None
    elif description.status == WorkflowExecutionStatus.COMPLETED:
        result = await handle.result()
    elif description.status in (
        WorkflowExecutionStatus.FAILED,
        WorkflowExecutionStatus.CANCELED,
        WorkflowExecutionStatus.TERMINATED,
        WorkflowExecutionStatus.TIMED_OUT,
    ):
        # Surface why it ended: walk the cause chain to the root failure —
        # WorkflowFailureError and ActivityError are generic wrappers; the
        # ApplicationError underneath carries the real message.
        try:
            await handle.result()
        except WorkflowFailureError as exc:
            root: BaseException = exc
            while getattr(root, "cause", None) is not None:
                root = root.cause  # type: ignore[assignment]
            error = str(root)
        except Exception as exc:  # noqa: BLE001 — still report the status
            error = str(exc)

    return SegmentConnectivityStatusResponse(
        workflow_id=workflow_id,
        status=status,
        progress=progress,
        result=result,
        error=error,
    )
