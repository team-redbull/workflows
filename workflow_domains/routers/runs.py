"""Workflow-run status — ONE endpoint for every domain and every workflow.

Temporal workflow ids are globally unique, so a run's status needs neither the
domain nor the workflow in its path: `GET /workflows/runs/{workflow_id}`
describes any run started by any domain router.

Keeping it out of the domain routers is also what makes a domain hold several
workflows. A `GET /workflows/<domain>/{workflow_id}` catch-all swallows every
sibling's path segment — with `open-segment-rules` and a future
`close-segment-rules` under the same domain, `GET
/workflows/segment-lifecycle/close-segment-rules` is a status lookup for a
workflow id that happens to be spelled like a route.

Progress and result are workflow-SPECIFIC shapes, so they come back as decoded
JSON rather than a typed model, and the progress query is addressed by NAME —
every workflow wanting a live progress surface defines `@workflow.query def
progress`. A per-workflow response model here would mean one status route per
workflow, which is exactly what this endpoint exists to avoid.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from temporalio.client import Client, WorkflowExecutionStatus, WorkflowFailureError
from temporalio.service import RPCError, RPCStatusCode

from workflow_domains.routers.deps import get_temporal_client

router = APIRouter(prefix="/workflows/runs", tags=["runs"])

# Name of the optional progress query a workflow may expose (see module docstring).
_PROGRESS_QUERY = "progress"
# Short: the endpoint must answer even when the workflow worker is down.
_PROGRESS_QUERY_TIMEOUT = timedelta(seconds=5)

_TERMINAL_FAILURE_STATUSES = (
    WorkflowExecutionStatus.FAILED,
    WorkflowExecutionStatus.CANCELED,
    WorkflowExecutionStatus.TERMINATED,
    WorkflowExecutionStatus.TIMED_OUT,
)


class WorkflowRunStatusResponse(BaseModel):
    workflow_id: str
    workflow_type: str  # e.g. "OpenSegmentRulesWorkflow" — which workflow this id belongs to
    status: str  # RUNNING / COMPLETED / FAILED / TERMINATED / ...
    progress: dict[str, Any] | None = None  # while RUNNING (workflow query)
    result: Any | None = None  # when COMPLETED
    error: str | None = None  # when FAILED/CANCELED/TERMINATED/TIMED_OUT


@router.get(
    "/{workflow_id}",
    response_model=WorkflowRunStatusResponse,
    response_model_exclude_none=True,
)
async def get_workflow_run_status(
    workflow_id: str,
    client: Client = Depends(get_temporal_client),
) -> WorkflowRunStatusResponse:
    """Status of any workflow run: state, live progress, final result."""
    handle = client.get_workflow_handle(workflow_id)
    try:
        description = await handle.describe()
    except RPCError as exc:
        if exc.status == RPCStatusCode.NOT_FOUND:
            raise HTTPException(status_code=404, detail=f"Workflow not found: {workflow_id}")
        raise

    status = description.status.name if description.status else "UNKNOWN"
    progress: dict[str, Any] | None = None
    result: Any | None = None
    error: str | None = None

    if description.status == WorkflowExecutionStatus.RUNNING:
        # Best effort — degrade to status-only when the workflow worker is down
        # or the workflow exposes no `progress` query, so this endpoint never
        # hangs and never 500s on a workflow that simply doesn't report progress.
        try:
            progress = await handle.query(
                _PROGRESS_QUERY, rpc_timeout=_PROGRESS_QUERY_TIMEOUT
            )
        except Exception:  # noqa: BLE001 — worker unavailable / no such query / timeout
            progress = None
    elif description.status == WorkflowExecutionStatus.COMPLETED:
        result = await handle.result()
    elif description.status in _TERMINAL_FAILURE_STATUSES:
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

    return WorkflowRunStatusResponse(
        workflow_id=workflow_id,
        workflow_type=description.workflow_type,
        status=status,
        progress=progress,
        result=result,
        error=error,
    )
