"""Workflow-run status — ONE endpoint for every domain and every workflow.

Plus one list, `GET /workflows/runs?workflow_type=<WorkflowClass>`: the newest
runs of one workflow, status and times only, which is what a page listing
runs needs (the temporary cluster-upgrade UI does). Progress stays on the
per-run endpoint, so a list never waits on a query per run.

Temporal workflow ids are globally unique, so a run's status needs neither the
domain nor the workflow in its path: `GET /workflows/runs/{workflow_id}`
describes any run started by any domain router.

Keeping it out of the domain routers is also what makes a domain hold several
workflows. A `GET /workflows/<domain>/{workflow_id}` catch-all swallows every
sibling's path segment — with `initialize-segment` and a future
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

from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
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


class WorkflowRunSummary(BaseModel):
    workflow_id: str
    workflow_type: str
    status: str  # RUNNING / COMPLETED / FAILED / TERMINATED / ...
    start_time: datetime | None = None
    close_time: datetime | None = None


# A workflow class name: checked so it can go into the visibility query as a literal.
_WORKFLOW_TYPE_PATTERN = r"^[A-Za-z][A-Za-z0-9_]{0,127}$"


@router.get("", response_model=list[WorkflowRunSummary])
async def list_workflow_runs(
    workflow_type: str = Query(pattern=_WORKFLOW_TYPE_PATTERN, description="e.g. UpgradeClusterWorkflow"),
    limit: int = Query(default=50, ge=1, le=200),
    client: Client = Depends(get_temporal_client),
) -> list[WorkflowRunSummary]:
    """The newest runs of one workflow type, newest first: status and times. Progress: GET /{workflow_id}."""
    runs = [
        WorkflowRunSummary(
            workflow_id=execution.id,
            workflow_type=execution.workflow_type,
            status=execution.status.name if execution.status else "UNKNOWN",
            start_time=execution.start_time,
            close_time=execution.close_time,
        )
        async for execution in client.list_workflows(f"WorkflowType = '{workflow_type}'", limit=limit)
    ]
    return sorted(runs, key=lambda run: run.start_time or datetime.min.replace(tzinfo=UTC), reverse=True)


class WorkflowRunStatusResponse(BaseModel):
    workflow_id: str
    workflow_type: str  # e.g. "InitializeSegmentWorkflow" — which workflow this id belongs to
    status: str  # RUNNING / COMPLETED / FAILED / TERMINATED / ...
    progress: dict[str, Any] | None = None  # workflow query — while RUNNING, and after a failure/cancellation
    result: Any | None = None  # when COMPLETED
    error: str | None = None  # when FAILED/CANCELED/TERMINATED/TIMED_OUT


async def _query_progress(handle: Any) -> dict[str, Any] | None:
    """The optional `progress` query, best effort.

    Degrades to None when the workflow worker is down, the workflow exposes no
    such query, or the query times out — so this endpoint never hangs and never
    500s on a workflow that simply doesn't report progress.
    """
    try:
        return await handle.query(_PROGRESS_QUERY, rpc_timeout=_PROGRESS_QUERY_TIMEOUT)
    except Exception:  # noqa: BLE001 — worker unavailable / no such query / timeout
        return None


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
        progress = await _query_progress(handle)
    elif description.status == WorkflowExecutionStatus.COMPLETED:
        result = await handle.result()
    elif description.status in _TERMINAL_FAILURE_STATUSES:
        # A run that died PART WAY is exactly when progress matters most: it is
        # the only place the work already done is reported (allocate-segment's
        # phase, for instance, says whether it died before the allocation or
        # after the values-repo push — and a pushed commit outlives this run).
        # Temporal answers queries on closed workflows by replaying their
        # history, so this costs the same best-effort call as a live one.
        progress = await _query_progress(handle)
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
