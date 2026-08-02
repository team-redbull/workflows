"""Segment-connectivity workflow HTTP surface — the single entry point.

This is where a segment's life starts: the caller POSTs the segment
DEFINITION here, and the workflow creates it in the Segments Manager itself
(create_segment, step 1) before opening its firewall rules. The Segments
Manager no longer triggers anything — so the whole flow, creation included,
is one Temporal run visible in the Temporal UI.

ASYNC trigger: POST returns 202 with the workflow id immediately (next-request
approval is human-driven and can take hours); the caller polls
GET /workflows/segment-connectivity/{workflow_id} for status/result.

All routes hang off one prefixed APIRouter so the shared `/workflows/<domain>`
prefix lives in exactly one place (mounted by workflows/api.py).
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from temporalio.client import Client, WorkflowExecutionStatus, WorkflowFailureError
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode

from shared.consts import OPEN_SEGMENT_RULES_WORKFLOW_QUEUE
from shared.models.segment_connectivity import (
    OpenSegmentRulesInput,
    OpenSegmentRulesProgress,
    OpenSegmentRulesResult,
    OpenSegmentRulesRunArgs,
)
from workflows.routers.deps import get_temporal_client
from workflows.open_segment_rules import OpenSegmentRulesWorkflow

router = APIRouter(prefix="/workflows/segment-connectivity", tags=["segment-connectivity"])


# API-layer request/response models — these never cross the workflow boundary.
class StartSegmentConnectivityResponse(BaseModel):
    workflow_id: str
    run_id: str


class BulkSegmentConnectivityInput(BaseModel):
    """Many segment definitions in one request (e.g. a CSV import).

    Fans out to one workflow PER SEGMENT rather than one workflow for the
    batch: each segment gets its own deterministic id (natural dedup), its own
    independently-approved firewall requests, and its own failure — a bad
    definition in row 7 must not hold up or fail row 8, and a batch-shaped
    workflow could offer none of that.
    """

    segments: list[OpenSegmentRulesInput] = Field(min_length=1)


class BulkSegmentConnectivityItem(BaseModel):
    """Per-segment outcome of the FAN-OUT only — not of the workflow, which by
    definition has barely begun. `started` means Temporal accepted the run."""

    segment: str
    workflow_id: str
    status: Literal["started", "already_running", "failed"]
    run_id: str | None = None
    error: str | None = None


class BulkStartSegmentConnectivityResponse(BaseModel):
    started: int
    already_running: int
    failed: int
    results: list[BulkSegmentConnectivityItem]


class SegmentConnectivityStatusResponse(BaseModel):
    workflow_id: str
    status: str  # RUNNING / COMPLETED / FAILED / TERMINATED / ...
    progress: OpenSegmentRulesProgress | None = None  # while RUNNING (workflow query)
    result: OpenSegmentRulesResult | None = None  # when COMPLETED
    error: str | None = None  # when FAILED/CANCELED/TERMINATED/TIMED_OUT


def _workflow_id(rules_input: OpenSegmentRulesInput) -> str:
    """Deterministic, URL-safe id: natural dedup per (type, segment).

    Prefixed with the WORKFLOW name, not the domain: a second workflow in this
    domain acting on the same segment (e.g. a future close-segment-rules) must
    get a distinct id, or it would collide with this one and be rejected as
    already-started.

    The CIDR mask is dropped from the id (e.g. 130.154.20.0/24 -> the id ends
    ...-130.154.20.0), so two requests for the same network address dedup
    regardless of how the mask was written.
    """
    network = rules_input.segment.split("/", 1)[0]
    return f"open-segment-rules-{rules_input.type.value}-{network}"


async def _start(client: Client, rules_input: OpenSegmentRulesInput):
    """Start one run. The single and bulk routes differ only in how they
    report the outcome, never in how the workflow is started."""
    return await client.start_workflow(
        OpenSegmentRulesWorkflow.run,
        OpenSegmentRulesRunArgs(input=rules_input),
        id=_workflow_id(rules_input),
        task_queue=OPEN_SEGMENT_RULES_WORKFLOW_QUEUE,
    )


@router.post("", response_model=StartSegmentConnectivityResponse, status_code=202)
async def start_segment_connectivity(
    rules_input: OpenSegmentRulesInput,
    client: Client = Depends(get_temporal_client),
) -> StartSegmentConnectivityResponse:
    """Create a segment and open its connectivity — returns immediately (202).

    The body is the full segment definition; the workflow creates it in the
    Segments Manager as its first step. Semantic validation (site, CIDR,
    overlap, VLAN) happens there, so an invalid definition surfaces as a FAILED
    workflow on the status endpoint, not as a 4xx here.
    """
    try:
        handle = await _start(client, rules_input)
    except WorkflowAlreadyStartedError:
        raise HTTPException(
            status_code=409,
            detail=(
                "Segment-connectivity workflow already running: "
                f"{_workflow_id(rules_input)}"
            ),
        )
    return StartSegmentConnectivityResponse(
        workflow_id=handle.id, run_id=handle.result_run_id or ""
    )


@router.post(
    "/bulk", response_model=BulkStartSegmentConnectivityResponse, status_code=202
)
async def start_segment_connectivity_bulk(
    bulk_input: BulkSegmentConnectivityInput,
    client: Client = Depends(get_temporal_client),
) -> BulkStartSegmentConnectivityResponse:
    """Start one workflow per segment definition — returns immediately (202).

    Always 202 with a per-item report, never a single pass/fail status: the
    fan-out is partial by nature (some runs start, some are already running,
    some are rejected by Temporal), and collapsing that into one status code
    would hide which segments actually got a workflow. Duplicate CIDRs within
    one request collapse onto the same deterministic workflow id, so the
    second occurrence reports `already_running`.
    """

    async def _start_one(
        rules_input: OpenSegmentRulesInput,
    ) -> BulkSegmentConnectivityItem:
        workflow_id = _workflow_id(rules_input)
        try:
            handle = await _start(client, rules_input)
        except WorkflowAlreadyStartedError:
            return BulkSegmentConnectivityItem(
                segment=rules_input.segment,
                workflow_id=workflow_id,
                status="already_running",
            )
        except Exception as exc:  # noqa: BLE001 — one bad row must not sink the batch
            return BulkSegmentConnectivityItem(
                segment=rules_input.segment,
                workflow_id=workflow_id,
                status="failed",
                error=str(exc),
            )
        return BulkSegmentConnectivityItem(
            segment=rules_input.segment,
            workflow_id=handle.id,
            status="started",
            run_id=handle.result_run_id or "",
        )

    results = list(await asyncio.gather(*(_start_one(s) for s in bulk_input.segments)))
    counts = {status: 0 for status in ("started", "already_running", "failed")}
    for item in results:
        counts[item.status] += 1
    return BulkStartSegmentConnectivityResponse(
        started=counts["started"],
        already_running=counts["already_running"],
        failed=counts["failed"],
        results=results,
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
    progress: OpenSegmentRulesProgress | None = None
    result: OpenSegmentRulesResult | None = None
    error: str | None = None

    if description.status == WorkflowExecutionStatus.RUNNING:
        # Best effort — degrade to status-only if the workflow worker is down,
        # so this endpoint never hangs on a query.
        try:
            progress = await handle.query(
                OpenSegmentRulesWorkflow.progress,
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
