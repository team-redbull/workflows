"""Segment-lifecycle HTTP surface — the domain's workflows, one path each.

This is where a segment's life starts: the caller POSTs the segment
DEFINITION here, and the workflow creates it in the Segments Manager itself.
The Segments Manager never triggers anything — so creation is one Temporal
run, visible in the Temporal UI and retried until it succeeds.

ASYNC trigger throughout: POST returns 202 with the workflow id immediately and
the caller polls GET /workflows/runs/{workflow_id}
(workflow_domains/routers/runs.py) for status/result. Async even though the
workflows are short now — the id IS the dedup key, so handing it back at once
is what lets a caller re-poll (or recognise a duplicate) rather than depending
on holding one HTTP connection open.

PATHS ARE `/workflows/<domain>/<workflow>`. The DOMAIN prefix lives in exactly
one place (this router, mounted by workflow_domains/api.py) and every workflow in the
domain adds its own path under it — a domain holds many workflows, so it can
never be the endpoint of one of them. Adding a fourth workflow here is then a
new route, not a redesign.
"""

from __future__ import annotations

import asyncio
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from temporalio.client import Client
from temporalio.exceptions import WorkflowAlreadyStartedError

from shared.consts import (
    ALLOCATE_SEGMENT_WORKFLOW_QUEUE,
    CONVERT_SEGMENT_WORKFLOW_QUEUE,
    INITIALIZE_SEGMENT_WORKFLOW_QUEUE,
)
from shared.models.segment_lifecycle import (
    AllocateSegmentInput,
    AllocateSegmentRunArgs,
    ConvertSegmentInput,
    ConvertSegmentRunArgs,
    InitializeSegmentInput,
    InitializeSegmentRunArgs,
)
from shared.workflow_ids import (
    allocate_segment_workflow_id,
    convert_segment_workflow_id,
    initialize_segment_workflow_id,
)
from workflow_domains.routers.deps import get_temporal_client
from workflow_domains.routers.models import StartWorkflowResponse
from workflow_domains.segment_lifecycle.allocate_segment import (
    AllocateSegmentWorkflow,
)
from workflow_domains.segment_lifecycle.convert_segment import (
    ConvertSegmentWorkflow,
)
from workflow_domains.segment_lifecycle.initialize_segment import (
    InitializeSegmentWorkflow,
)

router = APIRouter(prefix="/workflows/segment-lifecycle", tags=["segment-lifecycle"])

# One workflow of this domain = one path under the domain prefix.
_INITIALIZE_SEGMENT_PATH = "/initialize-segment"
_ALLOCATE_SEGMENT_PATH = "/allocate-segment"
_CONVERT_SEGMENT_PATH = "/convert-segment"


# API-layer request/response models — these never cross the workflow boundary.
# Named after the WORKFLOW, not the domain: they describe one workflow's input
# and its per-segment outcome, so a sibling workflow in this domain could not
# reuse them. The domain-agnostic StartWorkflowResponse comes from
# workflow_domains/routers/models.py instead.
class BulkInitializeSegmentInput(BaseModel):
    """Many segment definitions in one request (e.g. a CSV import).

    Fans out to one workflow PER SEGMENT rather than one workflow for the
    batch: each segment gets its own deterministic id (natural dedup), its own
    run status and its own failure — a bad definition in row 7 must not hold up
    or fail row 8, and a batch-shaped workflow could offer none of that.
    """

    segments: list[InitializeSegmentInput] = Field(min_length=1)


class BulkInitializeSegmentItem(BaseModel):
    """Per-segment outcome of the FAN-OUT only — not of the workflow, which by
    definition has barely begun. `started` means Temporal accepted the run."""

    segment: str
    workflow_id: str
    status: Literal["started", "already_running", "failed"]
    run_id: str | None = None
    error: str | None = None


class BulkStartInitializeSegmentResponse(BaseModel):
    started: int
    already_running: int
    failed: int
    results: list[BulkInitializeSegmentItem]


# Deterministic workflow ids come from shared/workflow_ids.py — the ONE
# definition per scheme, so the id a route builds and the id anything else
# builds for the same segment can never drift apart.
def _workflow_id(rules_input: InitializeSegmentInput) -> str:
    return initialize_segment_workflow_id(rules_input.type, rules_input.segment)


async def _start(client: Client, rules_input: InitializeSegmentInput):
    """Start one run. The single and bulk routes differ only in how they
    report the outcome, never in how the workflow is started."""
    return await client.start_workflow(
        InitializeSegmentWorkflow.run,
        InitializeSegmentRunArgs(input=rules_input),
        id=_workflow_id(rules_input),
        task_queue=INITIALIZE_SEGMENT_WORKFLOW_QUEUE,
    )


@router.post(
    _INITIALIZE_SEGMENT_PATH,
    response_model=StartWorkflowResponse,
    status_code=202,
)
async def start_initialize_segment(
    rules_input: InitializeSegmentInput,
    client: Client = Depends(get_temporal_client),
) -> StartWorkflowResponse:
    """Create a segment — returns immediately (202).

    The body is the full segment definition; the workflow creates it in the
    Segments Manager, and the segment is Available as soon as the run
    completes. Semantic validation (site, CIDR, overlap, VLAN) belongs to the
    Segments Manager, so an invalid definition surfaces as a FAILED workflow on
    GET /workflows/runs/{workflow_id}, not as a 4xx here — this route only
    rejects a body that does not fit InitializeSegmentInput at all.

    Every segment type is accepted, PXE included. Re-POSTing a definition for a
    segment that already exists is harmless (the activity is idempotent); doing
    so while its run is still going gets a 409 on the dedup id.
    """
    try:
        handle = await _start(client, rules_input)
    except WorkflowAlreadyStartedError:
        raise HTTPException(
            status_code=409,
            detail=(
                "Segment-lifecycle workflow already running: "
                f"{_workflow_id(rules_input)}"
            ),
        )
    return StartWorkflowResponse(
        workflow_id=handle.id, run_id=handle.result_run_id or ""
    )


@router.post(
    f"{_INITIALIZE_SEGMENT_PATH}/bulk",
    response_model=BulkStartInitializeSegmentResponse,
    status_code=202,
)
async def start_initialize_segment_bulk(
    bulk_input: BulkInitializeSegmentInput,
    client: Client = Depends(get_temporal_client),
) -> BulkStartInitializeSegmentResponse:
    """Start one workflow per segment definition — returns immediately (202).

    Always 202 with a per-item report, never a single pass/fail status: the
    fan-out is partial by nature (some runs start, some are already running,
    some are rejected by Temporal), and collapsing that into one status code
    would hide which segments actually got a workflow. Duplicate CIDRs within
    one request collapse onto the same deterministic workflow id, so the
    second occurrence reports `already_running`.
    """

    async def _start_one(
        rules_input: InitializeSegmentInput,
    ) -> BulkInitializeSegmentItem:
        workflow_id = _workflow_id(rules_input)
        try:
            handle = await _start(client, rules_input)
        except WorkflowAlreadyStartedError:
            return BulkInitializeSegmentItem(
                segment=rules_input.segment,
                workflow_id=workflow_id,
                status="already_running",
            )
        except Exception as exc:  # noqa: BLE001 — one bad row must not sink the batch
            return BulkInitializeSegmentItem(
                segment=rules_input.segment,
                workflow_id=workflow_id,
                status="failed",
                error=str(exc),
            )
        return BulkInitializeSegmentItem(
            segment=rules_input.segment,
            workflow_id=handle.id,
            status="started",
            run_id=handle.result_run_id or "",
        )

    results = list(await asyncio.gather(*(_start_one(s) for s in bulk_input.segments)))
    counts = {status: 0 for status in ("started", "already_running", "failed")}
    for item in results:
        counts[item.status] += 1
    return BulkStartInitializeSegmentResponse(
        started=counts["started"],
        already_running=counts["already_running"],
        failed=counts["failed"],
        results=results,
    )


def _allocate_segment_workflow_id(allocate_input: AllocateSegmentInput) -> str:
    return allocate_segment_workflow_id(allocate_input.type, allocate_input.cluster)


@router.post(
    _ALLOCATE_SEGMENT_PATH,
    response_model=StartWorkflowResponse,
    status_code=202,
)
async def start_allocate_segment(
    allocate_input: AllocateSegmentInput,
    client: Client = Depends(get_temporal_client),
) -> StartWorkflowResponse:
    """Allocate a VLAN segment for a hosted cluster — returns immediately (202).

    The body names the CLUSTER, nothing else: the site is derived from where
    the cluster's values file sits in the day1 values repo, and the segment
    itself is whatever the Segments Manager reserves. Poll
    GET /workflows/runs/{workflow_id} for progress/result; a bad cluster name
    or an exhausted pool surfaces there as a FAILED run, not as a 4xx here.
    """
    try:
        handle = await client.start_workflow(
            AllocateSegmentWorkflow.run,
            AllocateSegmentRunArgs(input=allocate_input),
            id=_allocate_segment_workflow_id(allocate_input),
            task_queue=ALLOCATE_SEGMENT_WORKFLOW_QUEUE,
        )
    except WorkflowAlreadyStartedError:
        raise HTTPException(
            status_code=409,
            detail=(
                "Segment-lifecycle workflow already running: "
                f"{_allocate_segment_workflow_id(allocate_input)}"
            ),
        )
    return StartWorkflowResponse(
        workflow_id=handle.id, run_id=handle.result_run_id or ""
    )


def _convert_segment_workflow_id(convert_input: ConvertSegmentInput) -> str:
    return convert_segment_workflow_id(
        convert_input.site, convert_input.source_type, convert_input.destination_type
    )


@router.post(
    _CONVERT_SEGMENT_PATH,
    response_model=StartWorkflowResponse,
    status_code=202,
)
async def start_convert_segment(
    convert_input: ConvertSegmentInput,
    client: Client = Depends(get_temporal_client),
) -> StartWorkflowResponse:
    """Convert source-type segments to another type — returns immediately (202).

    The workflow searches the site for Available, unassigned segments of the
    source type and re-types up to `quantity` of them (lowest vlan first) in
    the Segments Manager. Each converted segment stays Available and is
    immediately allocatable as its new type — the conversion is the whole
    operation. Fewer matches than requested is reported as a shortfall in the
    result, not an error. Poll GET /workflows/runs/{workflow_id} for
    progress/result. No bulk variant: the request is already batch-shaped.
    """
    try:
        handle = await client.start_workflow(
            ConvertSegmentWorkflow.run,
            ConvertSegmentRunArgs(input=convert_input),
            id=_convert_segment_workflow_id(convert_input),
            task_queue=CONVERT_SEGMENT_WORKFLOW_QUEUE,
        )
    except WorkflowAlreadyStartedError:
        raise HTTPException(
            status_code=409,
            detail=(
                "Segment-lifecycle workflow already running: "
                f"{_convert_segment_workflow_id(convert_input)}"
            ),
        )
    return StartWorkflowResponse(
        workflow_id=handle.id, run_id=handle.result_run_id or ""
    )
