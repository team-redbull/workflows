"""Segment-connectivity HTTP surface — the domain's workflows, one path each.

This is where a segment's life starts: the caller POSTs the segment
DEFINITION here, and the workflow creates it in the Segments Manager itself
(create_segment, step 1) before opening its firewall rules. The Segments
Manager no longer triggers anything — so the whole flow, creation included,
is one Temporal run visible in the Temporal UI.

ASYNC trigger: POST returns 202 with the workflow id immediately (next-request
approval is human-driven and can take hours); the caller polls
GET /workflows/runs/{workflow_id} (workflows/routers/runs.py) for status/result.

PATHS ARE `/workflows/<domain>/<workflow>`. The DOMAIN prefix lives in exactly
one place (this router, mounted by workflows/api.py) and every workflow in the
domain adds its own path under it — a domain holds many workflows, so it can
never be the endpoint of one of them. Adding, say, a close-segment-rules
workflow here is then a new route, not a redesign.
"""

from __future__ import annotations

import asyncio
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from temporalio.client import Client
from temporalio.exceptions import WorkflowAlreadyStartedError

from shared.consts import OPEN_SEGMENT_RULES_WORKFLOW_QUEUE
from shared.models.segment_connectivity import (
    OpenSegmentRulesInput,
    OpenSegmentRulesRunArgs,
)
from workflows.routers.deps import get_temporal_client
from workflows.routers.models import StartWorkflowResponse
from workflows.open_segment_rules import OpenSegmentRulesWorkflow

router = APIRouter(prefix="/workflows/segment-connectivity", tags=["segment-connectivity"])

# One workflow of this domain = one path under the domain prefix.
_OPEN_SEGMENT_RULES_PATH = "/open-segment-rules"


# API-layer request/response models — these never cross the workflow boundary.
# Named after the WORKFLOW, not the domain: they describe one workflow's input
# and its per-segment outcome, so a sibling workflow in this domain could not
# reuse them. The domain-agnostic StartWorkflowResponse comes from
# workflows/routers/models.py instead.
class BulkOpenSegmentRulesInput(BaseModel):
    """Many segment definitions in one request (e.g. a CSV import).

    Fans out to one workflow PER SEGMENT rather than one workflow for the
    batch: each segment gets its own deterministic id (natural dedup), its own
    independently-approved firewall requests, and its own failure — a bad
    definition in row 7 must not hold up or fail row 8, and a batch-shaped
    workflow could offer none of that.
    """

    segments: list[OpenSegmentRulesInput] = Field(min_length=1)


class BulkOpenSegmentRulesItem(BaseModel):
    """Per-segment outcome of the FAN-OUT only — not of the workflow, which by
    definition has barely begun. `started` means Temporal accepted the run."""

    segment: str
    workflow_id: str
    status: Literal["started", "already_running", "failed"]
    run_id: str | None = None
    error: str | None = None


class BulkStartOpenSegmentRulesResponse(BaseModel):
    started: int
    already_running: int
    failed: int
    results: list[BulkOpenSegmentRulesItem]


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


@router.post(
    _OPEN_SEGMENT_RULES_PATH,
    response_model=StartWorkflowResponse,
    status_code=202,
)
async def start_open_segment_rules(
    rules_input: OpenSegmentRulesInput,
    client: Client = Depends(get_temporal_client),
) -> StartWorkflowResponse:
    """Create a segment and open its connectivity — returns immediately (202).

    The body is the full segment definition; the workflow creates it in the
    Segments Manager as its first step. Semantic validation (site, CIDR,
    overlap, VLAN) happens there, so an invalid definition surfaces as a FAILED
    workflow on GET /workflows/runs/{workflow_id}, not as a 4xx here.
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
    return StartWorkflowResponse(
        workflow_id=handle.id, run_id=handle.result_run_id or ""
    )


@router.post(
    f"{_OPEN_SEGMENT_RULES_PATH}/bulk",
    response_model=BulkStartOpenSegmentRulesResponse,
    status_code=202,
)
async def start_open_segment_rules_bulk(
    bulk_input: BulkOpenSegmentRulesInput,
    client: Client = Depends(get_temporal_client),
) -> BulkStartOpenSegmentRulesResponse:
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
    ) -> BulkOpenSegmentRulesItem:
        workflow_id = _workflow_id(rules_input)
        try:
            handle = await _start(client, rules_input)
        except WorkflowAlreadyStartedError:
            return BulkOpenSegmentRulesItem(
                segment=rules_input.segment,
                workflow_id=workflow_id,
                status="already_running",
            )
        except Exception as exc:  # noqa: BLE001 — one bad row must not sink the batch
            return BulkOpenSegmentRulesItem(
                segment=rules_input.segment,
                workflow_id=workflow_id,
                status="failed",
                error=str(exc),
            )
        return BulkOpenSegmentRulesItem(
            segment=rules_input.segment,
            workflow_id=handle.id,
            status="started",
            run_id=handle.result_run_id or "",
        )

    results = list(await asyncio.gather(*(_start_one(s) for s in bulk_input.segments)))
    counts = {status: 0 for status in ("started", "already_running", "failed")}
    for item in results:
        counts[item.status] += 1
    return BulkStartOpenSegmentRulesResponse(
        started=counts["started"],
        already_running=counts["already_running"],
        failed=counts["failed"],
        results=results,
    )
