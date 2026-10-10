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
never be the endpoint of one of them. Each workflow (initialize, allocate,
release) is one route here, and the next one is another route, not a
redesign.
"""

from __future__ import annotations

import asyncio
import re
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator
from temporalio.client import Client
from temporalio.exceptions import WorkflowAlreadyStartedError

from shared.consts import (
    ALLOCATE_SEGMENT_WORKFLOW_QUEUE,
    INITIALIZE_SEGMENT_WORKFLOW_QUEUE,
    RELEASE_SEGMENT_WORKFLOW_QUEUE,
)
from shared.models.segment_lifecycle import (
    AllocateSegmentInput,
    AllocateSegmentRunArgs,
    InitializeSegmentInput,
    InitializeSegmentRunArgs,
    ReleaseSegmentInput,
    ReleaseSegmentRunArgs,
)
from shared.workflow_ids import (
    allocate_segment_workflow_id,
    initialize_segment_workflow_id,
    release_segment_workflow_id,
)
from workflow_domains.routers.deps import get_temporal_client
from workflow_domains.routers.models import StartWorkflowResponse
from workflow_domains.segment_lifecycle.allocate_segment import (
    AllocateSegmentWorkflow,
)
from workflow_domains.segment_lifecycle.initialize_segment import (
    InitializeSegmentWorkflow,
)
from workflow_domains.segment_lifecycle.release_segment import (
    ReleaseSegmentWorkflow,
)

router = APIRouter(prefix="/workflows/segment-lifecycle", tags=["segment-lifecycle"])

# One workflow of this domain = one path under the domain prefix.
_INITIALIZE_SEGMENT_PATH = "/initialize-segment"
_ALLOCATE_SEGMENT_PATH = "/allocate-segment"
_RELEASE_SEGMENT_PATH = "/release-segment"


# API-layer request/response models — these never cross the workflow boundary.
# Named after the WORKFLOW, not the domain: they describe one workflow's input
# and its per-segment outcome, so a sibling workflow in this domain could not
# reuse them. The domain-agnostic StartWorkflowResponse comes from
# workflow_domains/routers/models.py instead.
class InitializeSegmentRequest(InitializeSegmentInput):
    """The initialize-segment request body: InitializeSegmentInput, refusing
    unknown fields.

    Chiefly a `type`: a segment is born typeless and allocate-segment stamps
    the type on, so a caller still sending one expects it to mean something.
    Ignoring it would create the segment and silently drop what they asked
    for; the Segments Manager rejects it on create for the same reason.

    The strictness lives HERE, at the edge, not on InitializeSegmentInput:
    that model is decoded from Temporal history on every replay and from every
    scheduled activity input, and a run started before the type was removed
    carries one in its payload — a forbidding model would fail to decode it
    and wedge the run.
    """

    model_config = ConfigDict(extra="forbid")


# Characters git refuses in a ref name, plus whitespace and control characters.
_BRANCH_FORBIDDEN_CHARS = re.compile(r"[\x00-\x20\x7f~^:?*\[\\]")


class AllocateSegmentRequest(AllocateSegmentInput):
    """The allocate-segment request body: AllocateSegmentInput with the
    values-repo branch REQUIRED and unknown fields refused.

    `values_branch` is required HERE, not on AllocateSegmentInput: that model
    is decoded from Temporal history on every replay, and a run started before
    the field existed carries none — a required field there would fail to
    decode it and wedge the run. At the edge, a caller omitting it gets a 422
    now instead of a run that fails ValuesBranchMissing later; there is no
    configured default, so nothing is ever pushed to main by omission.

    The branch reaches git as an argument, from an API with no auth of its
    own, so it is also held to a ref-name shape here: nothing starting with
    `-` (git would read it as an option), no `..`, no whitespace, control or
    git-forbidden characters. The activity checks the remote actually has it.

    `extra="forbid"` refuses a misspelt field (`branch`, `site`) instead of
    silently allocating as if it had not been sent.
    """

    model_config = ConfigDict(extra="forbid")

    values_branch: str = Field(min_length=1)

    @field_validator("values_branch")
    @classmethod
    def _values_branch_is_a_plain_ref_name(cls, value: str) -> str:
        if value.startswith("-"):
            raise ValueError("must not start with '-'")
        if ".." in value:
            raise ValueError("must not contain '..'")
        if _BRANCH_FORBIDDEN_CHARS.search(value):
            raise ValueError(
                "must not contain whitespace, control characters or any of "
                "~ ^ : ? * [ \\"
            )
        return value


class ReleaseSegmentRequest(ReleaseSegmentInput):
    """The release-segment request body: ReleaseSegmentInput, refusing
    unknown fields.

    Release is keyed by CLUSTER (+ type): the segment's CIDR, site and vlan
    come from the Segments Manager, so a caller sending `segment` or `site`
    expects it to narrow something it does not — refused here instead of
    silently ignored. The strictness lives at the edge, never on
    ReleaseSegmentInput, which is decoded from Temporal history.
    """

    model_config = ConfigDict(extra="forbid")


class BulkInitializeSegmentInput(BaseModel):
    """Many segment definitions in one request (e.g. a CSV import).

    Fans out to one workflow PER SEGMENT rather than one workflow for the
    batch: each segment gets its own deterministic id (natural dedup), its own
    run status and its own failure — a bad definition in row 7 must not hold up
    or fail row 8, and a batch-shaped workflow could offer none of that.
    """

    segments: list[InitializeSegmentRequest] = Field(min_length=1)


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
    return initialize_segment_workflow_id(rules_input.segment)


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
    rules_input: InitializeSegmentRequest,
    client: Client = Depends(get_temporal_client),
) -> StartWorkflowResponse:
    """Create a segment — returns immediately (202).

    The body is the full segment definition; the workflow creates it in the
    Segments Manager, and the segment is Available as soon as the run
    completes. Semantic validation (site, CIDR, overlap, VLAN) belongs to the
    Segments Manager, so an invalid definition surfaces as a FAILED workflow on
    GET /workflows/runs/{workflow_id}, not as a 4xx here — this route only
    rejects a body that does not fit InitializeSegmentRequest at all.

    The definition carries no type: a segment gets one only when
    allocate-segment reserves it. Re-POSTing a definition for a segment that
    already exists is harmless (the activity is idempotent); doing so while its
    run is still going gets a 409 on the dedup id.
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
        rules_input: InitializeSegmentRequest,
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
    # The branch is deliberately NOT part of the id: the Segments Manager's
    # allocation is per (cluster, site, type) whatever the branch, so two
    # branches for one cluster must dedup onto one run and one segment.
    return allocate_segment_workflow_id(allocate_input.type, allocate_input.cluster)


@router.post(
    _ALLOCATE_SEGMENT_PATH,
    response_model=StartWorkflowResponse,
    status_code=202,
)
async def start_allocate_segment(
    allocate_input: AllocateSegmentRequest,
    client: Client = Depends(get_temporal_client),
) -> StartWorkflowResponse:
    """Allocate a VLAN segment for a hosted cluster — returns immediately (202).

    The body names the CLUSTER, the values-repo BRANCH its file lives on
    (`values_branch` — the day1 pipeline passes its own `$CI_COMMIT_BRANCH`)
    and the TYPE to allocate it as (default HC). The site is derived from
    where the cluster's values file sits on that branch, and the segment is
    whichever Available one the Segments Manager reserves — it becomes that
    type in the same step. The run completes once the block is pushed to the
    branch; the DHCP scope follows when a human merges it to main, outside the
    run. Poll GET /workflows/runs/{workflow_id} for progress/result; a bad
    cluster name, a missing branch or an exhausted pool surfaces there as a
    FAILED run, not as a 4xx here.
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


def _release_segment_workflow_id(release_input: ReleaseSegmentInput) -> str:
    return release_segment_workflow_id(release_input.type, release_input.cluster)


@router.post(
    _RELEASE_SEGMENT_PATH,
    response_model=StartWorkflowResponse,
    status_code=202,
)
async def start_release_segment(
    release_input: ReleaseSegmentRequest,
    client: Client = Depends(get_temporal_client),
) -> StartWorkflowResponse:
    """Give a decommissioned cluster's segment back to the pool — returns
    immediately (202).

    Call it only AFTER the cluster's Argo CD Application and everything it
    managed are gone: the run deletes the cluster's DHCP scope if it is still
    there, and a Crossplane Request that still exists would re-create it. The
    hostedcluster-setup chart's PostDelete hook calls this route at exactly
    that point (a 409 means a run for the cluster is already going, which the
    hook treats as success).

    The body names the CLUSTER and the TYPE (default HC); the segment is
    looked up in the Segments Manager. A cluster holding no such segment
    COMPLETES as a no-op (released=false) rather than failing, so a repeated
    trigger is harmless. Poll GET /workflows/runs/{workflow_id} for
    progress/result.

    This API has no auth of its own (CLAUDE.md §9), and this route ends in a
    DHCP scope DELETE and a segment release.
    """
    try:
        handle = await client.start_workflow(
            ReleaseSegmentWorkflow.run,
            ReleaseSegmentRunArgs(input=release_input),
            id=_release_segment_workflow_id(release_input),
            task_queue=RELEASE_SEGMENT_WORKFLOW_QUEUE,
        )
    except WorkflowAlreadyStartedError:
        raise HTTPException(
            status_code=409,
            detail=(
                "Segment-lifecycle workflow already running: "
                f"{_release_segment_workflow_id(release_input)}"
            ),
        )
    return StartWorkflowResponse(
        workflow_id=handle.id, run_id=handle.result_run_id or ""
    )
