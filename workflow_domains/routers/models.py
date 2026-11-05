"""API-layer models shared by EVERY domain router — never cross the workflow boundary.

A domain router owns the models that describe its own workflows' inputs and
outcomes (those carry the WORKFLOW name, e.g. BulkInitializeSegmentInput). What
lands here is the opposite: shapes with nothing domain- or workflow-specific
left in them, so a second domain reuses them instead of redeclaring an
identical model under its own name.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class StartWorkflowResponse(BaseModel):
    """202 answer to any "start one workflow" route: which run to poll.

    Deliberately domain-agnostic, exactly like GET /workflows/runs/{workflow_id}
    — the caller takes workflow_id straight from here to that status endpoint.
    """

    workflow_id: str
    run_id: str


class SlotStart(BaseModel):
    """One slot a pool request looked at, for a per-run report (workflow_domains/routers/slots.py).

    `started`: a new run on `workflow_id` (`run_id` set); `already_running`: the
    slot's run is still going and was stepped over; `failed`: Temporal refused
    the start (`error`), which ends the scan.
    """

    slot: int
    workflow_id: str
    status: Literal["started", "already_running", "failed"]
    run_id: str | None = None
    error: str | None = None
