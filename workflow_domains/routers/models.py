"""API-layer models shared by EVERY domain router — never cross the workflow boundary.

A domain router owns the models that describe its own workflows' inputs and
outcomes (those carry the WORKFLOW name, e.g. BulkOpenSegmentRulesInput). What
lands here is the opposite: shapes with nothing domain- or workflow-specific
left in them, so a second domain reuses them instead of redeclaring an
identical model under its own name.
"""

from __future__ import annotations

from pydantic import BaseModel


class StartWorkflowResponse(BaseModel):
    """202 answer to any "start one workflow" route: which run to poll.

    Deliberately domain-agnostic, exactly like GET /workflows/runs/{workflow_id}
    — the caller takes workflow_id straight from here to that status endpoint.
    """

    workflow_id: str
    run_id: str
