from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class SegmentAllocationInput(BaseModel):
    """Input to the SegmentAllocationWorkflow."""

    cluster_name: str = Field(min_length=1)
    site: str = Field(min_length=1)


class DeploymentStatus(BaseModel):
    """Structural view of a deployment resource as returned by the deployments API.

    We trust the external API's payload (black box) and only parse the fields we
    act on: the lifecycle `status` and, once created, the allocated `segment`
    carried under `additionalInfo.segment`.
    """

    status: str = Field(min_length=1)
    segment: str | None = None


class SegmentAllocationResult(BaseModel):
    """The outcome of allocating a segment to a cluster."""

    cluster_name: str
    site: str
    uuid: str
    segment: str
    allocated_at: datetime
