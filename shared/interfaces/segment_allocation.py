"""Activity signatures (the contract) for segment allocation.

These are @activity.defn declarations with NO implementation body. The concrete
implementations live in activities/segment_allocation/activities.py and are
registered against these names. Workflows import these signatures so that
execute_activity is type-checked against the exact contract — never a string.

This module must stay lightweight: temporalio + the shared models only. No httpx,
no kubernetes, no boto3.
"""

from __future__ import annotations

from temporalio import activity

from shared.models.segment_allocation import (
    DeploymentStatus,
    SegmentAllocationInput,
)


@activity.defn
async def create_deployment(allocation_input: SegmentAllocationInput) -> str:
    """Create a deployment via the deployments API and return its `uuid`."""
    ...


@activity.defn
async def get_deployment(uuid: str) -> DeploymentStatus:
    """Fetch a deployment's current status (and segment, once created)."""
    ...


@activity.defn
async def commit_segment_to_git(allocation_input: SegmentAllocationInput, segment: str) -> None:
    """Commit the allocated segment to the GitOps repository. Idempotent."""
    ...
