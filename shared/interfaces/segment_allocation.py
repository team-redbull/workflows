"""Activity signatures (the contract) for segment allocation.

These are @activity.defn declarations with NO implementation body. The concrete
implementations live in activities/segment_allocation/segment_tasks.py and are
registered against these names. Workflows import these signatures so that
execute_activity is type-checked against the exact contract — never a string.

This module must stay lightweight: temporalio + the shared models only. No httpx,
no kubernetes, no boto3.
"""

from __future__ import annotations

from temporalio import activity

from shared.models.segment_allocation import SegmentAllocationInput, SegmentAllocationResult, SegmentSpec


@activity.defn
async def get_available_segment(site: str) -> SegmentSpec | None:
    """Return an unallocated segment at the site, or None if the pool is empty."""
    ...


@activity.defn
async def request_segment(site: str) -> SegmentSpec:
    """Mint a new valid segment for the site via the external generator (IPAM)."""
    ...


@activity.defn
async def register_segment(spec: SegmentSpec) -> None:
    """Register a segment in the Segments Manager. Idempotent on duplicate VLAN."""
    ...


@activity.defn
async def allocate_segment(allocation_input: SegmentAllocationInput) -> SegmentAllocationResult:
    """Assign a segment at the site to the cluster. Idempotent per (cluster, site)."""
    ...
