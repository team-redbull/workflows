"""Connectivity activity signatures — the typed contract, no implementations.

The real implementations live in activities/connectivity/activities.py and are
registered against these names on the connectivity activity queue. Workflows
import THESE for type-checked activity references.
"""

from __future__ import annotations

from temporalio import activity

from shared.models.connectivity import (
    ConnectivityInput,
    ConnectivityRequestRef,
    ConnectivityRequestsUpdate,
    OpenRulesRequest,
)


@activity.defn
async def validate_segment_exists(connectivity_input: ConnectivityInput) -> None:
    """Assert the input segment exists in the Segments Manager (by type + CIDR).

    Raises SegmentNotFoundError if absent — deterministic, marked non-retryable
    by the workflow so a bad input fails fast instead of retrying.
    """
    ...


@activity.defn
async def list_mce_segments(connectivity_input: ConnectivityInput) -> list[str]:
    """Return the CIDRs of MCE-type segments in the same site as the input segment.

    MCE-only by design: every supported segment type currently peers with the
    MCE segments. Site-scoped: only MCE segments co-located with the input
    segment are valid peers.
    """
    ...


@activity.defn
async def submit_open_rules(request: OpenRulesRequest) -> ConnectivityRequestRef:
    """Submit one open-firewall-rules request to the next API.

    Idempotent in effect: a retried submission opens identical rules, which
    converge to the same firewall state (worst case an orphan request id).
    """
    ...


@activity.defn
async def check_connectivity_requests(request_ids: list[int]) -> list[int]:
    """Batch-check next request statuses; return the ids STILL PENDING."""
    ...


@activity.defn
async def publish_request_ids(update: ConnectivityRequestsUpdate) -> None:
    """Replace the pending request ids shown beside the segment's status in the
    Segments Manager UI. An empty list removes the display. Idempotent (PUT
    semantics: re-sending the same ids is a no-op).
    """
    ...


@activity.defn
async def unlock_segment(segment: str) -> None:
    """Flip the segment's status Locked -> Available in the Segments Manager.

    Identified by CIDR (POST /api/segments/unlock). Idempotent: an
    already-unlocked segment is treated as success.
    """
    ...
