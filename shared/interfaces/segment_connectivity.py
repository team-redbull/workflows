"""Segment-connectivity activity signatures — the typed contract, no implementations.

The real implementations live in activities/segment_connectivity/activities.py and are
registered against these names on the segment-connectivity activity queue. Workflows
import THESE for type-checked activity references.
"""

from __future__ import annotations

from temporalio import activity

from shared.models.segment_connectivity import (
    SegmentConnectivityFailureNotice,
    SegmentConnectivityInput,
    SegmentConnectivityRequestRef,
    SegmentConnectivityRequestsUpdate,
    OpenRulesRequest,
)


@activity.defn
async def get_segment_site(connectivity_input: SegmentConnectivityInput) -> str:
    """Validate the input segment exists in the Segments Manager (by type +
    CIDR) and return its site — one fetch serves both needs.

    Raises SegmentNotFoundError if absent — deterministic, marked non-retryable
    by the workflow so a bad input fails fast instead of retrying.
    """
    ...


@activity.defn
async def list_mce_segments(site: str) -> list[str]:
    """Return the CIDRs of MCE-type segments in the given site.

    MCE-only by design: every supported segment type currently peers with the
    MCE segments. Site-scoped: only MCE segments co-located with the input
    segment are valid peers.
    """
    ...


@activity.defn
async def submit_open_rules(request: OpenRulesRequest) -> SegmentConnectivityRequestRef:
    """Submit one open-firewall-rules request to the next API.

    Idempotent in effect: a retried submission opens identical rules, which
    converge to the same firewall state (worst case an orphan request id).
    """
    ...


@activity.defn
async def check_segment_connectivity_requests(request_ids: list[int]) -> list[int]:
    """Batch-check next request statuses; return the ids STILL PENDING."""
    ...


@activity.defn
async def get_next_checking_request_interval() -> int:
    """Seconds the workflow should wait between polls of next request status
    (operator-configured; differs between local/dev and prod)."""
    ...


@activity.defn
async def publish_request_ids(update: SegmentConnectivityRequestsUpdate) -> None:
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


@activity.defn
async def publish_segment_connectivity_failure(notice: SegmentConnectivityFailureNotice) -> None:
    """Best-effort terminal-failure surface: clear the pending request-ids
    display, then publish a "segment-connectivity workflow failed" note beside the
    segment's status badge (the Segments Manager's segment-connectivity-failure
    endpoint — the workflow swallows this activity's errors either way)."""
    ...
