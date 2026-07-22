"""Segment-connectivity activity signatures — the typed contract, no implementations.

The real implementations live in activities/segment_connectivity/activities.py and are
registered against these names on the segment-connectivity activity queue. Workflows
import THESE for type-checked activity references.
"""

from __future__ import annotations

from temporalio import activity

from shared.models.segment_connectivity import (
    BmcOpenRulesRequest,
    SegmentConnectivityFailureNotice,
    SegmentConnectivityInput,
    SegmentConnectivityRequestRef,
    SegmentConnectivityRequestsUpdate,
    OpenRulesRequest,
    PeerSegmentsQuery,
    SegmentRef,
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
async def list_peer_segments(query: PeerSegmentsQuery) -> list[SegmentRef]:
    """Return every same-site segment eligible to peer with query.source_type.

    Peer types are derived from the activity layer's configured
    PORTS_<SRC>_TO_<DST> port profiles — the port policy IS the peering
    topology (e.g. an HC source currently returns only MCE peers, while an
    MCE source returns HC + INVENTORY + PXE peers). Site-scoped: only
    same-site segments are valid peers.
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
async def get_bmc_segment(site: str) -> str:
    """Return the site's static BMC CIDR from ConfigMap (BMC_SEGMENTS_BY_SITE).

    BMC is not a Segments-Manager-tracked segment type, so this is a pure
    config lookup, not an API call. Raises BmcSegmentNotConfiguredError if the
    site has no configured entry — deterministic, non-retryable.
    """
    ...


@activity.defn
async def submit_bmc_open_rules(request: BmcOpenRulesRequest) -> SegmentConnectivityRequestRef:
    """Submit the one-directional MCE -> BMC open-rules request
    (PORTS_MCE_TO_BMC). Idempotent in the same sense as submit_open_rules."""
    ...


@activity.defn
async def publish_segment_connectivity_failure(notice: SegmentConnectivityFailureNotice) -> None:
    """Best-effort terminal-failure surface: clear the pending request-ids
    display, then publish a "segment-connectivity workflow failed" note beside the
    segment's status badge (the Segments Manager's segment-connectivity-failure
    endpoint — the workflow swallows this activity's errors either way)."""
    ...
