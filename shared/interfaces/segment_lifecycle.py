"""Segment-lifecycle activity signatures — the typed contract, no implementations.

The real implementations live in activities/segment_lifecycle/activities.py and are
registered against these names on the segment-lifecycle activity queue. Workflows
import THESE for type-checked activity references.
"""

from __future__ import annotations

from temporalio import activity

from shared.models.segment_lifecycle import (
    BmcOpenRulesRequest,
    SegmentConnectivityFailureNotice,
    OpenSegmentRulesInput,
    NextRequestRef,
    SegmentConnectivityRequestsUpdate,
    OpenRulesRequest,
    PeerSegmentsQuery,
    SegmentRef,
)


@activity.defn
async def create_segment(rules_input: OpenSegmentRulesInput) -> None:
    """Create the segment in the Segments Manager (POST /api/segments).

    Step 1 of the workflow, and the reason the workflow owns the whole
    lifecycle: the segment is born Locked here and unlocked by the last step,
    with every firewall request in between recorded in the same Temporal run.

    Idempotent by check-after-conflict: a create rejected because the CIDR
    already exists is looked up and, if the stored segment matches this
    definition, treated as success — which covers both a Temporal retry of an
    accepted-but-unacknowledged POST and an operator re-running connectivity
    for a segment that already exists.

    Raises SegmentValidationError (definition rejected) or SegmentConflictError
    (CIDR exists with different attributes) — both deterministic, marked
    non-retryable by the workflow so a bad input fails fast instead of
    retrying forever.
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
async def submit_open_rules(request: OpenRulesRequest) -> NextRequestRef:
    """Submit one open-firewall-rules request to the next API.

    Idempotent in effect: a retried submission opens identical rules, which
    converge to the same firewall state (worst case an orphan request id).
    """
    ...


@activity.defn
async def check_next_requests(request_ids: list[int]) -> list[int]:
    """Batch-check next request statuses; return the ids STILL PENDING.

    Named after NEXT, not after the workflow that submitted them: it takes bare
    request ids, so every workflow in this domain polls its own next requests
    through this one activity.
    """
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
    """Return the site's static BMC CIDR from ConfigMap (SITE_NETWORKS).

    BMC is not a Segments-Manager-tracked segment type, so this is a pure
    config lookup, not an API call. Raises BmcSegmentNotConfiguredError if the
    site has no configured entry — deterministic, non-retryable.
    """
    ...


@activity.defn
async def submit_bmc_open_rules(request: BmcOpenRulesRequest) -> NextRequestRef:
    """Submit the one-directional MCE -> BMC open-rules request
    (PORTS_MCE_TO_BMC). Idempotent in the same sense as submit_open_rules."""
    ...


@activity.defn
async def publish_segment_connectivity_failure(notice: SegmentConnectivityFailureNotice) -> None:
    """Best-effort terminal-failure surface: clear the pending request-ids
    display, then publish a "<workflow> failed" note beside the
    segment's status badge (the Segments Manager's segment-connectivity-failure
    endpoint — the workflow swallows this activity's errors either way)."""
    ...
