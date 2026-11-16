"""Segment-lifecycle activity signatures — the typed contract, no implementations.

The real implementations live in activities/segment_lifecycle/activities.py and are
registered against these names on the segment-lifecycle activity queue. Workflows
import THESE for type-checked activity references.
"""

from __future__ import annotations

from temporalio import activity

from shared.models.segment_lifecycle import (
    ClusterAllocationLookup,
    ClusterAllocationLookupRequest,
    DhcpScopeState,
    InitializeSegmentInput,
    SegmentAllocation,
    SegmentAllocationRequest,
    SegmentEntry,
)


@activity.defn
async def create_segment(rules_input: InitializeSegmentInput) -> None:
    """Create the segment in the Segments Manager (POST /api/segments).

    The whole of initialize-segment, and the reason that workflow exists: the
    segment is created here, Available immediately and with no type (the type
    is stamped on at allocation), with the creation recorded in a durable
    Temporal run instead of a caller's fire-and-forget call.

    Idempotent by check-after-conflict: a create rejected because the CIDR
    already exists is looked up and, if the stored segment matches this
    definition, treated as success — which covers both a Temporal retry of an
    accepted-but-unacknowledged POST and an operator re-running the workflow
    for a segment that already exists.

    Raises SegmentValidationError (definition rejected) or SegmentConflictError
    (CIDR exists with different attributes) — both deterministic, marked
    non-retryable by the workflow so a bad input fails fast instead of
    retrying forever.
    """
    ...


# --- allocate-segment -------------------------------------------------------


@activity.defn
async def get_valid_sites() -> list[str]:
    """Return the Segments Manager's configured site list (GET /api/sites).

    The workflow cross-checks the site it was given (provision-cluster reads
    it from the day1 folder the cluster file goes in) against this list, so a
    folder or spelling mismatch fails loudly as UnknownSite before anything is
    allocated.
    """
    ...


@activity.defn
async def allocate_segment(request: SegmentAllocationRequest) -> SegmentAllocation:
    """Reserve a segment in the Segments Manager (POST /api/segments/allocate).

    Any Available segment at the site qualifies; the manager stamps
    request.type onto the one it reserves. Idempotent server-side per
    (cluster, site, type): a repeat call — a
    Temporal retry, or a re-run — returns the existing allocation rather than
    reserving a second segment. Raises SegmentPoolExhaustedError (503, no
    Available segment at the site) or SegmentValidationError
    (400/422, e.g. a bad cluster name) — both non-retryable.
    """
    ...


@activity.defn
async def get_segment(segment: str) -> SegmentEntry:
    """Read one segment back from the Segments Manager
    (GET /api/segments/by-segment) — the verification step's read-back, used
    by allocate-segment after allocating and by release-segment before and
    after releasing.

    Raises SegmentNotFoundError on 404 (the segment the manager just reported
    is gone — deterministic, non-retryable). Read-only, so trivially
    idempotent.
    """
    ...


# --- shared with the server-lifecycle domain --------------------------------


@activity.defn
async def get_inventory_segment(mce_cluster: str) -> SegmentEntry:
    """Find the INVENTORY segment allocated to one MCE cluster (GET /api/segments).

    Lives in THIS domain, not server-lifecycle, because it reads the Segments
    Manager — whose base URL and token already sit on the segment-lifecycle
    limb. add-server-to-mce calls it across domains with
    task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE rather than a second
    deployment holding a copy of that credential.

    Each MCE owns ONE inventory network, so its VLAN is a property of the
    cluster — not of the server being installed, and not of the caller's
    request. The cluster NAME is therefore the whole lookup. (It briefly took a
    BMC protocol class as well, while INVENTORY was split into
    INVENTORY_REDFISH and INVENTORY_IPMI; that split is reverted on both sides.)

    The Segments Manager's list endpoint filters by `type` but has no
    `cluster_name` parameter (verified against its OpenAPI: site, status, type,
    fresh), so the cluster match is made client-side over the returned list.
    The type is re-checked there too — a filter the server ignores must not
    silently yield some other cluster's segment. Exactly one match is required.

    Raises InventorySegmentNotFoundError when the MCE has no INVENTORY segment
    and AmbiguousInventorySegmentError when several do; both deterministic and
    non-retryable.
    """
    ...


# --- release-segment --------------------------------------------------------


@activity.defn
async def find_cluster_allocation(
    request: ClusterAllocationLookupRequest,
) -> ClusterAllocationLookup:
    """Find the segment the Segments Manager holds for (request.cluster,
    request.type) — GET /api/segments?status=Allocated&type=<T>&fresh=true,
    matched on the EXACT cluster_name here.

    The Segments Manager has no cluster filter on that route (a
    `cluster_name` query parameter is silently ignored and returns every
    segment), and its search is a substring match, so the exact match has to
    be ours. Observe-only and idempotent: no allocation is found=False, a
    normal answer. More than one raises AmbiguousAllocationError
    (non-retryable).
    """
    ...


@activity.defn
async def release_segment(segment: str) -> None:
    """Release the segment (POST /api/segments/release {"segment": <CIDR>}):
    status back to Available, cluster_name and type cleared.

    Idempotent server-side: an already-Available segment answers 200 "already
    released". The Segments Manager does NOT check who holds the segment, so
    the workflow re-reads its owner first. Raises SegmentNotFoundError (404)
    or SegmentValidationError (400/422) — both non-retryable; a 500 (a lost
    release race) is retried and lands on "already released".
    """
    ...


@activity.defn
async def get_dhcp_scope(network: str) -> DhcpScopeState:
    """Observe the DHCP scope for a network address (GET
    /api/v1/scopes/{network}, anonymous; `network` is the mask-stripped
    address, e.g. 10.20.90.0).

    404 is NOT an error — it returns found=False. Raises
    DhcpScopeInvalidError (400, non-retryable); any outage is the retryable
    DhcpApiError. Read-only, so trivially idempotent.
    """
    ...


@activity.defn
async def delete_dhcp_scope(network: str) -> None:
    """Delete the DHCP scope for a network address (DELETE
    /api/v1/scopes/{network}, bearer DHCP_API_TOKEN).

    Idempotent server-side: the API answers 204 whether or not the scope
    existed. Raises DhcpApiAuthError (401/403) or DhcpScopeInvalidError (400)
    — both non-retryable; any outage is the retryable DhcpApiError. The
    workflow verifies the deletion with get_dhcp_scope afterwards.
    """
    ...
