"""Segment-lifecycle activity signatures — the typed contract, no implementations.

The real implementations live in activities/segment_lifecycle/activities.py and are
registered against these names on the segment-lifecycle activity queue. Workflows
import THESE for type-checked activity references.
"""

from __future__ import annotations

from temporalio import activity

from shared.models.segment_lifecycle import (
    ClusterFileLocation,
    ClusterValuesAppendRequest,
    DhcpScopeState,
    InitializeSegmentInput,
    SegmentAllocation,
    SegmentAllocationRequest,
    SegmentEntry,
    ValuesCommitRef,
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

    The workflow cross-checks the site DERIVED from the values-repo path
    against this list, so a repo layout mistake fails loudly as UnknownSite
    before anything is allocated.
    """
    ...


@activity.defn
async def locate_cluster_file(cluster: str) -> ClusterFileLocation:
    """Find the cluster's values file in the day1 values repo.

    Shallow-clones the repo and requires EXACTLY ONE
    <clusters root>/<site>/**/<cluster>.yaml. Zero raises
    ClusterFileNotFoundError, more than one AmbiguousClusterFileError — both
    deterministic and non-retryable. The site is the path segment directly
    beneath the clusters root.
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
    (GET /api/segments/by-segment) — the verification step's read-back.

    Raises SegmentNotFoundError on 404 (the allocation the manager just
    acknowledged is gone — deterministic, non-retryable).
    """
    ...


@activity.defn
async def append_allocation_to_cluster_values(
    request: ClusterValuesAppendRequest,
) -> ValuesCommitRef:
    """Append the marker block (vlanId + dhcp_values) to the cluster's values
    file and push to the values repo.

    Idempotent by re-clone + allocation check: a marker block already recording
    this vlanId on this network is a no-op success (changed=False, nothing
    pushed) whatever else an operator has tuned inside it; a DIFFERENT
    allocation — another vlan or network, an unreadable marker block, or an
    unmarked dhcp_values/vlanId key — raises ClusterValuesConflictError
    (non-retryable). A rejected push raises the
    retryable ValuesRepoGitError; the retry starts from a fresh clone and
    converges. Returns the derived DhcpValues either way, for the workflow's
    convergence poll.
    """
    ...


@activity.defn
async def get_dhcp_scope(network: str) -> DhcpScopeState:
    """Read-only observation of the DHCP API (GET /api/v1/scopes/{network}).

    404 is NOT an error — it returns found=False, the normal answer while
    Crossplane has not created the scope yet; the workflow's bounded timer
    loop owns the waiting. Only a failing/malformed API raises DhcpApiError
    (transient, retried). This activity never writes: git is the single source
    of truth and Crossplane the only writer, we merely observe convergence.
    """
    ...
