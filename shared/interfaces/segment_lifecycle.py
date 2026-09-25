"""Segment-lifecycle activity signatures — the typed contract, no implementations.

The real implementations live in activities/segment_lifecycle/activities.py and are
registered against these names on the segment-lifecycle activity queue. Workflows
import THESE for type-checked activity references.
"""

from __future__ import annotations

from temporalio import activity

from shared.models.segment_lifecycle import (
    ClusterFileLocation,
    ClusterFileLookupRequest,
    ClusterValuesAppendRequest,
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
async def locate_cluster_file(request: ClusterFileLookupRequest) -> ClusterFileLocation:
    """Find the cluster's values file in the day1 values repo, on
    request.values_branch.

    Checks the branch exists (git ls-remote), then shallow-clones THAT branch
    — a new cluster's file exists only on the pipeline branch that created it
    — and requires EXACTLY ONE <clusters root>/<site>/**/<cluster>.yaml. A
    missing branch raises ValuesBranchNotFoundError; zero files
    ClusterFileNotFoundError; more than one AmbiguousClusterFileError; a
    request without a branch ApplicationError type ValuesBranchMissing — all
    deterministic and non-retryable. The site is the path segment directly
    beneath the clusters root. Read-only, so trivially idempotent.
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
    file and push it to request.values_branch.

    The commit message carries `[skip ci]`: the push lands on the day1
    pipeline's own branch, and without the marker it would start a SECOND
    pipeline that re-runs the allocator.

    Idempotent by re-clone + allocation check: a marker block already recording
    this vlanId on this network is a no-op success (changed=False, nothing
    pushed) whatever else an operator has tuned inside it; a DIFFERENT
    allocation — another vlan or network, an unreadable marker block, or an
    unmarked dhcp_values/vlanId key — raises ClusterValuesConflictError
    (non-retryable). A missing branch raises ValuesBranchNotFoundError and a
    request without one ApplicationError type ValuesBranchMissing (both
    non-retryable). A rejected push raises the retryable ValuesRepoGitError;
    the retry starts from a fresh clone and converges. Returns the derived
    DhcpValues either way, so the run can report what the file records.
    """
    ...


# --- shared with the server-lifecycle domain --------------------------------


@activity.defn
async def get_inventory_segment(mce_cluster: str) -> SegmentEntry:
    """Find the INVENTORY segment allocated to one MCE cluster (GET /api/segments).

    Lives in THIS domain, not server-lifecycle, because it reads the Segments
    Manager — whose base URL and token already sit on the segment-lifecycle
    limb. install-server calls it with
    task_queue=SEGMENT_LIFECYCLE_ACTIVITY_QUEUE rather than a second
    deployment holding a copy of that credential.

    Each MCE owns one inventory network, so its VLAN is a property of the
    cluster, not of the server being installed or of the caller's request.

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
