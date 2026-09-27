"""Typed state for the segment-lifecycle workflow — the contract between brain and limbs.

Everything that crosses the workflow/activity boundary is a Pydantic model (or a
list of primitives), never an untyped dict.
"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class SegmentType(str, Enum):
    """Segment types known to the Segments Manager.

    A type is ALLOCATION state, not part of a segment's definition: a segment
    is created with none, the Segments Manager stamps the requested type on
    when allocate-segment reserves it, and release clears it again. So the type
    appears only on the allocation path (AllocateSegmentInput and what follows
    from it), never on initialize-segment's input. (It used to be fixed at
    creation, which is why there was once a convert-segment workflow to re-type
    Available segments between pools. There are no per-type pools any more, so
    there is nothing to convert.)
    """

    MCE = "MCE"
    HC = "HC"
    # ONE inventory type, keyed by CLUSTER NAME. It was briefly split by how a
    # server's BMC is driven — INVENTORY_REDFISH and INVENTORY_IPMI — because
    # Ironic reaches a Redfish BMC on one network and a UCS-managed blade over
    # IPMI on another. Reverted with the Segments Manager: the team keeps one
    # inventory scope per cluster, so an MCE's inventory VLAN is a property of
    # the CLUSTER alone and install-server resolves it once per run.
    INVENTORY = "INVENTORY"
    PXE = "PXE"


class InitializeSegmentInput(BaseModel):
    """Input to InitializeSegmentWorkflow: the segment to CREATE in the
    Segments Manager.

    This workflow is the single entry point for bringing a segment into
    existence — the Segments Manager is a dependency the workflow calls
    (create_segment), never the trigger. So the input is the full segment
    definition, not a reference to an existing one, and creation is visible in
    one Temporal run instead of happening outside it.

    Field names and constraints mirror the Segments Manager's own Segment
    schema (POST /api/segments, extra="forbid"): the shapes must match for the
    create_segment activity to post this straight through. Semantic validation
    (site is configured, CIDR matches the site prefix, no overlap, VLAN free)
    stays the Segments Manager's job — it is the validator of record, and
    re-deriving its rules here would only let the two drift.

    There is no `type`: a segment is born without one and gets it when it is
    allocated (see SegmentType). The manager rejects a type on create.
    """

    segment: str = Field(min_length=1)  # CIDR, e.g. "130.154.20.0/24"
    site: str = Field(min_length=1)
    vlan_id: int = Field(ge=1, le=4094)
    epg_name: str = Field(min_length=1)
    dhcp: bool = True


class InitializeSegmentRunArgs(BaseModel):
    """The workflow's single argument.

    A single-model argument is the Temporal-recommended shape. It also avoids
    an SDK gotcha: typed payload conversion is silently SKIPPED whenever the
    number of payloads differs from the number of declared run() parameters —
    a two-parameter run(input, resume=None) started with one payload receives
    a raw dict instead of a Pydantic model. The wrapper stays even though
    `input` is now its only field: it is the shape the run() signature is
    pinned to, and the next piece of internal state has somewhere to go.
    """

    input: InitializeSegmentInput


class InitializeSegmentProgress(BaseModel):
    """Returned by the workflow's `progress` query (surfaced by the status API)."""

    phase: str


class InitializeSegmentResult(BaseModel):
    """The run's outcome: the segment that now exists.

    A completed run means the segment is in the Segments Manager and
    Available — there is no second thing to report."""

    segment: str


# --- allocate-segment -------------------------------------------------------
# The domain's second workflow: allocate a VLAN segment for a hosted cluster
# and write the dhcp_values block into its values-repo file. Workflow-scoped
# models carry the workflow name; models any sibling could reuse (the
# allocation request/response, the segment read-back, the DHCP shapes) do not.


class AllocateSegmentInput(BaseModel):
    """Input to AllocateSegmentWorkflow: which cluster to allocate for, as
    which type, and on which branch of the values repo to record it.

    Deliberately tiny — the site is DERIVED from where the cluster's values
    file sits in the repo (sites/<site>/...), cross-checked against the
    Segments Manager's site list, so a caller can never claim a site the
    cluster does not live in. `type` is the ONE place a segment's type enters
    the system: Available segments have none, and the Segments Manager stamps
    this one onto whichever segment it reserves. It defaults to HC, the only
    supported type; anything else is rejected up front with
    UnsupportedSegmentType.

    `values_branch` is a PER-RUN input, never config: the day1 pipeline that
    triggers this workflow runs on a temporary branch, and a new cluster's
    file exists ONLY there until a human merges it. The block is pushed to
    that branch and the run ends there. It defaults to None solely so history
    recorded before the field existed still decodes (CLAUDE.md §5): the
    router's AllocateSegmentRequest makes it required, and the workflow fails
    ValuesBranchMissing if one ever arrives without it.
    """

    cluster: str = Field(min_length=1)
    type: SegmentType = SegmentType.HC
    values_branch: str | None = Field(default=None, min_length=1)


class AllocateSegmentRunArgs(BaseModel):
    """The workflow's single argument (same single-model rule as
    InitializeSegmentRunArgs — typed conversion is silently skipped when
    payload count differs from the declared parameter count)."""

    input: AllocateSegmentInput


class AllocateSegmentProgress(BaseModel):
    """Returned by the workflow's `progress` query (surfaced by the status API)."""

    phase: str


class ClusterFileLookupRequest(BaseModel):
    """Input to locate_cluster_file: which cluster, on which values-repo
    branch. It replaced a bare `cluster: str` argument because the branch has
    to travel with the name — a new cluster's file exists only on the
    pipeline branch that created it, so locating it on any other branch finds
    nothing.

    `values_branch` defaults to None only so a payload recorded before the
    field existed still decodes; the activity refuses None as
    ValuesBranchMissing (non-retryable) rather than guess a branch."""

    cluster: str = Field(min_length=1)
    values_branch: str | None = None


class ClusterFileLocation(BaseModel):
    """Where the cluster's values file lives in the values repo. The site is
    the path segment directly beneath the clusters root — the repo layout
    (sites/<site>/mces/<mce>/hostedClusters/<cluster>.yaml) is the source of
    truth for which site a cluster belongs to."""

    site: str = Field(min_length=1)
    relative_path: str = Field(min_length=1)


class SegmentAllocationRequest(BaseModel):
    """Input to allocate_segment: who is asking, where, and as what type.
    The type does not narrow the pool (Available segments carry none) — the
    manager stamps it onto the segment it reserves. Scoped by (cluster, site,
    type) exactly like the Segments Manager's idempotency, so a Temporal retry
    can never double-allocate."""

    cluster: str = Field(min_length=1)
    site: str = Field(min_length=1)
    type: SegmentType


class SegmentAllocation(BaseModel):
    """The Segments Manager's answer to an allocation: the reserved segment."""

    vlan_id: int
    segment: str = Field(min_length=1)  # CIDR
    epg_name: str


class SegmentEntry(BaseModel):
    """A segment as read back from the Segments Manager (GET
    /api/segments/by-segment) — what the verification step compares against
    the allocation it was just handed.

    `type` is None on an Available segment and set on an Allocated one — the
    allocation wrote it, so the read-back checks it like cluster_name."""

    segment: str = Field(min_length=1)
    site: str
    vlan_id: int
    status: str
    type: str | None = None
    cluster_name: str | None = None


class DhcpExclusion(BaseModel):
    """One excluded address range inside the DHCP scope."""

    start_address: str
    end_address: str


class DhcpValues(BaseModel):
    """The dhcp_values block written to the cluster's values file, derived
    from the allocated segment + that type's DHCP_EXCLUSION_OCTET_RANGES
    policy. `network` is the mask-stripped network address (10.20.90.0, never
    10.20.90.0/24) — the DHCP scope's identity.

    No startRange/endRange: dhcp_scope_manager derives .1-.253 when both are
    absent, and the exclusions carve the ends back out of it. `exclusions` is
    empty for a type whose policy defines none — a legitimate block, not a
    missing one."""

    network: str = Field(min_length=1)
    exclusions: list[DhcpExclusion] = Field(default_factory=list)


class ClusterValuesAppendRequest(BaseModel):
    """Input to append_allocation_to_cluster_values: which file to append to
    and what was allocated. The dhcp_values block itself is derived
    activity-side (the DHCP policy lives in the activity worker's config,
    which the sandboxed workflow cannot read).

    `type` travels with the request because the exclusion policy is PER TYPE:
    the activity selects that type's ranges out of DHCP_EXCLUSION_OCTET_RANGES.
    It is the workflow's typed input, not something re-derived from the segment
    read-back, so the file records the policy for the type actually allocated.

    `values_branch` is the branch the block is pushed to — the one the
    workflow was started for. It defaults to None only so a payload recorded
    before the field existed still decodes; the activity refuses None as
    ValuesBranchMissing (non-retryable) instead of pushing anywhere else."""

    cluster: str = Field(min_length=1)
    relative_path: str = Field(min_length=1)
    vlan_id: int
    segment: str = Field(min_length=1)  # CIDR
    type: SegmentType
    values_branch: str | None = None


class ValuesCommitRef(BaseModel):
    """Outcome of the values-repo append. `changed=False` (commit_sha None)
    means the file already carried this exact allocation — a re-run — and
    nothing was pushed. `dhcp_values` is returned in BOTH cases so the run can
    report what the file records. (It used to feed the DHCP convergence poll,
    since removed — CLAUDE.md §4.)"""

    commit_sha: str | None
    changed: bool
    dhcp_values: DhcpValues


class AllocateSegmentResult(BaseModel):
    """What the run did. `values_branch` is where the block landed — or, on a
    no-op re-run, the branch that already carried it. The DHCP scope is NOT
    part of this result: it appears only after a human merges that branch to
    main (Argo CD reads main only), outside this run."""

    cluster: str
    site: str
    type: SegmentType
    vlan_id: int
    segment: str
    epg_name: str
    values_branch: str
    commit_sha: str | None
    values_updated: bool
