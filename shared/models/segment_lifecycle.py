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
    # the CLUSTER alone and add-server-to-mce resolves it once per run.
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
# and verify it. Workflow-scoped models carry the workflow name; models any
# sibling could reuse (the allocation request/response, the segment
# read-back, the DHCP shapes) do not.


class AllocateSegmentInput(BaseModel):
    """Input to AllocateSegmentWorkflow: which cluster to allocate for, at
    which site, as which type.

    `site` is the caller's: provision-cluster reads it from the day1 folder
    the cluster file goes in (`sites/<site>/mces/<mce>/`) and passes it on, and
    the run cross-checks it against the Segments Manager's site list. It
    defaults to None solely so history recorded before the field existed
    still decodes (CLAUDE.md §5): the router's AllocateSegmentRequest makes it
    required, and the workflow fails SiteMissingError if a run ever arrives
    without one. `type` is the ONE place a segment's type enters the system:
    Available segments have none, and the Segments Manager stamps this one
    onto whichever segment it reserves. It defaults to HC, the only supported
    type; anything else is rejected up front with UnsupportedSegmentType.

    `values_branch` is RETIRED (2026-11-11): the run used to locate the
    cluster file on that branch and push the allocation block to it; since
    provision-cluster owns the cluster file, allocate-segment allocates and
    verifies only. The field stays, ignored, so history recorded before still
    decodes; the router refuses it with a 422 that says so.
    """

    cluster: str = Field(min_length=1)
    type: SegmentType = SegmentType.HC
    site: str | None = Field(default=None, min_length=1)
    values_branch: str | None = Field(default=None, min_length=1)


class AllocateSegmentRunArgs(BaseModel):
    """The workflow's single argument (same single-model rule as
    InitializeSegmentRunArgs — typed conversion is silently skipped when
    payload count differs from the declared parameter count)."""

    input: AllocateSegmentInput


class AllocateSegmentProgress(BaseModel):
    """Returned by the workflow's `progress` query (surfaced by the status API)."""

    phase: str


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
    allocation wrote it, so the read-back checks it like cluster_name. Release
    clears both again, so release-segment's read-back expects None for each."""

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
    """The dhcp_values block written to the cluster's file, derived from the
    allocated segment + that type's DHCP_EXCLUSION_OCTET_RANGES policy (by the
    cluster-lifecycle limb, which writes the file). `network` is the
    mask-stripped network address (10.20.90.0, never 10.20.90.0/24) — the DHCP
    scope's identity.

    No startRange/endRange: dhcp_scope_manager derives .1-.253 when both are
    absent, and the exclusions carve the ends back out of it. `exclusions` is
    empty for a type whose policy defines none — a legitimate block, not a
    missing one."""

    network: str = Field(min_length=1)
    exclusions: list[DhcpExclusion] = Field(default_factory=list)


class AllocateSegmentResult(BaseModel):
    """What the run did: the segment the Segments Manager holds for the cluster,
    verified by read-back. Nothing about git: provision-cluster records the
    allocation in the cluster file it publishes (the `values_branch`,
    `commit_sha` and `values_updated` fields went with that, 2026-11-11; a
    result recorded before still decodes, extras ignored). The DHCP scope is
    NOT part of this result either: it appears only after a human merges the
    cluster file to main (Argo CD reads main only), outside this run."""

    cluster: str
    site: str
    type: SegmentType
    vlan_id: int
    segment: str
    epg_name: str


# --- release-segment --------------------------------------------------------
# The domain's third workflow: give a decommissioned cluster's segment back to
# the pool — delete its DHCP scope if one survived the Argo CD cascade, then
# release it in the Segments Manager. Workflow-scoped models carry the
# workflow name; the lookup and DHCP shapes do not (a future deprovision- or
# create-cluster workflow reuses them).


class ReleaseSegmentInput(BaseModel):
    """Input to ReleaseSegmentWorkflow: which cluster's segment to give back,
    of which type.

    Keyed by CLUSTER, symmetric with allocate-segment: the caller (today the
    hostedcluster-setup chart's PostDelete hook, later a deprovision-cluster
    workflow) knows the cluster it just tore down, never the CIDR. The segment
    is looked up in the Segments Manager. `type` scopes the lookup — a cluster
    holds at most one segment per type — and defaults to HC, the only type
    allocate-segment allocates; anything else is rejected up front with
    UnsupportedSegmentType.
    """

    cluster: str = Field(min_length=1)
    type: SegmentType = SegmentType.HC


class ReleaseSegmentRunArgs(BaseModel):
    """The workflow's single argument (same single-model rule as
    InitializeSegmentRunArgs)."""

    input: ReleaseSegmentInput


class ReleaseSegmentProgress(BaseModel):
    """Returned by the workflow's `progress` query (surfaced by the status API)."""

    phase: str


class ReleaseSegmentResult(BaseModel):
    """What the run did.

    `released=False` with segment/vlan_id/site None means the Segments
    Manager held NO allocation of this type for the cluster — already
    released, or never allocated. That is a completed no-op, not a failure, so
    a late or repeated trigger stays green.

    `dhcp_scope_removed=True` means the safety net fired: the scope was still
    there when the run looked, and the run deleted it. Normally False — the
    Argo CD cascade (Application -> Request CR -> provider-http DELETE) has
    already removed it by the time the run starts."""

    cluster: str
    type: SegmentType
    released: bool
    segment: str | None
    vlan_id: int | None
    site: str | None
    dhcp_scope_removed: bool


class ClusterAllocationLookupRequest(BaseModel):
    """Input to find_cluster_allocation: whose allocation, of which type."""

    cluster: str = Field(min_length=1)
    type: SegmentType


class ClusterAllocationLookup(BaseModel):
    """The Segments Manager's allocation for one (cluster, type), if any.
    `found=False` (entry None) is a normal answer — nothing to release — never
    an error."""

    found: bool
    entry: SegmentEntry | None = None


class DhcpScopeState(BaseModel):
    """A read-only observation of the DHCP scope API: does a scope exist for
    this network, and with which exclusions? `found=False` (a 404) is a normal
    answer, never an error.

    Release-segment reads only `found`. The exclusions are kept because they
    are what a scope check compares against what was written to git — the
    shape a future create-cluster convergence check needs (CLAUDE.md §4)."""

    found: bool
    exclusions: list[DhcpExclusion] = Field(default_factory=list)
