"""Typed state for the segment-lifecycle workflow — the contract between brain and limbs.

Everything that crosses the workflow/activity boundary is a Pydantic model (or a
list of primitives), never an untyped dict.
"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field, model_validator


class SegmentType(str, Enum):
    """Segment types known to the Segments Manager.

    Every one of them is valid input to every workflow in this domain — the
    Segments Manager is the validator of record for what a segment may be, and
    nothing in the orchestrator narrows that set. (There used to be a gate here:
    firewall rules were defined for HC/INVENTORY/MCE only, so a PXE input was
    rejected before creation. The firewalls are open now, so the gate is gone
    with the flow that needed it.)
    """

    MCE = "MCE"
    HC = "HC"
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
    """

    segment: str = Field(min_length=1)  # CIDR, e.g. "130.154.20.0/24"
    type: SegmentType
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
    """The run's outcome: the segment that now exists, and its type.

    A completed run means the segment is in the Segments Manager and
    Available — there is no second thing to report."""

    segment: str
    type: SegmentType


# --- allocate-segment -------------------------------------------------------
# The domain's second workflow: allocate a VLAN segment for a hosted cluster
# and write the dhcp_values block into its values-repo file. Workflow-scoped
# models carry the workflow name; models any sibling could reuse (the
# allocation request/response, the segment read-back, the DHCP shapes) do not.


class AllocateSegmentInput(BaseModel):
    """Input to AllocateSegmentWorkflow: which cluster to allocate for.

    Deliberately tiny — the site is DERIVED from where the cluster's values
    file sits in the repo (sites/<site>/...), cross-checked against the
    Segments Manager's site list, so a caller can never claim a site the
    cluster does not live in. `type` defaults to HC, the only supported type;
    anything else is rejected up front with UnsupportedSegmentType.
    """

    cluster: str = Field(min_length=1)
    type: SegmentType = SegmentType.HC


class AllocateSegmentRunArgs(BaseModel):
    """The workflow's single argument (same single-model rule as
    InitializeSegmentRunArgs — typed conversion is silently skipped when
    payload count differs from the declared parameter count)."""

    input: AllocateSegmentInput


class AllocateSegmentProgress(BaseModel):
    """Returned by the workflow's `progress` query (surfaced by the status API)."""

    phase: str


class ClusterFileLocation(BaseModel):
    """Where the cluster's values file lives in the values repo. The site is
    the path segment directly beneath the clusters root — the repo layout
    (sites/<site>/mces/<mce>/hostedClusters/<cluster>.yaml) is the source of
    truth for which site a cluster belongs to."""

    site: str = Field(min_length=1)
    relative_path: str = Field(min_length=1)


class SegmentAllocationRequest(BaseModel):
    """Input to allocate_segment: who is asking, where, and for what type.
    Scoped by (cluster, site, type) exactly like the Segments Manager's
    idempotency, so a Temporal retry can never double-allocate."""

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
    the allocation it was just handed."""

    segment: str = Field(min_length=1)
    site: str
    vlan_id: int
    status: str
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
    read-back, so the file records the policy for the type actually allocated."""

    cluster: str = Field(min_length=1)
    relative_path: str = Field(min_length=1)
    vlan_id: int
    segment: str = Field(min_length=1)  # CIDR
    type: SegmentType


class ValuesCommitRef(BaseModel):
    """Outcome of the values-repo append. `changed=False` (commit_sha None)
    means the file already carried this exact allocation — a re-run — and
    nothing was pushed. `dhcp_values` is returned in BOTH cases so the
    workflow can poll the DHCP API for convergence against the exact values
    the file carries."""

    commit_sha: str | None
    changed: bool
    dhcp_values: DhcpValues


class DhcpScopeState(BaseModel):
    """A read-only observation of the DHCP API: does the scope exist yet, and
    with which exclusions? `found=False` is a normal answer while Crossplane
    has not converged — never an error.

    Exclusions rather than the distribution range: the range is the DHCP API's
    own derivation (.1-.253), so comparing it would only confirm that service
    agrees with itself. The exclusions are what this workflow actually wrote to
    git, so they are what convergence is checked against."""

    found: bool
    exclusions: list[DhcpExclusion] = Field(default_factory=list)


class AllocateSegmentResult(BaseModel):
    cluster: str
    site: str
    type: SegmentType
    vlan_id: int
    segment: str
    epg_name: str
    commit_sha: str | None
    values_updated: bool
    dhcp_scope_ready: bool


# --- convert-segment --------------------------------------------------------
# The domain's third workflow: rebalance segment inventory between types by
# re-typing existing AVAILABLE, unassigned segments of a source type at a site.
# Re-typing is the whole operation — a converted segment stays Available and is
# immediately allocatable as its new type. Workflow-scoped models carry the
# workflow name; models a sibling could reuse (the search query/result and the
# type-update request, which mirror Segments Manager endpoints) do not.


class ConvertSegmentInput(BaseModel):
    """Input to ConvertSegmentWorkflow: what to convert, where, and how many.

    `quantity` is a TARGET, not a requirement: fewer matching segments than
    requested converts what exists and reports the shortfall in the result —
    an operator rebalancing inventory wants the partial conversion either way.
    """

    site: str = Field(min_length=1)
    source_type: SegmentType
    destination_type: SegmentType
    quantity: int = Field(ge=1)

    @model_validator(mode="after")
    def _distinct_types(self) -> "ConvertSegmentInput":
        if self.source_type == self.destination_type:
            raise ValueError(
                "source_type and destination_type must differ — converting a "
                "segment to its own type is a no-op"
            )
        return self


class ConvertSegmentRunArgs(BaseModel):
    """The workflow's single argument (same single-model rule as
    InitializeSegmentRunArgs — typed conversion is silently skipped when
    payload count differs from the declared parameter count)."""

    input: ConvertSegmentInput


class ConvertibleSegmentsQuery(BaseModel):
    """Input to list_convertible_segments: which type to look for, and where.
    Status is NOT a parameter — "convertible" MEANS Available and unassigned,
    and that rule belongs to the activity, not to each caller. Allocated
    segments are in use, and an Available segment carrying a cluster is a
    manager invariant violation the activity refuses to convert."""

    site: str = Field(min_length=1)
    type: SegmentType


class ConvertibleSegment(BaseModel):
    """One search hit: everything the workflow needs to pick it, convert it and
    report it.

    Neither `status` nor `cluster_name` carries a choice for the workflow
    (every hit is Available and unassigned) — they are kept so the activity can
    ASSERT both, rather than trusting the filter it asked for.
    """

    segment: str = Field(min_length=1)  # CIDR
    vlan_id: int = Field(ge=1, le=4094)
    status: str = Field(min_length=1)  # always "Available"
    cluster_name: str | None = None  # always empty on a convertible hit


class SegmentTypeUpdate(BaseModel):
    """Input to convert_segment_type, mirroring the Segments Manager's
    PUT /api/segments/type body. `type` is the NEW type; `expected_type` is
    the compare-and-set guard — the type being converted FROM — so a
    concurrent conversion that re-typed the segment first turns this call
    into a refusal instead of a silent hijack."""

    segment: str = Field(min_length=1)
    type: SegmentType
    expected_type: SegmentType


class ConvertedSegmentReport(BaseModel):
    """Per-segment outcome of the conversion loop: which segment was re-typed.

    Nothing else to report — the conversion IS the operation, and every
    converted segment ends it Available under its new type. No
    `previous_status`/`cancelled_previous_run`/`open_rules_status`: each was a
    constant, and a constant dressed up as a per-segment finding is exactly the
    kind of field an operator reads as meaningful.
    """

    segment: str
    vlan_id: int


class ConvertSegmentProgress(BaseModel):
    """Returned by the workflow's `progress` query (surfaced by the status
    API). Carries the full per-segment report list — not just counts — so a
    mid-loop failure still shows which segments WERE converted (those
    conversions stand)."""

    phase: str
    matched: int
    selected: int
    converted: list[ConvertedSegmentReport]


class ConvertSegmentResult(BaseModel):
    """`shortfall` = how many requested conversions had no matching segment
    (0 when enough matched); the conversions that did happen are in
    `converted` either way."""

    site: str
    source_type: SegmentType
    destination_type: SegmentType
    requested_quantity: int
    matched: int
    shortfall: int
    converted: list[ConvertedSegmentReport]
