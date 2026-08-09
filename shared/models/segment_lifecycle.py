"""Typed state for the segment-lifecycle workflow — the contract between brain and limbs.

Everything that crosses the workflow/activity boundary is a Pydantic model (or a
list of primitives), never an untyped dict.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field, model_validator


class SegmentType(str, Enum):
    """Segment types known to the Segments Manager. Any type is valid input;
    the workflow decides which ones connectivity is implemented for."""

    MCE = "MCE"
    HC = "HC"
    INVENTORY = "INVENTORY"
    PXE = "PXE"


class OpenSegmentRulesInput(BaseModel):
    """Input to OpenSegmentRulesWorkflow: the segment to CREATE in the
    Segments Manager, then open firewall rules for.

    This workflow is the single entry point for a segment's whole lifecycle —
    the Segments Manager no longer triggers it on segment creation, it is a
    dependency the workflow calls (create_segment is step 1). So the input is
    the full segment definition, not just a reference to an existing one, and
    every step of the flow is visible in one Temporal run.

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


class OpenRulesRequest(BaseModel):
    """The brain's intent to open firewall rules for one source -> destination pair.

    Deliberately free of next-API payload shape: port profiles, system names and
    the domain are the activity layer's concern, keyed off the type pair.
    """

    source_segment: str = Field(min_length=1)
    destination_segment: str = Field(min_length=1)
    source_type: SegmentType
    destination_type: SegmentType


class SegmentRef(BaseModel):
    """A same-site segment eligible to peer with some source type — carries
    its own type because one source type can peer with several destination
    types at once (e.g. MCE peers with HC, INVENTORY and PXE)."""

    segment: str = Field(min_length=1)  # CIDR
    type: SegmentType


class PeerSegmentsQuery(BaseModel):
    """Input to list_peer_segments: which type is asking, and where."""

    source_type: SegmentType
    site: str = Field(min_length=1)


class BmcOpenRulesRequest(BaseModel):
    """One-directional MCE -> BMC firewall-rule request. BMC is not a
    Segments-Manager-tracked SegmentType — its CIDR is a static,
    ConfigMap-sourced value per site — so this is a deliberately separate,
    narrower model from OpenRulesRequest."""

    mce_segment: str = Field(min_length=1)
    bmc_segment: str = Field(min_length=1)


class NextRequestRef(BaseModel):
    """The next API's acknowledgement of a submitted request.

    Named after NEXT rather than after this domain or workflow: it is whatever
    next hands back for ANY submitted request, so a future workflow submitting
    a different kind of rule change gets the same shape back.
    """

    id: int
    status: str


class SegmentConnectivityRequestsUpdate(BaseModel):
    """The pending next request ids to surface beside the segment's status in
    the Segments Manager UI. An empty list removes the display (all complete)."""

    segment: str = Field(min_length=1)
    request_ids: list[int]
    submitted_at: datetime  # drives the "time since submit" header in the UI popover


class SegmentConnectivityFailureNotice(BaseModel):
    """Published to the Segments Manager when the workflow fails terminally:
    clears the pending request-ids display and surfaces a failure note beside
    the segment's status badge (the segment intentionally stays Locked)."""

    segment: str = Field(min_length=1)
    message: str = Field(min_length=1)


class OpenSegmentRulesResumeState(BaseModel):
    """Polling state carried across continue_as_new runs of the workflow."""

    request_ids: list[int]
    pending_request_ids: list[int]
    peer_segment_count: int
    submitted_at: datetime


class OpenSegmentRulesRunArgs(BaseModel):
    """The workflow's single argument: public input + internal resume state.

    A single-model argument is the Temporal-recommended shape. It also avoids
    an SDK gotcha: typed payload conversion is silently SKIPPED whenever the
    number of payloads differs from the number of declared run() parameters —
    a two-parameter run(input, resume=None) started with one payload receives
    a raw dict instead of a Pydantic model.
    """

    input: OpenSegmentRulesInput
    resume: OpenSegmentRulesResumeState | None = None


class OpenSegmentRulesProgress(BaseModel):
    """Returned by the workflow's `progress` query (surfaced by the status API)."""

    phase: str
    total_requests: int
    pending_requests: int


class OpenSegmentRulesResult(BaseModel):
    segment: str
    type: SegmentType
    peer_segment_count: int
    request_ids: list[int]


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
    OpenSegmentRulesRunArgs — typed conversion is silently skipped when
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
    from the allocated segment + the DHCP_EXCLUSION_OCTET_RANGES policy.
    `network` is the mask-stripped network address (10.20.90.0, never
    10.20.90.0/24) — the DHCP scope's identity."""

    network: str = Field(min_length=1)
    start_range: str = Field(min_length=1)
    end_range: str = Field(min_length=1)
    exclusions: list[DhcpExclusion]


class ClusterValuesAppendRequest(BaseModel):
    """Input to append_allocation_to_cluster_values: which file to append to
    and what was allocated. The dhcp_values block itself is derived
    activity-side (the DHCP policy lives in the activity worker's config,
    which the sandboxed workflow cannot read)."""

    cluster: str = Field(min_length=1)
    relative_path: str = Field(min_length=1)
    vlan_id: int
    segment: str = Field(min_length=1)  # CIDR


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
    with what range? `found=False` is a normal answer while Crossplane has not
    converged — never an error."""

    found: bool
    start_range: str | None = None
    end_range: str | None = None


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
# re-typing existing AVAILABLE segments of a source type at a site and
# re-running the open-segment-rules flow for each (the new type has different
# peers, so connectivity must be re-established). Workflow-scoped models carry
# the workflow name; models a sibling could reuse (the search query/result and
# the type-update request, which mirror Segments Manager endpoints) do not.


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
    OpenSegmentRulesRunArgs — typed conversion is silently skipped when
    payload count differs from the declared parameter count)."""

    input: ConvertSegmentInput


class ConvertibleSegmentsQuery(BaseModel):
    """Input to list_convertible_segments: which type to look for, and where.
    Status is NOT a parameter — "convertible" MEANS Available, and that rule
    belongs to the activity, not to each caller. Allocated segments are in use;
    Locked ones have no established connectivity and may still have a live
    open-segment-rules run, which this workflow deliberately never disturbs."""

    site: str = Field(min_length=1)
    type: SegmentType


class ConvertibleSegment(BaseModel):
    """One search hit: everything the workflow needs to pick it, convert it,
    and hand the open-segment-rules child a full OpenSegmentRulesInput —
    without a second read-back.

    `status` carries no choice for the workflow (every hit is Available) — it
    is kept so the activity can assert that, rather than trusting the filter it
    asked for.
    """

    segment: str = Field(min_length=1)  # CIDR
    vlan_id: int = Field(ge=1, le=4094)
    epg_name: str = Field(min_length=1)
    status: str = Field(min_length=1)  # always "Available"
    dhcp: bool


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
    """Per-segment outcome of the conversion loop. `open_rules_status` reports
    the FAN-OUT only (like the bulk route's items): `started` means Temporal
    accepted the child run, whose own progress/failure lives under its own
    workflow id.

    No `previous_status`/`cancelled_previous_run`: every converted segment was
    Available and no stale run is ever cancelled, so both were constants — and
    a constant dressed up as a per-segment finding is exactly the kind of field
    an operator reads as meaningful.
    """

    segment: str
    vlan_id: int
    open_segment_rules_workflow_id: str
    open_rules_status: Literal["started", "already_running"]


class ConvertSegmentProgress(BaseModel):
    """Returned by the workflow's `progress` query (surfaced by the status
    API). Carries the full per-segment report list — not just counts — so a
    mid-loop failure still shows which segments WERE converted (their child
    runs exist and proceed regardless)."""

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
