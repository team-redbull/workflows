"""Typed state for the segment-connectivity workflow — the contract between brain and limbs.

Everything that crosses the workflow/activity boundary is a Pydantic model (or a
list of primitives), never an untyped dict.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


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
