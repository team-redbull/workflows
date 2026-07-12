"""Typed state for the connectivity workflow — the contract between brain and limbs.

Everything that crosses the workflow/activity boundary is a Pydantic model (or a
list of primitives), never an untyped dict.
"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class SegmentType(str, Enum):
    """Segment types known to the Segments Manager. Any type is valid input;
    the workflow decides which ones connectivity is implemented for."""

    MCE = "MCE"
    HC = "HC"
    INVENTORY = "INVENTORY"
    PXE = "PXE"


class ConnectivityInput(BaseModel):
    """Input to ConnectivityWorkflow: the segment whose firewall rules to open."""

    segment: str = Field(min_length=1)  # CIDR, e.g. "130.154.20.0/24"
    type: SegmentType


class OpenRulesRequest(BaseModel):
    """The brain's intent to open firewall rules for one source -> destination pair.

    Deliberately free of next-API payload shape: port profiles, system names and
    the domain are the activity layer's concern, keyed off the type pair.
    """

    source_segment: str = Field(min_length=1)
    destination_segment: str = Field(min_length=1)
    source_type: SegmentType
    destination_type: SegmentType


class ConnectivityRequestRef(BaseModel):
    """The next API's acknowledgement of a submitted open-rules request."""

    id: int
    status: str


class ConnectivityRequestsUpdate(BaseModel):
    """The pending next request ids to surface beside the segment's status in
    the Segments Manager UI. An empty list removes the display (all complete)."""

    segment: str = Field(min_length=1)
    request_ids: list[int]


class ConnectivityResumeState(BaseModel):
    """Polling state carried across continue_as_new runs of the workflow."""

    request_ids: list[int]
    pending_ids: list[int]
    peer_segment_count: int


class ConnectivityRunArgs(BaseModel):
    """The workflow's single argument: public input + internal resume state.

    A single-model argument is the Temporal-recommended shape. It also avoids
    an SDK gotcha: typed payload conversion is silently SKIPPED whenever the
    number of payloads differs from the number of declared run() parameters —
    a two-parameter run(input, resume=None) started with one payload receives
    a raw dict instead of a Pydantic model.
    """

    input: ConnectivityInput
    resume: ConnectivityResumeState | None = None


class ConnectivityProgress(BaseModel):
    """Returned by the workflow's `progress` query (surfaced by the status API)."""

    phase: str
    total_requests: int
    pending_requests: int


class ConnectivityResult(BaseModel):
    segment: str
    type: SegmentType
    peer_segment_count: int
    request_ids: list[int]
    unlocked: bool = True
