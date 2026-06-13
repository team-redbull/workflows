from __future__ import annotations
from datetime import datetime
from pydantic import BaseModel, Field


class SegmentAllocationInput(BaseModel):
    """Input to the SegmentAllocationWorkflow."""

    cluster_name: str = Field(min_length=1)
    site: str = Field(min_length=1)


class SegmentSpec(BaseModel):
    """A network segment definition, as generated and/or stored.

    Mirrors the Segments Manager's create payload plus availability state.
    """

    site: str = Field(min_length=1)
    vlan_id: int = Field(ge=1, le=4094)
    segment: str = Field(min_length=1)  # CIDR, e.g. "192.168.15.0/24"
    epg_name: str = Field(min_length=1)
    dhcp: bool = True


class SegmentAllocationResult(BaseModel):
    """The outcome of allocating a segment to a cluster."""

    cluster_name: str
    site: str
    vlan_id: int
    segment: str
    epg_name: str
    allocated_at: datetime
