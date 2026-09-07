"""Deterministic workflow-id builders — ONE definition per id scheme.

Workflow ids are the dedup key for the whole system (a duplicate trigger while
running is rejected as already-started), so the scheme for each workflow must
exist in exactly one place. These builders are needed on BOTH sides of the
sandbox boundary — the routers build ids to start workflows, and workflows
build SIBLING ids (convert-segment starts an initialize-segment run per
converted segment, as a detached child) — which is why they live here as pure
string helpers rather than in a router module: a workflow file can never
import a router (FastAPI inside the sandbox).
"""

from __future__ import annotations

from shared.models.segment_lifecycle import SegmentType


def initialize_segment_workflow_id(segment_type: SegmentType, segment: str) -> str:
    """`initialize-segment-<TYPE>-<network>`: natural dedup per (type, segment).

    Prefixed with the WORKFLOW name, not the domain: a second workflow in this
    domain acting on the same segment (e.g. a future close-segment-rules) must
    get a distinct id, or it would collide with this one and be rejected as
    already-started.

    The CIDR mask is dropped from the id (e.g. 130.154.20.0/24 -> the id ends
    ...-130.154.20.0), so two requests for the same network address dedup
    regardless of how the mask was written.
    """
    network = segment.split("/", 1)[0]
    return f"initialize-segment-{segment_type.value}-{network}"


def allocate_segment_workflow_id(segment_type: SegmentType, cluster: str) -> str:
    """`allocate-segment-<TYPE>-<cluster>`: natural dedup per (type, cluster).

    The id MUST carry the type: allocation is scoped by (cluster, site, type)
    in the Segments Manager — one cluster can hold one segment per type at a
    site — so a type-less id would make two legitimate allocations collide.
    """
    return f"allocate-segment-{segment_type.value}-{cluster}"


def convert_segment_workflow_id(
    site: str, source_type: SegmentType, destination_type: SegmentType
) -> str:
    """`convert-segment-<site>-<SRC>-to-<DEST>`: natural dedup per conversion.

    Scoped by (site, source, destination): two identical conversion requests
    racing would select the same source segments, so they dedup onto one id.
    Conversions differing in EITHER type are legitimately concurrent — the
    Segments Manager's expected_type compare-and-set arbitrates when two of
    them (e.g. HC->MCE and HC->PXE at one site) grab the same segment.
    """
    return f"convert-segment-{site}-{source_type.value}-to-{destination_type.value}"
