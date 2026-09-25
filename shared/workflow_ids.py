"""Deterministic workflow-id builders — ONE definition per id scheme.

Workflow ids are the dedup key for the whole system (a duplicate trigger while
running is rejected as already-started), so the scheme for each workflow must
exist in exactly one place.

The routers are the only callers today. They stay HERE, as pure string helpers,
rather than in a router module, because an id scheme must be importable from
BOTH sides of the sandbox boundary: a workflow file can never import a router
(FastAPI is not sandbox-safe), so the moment any workflow needs to build a
sibling's id — as the since-removed convert-segment once did, starting an
initialize-segment run per converted segment — a scheme defined router-side
would have to be duplicated. One definition, reachable from either side, costs
nothing to keep.
"""

from __future__ import annotations

from shared.models.segment_lifecycle import SegmentType


def initialize_segment_workflow_id(segment: str) -> str:
    """`initialize-segment-<network>`: natural dedup per segment.

    Prefixed with the WORKFLOW name, not the domain: a second workflow in this
    domain acting on the same segment (e.g. a future release-segment) must get
    a distinct id, or it would collide with this one and be rejected as
    already-started.

    No type: a segment is created without one (it is stamped on at
    allocation), and the network is globally unique on its own. (The id used
    to be initialize-segment-<TYPE>-<network>, from when the type was part of
    the definition.)

    The CIDR mask is dropped from the id (e.g. 130.154.20.0/24 -> the id ends
    ...-130.154.20.0), so two requests for the same network address dedup
    regardless of how the mask was written.
    """
    network = segment.split("/", 1)[0]
    return f"initialize-segment-{network}"


def allocate_segment_workflow_id(segment_type: SegmentType, cluster: str) -> str:
    """`allocate-segment-<TYPE>-<cluster>`: natural dedup per (type, cluster).

    The id MUST carry the type: allocation is scoped by (cluster, site, type)
    in the Segments Manager — one cluster can hold one segment per type at a
    site — so a type-less id would make two legitimate allocations collide.
    """
    return f"allocate-segment-{segment_type.value}-{cluster}"


def install_server_workflow_id(infra_env: str, mce_cluster: str) -> str:
    """`install-server-<infraEnv>-<mce>`: dedup per target, not per server.

    Keyed on the DESTINATION rather than the machine, because the machine is
    not known when the run starts: which server gets installed is decided
    inside the workflow, by asking server-scan. Building the id from the server
    would mean acquiring it in the router first — putting the decision the
    run's outcome depends on outside the run, which is exactly the shape
    initialize-segment was changed to avoid.

    This makes installs into one (InfraEnv, MCE) SERIAL: a second trigger while
    one is in flight is rejected as already-started. That is deliberate for
    now. server-scan hands out candidates with no reservation (ADR-0032
    accepts this explicitly — `$sample` can draw the same server for two
    concurrent callers, and nothing changes its state until a cluster reports
    the node minutes later), so serializing per target is what stops two runs
    racing onto one machine. Installing several servers into one InfraEnv at
    once needs a real reservation in server-scan first; it is not just a
    matter of loosening this id.
    """
    return f"install-server-{infra_env}-{mce_cluster}"
