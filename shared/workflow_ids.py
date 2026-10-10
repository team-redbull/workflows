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

    Prefixed with the WORKFLOW name, not the domain: another workflow in this
    domain acting on the same segment (release-segment, say) must get a
    distinct id, or it would collide with this one and be rejected as
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


def install_server_workflow_id(infra_env: str, server_name: str | None = None) -> str:
    """`install-server-<infraEnv>`, or `install-server-name-<server>` when named.

    Keyed on the CANDIDATE POOL, not on the machine and not on the target.

    Not the machine, because it is not known when the run starts: which server
    gets installed is decided inside the workflow, by asking server-scan.
    Building the id from it would mean acquiring the server in the router
    first — putting the decision the run's outcome depends on outside the run,
    which is exactly the shape initialize-segment was changed to avoid.

    Not the (InfraEnv, MCE) pair either, which is what this keyed on first and
    was wrong: the pool a run draws from is `^ocp-<infraEnv>`, with no MCE in
    it. Two MCEs each filling an InfraEnv of the same name would get DIFFERENT
    ids while drawing from the SAME pool — the one case the serialization
    exists to prevent. The pool is the InfraEnv, so the id is too.

    Serializing per pool was the first defence against two runs racing onto
    one machine, since server-scan's draw hands candidates out unlocked. The
    run now also takes server-scan's install lock on the machine it chooses
    (ADR-0035 there), which covers what this id never could: a run for ANOTHER
    MCE drawing a server already installed elsewhere, invisible to its own
    BareMetalHost probe. With the lock, keying on (pool, MCE) would let two MCEs
    fill one InfraEnv concurrently — a deliberate change still to be made, not
    a consequence of adding the lock.

    An explicitly named server draws from a pool of one, so it gets its own id
    and does not serialize against pattern draws for the same InfraEnv.

    That name is LOWERCASED, which is not cosmetic: server-scan names carry an
    uppercase vendor serial, the run lowercases it to build the resource names,
    and an id that kept the original case would give ONE machine TWO ids
    depending on how a caller typed it. Both runs would then be accepted and
    both would race onto the same BareMetalHost — the exact collision this id
    exists to prevent. Two different servers cannot collide by lowercasing:
    server-scan names differ by more than case.
    """
    if server_name:
        return f"install-server-name-{server_name.lower()}"
    return f"install-server-{infra_env}"


def provision_dell_server_workflow_id(idrac_ip: str) -> str:
    """`provision-dell-server-<idrac ip>`: natural dedup per machine at the rack.

    The iDRAC address is the only identity the run has when it starts — the
    service tag is read FROM that iDRAC — and a technician double-submitting a
    row of a bulk list must land on the running run, not start a second one
    that races it through the same reboots.
    """
    return f"provision-dell-server-{idrac_ip}"

def release_segment_workflow_id(segment_type: SegmentType, cluster: str) -> str:
    """`release-segment-<TYPE>-<cluster>`: natural dedup per (type, cluster).

    The mirror of allocate_segment_workflow_id, scoped the same way: a cluster
    holds at most one segment per type, so (type, cluster) names exactly the
    allocation a run gives back, and a duplicate trigger while it runs is a
    409. The distinct prefix keeps an allocate and a release of one cluster
    from colliding on one id.
    """
    return f"release-segment-{segment_type.value}-{cluster}"
