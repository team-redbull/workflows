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


# The install run's ids carry the workflow name, then the MCE, then `_` before
# every further part: `install-server-<mce>_<...>`. The MCE ends at the first
# `_` because a Kubernetes name cannot contain one, so a hub's runs all sit under
# `install-server-<mce>_` and never under another hub's, even when one MCE's name
# extends another's (`ocp4-a`, `ocp4-a-b`). An id is not part of a run's replayed
# history, so adding the MCE cannot break a run in flight. Runs started before it
# keep the ids they were started with (`install-server-name-<server>`,
# `install-server-<infraEnv>_<slot>`); anything that counts runs by id has to look
# for both until they have closed.
_INSTALL_SERVER = "install-server"


def install_server_name_workflow_id(mce_cluster: str, server_name: str) -> str:
    """`install-server-<mce>_<server>`: an install of ONE NAMED machine on one MCE.

    install-server builds it for every name in `server_names`, one or a list,
    so one machine is one run however it is asked for: a second request for it
    while it installs on that MCE is a dedup hit, never a second run.

    The MCE is in the id so every run on a hub is found under one prefix,
    `install-server-<mce>_`, and a hub never sees another's runs. Two hubs
    installing the SAME machine at once are no longer stopped by this id: the
    server-scan install lock stops the second.

    The MCE is followed by `_` because a Kubernetes name cannot contain it, so
    where the MCE ends is unambiguous. With `-` after it, the prefix
    `install-server-ocp4-a-` would also match the runs of an MCE `ocp4-a-b`. A
    server name with an underscore is not a valid Kubernetes name, so a named id
    can not equal a pool id (`..._<infraEnv>_<slot>`) either.

    That name is LOWERCASED, which is not cosmetic: server-scan names carry an
    uppercase vendor serial, the run lowercases it to build the resource names,
    and an id that kept the original case would give ONE machine TWO ids
    depending on how a caller typed it. Both runs would then be accepted and
    both would race onto the same BareMetalHost — the exact collision this id
    exists to prevent. Two different servers cannot collide by lowercasing:
    server-scan names differ by more than case.
    """
    return f"{_INSTALL_SERVER}-{mce_cluster}_{server_name.lower()}"


def install_server_slot_workflow_id(mce_cluster: str, infra_env: str, slot: int) -> str:
    """`install-server-<mce>_<infraEnv>_<slot>`: one of the PARALLEL draws from a pool.

    Every install-server run that lets the pool pick is on a slot: `count: N`
    fills N free ones, and a request without `count` fills one. Keyed on the
    HUB, the CANDIDATE POOL and a slot number, and on none of the obvious
    other things:

    Not the machine, because it is not known when the run starts: which server
    gets installed is decided inside the workflow, by asking server-scan.
    Building the id from it would mean acquiring the server in the router
    first — putting the decision the run's outcome depends on outside the run,
    which is exactly the shape initialize-segment was changed to avoid. (It
    also would lose the walk to the next candidate when one is unusable.)

    The MCE IS in the id, since 2026-10-25. It was not before, and two MCEs
    filling an InfraEnv of the same name then shared ONE set of slots: a caller
    counting the open runs of its own pool counted the other hub's as well, and
    ordered too few. The slots are still a fixed, enumerable set (`_1`..`_100`),
    so a controller can ask Temporal about each id directly rather than search
    for them, and a search lags behind a start.

    The id does not stop two runs taking one machine; the install lock does,
    for any number of runs: server-scan takes it by compare-and-set on the
    document revision, refuses a live lock to any OTHER workflow id, and the
    run writes nothing until it holds it — a run that loses the race skips the
    machine (`reserved`) and tries its next candidate. So any number of slots
    may draw from one pool at once and still land on different machines.

    Pool draws used to be serialized on ONE id per pool, so a repeated request
    was a 409: first as the only defence against two runs drawing one machine
    (before the lock), then only to stop a double-click or a retried POST
    installing two. Dropped on 2026-10-24: its callers are people asking for one
    more server, and the 409 refused exactly that.

    `_` follows the MCE and joins the parts after it, for the reason
    install_server_name_workflow_id gives.
    """
    return f"{_INSTALL_SERVER}-{mce_cluster}_{infra_env}_{slot}"


def remove_server_workflow_id(server_name: str) -> str:
    """`remove-server-<server>`: one removal of a machine at a time.

    Keyed on the MACHINE, unlike install-server: the server is named up front,
    and it is what two removals would race on. The distinct prefix keeps a
    removal from colliding with an install of the same server. Lowercased for
    the reason install_server_name_workflow_id gives — one machine, one id, however
    a caller typed its uppercase serial.
    """
    return f"remove-server-{server_name.lower()}"


def uninstall_server_workflow_id(server_name: str) -> str:
    """`uninstall-server-<server>`: one uninstall of a machine at a time.

    Keyed on the MACHINE, like remove-server: the server is named up front and
    it is what two uninstalls would race on — two runs deleting one Machine and
    waiting on one Agent. The `uninstall-` prefix is what keeps it apart from
    the other two ids of the same machine, and both distinctions matter:

      * `install-server-<mce>_<server>` — an install and an uninstall of one
        machine are different operations, and refusing one because the other
        once ran (or is running) would be wrong in both directions;
      * `remove-server-<server>` — uninstall-server STARTS that id as its
        last step, an awaited child, so sharing it would make every uninstall
        collide with itself.

    Lowercased for the reason install_server_name_workflow_id gives — one
    machine, one id, however a caller typed its uppercase serial.
    """
    return f"uninstall-server-{server_name.lower()}"


def publish_infraenv_boot_workflow_id(mce_cluster: str, infra_env: str) -> str:
    """`publish-infraenv-boot-<mce>-<infraEnv>`: one publish of a boot entry at a time.

    Keyed on what it writes — ONE file per (MCE, InfraEnv) on the PXE VM — so
    two triggers for the same entry dedup, while the same InfraEnv name on two
    MCEs (two different files) does not serialize. Two runs racing would be
    harmless anyway (the PXE VM keeps the newer image), so this is dedup, not
    a safety lock.
    """
    return f"publish-infraenv-boot-{mce_cluster}-{infra_env}"


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


def upgrade_cluster_workflow_id(mce_cluster: str, cluster: str) -> str:
    """`upgrade-cluster-<mce>_<cluster>`: one upgrade per HostedCluster at a time.

    Keyed on the cluster, so a second request while one runs is a 409 — two
    runs committing mastertag and workertag to one file would undo each other.
    The MCE comes first and `_` follows it, as in install-server's ids: a
    Kubernetes name cannot contain `_`, so a hub's runs all sit under
    `upgrade-cluster-<mce>_` and never under an MCE whose name extends it.
    """
    return f"upgrade-cluster-{mce_cluster}_{cluster}"


def upgrade_control_plane_workflow_id(mce_cluster: str, slot: int) -> str:
    """`upgrade-control-plane-<mce>_<slot>`: one of an MCE's control-plane upgrade SLOTS.

    The id IS the concurrency cap. An MCE allows `maxConcurrentControlPlanes`
    hosted control planes upgrading at once (day1 `upgrade/mces/<mce>.yaml`);
    a run takes slot 1, 2, … in turn, and Temporal's one-running-workflow-per-id
    makes a busy slot a WorkflowAlreadyStartedError, so the run moves on, or
    waits when every slot is taken. Temporal's own limits cap activities per
    worker or per second, not "N control planes in flight per MCE", so they
    do not fit. A finished slot is free again (closed ids are reusable), the
    install-server slot precedent.
    """
    return f"upgrade-control-plane-{mce_cluster}_{slot}"
