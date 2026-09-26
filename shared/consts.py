# -- Segment-lifecycle domain --
# Queue names are scoped differently ON PURPOSE. The ACTIVITY queue belongs to
# the DOMAIN: one activity-worker deployment (`segment-lifecycle-worker`) owns that
# domain's dependency + credential set, and every workflow in the domain routes
# its activities there. Each WORKFLOW gets its OWN workflow queue, so a third
# workflow in this domain (e.g. a future release-segment) registers on its own
# queue while reusing the same limb.
INITIALIZE_SEGMENT_WORKFLOW_QUEUE = "initialize-segment-workflow"
ALLOCATE_SEGMENT_WORKFLOW_QUEUE = "allocate-segment-workflow"
SEGMENT_LIFECYCLE_ACTIVITY_QUEUE = "segment-lifecycle-activity"

# -- Server-lifecycle domain --
# Same split as above for the WORKFLOW queue — each workflow gets its own, so a
# future uninstall-server registers its own against the same limbs.
#
# The ACTIVITY queue is NOT the same, and this is the one place the pattern
# differs. It is scoped to the domain AND to one MCE cluster, because these
# activities WRITE TO A CLUSTER. The brain runs on the hub; the BareMetalHost,
# Secret and NMStateConfig have to be created on the MCE that owns the
# InfraEnv, which is a different API server reached at its own
# `api.<mce>.<domain>`.
#
# Routing by queue is what crosses that boundary, and it is deliberately not a
# credential. A worker runs INSIDE each MCE, authenticates to its own API
# server as its own ServiceAccount, and dials OUT to Temporal to long-poll its
# queue. So no cross-cluster kubeconfig exists anywhere, and the hub never
# needs inbound access to an MCE's API server — the connection only ever runs
# the other way. The alternative, one hub worker holding a kubeconfig per MCE,
# concentrates admin-equivalent credentials for the whole fleet onto one pod.
#
# It also makes a whole class of mistake impossible: the queue name IS the
# target, so the MCE whose VLAN is used and the cluster written to cannot
# disagree.
INSTALL_SERVER_WORKFLOW_QUEUE = "install-server-workflow"
SERVER_LIFECYCLE_ACTIVITY_QUEUE = "server-lifecycle-activity"


def server_lifecycle_activity_queue(mce_cluster: str) -> str:
    """The server-lifecycle activity queue for ONE MCE cluster.

    Pure and deterministic, so workflow code derives it from its own input and
    it replays identically. The worker in that MCE polls the same name, built
    from its MCE_CLUSTER setting.

    A name no worker polls is not silently wrong: the activity's
    schedule-to-start timeout fires and keeps firing, which shows in history as
    repeated timeouts rather than a task sitting invisibly in a queue, and the
    workflow's `progress` query reports the queue it is waiting on.
    """
    return f"{SERVER_LIFECYCLE_ACTIVITY_QUEUE}-{mce_cluster}"
