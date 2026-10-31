# -- Segment-lifecycle domain --
# Queue names are scoped differently ON PURPOSE. The ACTIVITY queue belongs to
# the DOMAIN: one activity-worker deployment (`segment-lifecycle-worker`) owns that
# domain's dependency + credential set, and every workflow in the domain routes
# its activities there. Each WORKFLOW gets its OWN workflow queue, so every
# workflow in this domain (initialize, allocate, release) registers on its own
# queue while reusing the same limb.
INITIALIZE_SEGMENT_WORKFLOW_QUEUE = "initialize-segment-workflow"
ALLOCATE_SEGMENT_WORKFLOW_QUEUE = "allocate-segment-workflow"
RELEASE_SEGMENT_WORKFLOW_QUEUE = "release-segment-workflow"
SEGMENT_LIFECYCLE_ACTIVITY_QUEUE = "segment-lifecycle-activity"

# -- Server-lifecycle domain --
# Same split as above for the WORKFLOW queue — each workflow gets its own, and
# every one of them (install, remove, uninstall, publish-infraenv-boot) runs
# its activities on the same per-MCE limb.
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
REMOVE_SERVER_WORKFLOW_QUEUE = "remove-server-workflow"
UNINSTALL_SERVER_WORKFLOW_QUEUE = "uninstall-server-workflow"
PUBLISH_INFRAENV_BOOT_WORKFLOW_QUEUE = "publish-infraenv-boot-workflow"
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

# -- Server-provisioning domain --
# Back to the plain segment-lifecycle shape: ONE activity queue for the domain,
# because this limb writes to no cluster. Its dependencies are the iDRACs, the
# OpenManage Enterprise appliance, the naming service and server-scan — all
# reached from one hub-side deployment holding the OME and iDRAC credentials.
# Each vendor gets its own WORKFLOW (and queue): provisioning a Dell through
# OME has nothing in common with an Intersight claim beyond its end state.
PROVISION_DELL_SERVER_WORKFLOW_QUEUE = "provision-dell-server-workflow"
SERVER_PROVISIONING_ACTIVITY_QUEUE = "server-provisioning-activity"

# -- Cluster-upgrade domain --
# Two workflows, each on its own queue as everywhere above: the per-cluster run,
# and the control-plane step it starts as a child on a per-MCE SLOT id (the
# slot is the concurrency cap, see shared/workflow_ids.py).
#
# THREE activity queues for one domain, because the domain's work needs three
# different credential sets (CLAUDE.md §2: the deployment follows the
# dependency + credential set):
#   * the HUB limb holds the day1 git push token, the mirror registry pull
#     credential and the Brain push token — one queue, like segment-lifecycle;
#   * the per-MCE cluster limb runs as the workflow ServiceAccount inside that
#     MCE (reads, and the Argo CD sync that is the run's only cluster write) —
#     a queue per MCE, routed exactly like server-lifecycle above;
#   * the per-MCE agent limb holds no Kubernetes credential at all: it calls
#     the kagent agent inside that MCE, whose tools run under their own
#     identity — a queue per MCE as well.
UPGRADE_CLUSTER_WORKFLOW_QUEUE = "upgrade-cluster-workflow"
UPGRADE_CONTROL_PLANE_WORKFLOW_QUEUE = "upgrade-control-plane-workflow"
UPGRADE_GITOPS_ACTIVITY_QUEUE = "upgrade-gitops-activity"
CLUSTER_UPGRADE_ACTIVITY_QUEUE = "cluster-upgrade-activity"
UPGRADE_AGENT_ACTIVITY_QUEUE = "upgrade-agent-activity"


def cluster_upgrade_activity_queue(mce_cluster: str) -> str:
    """The cluster-upgrade activity queue for ONE MCE (see server_lifecycle_activity_queue)."""
    return f"{CLUSTER_UPGRADE_ACTIVITY_QUEUE}-{mce_cluster}"


def upgrade_agent_activity_queue(mce_cluster: str) -> str:
    """The upgrade-agent activity queue for ONE MCE: the agent runs where its tools read."""
    return f"{UPGRADE_AGENT_ACTIVITY_QUEUE}-{mce_cluster}"
