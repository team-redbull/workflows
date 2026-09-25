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
# Same split as above: the ACTIVITY queue belongs to the DOMAIN (one
# `server-lifecycle-worker` deployment owns the Kubernetes client, the BMC
# credentials and the server-scan token), while each WORKFLOW gets its own
# workflow queue. A future uninstall-server registers its own workflow queue
# against this same limb.
INSTALL_SERVER_WORKFLOW_QUEUE = "install-server-workflow"
SERVER_LIFECYCLE_ACTIVITY_QUEUE = "server-lifecycle-activity"
