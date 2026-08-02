# -- Segment-connectivity domain --
# Queue names are scoped differently ON PURPOSE. The ACTIVITY queue belongs to
# the DOMAIN: one activity-worker deployment (`segment-connectivity`) owns that
# domain's dependency + credential set, and every workflow in the domain routes
# its activities there. Each WORKFLOW gets its OWN workflow queue, so a second
# workflow in this domain (e.g. a future close-segment-rules) registers on its
# own queue while reusing the same limb.
OPEN_SEGMENT_RULES_WORKFLOW_QUEUE = "open-segment-rules-workflow"
SEGMENT_CONNECTIVITY_ACTIVITY_QUEUE = "segment-connectivity-activity"
