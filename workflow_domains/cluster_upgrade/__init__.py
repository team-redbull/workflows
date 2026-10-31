"""Cluster-upgrade domain: upgrading HyperShift HostedClusters through day1 git.

Every change to a hosted cluster is a day1 commit followed by a sync of that
cluster's one Argo CD Application on its hosting MCE — the workflow never patches
a HostedCluster or NodePool itself. The control plane moves one minor at a time
along a verified ladder; NodePools follow, one at a time, at the user's pace.

Everything belonging to the domain's brain side lives here, mirroring its three
limbs under activities/ (upgrade_gitops/, cluster_upgrade/, upgrade_agent/):

  * the workflows, one file each — upgrade_cluster.py (one cluster's whole
    upgrade), upgrade_control_plane.py (one control-plane step, its child on a
    per-MCE slot) and upgrade_targets.py (the short read behind the "New
    upgrade" form);
  * dispatch.py — where each activity runs and under which timeouts and
    retries, shared by those workflows;
  * the pure policy they apply, with no I/O: upgrade_path.py (which steps take
    a cluster from its versions to its target) and gates.py (the planning and
    pre-flight checks and the rollout rules);
  * router.py — the HTTP surface under /workflows/cluster-upgrade; ui.py and
    ui/ — the temporary page at /ui/cluster-upgrade that calls it.

What a limb reads and the policy also matches (the day1 path, a release's
repositories, HyperShift's strings) is shared/upgrade_layout.py, not here.

Design: docs/upgrade/DESIGN.md in the Cluster-AutoUpgrade workspace.
"""
