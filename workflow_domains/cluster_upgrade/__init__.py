"""Cluster-upgrade domain: upgrading HyperShift HostedClusters through day1 git.

Every change to a hosted cluster is a day1 commit followed by a sync of that
cluster's one Argo CD Application on its hosting MCE — the workflow never patches
a HostedCluster or NodePool itself. The control plane moves one minor at a time
along a verified ladder; NodePools follow, one at a time, at the user's pace.

Beside the workflows sits the pure policy they apply: upgrade_path.py (which
steps take a cluster from its current versions to its target). It does no I/O.

Design: docs/upgrade/DESIGN.md in the Cluster-AutoUpgrade workspace.
"""
