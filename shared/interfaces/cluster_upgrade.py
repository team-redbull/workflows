"""Cluster-upgrade activity signatures — the domain's PER-MCE cluster limb.

Registered by `cluster-upgrade-worker`, one deployment inside each MCE, polling
`cluster_upgrade_activity_queue(<mce>)` — the server-lifecycle routing (see
shared/consts.py): the worker authenticates to its own API server as the
workflow ServiceAccount and dials out to Temporal, so no MCE credential ever
leaves the MCE.

Its one cluster WRITE is `sync_argo_application`. It never writes a
HostedCluster, NodePool or IDMS: those change only through a day1 commit that
Argo CD applies (DESIGN D1). `execute_proposed_action` runs a tier-2 action a
person approved, through the typed catalog.

The watches are long: they poll inside the activity and heartbeat, so a
three-hour control-plane upgrade is a handful of history events, and a worker
restart resumes from the last heartbeat with its clocks intact.
"""

from __future__ import annotations

from temporalio import activity

from shared.models.cluster_upgrade import (
    ActionResult,
    ArgoApplicationState,
    ArgoSyncRequest,
    ArgoSyncResult,
    EvidenceBundle,
    EvidenceRequest,
    GateOutcome,
    HostedClusterRequest,
    HostedClusterState,
    HostedHealth,
    HostedHealthRequest,
    MceRequest,
    MceState,
    NodePoolGateRequest,
    ProposedActionRequest,
    WatchControlPlaneRequest,
    WatchNodePoolRequest,
    WatchOutcome,
)


@activity.defn
async def read_mce_state(request: MceRequest) -> MceState:
    """ConfigMap `hypershift/supported-versions`, and the MCE's IDMS sources."""
    ...


@activity.defn
async def read_hosted_cluster(request: HostedClusterRequest) -> HostedClusterState:
    """The HostedCluster and its NodePools: versions, conditions, rollout settings."""
    ...


@activity.defn
async def read_argo_application(request: HostedClusterRequest) -> ArgoApplicationState:
    """The cluster's Application in openshift-gitops: sync, health, what is out of sync."""
    ...


@activity.defn
async def read_hosted_health(request: HostedHealthRequest) -> HostedHealth:
    """Through the hosted cluster's admin kubeconfig (the existing workflow-path pattern)."""
    ...


@activity.defn
async def sync_argo_application(request: ArgoSyncRequest) -> ArgoSyncResult:
    """Sync the Application to exactly `request.revision`, and wait for the operation.

    Raises ArgoSyncRefusedError (permanent) when anything outside
    `allowed_out_of_sync` is out of sync.
    """
    ...


@activity.defn
async def watch_control_plane(request: WatchControlPlaneRequest) -> WatchOutcome:
    """Until the HostedCluster runs `version`, nothing changes for the stall window, or the deadline.

    Done: version history[0] is `version` and Completed, ClusterVersionSucceeding
    and ValidReleaseInfo True, not Progressing, and every hosted ClusterOperator
    at `version`. Heartbeats every poll.
    """
    ...


@activity.defn
async def watch_nodepool(request: WatchNodePoolRequest) -> WatchOutcome:
    """Until the NodePool runs `version` with every node healthy, a stall, or the deadline.

    Done: status.version is `version`, UpdatingVersion False, AllNodesHealthy
    True (and SupportedVersionSkew True where the operator reports it).
    Heartbeats every poll.
    """
    ...


@activity.defn
async def watch_nodepool_gate(request: NodePoolGateRequest) -> GateOutcome:
    """Until every gate check has held for the soak window, or the deadline.

    Platform: the NodePool's nodes Ready and schedulable. Workload (when asked):
    PDBs on its nodes back to disruptionsAllowed >= 1, StatefulSets ready.
    Then the cluster's declared readiness checks. Heartbeats every poll.
    """
    ...


@activity.defn
async def collect_evidence(request: EvidenceRequest) -> EvidenceBundle:
    """Read-only evidence for the agent: conditions, operators, events, Argo, data plane."""
    ...


@activity.defn
async def execute_proposed_action(request: ProposedActionRequest) -> ActionResult:
    """Run one approved tier-2 action with the workflow's identity, through the typed catalog.

    Raises ProposedActionRefusedError (permanent) for a kind not in the catalog.
    """
    ...
